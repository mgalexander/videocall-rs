/*
 * Copyright 2025 Security Union LLC
 *
 * Licensed under either of
 *
 * * Apache License, Version 2.0
 *   (http://www.apache.org/licenses/LICENSE-2.0)
 * * MIT license
 *   (http://opensource.org/licenses/MIT)
 *
 * at your option.
 *
 * Unless you explicitly state otherwise, any contribution intentionally
 * submitted for inclusion in the work by you, as defined in the Apache-2.0
 * license, shall be dual licensed as above, without any additional terms or
 * conditions.
 */

//! Shared session logic for chat sessions.
//!
//! This module contains transport-agnostic session logic used by both
//! `WsChatSession` and `WtChatSession`. The actors become thin transport
//! adapters while all business logic lives here.

use crate::actors::chat_server::ChatServer;
use crate::actors::packet_handler::{
    classify_and_inspect, ClassifiedPacket, KeyframeRequestLimiter, PacketKind,
};
use crate::client_diagnostics::health_processor;
use crate::constants::{
    CONGESTION_DROP_THRESHOLD, CONGESTION_NOTIFY_MIN_INTERVAL, CONGESTION_WINDOW,
};
use crate::messages::server::{ClientMessage, Connect, Disconnect, JoinRoom, Packet};
use crate::messages::session::Message;
use crate::server_diagnostics::{
    send_connection_ended, send_connection_started, DataTracker, TrackerSender,
};
use crate::session_manager::SessionManager;
use actix::Addr;
use protobuf::Message as ProtobufMessage;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;
use videocall_types::protos::connection_packet::ConnectionPacket;
use videocall_types::protos::media_packet::MediaPacket;
use videocall_types::protos::packet_wrapper::packet_wrapper::PacketType;
use videocall_types::protos::packet_wrapper::PacketWrapper;

pub type SessionId = u64;
pub type RoomId = String;
pub type UserId = String;

/// Connection state for session management during election
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Connection is in testing phase (during election)
    Testing,
    /// Connection is active and should broadcast to NATS
    Active,
}

/// Result of handling an inbound packet
#[derive(Debug)]
pub enum InboundAction {
    /// Echo the packet back to sender (RTT measurement)
    Echo(Arc<Vec<u8>>),
    /// Forward to ChatServer for room routing
    Forward(Arc<Vec<u8>>),
    /// Already processed (health packet), no further action
    Processed,
    /// Keep-alive ping, no action needed
    KeepAlive,
}

// =========================================================================
// Congestion Tracking
// =========================================================================

/// Priority class for outbound packets.
///
/// NOTE: This is a temporary local stub pending p5-1 (vc-3ah), which will
/// introduce `actix-api/src/sfu/priority_queue.rs` as the canonical home for
/// `Class`. p5-7 will migrate call sites and this stub will be removed at
/// that time. The variants here are kept identical to the planned p5-1
/// shape so the migration is mechanical.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Class {
    P0Control,
    P1Audio,
    P2Keyframe,
    P3VideoBase,
    P4Enhancement,
}

impl Class {
    /// Drop-count threshold within [`Class::window`] at which a CONGESTION
    /// notification should fire for this class.
    ///
    /// P0Control has a threshold of 0 because P0 packets are NeverDrop —
    /// they are not supposed to be dropped at all. Any call into the
    /// class-aware path for P0Control indicates an upstream bug.
    fn threshold(self) -> u32 {
        match self {
            Class::P0Control => 0,
            Class::P1Audio => 3,
            Class::P2Keyframe => 1,
            Class::P3VideoBase => 5,
            Class::P4Enhancement => 10,
        }
    }

    /// Sliding window over which drops are counted for this class.
    ///
    /// For P2Keyframe the window is effectively irrelevant (threshold=1
    /// fires on the very first drop), but we still need a value for the
    /// timestamp deque's compaction logic — 60s is generous enough that a
    /// single stray entry will be discarded eventually without bloating
    /// memory.
    fn window(self) -> Duration {
        match self {
            // P0Control should never be dropped; window is irrelevant.
            Class::P0Control => Duration::from_secs(1),
            Class::P1Audio => Duration::from_millis(500),
            Class::P2Keyframe => Duration::from_secs(60),
            // Preserve the legacy 5-in-1s threshold for the base video layer.
            Class::P3VideoBase => Duration::from_secs(1),
            Class::P4Enhancement => Duration::from_secs(1),
        }
    }
}

/// Per-class drop tracking state for the class-aware congestion path.
///
/// Distinct from [`SenderDropState`] — that struct keys by sender session ID
/// and predates the priority-queue work. p5-7 will eventually migrate
/// callers off the per-sender path; until then both coexist.
struct ClassDropState {
    /// Timestamps of recent drops within the class's window. Compacted on
    /// every call: entries older than `class.window()` before `now` are
    /// popped from the front before the new drop is appended.
    drops: VecDeque<Instant>,
    /// Last time a CONGESTION notification was emitted for this class.
    /// Rate-limits subsequent `true` returns to once per
    /// [`CONGESTION_NOTIFY_MIN_INTERVAL`], matching the legacy per-sender path.
    last_notify: Option<Instant>,
}

impl ClassDropState {
    fn new() -> Self {
        Self {
            drops: VecDeque::new(),
            last_notify: None,
        }
    }
}

/// Per-sender drop tracking state for congestion feedback.
struct SenderDropState {
    /// Number of drops in the current window.
    drop_count: u32,
    /// Start of the current counting window.
    window_start: Instant,
    /// Last time a CONGESTION notification was sent for this sender.
    last_notify: Option<Instant>,
}

/// Tracks outbound packet drops per sender and generates CONGESTION feedback
/// when the drop rate exceeds the configured threshold.
///
/// Each receiver session has its own `CongestionTracker`. When the receiver's
/// outbound channel is full, the transport layer calls
/// [`CongestionTracker::record_drop`] with the sender's session ID. If enough
/// drops accumulate within the configured window, a CONGESTION `PacketWrapper`
/// is generated for publication to NATS so the sender can step down its
/// quality tier.
pub struct CongestionTracker {
    /// Drop state keyed by sender session ID.
    senders: HashMap<u64, SenderDropState>,
    /// Total drops since the last stale-entry cleanup. Cleanup runs every
    /// [`CLEANUP_INTERVAL`] drops to amortize the cost of `retain()`.
    total_drops: u32,
    /// Per-class drop state for the class-aware congestion path (p5-6).
    /// Populated lazily on the first call to
    /// [`CongestionTracker::record_drop_with_class`] for each class.
    classes: HashMap<Class, ClassDropState>,
}

impl Default for CongestionTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Number of drops between stale-entry cleanup passes. Amortizes the
/// O(n) `retain()` cost so it does not run on every single drop.
const CLEANUP_INTERVAL: u32 = 100;

impl CongestionTracker {
    pub fn new() -> Self {
        Self {
            senders: HashMap::new(),
            total_drops: 0,
            classes: HashMap::new(),
        }
    }

    /// Record a dropped outbound packet from the given sender.
    ///
    /// Returns `Some(sender_session_id)` when the drop threshold has been
    /// exceeded and a CONGESTION notification should be sent. Returns `None`
    /// if the threshold has not been met or the notification is rate-limited.
    ///
    /// Performs amortized cleanup of stale entries every [`CLEANUP_INTERVAL`]
    /// drops: any sender whose `window_start` is older than
    /// `CONGESTION_WINDOW * 10` (10 seconds of inactivity) is removed. This
    /// prevents unbounded growth when transient participants leave while
    /// avoiding an O(n) `retain()` on every single drop.
    pub fn record_drop(&mut self, sender_session_id: u64) -> Option<u64> {
        let now = Instant::now();

        // Amortized cleanup of stale sender entries.
        self.total_drops = self.total_drops.wrapping_add(1);
        if self.total_drops.is_multiple_of(CLEANUP_INTERVAL) {
            let stale_threshold = CONGESTION_WINDOW * 10;
            self.senders
                .retain(|_, state| now.duration_since(state.window_start) <= stale_threshold);
        }

        let state = self
            .senders
            .entry(sender_session_id)
            .or_insert_with(|| SenderDropState {
                drop_count: 0,
                window_start: now,
                last_notify: None,
            });

        // Reset window if it has elapsed.
        if now.duration_since(state.window_start) > CONGESTION_WINDOW {
            state.drop_count = 0;
            state.window_start = now;
        }

        state.drop_count += 1;

        if state.drop_count >= CONGESTION_DROP_THRESHOLD {
            // Rate-limit notifications.
            if let Some(last) = state.last_notify {
                if now.duration_since(last) < CONGESTION_NOTIFY_MIN_INTERVAL {
                    return None;
                }
            }
            state.last_notify = Some(now);
            state.drop_count = 0;
            state.window_start = now;
            Some(sender_session_id)
        } else {
            None
        }
    }

    /// Record a dropped outbound packet of the given priority [`Class`] and
    /// return whether a CONGESTION notification should be emitted.
    ///
    /// This is the class-aware companion to [`CongestionTracker::record_drop`]
    /// introduced in p5-6 (vc-l6x). It keeps a per-class ring buffer of
    /// drop timestamps and applies a class-specific threshold/window:
    ///
    /// | Class            | Threshold | Window  |
    /// |------------------|-----------|---------|
    /// | `P0Control`      | impossible (NeverDrop — see below) |
    /// | `P1Audio`        | 3 drops   | 500ms   |
    /// | `P2Keyframe`     | 1 drop    | (n/a — fires on first drop) |
    /// | `P3VideoBase`    | 5 drops   | 1s (preserves legacy threshold) |
    /// | `P4Enhancement`  | 10 drops  | 1s      |
    ///
    /// P0Control packets carry a NeverDrop policy and must not be dropped by
    /// any transport. If this method is called with [`Class::P0Control`],
    /// it indicates an upstream bug; we log an error and return `true` so
    /// the caller still surfaces the congestion event.
    ///
    /// As with the legacy path, repeated `true` returns are rate-limited
    /// to one per [`CONGESTION_NOTIFY_MIN_INTERVAL`] per class. The first
    /// qualifying drop (no prior notification) always returns `true`.
    ///
    /// Note: this method intentionally does not subsume
    /// [`CongestionTracker::record_drop`] — that signature must remain
    /// stable until p5-7 migrates all call sites.
    pub fn record_drop_with_class(&mut self, class: Class) -> bool {
        let now = Instant::now();

        // P0Control: NeverDrop. Reaching this branch indicates an upstream
        // bug. Log and surface the event without touching the class state
        // (P0 has no meaningful threshold).
        if class == Class::P0Control {
            error!(
                "CongestionTracker: record_drop_with_class called for P0Control \
                 (NeverDrop policy) — upstream bug"
            );
            return true;
        }

        let window = class.window();
        let threshold = class.threshold();
        let state = self
            .classes
            .entry(class)
            .or_insert_with(ClassDropState::new);

        // Compact: drop any timestamps older than the class's window before
        // counting. `now - window` may be earlier than the deque front by
        // an arbitrary amount, so iterate.
        while let Some(&front) = state.drops.front() {
            if now.duration_since(front) > window {
                state.drops.pop_front();
            } else {
                break;
            }
        }

        // Record this drop.
        state.drops.push_back(now);

        if state.drops.len() as u32 >= threshold {
            // Rate-limit notifications per class.
            if let Some(last) = state.last_notify {
                if now.duration_since(last) < CONGESTION_NOTIFY_MIN_INTERVAL {
                    return false;
                }
            }
            state.last_notify = Some(now);
            // Clear the buffer after firing so the next notification
            // requires a fresh accumulation of drops, matching the legacy
            // path's "reset count after trigger" behavior.
            state.drops.clear();
            true
        } else {
            false
        }
    }
}

// =========================================================================
// Observability helpers (logging only, no behavior change)
// =========================================================================

/// Emit a DEBUG log with the inner `RoutingHeader` fields, if present.
///
/// This is intentionally gated on `tracing::enabled!(Level::DEBUG)` so that in
/// production (where DEBUG is typically disabled) the function is effectively
/// a no-op — no field reads, no formatting, no allocation. Recall that this
/// fires on every inbound media packet (~30 fps per sender × N participants),
/// so any work performed here must be conditional on DEBUG actually being on.
///
/// The inner `MediaPacket` itself is parsed by `classify_and_inspect`, so we
/// reuse the existing parse rather than re-parsing here.
#[inline]
fn log_routing_header_if_enabled(media_packet: Option<&MediaPacket>, session_id: u64) {
    if !tracing::enabled!(tracing::Level::DEBUG) {
        return;
    }
    let Some(mp) = media_packet else {
        return;
    };
    let Some(rh) = mp.routing_header.as_ref() else {
        return;
    };
    debug!(
        sender = session_id,
        is_keyframe = rh.is_keyframe,
        temporal = rh.temporal_layer_id,
        spatial = rh.spatial_layer_id,
        audio_level = rh.audio_level,
        is_speaking = rh.is_speaking,
        "received routing header"
    );
}

/// Emit an INFO log advertising the client's declared SFU capabilities, if
/// any. Called once per CONNECTION packet (i.e. roughly once per session at
/// join time), so INFO is appropriate.
///
/// `client_capabilities` is a bitfield (see `connection_packet.proto`):
///   SFU_ROUTING_HEADER = 1, SVC = 2, SUBSCRIPTION = 4.
#[inline]
fn log_client_capabilities_if_present(
    connection_packet: Option<&ConnectionPacket>,
    session_id: u64,
    user_id: &str,
) {
    let Some(cp) = connection_packet else {
        return;
    };
    let Some(caps) = cp.client_capabilities else {
        return;
    };
    if caps == 0 {
        return;
    }
    info!(
        capabilities = caps,
        session = session_id,
        user = %user_id,
        "client connected with SFU capabilities"
    );
}

/// Shared session logic, transport-agnostic.
///
/// This struct contains all the business logic for a chat session.
/// The transport-specific actors (`WsChatSession`, `WtChatSession`)
/// own an instance of this and delegate to it.
pub struct SessionLogic {
    pub id: u64,
    pub room: RoomId,
    pub user_id: UserId,
    /// Participant's chosen display name (from JWT claims).
    /// Falls back to `user_id` when no display name is available.
    pub display_name: String,
    pub addr: Addr<ChatServer>,
    pub nats_client: async_nats::client::Client,
    pub tracker_sender: TrackerSender,
    pub session_manager: SessionManager,
    /// When true, this session is observer-only: it can receive messages
    /// but cannot publish media to the room.
    pub observer: bool,
    /// Tracks outbound packet drops per sender to generate CONGESTION feedback.
    pub congestion_tracker: CongestionTracker,
    /// Per-session rate limiter for KEYFRAME_REQUEST packets.
    pub keyframe_limiter: KeyframeRequestLimiter,
}

impl SessionLogic {
    /// Create a new session logic instance
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        addr: Addr<ChatServer>,
        room: String,
        user_id: String,
        display_name: String,
        nats_client: async_nats::client::Client,
        tracker_sender: TrackerSender,
        session_manager: SessionManager,
        observer: bool,
    ) -> Self {
        let id = (Uuid::new_v4().as_u128() & 0xffffffffffffffff) as u64;
        info!(
            "new session: room={} user_id={} display_name={} session_id={} observer={}",
            room, user_id, display_name, id, observer
        );

        SessionLogic {
            id,
            room,
            user_id,
            display_name,
            addr,
            nats_client,
            tracker_sender,
            session_manager,
            observer,
            congestion_tracker: CongestionTracker::new(),
            keyframe_limiter: KeyframeRequestLimiter::new(),
        }
    }

    // =========================================================================
    // Lifecycle
    // =========================================================================

    /// Track connection start for metrics
    pub fn track_connection_start(&self, transport: &str) {
        send_connection_started(
            &self.tracker_sender,
            self.id,
            self.user_id.clone(),
            self.room.clone(),
            transport.to_string(),
        );
    }

    /// Build MEETING_STARTED packet
    pub fn build_meeting_started(&self, start_time_ms: u64, creator_id: &str) -> Vec<u8> {
        SessionManager::build_meeting_started_packet(&self.room, start_time_ms, creator_id)
    }

    /// Build SESSION_ASSIGNED packet for this session
    pub fn build_session_assigned(&self) -> Vec<u8> {
        SessionManager::build_session_assigned_packet(self.id)
    }

    /// Build MEETING_ENDED packet (for errors)
    pub fn build_meeting_ended(&self, reason: &str) -> Vec<u8> {
        SessionManager::build_meeting_ended_packet(&self.room, reason)
    }

    /// Create Connect message for ChatServer registration
    pub fn create_connect_message<R>(&self, recipient: R) -> Connect
    where
        R: Into<actix::Recipient<Message>>,
    {
        Connect {
            id: self.id,
            addr: recipient.into(),
        }
    }

    /// Create JoinRoom message for ChatServer
    pub fn create_join_room_message(&self) -> JoinRoom {
        JoinRoom {
            room: self.room.clone(),
            session: self.id,
            user_id: self.user_id.clone(),
            display_name: self.display_name.clone(),
            observer: self.observer,
            // CONNECTION packets arrive after JoinRoom in the current
            // protocol, so the server has no client_capabilities to thread
            // here yet. p2-6 defaults to 0; capability-aware decisions in
            // later phases will update RoomState out of band.
            capabilities: 0,
        }
    }

    /// Create ClientMessage for forwarding a packet to ChatServer (NATS broadcast).
    pub fn create_client_message(&self, msg: Packet) -> ClientMessage {
        ClientMessage {
            session: self.id,
            user: self.user_id.clone(),
            room: self.room.clone(),
            msg,
        }
    }

    /// Handle JoinRoom response. Returns true if the session should stop (error case).
    pub fn handle_join_room_result(
        &self,
        result: Result<Result<(), String>, actix::MailboxError>,
    ) -> bool {
        match result {
            Ok(Ok(())) => {
                info!(
                    "Successfully joined room {} for session {}",
                    self.room, self.id
                );
                false
            }
            Ok(Err(e)) => {
                error!("Failed to join room: {}", e);
                true
            }
            Err(err) => {
                error!("Error sending JoinRoom: {:?}", err);
                true
            }
        }
    }

    /// Handle actor stopping - cleanup
    pub fn on_stopping(&self) {
        info!("Session stopping: {} in room {}", self.id, self.room);
        send_connection_ended(&self.tracker_sender, self.id);
        self.addr.do_send(Disconnect {
            session: self.id,
            room: self.room.clone(),
            user_id: self.user_id.clone(),
            display_name: self.display_name.clone(),
            observer: self.observer,
        });
    }

    // =========================================================================
    // Packet Handling
    // =========================================================================

    /// Returns true if this action should trigger connection activation.
    /// RTT probes (Echo) do not activate; any other packet does.
    pub fn should_activate_on_action(action: &InboundAction) -> bool {
        !matches!(action, InboundAction::Echo(_))
    }

    /// Handle an inbound packet from the client.
    ///
    /// Returns the action the transport should take.
    /// Observer sessions can still send RTT and health packets but all media
    /// data packets are silently dropped.
    pub fn handle_inbound(&mut self, data: &[u8]) -> InboundAction {
        // Track received data
        let data_tracker = DataTracker::new(self.tracker_sender.clone());
        data_tracker.track_received(self.id, data.len() as u64);

        // Classify and inspect once. The inspector surfaces the inner
        // MediaPacket / ConnectionPacket so we can read RoutingHeader and
        // client_capabilities without re-parsing the bytes.
        let ClassifiedPacket {
            kind,
            media_packet,
            connection_packet,
        } = classify_and_inspect(data);

        // Observability: log RoutingHeader (DEBUG, per packet) and
        // client_capabilities (INFO, one-shot per CONNECTION packet).
        // Both helpers are no-ops when the corresponding tracing level is
        // disabled, so they cost nothing on the hot path in production.
        log_routing_header_if_enabled(media_packet.as_ref(), self.id);
        log_client_capabilities_if_present(connection_packet.as_ref(), self.id, &self.user_id);

        match kind {
            PacketKind::Dropped => {
                debug!(
                    "Dropping disallowed packet from session {} (user {})",
                    self.id, self.user_id
                );
                InboundAction::Processed
            }
            PacketKind::Rtt => {
                trace!("RTT packet from {}, echoing back", self.user_id);
                let data_tracker = DataTracker::new(self.tracker_sender.clone());
                data_tracker.track_sent(self.id, data.len() as u64);
                InboundAction::Echo(Arc::new(data.to_vec()))
            }
            PacketKind::Health => {
                trace!("Health packet from {}", self.user_id);
                health_processor::process_health_packet_bytes(data, self.nats_client.clone());
                InboundAction::Processed
            }
            PacketKind::KeyframeRequest => {
                if self.observer {
                    return InboundAction::Processed;
                }
                // Rate-limit KEYFRAME_REQUEST packets to prevent abuse.
                // A malicious client could flood these to force senders to
                // continuously generate expensive keyframes.
                if !self.keyframe_limiter.allow() {
                    warn!(
                        "Rate-limiting KEYFRAME_REQUEST from session {} (user {})",
                        self.id, self.user_id
                    );
                    return InboundAction::Processed;
                }
                InboundAction::Forward(Arc::new(data.to_vec()))
            }
            PacketKind::Data => {
                if self.observer {
                    trace!(
                        "Observer session {} dropping media packet from {}",
                        self.id,
                        self.user_id
                    );
                    return InboundAction::Processed;
                }

                InboundAction::Forward(Arc::new(data.to_vec()))
            }
        }
    }

    /// Handle an outbound message from ChatServer (to be sent to client).
    ///
    /// Returns the bytes to send and tracks metrics. Cloning `Bytes` is a
    /// refcount bump, so the SFU fan-out path delivers the same allocation
    /// to every receiver.
    pub fn handle_outbound(&self, msg: &Message) -> bytes::Bytes {
        let data_tracker = DataTracker::new(self.tracker_sender.clone());
        data_tracker.track_sent(self.id, msg.msg.len() as u64);
        msg.msg.clone()
    }

    // =========================================================================
    // Congestion Feedback
    // =========================================================================

    /// Record that an outbound packet from `sender_session_id` was dropped
    /// because the outbound channel to this receiver was full.
    ///
    /// If the drop threshold is exceeded, a CONGESTION `PacketWrapper` is
    /// published to NATS so the sender's client can step down its quality
    /// tier. The notification is rate-limited per sender session.
    pub fn on_outbound_drop(&mut self, sender_session_id: u64) {
        if let Some(sender_sid) = self.congestion_tracker.record_drop(sender_session_id) {
            warn!(
                "Congestion: session {} dropping packets from sender {}, sending CONGESTION signal",
                self.id, sender_sid,
            );

            // Build a CONGESTION PacketWrapper targeted at the sender.
            // The `user_id` is set to our session's user_id so the sender
            // knows which receiver is congested. The `session_id` is set to
            // the sender's session_id so NATS routing delivers it there.
            let congestion_packet = PacketWrapper {
                packet_type: PacketType::CONGESTION.into(),
                user_id: self.user_id.as_bytes().to_vec(),
                session_id: sender_sid,
                ..Default::default()
            };

            match congestion_packet.write_to_bytes() {
                Ok(bytes) => {
                    // Publish to the sender's NATS subject so only the
                    // targeted sender receives the CONGESTION signal.
                    // The sender's subscription filter (`room.{room}.*`)
                    // matches `room.{room}.{sender_sid}`.
                    let subject = format!("room.{}.{}", self.room.replace(' ', "_"), sender_sid);
                    let nc = self.nats_client.clone();
                    let bytes = bytes::Bytes::from(bytes);
                    tokio::spawn(async move {
                        if let Err(e) = nc.publish(subject, bytes).await {
                            error!("Failed to publish CONGESTION signal: {}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to serialize CONGESTION packet: {}", e);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_inbound_action_debug() {
        let action = InboundAction::KeepAlive;
        assert_eq!(format!("{action:?}"), "KeepAlive");
    }

    #[test]
    fn test_congestion_tracker_cleans_stale_entries() {
        let mut tracker = CongestionTracker::new();

        // Insert a stale entry by manually inserting with an old window_start.
        let stale_id = 1000;
        tracker.senders.insert(
            stale_id,
            SenderDropState {
                drop_count: 0,
                // 20 seconds ago — well past the 10 * CONGESTION_WINDOW threshold
                window_start: Instant::now() - (CONGESTION_WINDOW * 20),
                last_notify: None,
            },
        );

        // Insert a fresh entry.
        let fresh_id = 2000;
        tracker.senders.insert(
            fresh_id,
            SenderDropState {
                drop_count: 0,
                window_start: Instant::now(),
                last_notify: None,
            },
        );

        assert_eq!(tracker.senders.len(), 2);

        // Set total_drops so the next record_drop triggers cleanup.
        tracker.total_drops = CLEANUP_INTERVAL - 1;

        // Recording a drop for a new sender should trigger cleanup.
        let trigger_id = 3000;
        tracker.record_drop(trigger_id);

        // The stale entry should have been removed.
        assert!(
            !tracker.senders.contains_key(&stale_id),
            "stale sender entry should be cleaned up"
        );
        // Fresh and trigger entries should remain.
        assert!(tracker.senders.contains_key(&fresh_id));
        assert!(tracker.senders.contains_key(&trigger_id));
    }

    #[test]
    fn test_congestion_tracker_retains_active_entries() {
        let mut tracker = CongestionTracker::new();

        // Record drops for two senders.
        tracker.record_drop(100);
        tracker.record_drop(200);

        assert_eq!(tracker.senders.len(), 2);

        // Record another drop — both entries are fresh, nothing should be cleaned.
        tracker.record_drop(100);

        assert_eq!(tracker.senders.len(), 2);
        assert!(tracker.senders.contains_key(&100));
        assert!(tracker.senders.contains_key(&200));
    }

    // =====================================================================
    // Drop recording and counting
    // =====================================================================

    #[test]
    fn test_drop_recording_increments_count() {
        let mut tracker = CongestionTracker::new();
        let sender_id = 42;

        // Record a single drop — should not yet trigger notification.
        let result = tracker.record_drop(sender_id);
        assert!(
            result.is_none(),
            "single drop should not trigger notification"
        );

        // The internal count should be 1.
        let state = tracker.senders.get(&sender_id).unwrap();
        assert_eq!(state.drop_count, 1);
    }

    #[test]
    fn test_drop_recording_multiple_senders_independent() {
        let mut tracker = CongestionTracker::new();

        // Record drops for two different senders.
        for _ in 0..3 {
            tracker.record_drop(100);
        }
        for _ in 0..2 {
            tracker.record_drop(200);
        }

        // Each sender should have independent counts.
        assert_eq!(tracker.senders.get(&100).unwrap().drop_count, 3);
        assert_eq!(tracker.senders.get(&200).unwrap().drop_count, 2);
    }

    #[test]
    fn test_drop_window_resets_after_expiry() {
        let mut tracker = CongestionTracker::new();
        let sender_id = 50;

        // Manually insert a sender with a window that started in the past
        // (just beyond CONGESTION_WINDOW) so the next record_drop resets it.
        tracker.senders.insert(
            sender_id,
            SenderDropState {
                drop_count: 3,
                window_start: Instant::now() - (CONGESTION_WINDOW + Duration::from_millis(10)),
                last_notify: None,
            },
        );

        // record_drop should reset the window and set count to 1 (not 4).
        tracker.record_drop(sender_id);
        let state = tracker.senders.get(&sender_id).unwrap();
        assert_eq!(
            state.drop_count, 1,
            "drop count should reset to 1 after window expiry"
        );
    }

    // =====================================================================
    // Congestion notification triggering
    // =====================================================================

    #[test]
    fn test_notification_triggers_at_threshold() {
        let mut tracker = CongestionTracker::new();
        let sender_id = 99;

        // Record drops up to one less than threshold — no notification.
        for _ in 0..(CONGESTION_DROP_THRESHOLD - 1) {
            let result = tracker.record_drop(sender_id);
            assert!(result.is_none());
        }

        // The threshold-th drop should trigger a notification.
        let result = tracker.record_drop(sender_id);
        assert_eq!(
            result,
            Some(sender_id),
            "should return sender_id when threshold is reached"
        );
    }

    #[test]
    fn test_notification_resets_count_after_trigger() {
        let mut tracker = CongestionTracker::new();
        let sender_id = 77;

        // Reach threshold to trigger notification.
        for _ in 0..CONGESTION_DROP_THRESHOLD {
            tracker.record_drop(sender_id);
        }

        // After triggering, count should be reset to 0.
        let state = tracker.senders.get(&sender_id).unwrap();
        assert_eq!(
            state.drop_count, 0,
            "drop count should reset after notification"
        );
    }

    #[test]
    fn test_rate_limiting_suppresses_rapid_notifications() {
        let mut tracker = CongestionTracker::new();
        let sender_id = 55;

        // First burst: trigger notification.
        for _ in 0..CONGESTION_DROP_THRESHOLD {
            tracker.record_drop(sender_id);
        }
        // The last call above returned Some(55). Now the last_notify is set.

        // Second burst immediately after: should be rate-limited because
        // CONGESTION_NOTIFY_MIN_INTERVAL has not elapsed.
        for i in 0..CONGESTION_DROP_THRESHOLD {
            let result = tracker.record_drop(sender_id);
            if i < CONGESTION_DROP_THRESHOLD - 1 {
                // Below threshold — always None.
                assert!(result.is_none());
            } else {
                // At threshold — rate-limited, so still None.
                assert!(
                    result.is_none(),
                    "notification should be suppressed by rate limiter"
                );
            }
        }
    }

    // =====================================================================
    // Stale entry cleanup
    // =====================================================================

    #[test]
    fn test_stale_cleanup_removes_multiple_stale_entries() {
        let mut tracker = CongestionTracker::new();

        // Insert several stale entries.
        for id in 1..=5 {
            tracker.senders.insert(
                id,
                SenderDropState {
                    drop_count: 0,
                    window_start: Instant::now() - (CONGESTION_WINDOW * 20),
                    last_notify: None,
                },
            );
        }

        // Insert one fresh entry.
        tracker.senders.insert(
            100,
            SenderDropState {
                drop_count: 0,
                window_start: Instant::now(),
                last_notify: None,
            },
        );

        assert_eq!(tracker.senders.len(), 6);

        // Set total_drops so the next record_drop triggers cleanup.
        tracker.total_drops = CLEANUP_INTERVAL - 1;

        // Trigger cleanup by recording a drop.
        tracker.record_drop(200);

        // All stale entries (1-5) should be gone; fresh (100) and new (200) remain.
        assert_eq!(tracker.senders.len(), 2);
        assert!(tracker.senders.contains_key(&100));
        assert!(tracker.senders.contains_key(&200));
    }

    #[test]
    fn test_entry_just_under_boundary_is_retained() {
        let mut tracker = CongestionTracker::new();

        // Insert an entry slightly under the stale boundary (10 * CONGESTION_WINDOW).
        // Use a 500ms margin to account for time elapsed between insertion and
        // the `retain` call inside `record_drop`.
        tracker.senders.insert(
            1,
            SenderDropState {
                drop_count: 2,
                window_start: Instant::now() - (CONGESTION_WINDOW * 10)
                    + Duration::from_millis(500),
                last_notify: None,
            },
        );

        // Set total_drops so the next record_drop triggers cleanup.
        tracker.total_drops = CLEANUP_INTERVAL - 1;

        tracker.record_drop(2);

        // Entry 1 is within the boundary — should be retained.
        assert!(
            tracker.senders.contains_key(&1),
            "entry just under stale boundary should be retained"
        );
    }

    // =====================================================================
    // should_notify_sender() — tested indirectly through record_drop
    // =====================================================================

    #[test]
    fn test_first_notification_for_sender_has_no_rate_limit() {
        let mut tracker = CongestionTracker::new();
        let sender_id = 10;

        // First time reaching threshold — no prior last_notify, should fire.
        for _ in 0..CONGESTION_DROP_THRESHOLD {
            tracker.record_drop(sender_id);
        }

        // Verify last_notify was set.
        let state = tracker.senders.get(&sender_id).unwrap();
        assert!(
            state.last_notify.is_some(),
            "last_notify should be set after first notification"
        );
    }

    #[test]
    fn test_notification_allowed_after_rate_limit_expires() {
        let mut tracker = CongestionTracker::new();
        let sender_id = 30;

        // Simulate a previous notification that happened long enough ago
        // that the rate limit has expired.
        tracker.senders.insert(
            sender_id,
            SenderDropState {
                drop_count: 0,
                window_start: Instant::now(),
                last_notify: Some(
                    Instant::now() - CONGESTION_NOTIFY_MIN_INTERVAL - Duration::from_millis(10),
                ),
            },
        );

        // Record enough drops to hit threshold.
        for _ in 0..CONGESTION_DROP_THRESHOLD {
            tracker.record_drop(sender_id);
        }

        // Should trigger because rate limit has expired.
        // The last record_drop was the threshold-th, which was the one that returned.
        // We need to check the return value of the last call.
        // Let's redo this more carefully.
        let mut tracker2 = CongestionTracker::new();
        tracker2.senders.insert(
            sender_id,
            SenderDropState {
                drop_count: 0,
                window_start: Instant::now(),
                last_notify: Some(
                    Instant::now() - CONGESTION_NOTIFY_MIN_INTERVAL - Duration::from_millis(10),
                ),
            },
        );

        let mut triggered = false;
        for _ in 0..CONGESTION_DROP_THRESHOLD {
            if tracker2.record_drop(sender_id).is_some() {
                triggered = true;
            }
        }
        assert!(
            triggered,
            "notification should fire after rate-limit window expires"
        );
    }

    #[test]
    fn test_default_trait_impl() {
        // Verify Default trait works and produces an empty tracker.
        let tracker = CongestionTracker::default();
        assert!(tracker.senders.is_empty());
    }

    // =====================================================================
    // Class-aware drop accounting (p5-6 / vc-l6x)
    // =====================================================================

    #[test]
    fn test_record_drop_with_class_p2_keyframe_fires_on_first_drop() {
        let mut tracker = CongestionTracker::new();
        // P2Keyframe threshold is 1 — a single drop must fire on a fresh
        // tracker (no prior notify => rate-limit not engaged).
        assert!(tracker.record_drop_with_class(Class::P2Keyframe));
    }

    #[test]
    fn test_record_drop_with_class_p4_below_threshold() {
        let mut tracker = CongestionTracker::new();
        // P4Enhancement threshold is 10 within 1s. 9 calls back-to-back
        // (well within the 1s window) must all return false.
        for i in 0..9 {
            assert!(
                !tracker.record_drop_with_class(Class::P4Enhancement),
                "drop #{} (of 9) should be below threshold",
                i + 1
            );
        }
    }

    #[test]
    fn test_record_drop_with_class_p4_at_threshold() {
        let mut tracker = CongestionTracker::new();
        // First 9 below threshold.
        for _ in 0..9 {
            assert!(!tracker.record_drop_with_class(Class::P4Enhancement));
        }
        // 10th call reaches the threshold and must fire.
        assert!(tracker.record_drop_with_class(Class::P4Enhancement));
    }

    #[test]
    fn test_record_drop_with_class_p4_drops_outside_window_dont_count() {
        let mut tracker = CongestionTracker::new();
        // Manually seed the class state with 9 timestamps from > 1s ago
        // (P4's window is 1s). These should be compacted away on the next
        // call, leaving the new drop as the only entry and not firing.
        let stale = Instant::now() - Duration::from_secs(2);
        let mut deque = VecDeque::new();
        for _ in 0..9 {
            deque.push_back(stale);
        }
        tracker.classes.insert(
            Class::P4Enhancement,
            ClassDropState {
                drops: deque,
                last_notify: None,
            },
        );

        // Stale entries should be compacted before counting; new drop is
        // the only one in-window => below threshold => returns false.
        assert!(!tracker.record_drop_with_class(Class::P4Enhancement));
        let state = tracker.classes.get(&Class::P4Enhancement).unwrap();
        assert_eq!(
            state.drops.len(),
            1,
            "stale entries should have been compacted; only the fresh drop remains"
        );
    }

    #[test]
    fn test_record_drop_with_class_p1_audio_threshold() {
        let mut tracker = CongestionTracker::new();
        // P1Audio: 3 in 500ms.
        assert!(!tracker.record_drop_with_class(Class::P1Audio));
        assert!(!tracker.record_drop_with_class(Class::P1Audio));
        // 3rd call hits threshold.
        assert!(tracker.record_drop_with_class(Class::P1Audio));
    }

    #[test]
    fn test_record_drop_with_class_p1_audio_drops_outside_500ms_dont_count() {
        let mut tracker = CongestionTracker::new();
        // Seed two stale (>500ms ago) drops; the next call within the
        // window should NOT cross the 3-drop threshold.
        let stale = Instant::now() - Duration::from_millis(600);
        let mut deque = VecDeque::new();
        deque.push_back(stale);
        deque.push_back(stale);
        tracker.classes.insert(
            Class::P1Audio,
            ClassDropState {
                drops: deque,
                last_notify: None,
            },
        );

        // Compaction drops the two stale entries; only the new one counts.
        // 1 < 3 => no fire.
        assert!(!tracker.record_drop_with_class(Class::P1Audio));
    }

    #[test]
    fn test_record_drop_with_class_p0_control_logs_and_returns_true() {
        let mut tracker = CongestionTracker::new();
        // P0Control should never be dropped; if it is, treat as upstream
        // bug — log an error and return true. State is intentionally
        // untouched.
        assert!(tracker.record_drop_with_class(Class::P0Control));
        assert!(
            !tracker.classes.contains_key(&Class::P0Control),
            "P0Control path should not allocate per-class state"
        );
    }

    #[test]
    fn test_record_drop_with_class_p3_preserves_legacy_5_in_1s() {
        let mut tracker = CongestionTracker::new();
        // P3VideoBase preserves the legacy 5/1s threshold.
        for i in 0..4 {
            assert!(
                !tracker.record_drop_with_class(Class::P3VideoBase),
                "drop #{} should be below threshold",
                i + 1
            );
        }
        // 5th call fires.
        assert!(tracker.record_drop_with_class(Class::P3VideoBase));
    }

    #[test]
    fn test_record_drop_with_class_rate_limited_after_fire() {
        let mut tracker = CongestionTracker::new();
        // Drive P4Enhancement to fire once.
        for _ in 0..9 {
            tracker.record_drop_with_class(Class::P4Enhancement);
        }
        assert!(tracker.record_drop_with_class(Class::P4Enhancement));

        // Drive it again immediately. last_notify is fresh so even if we
        // re-cross the threshold, the rate limiter must suppress the
        // notification.
        for _ in 0..10 {
            assert!(
                !tracker.record_drop_with_class(Class::P4Enhancement),
                "follow-up notifications must be rate-limited within \
                 CONGESTION_NOTIFY_MIN_INTERVAL"
            );
        }
    }

    #[test]
    fn test_record_drop_with_class_independent_per_class() {
        // Drops in one class must not affect another class's count.
        let mut tracker = CongestionTracker::new();
        for _ in 0..2 {
            assert!(!tracker.record_drop_with_class(Class::P1Audio));
        }
        // P4Enhancement count is independent; 1 drop is still below 10.
        assert!(!tracker.record_drop_with_class(Class::P4Enhancement));
        // P1Audio's 3rd call still fires (the P4 drop did not consume it).
        assert!(tracker.record_drop_with_class(Class::P1Audio));
    }

    #[test]
    fn test_should_activate_on_action() {
        // Echo (RTT probe) should NOT activate.
        assert!(!SessionLogic::should_activate_on_action(
            &InboundAction::Echo(Arc::new(vec![]))
        ));
        // Forward, Processed, KeepAlive should activate.
        assert!(SessionLogic::should_activate_on_action(
            &InboundAction::Forward(Arc::new(vec![]))
        ));
        assert!(SessionLogic::should_activate_on_action(
            &InboundAction::Processed
        ));
        assert!(SessionLogic::should_activate_on_action(
            &InboundAction::KeepAlive
        ));
    }
}
