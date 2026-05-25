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
use crate::messages::server::{
    ClientMessage, Connect, Disconnect, JoinDeclineKind, JoinRoom, JoinRoomError, Packet,
};
use crate::messages::session::Message;
use crate::server_diagnostics::{
    send_connection_ended, send_connection_started, DataTracker, TrackerSender,
};
use crate::session_manager::SessionManager;
use crate::sfu::priority_queue::Class;
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

/// Bound on the per-session off-actor NATS publish queue (vc-ud6o E3).
///
/// Each session has ONE long-lived publisher task draining a bounded channel
/// of this depth. `Handler<Packet>` `try_send`s prepared media frames into it
/// and DROPS on full. Under a NATS publish stall the drainer parks on
/// `Client::publish().await` (the client's own command channel is bounded too),
/// the queue fills, and new media frames are shed — dropping media under
/// congestion is the correct SFU behavior. A small bound caps per-session
/// memory tightly and sheds load fast rather than buffering stale frames.
const SESSION_PUBLISH_QUEUE_CAP: usize = 64;

/// A media frame prepared for off-actor NATS publish (vc-ud6o E3).
///
/// All actor-owned-state-dependent work (the `Active` gate, `session_id`
/// rewrite, subject construction) is done on the calling transport thread
/// BEFORE the frame is queued, so the drainer task only performs the
/// `Client::publish().await`.
struct PreparedPublish {
    subject: String,
    payload: bytes::Bytes,
}

/// Zero-copy `Bytes` owner wrapping the `Arc<Vec<u8>>` already held by an
/// inbound [`Packet`] (vc-ud6o item-2).
///
/// `bytes::Bytes::from_owner` requires `T: AsRef<[u8]> + Send + 'static`.
/// `Arc<Vec<u8>>` only implements `AsRef<Vec<u8>>`, so this newtype adapts it
/// to `AsRef<[u8]>`. Using it as the `Bytes` owner lets the publisher reuse the
/// existing buffer instead of copying it on the common encrypted-media path.
struct ArcBufOwner(Arc<Vec<u8>>);

impl AsRef<[u8]> for ArcBufOwner {
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
    }
}

/// Build the NATS publish payload for an off-actor media frame (vc-ud6o).
///
/// Applies the `session_id == 0` rewrite, exactly matching the former on-actor
/// `Handler<ClientMessage>`: if the wrapper parses and its `session_id` is
/// unset, stamp it with `session` and re-serialize. ONLY that branch allocates
/// a fresh buffer; every other path — `session_id` already set, or opaque /
/// unparseable bytes (the common encrypted-media case) — returns `Bytes`
/// borrowed zero-copy over the `Arc<Vec<u8>>` the [`Packet`] already holds (via
/// [`ArcBufOwner`] + [`bytes::Bytes::from_owner`]), avoiding a per-frame copy.
fn prepare_publish_payload(session: u64, data: Arc<Vec<u8>>) -> bytes::Bytes {
    match PacketWrapper::parse_from_bytes(&data) {
        Ok(mut packet_wrapper) if packet_wrapper.session_id == 0 => {
            packet_wrapper.session_id = session;
            match packet_wrapper.write_to_bytes() {
                Ok(bytes) => bytes::Bytes::from(bytes),
                Err(e) => {
                    error!("Failed to serialize PacketWrapper with session_id: {}", e);
                    bytes::Bytes::from_owner(ArcBufOwner(data))
                }
            }
        }
        // Parsed but session_id already set, or unparseable opaque bytes:
        // publish the original buffer as-is (zero-copy over the Arc).
        _ => bytes::Bytes::from_owner(ArcBufOwner(data)),
    }
}

/// Shared, lock-free map of per-session [`ConnectionState`] (vc-ud6o E3).
///
/// Owned authoritatively by the single `ChatServer` actor thread (the only
/// writer: `Connect` inserts `Testing`, `ActivateConnection` promotes to
/// `Active`, `Disconnect`/`Leave` remove). A `Clone` of the `Arc` is handed to
/// every [`SessionLogic`] so the off-actor media-publish path can read the
/// per-session `Active` gate without round-tripping through the actor mailbox.
///
/// `DashMap` gives lock-free sharded reads that contend only with a write to
/// the SAME session's shard — and there is exactly one writer (the actor), so
/// reads on the hot media path are effectively uncontended.
pub type SharedConnectionStates = Arc<dashmap::DashMap<SessionId, ConnectionState>>;

/// Result of handling an inbound packet
#[derive(Debug)]
pub enum InboundAction {
    /// Echo the packet back to sender (RTT measurement)
    Echo(Arc<Vec<u8>>),
    /// Forward to ChatServer for room routing, carrying the
    /// pre-computed `PacketKind` so the fan-out path can branch
    /// without re-parsing the wrapper.
    Forward(Arc<Vec<u8>>, PacketKind),
    /// Already processed (health packet), no further action
    Processed,
    /// Keep-alive ping, no action needed
    KeepAlive,
}

/// vc-n9o: why a transport session is being torn down. Both `WtChatSession`
/// and `WsChatSession` record this and emit `sfu_session_teardown_total`
/// exactly once (in their `stopping` hook) with [`TeardownReason::label`], so
/// the teardown counter lines up with `sfu_join_decision_total`:
///   * `Redirect` ↔ `sfu_join_decision_total{outcome=redirect}` — the bucket
///     the vc-n9o teardown-reliability fix protects (must not be missing on
///     either transport, must not be inflated by non-redirect teardowns).
///   * `Normal`   — ordinary client-initiated / lifecycle teardown.
///   * `Error`    — internal error, validation failure, or hard-cap reject.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeardownReason {
    Redirect,
    Normal,
    Error,
}

impl TeardownReason {
    pub fn label(self) -> &'static str {
        match self {
            TeardownReason::Redirect => "redirect",
            TeardownReason::Normal => "normal",
            TeardownReason::Error => "error",
        }
    }

    /// Map a [`JoinDeclineKind`] (the typed `JoinRoom` decline) onto the
    /// teardown reason. A `Reject` is bucketed as `Error` for teardown
    /// purposes — the redirect-vs-teardown invariant only requires that
    /// `Redirect` declines map to `Redirect` teardowns and that nothing else
    /// does, so a hard-cap reject must NOT be labeled `redirect`.
    pub fn from_join_decline(kind: JoinDeclineKind) -> Self {
        match kind {
            JoinDeclineKind::Redirect => TeardownReason::Redirect,
            JoinDeclineKind::Reject | JoinDeclineKind::Error => TeardownReason::Error,
        }
    }
}

// =========================================================================
// Congestion Tracking
// =========================================================================

/// Drop-count threshold within [`class_window`] at which a CONGESTION
/// notification should fire for this class.
///
/// P0Control has a threshold of 0 because P0 packets are NeverDrop —
/// they are not supposed to be dropped at all. Any call into the
/// class-aware path for P0Control indicates an upstream bug.
fn class_threshold(class: Class) -> u32 {
    match class {
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
fn class_window(class: Class) -> Duration {
    match class {
        // P0Control should never be dropped; window is irrelevant.
        Class::P0Control => Duration::from_secs(1),
        Class::P1Audio => Duration::from_millis(500),
        Class::P2Keyframe => Duration::from_secs(60),
        // Preserve the legacy 5-in-1s threshold for the base video layer.
        Class::P3VideoBase => Duration::from_secs(1),
        Class::P4Enhancement => Duration::from_secs(1),
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

        let window = class_window(class);
        let threshold = class_threshold(class);
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
    /// Shared, lock-free view of per-session connection states (vc-ud6o E3).
    ///
    /// A clone of the `ChatServer`-owned map. Read on the off-actor media-
    /// publish path to gate publishing on `ConnectionState::Active`, exactly
    /// as the on-actor `Handler<ClientMessage>` did, but without occupying the
    /// single actor thread per packet.
    pub connection_states: SharedConnectionStates,
    /// Bounded sender into this session's single long-lived publisher task
    /// (vc-ud6o E3). `forward_packet` `try_send`s prepared media frames here
    /// and drops on full. This replaces the prior unbounded `tokio::spawn`-
    /// per-frame model: one task per session, bounded memory, drop-on-full
    /// backpressure. See [`SESSION_PUBLISH_QUEUE_CAP`].
    publish_tx: tokio::sync::mpsc::Sender<PreparedPublish>,
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
        connection_states: SharedConnectionStates,
    ) -> Self {
        let id = (Uuid::new_v4().as_u128() & 0xffffffffffffffff) as u64;
        info!(
            "new session: room={} user_id={} display_name={} session_id={} observer={}",
            room, user_id, display_name, id, observer
        );

        // vc-ud6o E3: spawn the single long-lived per-session publisher task.
        // It drains the bounded queue and performs the actual
        // `Client::publish().await`, applying real backpressure to itself:
        // when NATS stalls the drainer parks on `publish().await`, the bounded
        // queue fills, and `forward_packet`'s `try_send` sheds new media. The
        // task ends when the sender (held in this `SessionLogic`) is dropped,
        // i.e. on session teardown. `new` is always called from inside a tokio
        // runtime (async HTTP/WT handlers), so `tokio::spawn` is valid here —
        // the same precondition `emit_congestion`'s spawn already relies on.
        let (publish_tx, publish_rx) =
            tokio::sync::mpsc::channel::<PreparedPublish>(SESSION_PUBLISH_QUEUE_CAP);
        Self::spawn_publisher(id, nats_client.clone(), publish_rx);

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
            connection_states,
            publish_tx,
        }
    }

    /// Spawn the single long-lived per-session NATS publisher task (vc-ud6o E3).
    ///
    /// Drains [`PreparedPublish`] frames in order and awaits each
    /// `Client::publish`. Because it processes one frame at a time, a NATS
    /// publish stall blocks the drainer (not the transport actor) and the
    /// bounded upstream queue applies drop-on-full backpressure. Exits cleanly
    /// when the channel closes (the `SessionLogic` holding the sender is
    /// dropped on teardown).
    fn spawn_publisher(
        session: u64,
        nc: async_nats::client::Client,
        mut rx: tokio::sync::mpsc::Receiver<PreparedPublish>,
    ) {
        tokio::spawn(async move {
            while let Some(PreparedPublish { subject, payload }) = rx.recv().await {
                if let Err(e) = nc.publish(subject.clone(), payload).await {
                    error!("error publishing message to {subject}: {e}");
                } else {
                    trace!("published message to {subject}");
                }
            }
            trace!("publisher task for session {session} ended (channel closed)");
        });
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

    /// Forward an inbound packet toward the room (vc-ud6o E3).
    ///
    /// This replaces the per-packet `addr.do_send(create_client_message(..))`
    /// that previously funneled EVERY inbound media packet through the single
    /// `ChatServer` actor mailbox — the throughput bottleneck that starved
    /// `JoinRoom` registration at scale.
    ///
    /// Routing by [`PacketKind`]:
    ///
    /// * High-volume media (`PacketKind::Data` — AUDIO/VIDEO/SCREEN, plus
    ///   opaque CONNECTION packets) is published to NATS **directly from this
    ///   transport/session task**, never touching the actor. The actor's
    ///   per-packet load is thereby removed.
    /// * Control packets that must read/mutate actor-owned state stay on the
    ///   actor via `do_send(ClientMessage)`:
    ///   * `PacketKind::SubscriptionUpdate` — applied to the per-room
    ///     `SubscriptionStore` on the single-writer actor thread, preserving
    ///     ordering against `JoinRoom` member-table updates and forwarder
    ///     reads. These are rare (one per subscription change).
    ///   * `PacketKind::KeyframeRequest` — the layer-aware drop policy reads
    ///     `room_members` + the forwarder's cached `LayerSelection`. These are
    ///     rate-limited per session and rare relative to media frames.
    ///
    /// The off-actor media path reproduces, exactly, the three media-relevant
    /// behaviors of the former on-actor `Handler<ClientMessage>`:
    ///   1. Connection-state gating: publish only when this session is
    ///      `ConnectionState::Active` (read lock-free from the shared map);
    ///      otherwise drop, matching the "Testing state" skip.
    ///   2. `session_id == 0` rewrite to this session's id before publish.
    ///   3. Subject `room.{room}.{session}` with spaces replaced by `_`.
    pub fn forward_packet(&self, msg: Packet) {
        match msg.kind {
            // Control paths: keep on the actor (rare; need actor-owned state).
            PacketKind::SubscriptionUpdate | PacketKind::KeyframeRequest => {
                self.addr.do_send(self.create_client_message(msg));
            }
            // High-volume media + opaque CONNECTION packets: publish off-actor.
            // CONNECTION classifies as `Data` (it is forwarded opaquely so peers
            // receive the join notification), so it shares the media fast path.
            PacketKind::Data => self.publish_media_off_actor(msg.data),
            // RTT / Health / Dropped never reach a `Forward` action (they are
            // handled inline in `handle_inbound`), so they cannot appear here.
            // Route them defensively through the actor publish path rather than
            // silently misclassifying — and log, so a future change that starts
            // forwarding one of these kinds is caught instead of silently taking
            // the media fast path.
            PacketKind::Rtt | PacketKind::Health | PacketKind::Dropped => {
                warn!(
                    "forward_packet received unexpected PacketKind {:?} for session {} \
                     (not produced by handle_inbound's Forward path); routing via actor",
                    msg.kind, self.id
                );
                self.addr.do_send(self.create_client_message(msg));
            }
        }
    }

    /// Off-actor NATS publish for high-volume media packets (vc-ud6o E3).
    ///
    /// Mirrors the media branch of the former on-actor `Handler<ClientMessage>`,
    /// but runs entirely off the single `ChatServer` mailbox. All
    /// actor-state-dependent work happens synchronously on the calling
    /// transport thread:
    ///   1. Connection-state gate — a lock-free `DashMap` read.
    ///   2. `session_id == 0` rewrite (re-serialize only when it fires).
    ///   3. Subject construction.
    ///
    /// The prepared frame is then `try_send`'d into this session's bounded
    /// publisher queue and DROPPED on full (NATS publish stall / congestion).
    /// This replaces the prior unbounded per-frame `tokio::spawn`: one
    /// long-lived task per session, bounded memory, correct SFU load-shedding.
    ///
    /// `data` is the `Arc<Vec<u8>>` already held by the `Packet`, so the common
    /// encrypted-media path (parse fails, or no rewrite needed) builds `Bytes`
    /// directly over the shared buffer via `Bytes::from_owner` — zero copy.
    fn publish_media_off_actor(&self, data: Arc<Vec<u8>>) {
        let session = self.id;

        // (1) Connection-state gate — only publish when Active. A missing
        // entry is treated as Testing (the former handler's default), so we
        // drop, exactly preserving the pre-vc-ud6o behavior.
        let active = self
            .connection_states
            .get(&session)
            .map(|s| *s == ConnectionState::Active)
            .unwrap_or(false);
        if !active {
            trace!(
                "Skipping off-actor NATS publish for session {} (not Active)",
                session
            );
            return;
        }

        // (3) Subject (vc-kcpg): with K==1 (default) this is the legacy
        // `room.{room}.{session}` (byte-identical to pre-vc-kcpg); with K>1 the
        // publisher's media goes to `room.{room}.{shard}.{session}` where
        // `shard = jump_hash(session, K)`, so the room's publishers are spread
        // across K ingest dispatchers. `K` is the process-wide env snapshot, so
        // it matches the subscribe side's `K` exactly.
        let k = crate::sfu::config::SfuConfig::ingest_shards_cached();
        let subject = crate::models::build_publish_subject(&self.room, session, k);

        // (2) session_id == 0 rewrite (see `prepare_publish_payload`).
        let payload = prepare_publish_payload(session, data);

        // Hand off to the per-session publisher task. Drop-on-full: when the
        // queue is saturated (NATS stall), shedding media is correct for an
        // SFU. `Closed` only happens during teardown after the task exits.
        match self
            .publish_tx
            .try_send(PreparedPublish { subject, payload })
        {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                crate::metrics::SFU_DROPPED_TOTAL
                    .with_label_values(&["publish_backpressure"])
                    .inc();
                trace!(
                    "Dropping media frame for session {} — publish queue full (NATS backpressure)",
                    session
                );
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                trace!(
                    "Publish queue closed for session {} (teardown); dropping frame",
                    session
                );
            }
        }
    }

    /// Handle JoinRoom response.
    ///
    /// Returns `None` when the session should keep running (join succeeded),
    /// or `Some(reason)` when it should stop — carrying the [`TeardownReason`]
    /// so the transport actor can label `sfu_session_teardown_total` to match
    /// the `sfu_join_decision_total` decision (vc-n9o). A `Redirect` decline
    /// maps to `TeardownReason::Redirect`; a `Reject` or any error maps to
    /// `Error`; a mailbox failure maps to `Error`.
    pub fn handle_join_room_result(
        &self,
        result: Result<Result<(), JoinRoomError>, actix::MailboxError>,
    ) -> Option<TeardownReason> {
        match result {
            Ok(Ok(())) => {
                info!(
                    "Successfully joined room {} for session {}",
                    self.room, self.id
                );
                None
            }
            Ok(Err(e)) => {
                error!("Failed to join room ({:?}): {}", e.kind, e.message);
                Some(TeardownReason::from_join_decline(e.kind))
            }
            Err(err) => {
                error!("Error sending JoinRoom: {:?}", err);
                Some(TeardownReason::Error)
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
            // Real client-initiated disconnect (transport closed). The
            // client may reconnect within RECONNECT_GRACE_PERIOD, so the
            // standard deferred-leave path applies. vc-9g7: only the
            // cross-region redirect path sets this true.
            redirect: false,
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
                InboundAction::Forward(Arc::new(data.to_vec()), PacketKind::KeyframeRequest)
            }
            PacketKind::SubscriptionUpdate => {
                // SUBSCRIPTION_UPDATE is a server-local control packet applied
                // on the ChatServer actor (per-room SubscriptionStore). Pre-
                // vc-ud6o it classified as `Data`, so observer sessions dropped
                // it before forwarding — preserve that exactly. Non-observers
                // forward it with the distinct `SubscriptionUpdate` kind so the
                // transport routes it through the actor mailbox (NOT the off-
                // actor media-publish fast path), keeping the store mutation on
                // the single-writer actor thread.
                if self.observer {
                    return InboundAction::Processed;
                }
                InboundAction::Forward(Arc::new(data.to_vec()), PacketKind::SubscriptionUpdate)
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

                InboundAction::Forward(Arc::new(data.to_vec()), PacketKind::Data)
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
    ///
    /// This is the legacy per-sender path. Prefer
    /// [`SessionLogic::on_outbound_drop_class`] for new call sites — it routes
    /// the drop through the class-aware [`CongestionTracker::record_drop_with_class`]
    /// path so a class-specific drop fires a class-specific CONGESTION signal.
    pub fn on_outbound_drop(&mut self, sender_session_id: u64) {
        if let Some(sender_sid) = self.congestion_tracker.record_drop(sender_session_id) {
            self.emit_congestion(sender_sid);
        }
    }

    /// Class-aware companion to [`SessionLogic::on_outbound_drop`].
    ///
    /// Records the drop into the per-class [`CongestionTracker`] state and, if
    /// the class-specific threshold has been exceeded, publishes a CONGESTION
    /// signal targeted at the sender. This is the wire-up that closes the
    /// loop from `PrioritySender::send()`'s `SendOutcome::Dropped(class, _)`
    /// back to the originating sender (p5-7).
    pub fn on_outbound_drop_class(&mut self, sender_session_id: u64, class: Class) {
        if self.congestion_tracker.record_drop_with_class(class) {
            self.emit_congestion(sender_session_id);
        }
    }

    /// Publish a CONGESTION `PacketWrapper` targeted at `sender_sid` via NATS.
    ///
    /// Shared helper for the legacy per-sender path
    /// ([`SessionLogic::on_outbound_drop`]) and the class-aware path
    /// ([`SessionLogic::on_outbound_drop_class`]). The receiver's session
    /// emits the signal; routing delivers it to the sender's NATS subject so
    /// only the targeted sender receives it.
    fn emit_congestion(&self, sender_sid: u64) {
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
                // Publish to the TARGET SENDER's NATS subject so only the
                // congested-from sender's media path carries the CONGESTION
                // signal. vc-kcpg: this is a TARGETED publish — it must land on
                // the SENDER's ingest shard (`jump_hash(sender_sid, K)`), not
                // ours, so the dispatcher subscribing that shard delivers it.
                // With K==1 this collapses to the legacy
                // `room.{room}.{sender_sid}` 3-token subject. (Delivery to the
                // sender CLIENT still goes through the room's shared fan-out —
                // every dispatcher fans out to the full receiver set — so the
                // sender receives this regardless of which shard ingested it.)
                let k = crate::sfu::config::SfuConfig::ingest_shards_cached();
                let subject = crate::models::build_publish_subject(&self.room, sender_sid, k);
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

    /// p5-7 wire-up: a synthetic burst of 5 P3VideoBase drops within the 1s
    /// window must cross the class-specific threshold and surface as a
    /// `true` return from `CongestionTracker::record_drop_with_class`, which
    /// is the boolean that `SessionLogic::on_outbound_drop_class` (added in
    /// p5-7) routes into the CONGESTION emit path.
    ///
    /// The wire-up itself (`on_outbound_drop_class` → tracker → NATS publish)
    /// is exercised end-to-end by the transport Handler<Message> arms in
    /// `wt_chat_session.rs` and `ws_chat_session.rs`. Constructing a full
    /// `SessionLogic` here would require a live NATS client and chat-server
    /// addr, so this test verifies the tracker boundary the transports call
    /// through. Greppable by the bead id.
    #[test]
    fn test_p5_7_p3videobase_burst_fires_congestion() {
        let mut tracker = CongestionTracker::new();
        // First 4 drops in a tight burst stay below the 5/1s threshold.
        for i in 0..4 {
            assert!(
                !tracker.record_drop_with_class(Class::P3VideoBase),
                "p5-7: drop #{} should be below threshold",
                i + 1
            );
        }
        // 5th drop within the 1s window crosses the threshold — this is the
        // boolean `on_outbound_drop_class` consumes to fire CONGESTION.
        assert!(
            tracker.record_drop_with_class(Class::P3VideoBase),
            "p5-7: 5th P3VideoBase drop within 1s must fire CONGESTION"
        );
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
            &InboundAction::Forward(Arc::new(vec![]), PacketKind::Data)
        ));
        assert!(SessionLogic::should_activate_on_action(
            &InboundAction::Processed
        ));
        assert!(SessionLogic::should_activate_on_action(
            &InboundAction::KeepAlive
        ));
    }

    // ---- vc-ud6o: off-actor publish payload preparation -----------------

    fn wrapper_bytes(session_id: u64) -> Vec<u8> {
        let w = PacketWrapper {
            packet_type: PacketType::MEDIA.into(),
            session_id,
            data: vec![1, 2, 3, 4],
            ..Default::default()
        };
        w.write_to_bytes().expect("serialize wrapper")
    }

    /// session_id == 0 must be rewritten to the session's id before publish,
    /// matching the former on-actor handler.
    #[test]
    fn test_prepare_publish_payload_rewrites_zero_session_id() {
        let raw = wrapper_bytes(0);
        let out = prepare_publish_payload(4242, Arc::new(raw));
        let parsed = PacketWrapper::parse_from_bytes(&out).expect("parse out");
        assert_eq!(
            parsed.session_id, 4242,
            "session_id==0 must be stamped with the publishing session id"
        );
    }

    /// A wrapper that already carries a session_id is published unchanged and
    /// zero-copy (the bytes are byte-identical to the input buffer).
    #[test]
    fn test_prepare_publish_payload_preserves_set_session_id_zero_copy() {
        let raw = wrapper_bytes(9999);
        let arc = Arc::new(raw.clone());
        let out = prepare_publish_payload(4242, arc);
        assert_eq!(
            out.as_ref(),
            raw.as_slice(),
            "already-set session_id frames must pass through unchanged"
        );
        let parsed = PacketWrapper::parse_from_bytes(&out).expect("parse out");
        assert_eq!(parsed.session_id, 9999, "existing session_id must be kept");
    }

    /// Opaque / unparseable bytes (e.g. the encrypted-media common case) are
    /// published verbatim over the original Arc buffer (no rewrite, no copy).
    #[test]
    fn test_prepare_publish_payload_passes_through_unparseable_bytes() {
        let raw = vec![0xff, 0xff, 0xff, 0xff];
        let out = prepare_publish_payload(4242, Arc::new(raw.clone()));
        assert_eq!(
            out.as_ref(),
            raw.as_slice(),
            "unparseable opaque bytes must be published verbatim"
        );
    }
}
