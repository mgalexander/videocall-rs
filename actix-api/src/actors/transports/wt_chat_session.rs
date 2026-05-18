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

//! WebTransport Chat Session Actor
//!
//! This is a thin transport adapter that delegates all business logic
//! to `SessionLogic`. It handles WebTransport-specific I/O via channels.

use crate::actors::chat_server::ChatServer;
use crate::actors::packet_handler::parse_and_inspect;
use crate::actors::session_logic::{InboundAction, SessionLogic};
use crate::constants::CLIENT_TIMEOUT;
use crate::messages::server::{ActivateConnection, Packet};
use crate::messages::session::Message;
use crate::server_diagnostics::TrackerSender;
use crate::session_manager::SessionManager;
use crate::sfu::priority_queue::{classify_outbound, Class, PrioritySender, SendOutcome};
use actix::{
    fut, Actor, ActorContext, ActorFutureExt, Addr, AsyncContext, Context, ContextFutureSpawner,
    Handler, Message as ActixMessage, Running, WrapFuture,
};
use bytes::Bytes;
use std::time::Duration;
use tracing::{error, info, trace, warn};

pub use crate::actors::session_logic::{RoomId, SessionId, UserId};

/// Heartbeat interval for WebTransport sessions
const WT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

/// Keep-alive ping data (WebTransport-specific)
const KEEP_ALIVE_PING: &[u8] = b"ping";

/// Source of inbound data
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WtInboundSource {
    UniStream,
    Datagram,
}

/// Inbound message from WebTransport session
#[derive(ActixMessage)]
#[rtype(result = "()")]
pub struct WtInbound {
    pub data: Bytes,
    pub source: WtInboundSource,
}

/// Signal to stop the session (sent when I/O tasks end)
#[derive(ActixMessage)]
#[rtype(result = "()")]
pub struct StopSession;

/// WebTransport Chat Session Actor
///
/// A thin transport adapter that delegates business logic to `SessionLogic`.
/// Handles WebTransport-specific I/O via channels.
pub struct WtChatSession {
    /// Shared session logic (business logic)
    logic: SessionLogic,

    /// Heartbeat tracking (transport-specific timing)
    heartbeat: actix::clock::Instant,

    /// Bandwidth-aware priority sender for outbound packets (p5-4).
    ///
    /// Replaces the legacy `mpsc::Sender<WtOutbound>(256)` with a five-class
    /// priority queue (P0 Control → P4 Enhancement) that applies per-class
    /// drop policies. The bridge writer task drains via [`PriorityReceiver`]
    /// with strict-priority + 8-packet fairness quantum.
    outbound_tx: PrioritySender,

    /// Track if ActivateConnection has been sent
    activated: bool,
}

impl WtChatSession {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        addr: Addr<ChatServer>,
        room: String,
        user_id: String,
        display_name: String,
        outbound_tx: PrioritySender,
        nats_client: async_nats::client::Client,
        tracker_sender: TrackerSender,
        session_manager: SessionManager,
        observer: bool,
    ) -> Self {
        let logic = SessionLogic::new(
            addr,
            room,
            user_id,
            display_name,
            nats_client,
            tracker_sender,
            session_manager,
            observer,
        );

        WtChatSession {
            logic,
            heartbeat: actix::clock::Instant::now(),
            outbound_tx,
            activated: false,
        }
    }

    /// Classify `bytes` (a serialized `PacketWrapper`) into a priority
    /// [`Class`], parsing the inner `MediaPacket` for routing-header awareness
    /// when the wrapper is MEDIA. Returns [`Class::P3VideoBase`] as the
    /// fallback if the wrapper itself fails to parse (matches
    /// [`classify_outbound`]'s unknown-type fallback).
    fn classify_bytes(bytes: &[u8]) -> Class {
        match parse_and_inspect(bytes) {
            Some(parsed) => {
                let media_type = parsed
                    .media_packet
                    .as_ref()
                    .map(|mp| mp.media_type.enum_value_or_default());
                classify_outbound(&parsed.wrapper, media_type, parsed.routing_header())
            }
            None => Class::P3VideoBase,
        }
    }

    /// Enqueue a server-originated control packet (SESSION_ASSIGNED,
    /// MEETING_STARTED, MEETING_ENDED) onto the outbound priority queue.
    ///
    /// Returns `false` if the P0 control class queue is full
    /// ([`SendOutcome::Refused`]), in which case the caller should treat the
    /// session as failed and stop. These packets are all P0Control by
    /// construction, but we classify uniformly to keep one code path.
    fn send(&self, data: Vec<u8>) -> bool {
        let bytes = Bytes::from(data);
        let class = Self::classify_bytes(&bytes);
        match self.outbound_tx.send(class, bytes) {
            SendOutcome::Sent => true,
            SendOutcome::Dropped(class, reason) => {
                // Should not happen for control packets (all classify to
                // P0Control which uses NeverDrop), but log if it does so we
                // surface unexpected classification regressions.
                warn!(
                    "Outbound control packet dropped on session {}: {:?} ({})",
                    self.logic.id, class, reason
                );
                true
            }
            SendOutcome::Refused(_) => {
                error!(
                    "P0Control class queue full for session {} on control send — \
                     terminating session per PLAN.md",
                    self.logic.id
                );
                false
            }
        }
    }

    /// Start heartbeat check (WebTransport-specific timing).
    ///
    /// With the legacy `mpsc::Sender::is_closed` gone, dead-connection
    /// detection now flows through the bridge writer task: when QUIC I/O
    /// fails the writer ends, `wait_for_disconnect` returns, and
    /// `StopSession` is delivered to this actor. The heartbeat watchdog
    /// remains as the client-inactivity backstop.
    fn start_heartbeat(&self, ctx: &mut Context<Self>) {
        ctx.run_interval(WT_HEARTBEAT_INTERVAL, |act, ctx| {
            if actix::clock::Instant::now().duration_since(act.heartbeat) > CLIENT_TIMEOUT {
                warn!(
                    "WebTransport client heartbeat failed, disconnecting session {}",
                    act.logic.id
                );
                ctx.stop();
            }
        });
    }
}

// =============================================================================
// Actor Implementation
// =============================================================================

impl Actor for WtChatSession {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // Track connection start
        self.logic.track_connection_start("webtransport");

        // Start session via SessionManager
        let session_manager = self.logic.session_manager.clone();
        let room = self.logic.room.clone();
        let user_id = self.logic.user_id.clone();
        let session_id = self.logic.id;

        ctx.wait(
            async move {
                session_manager
                    .start_session(&room, &user_id, session_id)
                    .await
            }
            .into_actor(self)
            .map(|result, act, ctx| match result {
                Ok(result) => {
                    act.send(act.logic.build_session_assigned());
                    let bytes = act
                        .logic
                        .build_meeting_started(result.start_time_ms, &result.creator_id);
                    act.send(bytes);
                }
                Err(e) => {
                    error!("Failed to start session: {}", e);
                    let bytes = act
                        .logic
                        .build_meeting_ended(&format!("Session rejected: {e}"));
                    act.send(bytes);
                    ctx.stop();
                }
            }),
        );

        // Register with ChatServer
        let addr = ctx.address();
        self.logic
            .addr
            .send(self.logic.create_connect_message(addr.recipient()))
            .into_actor(self)
            .then(|res, _act, ctx| {
                if let Err(err) = res {
                    error!("Failed to connect to ChatServer: {:?}", err);
                    ctx.stop();
                }
                fut::ready(())
            })
            .wait(ctx);

        // Join room
        self.join_room(ctx);

        // Start heartbeat AFTER all initialization is complete to avoid
        // premature timeout if Connect/JoinRoom are slow under load.
        self.start_heartbeat(ctx);
    }

    fn stopping(&mut self, _: &mut Self::Context) -> Running {
        self.logic.on_stopping();
        Running::Stop
    }
}

// =============================================================================
// Message Handlers
// =============================================================================

/// Handle outbound messages from ChatServer.
///
/// Routes packets into the per-session [`PrioritySender`] (p5-4 swap).
/// `parse_and_inspect` produces the outer wrapper and (for MEDIA) the inner
/// `MediaPacket` in a single pass so `classify_outbound` can read the routing
/// header without a second parse. Per-class drops feed
/// [`SessionLogic::on_outbound_drop_class`] (p5-7), which routes through the
/// class-aware [`CongestionTracker`] to emit CONGESTION on threshold trip.
///
/// Note: `msg.session` is the **receiver's** session ID (set by
/// `chat_server::handle_msg`), NOT the sender's. The sender's session ID
/// lives inside the serialized `PacketWrapper.session_id` field.
impl Handler<Message> for WtChatSession {
    type Result = ();

    fn handle(&mut self, msg: Message, ctx: &mut Self::Context) -> Self::Result {
        let bytes = self.logic.handle_outbound(&msg);

        let parsed = parse_and_inspect(&msg.msg);
        let sender_session_id = parsed.as_ref().map(|p| p.wrapper.session_id).unwrap_or(0);
        let class = match parsed.as_ref() {
            Some(p) => {
                let media_type = p
                    .media_packet
                    .as_ref()
                    .map(|mp| mp.media_type.enum_value_or_default());
                classify_outbound(&p.wrapper, media_type, p.routing_header())
            }
            None => Class::P3VideoBase,
        };

        match self.outbound_tx.send(class, bytes) {
            SendOutcome::Sent => {}
            SendOutcome::Dropped(dropped_class, _reason) => {
                // Priority queue dropped a packet under its per-class
                // policy. Route the drop through the class-aware
                // CongestionTracker path (p5-7) so a class-specific drop
                // fires a class-specific CONGESTION signal back to the
                // sender. `sender_session_id == 0` means we couldn't parse
                // the wrapper (e.g. a server-originated frame) — skip
                // attribution in that case.
                if sender_session_id != 0 {
                    self.logic
                        .on_outbound_drop_class(sender_session_id, dropped_class);
                }
            }
            SendOutcome::Refused(_) => {
                error!(
                    "P0Control class queue full for session {} — terminating session per PLAN.md",
                    self.logic.id
                );
                ctx.stop();
            }
        }
    }
}

/// Handle inbound data from WebTransport session
impl Handler<WtInbound> for WtChatSession {
    type Result = ();

    fn handle(&mut self, msg: WtInbound, ctx: &mut Self::Context) -> Self::Result {
        // Update heartbeat
        self.heartbeat = actix::clock::Instant::now();

        // Handle keep-alive ping (WebTransport-specific)
        if msg.source == WtInboundSource::Datagram && msg.data.as_ref() == KEEP_ALIVE_PING {
            trace!("Received keep-alive ping for session {}", self.logic.id);
            return;
        }

        let action = self.logic.handle_inbound(&msg.data);

        if !self.activated && SessionLogic::should_activate_on_action(&action) {
            self.logic.addr.do_send(ActivateConnection {
                session: self.logic.id,
            });
            self.activated = true;
            info!(
                "Session {} activated on first non-RTT packet",
                self.logic.id
            );
        }

        match action {
            InboundAction::Echo(data) => {
                // RTT echoes flow through the priority queue (P0Control by
                // classification). The bridge writer recovers UniStream-vs-
                // Datagram by inspecting the packet; the original inbound
                // source no longer drives the choice (MEDIA RTT → UniStream
                // matches the legacy `send_auto` path for `is_media=true`).
                let bytes = Bytes::from(data.as_ref().clone());
                let class = Self::classify_bytes(&bytes);
                match self.outbound_tx.send(class, bytes) {
                    SendOutcome::Sent => {}
                    SendOutcome::Dropped(_, _) => {
                        // RTT echoes that don't classify as P0Control could
                        // hit a tail-drop policy; the echo is best-effort
                        // and clients re-issue probes regularly.
                    }
                    SendOutcome::Refused(_) => {
                        error!(
                            "P0Control class queue full on RTT echo for session {} — \
                             terminating session",
                            self.logic.id
                        );
                        ctx.stop();
                    }
                }
            }
            InboundAction::Forward(data, kind) => {
                ctx.notify(Packet { data, kind });
            }
            InboundAction::Processed | InboundAction::KeepAlive => {}
        }
    }
}

/// Handle stop signal
impl Handler<StopSession> for WtChatSession {
    type Result = ();

    fn handle(&mut self, _msg: StopSession, ctx: &mut Self::Context) -> Self::Result {
        info!(
            "Received stop signal for WebTransport session {} in room {}",
            self.logic.id, self.logic.room
        );
        ctx.stop();
    }
}

/// Handle outbound packets (forwarding to ChatServer)
impl Handler<Packet> for WtChatSession {
    type Result = ();

    fn handle(&mut self, msg: Packet, _ctx: &mut Self::Context) -> Self::Result {
        trace!(
            "Forwarding packet to ChatServer: session {} room {}",
            self.logic.id,
            self.logic.room
        );
        self.logic
            .addr
            .do_send(self.logic.create_client_message(msg));
    }
}

// =============================================================================
// Helper Methods
// =============================================================================

impl WtChatSession {
    fn join_room(&self, ctx: &mut Context<Self>) {
        let join_room = self.logic.addr.send(self.logic.create_join_room_message());
        let join_room = join_room.into_actor(self);
        join_room
            .then(|response, act, ctx| {
                if act.logic.handle_join_room_result(response) {
                    ctx.stop();
                }
                fut::ready(())
            })
            .wait(ctx);
    }
}
