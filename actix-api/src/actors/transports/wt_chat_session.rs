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
use crate::actors::session_logic::{
    InboundAction, SessionLogic, SharedConnectionStates, TeardownReason,
};
use crate::constants::CLIENT_TIMEOUT;
use crate::messages::server::{ActivateConnection, Packet};
use crate::messages::session::Message;
use crate::server_diagnostics::TrackerSender;
use crate::session_manager::SessionManager;
use crate::sfu::priority_queue::{classify_outbound, Class, PrioritySender, SendOutcome};
use crate::webtransport::bridge::AcceptInboundFlag;
use actix::{
    fut, Actor, ActorContext, ActorFutureExt, Addr, AsyncContext, Context, ContextFutureSpawner,
    Handler, Message as ActixMessage, Running, WrapFuture,
};
use bytes::Bytes;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tracing::{error, info, trace, warn};

/// vc-n9o: how long the actor waits after enqueuing `StopSession` on a
/// redirect before force-stopping itself, in case the mailbox is still being
/// fed faster than it drains (deadline-escalation backstop). Bounded well
/// under the 500ms reconnect-responsiveness budget while leaving ample time
/// for the queued REDIRECT `Message` to drain and the writer to flush it.
const REDIRECT_TEARDOWN_DEADLINE: Duration = Duration::from_millis(300);

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

    /// vc-n9o: shared flag telling the bridge readers whether to keep
    /// forwarding inbound client frames. Cleared on a redirect teardown so
    /// the readers stop feeding the mailbox and the queued `StopSession`
    /// item can actually run (breaking the mailbox-starvation hang).
    accept_inbound: AcceptInboundFlag,

    /// vc-n9o: reason recorded for the eventual teardown so `stopping` can
    /// emit `sfu_session_teardown_total` exactly once with the right label.
    teardown_reason: TeardownReason,
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
        accept_inbound: AcceptInboundFlag,
        connection_states: SharedConnectionStates,
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
            connection_states,
        );

        WtChatSession {
            logic,
            heartbeat: actix::clock::Instant::now(),
            outbound_tx,
            activated: false,
            accept_inbound,
            teardown_reason: TeardownReason::Normal,
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
                    act.teardown_reason = TeardownReason::Error;
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
            .then(|res, act, ctx| {
                if let Err(err) = res {
                    error!("Failed to connect to ChatServer: {:?}", err);
                    act.teardown_reason = TeardownReason::Error;
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
        // vc-n9o: emit the teardown counter exactly once, on whatever stop
        // path fired. `teardown_reason` defaults to Normal and is set to
        // Redirect / Error at the specific decision sites below.
        crate::metrics::SFU_SESSION_TEARDOWN_TOTAL
            .with_label_values(&[self.teardown_reason.label()])
            .inc();
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
                self.teardown_reason = TeardownReason::Error;
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
                        self.teardown_reason = TeardownReason::Error;
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
        // vc-ud6o E3: route via SessionLogic. High-volume media publishes to
        // NATS directly from this task (off the single ChatServer mailbox);
        // only the rare control packets (SUBSCRIPTION_UPDATE, KEYFRAME_REQUEST)
        // still go through the actor.
        self.logic.forward_packet(msg);
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
                if let Some(reason) = act.logic.handle_join_room_result(response) {
                    // vc-883: queue StopSession instead of calling ctx.stop()
                    // directly. The JoinRoom rejection path (e.g.
                    // ADMISSION_DECISION{REDIRECT} on a non-owner pod, see
                    // chat_server.rs ~line 1538) `try_send`s a server-
                    // originated `Message` into THIS actor's mailbox BEFORE
                    // returning Err. If we call `ctx.stop()` here directly,
                    // the actix-0.13.5 poll loop sets the STOPPING flag and
                    // `mailbox.poll`'s `while !ctx.waiting()` guard sees
                    // STOPPING via `ContextParts::waiting`, so the queued
                    // Message NEVER runs — the REDIRECT bytes never reach
                    // `outbound_tx`, the bridge writer never writes them,
                    // and the client sees the QUIC session close with no
                    // packet in flight. Bots in `--orchestrate` mode then
                    // observed `accept_uni` error WITHOUT a REDIRECT to
                    // follow, defeating the entire vc-kni reconnect loop.
                    //
                    // `ctx.notify(StopSession)` instead pushes the stop
                    // request onto the actor's `items` list (see
                    // actix-0.13.5 `AsyncContext::notify` →
                    // `ContextParts::spawn`). The poll loop processes the
                    // mailbox BEFORE the items list, so the queued REDIRECT
                    // `Message` handler runs first (placing bytes in
                    // `outbound_tx`), the bridge writer drains them onto
                    // QUIC, and only then does the `StopSession` item run
                    // and call `ctx.stop()`. The `outbound_tx`
                    // `PrioritySender` stays alive across that window
                    // because it's a field on the actor, dropped only when
                    // the actor is fully stopped — by which time the writer
                    // has already pulled the REDIRECT off the queue.
                    //
                    // vc-n9o: under sustained inbound, the bridge readers keep
                    // `try_send`ing `WtInbound` into this mailbox, so it is
                    // never empty and the `StopSession` *item* (which actix
                    // processes only AFTER the mailbox drains) is starved — the
                    // actor never stops, `outbound_tx` never drops, the writer
                    // never sees recv→None, and the QUIC session never closes.
                    // The redirected sender then hangs on the non-owner pod and
                    // never publishes to NATS (the multi-pod 0-decode root
                    // cause).
                    //
                    // Breaking the starvation: clear `accept_inbound` so the
                    // bridge readers immediately STOP forwarding inbound
                    // frames. The mailbox then drains (the queued REDIRECT
                    // `Message` runs, placing bytes in `outbound_tx`) and the
                    // `StopSession` item finally runs. We clear the flag BEFORE
                    // `notify(StopSession)` so no further `WtInbound` is
                    // enqueued behind the stop. This does NOT touch the
                    // outbound/writer path, so the REDIRECT still flushes first
                    // (vc-883) over its reliable UniStream (vc-xnp) within the
                    // writer's grace (vc-s9e).
                    //
                    // vc-n9o (metric correctness): the teardown reason comes
                    // from the typed `JoinRoom` decline so it lines up with the
                    // decision counter — a hard-cap *reject* is labeled `error`,
                    // NOT `redirect`, so it cannot inflate the redirect teardown
                    // bucket and mask a real redirect-vs-teardown gap. The
                    // starvation-breaking flag-clear is applied to EVERY decline
                    // (redirect and reject both `try_send` a control packet that
                    // must drain before stop, and a media-sending client can hit
                    // either), so it is correct and harmless for all of them.
                    act.teardown_reason = reason;
                    act.accept_inbound.store(false, Ordering::Release);
                    ctx.notify(StopSession);
                    // Deadline-escalation backstop: if `StopSession` still
                    // hasn't run within REDIRECT_TEARDOWN_DEADLINE (e.g. a
                    // burst of inbound was already queued ahead of the items
                    // list before the flag took effect), force the stop so the
                    // teardown chain cannot stall.
                    ctx.run_later(REDIRECT_TEARDOWN_DEADLINE, |act, ctx| {
                        warn!(
                            "Redirect teardown deadline elapsed for session {} — \
                             forcing stop (mailbox-starvation backstop, vc-n9o)",
                            act.logic.id
                        );
                        ctx.stop();
                    });
                }
                fut::ready(())
            })
            .wait(ctx);
    }
}

#[cfg(test)]
mod tests {
    //! vc-883 regression tests for the actor lifecycle on a JoinRoom
    //! rejection that pre-queues a "last gasp" `Message` (the
    //! `ADMISSION_DECISION{REDIRECT}` payload in the real path).
    //!
    //! We do NOT spin up a full `WtChatSession` here because that requires
    //! a `ChatServer` actor, NATS client, session manager, and bridge
    //! writer — roughly half of `actix-api`. Instead these tests exercise
    //! the actix-0.13.5 mailbox semantics on a minimal pair of actors that
    //! mirror the exact pattern used by [`WtChatSession::join_room`]:
    //!
    //!   1. A `Server` actor whose `JoinRoom` handler `try_send`s an
    //!      `OutboundMessage` into the `Session` actor's mailbox, then
    //!      returns `Err(())`.
    //!   2. A `Session` actor whose `started` runs `addr.send(JoinRoom)`
    //!      under `.wait(ctx)`, then in the `.then` closure either calls
    //!      `ctx.stop()` (the broken pre-vc-883 behaviour) or
    //!      `ctx.notify(StopSession)` (the vc-883 fix).
    //!
    //! The `OutboundMessage` handler bumps a shared counter. After the
    //! `Session` actor terminates we assert the counter:
    //!
    //!   * Under the broken path the counter is `0`: the mailbox-poll
    //!     guard `while !ctx.waiting()` short-circuits once STOPPING is
    //!     set, so the queued message is dropped. This is exactly the
    //!     failure that made the bot's inbound consumer never see a
    //!     REDIRECT.
    //!   * Under the fixed path the counter is `1`: the mailbox drains
    //!     the queued message before the `StopSession` item runs.
    //!
    //! No protobuf, no PrioritySender, no QUIC — just the mailbox +
    //! items-list ordering rule that the real fix depends on. If actix
    //! changes its poll semantics in a future bump, these tests are the
    //! canary.

    use super::StopSession;
    use actix::{
        fut, Actor, ActorContext, ActorFutureExt, Addr, AsyncContext, Context,
        ContextFutureSpawner, Handler, Message as ActixMessage, MessageResult, WrapFuture,
    };
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    /// Marker message the server "leaks" into the session mailbox right
    /// before rejecting `JoinRoom`. Stand-in for the real REDIRECT
    /// `Message` (which carries `Bytes`); we only care about delivery.
    #[derive(ActixMessage)]
    #[rtype(result = "()")]
    struct OutboundMessage;

    /// Server-to-session JoinRoom request. The server's handler queues an
    /// `OutboundMessage` into the session mailbox BEFORE returning Err —
    /// mirroring `chat_server::JoinRoom` ~line 1541 calling
    /// `recipient.try_send(Message { msg: ... })` before
    /// `return MessageResult(Err(...))`.
    #[derive(ActixMessage)]
    #[rtype(result = "Result<(), ()>")]
    struct JoinRoom {
        session_recipient: actix::Recipient<OutboundMessage>,
    }

    struct Server;
    impl Actor for Server {
        type Context = Context<Self>;
    }
    impl Handler<JoinRoom> for Server {
        type Result = MessageResult<JoinRoom>;
        fn handle(&mut self, msg: JoinRoom, _ctx: &mut Context<Self>) -> Self::Result {
            // 1. Queue the "REDIRECT" into the session mailbox.
            let _ = msg.session_recipient.try_send(OutboundMessage);
            // 2. Reject the join. The session's `.then` closure decides
            //    how to stop.
            MessageResult(Err(()))
        }
    }

    /// Selects which stop pattern the session uses on JoinRoom Err.
    #[derive(Clone, Copy)]
    enum StopMode {
        /// Pre-vc-883 (broken): call `ctx.stop()` directly. The STOPPING
        /// flag blocks the mailbox-poll from draining the queued
        /// `OutboundMessage`.
        Direct,
        /// vc-883 fix: `ctx.notify(StopSession)`. The mailbox drains the
        /// queued `OutboundMessage` before the items-list runs the stop.
        ViaNotify,
    }

    struct Session {
        server: Addr<Server>,
        mode: StopMode,
        delivered: Arc<AtomicU32>,
    }
    impl Actor for Session {
        type Context = Context<Self>;
        fn started(&mut self, ctx: &mut Self::Context) {
            // Mirror `WtChatSession::join_room`: send JoinRoom and `.wait`
            // on the response. While waiting, the mailbox is blocked. The
            // server's handler queues an `OutboundMessage` into our
            // mailbox while we're parked here.
            let recipient = ctx.address().recipient::<OutboundMessage>();
            let mode = self.mode;
            self.server
                .send(JoinRoom {
                    session_recipient: recipient,
                })
                .into_actor(self)
                .then(move |res, _act, ctx| {
                    match res {
                        Ok(Err(())) => match mode {
                            StopMode::Direct => ctx.stop(),
                            StopMode::ViaNotify => ctx.notify(StopSession),
                        },
                        // Ok(Ok(())) — irrelevant for these tests; the
                        // server always returns Err.
                        // Err(MailboxError) — also stop, but the test
                        // path always succeeds at the mailbox layer.
                        _ => ctx.stop(),
                    }
                    fut::ready(())
                })
                .wait(ctx);
        }
    }
    impl Handler<OutboundMessage> for Session {
        type Result = ();
        fn handle(&mut self, _msg: OutboundMessage, _ctx: &mut Self::Context) -> Self::Result {
            // This is the handler that, in the real path, would push the
            // REDIRECT bytes into `outbound_tx` for the bridge writer to
            // flush onto the QUIC stream before the actor terminates.
            self.delivered.fetch_add(1, Ordering::SeqCst);
        }
    }
    impl Handler<StopSession> for Session {
        type Result = ();
        fn handle(&mut self, _msg: StopSession, ctx: &mut Self::Context) -> Self::Result {
            ctx.stop();
        }
    }

    async fn run_with(mode: StopMode) -> u32 {
        let counter = Arc::new(AtomicU32::new(0));
        let server = Server.start();
        let session = Session {
            server: server.clone(),
            mode,
            delivered: counter.clone(),
        }
        .start();
        // Wait for the session to fully terminate. `started` schedules
        // both the JoinRoom wait-future and either the stop or the
        // notify; once those resolve and the actor stops, its address
        // becomes disconnected. We poll with a short sleep budget — the
        // whole interaction is local-actor mailbox roundtrips, so even
        // 100ms is generous.
        for _ in 0..50 {
            if !session.connected() {
                break;
            }
            actix_rt::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        assert!(
            !session.connected(),
            "session actor did not terminate within the test budget"
        );
        counter.load(Ordering::SeqCst)
    }

    /// REGRESSION GUARD for vc-883.
    ///
    /// Documents the broken pre-vc-883 behaviour: calling `ctx.stop()`
    /// directly inside the JoinRoom-Err closure means a `Message` that
    /// the server `try_send`ed into the session mailbox BEFORE returning
    /// Err is DROPPED — the mailbox poll loop's `while !ctx.waiting()`
    /// guard sees the STOPPING flag and short-circuits without
    /// dispatching the queued message.
    ///
    /// This is the exact mechanism that prevented the bot's inbound
    /// consumer from ever seeing an `ADMISSION_DECISION{REDIRECT}` in
    /// `--orchestrate` mode — the bytes never made it from the actor
    /// mailbox to the PrioritySender to the bridge writer to QUIC. If
    /// this test ever starts FAILING (i.e. the counter becomes `1`),
    /// either actix changed semantics or someone reverted the fix; in
    /// the latter case the `ViaNotify` test below should also be
    /// re-examined.
    #[actix_rt::test]
    async fn ctx_stop_drops_messages_already_queued_in_the_mailbox() {
        let delivered = run_with(StopMode::Direct).await;
        assert_eq!(
            delivered, 0,
            "ctx.stop() directly after a queued Message MUST drop the message — \
             this is the actix-0.13.5 mailbox poll guard semantics that vc-883 hit"
        );
    }

    /// REGRESSION GUARD for vc-883: the fix.
    ///
    /// `ctx.notify(StopSession)` pushes the stop request onto the items
    /// list, NOT the STOPPING flag. The mailbox is polled BEFORE items
    /// in the actor poll loop, so the queued `OutboundMessage` runs
    /// first (in the real path: pushes REDIRECT bytes into `outbound_tx`
    /// so the bridge writer can flush them to QUIC), then `StopSession`
    /// runs and finally calls `ctx.stop()`.
    ///
    /// If this test FAILS, the bot will silently lose REDIRECTs again
    /// and `--orchestrate` runs will fall back to asymmetric shard load.
    #[actix_rt::test]
    async fn ctx_notify_stop_drains_queued_messages_before_terminating() {
        let delivered = run_with(StopMode::ViaNotify).await;
        assert_eq!(
            delivered, 1,
            "ctx.notify(StopSession) MUST allow the queued mailbox Message \
             to run before the actor terminates — this is the vc-883 fix"
        );
    }

    // =====================================================================
    // vc-n9o regression test: redirect teardown under mailbox starvation.
    //
    // vc-883 proved the queued REDIRECT drains when the mailbox can empty.
    // But under sustained inbound, the bridge readers keep `try_send`ing
    // `WtInbound` into the mailbox, so it NEVER empties and the
    // `StopSession` *item* (processed only after the mailbox drains) is
    // starved — the actor never stops, `outbound_tx` never drops, the QUIC
    // session never closes, and the redirected sender hangs (the multi-pod
    // 0-decode root cause). The fix is the shared `accept_inbound` flag:
    // clearing it on the redirect path makes the readers stop feeding the
    // mailbox so it can drain to the stop.
    //
    // This test reproduces the starvation with a "flooder" tokio task that
    // keeps `try_send`ing inbound messages into the session mailbox while
    // `accept_inbound` is true (mirroring the bridge readers). It asserts:
    //   1. The actor terminates despite the flood (starvation broken).
    //   2. The queued REDIRECT was delivered BEFORE teardown (vc-883 kept).
    //   3. An `outbound_tx` stand-in held as an actor field is dropped when
    //      the actor stops (so the real writer would see recv→None and
    //      `wait_for_disconnect` would return).
    // =====================================================================

    use std::sync::atomic::AtomicBool;

    /// A flood inbound message (stand-in for `WtInbound` under 30fps load).
    #[derive(ActixMessage)]
    #[rtype(result = "()")]
    struct FloodInbound;

    /// Like `Server`, but its `JoinRoom` handler enqueues the REDIRECT and
    /// then returns Err after a brief async delay — keeping the session
    /// parked in `.wait` long enough for the flooder to pack the mailbox
    /// BEFORE the redirect decision (the realistic ordering: client media is
    /// already flowing when the server decides to redirect).
    struct SlowServer;
    impl Actor for SlowServer {
        type Context = Context<Self>;
    }
    impl Handler<JoinRoom> for SlowServer {
        type Result = actix::ResponseActFuture<Self, Result<(), ()>>;
        fn handle(&mut self, msg: JoinRoom, _ctx: &mut Context<Self>) -> Self::Result {
            // 1. Enqueue the REDIRECT into the session mailbox first.
            let _ = msg.session_recipient.try_send(OutboundMessage);
            // 2. Delay the Err so the flood builds up while the session is
            //    still parked on the JoinRoom response.
            Box::pin(
                async {
                    actix_rt::time::sleep(std::time::Duration::from_millis(40)).await;
                }
                .into_actor(self)
                .map(|_, _, _| Err(())),
            )
        }
    }

    struct StarvedSession {
        server: Addr<SlowServer>,
        delivered: Arc<AtomicU32>,
        /// Set true the instant the actor stops (via `stopping`), so the
        /// test can confirm the REDIRECT (delivered=1) happened first.
        stopped: Arc<AtomicBool>,
        /// vc-n9o flag shared with the flooder task; cleared on redirect.
        accept_inbound: Arc<AtomicBool>,
        /// Stand-in for the actor's `outbound_tx` field. Its `Arc` strong
        /// count drops when the actor (and this field) is dropped on stop —
        /// mirroring the real `PrioritySender` drop that ends the writer.
        /// Never read: its drop-on-actor-stop is the whole point.
        #[allow(dead_code)]
        outbound_tx: Arc<()>,
    }
    impl Actor for StarvedSession {
        type Context = Context<Self>;
        fn started(&mut self, ctx: &mut Self::Context) {
            // Generous mailbox so the flood (and the pre-queued REDIRECT) all
            // fit; the real bridge mailbox is likewise not the bottleneck —
            // the bug is item-list starvation, not mailbox-full backpressure.
            ctx.set_mailbox_capacity(4096);
            let recipient = ctx.address().recipient::<OutboundMessage>();
            self.server
                .send(JoinRoom {
                    session_recipient: recipient,
                })
                .into_actor(self)
                .then(move |res, act, ctx| {
                    if let Ok(Err(())) = res {
                        // Exact mirror of `WtChatSession::join_room` redirect
                        // path: clear the flag (stop the flood) BEFORE
                        // notifying StopSession, so the mailbox can drain the
                        // queued REDIRECT and then run the stop item.
                        act.accept_inbound.store(false, Ordering::Release);
                        ctx.notify(StopSession);
                    } else {
                        ctx.stop();
                    }
                    fut::ready(())
                })
                .wait(ctx);
        }
        fn stopping(&mut self, _: &mut Self::Context) -> actix::Running {
            self.stopped.store(true, Ordering::SeqCst);
            actix::Running::Stop
        }
    }
    impl Handler<OutboundMessage> for StarvedSession {
        type Result = ();
        fn handle(&mut self, _msg: OutboundMessage, _ctx: &mut Self::Context) -> Self::Result {
            // REDIRECT delivery MUST be observed before `stopped` flips.
            assert!(
                !self.stopped.load(Ordering::SeqCst),
                "REDIRECT delivered AFTER teardown — vc-883 ordering violated"
            );
            self.delivered.fetch_add(1, Ordering::SeqCst);
        }
    }
    impl Handler<StopSession> for StarvedSession {
        type Result = ();
        fn handle(&mut self, _msg: StopSession, ctx: &mut Self::Context) -> Self::Result {
            ctx.stop();
        }
    }
    impl Handler<FloodInbound> for StarvedSession {
        type Result = ();
        fn handle(&mut self, _msg: FloodInbound, _ctx: &mut Self::Context) -> Self::Result {
            // Simulate per-frame inbound processing cost. The point is that
            // while these keep arriving, the `StopSession` item is starved
            // unless the flooder is stopped by the flag.
        }
    }

    /// vc-n9o: under sustained inbound, the redirect path must still tear the
    /// session down, deliver the REDIRECT first, and drop `outbound_tx`.
    #[actix_rt::test]
    async fn redirect_tears_down_under_mailbox_starvation_vc_n9o() {
        let delivered = Arc::new(AtomicU32::new(0));
        let stopped = Arc::new(AtomicBool::new(false));
        let accept_inbound = Arc::new(AtomicBool::new(true));
        let outbound_tx = Arc::new(());
        let outbound_observer = outbound_tx.clone();

        let server = SlowServer.start();
        let session = StarvedSession {
            server,
            delivered: delivered.clone(),
            stopped: stopped.clone(),
            accept_inbound: accept_inbound.clone(),
            outbound_tx,
        }
        .start();

        // Flooder: keep the mailbox non-empty (like the bridge readers under
        // 30fps) until the actor clears `accept_inbound`. Without the flag,
        // this flood would starve `StopSession` forever.
        let flood_recipient = session.clone().recipient::<FloodInbound>();
        let flood_flag = accept_inbound.clone();
        let flooder = tokio::spawn(async move {
            while flood_flag.load(Ordering::Acquire) {
                if flood_recipient.try_send(FloodInbound).is_err() {
                    break; // mailbox closed → actor stopped.
                }
                // Tiny yield so the actor gets scheduling slices; the mailbox
                // still stays effectively non-empty.
                actix_rt::time::sleep(std::time::Duration::from_micros(50)).await;
            }
        });

        // The actor must terminate despite the flood.
        let mut terminated = false;
        for _ in 0..250 {
            if !session.connected() {
                terminated = true;
                break;
            }
            actix_rt::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        let _ = flooder.await;
        // Brief settle so the actor's drop (and thus the `outbound_tx` field
        // drop) completes after the address reports disconnected.
        actix_rt::time::sleep(std::time::Duration::from_millis(20)).await;

        assert!(
            terminated,
            "actor did NOT terminate under mailbox starvation — the \
             accept_inbound flag failed to break the StopSession starvation \
             (vc-n9o regression: redirected sender would hang on the \
             non-owner pod)"
        );
        assert_eq!(
            delivered.load(Ordering::SeqCst),
            1,
            "the queued REDIRECT must be delivered exactly once, before \
             teardown (vc-883 preserved under starvation)"
        );
        assert!(
            stopped.load(Ordering::SeqCst),
            "actor stopping hook must have run"
        );
        // The actor (and its `outbound_tx` field) is dropped on stop, so the
        // observer's clone is now the only strong ref — mirroring the real
        // `PrioritySender` drop that makes the writer's recv return None and
        // `wait_for_disconnect` return.
        assert_eq!(
            Arc::strong_count(&outbound_observer),
            1,
            "outbound_tx stand-in was NOT dropped on actor stop — the real \
             writer would never see recv→None and the QUIC session would \
             never close (vc-n9o)"
        );
    }
}
