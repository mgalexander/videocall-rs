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

//! WebSocket Chat Session Actor
//!
//! This is a thin transport adapter that delegates all business logic
//! to `SessionLogic`. It handles WebSocket-specific I/O via `WebsocketContext`.

use crate::actors::chat_server::ChatServer;
use crate::actors::packet_handler::parse_and_inspect;
use crate::actors::session_logic::{InboundAction, SessionLogic, TeardownReason};
use crate::constants::{CLIENT_TIMEOUT, HEARTBEAT_INTERVAL};
use crate::messages::server::{ActivateConnection, Packet};
use crate::messages::session::Message;
use crate::server_diagnostics::TrackerSender;
use crate::session_manager::SessionManager;
use crate::sfu::priority_queue::{
    classify_outbound, Class, PriorityReceiver, PrioritySender, SendOutcome,
};
use actix::ActorFutureExt;
use actix::{
    clock::Instant, fut, Actor, ActorContext, Addr, AsyncContext, ContextFutureSpawner, Handler,
    Message as ActixMessage, Running, StreamHandler, WrapFuture,
};
use actix_web_actors::ws::{self, WebsocketContext};
use bytes::Bytes;
use tracing::{error, info, trace};

pub use crate::actors::session_logic::{RoomId, SessionId, UserId};

/// Internal actor message that carries a pre-classified outbound frame
/// drained from the per-session [`PriorityReceiver`] back to the actor for
/// `ctx.binary` write. Single TCP stream means the priority ordering happens
/// at the SEND side (drain order), not in the wire — the goal is to drop
/// enhancement layers before they hit the kernel buffer, not to reorder bytes
/// already on the wire.
///
/// Worst-case head-of-line on the wire side is exactly 1 frame: while the
/// drainer is parked on `addr.send(SendBinaryFrame).await`, a newly-arrived
/// higher-priority frame jumps the in-queue order but still sits behind the
/// already-committed in-flight `SendBinaryFrame`. By design (per PLAN.md
/// Phase 5) — single-frame HoL is acceptable for the same reason WS can't
/// reorder bytes already in the kernel buffer.
#[derive(ActixMessage)]
#[rtype(result = "()")]
struct SendBinaryFrame(Bytes);

/// WebSocket Chat Session Actor
///
/// A thin transport adapter that delegates business logic to `SessionLogic`.
/// Handles WebSocket-specific I/O via `WebsocketContext`.
pub struct WsChatSession {
    /// Shared session logic (business logic)
    logic: SessionLogic,

    /// Heartbeat tracking (transport-specific timing)
    heartbeat: Instant,

    /// Track if ActivateConnection has been sent
    activated: bool,

    /// Outbound priority sender (5-class bandwidth-aware queue, p5-1/p5-2/p5-3).
    /// `Handler<Message>` classifies each outbound frame and pushes it here.
    outbound_tx: PrioritySender,

    /// Per-session priority receiver, taken by `started()` and moved into the
    /// drainer task. `Option` so the receiver — which is single-consumer — can
    /// be handed off exactly once.
    pending_rx: Option<PriorityReceiver>,

    /// vc-n9o: reason recorded for the eventual teardown so `stopping` can
    /// emit `sfu_session_teardown_total` exactly once with the right label.
    /// WebSocket sessions go through the SAME `ChatServer::JoinRoom` handler
    /// as WebTransport, so a WS redirect increments
    /// `sfu_join_decision_total{outcome=redirect}`; without this field the WS
    /// teardown would never be counted under `reason=redirect`, leaving a
    /// permanent false-positive gap that looks like the vc-n9o regression.
    teardown_reason: TeardownReason,
}

impl WsChatSession {
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

        let (outbound_tx, channels) = PrioritySender::new();
        let pending_rx = Some(PriorityReceiver::new(channels));

        WsChatSession {
            logic,
            heartbeat: Instant::now(),
            activated: false,
            outbound_tx,
            pending_rx,
            teardown_reason: TeardownReason::Normal,
        }
    }

    /// Start heartbeat check (WebSocket-specific: uses ping frames)
    fn start_heartbeat(&self, ctx: &mut WebsocketContext<Self>) {
        ctx.run_interval(HEARTBEAT_INTERVAL, |act, ctx| {
            if Instant::now().duration_since(act.heartbeat) > CLIENT_TIMEOUT {
                error!("WebSocket client heartbeat failed, disconnecting!");
                ctx.stop();
                return;
            }
            ctx.ping(b"");
        });
    }
}

// =============================================================================
// Actor Implementation
// =============================================================================

impl Actor for WsChatSession {
    type Context = WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // Track connection start
        self.logic.track_connection_start("websocket");

        // Spawn the priority-queue drainer BEFORE registering with ChatServer
        // so any inbound `Message` already routed through `Handler<Message>` is
        // guaranteed a consumer. The drainer pulls from `PriorityReceiver` in
        // strict-priority + 8-packet-fairness order (p5-2) and round-trips
        // each `Bytes` back through the actor mailbox so `ctx.binary` can be
        // called inside the actor's context. `addr.send().await` provides
        // natural backpressure: if the actor cannot keep up with writes, the
        // drainer parks rather than overflowing the actor mailbox, leaving
        // the bounded class queues to apply their drop policies upstream.
        if let Some(mut receiver) = self.pending_rx.take() {
            let addr = ctx.address();
            actix_rt::spawn(async move {
                while let Some(bytes) = receiver.recv().await {
                    if addr.send(SendBinaryFrame(bytes)).await.is_err() {
                        // Actor stopped — its mailbox is closed. Exit cleanly.
                        break;
                    }
                }
            });
        }

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
                    ctx.binary(act.logic.build_session_assigned());
                    let bytes = act
                        .logic
                        .build_meeting_started(result.start_time_ms, &result.creator_id);
                    ctx.binary(bytes);
                }
                Err(e) => {
                    error!("Failed to start session: {}", e);
                    let bytes = act
                        .logic
                        .build_meeting_ended(&format!("Session rejected: {e}"));
                    ctx.binary(bytes);
                    ctx.close(Some(ws::CloseReason {
                        code: ws::CloseCode::Policy,
                        description: Some("Session rejected".to_string()),
                    }));
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
        // vc-n9o: emit the teardown counter exactly once, matching the WT
        // path, so a WS redirect (counted as a redirect *decision* in the
        // shared `ChatServer::JoinRoom` handler) has a matching redirect
        // *teardown* and does not leave a permanent false-positive gap.
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
/// Each frame is classified by [`classify_outbound`] (p5-3) and pushed into
/// the per-session [`PrioritySender`]. The bounded class queues drop layers
/// per their policy before the bytes hit the wire — this is the WS-side
/// production swap (p5-5) that makes bandwidth-aware priority queuing run on
/// the WebSocket path, matching p5-4 for the WebTransport path.
impl Handler<Message> for WsChatSession {
    type Result = ();

    fn handle(&mut self, msg: Message, ctx: &mut Self::Context) -> Self::Result {
        let bytes = self.logic.handle_outbound(&msg);

        // Parse the wrapper once for classification AND sender-session-id
        // attribution (the latter is what `on_outbound_drop_class` keys
        // CONGESTION emission off of, matching the WT-side `Handler<Message>`).
        let parsed = parse_and_inspect(bytes.as_ref());
        let class = match &parsed {
            Some(p) => {
                let media_type = p
                    .media_packet
                    .as_ref()
                    .map(|mp| mp.media_type.enum_value_or_default());
                classify_outbound(&p.wrapper, media_type, p.routing_header())
            }
            // Wrapper parse failure: fall back to P3VideoBase, matching
            // `classify_outbound`'s unknown-packet-type branch.
            None => Class::P3VideoBase,
        };
        let sender_session_id = parsed.as_ref().map(|p| p.wrapper.session_id).unwrap_or(0);

        match self.outbound_tx.send(class, bytes) {
            SendOutcome::Sent => {}
            SendOutcome::Dropped(dropped_class, reason) => {
                trace!(
                    "WS outbound drop: session {} class {:?} reason {}",
                    self.logic.id,
                    dropped_class,
                    reason
                );
                // Route the drop through the class-aware CongestionTracker
                // path (p5-7) so a class-specific drop fires a class-specific
                // CONGESTION signal back to the sender. Upstream filters
                // (CONGESTION carve-out p2-5/vc-b95, self-skip p2-3,
                // AllowSet p3-5, layer-drop p4-7) all run BEFORE this send,
                // so only already-eligible packets reach this drop point.
                if sender_session_id != 0 {
                    self.logic
                        .on_outbound_drop_class(sender_session_id, dropped_class);
                }
            }
            SendOutcome::Refused(_) => {
                // P0Control class full — per PLAN.md Phase 5 we terminate the
                // session because the control channel must never wedge.
                error!(
                    "P0Control class full — terminating WS session {}",
                    self.logic.id
                );
                self.teardown_reason = TeardownReason::Error;
                ctx.stop();
            }
        }
    }
}

/// Write a priority-ordered binary frame to the WebSocket. Delivered by the
/// drainer task (see [`Actor::started`]).
impl Handler<SendBinaryFrame> for WsChatSession {
    type Result = ();

    fn handle(&mut self, msg: SendBinaryFrame, ctx: &mut Self::Context) -> Self::Result {
        ctx.binary(msg.0);
    }
}

/// Handle outbound packets (forwarding to ChatServer)
impl Handler<Packet> for WsChatSession {
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
// WebSocket Stream Handler
// =============================================================================

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for WsChatSession {
    fn handle(&mut self, item: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        let msg = match item {
            Ok(msg) => msg,
            Err(err) => {
                error!("WebSocket protocol error: {:?}", err);
                ctx.stop();
                return;
            }
        };

        match msg {
            ws::Message::Binary(data) => {
                self.heartbeat = Instant::now();

                let action = self.logic.handle_inbound(&data);

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
                    InboundAction::Echo(bytes) => {
                        // RTT echo is the only path that produces Echo today
                        // (see `session_logic::handle_inbound` PacketKind::Rtt
                        // arm). RTT classifies as P0Control under
                        // `classify_outbound`, so route the echo through the
                        // priority queue at that class — keeps ordering
                        // invariants honest and avoids a direct-write bypass
                        // around the per-session queue.
                        let echo_bytes = Bytes::from(bytes.as_ref().clone());
                        match self.outbound_tx.send(Class::P0Control, echo_bytes) {
                            SendOutcome::Sent => {}
                            SendOutcome::Dropped(_, _) => {}
                            SendOutcome::Refused(_) => {
                                error!(
                                    "P0Control class full on RTT echo — terminating WS session {}",
                                    self.logic.id
                                );
                                self.teardown_reason = TeardownReason::Error;
                                ctx.stop();
                            }
                        }
                    }
                    InboundAction::Forward(bytes, kind) => {
                        ctx.notify(Packet { data: bytes, kind });
                    }
                    InboundAction::Processed | InboundAction::KeepAlive => {}
                }
            }
            ws::Message::Ping(msg) => {
                self.heartbeat = Instant::now();
                ctx.pong(&msg);
            }
            ws::Message::Pong(_) => {
                self.heartbeat = Instant::now();
            }
            ws::Message::Text(_) => {
                self.heartbeat = Instant::now();
            }
            ws::Message::Close(reason) => {
                info!(
                    "Close received for session {} in room {}",
                    self.logic.id, self.logic.room
                );
                // Do NOT send Leave here. ctx.stop() triggers stopping() which
                // sends Disconnect with the correct observer flag. A separate
                // Leave would bypass the observer check and emit a spurious
                // PARTICIPANT_LEFT for observer (waiting-room) sessions.
                ctx.close(reason);
                ctx.stop();
            }
            _ => (),
        }
    }

    fn started(&mut self, _ctx: &mut Self::Context) {}

    fn finished(&mut self, ctx: &mut Self::Context) {
        ctx.stop()
    }
}

/// Classify the outbound bytes by parse-and-inspecting the wrapper once.
///
/// Routes the bytes into one of the 5 priority classes per p5-3. If the outer
/// `PacketWrapper` fails to parse, we fall back to `P3VideoBase` — same
/// fallback as `classify_outbound`'s unknown-packet-type branch.
///
/// Test-only helper; `Handler<Message>` inlines the parse so it can also
/// extract the sender's `session_id` for CongestionTracker attribution
/// without a second parse pass.
#[cfg(test)]
fn classify_outbound_bytes(bytes: &Bytes) -> Class {
    match parse_and_inspect(bytes.as_ref()) {
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

// =============================================================================
// Helper Methods
// =============================================================================

impl WsChatSession {
    fn join_room(&self, ctx: &mut WebsocketContext<Self>) {
        let join_room = self.logic.addr.send(self.logic.create_join_room_message());
        let join_room = join_room.into_actor(self);
        join_room
            .then(|response, act, ctx| {
                if let Some(reason) = act.logic.handle_join_room_result(response) {
                    // vc-n9o: record the decline reason so `stopping` labels
                    // the teardown to match the decision counter — a WS
                    // redirect counts as `reason=redirect`, a reject as
                    // `error`. Unlike WebTransport, WS has no quinn bridge:
                    // `ctx.stop()` synchronously ends the actor and closes the
                    // actix-web-actors stream, so there is no mailbox-
                    // starvation hang and no `accept_inbound` flag to clear.
                    act.teardown_reason = reason;
                    ctx.stop();
                }
                fut::ready(())
            })
            .wait(ctx);
    }
}

// ==========================================================================
// Session Lifecycle Integration Test (WebSocket)
// ==========================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use crate::actors::chat_server::ChatServer;
    use crate::server_diagnostics::ServerDiagnostics;
    use crate::session_manager::SessionManager;
    use actix::Actor;
    use actix_web::{web, App, HttpRequest, HttpServer};
    use actix_web_actors::ws;
    use futures_util::StreamExt;
    use protobuf::Message as ProtoMessage;
    use serial_test::serial;
    use std::time::Duration;
    use tokio_tungstenite::tungstenite::Message;

    /// Test helper: create a database pool for future JWT flow integration tests.
    #[allow(dead_code)]
    async fn get_test_pool() -> sqlx::PgPool {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set for tests");
        sqlx::PgPool::connect(&database_url)
            .await
            .expect("Failed to connect to test database")
    }

    /// Start WebSocket server for testing
    async fn start_websocket_server(port: u16) {
        let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());
        let nats_client = crate::nats_connect::connect(&nats_url)
            .await
            .expect("Failed to connect to NATS");

        let chat = ChatServer::new(nats_client.clone()).await.start();
        let session_manager = SessionManager::new();

        let (_, tracker_sender, _) = ServerDiagnostics::new_with_channel(nats_client.clone());

        // Use actix_rt::spawn which doesn't require Send
        actix_rt::spawn(async move {
            let _ = HttpServer::new(move || {
                let chat = chat.clone();
                let nats_client = nats_client.clone();
                let tracker_sender = tracker_sender.clone();
                let session_manager = session_manager.clone();

                App::new().route(
                    "/ws/{room}/{user_id}",
                    web::get().to(
                        move |req: HttpRequest,
                              stream: web::Payload,
                              path: web::Path<(String, String)>| {
                            let chat = chat.clone();
                            let nats_client = nats_client.clone();
                            let tracker_sender = tracker_sender.clone();
                            let session_manager = session_manager.clone();

                            async move {
                                let (room, user_id) = path.into_inner();
                                let display_name = user_id.clone(); // test fallback
                                let actor = WsChatSession::new(
                                    chat,
                                    room,
                                    user_id,
                                    display_name,
                                    nats_client,
                                    tracker_sender,
                                    session_manager,
                                    false, // tests use non-observer sessions
                                );
                                ws::start(actor, &req, stream)
                                    .map_err(actix_web::error::ErrorInternalServerError)
                            }
                        },
                    ),
                )
            })
            .bind(format!("127.0.0.1:{port}"))
            .expect("Failed to bind server")
            .run()
            .await;
        });
    }

    async fn wait_for_server_ready(port: u16) {
        let url = format!("ws://127.0.0.1:{port}/ws/test/test");
        for _ in 0..50 {
            if tokio_tungstenite::connect_async(&url).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("WebSocket server not ready after 5 seconds");
    }

    async fn connect_ws_client(
        port: u16,
        room: &str,
        user: &str,
    ) -> Result<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        Box<dyn std::error::Error>,
    > {
        let url = format!("ws://127.0.0.1:{port}/ws/{room}/{user}");
        let (ws_stream, _) = tokio_tungstenite::connect_async(&url).await?;
        Ok(ws_stream)
    }

    async fn wait_for_meeting_started(
        ws: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        use videocall_types::protos::meeting_packet::meeting_packet::MeetingEventType;
        use videocall_types::protos::meeting_packet::MeetingPacket;
        use videocall_types::protos::packet_wrapper::packet_wrapper::PacketType;
        use videocall_types::protos::packet_wrapper::PacketWrapper;

        let deadline = tokio::time::Instant::now() + timeout;
        while tokio::time::Instant::now() < deadline {
            tokio::select! {
                msg = ws.next() => {
                    if let Some(Ok(Message::Binary(data))) = msg {
                        if let Ok(wrapper) = PacketWrapper::parse_from_bytes(&data) {
                            if wrapper.packet_type == PacketType::MEETING.into() {
                                if let Ok(meeting) = MeetingPacket::parse_from_bytes(&wrapper.data) {
                                    if meeting.event_type == MeetingEventType::MEETING_STARTED.into() {
                                        return Ok(());
                                    }
                                }
                            }
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(50)) => {}
            }
        }
        anyhow::bail!("Timeout waiting for MEETING_STARTED")
    }

    #[actix_rt::test]
    #[serial]
    async fn test_meeting_lifecycle_websocket() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(std::io::stderr)
            .try_init();

        // Enable meeting management for this test
        videocall_types::FeatureFlags::set_meeting_management_override(true);

        let result = test_meeting_lifecycle_ws_impl().await;

        // Clean up feature flag
        videocall_types::FeatureFlags::clear_meeting_management_override();

        if let Err(e) = result {
            panic!("Test failed: {e}");
        }
    }

    async fn test_meeting_lifecycle_ws_impl() -> anyhow::Result<()> {
        println!("=== STARTING SESSION LIFECYCLE TEST (WebSocket) ===");

        let room_id = "ws-meeting-lifecycle-test";
        let port = 18080; // Use a unique port for testing

        println!("Starting WebSocket server on port {port}...");
        start_websocket_server(port).await;

        // Wait for server to be ready
        wait_for_server_ready(port).await;
        println!("✓ Server ready");

        // ========== STEP 1: First user connects ==========
        println!("\n--- Step 1: Alice connects (first participant) ---");

        let mut ws_alice = connect_ws_client(port, room_id, "alice")
            .await
            .expect("connect alice");
        wait_for_meeting_started(&mut ws_alice, Duration::from_secs(5)).await?;
        println!("✓ Alice connected and received MEETING_STARTED");

        // ========== STEP 2: Second user connects ==========
        println!("\n--- Step 2: Bob connects (second participant) ---");

        let mut ws_bob = connect_ws_client(port, room_id, "bob")
            .await
            .expect("connect bob");
        wait_for_meeting_started(&mut ws_bob, Duration::from_secs(5)).await?;
        println!("✓ Bob connected and received MEETING_STARTED");

        // ========== STEP 3: Third user connects ==========
        println!("\n--- Step 3: Charlie connects (third participant) ---");

        let mut ws_charlie = connect_ws_client(port, room_id, "charlie")
            .await
            .expect("connect charlie");
        wait_for_meeting_started(&mut ws_charlie, Duration::from_secs(5)).await?;
        println!("✓ Charlie connected and received MEETING_STARTED");

        // ========== STEP 4: Charlie disconnects ==========
        println!("\n--- Step 4: Charlie disconnects ---");
        drop(ws_charlie);
        tokio::time::sleep(Duration::from_millis(500)).await;
        println!("✓ Charlie disconnected");

        // ========== STEP 5: Bob disconnects ==========
        println!("\n--- Step 5: Bob disconnects ---");
        drop(ws_bob);
        tokio::time::sleep(Duration::from_millis(500)).await;
        println!("✓ Bob disconnected");

        // ========== STEP 6: Alice (last) disconnects ==========
        println!("\n--- Step 6: Alice disconnects - session ends ---");
        drop(ws_alice);
        tokio::time::sleep(Duration::from_millis(500)).await;
        println!("✓ Alice disconnected");

        println!("\n=== SESSION LIFECYCLE TEST PASSED (WebSocket) ===");
        Ok(())
    }

    // ----- p5-5: classify + priority-queue swap (burst test) -----
    //
    // Acceptance: feed a burst of P4Enhancement frames alongside a P0Control
    // frame, assert P0Control drains before any of the burst's tail. This is
    // the WebSocket-side equivalent of p5-4's WT burst test.

    use crate::sfu::priority_queue::PrioritySender as PqSender;
    use videocall_types::protos::media_packet::media_packet::MediaType as PbMediaType;
    use videocall_types::protos::media_packet::{MediaPacket, RoutingHeader};
    use videocall_types::protos::packet_wrapper::packet_wrapper::PacketType as PbPacketType;
    use videocall_types::protos::packet_wrapper::PacketWrapper;

    fn build_p0_control_bytes() -> Bytes {
        // SPEAKER_UPDATE is unambiguously P0Control, with no need for an
        // inner MediaPacket parse.
        let mut w = PacketWrapper::new();
        w.packet_type = PbPacketType::SPEAKER_UPDATE.into();
        Bytes::from(w.write_to_bytes().expect("serialize SPEAKER_UPDATE"))
    }

    fn build_p4_enhancement_bytes() -> Bytes {
        // VIDEO + non-T0/S0 routing header lands in P4Enhancement.
        let mut rh = RoutingHeader::new();
        rh.is_keyframe = false;
        rh.temporal_layer_id = 2;
        rh.spatial_layer_id = 0;

        let mut mp = MediaPacket::new();
        mp.media_type = PbMediaType::VIDEO.into();
        mp.routing_header = ::protobuf::MessageField::some(rh);
        let inner = mp.write_to_bytes().expect("serialize MediaPacket");

        let mut w = PacketWrapper::new();
        w.packet_type = PbPacketType::MEDIA.into();
        w.data = inner;
        Bytes::from(w.write_to_bytes().expect("serialize MEDIA wrapper"))
    }

    #[test]
    fn classify_outbound_bytes_routes_speaker_update_to_p0_control() {
        let bytes = build_p0_control_bytes();
        assert_eq!(classify_outbound_bytes(&bytes), Class::P0Control);
    }

    #[test]
    fn classify_outbound_bytes_routes_video_enhancement_to_p4_enhancement() {
        let bytes = build_p4_enhancement_bytes();
        assert_eq!(classify_outbound_bytes(&bytes), Class::P4Enhancement);
    }

    #[test]
    fn classify_outbound_bytes_unparseable_falls_back_to_p3_video_base() {
        // Garbage that fails PacketWrapper parse.
        let bytes = Bytes::from_static(&[0xff, 0xff, 0xff, 0xff]);
        assert_eq!(classify_outbound_bytes(&bytes), Class::P3VideoBase);
    }

    /// Burst test: push 100 P4Enhancement frames followed by a single
    /// P0Control frame through the same path used by `Handler<Message>`
    /// (classify + `PrioritySender::send`). Assert the P0Control frame
    /// drains BEFORE any of the P4 tail — i.e. strict priority is honored
    /// even when the P4 queue is loaded first.
    #[tokio::test]
    async fn p5_5_burst_test_p0_preempts_p4_tail_on_ws_path() {
        let (sender, channels) = PqSender::new();
        let mut receiver = PriorityReceiver::new(channels);

        // Push a burst of P4Enhancement frames. P4 has a 256-slot HeadDrop
        // queue, so 100 will all be admitted.
        for _ in 0..100 {
            let bytes = build_p4_enhancement_bytes();
            let class = classify_outbound_bytes(&bytes);
            assert_eq!(class, Class::P4Enhancement);
            assert_eq!(sender.send(class, bytes), SendOutcome::Sent);
        }

        // Now push a single P0Control frame AFTER the P4 tail is enqueued.
        let p0_bytes = build_p0_control_bytes();
        let p0_class = classify_outbound_bytes(&p0_bytes);
        assert_eq!(p0_class, Class::P0Control);
        let p0_marker = p0_bytes.clone();
        assert_eq!(sender.send(p0_class, p0_bytes), SendOutcome::Sent);

        // First drain MUST be the P0Control frame, not any of the buffered
        // P4 frames — strict priority. We compare bytes equality against the
        // SPEAKER_UPDATE marker because P4 frames are MEDIA wrappers with
        // distinct inner payloads.
        let first = receiver.recv().await.expect("at least one packet");
        assert_eq!(
            first, p0_marker,
            "P0Control must drain first under strict priority, ahead of the P4 burst tail"
        );

        // Remaining 100 drains must all be P4 frames (and exactly 100).
        let mut tail_count = 0;
        drop(sender);
        while let Some(bytes) = receiver.recv().await {
            assert_ne!(bytes, p0_marker, "no second P0Control should appear");
            // Sanity: every tail entry classifies back to P4Enhancement.
            assert_eq!(classify_outbound_bytes(&bytes), Class::P4Enhancement);
            tail_count += 1;
        }
        assert_eq!(tail_count, 100, "all 100 P4 frames must drain");
    }
}
