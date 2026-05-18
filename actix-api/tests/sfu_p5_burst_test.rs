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
 */

//! p5-9: P5 close-gate burst-load integration test for the SFU outbound
//! priority queue.
//!
//! Asserts the real-world value of the five-class priority queue: under a
//! sustained video burst that exceeds a constrained receiver's bandwidth,
//! audio (P1) still gets through with <0.1% loss while video (P3/P4) drops
//! happen, P0 control packets are never dropped, and the SFU drop counters
//! advance.
//!
//! ## Fixture shape
//!
//! Mirrors `sfu_p4_throttle_test.rs`:
//!
//! * In-process [`ChatServer`] in SFU mode over a real NATS server.
//! * 1 sender publishing synthetic L1T3 VIDEO at ~1.2 Mbps shape (P2 keyframes + P3 base + P4 enhancement) plus synthetic AUDIO at 32 kbps shape.
//! * 1 receiver clamped to 1000 kbps via `DiagnosticsPacket`, forcing the p4-7 layer-drop filter to discard enhancement-layer video before it reaches the receiver.
//! * An auxiliary CONGESTION sender so we can validate the P0Control carve-out under the same load.
//!
//! ## Wiring the receiver through a real `PrioritySender`
//!
//! The CapturingSession used by `sfu_p4_throttle_test.rs` writes every
//! delivered `Message` straight into a `Vec` — it does NOT exercise the
//! P5 priority queue at all. To make the test meaningful for the
//! close-gate, this fixture defines [`PriorityCapturingSession`], which
//! mirrors the production `WsChatSession` wiring (see
//! `actors/transports/ws_chat_session.rs`):
//!
//!   1. `Handler<Message>` classifies the inbound frame via
//!      [`classify_outbound`] and pushes it into a per-session
//!      [`PrioritySender`]. The `SendOutcome` (Sent / Dropped / Refused)
//!      is recorded per class — this is the "every packet handed to the
//!      PrioritySender" sample point.
//!   2. A spawned drainer task pulls from the matching
//!      [`PriorityReceiver`] in strict-priority + 8-quantum order
//!      (`PriorityReceiver::recv`), and records every drained frame per
//!      class — this is the "every packet that actually reaches the
//!      receiver" sample point.
//!
//! Per-class loss is then `1 - (received / sent_into_priority_queue)`.
//! "Sent into priority queue" deliberately excludes packets the SFU
//! forwarder dropped upstream (e.g. layer_budget) — those drops are
//! attributed to the forwarder and accounted for in the offered-vs-sent
//! diagnostic, but the priority-queue loss rate is the post-forwarder
//! arithmetic the bead asks us to assert.
//!
//! ### Why not use the real `WsChatSession`?
//!
//! Adding a full WS server fixture would inflate test runtime well past
//! the 30 s budget the bead mandates and would couple this close-gate
//! test to actix-web's HTTP machinery. The production wiring we DO need
//! to exercise — `classify_outbound` + `PrioritySender::send` +
//! `PriorityReceiver::recv` + drainer task — is reproduced verbatim
//! here; nothing about the priority-queue contract is being mocked.
//!
//! ## No test-only hooks added to production code
//!
//! All recording lives inside this test file's helper actor. Production
//! source is unchanged.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use actix::{Actor, Context, Handler, Recipient};
use protobuf::Message as ProtobufMessage;
use protobuf::MessageField;
use serial_test::serial;
use tokio::time::sleep;

use sec_api::actors::chat_server::ChatServer;
use sec_api::actors::packet_handler::parse_and_inspect;
use sec_api::actors::session_logic::SessionId;
use sec_api::messages::server::{ActivateConnection, ClientMessage, Connect, JoinRoom, Packet};
use sec_api::messages::session::Message;
use sec_api::metrics::SFU_DROPPED_TOTAL;
use sec_api::sfu::priority_queue::{
    classify_outbound, Class, PriorityReceiver, PrioritySender, SendOutcome,
};

use videocall_types::protos::diagnostics_packet::{BandwidthEstimate, DiagnosticsPacket};
use videocall_types::protos::media_packet::media_packet::MediaType;
use videocall_types::protos::media_packet::{MediaPacket, RoutingHeader};
use videocall_types::protos::packet_wrapper::packet_wrapper::PacketType;
use videocall_types::protos::packet_wrapper::PacketWrapper;
use videocall_types::protos::subscription_packet::SubscriptionUpdate;

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

/// RAII guard for SFU_MODE — mirrors `sfu_p4_throttle_test.rs::EnvGuard`.
struct EnvGuard {
    prior: Option<String>,
}

impl EnvGuard {
    fn new() -> Self {
        Self {
            prior: std::env::var("SFU_MODE").ok(),
        }
    }
    fn set(&self, value: &str) {
        std::env::set_var("SFU_MODE", value);
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.prior {
            Some(v) => std::env::set_var("SFU_MODE", v),
            None => std::env::remove_var("SFU_MODE"),
        }
    }
}

/// Per-class counters for both the enqueue and drain sample points.
///
/// Indices match [`Class::all()`] ordering: 0=P0Control, 1=P1Audio,
/// 2=P2Keyframe, 3=P3VideoBase, 4=P4Enhancement.
#[derive(Default, Debug, Clone)]
struct ClassTally {
    enqueued: [usize; 5],
    enqueue_dropped: [usize; 5],
    enqueue_refused: [usize; 5],
    drained: [usize; 5],
}

impl ClassTally {
    fn idx(c: Class) -> usize {
        match c {
            Class::P0Control => 0,
            Class::P1Audio => 1,
            Class::P2Keyframe => 2,
            Class::P3VideoBase => 3,
            Class::P4Enhancement => 4,
        }
    }
}

/// Actor that simulates the production `WsChatSession` priority-queue
/// pipeline. `Handler<Message>` classifies + pushes into the per-session
/// [`PrioritySender`]; a separately-spawned drainer task pulls from the
/// matching [`PriorityReceiver`] and records every delivered frame.
///
/// All four sample points (enqueue Sent, enqueue Dropped, enqueue Refused,
/// drain) update the shared `tally` so the test body can compute per-class
/// loss after the burst.
struct PriorityCapturingSession {
    outbound_tx: PrioritySender,
    tally: Arc<Mutex<ClassTally>>,
}

impl Actor for PriorityCapturingSession {
    type Context = Context<Self>;
}

impl Handler<Message> for PriorityCapturingSession {
    type Result = ();
    fn handle(&mut self, msg: Message, _ctx: &mut Self::Context) {
        // Mirror `WsChatSession::Handler<Message>`: parse once for
        // classification, then send into the per-class queue.
        let bytes = msg.msg.clone();
        let parsed = parse_and_inspect(bytes.as_ref());
        let class = match &parsed {
            Some(p) => {
                let media_type = p
                    .media_packet
                    .as_ref()
                    .map(|mp| mp.media_type.enum_value_or_default());
                classify_outbound(&p.wrapper, media_type, p.routing_header())
            }
            None => Class::P3VideoBase,
        };

        let outcome = self.outbound_tx.send(class, bytes);
        let mut t = self.tally.lock().expect("tally mutex");
        let i = ClassTally::idx(class);
        match outcome {
            SendOutcome::Sent => t.enqueued[i] += 1,
            SendOutcome::Dropped(dropped_class, _reason) => {
                // Account the drop against the class the queue actually
                // attempted to enqueue — same as `class` here, but defensive
                // in case classify_outbound is ever extended to remap.
                let di = ClassTally::idx(dropped_class);
                t.enqueue_dropped[di] += 1;
                // Even on a TailDropOldest eviction the *new* entry is now
                // in the queue (per `priority_queue.rs::ClassSender::send`),
                // so it will eventually drain. To match the bead's "every
                // packet handed to the PrioritySender" we count it as
                // enqueued for the purposes of the loss-rate denominator —
                // the eviction is reflected in `enqueue_dropped` separately.
                t.enqueued[i] += 1;
            }
            SendOutcome::Refused(_) => {
                t.enqueue_refused[i] += 1;
            }
        }
    }
}

struct Participant {
    sid: SessionId,
    user: String,
    recipient: Recipient<Message>,
    tally: Arc<Mutex<ClassTally>>,
}

impl Participant {
    /// Construct a participant whose receiving side runs through a real
    /// [`PrioritySender`] + [`PriorityReceiver`] pipeline.
    fn new_with_priority(sid: SessionId, user: &str) -> Self {
        let tally: Arc<Mutex<ClassTally>> = Arc::new(Mutex::new(ClassTally::default()));
        let (outbound_tx, channels) = PrioritySender::new();

        // Drainer task — mirrors the `actix_rt::spawn` in
        // `WsChatSession::started`. Each pop records the drained class into
        // the shared tally.
        let drain_tally = Arc::clone(&tally);
        let mut receiver = PriorityReceiver::new(channels);
        tokio::spawn(async move {
            while let Some(bytes) = receiver.recv().await {
                let class = parse_and_inspect(bytes.as_ref())
                    .map(|p| {
                        let media_type = p
                            .media_packet
                            .as_ref()
                            .map(|mp| mp.media_type.enum_value_or_default());
                        classify_outbound(&p.wrapper, media_type, p.routing_header())
                    })
                    .unwrap_or(Class::P3VideoBase);
                let mut t = drain_tally.lock().expect("tally mutex");
                t.drained[ClassTally::idx(class)] += 1;
            }
        });

        let actor = PriorityCapturingSession {
            outbound_tx,
            tally: Arc::clone(&tally),
        }
        .start();

        Self {
            sid,
            user: user.to_string(),
            recipient: actor.recipient(),
            tally,
        }
    }

    /// Snapshot the tally for diagnostics / assertions.
    fn snapshot(&self) -> ClassTally {
        self.tally.lock().expect("tally mutex").clone()
    }
}

/// Trivial recipient for non-receiver participants (sender + cong-origin).
/// They only publish; nothing in the test inspects their inbound traffic.
struct NullSession;
impl Actor for NullSession {
    type Context = Context<Self>;
}
impl Handler<Message> for NullSession {
    type Result = ();
    fn handle(&mut self, _msg: Message, _ctx: &mut Self::Context) {}
}

struct NullParticipant {
    sid: SessionId,
    user: String,
    recipient: Recipient<Message>,
}

impl NullParticipant {
    fn new(sid: SessionId, user: &str) -> Self {
        let actor = NullSession.start();
        Self {
            sid,
            user: user.to_string(),
            recipient: actor.recipient(),
        }
    }
}

/// Identical to `sfu_p4_throttle_test.rs::register_and_join` but generic
/// over participant shape.
async fn register_and_join(
    chat: &actix::Addr<ChatServer>,
    sid: SessionId,
    user: &str,
    recipient: Recipient<Message>,
    room: &str,
) -> Result<(), String> {
    chat.send(Connect {
        id: sid,
        addr: recipient,
    })
    .await
    .map_err(|e| format!("Connect mailbox: {e}"))?;

    chat.send(JoinRoom {
        session: sid,
        room: room.to_string(),
        user_id: user.to_string(),
        display_name: user.to_string(),
        observer: false,
        capabilities: 0,
    })
    .await
    .map_err(|e| format!("JoinRoom mailbox: {e}"))??;

    chat.send(ActivateConnection { session: sid })
        .await
        .map_err(|e| format!("ActivateConnection mailbox: {e}"))?;

    Ok(())
}

/// Build a `PacketWrapper` carrying a `MediaPacket` of `media_type` with the
/// given routing-header layer ids. Mirrors
/// `sfu_p4_throttle_test.rs::build_l1t3_video` but generalized for AUDIO as
/// well so a single helper can synthesize both classes.
///
/// `payload_bytes` controls the inner `MediaPacket.data` length so each
/// emitted frame matches the bead's rough byte-rate target (~5 KB video
/// frames at 30 fps ≈ 1.2 Mbps; ~80 B audio frames at 50 fps ≈ 32 kbps).
///
/// 8 arguments is intentional: this helper threads through every field
/// the test sweeps independently (sender identity, media class, SVC
/// layer ids, sequencing/picture id, content seed, payload size). A
/// builder struct would obscure the per-call sweep pattern.
#[allow(clippy::too_many_arguments)]
fn build_media(
    sender_sid: SessionId,
    sender_user: &str,
    media_type: MediaType,
    is_keyframe: bool,
    temporal: u32,
    picture_id: u64,
    seed: u8,
    payload_bytes: usize,
) -> Vec<u8> {
    let rh = RoutingHeader {
        is_keyframe,
        spatial_layer_id: 0,
        temporal_layer_id: temporal,
        frame_marker: 0,
        picture_id,
        ..Default::default()
    };
    let media = MediaPacket {
        media_type: media_type.into(),
        data: vec![seed; payload_bytes],
        routing_header: MessageField::some(rh),
        ..Default::default()
    };
    let wrapper = PacketWrapper {
        packet_type: PacketType::MEDIA.into(),
        session_id: sender_sid,
        user_id: sender_user.as_bytes().to_vec(),
        data: media.write_to_bytes().expect("encode MediaPacket"),
        ..Default::default()
    };
    wrapper.write_to_bytes().expect("encode PacketWrapper")
}

/// Inject a bandwidth estimate for `receiver` via a `DiagnosticsPacket`.
/// Mirrors `sfu_p4_throttle_test.rs::inject_bandwidth`.
async fn inject_bandwidth(
    chat: &actix::Addr<ChatServer>,
    receiver_sid: SessionId,
    receiver_user: &str,
    room: &str,
    kbps: u32,
) {
    let mut est = BandwidthEstimate::new();
    est.estimated_downlink_kbps = kbps;
    let diag = DiagnosticsPacket {
        bandwidth_estimate: MessageField::some(est),
        ..Default::default()
    };
    let wrapper = PacketWrapper {
        packet_type: PacketType::DIAGNOSTICS.into(),
        session_id: receiver_sid,
        user_id: receiver_user.as_bytes().to_vec(),
        data: diag.write_to_bytes().expect("encode DiagnosticsPacket"),
        ..Default::default()
    };
    let bytes = wrapper.write_to_bytes().expect("encode wrapper");
    chat.send(ClientMessage {
        session: receiver_sid,
        room: room.to_string(),
        user: receiver_user.to_string(),
        msg: Packet {
            data: Arc::new(bytes),
        },
    })
    .await
    .expect("DiagnosticsPacket ClientMessage");
}

/// Pin the receiver to a single sender so the LayerSelector budget arithmetic
/// matches the bead's single-publisher scenario (mirrors
/// `sfu_p4_throttle_test.rs::pin_receiver_to`).
async fn pin_receiver_to(
    chat: &actix::Addr<ChatServer>,
    receiver_sid: SessionId,
    receiver_user: &str,
    room: &str,
    pinned: SessionId,
) {
    let mut update = SubscriptionUpdate::new();
    update.pinned_sessions = vec![pinned];
    update.slots = vec![];
    // `receive_all_audio=true` so the receiver gets the sender's audio
    // independent of the video pin's layer-budget arithmetic.
    update.receive_all_audio = true;
    let wrapper = PacketWrapper {
        packet_type: PacketType::SUBSCRIPTION_UPDATE.into(),
        session_id: receiver_sid,
        user_id: receiver_user.as_bytes().to_vec(),
        data: update.write_to_bytes().expect("encode SubscriptionUpdate"),
        ..Default::default()
    };
    let bytes = wrapper.write_to_bytes().expect("encode wrapper");
    chat.send(ClientMessage {
        session: receiver_sid,
        room: room.to_string(),
        user: receiver_user.to_string(),
        msg: Packet {
            data: Arc::new(bytes),
        },
    })
    .await
    .expect("SubscriptionUpdate ClientMessage");
}

/// Publish a CONGESTION packet from a non-receiver session so the
/// per-room dispatcher fans it to the receiver via the egress carve-out.
async fn publish_congestion(
    chat: &actix::Addr<ChatServer>,
    sender_sid: SessionId,
    sender_user: &str,
    room: &str,
    seed: u8,
) {
    let wrapper = PacketWrapper {
        packet_type: PacketType::CONGESTION.into(),
        session_id: sender_sid,
        user_id: sender_user.as_bytes().to_vec(),
        data: vec![seed; 24],
        ..Default::default()
    };
    let bytes = wrapper.write_to_bytes().expect("encode CONGESTION wrapper");
    chat.send(ClientMessage {
        session: sender_sid,
        room: room.to_string(),
        user: sender_user.to_string(),
        msg: Packet {
            data: Arc::new(bytes),
        },
    })
    .await
    .expect("CONGESTION ClientMessage");
}

// ---------------------------------------------------------------------------
// Timings — mirror `sfu_p4_throttle_test.rs` constants
// ---------------------------------------------------------------------------

/// Settle after JoinRoom / membership-shifting events so the per-room
/// dispatcher has subscribed and the subscription store has converged.
const SUBSCRIBE_SETTLE: Duration = Duration::from_millis(300);

/// Settle after a DiagnosticsPacket so the layer-selector cache has
/// invalidated against the new bandwidth.
const BW_SETTLE: Duration = Duration::from_millis(80);

/// Settle after the burst so the per-room dispatcher has fanned every NATS
/// message to the receiver and the receiver's drainer has consumed every
/// queued frame.
const FANOUT_SETTLE: Duration = Duration::from_millis(800);

/// Total number of GOPs to burst through the SFU. One GOP = 1 keyframe + 3
/// T0 + 3 T1 + 3 T2 deltas (10 video frames) + 5 audio packets.
///
/// At 30 fps video and 50 fps audio in real time, each GOP corresponds to
/// ~333 ms of media. 80 GOPs = ~26.6 s of media playback at the bead's
/// "10 MB of video" envelope (10 MB / 1.2 Mbps ≈ 67 s — the in-process
/// fixture compresses wall-clock dramatically; we keep the per-class
/// packet counts large enough for the loss-rate denominators to be
/// statistically meaningful while bounding wall-clock test runtime well
/// under the 30 s acceptance budget).
const GOP_COUNT: usize = 80;

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[actix_rt::test]
#[serial]
async fn sfu_p5_burst_priority_queue() {
    let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());

    let env = EnvGuard::new();
    env.set("sfu");

    let nats_client = async_nats::connect(&nats_url)
        .await
        .expect("connect to NATS");

    let room = "p5-9-burst".to_string();
    let chat = ChatServer::new(nats_client.clone()).await.start();

    // One video+audio sender, one constrained receiver, one CONGESTION
    // origin. Distinct session ids per role; the receiver alone owns the
    // PrioritySender pipeline.
    let sender = NullParticipant::new(90_001, "sender@p5-9");
    let receiver = Participant::new_with_priority(90_002, "receiver@p5-9");
    let cong_origin = NullParticipant::new(90_003, "cong-origin@p5-9");

    register_and_join(
        &chat,
        sender.sid,
        &sender.user,
        sender.recipient.clone(),
        &room,
    )
    .await
    .expect("sender join");
    register_and_join(
        &chat,
        receiver.sid,
        &receiver.user,
        receiver.recipient.clone(),
        &room,
    )
    .await
    .expect("receiver join");
    register_and_join(
        &chat,
        cong_origin.sid,
        &cong_origin.user,
        cong_origin.recipient.clone(),
        &room,
    )
    .await
    .expect("cong-origin join");
    sleep(SUBSCRIBE_SETTLE).await;

    // Pin receiver to the sender so layer-budget arithmetic is single-publisher.
    pin_receiver_to(&chat, receiver.sid, &receiver.user, &room, sender.sid).await;
    sleep(SUBSCRIBE_SETTLE).await;

    // Constrain receiver to 1000 kbps. With the 0.85 headroom in
    // `LayerSelector` this yields an 850 kbps budget — under a sustained
    // ~1.2 Mbps L1T3 video offer, the forwarder will drop T2 enhancement
    // frames via the `layer_budget` filter while T0+T1 (480+360 ≈ 840 kbps
    // for the GOP shape below) still fits.
    inject_bandwidth(&chat, receiver.sid, &receiver.user, &room, 1000).await;
    sleep(BW_SETTLE).await;

    // Snapshot the layer_budget counter so we can assert it advances. We
    // also snapshot self_skip / unsubscribed / kfr_unsubscribed for the
    // post-test diagnostic print.
    let layer_budget_before = SFU_DROPPED_TOTAL.with_label_values(&["layer_budget"]).get();

    // -----------------------------------------------------------------
    // Sender-side bookkeeping: count what we hand into the SFU, per class.
    // These are the "offered" counts; they are NOT the priority-queue
    // denominators (those come from `receiver.tally().enqueued`).
    // -----------------------------------------------------------------
    let mut offered_audio = 0usize;
    let mut offered_video_kf = 0usize;
    let mut offered_video_t0 = 0usize;
    let mut offered_video_t1 = 0usize;
    let mut offered_video_t2 = 0usize;

    let mut picture_id: u64 = 0;
    let mut audio_picture_id: u64 = 100_000;
    let mut seed: u32 = 0;

    // Per-GOP layer pattern (matches `sfu_p4_throttle_test.rs::drive_l1t3_burst`).
    let video_pattern: [(u32, bool); 10] = [
        (0, true),  // T0+S0 keyframe (P2Keyframe)
        (2, false), // T2 delta      (P4Enhancement)
        (1, false), // T1 delta      (P4Enhancement)
        (2, false),
        (0, false), // T0 delta      (P3VideoBase)
        (2, false),
        (1, false),
        (2, false),
        (0, false),
        (1, false),
    ];

    // Synthetic payload sizes — chosen so the *aggregate* offered byte-
    // rate (per ~333 ms GOP, treating each emitted packet as one frame)
    // matches the bead's 1.2 Mbps video + 32 kbps audio shape closely
    // enough for the layer-budget filter to engage at 1000 kbps.
    //
    // 1.2 Mbps over 30 video frames = 40 kbit / frame = 5000 bytes.
    // 32 kbps over 5 audio packets/GOP = 6.4 kbit / pkt = 800 bytes.
    //
    // The actual byte count *per packet on the wire* includes the
    // PacketWrapper + MediaPacket overhead (~100 B) — small relative to
    // the payload so the layer_budget arithmetic still reflects the
    // intended class shape.
    const VIDEO_PAYLOAD: usize = 5000;
    const AUDIO_PAYLOAD: usize = 800;

    for _gop in 0..GOP_COUNT {
        // 10 video frames per GOP.
        for (temporal, is_kf) in video_pattern {
            seed = seed.wrapping_add(1);
            picture_id = picture_id.wrapping_add(1);
            let bytes = build_media(
                sender.sid,
                &sender.user,
                MediaType::VIDEO,
                is_kf,
                temporal,
                picture_id,
                (seed & 0xFF) as u8,
                VIDEO_PAYLOAD,
            );
            chat.send(ClientMessage {
                session: sender.sid,
                room: room.clone(),
                user: sender.user.clone(),
                msg: Packet {
                    data: Arc::new(bytes),
                },
            })
            .await
            .expect("video ClientMessage");

            if is_kf {
                offered_video_kf += 1;
            } else {
                match temporal {
                    0 => offered_video_t0 += 1,
                    1 => offered_video_t1 += 1,
                    2 => offered_video_t2 += 1,
                    _ => {}
                }
            }
        }

        // 5 audio packets per GOP — interleaved with video so they share
        // dispatcher fan-out turns. Audio carries no SVC layers; it always
        // classifies as P1Audio.
        for _ in 0..5 {
            seed = seed.wrapping_add(1);
            audio_picture_id = audio_picture_id.wrapping_add(1);
            let bytes = build_media(
                sender.sid,
                &sender.user,
                MediaType::AUDIO,
                false,
                0,
                audio_picture_id,
                (seed & 0xFF) as u8,
                AUDIO_PAYLOAD,
            );
            chat.send(ClientMessage {
                session: sender.sid,
                room: room.clone(),
                user: sender.user.clone(),
                msg: Packet {
                    data: Arc::new(bytes),
                },
            })
            .await
            .expect("audio ClientMessage");
            offered_audio += 1;
        }

        // Tight pacing — enough that the dispatcher gets scheduled
        // between packets, matching the p4-13 burst loop's 2 ms cadence.
        sleep(Duration::from_millis(2)).await;
    }

    // Inject one CONGESTION packet near the END of the burst — under the
    // sustained load, the P0Control carve-out must still deliver it to
    // the receiver. This is assertion 3.
    publish_congestion(&chat, cong_origin.sid, &cong_origin.user, &room, 0xC1).await;

    // Let the dispatcher + drainer fully consume the backlog before
    // sampling tallies.
    sleep(FANOUT_SETTLE).await;

    let tally = receiver.snapshot();
    let layer_budget_after = SFU_DROPPED_TOTAL.with_label_values(&["layer_budget"]).get();
    let layer_budget_delta = layer_budget_after - layer_budget_before;

    // -----------------------------------------------------------------
    // Compute per-class loss rates against the priority-queue boundary:
    //
    //   sent_<class>     = tally.enqueued[<class>]   (post-forwarder, pre-queue)
    //   received_<class> = tally.drained[<class>]    (post-drainer)
    //   loss             = 1 - (received / sent)
    //
    // This is what the bead asks for: the priority queue's effect, not
    // the forwarder's. Forwarder layer-budget drops are captured by the
    // `layer_budget_delta` assertion below and by the offered-vs-enqueued
    // diagnostic.
    // -----------------------------------------------------------------
    let p0_idx = ClassTally::idx(Class::P0Control);
    let p1_idx = ClassTally::idx(Class::P1Audio);
    let p2_idx = ClassTally::idx(Class::P2Keyframe);
    let p3_idx = ClassTally::idx(Class::P3VideoBase);
    let p4_idx = ClassTally::idx(Class::P4Enhancement);

    let sent_audio = tally.enqueued[p1_idx];
    let received_audio = tally.drained[p1_idx];
    // `sent_video` (post-forwarder, pre-PQ) is reported in the diagnostic
    // print only — assertion 2 measures end-to-end loss against the
    // OFFERED count (pre-forwarder) so it captures BOTH the forwarder's
    // layer_budget filter AND any priority-queue drops.
    let _sent_video_post_forwarder =
        tally.enqueued[p2_idx] + tally.enqueued[p3_idx] + tally.enqueued[p4_idx];
    let received_video = tally.drained[p2_idx] + tally.drained[p3_idx] + tally.drained[p4_idx];

    let audio_loss = if sent_audio == 0 {
        // No audio enqueued is itself a test failure — turn it into one.
        1.0
    } else {
        1.0 - (received_audio as f64 / sent_audio as f64)
    };

    // Track video drops at the BOTH layers we care about: the forwarder's
    // layer_budget (offered → enqueued shortfall) and the priority queue's
    // class drops (enqueued → drained shortfall). For assertion 2 the
    // bead just asks "video_loss > 0" measured offered → received; we
    // satisfy that with the broader offered→received view.
    let offered_video = offered_video_kf + offered_video_t0 + offered_video_t1 + offered_video_t2;
    let video_loss_offered_to_received = if offered_video == 0 {
        1.0
    } else {
        1.0 - (received_video as f64 / offered_video as f64)
    };

    // Diagnostic print (visible under `cargo test -- --nocapture`).
    eprintln!("==================== p5-9 burst test tally ====================");
    eprintln!(
        "OFFERED  audio={} video(kf+t0+t1+t2)={}+{}+{}+{}={}",
        offered_audio,
        offered_video_kf,
        offered_video_t0,
        offered_video_t1,
        offered_video_t2,
        offered_video,
    );
    eprintln!(
        "ENQUEUED (post-forwarder, pre-PQ) per class: P0={} P1(audio)={} P2(kf)={} P3(base)={} P4(enh)={}",
        tally.enqueued[p0_idx],
        tally.enqueued[p1_idx],
        tally.enqueued[p2_idx],
        tally.enqueued[p3_idx],
        tally.enqueued[p4_idx],
    );
    eprintln!(
        "DRAINED  (post-PQ) per class:                P0={} P1(audio)={} P2(kf)={} P3(base)={} P4(enh)={}",
        tally.drained[p0_idx],
        tally.drained[p1_idx],
        tally.drained[p2_idx],
        tally.drained[p3_idx],
        tally.drained[p4_idx],
    );
    eprintln!(
        "PQ DROPS per class:                          P0={} P1={} P2={} P3={} P4={}",
        tally.enqueue_dropped[p0_idx],
        tally.enqueue_dropped[p1_idx],
        tally.enqueue_dropped[p2_idx],
        tally.enqueue_dropped[p3_idx],
        tally.enqueue_dropped[p4_idx],
    );
    eprintln!(
        "PQ REFUSED per class:                        P0={} P1={} P2={} P3={} P4={}",
        tally.enqueue_refused[p0_idx],
        tally.enqueue_refused[p1_idx],
        tally.enqueue_refused[p2_idx],
        tally.enqueue_refused[p3_idx],
        tally.enqueue_refused[p4_idx],
    );
    eprintln!(
        "audio_loss(PQ)         = {:.6}  (sent={} received={})",
        audio_loss, sent_audio, received_audio,
    );
    eprintln!(
        "video_loss(end-to-end) = {:.6}  (offered={} received={})",
        video_loss_offered_to_received, offered_video, received_video,
    );
    eprintln!(
        "sfu_dropped_total{{reason=\"layer_budget\"}} delta = {}",
        layer_budget_delta,
    );
    eprintln!("===============================================================");

    // -----------------------------------------------------------------
    // Assertion 1: audio loss < 0.1% — P1 survives the burst.
    //
    // Audio is its own class (P1Audio, capacity 128, TailDropOldest). The
    // priority queue drains it ahead of P2/P3/P4 every quantum cycle, so
    // even under a saturating video burst the receiver should see every
    // audio packet enqueued.
    // -----------------------------------------------------------------
    assert!(
        sent_audio > 0,
        "no audio packets reached the priority queue — fixture broken"
    );
    assert!(
        audio_loss < 0.001,
        "audio loss {:.6} >= 0.001 (sent={} received={}). P1Audio class \
         is leaking under burst load — priority queue is broken.",
        audio_loss,
        sent_audio,
        received_audio,
    );

    // -----------------------------------------------------------------
    // Assertion 2: video drops happen. The receiver's 1000 kbps clamp
    // forces the forwarder + priority queue to discard some of the
    // ~1.2 Mbps offered video. If video_loss is zero the constraint
    // was never engaged and the test isn't proving anything.
    // -----------------------------------------------------------------
    assert!(
        offered_video > 0,
        "no video packets offered to the SFU — fixture broken"
    );
    assert!(
        video_loss_offered_to_received > 0.0,
        "video loss is zero (offered={} received={}). The bandwidth \
         clamp + priority queue did not drop ANY video — the close-gate \
         assertion is meaningless without observable video pressure.",
        offered_video,
        received_video,
    );

    // -----------------------------------------------------------------
    // Assertion 3: NO P0Control packet was dropped — CONGESTION reaches
    // the receiver and the P0 class never refused or evicted.
    //
    // We sent one CONGESTION during the burst (cong-origin). The
    // priority queue's P0Control policy is NeverDrop (refuse on full),
    // and we expect zero refusals AND zero TailDrop/HeadDrop evictions
    // (NeverDrop never evicts).
    // -----------------------------------------------------------------
    assert_eq!(
        tally.enqueue_dropped[p0_idx], 0,
        "P0Control class registered {} drops under burst load — \
         NeverDrop invariant violated",
        tally.enqueue_dropped[p0_idx],
    );
    assert_eq!(
        tally.enqueue_refused[p0_idx], 0,
        "P0Control class was REFUSED {} time(s) — the P0 queue went full \
         under burst load; control packets are being lost",
        tally.enqueue_refused[p0_idx],
    );
    assert!(
        tally.drained[p0_idx] >= 1,
        "CONGESTION packet did not reach the receiver (P0Control drained \
         count = {}); P0 carve-out is broken under burst load",
        tally.drained[p0_idx],
    );

    // -----------------------------------------------------------------
    // Assertion 4: sfu_dropped_total{reason="layer_budget"} incremented.
    //
    // The p5-10 per-class priority-queue metric has not landed in this
    // tree (no `sfu_priority_dropped_total` or equivalent exists in
    // `metrics.rs`); the bead explicitly accepts the per-class
    // equivalent OR the layer_budget counter. Layer_budget is what the
    // 1000 kbps clamp + p4-7 filter advance, and it is the canonical
    // "priority-aware drop" counter in this tree today.
    // -----------------------------------------------------------------
    assert!(
        layer_budget_delta > 0.0,
        "sfu_dropped_total{{reason=\"layer_budget\"}} did not advance \
         (delta = {}). The forwarder's layer-budget filter did not engage \
         under the 1000 kbps clamp — the close-gate isn't validating \
         what it thinks it is.",
        layer_budget_delta,
    );
}
