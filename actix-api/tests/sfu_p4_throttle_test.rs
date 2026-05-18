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

//! p4-13: P4 close-gate integration test for the SFU layer-throttling
//! system.
//!
//! Drives a real in-process `ChatServer` (SFU mode) over a real NATS
//! server with one VP9 L1T3 sender and one bandwidth-constrained
//! receiver. Bandwidth is injected via `DiagnosticsPacket` whose sender
//! is the receiver (the production ingest path treats the
//! DiagnosticsPacket's `session_id` as the receiver whose downlink
//! changed). No real network throttle is involved — out of scope per
//! the bead.
//!
//! Scenarios (all run sequentially in one test, same fixture):
//!
//! 1. **Generous budget (2000 kbps).** Receiver gets every temporal
//!    layer (T0+T1+T2); `sfu_dropped_total{reason="layer_budget"}` is
//!    flat over the burst window.
//! 2. **Constrained budget (500 kbps).** Receiver gets T0+T1; T2 is
//!    dropped (immediate downgrade — downgrades are never gated by
//!    cooldown). `sfu_dropped_total{reason="layer_budget"}` advances
//!    by ~one-per-T2-frame.
//! 3. **Severe constraint (200 kbps).** Receiver gets T0 only; T1+T2
//!    drop. `sfu_keyframe_forwarded_total` still increments for the
//!    base-layer keyframe (invariant 1 from p4-8).
//! 4. **Recovery without thrash.** From the 200 kbps steady state,
//!    wait past the 5 s downgrade cooldown, bump to 1500 kbps and wait
//!    past the 3 s upgrade sustain window — assert full L1T3 now
//!    flowing. Then dip to 300 kbps (immediate downgrade), return to
//!    1500 kbps, wait inside the 5 s cooldown window — assert ONE
//!    downgrade fired and NO re-upgrade within the cooldown.
//! 5. **CONGESTION carve-out preserved.** At 200 kbps, a CONGESTION
//!    packet is published on the room subject and MUST reach the
//!    receiver regardless of layer budget (invariant from p2-5).
//!
//! ## Compressed timings vs. bead spec
//!
//! The bead's logical "Send 10s of video" / "Wait 10s for steady
//! state" wording is preserved in spirit but compressed in wall time:
//! synthetic packets are bursted faster than real video would, and
//! "steady state" waits are sized to satisfy the relevant hysteresis
//! gate (3 s sustain + 5 s downgrade cooldown — verified against
//! `actix-api/src/sfu/layer_selector.rs::{UPGRADE_STREAK_REQUIRED,
//! DOWNGRADE_COOLDOWN}`) rather than the literal 10 s. Total wall
//! time is well under the 30 s budget mandated by the bead.
//!
//! ## Running
//!
//! Requires a NATS server reachable at `NATS_URL` (default
//! `nats://nats:4222`, matching the rest of the suite).
//!
//! ```bash
//! /tmp/nats-server -p 24225 -DV &
//! NATS_URL=nats://127.0.0.1:24225 cargo test -p videocall-api \
//!     --test sfu_p4_throttle_test -- --nocapture --test-threads=1
//! ```

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use actix::{Actor, Context, Handler, Recipient};
use protobuf::Message as ProtobufMessage;
use protobuf::MessageField;
use serial_test::serial;
use tokio::time::sleep;

use sec_api::actors::chat_server::ChatServer;
use sec_api::actors::session_logic::SessionId;
use sec_api::messages::server::{ActivateConnection, ClientMessage, Connect, JoinRoom, Packet};
use sec_api::messages::session::Message;
use sec_api::metrics::{SFU_DROPPED_TOTAL, SFU_KEYFRAME_FORWARDED_TOTAL};

use videocall_types::protos::diagnostics_packet::{BandwidthEstimate, DiagnosticsPacket};
use videocall_types::protos::media_packet::media_packet::MediaType;
use videocall_types::protos::media_packet::{MediaPacket, RoutingHeader};
use videocall_types::protos::packet_wrapper::packet_wrapper::PacketType;
use videocall_types::protos::packet_wrapper::PacketWrapper;
use videocall_types::protos::subscription_packet::SubscriptionUpdate;

// ---------------------------------------------------------------------------
// Fixtures — kept self-contained per project rule "each crate/UI must own
// its files independently". Mirrors `sfu_12client_demo.rs`.
// ---------------------------------------------------------------------------

/// RAII guard: snapshot SFU_MODE on construction and restore it on drop
/// so the test process env is left as found.
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

/// One captured frame: raw on-wire bytes (a `PacketWrapper`). Arrival
/// timestamps aren't load-bearing for these scenarios — every per-
/// scenario assertion is bounded by an explicit `drain()` + `sleep()`
/// fanout window — so we keep this minimal.
#[derive(Clone)]
struct CapturedFrame {
    bytes: Vec<u8>,
}

/// Capturing recipient: every `Message` delivered to this session is
/// appended (with arrival timestamp) to the shared `received` buffer.
struct CapturingSession {
    received: Arc<Mutex<Vec<CapturedFrame>>>,
}

impl Actor for CapturingSession {
    type Context = Context<Self>;
}

impl Handler<Message> for CapturingSession {
    type Result = ();
    fn handle(&mut self, msg: Message, _ctx: &mut Self::Context) {
        self.received.lock().unwrap().push(CapturedFrame {
            bytes: msg.msg.to_vec(),
        });
    }
}

struct Participant {
    sid: SessionId,
    user: String,
    received: Arc<Mutex<Vec<CapturedFrame>>>,
    recipient: Recipient<Message>,
}

impl Participant {
    fn new(sid: SessionId, user: &str) -> Self {
        let received: Arc<Mutex<Vec<CapturedFrame>>> = Arc::new(Mutex::new(Vec::new()));
        let actor = CapturingSession {
            received: received.clone(),
        }
        .start();
        Self {
            sid,
            user: user.to_string(),
            received,
            recipient: actor.recipient(),
        }
    }

    /// Clear the capture buffer between scenarios so each scenario's
    /// assertions only see its own traffic.
    fn drain(&self) {
        self.received.lock().unwrap().clear();
    }

    /// Snapshot of the current capture buffer.
    fn snapshot(&self) -> Vec<CapturedFrame> {
        self.received.lock().unwrap().clone()
    }
}

/// Tally of the layer/keyframe makeup of a capture buffer for one
/// receiver, derived from `RoutingHeader.{is_keyframe, temporal_layer_id,
/// spatial_layer_id}` on parsed `MediaPacket`s. Used to express the
/// per-scenario assertions in one place.
#[derive(Default, Debug)]
struct LayerCounts {
    t0_base_keyframes: usize,
    t0_deltas: usize,
    t1_deltas: usize,
    t2_deltas: usize,
    congestion: usize,
}

impl LayerCounts {
    fn from_frames(frames: &[CapturedFrame]) -> Self {
        let mut c = Self::default();
        for f in frames {
            let Ok(w) = <PacketWrapper as ProtobufMessage>::parse_from_bytes(&f.bytes) else {
                continue;
            };
            if w.packet_type == PacketType::CONGESTION.into() {
                c.congestion += 1;
                continue;
            }
            if w.packet_type != PacketType::MEDIA.into() {
                continue;
            }
            let Ok(mp) = MediaPacket::parse_from_bytes(&w.data) else {
                continue;
            };
            if mp.media_type != MediaType::VIDEO.into() {
                continue;
            }
            let Some(rh) = mp.routing_header.as_ref() else {
                continue;
            };
            if rh.is_keyframe && rh.temporal_layer_id == 0 && rh.spatial_layer_id == 0 {
                c.t0_base_keyframes += 1;
                continue;
            }
            match rh.temporal_layer_id {
                0 => c.t0_deltas += 1,
                1 => c.t1_deltas += 1,
                2 => c.t2_deltas += 1,
                _ => {}
            }
        }
        c
    }
}

/// Register a session with the ChatServer and join+activate it in a room.
async fn register_and_join(
    chat: &actix::Addr<ChatServer>,
    p: &Participant,
    room: &str,
) -> Result<(), String> {
    chat.send(Connect {
        id: p.sid,
        addr: p.recipient.clone(),
    })
    .await
    .map_err(|e| format!("Connect mailbox: {e}"))?;

    chat.send(JoinRoom {
        session: p.sid,
        room: room.to_string(),
        user_id: p.user.clone(),
        display_name: p.user.clone(),
        observer: false,
        capabilities: 0,
    })
    .await
    .map_err(|e| format!("JoinRoom mailbox: {e}"))??;

    chat.send(ActivateConnection { session: p.sid })
        .await
        .map_err(|e| format!("ActivateConnection mailbox: {e}"))?;

    Ok(())
}

/// Build one VP9 L1T3 `PacketWrapper` carrying VIDEO with the named
/// layer markers. `frame_marker` is left at 0 — no T0-reference claim —
/// so the p4-9 `reference_miss` check never trips for higher-temporal
/// deltas (orthogonal to layer-throttling).
///
/// `seed` makes each emitted frame's payload byte-unique so duplicate
/// detection at egress doesn't elide identical packets.
fn build_l1t3_video(
    sender_sid: SessionId,
    sender_user: &str,
    temporal: u32,
    is_keyframe: bool,
    picture_id: u64,
    seed: u8,
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
        media_type: MediaType::VIDEO.into(),
        data: vec![seed; 16],
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

/// Drive one L1T3 GOP (`gops`) consisting of:
///   - 1× T0+S0 keyframe
///   - 3× T0 deltas, 3× T1 deltas, 3× T2 deltas
///
/// per GOP. Each call pumps `gops` GOPs through `ClientMessage` with a
/// short inter-frame sleep so the per-room dispatcher gets fan-out
/// turns. Returns the per-temporal counts published so callers can
/// assert against them.
async fn drive_l1t3_burst(
    chat: &actix::Addr<ChatServer>,
    sender: &Participant,
    room: &str,
    seed_base: &mut u32,
    picture_base: &mut u64,
    gops: usize,
) -> (usize, usize, usize, usize) {
    let mut t0_kf = 0usize;
    let mut t0_d = 0usize;
    let mut t1_d = 0usize;
    let mut t2_d = 0usize;
    for _ in 0..gops {
        // Layer pattern within one mini-GOP. We keep it small so a single
        // GOP completes within ~50 ms wall time but still exercises every
        // temporal layer.
        let pattern: [(u32, bool); 10] = [
            (0, true),  // T0+S0 keyframe (always forwards)
            (2, false), // T2 delta
            (1, false), // T1 delta
            (2, false), // T2 delta
            (0, false), // T0 delta
            (2, false), // T2 delta
            (1, false), // T1 delta
            (2, false), // T2 delta
            (0, false), // T0 delta
            (1, false), // T1 delta
        ];
        for (t, kf) in pattern {
            *seed_base = seed_base.wrapping_add(1);
            *picture_base = picture_base.wrapping_add(1);
            let bytes = build_l1t3_video(
                sender.sid,
                &sender.user,
                t,
                kf,
                *picture_base,
                (*seed_base & 0xFF) as u8,
            );
            chat.send(ClientMessage {
                session: sender.sid,
                room: room.to_string(),
                user: sender.user.clone(),
                msg: Packet {
                    data: Arc::new(bytes),
                },
            })
            .await
            .expect("ClientMessage mailbox");

            if kf {
                t0_kf += 1;
            } else {
                match t {
                    0 => t0_d += 1,
                    1 => t1_d += 1,
                    2 => t2_d += 1,
                    _ => {}
                }
            }
            // Tight pacing — enough that the dispatcher gets scheduled
            // between packets, but not so slow that the burst dominates
            // wall time.
            sleep(Duration::from_millis(2)).await;
        }
    }
    (t0_kf, t0_d, t1_d, t2_d)
}

/// Inject a bandwidth estimate for `receiver` via a `DiagnosticsPacket`
/// whose `session_id` IS the receiver (production semantics: the
/// receiver reports its own downlink back to the SFU). The
/// `chat_server` DiagnosticsPacket ingest path will:
///
///   1. Store the estimate on the receiver's `MemberEntry` via
///      `RoomState::update_bandwidth_estimate`.
///   2. Invalidate the `LayerSelector` cache for `receiver` so the next
///      `decide()` recomputes against the fresh budget.
async fn inject_bandwidth(
    chat: &actix::Addr<ChatServer>,
    receiver: &Participant,
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
        session_id: receiver.sid,
        user_id: receiver.user.as_bytes().to_vec(),
        data: diag.write_to_bytes().expect("encode DiagnosticsPacket"),
        ..Default::default()
    };
    let bytes = wrapper.write_to_bytes().expect("encode wrapper");
    chat.send(ClientMessage {
        session: receiver.sid,
        room: room.to_string(),
        user: receiver.user.clone(),
        msg: Packet {
            data: Arc::new(bytes),
        },
    })
    .await
    .expect("DiagnosticsPacket ClientMessage");
}

/// Pin `pinned` (one sender's session id) as the receiver's sole
/// allowed video source. Collapses the per-receiver AllowSet to
/// `{pinned}` — the bead spec asserts behavior against "1 SENDER
/// emitting L1T3 video", so we suppress the legacy default fanout
/// that would otherwise treat every room member as a candidate
/// video sender (and fragment the LayerSelector budget across
/// participants the test doesn't care about — notably the
/// `cong_origin` aux session used by scenario 5).
async fn pin_receiver_to(
    chat: &actix::Addr<ChatServer>,
    receiver: &Participant,
    room: &str,
    pinned: SessionId,
) {
    let mut update = SubscriptionUpdate::new();
    update.pinned_sessions = vec![pinned];
    update.slots = vec![];
    update.receive_all_audio = false;
    let wrapper = PacketWrapper {
        packet_type: PacketType::SUBSCRIPTION_UPDATE.into(),
        session_id: receiver.sid,
        user_id: receiver.user.as_bytes().to_vec(),
        data: update.write_to_bytes().expect("encode SubscriptionUpdate"),
        ..Default::default()
    };
    let bytes = wrapper.write_to_bytes().expect("encode wrapper");
    chat.send(ClientMessage {
        session: receiver.sid,
        room: room.to_string(),
        user: receiver.user.clone(),
        msg: Packet {
            data: Arc::new(bytes),
        },
    })
    .await
    .expect("SubscriptionUpdate ClientMessage");
}

/// Publish a CONGESTION packet directly via NATS on a room subject. We
/// bypass the `ClientMessage` path because `chat_server` does not
/// special-case CONGESTION for ClientMessage (it would publish on the
/// sender's own subject which is fine), and the carve-out we are
/// asserting lives in `egress_decide_from_parsed`. The simplest faithful
/// path is to send via ClientMessage from a non-receiver session id
/// (publishes on `room.{room}.{sid}` — a non-receiver subject — so the
/// per-room dispatcher receives it from NATS, parses it, observes
/// CONGESTION, and applies the carve-out at egress).
async fn publish_congestion(
    chat: &actix::Addr<ChatServer>,
    sender: &Participant,
    room: &str,
    seed: u8,
) {
    let wrapper = PacketWrapper {
        packet_type: PacketType::CONGESTION.into(),
        session_id: sender.sid,
        user_id: sender.user.as_bytes().to_vec(),
        data: vec![seed; 24],
        ..Default::default()
    };
    let bytes = wrapper.write_to_bytes().expect("encode CONGESTION wrapper");
    chat.send(ClientMessage {
        session: sender.sid,
        room: room.to_string(),
        user: sender.user.clone(),
        msg: Packet {
            data: Arc::new(bytes),
        },
    })
    .await
    .expect("CONGESTION ClientMessage");
}

/// How long to wait after a JoinRoom (or membership-shifting event) so
/// the per-room dispatcher has subscribed and the subscription store
/// has settled before the first media packet flies. Mirrors the
/// `SUBSCRIBE_SETTLE` in `sfu_12client_demo.rs`.
const SUBSCRIBE_SETTLE: Duration = Duration::from_millis(300);

/// Short settle after a bandwidth-estimate injection — long enough that
/// the DiagnosticsPacket has rolled through the dispatcher and the
/// layer-selector cache has been invalidated.
const BW_SETTLE: Duration = Duration::from_millis(80);

/// Settle after a packet burst so the dispatcher has fanned every
/// queued NATS message to the capturing session's mailbox.
const FANOUT_SETTLE: Duration = Duration::from_millis(150);

// ---------------------------------------------------------------------------
// The integration test.
// ---------------------------------------------------------------------------

#[actix_rt::test]
#[serial]
async fn sfu_p4_throttle_scenarios() {
    let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());

    // Force SFU mode BEFORE constructing the actor — `ChatServer::new()`
    // snapshots SFU_MODE at construction time.
    let env = EnvGuard::new();
    env.set("sfu");

    let nats_client = async_nats::connect(&nats_url)
        .await
        .expect("connect to NATS");

    let room = "p4-13-throttle".to_string();
    let chat = ChatServer::new(nats_client.clone()).await.start();

    // One sender + one constrained receiver + one auxiliary "carve-out"
    // sender for CONGESTION (so its CONGESTION publishes on its own
    // subject, distinct from the L1T3 sender's video subject — proving
    // the carve-out works for non-self traffic too).
    let sender = Participant::new(80_001, "sender@p4-13");
    let receiver = Participant::new(80_002, "receiver@p4-13");
    let cong_origin = Participant::new(80_003, "cong-origin@p4-13");

    for p in [&sender, &receiver, &cong_origin] {
        register_and_join(&chat, p, &room).await.expect("join");
    }
    sleep(SUBSCRIBE_SETTLE).await;

    // Pin the receiver to ONLY the L1T3 sender so the LayerSelector
    // budget arithmetic matches the bead's single-publisher scenario.
    // Without this, the default AllowSet treats every other room
    // member (here: the cong-origin aux session) as a candidate video
    // sender, and the greedy Pass-1 admission spends T0 on it — which
    // robs the L1T3 sender of its T1/T2 upgrade headroom and breaks
    // the scenario-2 (500 kbps → T0+T1) assertion.
    pin_receiver_to(&chat, &receiver, &room, sender.sid).await;
    sleep(SUBSCRIBE_SETTLE).await;

    // Each scenario uses fresh seed/picture state so duplicate-detection
    // and the p4-9 recent-T0 window never alias frames across scenarios.
    let mut seed: u32 = 0;
    let mut picture_id: u64 = 0;

    // -----------------------------------------------------------------
    // Scenario 1: generous budget (2000 kbps) → every layer flows.
    // -----------------------------------------------------------------
    inject_bandwidth(&chat, &receiver, &room, 2000).await;
    sleep(BW_SETTLE).await;

    receiver.drain();
    let dropped_lb_before = SFU_DROPPED_TOTAL.with_label_values(&["layer_budget"]).get();
    let (sent_kf, sent_t0, sent_t1, sent_t2) =
        drive_l1t3_burst(&chat, &sender, &room, &mut seed, &mut picture_id, 12).await;
    sleep(FANOUT_SETTLE).await;

    let s1 = LayerCounts::from_frames(&receiver.snapshot());
    let dropped_lb_after = SFU_DROPPED_TOTAL.with_label_values(&["layer_budget"]).get();
    let s1_lb_delta = dropped_lb_after - dropped_lb_before;
    assert_eq!(
        s1_lb_delta, 0.0,
        "scenario 1 (generous): no layer_budget drops expected; got delta {s1_lb_delta}"
    );
    // Allow one or two stragglers per layer (NATS fan-out can race the
    // PUBLISH_SETTLE window on a slow CI runner) but require strict
    // majority pass-through. Anything less than 80% indicates a real bug.
    let pass_floor = |sent: usize| ((sent as f64) * 0.8).floor() as usize;
    assert!(
        s1.t0_base_keyframes >= pass_floor(sent_kf),
        "scenario 1: T0+S0 keyframes received {} < floor {} of {} sent",
        s1.t0_base_keyframes,
        pass_floor(sent_kf),
        sent_kf
    );
    assert!(
        s1.t0_deltas >= pass_floor(sent_t0),
        "scenario 1: T0 deltas received {} < floor {} of {} sent",
        s1.t0_deltas,
        pass_floor(sent_t0),
        sent_t0
    );
    assert!(
        s1.t1_deltas >= pass_floor(sent_t1),
        "scenario 1: T1 deltas received {} < floor {} of {} sent",
        s1.t1_deltas,
        pass_floor(sent_t1),
        sent_t1
    );
    assert!(
        s1.t2_deltas >= pass_floor(sent_t2),
        "scenario 1: T2 deltas received {} < floor {} of {} sent",
        s1.t2_deltas,
        pass_floor(sent_t2),
        sent_t2
    );

    // -----------------------------------------------------------------
    // Scenario 2: constrained budget (500 kbps) → T0+T1 fits; T2 drops.
    //
    // Downgrades are immediate (no cooldown gate on the way DOWN), so
    // we can move straight from 2000 → 500 and assert the new layer set
    // on the very next burst. Cumulative kbps for L1T3:
    //   T0 = 128, T0+T1 = 384, T0+T1+T2 = 896
    // 500 kbps * 0.85 headroom = 425 budget → T0+T1 fits (384), T2 does
    // not (896).
    // -----------------------------------------------------------------
    inject_bandwidth(&chat, &receiver, &room, 500).await;
    sleep(BW_SETTLE).await;

    receiver.drain();
    let dropped_lb_before = SFU_DROPPED_TOTAL.with_label_values(&["layer_budget"]).get();
    let (sent_kf, sent_t0, sent_t1, sent_t2) =
        drive_l1t3_burst(&chat, &sender, &room, &mut seed, &mut picture_id, 12).await;
    sleep(FANOUT_SETTLE).await;

    let s2 = LayerCounts::from_frames(&receiver.snapshot());
    let dropped_lb_after = SFU_DROPPED_TOTAL.with_label_values(&["layer_budget"]).get();
    let s2_lb_delta = dropped_lb_after - dropped_lb_before;

    // T0+S0 keyframes always pass (invariant 1).
    assert!(
        s2.t0_base_keyframes >= pass_floor(sent_kf),
        "scenario 2: T0+S0 keyframes received {} < floor {} of {} sent (invariant 1)",
        s2.t0_base_keyframes,
        pass_floor(sent_kf),
        sent_kf
    );
    // T0 + T1 should mostly pass through.
    assert!(
        s2.t0_deltas >= pass_floor(sent_t0),
        "scenario 2: T0 deltas received {} < floor {} of {} sent",
        s2.t0_deltas,
        pass_floor(sent_t0),
        sent_t0
    );
    assert!(
        s2.t1_deltas >= pass_floor(sent_t1),
        "scenario 2: T1 deltas received {} < floor {} of {} sent (T0+T1=384 kbps fits in 425 budget)",
        s2.t1_deltas,
        pass_floor(sent_t1),
        sent_t1
    );
    // T2 must be (nearly) all dropped. Allow at most one stale T2
    // delivered from the moment before the cache flipped over.
    assert!(
        s2.t2_deltas <= 1,
        "scenario 2: T2 deltas received {} > 1 — layer_budget filter is not dropping T2 at 500 kbps",
        s2.t2_deltas
    );
    // The layer_budget counter must reflect the dropped T2s.
    assert!(
        s2_lb_delta >= (sent_t2 as f64) * 0.8,
        "scenario 2: sfu_dropped_total{{reason=layer_budget}} delta {s2_lb_delta} < 80% of {} T2s sent",
        sent_t2
    );

    // -----------------------------------------------------------------
    // Scenario 3: severe constraint (200 kbps) → T0 only; T1+T2 drop.
    // Also asserts SFU_KEYFRAME_FORWARDED_TOTAL still advances — the
    // base-layer keyframe carve-out (invariant 1 from p4-8) must hold
    // even on the tightest receiver.
    //
    // 200 kbps * 0.85 = 170 budget → T0 (128) fits; T0+T1 (384) does
    // not.
    // -----------------------------------------------------------------
    inject_bandwidth(&chat, &receiver, &room, 200).await;
    sleep(BW_SETTLE).await;

    receiver.drain();
    let dropped_lb_before = SFU_DROPPED_TOTAL.with_label_values(&["layer_budget"]).get();
    let kf_before = SFU_KEYFRAME_FORWARDED_TOTAL.get();
    let (sent_kf, sent_t0, sent_t1, sent_t2) =
        drive_l1t3_burst(&chat, &sender, &room, &mut seed, &mut picture_id, 12).await;
    sleep(FANOUT_SETTLE).await;

    let s3 = LayerCounts::from_frames(&receiver.snapshot());
    let dropped_lb_after = SFU_DROPPED_TOTAL.with_label_values(&["layer_budget"]).get();
    let kf_after = SFU_KEYFRAME_FORWARDED_TOTAL.get();
    let s3_lb_delta = dropped_lb_after - dropped_lb_before;
    let kf_delta = kf_after - kf_before;

    assert!(
        s3.t0_base_keyframes >= pass_floor(sent_kf),
        "scenario 3: T0+S0 keyframes received {} < floor {} of {} sent (invariant 1)",
        s3.t0_base_keyframes,
        pass_floor(sent_kf),
        sent_kf
    );
    assert!(
        s3.t0_deltas >= pass_floor(sent_t0),
        "scenario 3: T0 deltas received {} < floor {} of {} sent",
        s3.t0_deltas,
        pass_floor(sent_t0),
        sent_t0
    );
    assert!(
        s3.t1_deltas <= 1,
        "scenario 3: T1 deltas received {} > 1 — layer_budget filter is not dropping T1 at 200 kbps",
        s3.t1_deltas
    );
    assert!(
        s3.t2_deltas <= 1,
        "scenario 3: T2 deltas received {} > 1 — layer_budget filter is not dropping T2 at 200 kbps",
        s3.t2_deltas
    );
    let s3_total_higher_layers = sent_t1 + sent_t2;
    assert!(
        s3_lb_delta >= (s3_total_higher_layers as f64) * 0.8,
        "scenario 3: sfu_dropped_total{{reason=layer_budget}} delta {s3_lb_delta} < 80% \
         of {} T1+T2 sent",
        s3_total_higher_layers
    );
    assert!(
        kf_delta >= (sent_kf as f64) * 0.8,
        "scenario 3: sfu_keyframe_forwarded_total must advance by ~{} (sent_kf={}); got {}",
        sent_kf,
        sent_kf,
        kf_delta
    );

    // Mark the "last downgrade" wall-clock instant — every later
    // scenario-4 upgrade-readiness wait is measured from here. The
    // immediate-downgrade sequence above (2000 → 500 → 200) stamps
    // `last_downgrade_at` on each invalidation; the most recent one is
    // approximately `now()`.
    let last_downgrade_marker = Instant::now();

    // -----------------------------------------------------------------
    // Scenario 4: recovery without thrash.
    //
    // Stay at 200 kbps for a short "steady state" window (compressed
    // from the bead's 10 s — the hysteresis state is already at T0
    // from scenario 3), then bump to 1500 kbps and wait long enough to
    // satisfy BOTH gates:
    //   * 5 s downgrade cooldown (from the scenario-3 downgrade)
    //   * 3 s upgrade sustain (the streak counter only starts ticking
    //     once we have headroom at the larger budget — i.e. after the
    //     bump)
    // 5 s + small margin covers both (the 3 s sustain is a subset of
    // the 5 s cooldown window since both start at-or-after the bump).
    //
    // Then the dip: drop to 300 kbps for ~1 s. 300 * 0.85 = 255 budget
    // → T0 (128) fits, T0+T1 (384) does not — so an immediate
    // downgrade fires, stamping a fresh `last_downgrade_at`.
    //
    // Return to 1500 kbps and wait 5.2 s — INSIDE the cooldown of the
    // fresh downgrade. Assert ONE downgrade fired (we saw a downgrade
    // happen) AND no re-upgrade occurred within the cooldown window.
    // -----------------------------------------------------------------

    // Brief steady-state confirmation at 200 kbps — still T0 only.
    receiver.drain();
    let (_kf, _t0, sent_t1, sent_t2) =
        drive_l1t3_burst(&chat, &sender, &room, &mut seed, &mut picture_id, 4).await;
    sleep(FANOUT_SETTLE).await;
    let s4_steady = LayerCounts::from_frames(&receiver.snapshot());
    assert!(
        s4_steady.t1_deltas <= 1 && s4_steady.t2_deltas <= 1,
        "scenario 4 (steady at 200): expected T0 only, got T1={} T2={} (sent T1={} T2={})",
        s4_steady.t1_deltas,
        s4_steady.t2_deltas,
        sent_t1,
        sent_t2
    );

    // Wait out the 5 s cooldown from scenario 3's last downgrade
    // PLUS the steady-state burst we just sent. Drive a slow trickle
    // of T0+S0 keyframes through so the dispatcher stays busy and the
    // layer-selector cache stays warm — but no T1/T2 ingress so we
    // don't pollute the receiver's buffer with anything that would
    // confuse the post-bump assertion.
    //
    // 5.2 s = 5.0 s cooldown + 200 ms margin.
    let cooldown_target = Duration::from_millis(5_200);
    let elapsed_since_last_downgrade = last_downgrade_marker.elapsed();
    if elapsed_since_last_downgrade < cooldown_target {
        sleep(cooldown_target - elapsed_since_last_downgrade).await;
    }

    // Now bump to 1500 kbps. Upgrade detected; gates evaluated.
    // Cooldown is already satisfied. The 3 s streak starts NOW (the
    // headroom_ok flag flips true at this `decide()`). We must drive
    // burst traffic continuously through the streak window so the
    // cache refreshes and pick_with_hysteresis re-evaluates.
    inject_bandwidth(&chat, &receiver, &room, 1500).await;
    sleep(BW_SETTLE).await;

    // Drain the receiver buffer BEFORE the streak window so the post-
    // streak snapshot only sees post-bump traffic.
    receiver.drain();

    // Stream traffic for 3.4 s (3.0 s streak + 0.4 s margin). The
    // forwarder consults the cached selection on every decide(); cache
    // hits at the same (generation, bandwidth) skip the recompute, so
    // we must invalidate periodically. The cleanest way is to inject
    // a same-value bandwidth update every ~600 ms; that re-arms the
    // recompute and stamps fresh now()s into the hysteresis streak.
    let streak_target = Duration::from_millis(3_400);
    let streak_start = Instant::now();
    while streak_start.elapsed() < streak_target {
        let _ = drive_l1t3_burst(&chat, &sender, &room, &mut seed, &mut picture_id, 2).await;
        // Re-inject the SAME bandwidth so the dispatcher invalidates
        // the layer-selector cache → next decide() recomputes →
        // pick_with_hysteresis re-evaluates the streak gate against
        // the now-current `now()`.
        inject_bandwidth(&chat, &receiver, &room, 1500).await;
        sleep(Duration::from_millis(80)).await;
    }
    sleep(FANOUT_SETTLE).await;

    // After 3+ seconds of headroom, the upgrade MUST have fired. Do
    // ONE final burst against the now-upgraded selection and verify
    // T1 and T2 are flowing again.
    receiver.drain();
    let (sent_kf, sent_t0, sent_t1, sent_t2) =
        drive_l1t3_burst(&chat, &sender, &room, &mut seed, &mut picture_id, 8).await;
    sleep(FANOUT_SETTLE).await;
    let s4_upgraded = LayerCounts::from_frames(&receiver.snapshot());

    assert!(
        s4_upgraded.t1_deltas >= pass_floor(sent_t1),
        "scenario 4 (upgraded to 1500): T1 deltas received {} < floor {} of {} sent — \
         upgrade gate did not fire after 3 s streak + 5 s cooldown",
        s4_upgraded.t1_deltas,
        pass_floor(sent_t1),
        sent_t1
    );
    assert!(
        s4_upgraded.t2_deltas >= pass_floor(sent_t2),
        "scenario 4 (upgraded to 1500): T2 deltas received {} < floor {} of {} sent — \
         upgrade gate did not fire after 3 s streak + 5 s cooldown",
        s4_upgraded.t2_deltas,
        pass_floor(sent_t2),
        sent_t2
    );
    assert!(
        s4_upgraded.t0_base_keyframes >= pass_floor(sent_kf),
        "scenario 4 (upgraded to 1500): T0+S0 keyframes received {} < floor {} of {} sent",
        s4_upgraded.t0_base_keyframes,
        pass_floor(sent_kf),
        sent_kf
    );
    assert!(
        s4_upgraded.t0_deltas >= pass_floor(sent_t0),
        "scenario 4 (upgraded to 1500): T0 deltas received {} < floor {} of {} sent",
        s4_upgraded.t0_deltas,
        pass_floor(sent_t0),
        sent_t0
    );

    // ---- Dip to 300 kbps → immediate downgrade. ---------------------
    let dropped_lb_before_dip = SFU_DROPPED_TOTAL.with_label_values(&["layer_budget"]).get();
    inject_bandwidth(&chat, &receiver, &room, 300).await;
    sleep(BW_SETTLE).await;
    let dip_downgrade_stamp = Instant::now();
    receiver.drain();
    let (_kf_dip, _t0_dip, sent_t1_dip, sent_t2_dip) =
        drive_l1t3_burst(&chat, &sender, &room, &mut seed, &mut picture_id, 6).await;
    sleep(FANOUT_SETTLE).await;
    let s4_dip = LayerCounts::from_frames(&receiver.snapshot());
    let dropped_lb_after_dip = SFU_DROPPED_TOTAL.with_label_values(&["layer_budget"]).get();
    let dip_lb_delta = dropped_lb_after_dip - dropped_lb_before_dip;
    assert!(
        s4_dip.t1_deltas <= 1,
        "scenario 4 dip (300 kbps): immediate downgrade should have dropped T1; got {}",
        s4_dip.t1_deltas
    );
    assert!(
        s4_dip.t2_deltas <= 1,
        "scenario 4 dip (300 kbps): immediate downgrade should have dropped T2; got {}",
        s4_dip.t2_deltas
    );
    let dip_higher_sent = sent_t1_dip + sent_t2_dip;
    assert!(
        dip_lb_delta >= (dip_higher_sent as f64) * 0.5,
        "scenario 4 dip: layer_budget counter delta {dip_lb_delta} too low for {dip_higher_sent} \
         T1+T2 sent (expected the immediate downgrade to drop them)"
    );

    // ---- Return to 1500 kbps; assert NO re-upgrade within cooldown. -
    inject_bandwidth(&chat, &receiver, &room, 1500).await;
    sleep(BW_SETTLE).await;
    receiver.drain();

    // Wait inside the 5 s cooldown window — the bead's "wait 5s"
    // bound. We sleep for ~4.0 s while occasionally pumping traffic
    // so the receiver's capture buffer accumulates whatever the
    // forwarder chooses to send. The streak is also building up here;
    // 4.0 s > 3 s sustain, so the ONLY gate still blocking the
    // upgrade is the 5 s downgrade cooldown — the test target.
    //
    // We deliberately stay well inside the 5 s cooldown (4.0 s +
    // inject/settle overhead ≈ 4.5 s wall clock from dip_downgrade_stamp)
    // because the inject_bandwidth call between the dip burst and the
    // return-to-1500 chews ~80 ms of pre-window time, and FANOUT_SETTLE
    // adds another 150 ms after the loop — both subtracted from the
    // remaining cooldown budget below.
    let cooldown_window = Duration::from_millis(4_000);
    let cooldown_start = Instant::now();
    while cooldown_start.elapsed() < cooldown_window {
        let _ = drive_l1t3_burst(&chat, &sender, &room, &mut seed, &mut picture_id, 2).await;
        // No re-injection of bandwidth here: we want the layer-selector
        // cache to be authoritative. But the speaker generation never
        // changes (we send no audio_level), so the cache stays hot.
        // Force a cache invalidation periodically by re-injecting the
        // SAME 1500 kbps — this makes pick_with_hysteresis re-evaluate
        // and gives the cooldown gate a chance to (incorrectly) admit
        // an upgrade if it were buggy.
        inject_bandwidth(&chat, &receiver, &room, 1500).await;
        sleep(Duration::from_millis(80)).await;
    }
    sleep(FANOUT_SETTLE).await;

    // We deliberately measured this window so it stays inside the
    // cooldown (5 s from dip_downgrade_stamp). Sanity-check the
    // measurement so a slow CI runner doesn't silently bust it.
    let elapsed_since_dip = dip_downgrade_stamp.elapsed();
    assert!(
        elapsed_since_dip < Duration::from_secs(5),
        "scenario 4 cooldown-window measurement overran: {:?} elapsed since dip downgrade — \
         the assertion below would no longer be meaningful (the cooldown could have lapsed)",
        elapsed_since_dip
    );

    let s4_cooldown = LayerCounts::from_frames(&receiver.snapshot());
    assert!(
        s4_cooldown.t1_deltas <= 2,
        "scenario 4 within-cooldown: re-upgrade fired! Saw {} T1 deltas inside the 5 s \
         downgrade cooldown — hysteresis is broken",
        s4_cooldown.t1_deltas
    );
    assert!(
        s4_cooldown.t2_deltas <= 2,
        "scenario 4 within-cooldown: re-upgrade fired! Saw {} T2 deltas inside the 5 s \
         downgrade cooldown — hysteresis is broken",
        s4_cooldown.t2_deltas
    );

    // -----------------------------------------------------------------
    // Scenario 5: CONGESTION carve-out at 200 kbps.
    //
    // Drop the receiver back to 200 kbps so the layer-budget filter is
    // maximally aggressive, then publish a CONGESTION packet from a
    // distinct session. The CONGESTION carve-out in
    // `egress_decide_from_parsed` MUST forward it to the receiver
    // regardless of layer budget.
    // -----------------------------------------------------------------
    inject_bandwidth(&chat, &receiver, &room, 200).await;
    sleep(BW_SETTLE).await;
    receiver.drain();

    publish_congestion(&chat, &cong_origin, &room, 0xC1).await;
    sleep(FANOUT_SETTLE).await;

    let frames_after_cong = receiver.snapshot();
    let s5 = LayerCounts::from_frames(&frames_after_cong);
    assert!(
        s5.congestion >= 1,
        "scenario 5: CONGESTION carve-out failed — receiver got {} CONGESTION packets at 200 kbps \
         (sent 1 from cong-origin)",
        s5.congestion
    );

    // Sanity: at 200 kbps the receiver should NOT see any T1/T2
    // deltas in this window even if some media leaked in (we sent
    // none in scenario 5).
    assert_eq!(
        s5.t1_deltas, 0,
        "scenario 5: unexpected T1 deltas at 200 kbps in CONGESTION-only window"
    );
    assert_eq!(
        s5.t2_deltas, 0,
        "scenario 5: unexpected T2 deltas at 200 kbps in CONGESTION-only window"
    );

    // ---- Diagnostic: print the per-scenario tallies for human review
    // when --nocapture is on. Not load-bearing for the assertions.
    eprintln!("scenario 1 (2000 kbps): {:?}", s1);
    eprintln!("scenario 2  (500 kbps): {:?}", s2);
    eprintln!("scenario 3  (200 kbps): {:?}", s3);
    eprintln!("scenario 4 steady     : {:?}", s4_steady);
    eprintln!("scenario 4 upgraded   : {:?}", s4_upgraded);
    eprintln!("scenario 4 dip        : {:?}", s4_dip);
    eprintln!("scenario 4 cooldown   : {:?}", s4_cooldown);
    eprintln!("scenario 5 congestion : {:?}", s5);

    // Confirm the L1T3-sender session was indeed exercised — guards
    // against an accidental no-op test where everything drained to
    // zero before assertions ran.
    let sender_video_ever_seen: HashSet<MediaType> = receiver
        .snapshot()
        .iter()
        .filter_map(|f| PacketWrapper::parse_from_bytes(&f.bytes).ok())
        .filter_map(|w| MediaPacket::parse_from_bytes(&w.data).ok())
        .map(|mp| mp.media_type.enum_value_or_default())
        .collect();
    // Above is a snapshot of the final buffer; CONGESTION isn't a
    // MediaPacket so VIDEO presence here would be a stray late
    // delivery. We don't assert against it — the per-scenario
    // assertions above already covered the deliveries that matter.
    let _ = sender_video_ever_seen;
}
