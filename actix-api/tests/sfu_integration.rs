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

//! P2-9: SFU_MODE=sfu integration parity test.
//!
//! Drives a real in-process `ChatServer` actor over a real NATS server and
//! compares fan-out across `SFU_MODE=legacy` and `SFU_MODE=sfu` at the
//! actor boundary. Asserts:
//!
//!   * 1:1 and 1:N rooms: every non-sender receives all of the sender's MEDIA
//!     packets; the sender receives none of its own MEDIA echoes.
//!   * Byte-identical parity: each receiver's MEDIA delivery sequence in SFU
//!     mode equals the same sequence in legacy mode (golden trace).
//!   * CONGESTION carve-out: a sender's CONGESTION packet reaches every other
//!     receiver in both modes.
//!   * Metric: `sfu_forwarded_total{packet_type="media"}` increases by the
//!     expected count when in SFU mode (and only then — the legacy path does
//!     not invoke the forwarder).
//!
//! ## File-layout decision
//!
//! Bead `vc-r6n / p2-9` specifies the acceptance command as
//! `cargo test --test sfu_integration`, which mandates an external integration
//! test file. The metric `sec_api::metrics::SFU_FORWARDED_TOTAL` is exposed
//! via a `pub static ref` in a `pub mod`, so it is reachable from this
//! integration crate — no in-crate workaround is required.
//!
//! NOTE: Per the project rule "Each crate/UI must own its files independently"
//! the parity helpers from `src/sfu/tests/forwarder_parity_tests.rs` are
//! re-inlined here rather than shared via a symlink or shared helper file.
//!
//! ## Why a single `#[actix_rt::test]`
//!
//! `SFU_MODE` is process-global env state and `ChatServer::new()` snapshots
//! it at construction. Running each scenario in its own test would multiply
//! actix-runtime startup cost; the bead requires total runtime under 5s, so
//! all scenarios share one test function and one NATS client and use
//! distinct rooms / session IDs for isolation.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use actix::{Actor, Context, Handler, Recipient};
use protobuf::Message as ProtobufMessage;
use serial_test::serial;
use tokio::time::sleep;

use sec_api::actors::chat_server::ChatServer;
use sec_api::actors::packet_handler::PacketKind;
use sec_api::actors::session_logic::SessionId;
use sec_api::messages::server::{ActivateConnection, ClientMessage, Connect, JoinRoom, Packet};
use sec_api::messages::session::Message;
use sec_api::metrics::{SFU_DROPPED_TOTAL, SFU_FORWARDED_TOTAL};

use videocall_types::protos::media_packet::media_packet::MediaType;
use videocall_types::protos::media_packet::MediaPacket;
use videocall_types::protos::packet_wrapper::packet_wrapper::PacketType;
use videocall_types::protos::packet_wrapper::PacketWrapper;
use videocall_types::protos::subscription_packet::SubscriptionUpdate;

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

/// RAII guard that snapshots `SFU_MODE` on construction and restores it on
/// drop, so the test leaves the process env exactly as it found it. Mirrors
/// the pattern in `src/sfu/config.rs`.
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

/// Capturing recipient: every `Message` delivered to this session is appended
/// to the shared `received` buffer so the test can inspect the per-receiver
/// delivery list after fan-out completes.
struct CapturingSession {
    received: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl Actor for CapturingSession {
    type Context = Context<Self>;
}

impl Handler<Message> for CapturingSession {
    type Result = ();
    fn handle(&mut self, msg: Message, _ctx: &mut Self::Context) {
        self.received.lock().unwrap().push(msg.msg.to_vec());
    }
}

/// One participant in a test scenario: a session id + a capture buffer.
struct Participant {
    sid: SessionId,
    user: String,
    received: Arc<Mutex<Vec<Vec<u8>>>>,
    recipient: Recipient<Message>,
}

impl Participant {
    fn new(sid: SessionId, user: &str) -> Self {
        let received: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
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

    /// Filter the captured buffer down to wrappers whose `packet_type` matches.
    /// JoinRoom delivers MEETING_STARTED / PARTICIPANT_JOINED system packets
    /// alongside the media we care about, so callers extract just the media
    /// (or congestion) frames before asserting counts and byte equality.
    fn captured_of(&self, t: PacketType) -> Vec<Vec<u8>> {
        let lock = self.received.lock().unwrap();
        lock.iter()
            .filter(|bytes| {
                <PacketWrapper as ProtobufMessage>::parse_from_bytes(bytes)
                    .map(|w| w.packet_type == t.into())
                    .unwrap_or(false)
            })
            .cloned()
            .collect()
    }
}

/// Build a deterministic serialized MEDIA `PacketWrapper`. Identical to the
/// helper in `src/sfu/tests/forwarder_parity_tests.rs`; inlined per project
/// rule (no symlinks / shared helpers between crates and modules).
fn build_media_payload(sender_sid: SessionId, sender_user: &str, seed: u8) -> Vec<u8> {
    let media = MediaPacket {
        media_type: MediaType::VIDEO.into(),
        // Deterministic 32-byte body so byte-equality assertions are
        // meaningful across modes.
        data: vec![seed; 32],
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

/// vc-9eh: build a deterministic serialized MEDIA `PacketWrapper` of an
/// explicit `MediaType`. The base `build_media_payload` always emits VIDEO; the
/// late-listener load test needs BOTH AUDIO and VIDEO so it can assert the late
/// listener captured each independently (the budget calls out audio AND video).
fn build_media_payload_typed(
    sender_sid: SessionId,
    sender_user: &str,
    media_type: MediaType,
    seed: u8,
) -> Vec<u8> {
    let media = MediaPacket {
        media_type: media_type.into(),
        data: vec![seed; 32],
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

/// vc-9eh: count captured MEDIA wrappers of a given `MediaType`.
fn captured_media_of_type(received: &Arc<Mutex<Vec<Vec<u8>>>>, media_type: MediaType) -> usize {
    let lock = received.lock().unwrap();
    lock.iter()
        .filter(|bytes| {
            let Ok(w) = <PacketWrapper as ProtobufMessage>::parse_from_bytes(bytes) else {
                return false;
            };
            if w.packet_type != PacketType::MEDIA.into() {
                return false;
            }
            MediaPacket::parse_from_bytes(&w.data)
                .map(|m| m.media_type == media_type.into())
                .unwrap_or(false)
        })
        .count()
}

/// Build a serialized CONGESTION `PacketWrapper`.
fn build_congestion_payload(sender_sid: SessionId, sender_user: &str, seed: u8) -> Vec<u8> {
    let wrapper = PacketWrapper {
        packet_type: PacketType::CONGESTION.into(),
        session_id: sender_sid,
        user_id: sender_user.as_bytes().to_vec(),
        data: vec![seed; 8],
        ..Default::default()
    };
    wrapper.write_to_bytes().expect("encode PacketWrapper")
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

/// Publish `count` deterministic MEDIA frames from `sender` into `room`,
/// returning the on-wire bytes the actor will hand off to NATS. The publisher
/// fills `session_id` with `sender.sid` so `ChatServer::handle::<ClientMessage>`
/// has nothing to re-write (the equivalent of a well-formed client).
async fn send_media_burst(
    chat: &actix::Addr<ChatServer>,
    sender: &Participant,
    room: &str,
    count: usize,
) -> Vec<Vec<u8>> {
    let mut out = Vec::with_capacity(count);
    for k in 0..count {
        // Distinct seed per packet so byte-equality across modes is a
        // strictly stronger assertion than "count matches".
        let seed = (k as u8).wrapping_add(1);
        let bytes = build_media_payload(sender.sid, &sender.user, seed);
        out.push(bytes.clone());
        chat.send(ClientMessage {
            session: sender.sid,
            room: room.to_string(),
            user: sender.user.clone(),
            msg: Packet {
                data: Arc::new(bytes),
                kind: PacketKind::Data,
            },
        })
        .await
        .expect("ClientMessage mailbox");
    }
    out
}

/// How long to wait between JoinRoom and the first publish so the per-session
/// NATS subscription tasks have time to attach. Existing tests in the crate
/// use 500ms; we bring up several rooms back-to-back so we keep this snug.
const SUBSCRIBE_SETTLE: Duration = Duration::from_millis(250);
/// How long to wait after the last publish for NATS fan-out + handler delivery
/// to settle before reading capture buffers.
const PUBLISH_SETTLE: Duration = Duration::from_millis(400);

/// Run scenario {1:1, 1:N, CONGESTION} against one `ChatServer` configured
/// with `mode`. Returns, per receiver, the ordered list of MEDIA delivery
/// byte-vectors plus the CONGESTION delivery byte-vectors so the caller can
/// compare across modes for byte parity.
struct ScenarioOutcomes {
    /// MEDIA bytes received by B in the 1:1 room, in delivery order.
    one_to_one_b_media: Vec<Vec<u8>>,
    /// MEDIA bytes received by A in the 1:1 room (must be empty: self-skip).
    one_to_one_a_media: Vec<Vec<u8>>,
    /// MEDIA bytes received by each of B/C/D in the 1:N room.
    one_to_n_media: Vec<(SessionId, Vec<Vec<u8>>)>,
    /// MEDIA received by A in the 1:N room (must be empty: self-skip).
    one_to_n_a_media: Vec<Vec<u8>>,
    /// CONGESTION bytes received by each non-sender in the 1:N room.
    congestion_others: Vec<(SessionId, Vec<Vec<u8>>)>,
}

async fn run_all_scenarios(mode: &str, sid_base: SessionId, nats_url: &str) -> ScenarioOutcomes {
    // Snapshot + override SFU_MODE BEFORE `ChatServer::new()` reads it.
    let env = EnvGuard::new();
    env.set(mode);

    let nats_client = async_nats::connect(nats_url)
        .await
        .expect("connect to NATS");
    let chat = ChatServer::new(nats_client).await.start();

    // ----- Scenario 1: 1:1 room (A + B) -----
    let room_11 = format!("p2-9-1to1-{mode}");
    let a = Participant::new(sid_base, "alice@example.com");
    let b = Participant::new(sid_base + 1, "bob@example.com");
    register_and_join(&chat, &a, &room_11)
        .await
        .expect("A join");
    register_and_join(&chat, &b, &room_11)
        .await
        .expect("B join");
    sleep(SUBSCRIBE_SETTLE).await;
    let _sent_11 = send_media_burst(&chat, &a, &room_11, 10).await;
    sleep(PUBLISH_SETTLE).await;
    let one_to_one_b_media = b.captured_of(PacketType::MEDIA);
    let one_to_one_a_media = a.captured_of(PacketType::MEDIA);

    // ----- Scenario 2: 1:N room (A + B, C, D) -----
    let room_1n = format!("p2-9-1toN-{mode}");
    let a2 = Participant::new(sid_base + 10, "alice2@example.com");
    let b2 = Participant::new(sid_base + 11, "bob2@example.com");
    let c2 = Participant::new(sid_base + 12, "carol2@example.com");
    let d2 = Participant::new(sid_base + 13, "dave2@example.com");
    register_and_join(&chat, &a2, &room_1n)
        .await
        .expect("A2 join");
    register_and_join(&chat, &b2, &room_1n)
        .await
        .expect("B2 join");
    register_and_join(&chat, &c2, &room_1n)
        .await
        .expect("C2 join");
    register_and_join(&chat, &d2, &room_1n)
        .await
        .expect("D2 join");
    sleep(SUBSCRIBE_SETTLE).await;
    let _sent_1n = send_media_burst(&chat, &a2, &room_1n, 10).await;
    sleep(PUBLISH_SETTLE).await;
    let one_to_n_media: Vec<(SessionId, Vec<Vec<u8>>)> = [&b2, &c2, &d2]
        .iter()
        .map(|p| (p.sid, p.captured_of(PacketType::MEDIA)))
        .collect();
    let one_to_n_a_media = a2.captured_of(PacketType::MEDIA);

    // ----- Scenario 3: CONGESTION carve-out in the 1:N room -----
    // Reuse the 1:N room+participants so we don't pay another set of
    // subscription-settle sleeps. A publishes one CONGESTION packet; bead
    // requirement is "all OTHER receivers get it in BOTH modes".
    let cong_bytes = build_congestion_payload(a2.sid, &a2.user, 0xC1);
    chat.send(ClientMessage {
        session: a2.sid,
        room: room_1n.clone(),
        user: a2.user.clone(),
        msg: Packet {
            data: Arc::new(cong_bytes),
            kind: PacketKind::Data,
        },
    })
    .await
    .expect("CONGESTION ClientMessage");
    sleep(PUBLISH_SETTLE).await;
    let congestion_others: Vec<(SessionId, Vec<Vec<u8>>)> = [&b2, &c2, &d2]
        .iter()
        .map(|p| (p.sid, p.captured_of(PacketType::CONGESTION)))
        .collect();

    ScenarioOutcomes {
        one_to_one_b_media,
        one_to_one_a_media,
        one_to_n_media,
        one_to_n_a_media,
        congestion_others,
    }
}

// ---------------------------------------------------------------------------
// The integration test
// ---------------------------------------------------------------------------

#[actix_rt::test]
#[serial]
async fn sfu_mode_integration_parity() {
    let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());

    // Run legacy first so the SFU run's metric delta is measured against a
    // baseline taken AFTER the legacy run (the legacy path must not increment
    // `sfu_forwarded_total`, which is a property we also assert below).
    let legacy_baseline = SFU_FORWARDED_TOTAL.with_label_values(&["media"]).get();

    let legacy = run_all_scenarios("legacy", 50_000, &nats_url).await;

    let after_legacy = SFU_FORWARDED_TOTAL.with_label_values(&["media"]).get();
    // Legacy path bypasses the forwarder entirely — no media-forwarded
    // increments should be observed.
    assert!(
        (after_legacy - legacy_baseline).abs() < f64::EPSILON,
        "SFU_MODE=legacy must NOT increment sfu_forwarded_total{{packet_type=\"media\"}}: \
         baseline={legacy_baseline}, after_legacy={after_legacy}"
    );

    let sfu = run_all_scenarios("sfu", 60_000, &nats_url).await;

    let after_sfu = SFU_FORWARDED_TOTAL.with_label_values(&["media"]).get();
    let sfu_delta = after_sfu - after_legacy;

    // ---------- 1:1 room assertions ----------
    assert_eq!(
        legacy.one_to_one_b_media.len(),
        10,
        "legacy 1:1: B must receive all 10 MEDIA packets, got {}",
        legacy.one_to_one_b_media.len()
    );
    assert_eq!(
        sfu.one_to_one_b_media.len(),
        10,
        "sfu 1:1: B must receive all 10 MEDIA packets, got {}",
        sfu.one_to_one_b_media.len()
    );
    assert!(
        legacy.one_to_one_a_media.is_empty(),
        "legacy 1:1: A must not receive its own MEDIA, got {} packets",
        legacy.one_to_one_a_media.len()
    );
    assert!(
        sfu.one_to_one_a_media.is_empty(),
        "sfu 1:1: A must not receive its own MEDIA, got {} packets",
        sfu.one_to_one_a_media.len()
    );

    // Byte-identical golden trace: B's delivery sequence must be bit-equal
    // between legacy and SFU after normalizing the sender's session_id. The
    // legacy and SFU runs use distinct sid_base values so their actor state
    // machines don't collide; everything else on the wire (packet_type,
    // user_id, inner MediaPacket bytes, ordering) must match exactly.
    let legacy_b_norm: Vec<Vec<u8>> = legacy
        .one_to_one_b_media
        .iter()
        .map(|b| normalize_for_parity(b))
        .collect();
    let sfu_b_norm: Vec<Vec<u8>> = sfu
        .one_to_one_b_media
        .iter()
        .map(|b| normalize_for_parity(b))
        .collect();
    assert_eq!(
        legacy_b_norm, sfu_b_norm,
        "1:1 MEDIA byte sequence to B must be identical between legacy and sfu \
         (post-sid normalization)"
    );

    // ---------- 1:N room assertions ----------
    assert!(
        legacy.one_to_n_a_media.is_empty(),
        "legacy 1:N: A must not receive its own MEDIA"
    );
    assert!(
        sfu.one_to_n_a_media.is_empty(),
        "sfu 1:N: A must not receive its own MEDIA"
    );
    for (sid, deliveries) in &legacy.one_to_n_media {
        assert_eq!(
            deliveries.len(),
            10,
            "legacy 1:N: receiver {sid} must receive all 10 MEDIA packets, got {}",
            deliveries.len()
        );
    }
    for (sid, deliveries) in &sfu.one_to_n_media {
        assert_eq!(
            deliveries.len(),
            10,
            "sfu 1:N: receiver {sid} must receive all 10 MEDIA packets, got {}",
            deliveries.len()
        );
    }

    // 1:N byte-parity per receiver. Pair by ordering (both arrays were built
    // by iterating B,C,D in the same order under each mode).
    assert_eq!(
        legacy.one_to_n_media.len(),
        sfu.one_to_n_media.len(),
        "1:N receiver count must match across modes"
    );
    for ((l_sid, l_msgs), (s_sid, s_msgs)) in
        legacy.one_to_n_media.iter().zip(sfu.one_to_n_media.iter())
    {
        // SIDs differ between modes (we used distinct sid_base values to
        // keep the actor's state machines independent), but the *delivery
        // sequence* must be byte-identical packet-for-packet.
        assert_eq!(
            l_msgs.len(),
            s_msgs.len(),
            "1:N delivery count mismatch (legacy sid {l_sid} vs sfu sid {s_sid})"
        );
        for (i, (l, s)) in l_msgs.iter().zip(s_msgs.iter()).enumerate() {
            // Note: PacketWrapper.session_id reflects the sender's sid,
            // which differs between legacy (sid_base=50_000) and sfu
            // (sid_base=60_000) runs. Strip that field before byte
            // comparison so the parity assertion measures payload
            // transformation, not test-fixture renumbering.
            let l_norm = normalize_for_parity(l);
            let s_norm = normalize_for_parity(s);
            assert_eq!(
                l_norm, s_norm,
                "1:N MEDIA byte mismatch at delivery index {i} (legacy sid {l_sid} vs sfu sid {s_sid})"
            );
        }
    }

    // ---------- CONGESTION carve-out ----------
    for (sid, deliveries) in &legacy.congestion_others {
        assert_eq!(
            deliveries.len(),
            1,
            "legacy CONGESTION: receiver {sid} (non-sender) must receive the carve-out broadcast"
        );
    }
    for (sid, deliveries) in &sfu.congestion_others {
        assert_eq!(
            deliveries.len(),
            1,
            "sfu CONGESTION: receiver {sid} (non-sender) must receive the carve-out broadcast"
        );
    }
    // CONGESTION byte parity across modes (normalize sender sid as above).
    for ((_l_sid, l_msgs), (_s_sid, s_msgs)) in legacy
        .congestion_others
        .iter()
        .zip(sfu.congestion_others.iter())
    {
        let l_norm = normalize_for_parity(&l_msgs[0]);
        let s_norm = normalize_for_parity(&s_msgs[0]);
        assert_eq!(
            l_norm, s_norm,
            "CONGESTION byte mismatch between legacy and sfu modes (post-sid normalization)"
        );
    }

    // ---------- Metric: sfu_forwarded_total{packet_type="media"} ----------
    //
    // SFU run expected MEDIA-forwarded increments:
    //   1:1 scenario: 10 packets × 1 non-self receiver (B)            = 10
    //   1:N scenario: 10 packets × 3 non-self receivers (B,C,D)       = 30
    //   CONGESTION:   bypasses the forwarder entirely                 =  0
    //   Total                                                         = 40
    //
    // The 1:1 self-receiver (A) is filtered by the self-subject check in
    // egress_decide_bytes BEFORE the forwarder is invoked, so it does NOT
    // contribute a "self_skip" decision either. Same for the 1:N A.
    let expected_sfu_increments = 10.0 + 30.0;
    assert!(
        (sfu_delta - expected_sfu_increments).abs() < f64::EPSILON,
        "SFU_MODE=sfu must increment sfu_forwarded_total{{packet_type=\"media\"}} by exactly \
         {expected_sfu_increments}, got delta={sfu_delta} (after_legacy={after_legacy}, after_sfu={after_sfu})"
    );
}

/// Normalize a serialized `PacketWrapper` for cross-mode byte comparison by
/// zeroing the `session_id` field. The legacy and SFU runs use distinct
/// `sid_base` values (so their ChatServer state machines don't collide), so
/// the literal on-wire bytes differ by exactly that field; everything else
/// (packet_type, user_id, inner MediaPacket bytes, ordering) must match.
fn normalize_for_parity(bytes: &[u8]) -> Vec<u8> {
    match <PacketWrapper as ProtobufMessage>::parse_from_bytes(bytes) {
        Ok(mut w) => {
            w.session_id = 0;
            w.write_to_bytes().unwrap_or_else(|_| bytes.to_vec())
        }
        Err(_) => bytes.to_vec(),
    }
}

// ===========================================================================
// p3-5: per-receiver SubscriptionUpdate filtering (acceptance criterion)
// ===========================================================================
//
// Three-client room in SFU mode where B sends `SubscriptionUpdate{pinned:[C]}`
// before any MEDIA flows. A (never sent an update) keeps the legacy-default
// fanout; B sees only C's MEDIA; C (also never sent an update) keeps full
// fanout. The `sfu_dropped_total{reason=unsubscribed}` counter must observe a
// strictly positive delta covering the A→B MEDIA packets that get filtered.

/// Build a SUBSCRIPTION_UPDATE wrapper carrying `update`. The server's
/// `ClientMessage` handler intercepts wrappers of this type and applies them
/// to the per-room `SubscriptionStore` rather than republishing on NATS.
fn build_subscription_update_payload(
    sender_sid: SessionId,
    sender_user: &str,
    update: SubscriptionUpdate,
) -> Vec<u8> {
    let wrapper = PacketWrapper {
        packet_type: PacketType::SUBSCRIPTION_UPDATE.into(),
        session_id: sender_sid,
        user_id: sender_user.as_bytes().to_vec(),
        data: update.write_to_bytes().expect("encode SubscriptionUpdate"),
        ..Default::default()
    };
    wrapper.write_to_bytes().expect("encode PacketWrapper")
}

async fn send_subscription_update(
    chat: &actix::Addr<ChatServer>,
    sender: &Participant,
    room: &str,
    update: SubscriptionUpdate,
) {
    let bytes = build_subscription_update_payload(sender.sid, &sender.user, update);
    chat.send(ClientMessage {
        session: sender.sid,
        room: room.to_string(),
        user: sender.user.clone(),
        msg: Packet {
            data: Arc::new(bytes),
            kind: PacketKind::Data,
        },
    })
    .await
    .expect("SubscriptionUpdate ClientMessage");
}

#[actix_rt::test]
#[serial]
async fn sfu_subscription_filters_per_receiver() {
    let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());

    // SFU mode is required: the per-receiver AllowSet filter only runs on
    // the SFU path. Legacy is unconditional broadcast.
    let env = EnvGuard::new();
    env.set("sfu");

    let nats_client = async_nats::connect(&nats_url)
        .await
        .expect("connect to NATS");
    let chat = ChatServer::new(nats_client).await.start();

    let room = "p3-5-sub-filter".to_string();
    // Distinct sid range from the parity test so concurrent runs don't
    // collide on ChatServer's joined_sessions / session_manager.
    let a = Participant::new(70_000, "a-sub@example.com");
    let b = Participant::new(70_001, "b-sub@example.com");
    let c = Participant::new(70_002, "c-sub@example.com");
    register_and_join(&chat, &a, &room).await.expect("A join");
    register_and_join(&chat, &b, &room).await.expect("B join");
    register_and_join(&chat, &c, &room).await.expect("C join");

    // Snapshot the unsubscribed-drop counter BEFORE any filtering work runs
    // so we can assert a strict positive delta on the unsubscribed drops we
    // expect to observe (A → B MEDIA frames).
    let dropped_before = SFU_DROPPED_TOTAL.with_label_values(&["unsubscribed"]).get();

    // B declares a restrictive subscription pinned to C only. No slots, no
    // receive_all_audio — B should see C's MEDIA exclusively. The update
    // must land BEFORE any MEDIA flows so the forwarder consults it from
    // the very first packet.
    let mut update = SubscriptionUpdate::new();
    update.pinned_sessions = vec![c.sid];
    update.slots = vec![];
    update.receive_all_audio = false;
    send_subscription_update(&chat, &b, &room, update).await;

    // Settle: subscription apply is synchronous in the ClientMessage handler,
    // but room subscriptions still need a moment to attach.
    sleep(SUBSCRIBE_SETTLE).await;

    // Each of A, B, C publishes 5 VIDEO MEDIA frames.
    const BURST: usize = 5;
    let _sent_a = send_media_burst(&chat, &a, &room, BURST).await;
    let _sent_b = send_media_burst(&chat, &b, &room, BURST).await;
    let _sent_c = send_media_burst(&chat, &c, &room, BURST).await;

    sleep(PUBLISH_SETTLE).await;

    let a_media = a.captured_of(PacketType::MEDIA);
    let b_media = b.captured_of(PacketType::MEDIA);
    let c_media = c.captured_of(PacketType::MEDIA);

    // A never sent a SubscriptionUpdate → legacy-default AllowSet covers
    // every other member, so A sees B's 5 + C's 5 frames.
    assert_eq!(
        a_media.len(),
        BURST * 2,
        "A (no SubscriptionUpdate) must see both other senders' MEDIA: got {}",
        a_media.len()
    );

    // B pinned only C → A's MEDIA filtered as unsubscribed, B's own MEDIA
    // self-skipped, leaving exactly C's 5.
    assert_eq!(
        b_media.len(),
        BURST,
        "B (pinned=[C]) must see only C's MEDIA: got {}",
        b_media.len()
    );
    for bytes in &b_media {
        let w = <PacketWrapper as ProtobufMessage>::parse_from_bytes(bytes)
            .expect("decode delivered wrapper");
        assert_eq!(
            w.session_id, c.sid,
            "B's filtered delivery must originate from C: got sid={}",
            w.session_id
        );
    }

    // C never sent a SubscriptionUpdate → legacy-default AllowSet, sees
    // both A's 5 and B's 5.
    assert_eq!(
        c_media.len(),
        BURST * 2,
        "C (no SubscriptionUpdate) must see both other senders' MEDIA: got {}",
        c_media.len()
    );

    // The unsubscribed-drop counter must have advanced by at least BURST
    // (A → B drops). It may advance by more if other in-process tests
    // happen to overlap on the global registry; the strict-positive bound
    // is the meaningful guarantee here.
    let dropped_after = SFU_DROPPED_TOTAL.with_label_values(&["unsubscribed"]).get();
    let drop_delta = dropped_after - dropped_before;
    assert!(
        drop_delta >= BURST as f64,
        "sfu_dropped_total{{reason=\"unsubscribed\"}} must increment by at least {BURST} \
         (A→B drops): before={dropped_before} after={dropped_after} delta={drop_delta}"
    );
}

// ===========================================================================
// vc-3s8: webinar first-joiner bug — sender that joins AFTER a listener must
// still be visible to that listener.
//
// Two scenarios mirroring the bead's repro:
//   1. Listener joins first, sender joins later. Listener never sends a
//      SubscriptionUpdate. Sender publishes MEDIA. Listener must capture it
//      (legacy-default AllowSet path).
//   2. Listener joins first, sends an empty SubscriptionUpdate with
//      receive_all_audio=true (the coalescer's default opening emit once any
//      visibility/pin flips). Sender joins later. Sender publishes MEDIA.
//      Listener must capture it — this is the regression the fix targets.
// ===========================================================================

#[actix_rt::test]
#[serial]
async fn sfu_vc_3s8_late_joiner_visible_no_subscription_update() {
    let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());

    let env = EnvGuard::new();
    env.set("sfu");

    let nats_client = async_nats::connect(&nats_url)
        .await
        .expect("connect to NATS");
    let chat = ChatServer::new(nats_client).await.start();

    let room = "vc-3s8-no-sub".to_string();
    // Distinct sid range from other tests in this file.
    let listener = Participant::new(80_000, "listener-1@example.com");
    let sender = Participant::new(80_001, "sender-1@example.com");

    // Step 1: listener joins FIRST and never sends a SubscriptionUpdate.
    register_and_join(&chat, &listener, &room)
        .await
        .expect("listener join");
    sleep(SUBSCRIBE_SETTLE).await;

    // Step 2: sender joins AFTER the listener.
    register_and_join(&chat, &sender, &room)
        .await
        .expect("sender join");
    sleep(SUBSCRIBE_SETTLE).await;

    // Step 3: sender publishes MEDIA.
    const BURST: usize = 5;
    let _sent = send_media_burst(&chat, &sender, &room, BURST).await;
    sleep(PUBLISH_SETTLE).await;

    let listener_media = listener.captured_of(PacketType::MEDIA);
    assert_eq!(
        listener_media.len(),
        BURST,
        "vc-3s8 scenario 1: listener (no SubscriptionUpdate) must capture all \
         {BURST} MEDIA packets from a sender that joined AFTER it, got {}",
        listener_media.len()
    );
}

#[actix_rt::test]
#[serial]
async fn sfu_vc_3s8_late_joiner_visible_with_empty_receive_all_audio_update() {
    let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());

    let env = EnvGuard::new();
    env.set("sfu");

    let nats_client = async_nats::connect(&nats_url)
        .await
        .expect("connect to NATS");
    let chat = ChatServer::new(nats_client).await.start();

    let room = "vc-3s8-empty-sub".to_string();
    let listener = Participant::new(80_010, "listener-2@example.com");
    let sender = Participant::new(80_011, "sender-2@example.com");

    // Step 1: listener joins FIRST.
    register_and_join(&chat, &listener, &room)
        .await
        .expect("listener join");

    // Step 2: listener sends an empty SubscriptionUpdate with both
    // receive_all_audio and receive_all_video set. This mirrors the client's
    // `SubscriptionCoalescer` opening emit: visible={}, pinned={},
    // receive_all_audio=true, receive_all_video=true (vc-3s8). Real clients
    // flush this packet the first time any visibility/pin flip happens,
    // often BEFORE the first peer has joined the room.
    let mut update = SubscriptionUpdate::new();
    update.pinned_sessions = vec![];
    update.slots = vec![];
    update.receive_all_audio = true;
    update.receive_all_video = true;
    send_subscription_update(&chat, &listener, &room, update).await;
    sleep(SUBSCRIBE_SETTLE).await;

    // Step 3: sender joins AFTER the listener's subscription was applied.
    register_and_join(&chat, &sender, &room)
        .await
        .expect("sender join");
    sleep(SUBSCRIBE_SETTLE).await;

    // Step 4: sender publishes MEDIA.
    const BURST: usize = 5;
    let _sent = send_media_burst(&chat, &sender, &room, BURST).await;
    sleep(PUBLISH_SETTLE).await;

    let listener_media = listener.captured_of(PacketType::MEDIA);
    assert_eq!(
        listener_media.len(),
        BURST,
        "vc-3s8 scenario 2: listener (empty update, receive_all_audio=true) \
         must capture all {BURST} MEDIA packets from a sender that joined \
         AFTER the subscription was applied, got {}",
        listener_media.len()
    );
}

// ===========================================================================
// vc-7wi: symmetric counterpart to vc-3s8 — listener joins AFTER an existing
// publisher. The publisher is already in the room and may already have been
// publishing before the listener joined; the listener must receive media for
// all packets the publisher sends from the moment the listener is registered
// as a receiver in the per-room dispatcher.
//
// Two scenarios that mirror the vc-3s8 layout but flip the join order:
//   A. Sender joins first and publishes. Listener joins AFTER. Listener never
//      sends a SubscriptionUpdate. Listener must capture all media the sender
//      emits AFTER the listener joined (legacy-default AllowSet path).
//   B. Sender joins first and publishes. Listener joins AFTER and sends an
//      empty SubscriptionUpdate with `receive_all_audio=true,
//      receive_all_video=true` (the SubscriptionCoalescer's opening emit).
//      Listener must capture all subsequent media from the sender.
//
// These tests pin down the symmetric direction of the fix. The forwarder's
// per-room dispatcher snapshots the `receivers` map on every inbound NATS
// message, and `SubscriptionStore::resolve_inner` returns either the legacy
// default fan-out (no SubscriptionUpdate ever applied) or applies the
// `receive_all_video` catch-all over the *live* `current_members` set — both
// paths already cover late-joining receivers. The tests below lock that
// behavior in as a regression guard so future tuning of the resolver or
// dispatcher cannot silently break it.
// ===========================================================================

#[actix_rt::test]
#[serial]
async fn sfu_vc_7wi_late_joining_listener_sees_existing_publisher_no_subscription_update() {
    let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());

    let env = EnvGuard::new();
    env.set("sfu");

    let nats_client = async_nats::connect(&nats_url)
        .await
        .expect("connect to NATS");
    let chat = ChatServer::new(nats_client).await.start();

    let room = "vc-7wi-no-sub".to_string();
    // Distinct sid range from other tests in this file.
    let sender = Participant::new(81_000, "sender-7wi-1@example.com");
    let listener = Participant::new(81_001, "listener-7wi-1@example.com");

    // Step 1: sender joins FIRST.
    register_and_join(&chat, &sender, &room)
        .await
        .expect("sender join");
    sleep(SUBSCRIBE_SETTLE).await;

    // Step 2: sender publishes a pre-listener burst. These packets predate
    // the listener's membership and we explicitly do NOT require the listener
    // to receive them — there is no buffering in the SFU pass-through. This
    // burst exists only to make sure the dispatcher is hot and the sender's
    // outbound NATS publish path is fully attached before the listener
    // arrives.
    const PRE_LISTENER_BURST: usize = 3;
    let _pre = send_media_burst(&chat, &sender, &room, PRE_LISTENER_BURST).await;
    sleep(PUBLISH_SETTLE).await;

    // Step 3: listener joins AFTER the sender has already been publishing.
    // Listener NEVER sends a SubscriptionUpdate, so the SubscriptionStore
    // returns the legacy-default AllowSet (everyone else, base layer) on
    // every resolve.
    register_and_join(&chat, &listener, &room)
        .await
        .expect("listener join");
    sleep(SUBSCRIBE_SETTLE).await;

    // Step 4: sender publishes a fresh burst AFTER the listener has joined.
    // These are the packets the listener must capture.
    const POST_JOIN_BURST: usize = 5;
    let _post = send_media_burst(&chat, &sender, &room, POST_JOIN_BURST).await;
    sleep(PUBLISH_SETTLE).await;

    let listener_media = listener.captured_of(PacketType::MEDIA);
    assert_eq!(
        listener_media.len(),
        POST_JOIN_BURST,
        "vc-7wi scenario A: listener (no SubscriptionUpdate, joined AFTER an \
         existing publisher) must capture all {POST_JOIN_BURST} MEDIA packets \
         the publisher emits AFTER the listener joined, got {}",
        listener_media.len()
    );
}

#[actix_rt::test]
#[serial]
async fn sfu_vc_7wi_late_joining_listener_sees_existing_publisher_with_empty_receive_all_update() {
    let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());

    let env = EnvGuard::new();
    env.set("sfu");

    let nats_client = async_nats::connect(&nats_url)
        .await
        .expect("connect to NATS");
    let chat = ChatServer::new(nats_client).await.start();

    let room = "vc-7wi-empty-sub".to_string();
    let sender = Participant::new(81_010, "sender-7wi-2@example.com");
    let listener = Participant::new(81_011, "listener-7wi-2@example.com");

    // Step 1: sender joins FIRST and publishes a pre-listener warm-up burst.
    register_and_join(&chat, &sender, &room)
        .await
        .expect("sender join");
    sleep(SUBSCRIBE_SETTLE).await;
    const PRE_LISTENER_BURST: usize = 3;
    let _pre = send_media_burst(&chat, &sender, &room, PRE_LISTENER_BURST).await;
    sleep(PUBLISH_SETTLE).await;

    // Step 2: listener joins AFTER. Sender is already a member of the room.
    register_and_join(&chat, &listener, &room)
        .await
        .expect("listener join");

    // Step 3: listener sends its opening empty SubscriptionUpdate with both
    // catch-alls set, mirroring the SubscriptionCoalescer's default emit. The
    // catch-all reads `current_members`, which already contains the sender.
    // The vc-3s8 fix added the `receive_all_video` fan-out tier specifically
    // to cover this path on the resolver side; the symmetric vc-7wi assertion
    // is that the per-room dispatcher's `receivers` snapshot already includes
    // the just-joined listener by the time the next inbound NATS message
    // arrives.
    let mut update = SubscriptionUpdate::new();
    update.pinned_sessions = vec![];
    update.slots = vec![];
    update.receive_all_audio = true;
    update.receive_all_video = true;
    send_subscription_update(&chat, &listener, &room, update).await;
    sleep(SUBSCRIBE_SETTLE).await;

    // Step 4: sender publishes a fresh burst. Listener must capture every
    // packet — the existing-publisher direction of the vc-3s8 regression.
    const POST_JOIN_BURST: usize = 5;
    let _post = send_media_burst(&chat, &sender, &room, POST_JOIN_BURST).await;
    sleep(PUBLISH_SETTLE).await;

    let listener_media = listener.captured_of(PacketType::MEDIA);
    assert_eq!(
        listener_media.len(),
        POST_JOIN_BURST,
        "vc-7wi scenario B: listener (empty update with receive_all_audio=true \
         and receive_all_video=true, joined AFTER an existing publisher) must \
         capture all {POST_JOIN_BURST} MEDIA packets the publisher emits AFTER \
         the listener joined, got {}",
        listener_media.len()
    );
}

// ===========================================================================
// vc-9eh: late-listener-onto-active-publishers under SUSTAINED LOAD.
//
// This reproduces the bottom-left matrix cell from the root-cause analysis
// (LATE-LISTENER-ROOTCAUSE.md §1/§5) that the vc-7wi tests above CANNOT: those
// use a single listener, no pre-existing receiver cohort, and a 5-packet burst.
// The real failure (Bug A) only manifests with (a) a populated `receivers` map
// (the early cohort) keeping the dispatcher hot AND (b) a sustained publisher
// stream, after which a LATE listener that joins must still capture continuous
// publisher media — BOTH audio AND video — within the responsiveness budget.
//
// The fix under test is the per-room delivery watchdog in
// `spawn_room_dispatcher` (Part A) plus the locked-in insert ordering (Part B):
// even if the dispatcher's wildcard subscription goes silent under the storm,
// the watchdog forces a clean resubscribe against the SAME `receivers` Arc, so
// the late cohort resumes receiving without any client reconnect.
// ===========================================================================

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

/// vc-9eh: spawn a background task that publishes a SUSTAINED interleaved
/// audio+video stream from `sender` into `room` until `stop` is set. Rate is
/// well above the toy `send_media_burst` (which sends a fixed handful): ~10ms
/// between frames (~100 packets/sec, audio+video alternating). Returns the
/// `JoinHandle` so the caller can await drain after stopping.
fn spawn_sustained_publisher(
    chat: actix::Addr<ChatServer>,
    sender_sid: SessionId,
    sender_user: String,
    room: String,
    stop: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut seed: u8 = 0;
        while !stop.load(Ordering::Relaxed) {
            seed = seed.wrapping_add(1);
            // Interleave audio and video so a late listener can be asserted to
            // capture BOTH independently.
            for media_type in [MediaType::AUDIO, MediaType::VIDEO] {
                let bytes = build_media_payload_typed(sender_sid, &sender_user, media_type, seed);
                // `do_send` (not `send().await`): fire-and-forget that DROPS on a
                // full mailbox instead of waiting for a slot. This mirrors the
                // production drop-on-overflow path and, crucially, keeps the
                // publisher emitting at the intended rate to actually pressure
                // the dispatcher — `send().await` would backpressure the
                // publisher and never build a queue.
                chat.do_send(ClientMessage {
                    session: sender_sid,
                    room: room.clone(),
                    user: sender_user.clone(),
                    msg: Packet {
                        data: Arc::new(bytes),
                        kind: PacketKind::Data,
                    },
                });
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
}

#[actix_rt::test]
#[serial]
async fn sfu_vc_9eh_late_listener_under_sustained_load_sees_audio_and_video() {
    let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());

    let env = EnvGuard::new();
    env.set("sfu");

    let nats_client = sec_api::nats_connect::connect(&nats_url)
        .await
        .expect("connect to NATS");
    let chat = ChatServer::new(nats_client).await.start();

    let room = "vc-9eh-sustained".to_string();

    // --- N publishers (sustained streams) ---------------------------------
    const N_PUBLISHERS: usize = 3;
    let mut publishers = Vec::new();
    for i in 0..N_PUBLISHERS {
        let p = Participant::new(90_000 + i as SessionId, &format!("pub-9eh-{i}@example.com"));
        register_and_join(&chat, &p, &room)
            .await
            .expect("publisher join");
        publishers.push(p);
    }
    sleep(SUBSCRIBE_SETTLE).await;

    // Start the sustained publisher streams (audio+video interleaved).
    let stop = Arc::new(AtomicBool::new(false));
    let mut pub_tasks = Vec::new();
    for p in &publishers {
        pub_tasks.push(spawn_sustained_publisher(
            chat.clone(),
            p.sid,
            p.user.clone(),
            room.clone(),
            stop.clone(),
        ));
    }

    // --- Early listener cohort (populate `receivers`, keep dispatcher hot) --
    const EARLY_COHORT: usize = 5;
    let mut early = Vec::new();
    for i in 0..EARLY_COHORT {
        let l = Participant::new(
            90_100 + i as SessionId,
            &format!("early-9eh-{i}@example.com"),
        );
        register_and_join(&chat, &l, &room)
            .await
            .expect("early listener join");
        early.push(l);
    }

    // --- Sustained window so the dispatcher subscription is under load ------
    sleep(Duration::from_millis(1500)).await;

    // --- LATE listener joins (no SubscriptionUpdate) ------------------------
    let late = Participant::new(90_200, "late-9eh@example.com");
    register_and_join(&chat, &late, &room)
        .await
        .expect("late listener join");
    let late_join_at = Instant::now();

    // --- Publishers continue. Poll the late listener's capture buffer for
    // the responsiveness budget: first media <= 1.5s, usable audio <= 2.0s.
    let mut first_media_at: Option<Duration> = None;
    let mut first_audio_at: Option<Duration> = None;
    loop {
        let elapsed = late_join_at.elapsed();
        let audio = captured_media_of_type(&late.received, MediaType::AUDIO);
        let video = captured_media_of_type(&late.received, MediaType::VIDEO);
        if first_media_at.is_none() && (audio + video) > 0 {
            first_media_at = Some(elapsed);
        }
        if first_audio_at.is_none() && audio > 0 {
            first_audio_at = Some(elapsed);
        }
        // Stop once we have a healthy continuous sample of both, or we blow
        // the budget.
        if audio >= 5 && video >= 5 {
            break;
        }
        if elapsed > Duration::from_millis(2500) {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }

    // Stop publishing and drain.
    stop.store(true, Ordering::Relaxed);
    for t in pub_tasks {
        let _ = t.await;
    }

    let late_audio = captured_media_of_type(&late.received, MediaType::AUDIO);
    let late_video = captured_media_of_type(&late.received, MediaType::VIDEO);

    // Budget assertions.
    let first_media =
        first_media_at.expect("late listener captured NO publisher media at all (Bug A)");
    assert!(
        first_media <= Duration::from_millis(1500),
        "vc-9eh: first media must arrive within 1.5s of the late join, got {}ms \
         (audio={late_audio}, video={late_video})",
        first_media.as_millis()
    );
    let first_audio = first_audio_at.expect("late listener captured NO publisher AUDIO (Bug A)");
    assert!(
        first_audio <= Duration::from_millis(2000),
        "vc-9eh: usable audio must arrive within 2.0s of the late join, got {}ms",
        first_audio.as_millis()
    );

    // Continuous (not just first packet) delivery of BOTH media types.
    assert!(
        late_audio >= 5,
        "vc-9eh: late listener must capture CONTINUOUS publisher AUDIO, got {late_audio}"
    );
    assert!(
        late_video >= 5,
        "vc-9eh: late listener must capture CONTINUOUS publisher VIDEO, got {late_video}"
    );

    // Sanity: the early cohort kept receiving too (the storm didn't black-hole
    // the whole room).
    let early_audio = captured_media_of_type(&early[0].received, MediaType::AUDIO);
    let early_video = captured_media_of_type(&early[0].received, MediaType::VIDEO);
    assert!(
        early_audio > 0 && early_video > 0,
        "vc-9eh: early-cohort listener must also receive media \
         (audio={early_audio}, video={early_video})"
    );
}

// ===========================================================================
// vc-9eh REGRESSION GUARD: the watchdog gating predicate must fire exactly when
// the subscription has gone silent WHILE receivers are non-empty AND there are
// active publishers — and must NOT fire (no thrash) otherwise. Reliably forcing
// real async-nats slow-consumer backpressure in-process is impractical and
// timing-dependent, so per the bead's allowance we exercise the watchdog
// decision branch directly via the factored-out pure predicate
// `watchdog_should_resubscribe` (the SAME function the live dispatcher
// `select!` arm calls). This locks in that the watchdog cannot silently rot.
// ===========================================================================

#[test]
fn sfu_vc_9eh_watchdog_resubscribe_gating() {
    use sec_api::actors::chat_server::{
        watchdog_should_resubscribe, watchdog_silence_window, WATCHDOG_GRACE, WATCHDOG_SILENCE,
    };

    let past_grace = WATCHDOG_GRACE + Duration::from_millis(1);
    // Window at trips=0 is the base SILENCE; use a silence just past it.
    let window0 = watchdog_silence_window(0);
    let silent = window0 + Duration::from_millis(1);

    // THE failure condition: past grace, silent, receivers present, publishers
    // present => MUST force a resubscribe (this is the Bug A recovery).
    assert!(
        watchdog_should_resubscribe(past_grace, silent, window0, true, true),
        "watchdog must resubscribe when silent with receivers + publishers"
    );

    // Must NOT fire (each gate independently suppresses, preventing thrash):
    assert!(
        !watchdog_should_resubscribe(past_grace, silent, window0, false, true),
        "no receivers => nothing to recover"
    );
    assert!(
        !watchdog_should_resubscribe(past_grace, silent, window0, true, false),
        "no publishers => silence is expected (idle/all-muted room must not thrash)"
    );
    assert!(
        !watchdog_should_resubscribe(
            past_grace,
            window0 - Duration::from_millis(1),
            window0,
            true,
            true
        ),
        "recent delivery (< current window) => subscription is healthy"
    );
    assert!(
        !watchdog_should_resubscribe(
            WATCHDOG_GRACE - Duration::from_millis(1),
            silent,
            window0,
            true,
            true
        ),
        "within grace => a fresh/resubscribed subscription must not be judged dead"
    );

    // Budget sanity: the FIRST trip after traffic (trips=0) uses the base
    // window <= 750ms so a genuinely-broken subscription with active publishers
    // resubscribes fast enough to keep first media within the 1.5s budget.
    assert!(
        window0 <= Duration::from_millis(750) && WATCHDOG_SILENCE <= Duration::from_millis(750),
        "base silence window must be <= 750ms to keep first media within budget"
    );
}

#[test]
fn sfu_vc_9eh_watchdog_backoff_escalates_and_caps() {
    use sec_api::actors::chat_server::{
        watchdog_should_resubscribe, watchdog_silence_window, WATCHDOG_GRACE, WATCHDOG_SILENCE,
        WATCHDOG_SILENCE_CAP,
    };

    // Escalation: each consecutive silent trip doubles the window from the base
    // (750ms, 1.5s, 3s, ...) up to the cap.
    assert_eq!(
        watchdog_silence_window(0),
        WATCHDOG_SILENCE,
        "trip 0 = base"
    );
    assert_eq!(
        watchdog_silence_window(1),
        WATCHDOG_SILENCE * 2,
        "trip 1 = 2x base"
    );
    assert_eq!(
        watchdog_silence_window(2),
        WATCHDOG_SILENCE * 4,
        "trip 2 = 4x base"
    );
    // Monotonic non-decreasing and capped.
    let mut prev = Duration::ZERO;
    for trips in 0..40u32 {
        let w = watchdog_silence_window(trips);
        assert!(w >= prev, "window must be monotonic non-decreasing");
        assert!(
            w <= WATCHDOG_SILENCE_CAP,
            "window must never exceed the cap"
        );
        prev = w;
    }
    assert_eq!(
        watchdog_silence_window(1000),
        WATCHDOG_SILENCE_CAP,
        "deep trip count saturates at the cap (no overflow)"
    );

    // Anti-thrash semantics through the predicate: a populated, quiet room that
    // has already tripped once is NOT eligible again until the LONGER (escalated)
    // window has elapsed — so it cannot resubscribe at a fixed fast cadence.
    let past_grace = WATCHDOG_GRACE + Duration::from_millis(1);
    let window1 = watchdog_silence_window(1); // 1.5s
                                              // Silence just past the BASE window but short of the escalated window:
                                              // after one trip, must NOT fire again yet.
    let silence_between = WATCHDOG_SILENCE + Duration::from_millis(1);
    assert!(silence_between < window1);
    assert!(
        !watchdog_should_resubscribe(past_grace, silence_between, window1, true, true),
        "after escalation the room must wait the LONGER window before firing again \
         (this is the persisted backoff that prevents thrash)"
    );
    // Once silence exceeds the escalated window, it fires again.
    assert!(
        watchdog_should_resubscribe(
            past_grace,
            window1 + Duration::from_millis(1),
            window1,
            true,
            true
        ),
        "a still-broken subscription eventually re-fires at the escalated window"
    );
}

/// vc-9eh: model the dispatcher's ACTUAL watchdog state machine across ticks to
/// prove the steady-state cadence DECAYS to the 30s cap (not collapses to ~1s).
///
/// This is the regression that the window-table-only test missed: the bug was
/// that `silence` is measured from the last REAL message and grows monotonically
/// for a quiet room, so once it permanently exceeds the cap the `silence >=
/// window` gate is always satisfied and the only remaining gate is GRACE (reset
/// on every resubscribe) — flooring the cadence at ~GRACE. The fix is resetting
/// the silence clock (`last_msg_at`) on each resubscribe. This test mirrors the
/// loop's bookkeeping (a virtual clock, no real time) and asserts:
///   1. first detection fires at exactly the base 750ms window from last traffic
///      (budget preserved),
///   2. inter-trip spacing escalates 750ms → 1.5s → 3s → … and FLOORS at the
///      30s cap (the decay the bead requires), and
///   3. traffic resets both the silence clock and the trip counter (fast
///      recovery).
#[test]
fn sfu_vc_9eh_watchdog_cadence_decays_to_cap() {
    use sec_api::actors::chat_server::{
        watchdog_should_resubscribe, watchdog_silence_window, WATCHDOG_GRACE, WATCHDOG_SILENCE,
        WATCHDOG_SILENCE_CAP, WATCHDOG_TICK,
    };

    // Virtual dispatcher state, mirroring spawn_room_dispatcher's locals.
    // We use Durations-since-epoch as the virtual clock (monotonic millis).
    let mut now = Duration::ZERO;
    let mut subscribe_at = now; // grace clock
    let mut last_msg_at = now; // silence clock
    let mut trips: u32 = 0;

    // Drive the loop forward one TICK at a time, applying the SAME gating the
    // dispatcher applies, and record the virtual time of each resubscribe trip.
    let mut trip_times: Vec<Duration> = Vec::new();
    // Run long enough to exit the escalation ramp and reach the capped cadence
    // several times over (sum of windows to cap ~= 56s; run ~3 minutes).
    let horizon = Duration::from_secs(180);
    while now < horizon {
        now += WATCHDOG_TICK;
        let uptime = now - subscribe_at;
        let silence = now - last_msg_at;
        let window = watchdog_silence_window(trips);
        if watchdog_should_resubscribe(
            uptime, silence, window, /*recv*/ true, /*pub*/ true,
        ) {
            trip_times.push(now);
            // Mirror the FIXED resubscribe arm: escalate trips, restart BOTH the
            // grace clock and the silence clock from the resubscribe instant.
            trips = trips.saturating_add(1);
            subscribe_at = now;
            last_msg_at = now;
        }
    }

    assert!(
        trip_times.len() >= 6,
        "expected several trips over the horizon, got {}",
        trip_times.len()
    );

    // (1) First detection fires at the base window from last traffic. The tick
    // granularity rounds up to the next 250ms boundary at/after 750ms => 750ms.
    assert_eq!(
        trip_times[0], WATCHDOG_SILENCE,
        "first trip must fire at the base 750ms window from last traffic \
         (budget preserved); got {:?}",
        trip_times[0]
    );

    // (2) Inter-trip spacing escalates then floors at the cap. Spacing[i] is the
    // gap between trip i and trip i+1; it must equal window(i+1) rounded up to a
    // tick boundary (window is tied to the trip count AFTER the i-th escalation).
    let round_up_to_tick = |d: Duration| -> Duration {
        let tick = WATCHDOG_TICK.as_millis() as u64;
        let ms = d.as_millis() as u64;
        Duration::from_millis(ms.div_ceil(tick) * tick)
    };
    for i in 0..trip_times.len() - 1 {
        let spacing = trip_times[i + 1] - trip_times[i];
        let expected = round_up_to_tick(watchdog_silence_window((i as u32) + 1));
        assert_eq!(
            spacing, expected,
            "inter-trip spacing #{i} must follow the escalating window \
             (got {:?}, expected {:?})",
            spacing, expected
        );
    }
    // The LAST observed spacing must be the capped cadence (rounded to tick),
    // proving the cadence decays to ~30s and does NOT collapse to ~1s.
    let last_spacing = trip_times[trip_times.len() - 1] - trip_times[trip_times.len() - 2];
    assert_eq!(
        last_spacing,
        round_up_to_tick(WATCHDOG_SILENCE_CAP),
        "steady-state cadence must floor at the 30s cap, not collapse to ~GRACE"
    );
    assert!(
        last_spacing >= Duration::from_secs(30),
        "capped cadence must be >= 30s (regression guard against the ~1s collapse)"
    );

    // (3) Traffic resets BOTH the silence clock and the trip counter. Model a
    // dispatcher deep into escalation, then a real message arriving mid-stall.
    let trips_before_traffic: u32 = 4; // window would be 12s
    let escalated_window = watchdog_silence_window(trips_before_traffic);
    assert_eq!(
        escalated_window,
        WATCHDOG_SILENCE * 16,
        "trip 4 window is 16x base (12s)"
    );
    // Before traffic, a base-window silence is NOT enough to trip (the escalated
    // window governs) — this is the persisted backoff.
    assert!(
        !watchdog_should_resubscribe(
            WATCHDOG_GRACE + Duration::from_millis(1),
            WATCHDOG_SILENCE + Duration::from_millis(1),
            escalated_window,
            true,
            true
        ),
        "while escalated, a base-window silence must not trip"
    );
    // A message arrives: the Some(msg) arm sets last_msg_at = now AND trips = 0.
    let trips_after_traffic: u32 = 0;
    let base_window = watchdog_silence_window(trips_after_traffic);
    assert_eq!(
        base_window, WATCHDOG_SILENCE,
        "reset returns to the base window"
    );
    // The NEXT stall must now fire at the base window again — fast recovery.
    assert!(
        watchdog_should_resubscribe(
            WATCHDOG_GRACE + Duration::from_millis(1),
            WATCHDOG_SILENCE + Duration::from_millis(1),
            base_window,
            true,
            true
        ),
        "after traffic resets trips to 0, the next stall must trip at the BASE \
         window (fast recovery), not the previously-escalated window"
    );
    // And the grace gate still protects a freshly-reset clock.
    assert!(
        !watchdog_should_resubscribe(
            WATCHDOG_GRACE - Duration::from_millis(1),
            WATCHDOG_SILENCE + Duration::from_millis(1),
            watchdog_silence_window(0),
            true,
            true
        ),
        "within grace, even a base-window silence must not trip"
    );
}
