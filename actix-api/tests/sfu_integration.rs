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
use sec_api::actors::session_logic::SessionId;
use sec_api::messages::server::{ActivateConnection, ClientMessage, Connect, JoinRoom, Packet};
use sec_api::messages::session::Message;
use sec_api::metrics::SFU_FORWARDED_TOTAL;

use videocall_types::protos::media_packet::media_packet::MediaType;
use videocall_types::protos::media_packet::MediaPacket;
use videocall_types::protos::packet_wrapper::packet_wrapper::PacketType;
use videocall_types::protos::packet_wrapper::PacketWrapper;

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
