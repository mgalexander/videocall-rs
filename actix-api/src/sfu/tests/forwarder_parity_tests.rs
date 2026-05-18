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

//! Golden-trace parity tests: SFU path vs legacy path (p2-8).
//!
//! Asserts that for the wave-3 pass-through forwarder, the SFU egress path
//! (`SfuMode::Sfu`) and the legacy broadcast path (`SfuMode::Legacy`) produce
//! **byte-for-byte identical** fan-out for the same deterministic input
//! stream. This is the "no behavior change, just plumbing" invariant Phase 2
//! is built on; once P3+ adds real selection logic the test will be updated.
//!
//! The tests drive the *production* helper [`egress_decide_bytes`] in
//! `chat_server` directly — see the doc comment on that function. That
//! function is the pure egress core extracted from `chat_server::handle_msg`,
//! so this test cannot silently drift from production: any change to the
//! decision logic must go through the helper that production uses too.
//!
//! In-process, no NATS, no actors, no real network. Designed to run in well
//! under a second.

use std::sync::{Arc, RwLock};

use bytes::Bytes;
use protobuf::Message as ProtobufMessage;
use videocall_types::protos::media_packet::media_packet::MediaType;
use videocall_types::protos::media_packet::MediaPacket;
use videocall_types::protos::packet_wrapper::packet_wrapper::PacketType;
use videocall_types::protos::packet_wrapper::PacketWrapper;

use crate::actors::chat_server::egress_decide_bytes;
use crate::actors::session_logic::SessionId;
use crate::sfu::forwarder::Forwarder;
use crate::sfu::room_state::RoomState;
use crate::sfu::SfuMode;

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

const TEST_ROOM: &str = "parity-room";

/// Build a serialized MEDIA `PacketWrapper` whose sender session id is
/// `sender_sid`. The inner `MediaPacket` carries deterministic body bytes
/// drawn from `seed` so each packet in a stream is distinguishable.
fn build_media_payload(sender_sid: SessionId, seed: u8) -> Bytes {
    let media = MediaPacket {
        media_type: MediaType::VIDEO.into(),
        // Deterministic body so byte-equality assertions are meaningful.
        data: vec![seed; 32],
        ..Default::default()
    };
    let wrapper = PacketWrapper {
        packet_type: PacketType::MEDIA.into(),
        session_id: sender_sid,
        // user_id is irrelevant for the egress decision but matches what
        // chat_server sees on the wire.
        user_id: b"sender@example.com".to_vec(),
        data: media.write_to_bytes().expect("encode MediaPacket"),
        ..Default::default()
    };
    Bytes::from(wrapper.write_to_bytes().expect("encode PacketWrapper"))
}

/// Build a serialized CONGESTION `PacketWrapper` purportedly addressed to
/// `target_sid` (the receiver who is being told to back off). At egress, the
/// CONGESTION carve-out broadcasts these to *all* receivers in the room — see
/// the CRITICAL comment in `egress_decide_bytes`.
fn build_congestion_payload(target_sid: SessionId, seed: u8) -> Bytes {
    let wrapper = PacketWrapper {
        packet_type: PacketType::CONGESTION.into(),
        // `session_id` on a CONGESTION packet is the session being told to
        // throttle — see `congestion::CongestionTracker`.
        session_id: target_sid,
        data: vec![seed; 8],
        ..Default::default()
    };
    Bytes::from(wrapper.write_to_bytes().expect("encode PacketWrapper"))
}

/// Mirror chat_server's NATS subject formatting:
/// `format!("room.{room}.{sender_sid}").replace(' ', "_")`.
fn publish_subject(sender_sid: SessionId) -> String {
    format!("room.{TEST_ROOM}.{sender_sid}").replace(' ', "_")
}

/// Build a fresh forwarder over a populated `RoomState` containing every
/// receiver session id. Returns the forwarder; the room handle is not
/// otherwise needed by these tests (the wave-3 pass-through decision only
/// reads the room for its size gauge + self-skip check).
fn build_forwarder(receivers: &[SessionId]) -> Arc<Forwarder> {
    let room = Arc::new(RwLock::new(RoomState::new(TEST_ROOM.to_string())));
    {
        let mut w = room.write().expect("room write lock");
        for &sid in receivers {
            // Capabilities don't influence the pass-through decision; pass 0.
            w.insert_member(sid, 0);
        }
    }
    Arc::new(Forwarder::with_room_only(room))
}

/// One element of the deterministic input stream: who published it on which
/// subject, and the on-wire bytes.
struct Event {
    /// Session id of the publisher — each session publishes on its own
    /// subject (`room.{room}.{publisher}`) in this codebase, so this drives
    /// both the subject used for fan-out and the self-skip decision.
    publisher: SessionId,
    payload: Bytes,
}

/// Run the egress decision for every (event, receiver) pair under `mode` and
/// return a map from `receiver_sid` → ordered list of byte-blobs delivered.
fn collect_fanout(
    mode: SfuMode,
    forwarder: &Forwarder,
    receivers: &[SessionId],
    stream: &[Event],
) -> std::collections::BTreeMap<SessionId, Vec<Bytes>> {
    let mut out: std::collections::BTreeMap<SessionId, Vec<Bytes>> =
        receivers.iter().map(|&sid| (sid, Vec::new())).collect();
    for ev in stream {
        let subj = publish_subject(ev.publisher);
        for &rsid in receivers {
            if let Some(bytes) =
                egress_decide_bytes(mode, forwarder, rsid, TEST_ROOM, &subj, &ev.payload)
            {
                out.get_mut(&rsid).expect("receiver in map").push(bytes);
            }
        }
    }
    out
}

/// Assert that legacy and SFU produce byte-identical fan-out for `stream`.
/// Returns the per-receiver delivery counts so callers can make additional
/// assertions (e.g. "this CONGESTION reached everyone").
fn assert_parity(
    receivers: &[SessionId],
    stream: &[Event],
) -> std::collections::BTreeMap<SessionId, usize> {
    // Each mode uses its own forwarder so per-receiver state added in later
    // phases (rate limiters, jitter buffers, etc.) cannot leak between
    // modes and silently mask a parity failure.
    let fwd_legacy = build_forwarder(receivers);
    let fwd_sfu = build_forwarder(receivers);

    let legacy = collect_fanout(SfuMode::Legacy, &fwd_legacy, receivers, stream);
    let sfu = collect_fanout(SfuMode::Sfu, &fwd_sfu, receivers, stream);

    assert_eq!(
        legacy.keys().collect::<Vec<_>>(),
        sfu.keys().collect::<Vec<_>>(),
        "receiver sets must match across modes",
    );

    for (&rsid, legacy_msgs) in &legacy {
        let sfu_msgs = sfu.get(&rsid).expect("receiver present in sfu map");
        assert_eq!(
            legacy_msgs.len(),
            sfu_msgs.len(),
            "delivery count mismatch for receiver {rsid}: legacy={} sfu={}",
            legacy_msgs.len(),
            sfu_msgs.len(),
        );
        for (i, (lbytes, sbytes)) in legacy_msgs.iter().zip(sfu_msgs.iter()).enumerate() {
            assert_eq!(
                lbytes, sbytes,
                "byte mismatch for receiver {rsid} at delivery index {i}",
            );
        }
    }

    legacy.into_iter().map(|(k, v)| (k, v.len())).collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn basic_fanout_parity() {
    // N=2 senders, M=3 receivers; one receiver is also a sender (sid 1).
    let senders: Vec<SessionId> = vec![1, 2];
    let receivers: Vec<SessionId> = vec![1, 2, 3];

    // Deterministic media stream: each sender emits 3 packets with distinct
    // seed bytes, so byte-equality is a meaningful assertion.
    let mut stream = Vec::new();
    for (i, &s) in senders.iter().enumerate() {
        for k in 0..3u8 {
            let seed = (i as u8) * 16 + k + 1;
            stream.push(Event {
                publisher: s,
                payload: build_media_payload(s, seed),
            });
        }
    }

    let counts = assert_parity(&receivers, &stream);

    // Sanity: receivers that are also senders get N-1 senders' packets;
    // receiver 3 (not a sender) gets all senders' packets.
    // 2 senders × 3 packets = 6; subtract this receiver's own publishes
    // (3 if the receiver is also a sender, else 0).
    assert_eq!(counts[&1], 3, "receiver 1 (also sender) sees only sender 2");
    assert_eq!(counts[&2], 3, "receiver 2 (also sender) sees only sender 1");
    assert_eq!(counts[&3], 6, "receiver 3 sees everything");
}

#[test]
fn self_skip_parity() {
    // One sender that is also a receiver, plus other receivers. The sender's
    // own MEDIA must NOT be delivered to itself in either path, but MUST be
    // delivered to others byte-identically.
    let sender: SessionId = 7;
    let receivers: Vec<SessionId> = vec![7, 8, 9];

    let stream = vec![Event {
        publisher: sender,
        payload: build_media_payload(sender, 0xAB),
    }];

    let counts = assert_parity(&receivers, &stream);
    assert_eq!(counts[&7], 0, "self-skip must drop sender's own MEDIA");
    assert_eq!(counts[&8], 1);
    assert_eq!(counts[&9], 1);
}

#[test]
fn congestion_broadcast_parity() {
    // CONGESTION is the carve-out: the sender's own subject delivers it back
    // to the sender (it is not echo — see comment in egress_decide_bytes)
    // and forwarder.decide is bypassed entirely. Must hold byte-identically
    // in both modes.
    let sender: SessionId = 11;
    let receivers: Vec<SessionId> = vec![11, 12, 13];

    let stream = vec![Event {
        publisher: sender,
        payload: build_congestion_payload(sender, 0x42),
    }];

    let counts = assert_parity(&receivers, &stream);
    for &rsid in &receivers {
        assert_eq!(
            counts[&rsid], 1,
            "CONGESTION must reach EVERY receiver (including sender {sender}) in both paths; \
             receiver {rsid} got {}",
            counts[&rsid]
        );
    }
}

#[test]
fn empty_room_parity() {
    // 0 receivers → 0 forwards in both modes; no panics, no crashes.
    let receivers: Vec<SessionId> = vec![];
    let stream = vec![
        Event {
            publisher: 1,
            payload: build_media_payload(1, 0x01),
        },
        Event {
            publisher: 2,
            payload: build_congestion_payload(2, 0x02),
        },
    ];

    let counts = assert_parity(&receivers, &stream);
    assert!(counts.is_empty(), "no receivers means no deliveries");
}

#[test]
fn mixed_stream_parity() {
    // Longer interleaved stream: multiple MEDIA from multiple senders, with
    // CONGESTION broadcasts sprinkled in. The full byte sequence each
    // receiver collects must match exactly between modes (ordering is
    // preserved by the iteration order in collect_fanout).
    let senders: Vec<SessionId> = vec![100, 200, 300];
    let receivers: Vec<SessionId> = vec![100, 200, 300, 400];

    let mut stream: Vec<Event> = Vec::new();
    // Deterministic interleave: 12 media frames + 2 congestion signals.
    for round in 0..4u8 {
        for (i, &s) in senders.iter().enumerate() {
            stream.push(Event {
                publisher: s,
                payload: build_media_payload(s, round * 4 + i as u8 + 1),
            });
        }
        if round == 1 {
            // CongestionTracker addresses sender 200 telling it to back off,
            // published on sender 200's own subject.
            stream.push(Event {
                publisher: 200,
                payload: build_congestion_payload(200, 0xC1),
            });
        }
        if round == 3 {
            stream.push(Event {
                publisher: 300,
                payload: build_congestion_payload(300, 0xC2),
            });
        }
    }

    let counts = assert_parity(&receivers, &stream);

    // Receivers that are also senders see all other senders' MEDIA plus
    // every CONGESTION (CONGESTION reaches the sender too).
    // 3 senders × 4 rounds = 12 MEDIA total; each sender-receiver skips
    // their own 4 MEDIA, leaving 8. Plus 2 CONGESTION.
    assert_eq!(counts[&100], 8 + 2);
    assert_eq!(counts[&200], 8 + 2);
    assert_eq!(counts[&300], 8 + 2);
    // Receiver 400 is a pure observer (no self-skip): all 12 MEDIA + 2 CONGESTION.
    assert_eq!(counts[&400], 12 + 2);
}
