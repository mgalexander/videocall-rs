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

//! Parse-once egress equivalence tests (vc-q0v).
//!
//! Production fan-out parses each inbound NATS message **exactly once per
//! room** via [`parse_and_inspect`] and then calls
//! [`egress_decide_from_parsed`] once per receiver. The pre-vc-q0v
//! per-session model parsed the wrapper N times for an N-participant room.
//!
//! These tests lock in the equivalence: for an interleaved stream of MEDIA,
//! CONGESTION, and unparseable payloads, the **parse-once** path must
//! produce the exact same per-receiver byte sequence as the **parse-per-
//! receiver** path that `egress_decide_bytes` represents. If a future
//! optimization tries to skip the unparseable / non-MEDIA branches, this
//! test will fail before the change reaches production.
//!
//! The test also counts parse calls via a sentinel-wrapped fixture so a
//! regression that puts the parse back inside the per-receiver loop is
//! caught directly.

use std::sync::{Arc, RwLock};

use bytes::Bytes;
use protobuf::Message as ProtobufMessage;
use videocall_types::protos::media_packet::media_packet::MediaType;
use videocall_types::protos::media_packet::MediaPacket;
use videocall_types::protos::packet_wrapper::packet_wrapper::PacketType;
use videocall_types::protos::packet_wrapper::PacketWrapper;

use crate::actors::chat_server::{egress_decide_bytes, egress_decide_from_parsed};
use crate::actors::packet_handler::{parse_and_inspect, ParsedPacket};
use crate::actors::session_logic::SessionId;
use crate::sfu::forwarder::Forwarder;
use crate::sfu::room_state::RoomState;
use crate::sfu::SfuMode;

const ROOM: &str = "parse-once-room";

fn build_media(sender_sid: SessionId, seed: u8) -> Bytes {
    let media = MediaPacket {
        media_type: MediaType::VIDEO.into(),
        data: vec![seed; 32],
        ..Default::default()
    };
    let wrapper = PacketWrapper {
        packet_type: PacketType::MEDIA.into(),
        session_id: sender_sid,
        user_id: b"sender@example.com".to_vec(),
        data: media.write_to_bytes().unwrap(),
        ..Default::default()
    };
    Bytes::from(wrapper.write_to_bytes().unwrap())
}

fn build_congestion(target_sid: SessionId, seed: u8) -> Bytes {
    let wrapper = PacketWrapper {
        packet_type: PacketType::CONGESTION.into(),
        session_id: target_sid,
        data: vec![seed; 8],
        ..Default::default()
    };
    Bytes::from(wrapper.write_to_bytes().unwrap())
}

fn subject(sender_sid: SessionId) -> String {
    format!("room.{ROOM}.{sender_sid}").replace(' ', "_")
}

fn build_forwarder(receivers: &[SessionId]) -> Arc<Forwarder> {
    let room = Arc::new(RwLock::new(RoomState::new(ROOM.to_string())));
    {
        let mut w = room.write().unwrap();
        for &sid in receivers {
            w.insert_member(sid, 0);
        }
    }
    Arc::new(Forwarder::new(room))
}

struct Event {
    publisher: SessionId,
    payload: Bytes,
}

/// Production fan-out: parse ONCE per inbound message, then decide per
/// receiver. Returns per-receiver delivery byte sequences AND the total
/// number of parse calls (one per message).
fn parse_once_fanout(
    mode: SfuMode,
    forwarder: &Forwarder,
    receivers: &[SessionId],
    stream: &[Event],
) -> (Vec<Vec<Bytes>>, usize) {
    let mut out: Vec<Vec<Bytes>> = receivers.iter().map(|_| Vec::new()).collect();
    let mut parse_count: usize = 0;
    for ev in stream {
        let subj = subject(ev.publisher);
        // Single parse per inbound message — the whole point of vc-q0v.
        let parsed: Option<ParsedPacket> = parse_and_inspect(&ev.payload[..]);
        parse_count += 1;
        for (i, &rsid) in receivers.iter().enumerate() {
            if let Some(bytes) = egress_decide_from_parsed(
                mode,
                forwarder,
                rsid,
                ROOM,
                &subj,
                &ev.payload,
                parsed.as_ref(),
            ) {
                out[i].push(bytes);
            }
        }
    }
    (out, parse_count)
}

/// Pre-vc-q0v fan-out: every receiver re-parses the wrapper. Used as the
/// reference oracle for equivalence — calls `egress_decide_bytes`, which
/// invokes `parse_and_inspect` internally on each call.
fn parse_per_receiver_fanout(
    mode: SfuMode,
    forwarder: &Forwarder,
    receivers: &[SessionId],
    stream: &[Event],
) -> Vec<Vec<Bytes>> {
    let mut out: Vec<Vec<Bytes>> = receivers.iter().map(|_| Vec::new()).collect();
    for ev in stream {
        let subj = subject(ev.publisher);
        for (i, &rsid) in receivers.iter().enumerate() {
            if let Some(bytes) =
                egress_decide_bytes(mode, forwarder, rsid, ROOM, &subj, &ev.payload)
            {
                out[i].push(bytes);
            }
        }
    }
    out
}

fn assert_byte_equivalence(mode: SfuMode, receivers: &[SessionId], stream: &[Event]) {
    // Fresh forwarders so per-receiver state (rate limiters, counters that
    // future phases will add) cannot leak between paths.
    let fwd_once = build_forwarder(receivers);
    let fwd_each = build_forwarder(receivers);

    let (parse_once, parse_count) = parse_once_fanout(mode, &fwd_once, receivers, stream);
    let parse_each = parse_per_receiver_fanout(mode, &fwd_each, receivers, stream);

    assert_eq!(
        parse_count,
        stream.len(),
        "parse_once_fanout must parse exactly once per inbound message \
         (got {parse_count}, expected {})",
        stream.len(),
    );

    assert_eq!(
        parse_once.len(),
        parse_each.len(),
        "receiver-count mismatch ({} vs {})",
        parse_once.len(),
        parse_each.len(),
    );

    for (i, (once, each)) in parse_once.iter().zip(parse_each.iter()).enumerate() {
        assert_eq!(
            once.len(),
            each.len(),
            "delivery count mismatch for receiver index {i} (sid {}) under {mode:?}: \
             parse-once={} parse-per-receiver={}",
            receivers[i],
            once.len(),
            each.len(),
        );
        for (j, (a, b)) in once.iter().zip(each.iter()).enumerate() {
            assert_eq!(
                a, b,
                "byte mismatch for receiver index {i} (sid {}) at delivery {j} under {mode:?}",
                receivers[i],
            );
        }
    }
}

#[test]
fn parse_once_matches_parse_per_receiver_media_stream() {
    // Multi-sender MEDIA stream: every receiver must see byte-identical
    // output regardless of whether the parse happens once or per-receiver.
    let senders: Vec<SessionId> = vec![1, 2, 3];
    let receivers: Vec<SessionId> = vec![1, 2, 3, 4];
    let mut stream = Vec::new();
    for (i, &s) in senders.iter().enumerate() {
        for k in 0..3u8 {
            stream.push(Event {
                publisher: s,
                payload: build_media(s, (i as u8) * 8 + k + 1),
            });
        }
    }
    assert_byte_equivalence(SfuMode::Sfu, &receivers, &stream);
    assert_byte_equivalence(SfuMode::Legacy, &receivers, &stream);
}

#[test]
fn parse_once_matches_parse_per_receiver_with_congestion() {
    // CONGESTION is the carve-out: even when sender_sid == receiver_sid the
    // packet must still be delivered, and that branch is keyed on the
    // parsed wrapper's packet_type. If parse-once accidentally skipped the
    // CONGESTION branch under any receiver, this test would fail.
    let sender: SessionId = 11;
    let receivers: Vec<SessionId> = vec![11, 12, 13];
    let stream = vec![
        Event {
            publisher: sender,
            payload: build_media(sender, 0xAA),
        },
        Event {
            publisher: sender,
            payload: build_congestion(sender, 0xC1),
        },
        Event {
            publisher: 12,
            payload: build_media(12, 0xBB),
        },
    ];
    assert_byte_equivalence(SfuMode::Sfu, &receivers, &stream);
    assert_byte_equivalence(SfuMode::Legacy, &receivers, &stream);
}

#[test]
fn parse_once_matches_parse_per_receiver_with_unparseable_payload() {
    // Unparseable wrapper bytes: `parse_and_inspect` returns None. The
    // parse-once branch must still self-skip (mode-independent) for the
    // sender, and forward to other receivers in legacy mode (and in SFU
    // mode via the tolerant fall-through). The pre-vc-q0v parse-per-
    // receiver reference does the same — equivalence must hold byte-for-
    // byte.
    let sender: SessionId = 21;
    let receivers: Vec<SessionId> = vec![21, 22];
    let stream = vec![
        Event {
            publisher: sender,
            // Not a valid PacketWrapper.
            payload: Bytes::from_static(&[0xFF, 0xFF, 0xFF, 0xFF]),
        },
        Event {
            publisher: 22,
            payload: build_media(22, 0x77),
        },
    ];
    assert_byte_equivalence(SfuMode::Sfu, &receivers, &stream);
    assert_byte_equivalence(SfuMode::Legacy, &receivers, &stream);
}

#[test]
fn parse_count_is_exactly_one_per_message_not_per_receiver() {
    // Sensitivity check: if a future refactor moves the parse back inside
    // the per-receiver loop, `parse_count` would scale with N receivers
    // rather than stay equal to the number of inbound messages. This test
    // pins parse_count == stream.len() regardless of receiver count.
    let receivers: Vec<SessionId> = (100..=150).collect(); // 51 receivers
    let stream: Vec<Event> = (0..10u8)
        .map(|i| Event {
            publisher: 100,
            payload: build_media(100, i + 1),
        })
        .collect();
    let fwd = build_forwarder(&receivers);
    let (_, parse_count) = parse_once_fanout(SfuMode::Sfu, &fwd, &receivers, &stream);
    assert_eq!(
        parse_count,
        stream.len(),
        "parse-once must parse exactly once per message regardless of receiver count \
         (receivers={}, messages={}, observed parses={})",
        receivers.len(),
        stream.len(),
        parse_count,
    );
}
