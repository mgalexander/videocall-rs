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

//! Unit tests for `sfu::forwarder::Forwarder` (wave-3 pass-through).

use std::sync::{Arc, RwLock};
use std::thread;

use videocall_types::protos::packet_wrapper::packet_wrapper::PacketType;
use videocall_types::protos::packet_wrapper::PacketWrapper;

use crate::metrics::{SFU_DECIDE_LATENCY_US, SFU_DROPPED_TOTAL, SFU_FORWARDED_TOTAL};
use crate::sfu::forwarder::{ForwardDecision, Forwarder};
use crate::sfu::room_state::RoomState;

fn make_packet(sender_sid: u64) -> PacketWrapper {
    let mut pw = PacketWrapper::new();
    pw.session_id = sender_sid;
    pw.packet_type = PacketType::MEDIA.into();
    pw.user_id = b"sender@example.com".to_vec();
    pw.data = b"hello".to_vec();
    pw
}

#[test]
fn passthrough_forwards_to_unrelated_receiver() {
    let room = Arc::new(RwLock::new(RoomState::new("r".to_string())));
    let fwd = Forwarder::new(room);

    let sender_sid = 1u64;
    let receiver_sid = 2u64;
    let pw = make_packet(sender_sid);

    // The decision no longer carries bytes — the call site is responsible
    // for forwarding the original on-wire NATS payload on `Forward`.
    assert!(matches!(
        fwd.decide(receiver_sid, &pw, None),
        ForwardDecision::Forward
    ));
}

#[test]
fn self_skip_drops_when_receiver_is_sender() {
    let room = Arc::new(RwLock::new(RoomState::new("r".to_string())));
    let fwd = Forwarder::new(room);

    let sid = 42u64;
    let pw = make_packet(sid);

    match fwd.decide(sid, &pw, None) {
        ForwardDecision::Drop => {}
        ForwardDecision::Forward => panic!("expected Drop for self-skip"),
    }
}

#[test]
fn concurrent_decide_calls_against_shared_room() {
    let room = Arc::new(RwLock::new(RoomState::new("r".to_string())));
    {
        let mut w = room.write().expect("lock");
        for sid in 0..16u64 {
            w.insert_member(sid, 0);
        }
    }
    let fwd = Arc::new(Forwarder::new(room));

    let mut handles = Vec::new();
    for receiver_sid in 0..100u64 {
        let fwd = Arc::clone(&fwd);
        handles.push(thread::spawn(move || {
            let pw = make_packet(receiver_sid.wrapping_add(1));
            matches!(
                fwd.decide(receiver_sid, &pw, None),
                ForwardDecision::Forward
            )
        }));
    }

    for h in handles {
        assert!(h.join().expect("join"));
    }
}

#[test]
fn metrics_forward_counter_increments_on_forward() {
    let room = Arc::new(RwLock::new(RoomState::new("metrics-fwd-room".to_string())));
    let fwd = Forwarder::new(room);

    let sender_sid = 11u64;
    let receiver_sid = 22u64;
    let pw = make_packet(sender_sid);

    // Metrics are global / lazy_static — capture deltas, not absolute values,
    // so the test is order-independent w.r.t. other forwarder tests.
    let before = SFU_FORWARDED_TOTAL.with_label_values(&["media"]).get();
    let latency_before = SFU_DECIDE_LATENCY_US.get_sample_count();

    match fwd.decide(receiver_sid, &pw, None) {
        ForwardDecision::Forward => {}
        ForwardDecision::Drop => panic!("expected Forward for unrelated receiver"),
    }

    let after = SFU_FORWARDED_TOTAL.with_label_values(&["media"]).get();
    assert!(
        after > before,
        "sfu_forwarded_total{{packet_type=\"media\"}} did not increase: before={before} after={after}"
    );

    let latency_after = SFU_DECIDE_LATENCY_US.get_sample_count();
    assert!(
        latency_after > latency_before,
        "sfu_decide_latency_us sample count did not increase: before={latency_before} after={latency_after}"
    );
}

#[test]
fn metrics_drop_counter_increments_on_self_skip() {
    let room = Arc::new(RwLock::new(RoomState::new("metrics-drop-room".to_string())));
    let fwd = Forwarder::new(room);

    let sid = 99u64;
    let pw = make_packet(sid);

    let before = SFU_DROPPED_TOTAL.with_label_values(&["self_skip"]).get();

    match fwd.decide(sid, &pw, None) {
        ForwardDecision::Drop => {}
        ForwardDecision::Forward => panic!("expected Drop for self-skip"),
    }

    let after = SFU_DROPPED_TOTAL.with_label_values(&["self_skip"]).get();
    assert!(
        after > before,
        "sfu_dropped_total{{reason=\"self_skip\"}} did not increase: before={before} after={after}"
    );
}
