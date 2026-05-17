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

use protobuf::Message;
use videocall_types::protos::packet_wrapper::packet_wrapper::PacketType;
use videocall_types::protos::packet_wrapper::PacketWrapper;

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

    match fwd.decide(receiver_sid, &pw, None) {
        ForwardDecision::Forward(bytes) => {
            let expected = pw.write_to_bytes().expect("serialize");
            assert_eq!(bytes.as_ref(), expected.as_slice());
        }
        ForwardDecision::Drop => panic!("expected Forward for unrelated receiver"),
    }
}

#[test]
fn self_skip_drops_when_receiver_is_sender() {
    let room = Arc::new(RwLock::new(RoomState::new("r".to_string())));
    let fwd = Forwarder::new(room);

    let sid = 42u64;
    let pw = make_packet(sid);

    match fwd.decide(sid, &pw, None) {
        ForwardDecision::Drop => {}
        ForwardDecision::Forward(_) => panic!("expected Drop for self-skip"),
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
                ForwardDecision::Forward(_)
            )
        }));
    }

    for h in handles {
        assert!(h.join().expect("join"));
    }
}
