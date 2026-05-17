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

//! Unit tests for `sfu::room_state::RoomState`.

use crate::sfu::room_state::{RoomState, CAP_SFU_ROUTING_HEADER, CAP_SUBSCRIPTION, CAP_SVC};

#[test]
fn new_room_is_empty() {
    let room = RoomState::new("alpha".to_string());
    assert_eq!(room.room_id, "alpha");
    assert_eq!(room.member_count(), 0);
    assert!(room.get_capabilities(42).is_none());
}

#[test]
fn insert_and_get_capabilities_roundtrip() {
    let mut room = RoomState::new("r".to_string());
    let caps = CAP_SFU_ROUTING_HEADER | CAP_SUBSCRIPTION;
    room.insert_member(7, caps);

    assert_eq!(room.get_capabilities(7), Some(caps));
    assert_eq!(room.member_count(), 1);
}

#[test]
fn remove_deletes_the_entry() {
    let mut room = RoomState::new("r".to_string());
    room.insert_member(1, CAP_SFU_ROUTING_HEADER);
    assert_eq!(room.member_count(), 1);

    room.remove_member(1);
    assert_eq!(room.member_count(), 0);
    assert!(room.get_capabilities(1).is_none());
}

#[test]
fn remove_missing_is_noop() {
    let mut room = RoomState::new("r".to_string());
    room.remove_member(999); // does not panic
    assert_eq!(room.member_count(), 0);
}

#[test]
fn supports_positive_for_each_bit() {
    let mut room = RoomState::new("r".to_string());
    let caps = CAP_SFU_ROUTING_HEADER | CAP_SVC | CAP_SUBSCRIPTION;
    room.insert_member(1, caps);

    assert!(room.supports(1, CAP_SFU_ROUTING_HEADER));
    assert!(room.supports(1, CAP_SVC));
    assert!(room.supports(1, CAP_SUBSCRIPTION));
}

#[test]
fn supports_negative_when_bit_not_set() {
    let mut room = RoomState::new("r".to_string());
    // Only routing-header set; SVC and SUBSCRIPTION are not.
    room.insert_member(1, CAP_SFU_ROUTING_HEADER);

    assert!(room.supports(1, CAP_SFU_ROUTING_HEADER));
    assert!(!room.supports(1, CAP_SVC));
    assert!(!room.supports(1, CAP_SUBSCRIPTION));
}

#[test]
fn supports_false_for_unknown_member() {
    let room = RoomState::new("r".to_string());
    assert!(!room.supports(42, CAP_SFU_ROUTING_HEADER));
}

#[test]
fn supports_requires_all_bits_in_mask() {
    let mut room = RoomState::new("r".to_string());
    // Member has routing-header but not subscription.
    room.insert_member(1, CAP_SFU_ROUTING_HEADER);

    let combined = CAP_SFU_ROUTING_HEADER | CAP_SUBSCRIPTION;
    assert!(
        !room.supports(1, combined),
        "supports(combined) must require every bit in the mask",
    );
}

#[test]
fn member_count_tracks_insert_and_remove() {
    let mut room = RoomState::new("r".to_string());
    room.insert_member(1, 0);
    room.insert_member(2, 0);
    room.insert_member(3, 0);
    assert_eq!(room.member_count(), 3);

    room.remove_member(2);
    assert_eq!(room.member_count(), 2);

    room.remove_member(1);
    room.remove_member(3);
    assert_eq!(room.member_count(), 0);
}

#[test]
fn insert_overwrites_existing_member() {
    let mut room = RoomState::new("r".to_string());
    room.insert_member(1, CAP_SFU_ROUTING_HEADER);
    room.insert_member(1, CAP_SVC | CAP_SUBSCRIPTION);

    assert_eq!(room.member_count(), 1);
    assert_eq!(room.get_capabilities(1), Some(CAP_SVC | CAP_SUBSCRIPTION),);
}

#[test]
fn senders_excludes_observers() {
    let mut room = RoomState::new("r".to_string());
    room.insert_member(1, CAP_SFU_ROUTING_HEADER);
    room.insert_member(2, CAP_SFU_ROUTING_HEADER);
    room.insert_member(3, CAP_SFU_ROUTING_HEADER);

    // Mark session 2 as an observer via direct field access.
    room.members
        .get_mut(&2)
        .expect("inserted above")
        .is_observer = true;

    let sender_ids: Vec<u64> = {
        let mut ids: Vec<u64> = room.senders().map(|m| m.session_id).collect();
        ids.sort_unstable();
        ids
    };

    assert_eq!(sender_ids, vec![1, 3]);
    // member_count still counts the observer.
    assert_eq!(room.member_count(), 3);
}

#[test]
fn senders_empty_when_all_observers() {
    let mut room = RoomState::new("r".to_string());
    room.insert_member(1, 0);
    room.insert_member(2, 0);
    for m in room.members.values_mut() {
        m.is_observer = true;
    }

    assert_eq!(room.senders().count(), 0);
    assert_eq!(room.member_count(), 2);
}

#[test]
fn default_room_has_empty_id() {
    let room = RoomState::default();
    assert_eq!(room.room_id, "");
    assert_eq!(room.member_count(), 0);
}

#[test]
fn member_entry_defaults_speaker_state() {
    let mut room = RoomState::new("r".to_string());
    room.insert_member(1, CAP_SFU_ROUTING_HEADER);

    let entry = room.members.get(&1).expect("just inserted");
    assert_eq!(entry.last_speaker_score, 0.0);
    assert!(!entry.is_speaking);
    assert!(!entry.is_observer);
    assert_eq!(entry.capabilities, CAP_SFU_ROUTING_HEADER);
}
