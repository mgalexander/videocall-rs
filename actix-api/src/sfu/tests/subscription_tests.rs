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

//! Reconciliation matrix unit tests for `sfu::subscription` (bead vc-6lb, p3-10).
//!
//! Locks down the seven-category coverage matrix from ADR-0003:
//!   1. Default AllowSet (no update sent)
//!   2. Pin reconciliation (in-room / stale / pre-join / pending cap)
//!   3. Slot reconciliation (with prefs / non-member dropped)
//!   4. Speaker-set inclusion (alone / union with pins)
//!   5. Sizing caps (MAX_VISIBLE_VIDEO video cap, audio uncapped when
//!      `receive_all_audio=true`)
//!   6. Declarative replace semantics (pin=[A] then pin=[B] => {B}, not {A,B})
//!   7. forget() returns receiver to legacy-default AllowSet.

use std::collections::HashSet;

use videocall_types::protos::subscription_packet::{SubscriptionUpdate, VisibilitySlot};

use crate::actors::session_logic::SessionId;
use crate::sfu::subscription::{
    AllowSet, LayerPref, SubscriptionStore, MAX_VISIBLE_VIDEO, PENDING_CAP,
};

// ---------- helpers ----------

fn members(ids: &[SessionId]) -> HashSet<SessionId> {
    ids.iter().copied().collect()
}

fn slot(session_id: SessionId, spatial: u32, temporal: u32) -> VisibilitySlot {
    let mut s = VisibilitySlot::new();
    s.session_id = session_id;
    s.preferred_spatial = spatial;
    s.preferred_temporal = temporal;
    s
}

fn update(
    pinned: &[SessionId],
    slots: Vec<VisibilitySlot>,
    receive_all_audio: bool,
) -> SubscriptionUpdate {
    let mut u = SubscriptionUpdate::new();
    u.pinned_sessions = pinned.to_vec();
    u.slots = slots;
    u.max_video_kbps = 0;
    u.receive_all_audio = receive_all_audio;
    u
}

fn video_ids(allow: &AllowSet) -> Vec<SessionId> {
    let mut v: Vec<SessionId> = allow.video.keys().copied().collect();
    v.sort_unstable();
    v
}

// ---------- 1. Default AllowSet (no update) ----------

#[test]
fn matrix_1_default_allowset_covers_all_members_minus_self() {
    let store = SubscriptionStore::new();
    let room = members(&[1, 2, 3, 4]);

    let allow = store.resolve(1, &room, &[]);

    assert_eq!(video_ids(&allow), vec![2, 3, 4]);
    for sid in [2, 3, 4] {
        assert_eq!(
            allow.video.get(&sid),
            Some(&LayerPref::default()),
            "default AllowSet must use base-layer prefs for {sid}"
        );
    }
    assert_eq!(
        allow.audio,
        [2, 3, 4].into_iter().collect::<HashSet<_>>(),
        "default AllowSet audio must mirror video membership"
    );
}

// ---------- 2. Pin reconciliation ----------

#[test]
fn matrix_2a_pin_in_room_lands_in_allowset_video() {
    let mut store = SubscriptionStore::new();
    let room = members(&[1, 2, 3]);
    store.apply_update(1, update(&[2], vec![], false), &room);

    let allow = store.resolve(1, &room, &[]);
    assert_eq!(video_ids(&allow), vec![2]);
    assert_eq!(allow.video.get(&2), Some(&LayerPref::default()));
}

#[test]
fn matrix_2b_pin_not_in_room_dropped_from_current_allowset() {
    let mut store = SubscriptionStore::new();
    let room = members(&[1, 2]); // 99 not in room
    store.apply_update(1, update(&[2, 99], vec![], false), &room);

    let allow = store.resolve(1, &room, &[]);
    // 99 is silently dropped from the current AllowSet (parked in pending).
    assert!(!allow.video.contains_key(&99));
    assert_eq!(video_ids(&allow), vec![2]);
}

#[test]
fn matrix_2c_pre_join_pin_promoted_when_sender_joins() {
    let mut store = SubscriptionStore::new();
    let room_before = members(&[1, 2]); // 5 not yet joined

    // Receiver 1 pins 5 before 5 has joined.
    store.apply_update(1, update(&[5], vec![], false), &room_before);
    assert!(
        !store.resolve(1, &room_before, &[]).video.contains_key(&5),
        "pre-join pin must NOT appear until sender is in the room"
    );

    // 5 joins. Receiver sends a fresh (empty) update — pending must be promoted.
    let room_after = members(&[1, 2, 5]);
    store.apply_update(1, update(&[], vec![], false), &room_after);

    let allow = store.resolve(1, &room_after, &[]);
    assert!(
        allow.video.contains_key(&5),
        "pre-join pin must be promoted on the next apply_update once the sender has joined"
    );
}

#[test]
fn matrix_2d_pending_cap_drops_oldest_at_51() {
    // Sanity check the cap value the spec mentions explicitly.
    assert_eq!(PENDING_CAP, 50, "spec assumes PENDING_CAP == 50");

    let mut store = SubscriptionStore::new();
    let just_receiver = members(&[1]);

    // Submit 51 unknown pins: 1000 is oldest, 1050 is newest. Drop-oldest
    // means 1000 should be evicted from pending while 1001..=1050 survive.
    let pins: Vec<SessionId> = (1000..=1050).collect();
    assert_eq!(pins.len(), 51);
    store.apply_update(1, update(&pins, vec![], false), &just_receiver);

    // Now 1000 (oldest, should have been evicted) AND 1001 (second-oldest,
    // should have survived the drop-oldest eviction) both join. With
    // drop-oldest, 1000 must NOT be promoted but 1001 MUST be — this rules
    // out a bug that incidentally drops the wrong entry.
    let room_with_two = members(&[1, 1000, 1001]);
    store.apply_update(1, update(&[], vec![], false), &room_with_two);

    let allow = store.resolve(1, &room_with_two, &[]);
    assert!(
        !allow.video.contains_key(&1000),
        "the 51st-submitted unknown pin (oldest, id=1000) must have been evicted from pending"
    );
    assert!(
        allow.video.contains_key(&1001),
        "the second-oldest pending id (1001) must survive eviction and promote"
    );

    // Counter-test: 50 pending entries (no overflow) must all survive. The
    // oldest entry, 1000, must be promoted when it later joins.
    let mut store_50 = SubscriptionStore::new();
    let pins_50: Vec<SessionId> = (1000..1050).collect();
    assert_eq!(pins_50.len(), 50);
    store_50.apply_update(1, update(&pins_50, vec![], false), &just_receiver);

    let room_with_oldest = members(&[1, 1000]);
    store_50.apply_update(1, update(&[], vec![], false), &room_with_oldest);

    let allow_50 = store_50.resolve(1, &room_with_oldest, &[]);
    assert!(
        allow_50.video.contains_key(&1000),
        "with exactly PENDING_CAP unknowns, the oldest entry must survive and promote"
    );
}

// ---------- 3. Slot reconciliation ----------

#[test]
fn matrix_3a_slot_in_room_carries_declared_layer_pref() {
    let mut store = SubscriptionStore::new();
    let room = members(&[1, 2, 3]);

    // Slot {session=3, spatial=1, temporal=2}.
    store.apply_update(1, update(&[], vec![slot(3, 1, 2)], false), &room);

    let allow = store.resolve(1, &room, &[]);
    assert_eq!(video_ids(&allow), vec![3]);
    assert_eq!(
        allow.video.get(&3),
        Some(&LayerPref {
            preferred_spatial: 1,
            preferred_temporal: 2,
        })
    );
}

#[test]
fn matrix_3b_slot_for_non_member_silently_dropped() {
    let mut store = SubscriptionStore::new();
    let room = members(&[1, 2]); // 99 not in room

    store.apply_update(
        1,
        update(&[], vec![slot(2, 1, 1), slot(99, 3, 3)], false),
        &room,
    );

    let allow = store.resolve(1, &room, &[]);
    assert!(!allow.video.contains_key(&99));
    assert_eq!(video_ids(&allow), vec![2]);
}

#[test]
fn matrix_3c_slot_pref_wins_over_pinned_default_for_same_sender() {
    let mut store = SubscriptionStore::new();
    let room = members(&[1, 2, 3]);

    // Sender 2 is BOTH pinned and slotted. The slot's declared LayerPref must
    // win over the base-layer default that a bare pin would apply.
    store.apply_update(1, update(&[2, 3], vec![slot(2, 1, 2)], false), &room);

    let allow = store.resolve(1, &room, &[]);
    assert_eq!(video_ids(&allow), vec![2, 3]);
    assert_eq!(
        allow.video.get(&2),
        Some(&LayerPref {
            preferred_spatial: 1,
            preferred_temporal: 2,
        }),
        "slot LayerPref must win when the same sender is also pinned"
    );
    assert_eq!(
        allow.video.get(&3),
        Some(&LayerPref::default()),
        "pinned-only sender keeps the base-layer default"
    );
}

// ---------- 4. Speaker-set inclusion ----------

#[test]
fn matrix_4a_speaker_set_alone_populates_allowset_video() {
    let mut store = SubscriptionStore::new();
    let room = members(&[1, 2, 3, 4]);

    // Receiver issued an empty update (declarative — no pins, no slots).
    store.apply_update(1, update(&[], vec![], false), &room);

    let allow = store.resolve(1, &room, &[2, 3]);
    assert_eq!(video_ids(&allow), vec![2, 3]);
    for sid in [2, 3] {
        assert_eq!(
            allow.video.get(&sid),
            Some(&LayerPref::default()),
            "speaker-only entries fall back to base-layer prefs"
        );
    }
}

#[test]
fn matrix_4b_speaker_set_union_with_pinned_yields_combined_video() {
    let mut store = SubscriptionStore::new();
    let room = members(&[1, 2, 3, 4]);

    // pinned=[4], speaker_set=[2, 3] => video should contain 2, 3, 4.
    store.apply_update(1, update(&[4], vec![], false), &room);

    let allow = store.resolve(1, &room, &[2, 3]);
    assert_eq!(video_ids(&allow), vec![2, 3, 4]);
}

// ---------- 5. Sizing caps ----------

#[test]
fn matrix_5a_max_visible_video_caps_video_at_six() {
    let mut store = SubscriptionStore::new();
    let pins: Vec<SessionId> = (10..20).collect(); // 10 senders
    let mut all = pins.clone();
    all.push(1);
    let room = members(&all);

    // receive_all_audio=false: audio mirrors capped video.
    store.apply_update(1, update(&pins, vec![], false), &room);
    let allow = store.resolve(1, &room, &[]);

    assert_eq!(allow.video.len(), MAX_VISIBLE_VIDEO as usize);
    assert_eq!(
        allow.audio.len(),
        MAX_VISIBLE_VIDEO as usize,
        "with receive_all_audio=false, audio must mirror capped video"
    );
}

#[test]
fn matrix_5b_receive_all_audio_uncapped_even_when_video_is_capped() {
    let mut store = SubscriptionStore::new();
    let pins: Vec<SessionId> = (10..20).collect(); // 10 senders
    let mut all = pins.clone();
    all.push(1);
    let room = members(&all);

    // receive_all_audio=true: audio covers all senders (minus self), even
    // while video is capped at MAX_VISIBLE_VIDEO.
    store.apply_update(1, update(&pins, vec![], true), &room);
    let allow = store.resolve(1, &room, &[]);

    assert_eq!(allow.video.len(), MAX_VISIBLE_VIDEO as usize);
    assert_eq!(
        allow.audio.len(),
        pins.len(),
        "receive_all_audio=true must include all members minus self"
    );
    assert!(
        !allow.audio.contains(&1),
        "receiver itself must never appear in its own AllowSet"
    );
}

#[test]
fn matrix_5c_receiver_excluded_from_own_video_and_audio() {
    let mut store = SubscriptionStore::new();
    let room = members(&[1, 2, 3]);

    // Receiver 1 self-pins, self-slots, and the speaker_set names it too.
    // None of those paths may smuggle the receiver into its own AllowSet.
    store.apply_update(1, update(&[1, 2], vec![slot(1, 2, 2)], true), &room);

    let allow = store.resolve(1, &room, &[1, 3]);
    assert!(
        !allow.video.contains_key(&1),
        "receiver must NEVER appear in its own video AllowSet (self-pin / self-slot / self-speaker)"
    );
    assert!(
        !allow.audio.contains(&1),
        "receiver must NEVER appear in its own audio AllowSet"
    );
    // Sanity: legitimate others still make it through.
    assert!(allow.video.contains_key(&2), "pinned peer must be present");
    assert!(allow.video.contains_key(&3), "speaker peer must be present");
}

// ---------- 6. Declarative replace semantics ----------

#[test]
fn matrix_6_replace_semantics_second_update_supersedes_first() {
    let mut store = SubscriptionStore::new();
    let room = members(&[1, 2, 3]);

    // First update: pinned=[2].
    store.apply_update(1, update(&[2], vec![], false), &room);
    assert_eq!(
        video_ids(&store.resolve(1, &room, &[])),
        vec![2],
        "first update must seed AllowSet with sender 2"
    );

    // Second update: pinned=[3]. Declarative replace -> AllowSet contains 3
    // only; 2 must be gone (not unioned in).
    store.apply_update(1, update(&[3], vec![], false), &room);

    let allow = store.resolve(1, &room, &[]);
    assert_eq!(video_ids(&allow), vec![3]);
    assert!(
        !allow.video.contains_key(&2),
        "declarative replace must DROP prior pin 2 when superseded by pin [3]"
    );
}

// ---------- 7. forget() ----------

#[test]
fn matrix_7_forget_returns_receiver_to_legacy_default() {
    let mut store = SubscriptionStore::new();
    let room = members(&[1, 2, 3]);

    // Seed receiver 1 with a non-default subscription.
    store.apply_update(1, update(&[2], vec![], false), &room);
    let seeded = store.resolve(1, &room, &[]);
    assert_eq!(
        video_ids(&seeded),
        vec![2],
        "sanity: stored state should yield AllowSet=[2] before forget()"
    );

    store.forget(1);

    // After forget(), the receiver behaves as if it never sent an update —
    // resolve returns the legacy-default AllowSet covering all members.
    let allow = store.resolve(1, &room, &[]);
    assert_eq!(video_ids(&allow), vec![2, 3]);
    for sid in [2, 3] {
        assert_eq!(allow.video.get(&sid), Some(&LayerPref::default()));
    }
    assert_eq!(allow.audio, [2, 3].into_iter().collect::<HashSet<_>>());
}
