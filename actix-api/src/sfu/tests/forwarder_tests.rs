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

//! Unit tests for `sfu::forwarder::Forwarder`.
//!
//! Coverage spans the wave-3 pass-through (self-skip, metrics) and the p3-5
//! AllowSet-driven per-receiver filter that consults [`SubscriptionStore`] +
//! the current active speaker set to decide which MEDIA packets a receiver
//! should actually see.

use std::sync::{Arc, RwLock};
use std::thread;

use tokio::sync::watch;
use videocall_types::frame_marker::REFERENCES_T0;
use videocall_types::protos::diagnostics_packet::BandwidthEstimate;
use videocall_types::protos::media_packet::media_packet::MediaType;
use videocall_types::protos::media_packet::{MediaPacket, RoutingHeader};
use videocall_types::protos::packet_wrapper::packet_wrapper::PacketType;
use videocall_types::protos::packet_wrapper::PacketWrapper;
use videocall_types::protos::subscription_packet::SubscriptionUpdate;

use crate::actors::session_logic::SessionId;
use crate::metrics::{
    SFU_DECIDE_LATENCY_US, SFU_DROPPED_TOTAL, SFU_FORWARDED_TOTAL, SFU_KEYFRAME_FORWARDED_TOTAL,
};
use crate::sfu::forwarder::{ForwardDecision, Forwarder};
use crate::sfu::layer_selector::LayerSelector;
use crate::sfu::room_state::RoomState;
use crate::sfu::speaker::ActiveSpeakerSet;
use crate::sfu::subscription::{AllowSet, LayerPref, SubscriptionStore, MAX_VISIBLE_VIDEO};

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
    let fwd = Forwarder::with_room_only(room);

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
    let fwd = Forwarder::with_room_only(room);

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
    let fwd = Arc::new(Forwarder::with_room_only(room));

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
    let fwd = Forwarder::with_room_only(room);

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
    let fwd = Forwarder::with_room_only(room);

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

// ===========================================================================
// p3-5: AllowSet-driven per-receiver filter
// ===========================================================================
//
// These tests build a fully-wired forwarder (room + SubscriptionStore + a
// watch channel over ActiveSpeakerSet) and assert that `decide()` consults
// the AllowSet correctly for each receiver. They cover the four acceptance
// scenarios in the p3-5 bead description:
//   1. legacy parity: no SubscriptionUpdate → all senders allowed
//   2. pinned subscription with receive_all_audio: AUDIO uncapped, VIDEO filtered
//   3. restrictive subscription: unsubscribed VIDEO senders dropped
//   4. speaker entering active set is allowed even without an explicit pin

/// Build a MEDIA-wrapped packet for `sender_sid` with the given inner
/// MediaType. Returns the wrapper and the parsed MediaPacket separately
/// because `decide()` takes both.
fn build_media(sender_sid: SessionId, media_type: MediaType) -> (PacketWrapper, MediaPacket) {
    let mp = MediaPacket {
        media_type: media_type.into(),
        ..Default::default()
    };
    let mut pw = PacketWrapper::new();
    pw.session_id = sender_sid;
    pw.packet_type = PacketType::MEDIA.into();
    pw.user_id = b"sender@example.com".to_vec();
    // The wrapper's `data` is the serialised inner MediaPacket — but
    // `Forwarder::decide` reads `media_packet` directly, so the wire bytes
    // here aren't asserted on.
    pw.data = b"opaque-media-bytes".to_vec();
    (pw, mp)
}

/// Build a forwarder with `members` in the room and the supplied subscription
/// store / active-speaker snapshot. Returns the forwarder plus the store
/// handle so callers can keep mutating it for staged scenarios.
fn build_wired_forwarder(
    room_name: &str,
    members: &[SessionId],
    speakers: ActiveSpeakerSet,
) -> (Arc<Forwarder>, Arc<RwLock<SubscriptionStore>>) {
    let room = Arc::new(RwLock::new(RoomState::new(room_name.to_string())));
    {
        let mut w = room.write().unwrap();
        for &sid in members {
            w.insert_member(sid, 0);
        }
    }
    let subs = Arc::new(RwLock::new(SubscriptionStore::new()));
    let (tx, rx) = watch::channel(speakers);
    // Keep sender alive for the lifetime of the forwarder — borrow() on the
    // receiver would otherwise return the closed-channel sentinel value.
    std::mem::forget(tx);
    let layer_selector = Arc::new(LayerSelector::new());
    let fwd = Arc::new(Forwarder::new(room, subs.clone(), rx, layer_selector));
    (fwd, subs)
}

/// Like [`build_wired_forwarder`] but also returns the shared `RoomState`
/// handle so the caller can mutate membership / seed a bandwidth estimate
/// after construction (vc-72a co-arrival + layer-budget tests).
fn build_wired_forwarder_with_room(
    room_name: &str,
    members: &[SessionId],
    speakers: ActiveSpeakerSet,
) -> (
    Arc<Forwarder>,
    Arc<RwLock<SubscriptionStore>>,
    Arc<RwLock<RoomState>>,
) {
    let room = Arc::new(RwLock::new(RoomState::new(room_name.to_string())));
    {
        let mut w = room.write().unwrap();
        for &sid in members {
            w.insert_member(sid, 0);
        }
    }
    let subs = Arc::new(RwLock::new(SubscriptionStore::new()));
    let (tx, rx) = watch::channel(speakers);
    std::mem::forget(tx);
    let layer_selector = Arc::new(LayerSelector::new());
    let fwd = Arc::new(Forwarder::new(
        room.clone(),
        subs.clone(),
        rx,
        layer_selector,
    ));
    (fwd, subs, room)
}

/// Build a SubscriptionUpdate with the supplied fields, suitable for handing
/// directly to `SubscriptionStore::apply_update`.
fn sub_update(pinned: &[SessionId], receive_all_audio: bool) -> SubscriptionUpdate {
    let mut u = SubscriptionUpdate::new();
    u.pinned_sessions = pinned.to_vec();
    u.receive_all_audio = receive_all_audio;
    u
}

/// Acceptance #1 — Receiver in default state (no SubscriptionUpdate ever
/// applied) receives every other sender's AUDIO and VIDEO packets. This is
/// the legacy-parity guarantee for un-upgraded clients.
#[test]
fn p3_5_default_receiver_gets_full_fanout() {
    let receiver: SessionId = 100;
    let sender_a: SessionId = 200;
    let sender_b: SessionId = 300;
    let (fwd, _subs) = build_wired_forwarder(
        "p3-5-default",
        &[receiver, sender_a, sender_b],
        ActiveSpeakerSet::empty(),
    );

    // AUDIO from A → forward (legacy default fills audio set).
    let (pw_a_audio, mp_a_audio) = build_media(sender_a, MediaType::AUDIO);
    assert!(matches!(
        fwd.decide(receiver, &pw_a_audio, Some(&mp_a_audio)),
        ForwardDecision::Forward
    ));

    // VIDEO from A → forward (legacy default fills video set).
    let (pw_a_video, mp_a_video) = build_media(sender_a, MediaType::VIDEO);
    assert!(matches!(
        fwd.decide(receiver, &pw_a_video, Some(&mp_a_video)),
        ForwardDecision::Forward
    ));

    // Same for sender_b — both senders are reachable to a default receiver.
    let (pw_b_audio, mp_b_audio) = build_media(sender_b, MediaType::AUDIO);
    assert!(matches!(
        fwd.decide(receiver, &pw_b_audio, Some(&mp_b_audio)),
        ForwardDecision::Forward
    ));
    let (pw_b_video, mp_b_video) = build_media(sender_b, MediaType::VIDEO);
    assert!(matches!(
        fwd.decide(receiver, &pw_b_video, Some(&mp_b_video)),
        ForwardDecision::Forward
    ));
}

/// Acceptance #2 — Receiver pins one sender and opts into receive_all_audio.
/// VIDEO from non-pinned senders is dropped, but AUDIO from every other
/// sender continues to flow (room-wide audio).
#[test]
fn p3_5_pinned_video_with_receive_all_audio() {
    let receiver: SessionId = 100;
    let pinned: SessionId = 200;
    let other: SessionId = 300;
    let (fwd, subs) = build_wired_forwarder(
        "p3-5-pin-audio",
        &[receiver, pinned, other],
        ActiveSpeakerSet::empty(),
    );

    {
        let members = [receiver, pinned, other].into_iter().collect();
        let mut s = subs.write().unwrap();
        s.apply_update(receiver, sub_update(&[pinned], true), &members);
    }

    // VIDEO from pinned sender → forward.
    let (pw_pin_video, mp_pin_video) = build_media(pinned, MediaType::VIDEO);
    assert!(matches!(
        fwd.decide(receiver, &pw_pin_video, Some(&mp_pin_video)),
        ForwardDecision::Forward
    ));

    // VIDEO from the un-pinned sender → drop, AND sfu_dropped_total{reason=unsubscribed}
    // must increment (covers the acceptance metric requirement).
    let before = SFU_DROPPED_TOTAL.with_label_values(&["unsubscribed"]).get();
    let (pw_other_video, mp_other_video) = build_media(other, MediaType::VIDEO);
    assert!(matches!(
        fwd.decide(receiver, &pw_other_video, Some(&mp_other_video)),
        ForwardDecision::Drop
    ));
    let after = SFU_DROPPED_TOTAL.with_label_values(&["unsubscribed"]).get();
    assert!(
        after > before,
        "sfu_dropped_total{{reason=\"unsubscribed\"}} must increment on unsubscribed VIDEO drop: before={before} after={after}"
    );

    // AUDIO from the un-pinned sender → forward (receive_all_audio=true).
    let (pw_other_audio, mp_other_audio) = build_media(other, MediaType::AUDIO);
    assert!(matches!(
        fwd.decide(receiver, &pw_other_audio, Some(&mp_other_audio)),
        ForwardDecision::Forward
    ));
}

/// Acceptance #3 — Receiver declares a restrictive subscription (no pins,
/// no slots, no receive_all_audio). Resolve produces an empty AllowSet so
/// both AUDIO and VIDEO from any other sender are dropped as unsubscribed.
#[test]
fn p3_5_restrictive_subscription_drops_unsubscribed() {
    let receiver: SessionId = 100;
    let sender: SessionId = 200;
    let (fwd, subs) = build_wired_forwarder(
        "p3-5-restrictive",
        &[receiver, sender],
        ActiveSpeakerSet::empty(),
    );

    {
        let members = [receiver, sender].into_iter().collect();
        let mut s = subs.write().unwrap();
        // Empty pinned, no slots, no all-audio — restrictive.
        s.apply_update(receiver, sub_update(&[], false), &members);
    }

    let (pw_video, mp_video) = build_media(sender, MediaType::VIDEO);
    assert!(matches!(
        fwd.decide(receiver, &pw_video, Some(&mp_video)),
        ForwardDecision::Drop
    ));
    let (pw_audio, mp_audio) = build_media(sender, MediaType::AUDIO);
    assert!(matches!(
        fwd.decide(receiver, &pw_audio, Some(&mp_audio)),
        ForwardDecision::Drop
    ));
}

/// Acceptance #4 — A sender entering the active speaker set is admitted to
/// every other receiver's AllowSet automatically, even when that receiver
/// has an explicit subscription that does NOT name them. This is the
/// "speakers always heard" guarantee of the AllowSet resolver.
#[test]
fn p3_5_active_speaker_is_admitted_without_explicit_pin() {
    let receiver: SessionId = 100;
    let speaker: SessionId = 200; // Will rotate into the active set.
    let stranger: SessionId = 300; // Stays out — control sender.

    // Seed the active-speaker channel with `speaker` already promoted.
    let speakers_snap = ActiveSpeakerSet {
        top: Arc::new(vec![speaker]),
        generation: 1,
        ..ActiveSpeakerSet::empty()
    };
    let (fwd, subs) = build_wired_forwarder(
        "p3-5-speaker",
        &[receiver, speaker, stranger],
        speakers_snap,
    );

    // Receiver has an explicit subscription that does NOT pin the speaker —
    // but the speaker tier in resolve() unions them in automatically.
    {
        let members = [receiver, speaker, stranger].into_iter().collect();
        let mut s = subs.write().unwrap();
        s.apply_update(receiver, sub_update(&[], false), &members);
    }

    let (pw_speaker_video, mp_speaker_video) = build_media(speaker, MediaType::VIDEO);
    assert!(
        matches!(
            fwd.decide(receiver, &pw_speaker_video, Some(&mp_speaker_video)),
            ForwardDecision::Forward
        ),
        "active speaker's MEDIA must be admitted even without an explicit pin"
    );

    // Stranger isn't in the speaker set or the receiver's pins → still dropped.
    let (pw_stranger_video, mp_stranger_video) = build_media(stranger, MediaType::VIDEO);
    assert!(matches!(
        fwd.decide(receiver, &pw_stranger_video, Some(&mp_stranger_video)),
        ForwardDecision::Drop
    ));
}

// ===========================================================================
// p4-7: VP9 SVC layer-drop using RoutingHeader temporal/spatial ids
// ===========================================================================
//
// One VP9 L1T3 sender emitting T0/T1/T2 frames; three receivers with
// distinct downlink bandwidth estimates. The LayerSelector budget (with the
// default 0.85 headroom) is:
//   * 200 kbps → 170 effective → only T0 (128) fits → T1+T2 dropped.
//   * 500 kbps → 425 effective → T0 (128) + T1 (+256 = 384) fits, T2
//     (cum 896) does NOT → T2 dropped.
//   * 2000 kbps → 1700 effective → full T0+T1+T2 (cum 896) fits → all
//     temporal layers forwarded.
//
// Keyframes always pass through regardless of the layer budget — this is
// invariant 1 (dropping a keyframe destroys the entire reference chain).

/// Build a MEDIA-wrapped VIDEO packet from `sender` with a `RoutingHeader`
/// indicating the given spatial/temporal layer and keyframe bit.
fn build_video_with_layer(
    sender: SessionId,
    spatial: u32,
    temporal: u32,
    is_keyframe: bool,
) -> (PacketWrapper, MediaPacket) {
    let mut rh = RoutingHeader::new();
    rh.is_keyframe = is_keyframe;
    rh.spatial_layer_id = spatial;
    rh.temporal_layer_id = temporal;
    let mp = MediaPacket {
        media_type: MediaType::VIDEO.into(),
        routing_header: ::protobuf::MessageField::some(rh),
        ..Default::default()
    };
    let mut pw = PacketWrapper::new();
    pw.session_id = sender;
    pw.packet_type = PacketType::MEDIA.into();
    pw.user_id = b"sender@example.com".to_vec();
    pw.data = b"opaque-vp9-bytes".to_vec();
    (pw, mp)
}

/// Seed `receiver`'s most-recent bandwidth estimate on the shared room
/// state. This is what the bandwidth-ingest path
/// (`chat_server` DiagnosticsPacket handler) does in production.
fn set_receiver_bandwidth(room: &Arc<RwLock<RoomState>>, receiver: SessionId, downlink_kbps: u32) {
    let mut est = BandwidthEstimate::new();
    est.estimated_downlink_kbps = downlink_kbps;
    let mut guard = room.write().unwrap();
    // Discarding the invalidate-hint is fine in a test helper that only
    // seeds initial state — the forwarder tests don't exercise the
    // LayerSelector cache-suppression path.
    let _ = guard.update_bandwidth_estimate(receiver, &est);
}

/// Acceptance for p4-7: one sender, three receivers at 200 / 500 / 2000
/// kbps. Assert that each receiver only sees the temporal layers its
/// budget can afford, and that the keyframe always passes through for
/// each receiver regardless of budget.
#[test]
fn p4_7_layer_drop_three_receivers_distinct_budgets() {
    let sender: SessionId = 200;
    let r_tight: SessionId = 100; // 200 kbps → T0 only
    let r_mid: SessionId = 101; // 500 kbps → T0+T1
    let r_fat: SessionId = 102; // 2000 kbps → T0+T1+T2

    // Build the wired forwarder and snapshot the room handle so we can
    // seed bandwidth estimates on each receiver directly.
    let room = Arc::new(RwLock::new(RoomState::new("p4-7-budgets".to_string())));
    {
        let mut w = room.write().unwrap();
        for &sid in &[sender, r_tight, r_mid, r_fat] {
            w.insert_member(sid, 0);
        }
    }
    let subs = Arc::new(RwLock::new(SubscriptionStore::new()));
    // Pin each receiver to ONLY the sender so the LayerSelector budget
    // isn't fragmented across the other receivers (who are members of
    // the room but, in this test, do not themselves send video). The
    // legacy-default AllowSet treats every other member as a candidate
    // sender; an explicit `SubscriptionUpdate` with a single pinned
    // session collapses that to {sender}, matching the realistic
    // "one publisher, three subscribers" scenario p4-7 targets.
    {
        let members: std::collections::HashSet<SessionId> =
            [sender, r_tight, r_mid, r_fat].into_iter().collect();
        let mut s = subs.write().unwrap();
        s.apply_update(r_tight, sub_update(&[sender], true), &members);
        s.apply_update(r_mid, sub_update(&[sender], true), &members);
        s.apply_update(r_fat, sub_update(&[sender], true), &members);
    }
    let (tx, rx) = watch::channel(ActiveSpeakerSet::empty());
    std::mem::forget(tx);
    let layer_selector = Arc::new(LayerSelector::new());
    let fwd = Arc::new(Forwarder::new(
        room.clone(),
        subs.clone(),
        rx,
        layer_selector,
    ));

    // Seed each receiver's downlink budget.
    set_receiver_bandwidth(&room, r_tight, 200);
    set_receiver_bandwidth(&room, r_mid, 500);
    set_receiver_bandwidth(&room, r_fat, 2000);

    // Build the three temporal-layer packets (delta frames, NOT keyframes).
    let (pw_t0, mp_t0) = build_video_with_layer(sender, 0, 0, false);
    let (pw_t1, mp_t1) = build_video_with_layer(sender, 0, 1, false);
    let (pw_t2, mp_t2) = build_video_with_layer(sender, 0, 2, false);

    // --- r_tight (200 kbps): only T0 forwards. ---
    assert!(
        matches!(
            fwd.decide(r_tight, &pw_t0, Some(&mp_t0)),
            ForwardDecision::Forward
        ),
        "T0 base must forward to tight receiver"
    );
    let before_lb = SFU_DROPPED_TOTAL.with_label_values(&["layer_budget"]).get();
    assert!(
        matches!(
            fwd.decide(r_tight, &pw_t1, Some(&mp_t1)),
            ForwardDecision::Drop
        ),
        "T1 must drop for tight receiver (170 kbps budget)"
    );
    let after_lb = SFU_DROPPED_TOTAL.with_label_values(&["layer_budget"]).get();
    assert!(
        after_lb > before_lb,
        "sfu_dropped_total{{reason=\"layer_budget\"}} must increment on T1 drop"
    );
    assert!(
        matches!(
            fwd.decide(r_tight, &pw_t2, Some(&mp_t2)),
            ForwardDecision::Drop
        ),
        "T2 must drop for tight receiver"
    );

    // --- r_mid (500 kbps): T0+T1 forward, T2 drops. ---
    assert!(matches!(
        fwd.decide(r_mid, &pw_t0, Some(&mp_t0)),
        ForwardDecision::Forward
    ));
    assert!(matches!(
        fwd.decide(r_mid, &pw_t1, Some(&mp_t1)),
        ForwardDecision::Forward
    ));
    assert!(
        matches!(
            fwd.decide(r_mid, &pw_t2, Some(&mp_t2)),
            ForwardDecision::Drop
        ),
        "T2 must drop for mid receiver (425 kbps budget, T0+T1=384 fits but cum-T2=896 does not)"
    );

    // --- r_fat (2000 kbps): all temporal layers forward. ---
    for (pw, mp) in [(&pw_t0, &mp_t0), (&pw_t1, &mp_t1), (&pw_t2, &mp_t2)] {
        assert!(
            matches!(fwd.decide(r_fat, pw, Some(mp)), ForwardDecision::Forward),
            "fat receiver (1700 kbps budget) must forward every temporal layer"
        );
    }

    // --- Base-layer (T0+S0) keyframes always pass through, even on the
    //     tightest receiver — invariant 1. A T0+S0 KEYFRAME from the
    //     sender goes to r_tight: must Forward regardless of budget,
    //     because dropping it would break the entire reference chain.
    //     Higher-layer keyframes are tested in `p4_8_*` below — they
    //     are NOT load-bearing for decode and follow the normal budget.
    let (pw_kf_base, mp_kf_base) = build_video_with_layer(sender, 0, 0, true);
    assert!(
        matches!(
            fwd.decide(r_tight, &pw_kf_base, Some(&mp_kf_base)),
            ForwardDecision::Forward
        ),
        "base-layer (T0+S0) keyframes ALWAYS pass through regardless of layer budget (invariant 1)"
    );
}

/// A receiver that has not yet reported a bandwidth estimate must keep
/// receiving every layer (legacy pass-through). The LayerSelector cache
/// is only consulted when there's a budget to compare against; the
/// no-bandwidth path bypasses layer-drop entirely.
#[test]
fn p4_7_no_bandwidth_estimate_disables_layer_drop() {
    let sender: SessionId = 200;
    let receiver: SessionId = 100;

    let (fwd, _subs) =
        build_wired_forwarder("p4-7-no-bw", &[receiver, sender], ActiveSpeakerSet::empty());

    // No `update_bandwidth_estimate` call — receiver has no budget.
    let (pw_t2, mp_t2) = build_video_with_layer(sender, 0, 2, false);
    assert!(
        matches!(
            fwd.decide(receiver, &pw_t2, Some(&mp_t2)),
            ForwardDecision::Forward
        ),
        "without a bandwidth estimate the layer-drop is skipped"
    );
}

/// Legacy clients that don't emit a `RoutingHeader` are unaffected by the
/// p4-7 layer-drop. A receiver WITH a tight bandwidth budget still gets
/// the legacy sender's media forwarded as long as the AllowSet permits.
#[test]
fn p4_7_legacy_no_routing_header_forwards() {
    let sender: SessionId = 200;
    let receiver: SessionId = 100;

    let room = Arc::new(RwLock::new(RoomState::new("p4-7-legacy".to_string())));
    {
        let mut w = room.write().unwrap();
        w.insert_member(sender, 0);
        w.insert_member(receiver, 0);
    }
    let subs = Arc::new(RwLock::new(SubscriptionStore::new()));
    let (tx, rx) = watch::channel(ActiveSpeakerSet::empty());
    std::mem::forget(tx);
    let layer_selector = Arc::new(LayerSelector::new());
    let fwd = Arc::new(Forwarder::new(
        room.clone(),
        subs.clone(),
        rx,
        layer_selector,
    ));
    set_receiver_bandwidth(&room, receiver, 200); // tight, T1+ would drop

    // No RoutingHeader on this VIDEO packet → pass-through.
    let mp = MediaPacket {
        media_type: MediaType::VIDEO.into(),
        ..Default::default()
    };
    let mut pw = PacketWrapper::new();
    pw.session_id = sender;
    pw.packet_type = PacketType::MEDIA.into();
    pw.user_id = b"sender@example.com".to_vec();
    pw.data = b"opaque".to_vec();
    assert!(matches!(
        fwd.decide(receiver, &pw, Some(&mp)),
        ForwardDecision::Forward
    ));
}

// ===========================================================================
// p4-8: always forward keyframe+T0+S0 regardless of budget
// ===========================================================================
//
// p4-7 had a blanket carve-out: ANY keyframe (at any spatial/temporal layer)
// bypassed the layer-budget check. p4-8 tightens that to the base-layer
// keyframe only — T0+S0. Higher-layer keyframes only restart the dependent
// enhancement chain and are not load-bearing for decode, so they go through
// the same budget check as P-frames.
//
// The T0+S0 keyframe is the root of every dependent reference chain. Dropping
// one breaks decode for every subsequent frame until the next keyframe arrives.
// We forward it even when the receiver's bandwidth budget would not otherwise
// admit a packet at all — burning the budget once is the lesser evil.

/// Acceptance: a receiver whose bandwidth estimate is BELOW the T0 budget
/// (10 kbps — well under the 128 kbps T0 cost at default headroom) still
/// receives the base-layer (T0+S0) keyframe. The `sfu_keyframe_forwarded_total`
/// counter must increment so operators can verify keyframes reach receivers.
#[test]
fn p4_8_base_keyframe_forwards_below_t0_budget() {
    let sender: SessionId = 200;
    let receiver: SessionId = 100;

    let room = Arc::new(RwLock::new(RoomState::new("p4-8-base-kf".to_string())));
    {
        let mut w = room.write().unwrap();
        w.insert_member(sender, 0);
        w.insert_member(receiver, 0);
    }
    let subs = Arc::new(RwLock::new(SubscriptionStore::new()));
    {
        let members = [sender, receiver].into_iter().collect();
        let mut s = subs.write().unwrap();
        s.apply_update(receiver, sub_update(&[sender], true), &members);
    }
    let (tx, rx) = watch::channel(ActiveSpeakerSet::empty());
    std::mem::forget(tx);
    let layer_selector = Arc::new(LayerSelector::new());
    let fwd = Arc::new(Forwarder::new(
        room.clone(),
        subs.clone(),
        rx,
        layer_selector,
    ));
    // 10 kbps is well below the T0 budget (128 kbps cumulative cost,
    // 0.85 headroom → would need ~150 kbps downlink). Even a T0 delta
    // frame would drop under this budget; the base-layer KEYFRAME must
    // still forward (invariant 1).
    set_receiver_bandwidth(&room, receiver, 10);

    let before = SFU_KEYFRAME_FORWARDED_TOTAL.get();
    let (pw_kf_base, mp_kf_base) = build_video_with_layer(sender, 0, 0, true);
    assert!(
        matches!(
            fwd.decide(receiver, &pw_kf_base, Some(&mp_kf_base)),
            ForwardDecision::Forward
        ),
        "T0+S0 keyframe must forward even at 10kbps budget (invariant 1)"
    );
    let after = SFU_KEYFRAME_FORWARDED_TOTAL.get();
    assert!(
        after > before,
        "sfu_keyframe_forwarded_total must increment on base-layer keyframe forward (before={before}, after={after})"
    );

    // Sanity: a T0 DELTA frame at the same 10 kbps budget MUST drop —
    // confirming the budget is actually tight enough to trigger the
    // drop path, and that the keyframe-pass is specifically due to
    // the invariant-1 carve-out, not a hole in the budget logic.
    let (pw_t0_delta, mp_t0_delta) = build_video_with_layer(sender, 0, 0, false);
    assert!(
        matches!(
            fwd.decide(receiver, &pw_t0_delta, Some(&mp_t0_delta)),
            ForwardDecision::Drop
        ),
        "T0 delta frame must drop at 10kbps budget (confirms the carve-out is keyframe-specific)"
    );
}

/// A keyframe at a HIGHER layer (T>0 or S>0) is NOT load-bearing for
/// decode in the same way as the T0+S0 root — it only restarts the
/// dependent enhancement chain. p4-8 narrows the always-forward carve-out
/// so higher-layer keyframes follow the same budget rules as P-frames.
#[test]
fn p4_8_higher_layer_keyframe_obeys_budget() {
    let sender: SessionId = 200;
    let receiver: SessionId = 100;

    let room = Arc::new(RwLock::new(RoomState::new("p4-8-higher-kf".to_string())));
    {
        let mut w = room.write().unwrap();
        w.insert_member(sender, 0);
        w.insert_member(receiver, 0);
    }
    let subs = Arc::new(RwLock::new(SubscriptionStore::new()));
    {
        let members = [sender, receiver].into_iter().collect();
        let mut s = subs.write().unwrap();
        s.apply_update(receiver, sub_update(&[sender], true), &members);
    }
    let (tx, rx) = watch::channel(ActiveSpeakerSet::empty());
    std::mem::forget(tx);
    let layer_selector = Arc::new(LayerSelector::new());
    let fwd = Arc::new(Forwarder::new(
        room.clone(),
        subs.clone(),
        rx,
        layer_selector,
    ));
    // 200 kbps → effective 170 kbps after default headroom → only T0
    // (128 kbps cumulative) fits.
    set_receiver_bandwidth(&room, receiver, 200);

    let before_kf = SFU_KEYFRAME_FORWARDED_TOTAL.get();
    let before_lb = SFU_DROPPED_TOTAL.with_label_values(&["layer_budget"]).get();
    let (pw_kf_t2, mp_kf_t2) = build_video_with_layer(sender, 0, 2, true);
    assert!(
        matches!(
            fwd.decide(receiver, &pw_kf_t2, Some(&mp_kf_t2)),
            ForwardDecision::Drop
        ),
        "T2 keyframe must drop at tight budget — only T0+S0 keyframes are invariant 1"
    );
    let after_kf = SFU_KEYFRAME_FORWARDED_TOTAL.get();
    let after_lb = SFU_DROPPED_TOTAL.with_label_values(&["layer_budget"]).get();
    assert_eq!(
        after_kf, before_kf,
        "sfu_keyframe_forwarded_total counts only T0+S0 keyframes — must not increment for T2 keyframe"
    );
    assert!(
        after_lb > before_lb,
        "sfu_dropped_total{{reason=\"layer_budget\"}} must increment on higher-layer keyframe drop"
    );
}

// ===========================================================================
// p4-9: REFERENCES_T0 drop when the referenced T0 was not forwarded
// ===========================================================================
//
// The SFU keeps a per-`(receiver, sender)` bounded set of recently-forwarded
// T0 picture_ids. A T1/T2 delta whose `frame_marker & REFERENCES_T0` bit is
// set but whose `picture_id` is not in that set is dropped with reason
// `reference_miss` — its reference picture was dropped upstream (typically
// by an AllowSet flip mid-stream) and forwarding it would only produce a
// decoder reference error on the client. Keyframes always bypass this check
// because they reset the reference chain (invariant 1).

/// Build a delta VIDEO MediaPacket with explicit `picture_id` and
/// `frame_marker`. Mirrors `build_video_with_layer` but also sets the
/// reference-tracking fields that p4-9 cares about.
fn build_video_ref(
    sender: SessionId,
    temporal: u32,
    picture_id: u64,
    frame_marker: u32,
    is_keyframe: bool,
) -> (PacketWrapper, MediaPacket) {
    let mut rh = RoutingHeader::new();
    rh.is_keyframe = is_keyframe;
    rh.spatial_layer_id = 0;
    rh.temporal_layer_id = temporal;
    rh.picture_id = picture_id;
    rh.frame_marker = frame_marker;
    let mp = MediaPacket {
        media_type: MediaType::VIDEO.into(),
        routing_header: ::protobuf::MessageField::some(rh),
        ..Default::default()
    };
    let mut pw = PacketWrapper::new();
    pw.session_id = sender;
    pw.packet_type = PacketType::MEDIA.into();
    pw.user_id = b"sender@example.com".to_vec();
    pw.data = b"opaque-vp9-bytes".to_vec();
    (pw, mp)
}

/// Acceptance for p4-9:
///   * T1 before any T0 → Drop (reference_miss).
///   * T0 picture_id=X → Forward (and recorded).
///   * T1 picture_id=X referencing T0 → Forward.
///   * T1 picture_id=Y (never seen as T0) → Drop.
///   * Keyframe with picture_id never seen as T0 → Forward (bypass).
#[test]
fn p4_9_t1_dropped_when_t0_not_forwarded() {
    let sender: SessionId = 200;
    let receiver: SessionId = 100;

    // No bandwidth estimate → p4-7 layer-budget is disabled (legacy
    // pass-through). The AllowSet defaults admit every other member, so
    // the only filter in play is the new p4-9 reference check.
    let (fwd, _subs) = build_wired_forwarder(
        "p4-9-references-t0",
        &[receiver, sender],
        ActiveSpeakerSet::empty(),
    );

    // --- 1. T1 with no preceding T0 → Drop (reference_miss). ---
    let before = SFU_DROPPED_TOTAL
        .with_label_values(&["reference_miss"])
        .get();
    let (pw_t1_orphan, mp_t1_orphan) = build_video_ref(sender, 1, 42, REFERENCES_T0, false);
    assert!(
        matches!(
            fwd.decide(receiver, &pw_t1_orphan, Some(&mp_t1_orphan)),
            ForwardDecision::Drop
        ),
        "T1 referencing an unseen T0 must drop"
    );
    let after = SFU_DROPPED_TOTAL
        .with_label_values(&["reference_miss"])
        .get();
    assert!(
        after > before,
        "sfu_dropped_total{{reason=\"reference_miss\"}} must increment on the orphan-T1 drop: before={before} after={after}"
    );

    // --- 2. T0 picture_id=100 → Forward (and recorded). ---
    // T0 deltas don't have the REFERENCES_T0 bit set.
    let (pw_t0, mp_t0) = build_video_ref(sender, 0, 100, 0, false);
    assert!(
        matches!(
            fwd.decide(receiver, &pw_t0, Some(&mp_t0)),
            ForwardDecision::Forward
        ),
        "T0 delta must forward when AllowSet admits the sender"
    );

    // --- 3. T1 picture_id=100 referencing the just-recorded T0 → Forward. ---
    let (pw_t1_ok, mp_t1_ok) = build_video_ref(sender, 1, 100, REFERENCES_T0, false);
    assert!(
        matches!(
            fwd.decide(receiver, &pw_t1_ok, Some(&mp_t1_ok)),
            ForwardDecision::Forward
        ),
        "T1 referencing a recorded T0 must forward"
    );

    // --- 4. T1 picture_id=999 (never seen as T0) → Drop. ---
    let (pw_t1_miss, mp_t1_miss) = build_video_ref(sender, 1, 999, REFERENCES_T0, false);
    assert!(
        matches!(
            fwd.decide(receiver, &pw_t1_miss, Some(&mp_t1_miss)),
            ForwardDecision::Drop
        ),
        "T1 referencing an unseen T0 picture_id must drop even after other T0s were recorded"
    );

    // --- 5. Keyframe with picture_id never seen as T0 → Forward (bypass). ---
    // Keyframes reset the reference chain — they MUST pass through
    // regardless of recent-T0 state. Note the REFERENCES_T0 bit is set
    // here just to prove the keyframe bypass takes precedence over the
    // bit check (a real encoder wouldn't set REFERENCES_T0 on a keyframe).
    //
    // We use a higher-spatial-layer keyframe (S=1) so we don't increment
    // the global SFU_KEYFRAME_FORWARDED_TOTAL counter — that counter
    // tracks only T0+S0 base keyframes (p4-8 invariant) and is asserted
    // on by `p4_8_higher_layer_keyframe_obeys_budget`, which races this
    // test under parallel `cargo test` execution.
    let mut rh_kf = RoutingHeader::new();
    rh_kf.is_keyframe = true;
    rh_kf.spatial_layer_id = 1;
    rh_kf.temporal_layer_id = 0;
    rh_kf.picture_id = 7777;
    rh_kf.frame_marker = REFERENCES_T0;
    let mp_kf = MediaPacket {
        media_type: MediaType::VIDEO.into(),
        routing_header: ::protobuf::MessageField::some(rh_kf),
        ..Default::default()
    };
    let mut pw_kf = PacketWrapper::new();
    pw_kf.session_id = sender;
    pw_kf.packet_type = PacketType::MEDIA.into();
    pw_kf.user_id = b"sender@example.com".to_vec();
    pw_kf.data = b"opaque-vp9-bytes".to_vec();
    assert!(
        matches!(
            fwd.decide(receiver, &pw_kf, Some(&mp_kf)),
            ForwardDecision::Forward
        ),
        "keyframes must bypass the reference-miss check (invariant 1)"
    );
}

/// A T2 (or higher) delta with the REFERENCES_T0 bit set behaves the same
/// way as a T1: the picture_id must have been recorded as a forwarded T0
/// or the packet is dropped.
#[test]
fn p4_9_t2_also_requires_recent_t0() {
    let sender: SessionId = 200;
    let receiver: SessionId = 100;

    let (fwd, _subs) = build_wired_forwarder(
        "p4-9-t2-ref",
        &[receiver, sender],
        ActiveSpeakerSet::empty(),
    );

    // T2 referencing an unseen T0 → drop.
    let (pw_t2_orphan, mp_t2_orphan) = build_video_ref(sender, 2, 11, REFERENCES_T0, false);
    assert!(matches!(
        fwd.decide(receiver, &pw_t2_orphan, Some(&mp_t2_orphan)),
        ForwardDecision::Drop
    ));

    // Forward the matching T0, then the T2 must forward.
    let (pw_t0, mp_t0) = build_video_ref(sender, 0, 11, 0, false);
    assert!(matches!(
        fwd.decide(receiver, &pw_t0, Some(&mp_t0)),
        ForwardDecision::Forward
    ));
    let (pw_t2_ok, mp_t2_ok) = build_video_ref(sender, 2, 11, REFERENCES_T0, false);
    assert!(matches!(
        fwd.decide(receiver, &pw_t2_ok, Some(&mp_t2_ok)),
        ForwardDecision::Forward
    ));
}

/// A T1 whose `frame_marker` does NOT have the `REFERENCES_T0` bit set is
/// not subject to the reference-miss check — the SFU has no claim about
/// what frame it depends on, so we conservatively let it through.
#[test]
fn p4_9_t1_without_references_bit_is_passthrough() {
    let sender: SessionId = 200;
    let receiver: SessionId = 100;

    let (fwd, _subs) = build_wired_forwarder(
        "p4-9-no-ref-bit",
        &[receiver, sender],
        ActiveSpeakerSet::empty(),
    );

    // frame_marker = 0 → no REFERENCES_T0 bit, even though temporal_layer_id
    // is 1. Real encoders always set the bit, but the SFU must not assume.
    let (pw, mp) = build_video_ref(sender, 1, 1234, 0, false);
    assert!(matches!(
        fwd.decide(receiver, &pw, Some(&mp)),
        ForwardDecision::Forward
    ));
}

/// Legacy clients that don't emit a `RoutingHeader` at all are unaffected
/// — same carve-out as p4-7. A VIDEO MediaPacket with no RoutingHeader
/// passes straight through the AllowSet to the forward branch.
#[test]
fn p4_9_legacy_no_routing_header_passthrough() {
    let sender: SessionId = 200;
    let receiver: SessionId = 100;

    let (fwd, _subs) = build_wired_forwarder(
        "p4-9-legacy",
        &[receiver, sender],
        ActiveSpeakerSet::empty(),
    );

    let mp = MediaPacket {
        media_type: MediaType::VIDEO.into(),
        ..Default::default()
    };
    let mut pw = PacketWrapper::new();
    pw.session_id = sender;
    pw.packet_type = PacketType::MEDIA.into();
    pw.user_id = b"sender@example.com".to_vec();
    pw.data = b"opaque".to_vec();
    assert!(matches!(
        fwd.decide(receiver, &pw, Some(&mp)),
        ForwardDecision::Forward
    ));
}

/// vc-78q: `Forwarder::prune_session` removes every recent-T0 pair
/// that touches the departing session (as receiver OR as sender) and
/// leaves pairs that don't touch it alone. Idempotent.
#[test]
fn vc_78q_prune_session_clears_recent_t0_for_departed_sid() {
    let a: SessionId = 100;
    let b: SessionId = 200;
    let c: SessionId = 300;

    // Three members, no bandwidth estimate set, default AllowSet —
    // every T0 will be forwarded and recorded under the (receiver,
    // sender) key it was decided for.
    let (fwd, _subs) = build_wired_forwarder("vc-78q-prune", &[a, b, c], ActiveSpeakerSet::empty());

    // Forward a T0 in every directed pair so the recent_t0 map ends
    // up with 6 entries: (rcv, snd) for every ordered (rcv != snd).
    let mut picture_id: u64 = 1;
    for &rcv in &[a, b, c] {
        for &snd in &[a, b, c] {
            if rcv == snd {
                continue;
            }
            let (pw, mp) = build_video_ref(snd, 0, picture_id, 0, false);
            assert!(
                matches!(fwd.decide(rcv, &pw, Some(&mp)), ForwardDecision::Forward),
                "T0 for (rcv={rcv}, snd={snd}) must forward"
            );
            picture_id += 1;
        }
    }
    assert_eq!(fwd.recent_t0_pair_count(), 6);

    // Prune `b`: every pair involving `b` (as receiver or sender)
    // must vanish. The remaining (a, c) and (c, a) pairs must stay.
    fwd.prune_session(b);
    assert_eq!(fwd.recent_t0_pair_count(), 2);
    assert!(fwd.recent_t0_contains_pair(a, c));
    assert!(fwd.recent_t0_contains_pair(c, a));
    assert!(!fwd.recent_t0_contains_pair(a, b));
    assert!(!fwd.recent_t0_contains_pair(b, a));
    assert!(!fwd.recent_t0_contains_pair(b, c));
    assert!(!fwd.recent_t0_contains_pair(c, b));

    // Idempotent: pruning a sid that has no remaining state is a no-op.
    fwd.prune_session(b);
    assert_eq!(fwd.recent_t0_pair_count(), 2);

    // Pruning `a` and then `c` empties the map entirely.
    fwd.prune_session(a);
    fwd.prune_session(c);
    assert_eq!(fwd.recent_t0_pair_count(), 0);
}

/// vc-78q: `Forwarder::prune_session` also reaps `LayerSelector`
/// per-receiver state for the departing sid (hysteresis + cached
/// selection). Cross-checked via `LayerSelector::last_selection_for`.
#[test]
fn vc_78q_prune_session_clears_layer_selector_state() {
    let receiver: SessionId = 100;
    let sender: SessionId = 200;

    let room = Arc::new(RwLock::new(RoomState::new("vc-78q-ls".to_string())));
    {
        let mut w = room.write().unwrap();
        w.insert_member(receiver, 0);
        w.insert_member(sender, 0);
    }
    let subs = Arc::new(RwLock::new(SubscriptionStore::new()));
    let (tx, rx) = watch::channel(ActiveSpeakerSet::empty());
    std::mem::forget(tx);
    let layer_selector = Arc::new(LayerSelector::new());
    let fwd = Forwarder::new(room, subs, rx, layer_selector.clone());

    // Seed the cached selection for `receiver`. Bandwidth=2000 kbps
    // is comfortably above the L1T3 ladder so at least the sender's
    // T0 will be admitted.
    let mut allow = AllowSet::new();
    allow.video.insert(sender, LayerPref::default());
    allow.audio.insert(sender);
    layer_selector.recompute_for_receiver(receiver, &allow, &[sender], 2000, 0);
    assert!(
        layer_selector.last_selection_for(receiver).is_some(),
        "precondition: receiver should have a cached selection after recompute"
    );

    fwd.prune_session(receiver);
    assert!(
        layer_selector.last_selection_for(receiver).is_none(),
        "prune_session must drop the receiver's cached LayerSelector state"
    );
}

// ===========================================================================
// vc-72a: T=0 co-arrival — a listener present before/with the first publisher
// must receive that publisher's media, including when the publisher is NOT a
// local room member (cross-pod co-arrival, or the brief window before a
// same-pod sender's `insert_member` lands). The AllowSet is membership-bound,
// so a receiver in receive-all mode falls back to admitting any sender whose
// media actually reached this pod.
// ===========================================================================

/// vc-72a: a bot listener that never sent a `SubscriptionUpdate`
/// (legacy-default → implicit "receive everyone") must receive the media of a
/// publisher that is NOT in this pod's local member snapshot. This is the
/// zero-media regression: in a multi-pod deployment the publisher joined a
/// different pod, so it never appears in `current_members` here, yet its media
/// is delivered over NATS and the listener must get it.
#[test]
fn vc_72a_co_arrival_no_update_admits_non_member_publisher() {
    let listener: SessionId = 1;
    let publisher: SessionId = 2;
    // Only the listener is a LOCAL member; the publisher joined elsewhere.
    // Give the listener a realistic bandwidth estimate (the normal real-world
    // case) so the layer-budget stage actually engages — this is what made
    // the original fix's video half a no-op (keyframes only).
    let (fwd, _subs, room) =
        build_wired_forwarder_with_room("vc-72a-no-update", &[listener], ActiveSpeakerSet::empty());
    set_receiver_bandwidth(&room, listener, 2000); // fat pipe → T0+T1+T2 fit

    let (pw_audio, mp_audio) = build_media(publisher, MediaType::AUDIO);
    assert!(
        matches!(
            fwd.decide(listener, &pw_audio, Some(&mp_audio)),
            ForwardDecision::Forward
        ),
        "no-update listener must hear a non-member publisher's audio (cross-pod co-arrival)"
    );

    // Base keyframe (T0+S0) — always forwards (invariant 1), even pre-fix.
    let (pw_kf, mp_kf) = build_video_with_layer(publisher, 0, 0, true);
    assert!(
        matches!(
            fwd.decide(listener, &pw_kf, Some(&mp_kf)),
            ForwardDecision::Forward
        ),
        "no-update listener must see a non-member publisher's base keyframe"
    );

    // Non-keyframe T1 delta — this is the real test. Before the layer-budget
    // fix this was DROPPED (sender absent from membership-bound allow.video →
    // ordered_senders filters it out → no budget entry → drop), leaving the
    // listener with frozen video. With a fat 2000 kbps budget the augmented
    // pick_layers allocates the non-member T0+T1+T2, so a T1 must forward.
    let (pw_t1, mp_t1) = build_video_with_layer(publisher, 0, 1, false);
    assert!(
        matches!(
            fwd.decide(listener, &pw_t1, Some(&mp_t1)),
            ForwardDecision::Forward
        ),
        "no-update listener must receive a non-member publisher's NON-keyframe \
         video (T1) — not just periodic keyframes (vc-72a video half)"
    );
}

/// vc-72a: a listener that declared an empty subscription but with
/// `receive_all_audio=true` / `receive_all_video=true` (the
/// `SubscriptionCoalescer`'s opening flush) must likewise admit a non-member
/// publisher's media. The receive-all flags express intent independent of
/// local membership.
#[test]
fn vc_72a_co_arrival_receive_all_admits_non_member_publisher() {
    let listener: SessionId = 100;
    let publisher: SessionId = 200;
    let (fwd, subs, room) = build_wired_forwarder_with_room(
        "vc-72a-receive-all",
        &[listener],
        ActiveSpeakerSet::empty(),
    );
    set_receiver_bandwidth(&room, listener, 2000);

    // Opening empty update with both receive-all flags set. Note the room
    // snapshot at apply time contains only the listener.
    {
        let members = [listener].into_iter().collect();
        let mut s = subs.write().unwrap();
        let mut u = SubscriptionUpdate::new();
        u.receive_all_audio = true;
        u.receive_all_video = true;
        s.apply_update(listener, u, &members);
    }

    let (pw_audio, mp_audio) = build_media(publisher, MediaType::AUDIO);
    assert!(
        matches!(
            fwd.decide(listener, &pw_audio, Some(&mp_audio)),
            ForwardDecision::Forward
        ),
        "receive_all_audio listener must hear a non-member publisher"
    );
    // Non-keyframe T1 from a non-member publisher must forward through the
    // layer-budget stage (not just keyframes).
    let (pw_t1, mp_t1) = build_video_with_layer(publisher, 0, 1, false);
    assert!(
        matches!(
            fwd.decide(listener, &pw_t1, Some(&mp_t1)),
            ForwardDecision::Forward
        ),
        "receive_all_video listener must see a non-member publisher's NON-keyframe video"
    );
}

/// vc-72a layer-budget: a non-member publisher admitted via the receive-all
/// fallback is still subject to the receiver's SVC budget. At a TIGHT 200 kbps
/// downlink only T0 (128 kbps) fits, so a T1/T2 non-keyframe must be dropped
/// with `layer_budget` — exactly like a local member. This proves the
/// augmented `pick_layers` path applies the budget, not a blanket admit.
#[test]
fn vc_72a_non_member_video_obeys_tight_layer_budget() {
    let listener: SessionId = 1;
    let publisher: SessionId = 2; // non-member
    let (fwd, _subs, room) = build_wired_forwarder_with_room(
        "vc-72a-tight-budget",
        &[listener],
        ActiveSpeakerSet::empty(),
    );
    set_receiver_bandwidth(&room, listener, 200); // 170 effective → T0 only

    // T0 delta forwards (fits the budget).
    let (pw_t0, mp_t0) = build_video_with_layer(publisher, 0, 0, false);
    assert!(
        matches!(
            fwd.decide(listener, &pw_t0, Some(&mp_t0)),
            ForwardDecision::Forward
        ),
        "non-member T0 must fit a 200 kbps budget"
    );

    // T1 delta does NOT fit → dropped as layer_budget.
    let before = SFU_DROPPED_TOTAL.with_label_values(&["layer_budget"]).get();
    let (pw_t1, mp_t1) = build_video_with_layer(publisher, 0, 1, false);
    assert!(
        matches!(
            fwd.decide(listener, &pw_t1, Some(&mp_t1)),
            ForwardDecision::Drop
        ),
        "non-member T1 must be dropped at a 200 kbps budget (T0 only)"
    );
    let after = SFU_DROPPED_TOTAL.with_label_values(&["layer_budget"]).get();
    assert!(
        after > before,
        "dropped non-member T1 must increment sfu_dropped_total{{layer_budget}}"
    );
}

/// vc-72a cap interaction: the receive-all non-member fallback MUST honor
/// MAX_VISIBLE_VIDEO. With the membership-bound AllowSet already at the cap
/// (6 local member publishers, all visible via the legacy-default fan-out),
/// a 7th NON-member publisher's video must be dropped — otherwise a
/// receive-all receiver in a >6-publisher multi-pod room could be flooded
/// with unbounded cross-pod video streams.
#[test]
fn vc_72a_non_member_video_respects_max_visible_cap() {
    let listener: SessionId = 1;
    // 6 local member publishers fill the cap for the no-update listener.
    let local_pubs: Vec<SessionId> = (10..16).collect(); // exactly MAX_VISIBLE_VIDEO
    let mut members = vec![listener];
    members.extend(&local_pubs);
    let (fwd, _subs, room) =
        build_wired_forwarder_with_room("vc-72a-cap", &members, ActiveSpeakerSet::empty());
    set_receiver_bandwidth(&room, listener, 100_000); // huge budget — cap, not budget, must bind

    // Sanity: the membership-bound AllowSet already holds MAX_VISIBLE_VIDEO
    // entries, so the cap is full.
    assert_eq!(local_pubs.len(), MAX_VISIBLE_VIDEO as usize);

    // A local member's video forwards (it is within the cap).
    let (pw_member, mp_member) = build_video_with_layer(local_pubs[0], 0, 1, false);
    assert!(
        matches!(
            fwd.decide(listener, &pw_member, Some(&mp_member)),
            ForwardDecision::Forward
        ),
        "a capped local member's video must still forward"
    );

    // A 7th NON-member publisher's video must be DROPPED — cap is full.
    let non_member: SessionId = 999;
    let before = SFU_DROPPED_TOTAL.with_label_values(&["unsubscribed"]).get();
    let (pw_extra, mp_extra) = build_video_with_layer(non_member, 0, 1, false);
    assert!(
        matches!(
            fwd.decide(listener, &pw_extra, Some(&mp_extra)),
            ForwardDecision::Drop
        ),
        "a non-member video admit beyond MAX_VISIBLE_VIDEO must be dropped"
    );
    let after = SFU_DROPPED_TOTAL.with_label_values(&["unsubscribed"]).get();
    assert!(
        after > before,
        "the capped-out non-member video drop must count as unsubscribed"
    );

    // Audio is NOT subject to the video cap — the 7th publisher is still
    // audible (receive-all audio).
    let (pw_audio, mp_audio) = build_media(non_member, MediaType::AUDIO);
    assert!(
        matches!(
            fwd.decide(listener, &pw_audio, Some(&mp_audio)),
            ForwardDecision::Forward
        ),
        "non-member audio is not subject to the video cap"
    );
}

/// vc-72a regression guard: the receive-all fallback must NOT leak media to a
/// listener that declared a genuinely restrictive subscription (both
/// receive-all flags false, no pins/slots). Such a receiver still gets the
/// membership-bound AllowSet and a non-subscribed sender is dropped.
#[test]
fn vc_72a_restrictive_subscription_still_drops_non_member() {
    let listener: SessionId = 100;
    let publisher: SessionId = 200;
    let (fwd, subs) = build_wired_forwarder(
        "vc-72a-restrictive",
        &[listener, publisher],
        ActiveSpeakerSet::empty(),
    );

    {
        let members = [listener, publisher].into_iter().collect();
        let mut s = subs.write().unwrap();
        // Restrictive: empty pins, no slots, both receive-all flags false.
        s.apply_update(listener, sub_update(&[], false), &members);
    }

    let (pw_audio, mp_audio) = build_media(publisher, MediaType::AUDIO);
    assert!(
        matches!(
            fwd.decide(listener, &pw_audio, Some(&mp_audio)),
            ForwardDecision::Drop
        ),
        "restrictive listener must NOT receive an unsubscribed sender's audio"
    );
    let (pw_video, mp_video) = build_media(publisher, MediaType::VIDEO);
    assert!(
        matches!(
            fwd.decide(listener, &pw_video, Some(&mp_video)),
            ForwardDecision::Drop
        ),
        "restrictive listener must NOT receive an unsubscribed sender's video"
    );
}

/// vc-72a: ordering test (listener registers, THEN publisher registers and
/// produces). Mirrors the staircase shard-A shape: the listener is present
/// first, the publisher joins as a local member afterward, and the AllowSet
/// must expand to deliver its media on the very first packet.
#[test]
fn vc_72a_listener_first_then_publisher_joins_local() {
    let listener: SessionId = 1;
    let publisher: SessionId = 2;
    let room = Arc::new(RwLock::new(RoomState::new("vc-72a-order".to_string())));
    {
        let mut w = room.write().unwrap();
        w.insert_member(listener, 0);
    }
    let subs = Arc::new(RwLock::new(SubscriptionStore::new()));
    let (tx, rx) = watch::channel(ActiveSpeakerSet::empty());
    std::mem::forget(tx);
    let fwd = Arc::new(Forwarder::new(
        room.clone(),
        subs,
        rx,
        Arc::new(LayerSelector::new()),
    ));

    // Publisher joins as a local member after the listener is already present.
    {
        let mut w = room.write().unwrap();
        w.insert_member(publisher, 0);
    }

    let (pw_audio, mp_audio) = build_media(publisher, MediaType::AUDIO);
    assert!(
        matches!(
            fwd.decide(listener, &pw_audio, Some(&mp_audio)),
            ForwardDecision::Forward
        ),
        "listener-first then publisher-joins: first audio packet must forward"
    );
}
