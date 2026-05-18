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
use crate::sfu::subscription::SubscriptionStore;

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
    guard.update_bandwidth_estimate(receiver, &est);
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
