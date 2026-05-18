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
use videocall_types::protos::media_packet::media_packet::MediaType;
use videocall_types::protos::media_packet::MediaPacket;
use videocall_types::protos::packet_wrapper::packet_wrapper::PacketType;
use videocall_types::protos::packet_wrapper::PacketWrapper;
use videocall_types::protos::subscription_packet::SubscriptionUpdate;

use crate::actors::session_logic::SessionId;
use crate::metrics::{SFU_DECIDE_LATENCY_US, SFU_DROPPED_TOTAL, SFU_FORWARDED_TOTAL};
use crate::sfu::forwarder::{ForwardDecision, Forwarder};
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
    let fwd = Arc::new(Forwarder::new(room, subs.clone(), rx));
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
        top: vec![speaker],
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
