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

//! Outbound priority queue wrapper for SFU egress (P5 wave-1).
//!
//! Defines the five outbound classes (P0 Control, P1 Audio, P2 Keyframe + base
//! T0 video, P3 base spatial P-frames, P4 enhancement + screen), their bounded
//! capacities, and their drop policies — per `sfu-update/PLAN.md` Phase 5.
//!
//! This module is intentionally just the wrapper:
//!   * classification (`PacketWrapper -> Class`) is p5-3,
//!   * consumer-side strict-priority + fairness quantum select is p5-2,
//!   * transport wiring (replacing the single `mpsc::channel::<WtOutbound>(256)`
//!     in `webtransport/mod.rs`) is p5-4 / p5-5,
//!   * metrics / CongestionTracker hooks are p5-6.
//!
//! The five `ClassReceiver` halves are exposed as public fields on
//! `PriorityChannels` so p5-2 can build its own select loop directly on top
//! of them without further plumbing here.
//!
//! ## Why a custom bounded queue (not `tokio::sync::mpsc`)
//!
//! `tokio::sync::mpsc` cannot satisfy TailDropOldest cleanly: once a message
//! is enqueued, only the consumer can pop the head, so the sender can't evict
//! the oldest entry when the queue is full. Instead, each class uses a small
//! bounded queue backed by `Arc<Mutex<VecDeque<Bytes>>>` plus a
//! `tokio::sync::Notify` to wake the receiver, and an `AtomicUsize` sender
//! count so the receiver can return `None` once all senders have dropped.

use bytes::Bytes;
use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use tracing::debug;
use videocall_types::protos::media_packet::media_packet::MediaType;
use videocall_types::protos::media_packet::RoutingHeader;
use videocall_types::protos::packet_wrapper::packet_wrapper::PacketType;
use videocall_types::protos::packet_wrapper::PacketWrapper;

/// Outbound class. Lower number = higher priority.
///
/// See `sfu-update/PLAN.md` Phase 5 for the canonical class table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Class {
    /// P0: control / signaling. Never drop.
    P0Control,
    /// P1: audio. Tail-drop oldest when full.
    P1Audio,
    /// P2: keyframes + base T0 video. Tail-drop oldest when full.
    P2Keyframe,
    /// P3: base spatial P-frames. Tail-drop oldest when full.
    P3VideoBase,
    /// P4: enhancement layers + screenshare. Head-drop the NEW entry when full.
    P4Enhancement,
}

impl Class {
    /// Bounded capacity of this class's queue, per PLAN.md Phase 5.
    pub fn capacity(self) -> usize {
        match self {
            Class::P0Control => 32,
            Class::P1Audio => 128,
            Class::P2Keyframe => 128,
            Class::P3VideoBase => 256,
            Class::P4Enhancement => 256,
        }
    }

    /// Drop policy of this class's queue, per PLAN.md Phase 5.
    pub fn drop_policy(self) -> DropPolicy {
        match self {
            Class::P0Control => DropPolicy::NeverDrop,
            Class::P1Audio => DropPolicy::TailDropOldest,
            Class::P2Keyframe => DropPolicy::TailDropOldest,
            Class::P3VideoBase => DropPolicy::TailDropOldest,
            Class::P4Enhancement => DropPolicy::HeadDropOldest,
        }
    }

    /// All five classes, ordered highest-priority first. Useful for tests and
    /// for p5-2's select loop.
    pub const fn all() -> [Class; 5] {
        [
            Class::P0Control,
            Class::P1Audio,
            Class::P2Keyframe,
            Class::P3VideoBase,
            Class::P4Enhancement,
        ]
    }
}

/// Classify an outbound packet into the priority queue [`Class`] it should
/// be routed into.
///
/// This is the p5-3 classifier called by the p5-4 / p5-5 transport wiring
/// to decide which class queue a `PacketWrapper` should land in on egress.
///
/// # Hot path discipline
///
/// This function is invoked on every outbound packet and must remain
/// allocation-free. It only reads:
/// * `packet_wrapper.packet_type` (a `protobuf::EnumOrUnknown<PacketType>`),
/// * `media_type` (the caller's already-parsed `MediaPacket.media_type`
///   — passed in so we never re-parse the inner payload here),
/// * `routing_header` layer ids (`is_keyframe`, `temporal_layer_id`,
///   `spatial_layer_id`).
///
/// # Deviation from bead vc-cbj spec
///
/// The bead's pseudocode references several `PacketType` variants that do
/// not actually exist in the protobuf-generated enum
/// (`videocall_types::protos::packet_wrapper::packet_wrapper::PacketType`).
/// Specifically:
/// * `PacketType::HEARTBEAT` / `PacketType::RTT` / `PacketType::AUDIO`
///   are not `PacketType` values — they are `MediaType` sub-variants nested
///   inside a MEDIA-wrapped `MediaPacket`.
/// * `PacketType::MEETING_ACTIVATED` / `PacketType::MEETING_DEACTIVATED`
///   are `MeetingEventType` sub-variants nested inside a MEETING-wrapped
///   `MeetingPacket`.
///
/// To preserve the bead's intent (HEARTBEAT/RTT/AUDIO classifying correctly)
/// without doing a second parse of the inner payload, the signature takes
/// an explicit `media_type: Option<MediaType>` parameter that the caller
/// supplies from the already-parsed `MediaPacket` (e.g. via
/// `ParsedPacket::media_packet.as_ref().map(|mp| mp.media_type.enum_value_or_default())`).
/// `MEETING` as a whole is routed to `P0Control` since both of its event
/// sub-types in the bead spec are control-class.
///
/// # Returns
/// * [`Class::P0Control`] for control packets (`CONGESTION`, `SESSION_ASSIGNED`,
///   `SPEAKER_UPDATE`, `MEETING`, `SUBSCRIPTION_UPDATE`, `LAYER_HINT`,
///   `ADMISSION_DECISION`, `CAPABILITY_ANNOUNCE`, plus MEDIA wrappers whose
///   inner `MediaType` is `HEARTBEAT`, `RTT`, or `KEYFRAME_REQUEST`).
/// * [`Class::P1Audio`] for MEDIA wrappers whose inner `MediaType` is `AUDIO`.
/// * [`Class::P2Keyframe`] for MEDIA video with `is_keyframe && temporal=0 && spatial=0`.
/// * [`Class::P3VideoBase`] for MEDIA video with `temporal=0 && spatial=0`
///   (non-keyframe), MEDIA without a routing header (legacy clients), and as
///   the fallback for unhandled / unknown packet types (logged at debug level
///   so a flood of unknown types from a misbehaving client can't spam
///   production logs on the hot path).
/// * [`Class::P4Enhancement`] for MEDIA video on enhancement layers or
///   screenshare. SCREEN is always P4 regardless of routing-header layer ids.
pub fn classify_outbound(
    packet_wrapper: &PacketWrapper,
    media_type: Option<MediaType>,
    routing_header: Option<&RoutingHeader>,
) -> Class {
    // 1. Control-class shortcuts (highest priority). `packet_type` is a
    //    `protobuf::EnumOrUnknown<PacketType>`; comparing against
    //    `PacketType::FOO.into()` is the established codebase pattern (see
    //    e.g. `chat_server.rs` CONGESTION carve-out and `packet_handler.rs`
    //    classification).
    let pt = packet_wrapper.packet_type;
    if pt == PacketType::CONGESTION.into()
        || pt == PacketType::SESSION_ASSIGNED.into()
        || pt == PacketType::SPEAKER_UPDATE.into()
        || pt == PacketType::MEETING.into()
        || pt == PacketType::SUBSCRIPTION_UPDATE.into()
        || pt == PacketType::LAYER_HINT.into()
        || pt == PacketType::ADMISSION_DECISION.into()
        || pt == PacketType::CAPABILITY_ANNOUNCE.into()
    {
        return Class::P0Control;
    }

    // 2. MEDIA wrappers: dispatch by inner `MediaType` first (so HEARTBEAT /
    //    RTT control sub-types and AUDIO get correct routing), then by
    //    routing-header layer ids for VIDEO / SCREEN.
    if pt == PacketType::MEDIA.into() {
        match media_type {
            Some(MediaType::HEARTBEAT)
            | Some(MediaType::RTT)
            | Some(MediaType::KEYFRAME_REQUEST) => return Class::P0Control,
            Some(MediaType::AUDIO) => return Class::P1Audio,
            _ => {}
        }

        // Screenshare is always P4 enhancement, regardless of layer ids.
        // Without this short-circuit, a SCREEN packet with is_keyframe + T0/S0
        // would otherwise land in P2Keyframe, contradicting the class table.
        if matches!(media_type, Some(MediaType::SCREEN)) {
            return Class::P4Enhancement;
        }

        let rh = match routing_header {
            Some(h) => h,
            // Legacy client / unparseable inner: default to base video.
            None => return Class::P3VideoBase,
        };

        // Keyframe + T0 + S0 -> P2 (the most critical video class).
        if rh.is_keyframe && rh.temporal_layer_id == 0 && rh.spatial_layer_id == 0 {
            return Class::P2Keyframe;
        }
        // Base spatial (S0), base temporal (T0), non-keyframe -> P3.
        if rh.spatial_layer_id == 0 && rh.temporal_layer_id == 0 {
            return Class::P3VideoBase;
        }
        // Enhancement layers (T>0 or S>0) and screen share -> P4.
        return Class::P4Enhancement;
    }

    // 3. Fallback for unhandled packet types. Logged at `debug!` rather than
    //    `warn!` because a misbehaving client could flood unknown packet types
    //    and produce unbounded log spam on the hot path; developers running
    //    with `RUST_LOG=debug` will still see if a new variant slips through.
    debug!(
        packet_type = ?packet_wrapper.packet_type,
        "unclassified outbound packet, defaulting to P3VideoBase"
    );
    Class::P3VideoBase
}

/// Drop behavior when a class queue is at capacity.
///
/// Direction note (intentional, called out in the bead):
/// * `TailDropOldest` — when full, evict the OLDEST queued entry (the head)
///   and push the NEW entry at the tail. Caller can read the new entry; an
///   already-queued head item is dropped.
/// * `HeadDropOldest` — when full, drop the NEW entry without enqueueing it.
///   In-flight entries already in the queue are preserved. The name reads
///   "head drop" because the head of the queue is the oldest in-flight entry
///   and we *keep* it; the new entry never makes it past the gate. P4 is
///   best-effort enhancement, so preserving in-flight enhancement layers
///   (which may still pair with already-buffered base-layer playout) is
///   preferable to constant churn at saturation.
/// * `NeverDrop` — when full, refuse the send. Caller (p5-2 consumer) will
///   log and stop the session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropPolicy {
    /// When full, evict the head (oldest) and push the new entry at the tail.
    TailDropOldest,
    /// When full, drop the NEW entry. The head (oldest in-flight) is kept.
    HeadDropOldest,
    /// When full, refuse the send (returned as `SendOutcome::Refused`).
    NeverDrop,
}

/// Marker error returned when a `NeverDrop` class queue is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SendError;

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("P0 control queue full")
    }
}

impl std::error::Error for SendError {}

/// Result of `PrioritySender::send`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendOutcome {
    /// Entry was enqueued.
    Sent,
    /// Entry was admitted under a drop policy that evicted another (or
    /// dropped the new entry, for HeadDropOldest). The static string is a
    /// short reason tag suitable for metrics.
    Dropped(Class, &'static str),
    /// Class was full and uses `NeverDrop`; nothing was enqueued.
    Refused(SendError),
}

/// Shared inner state for a single class queue. Each class tags its
/// `Class` directly so producers can attach an accurate identity to
/// `SendOutcome::Dropped` without re-deriving it from (policy, capacity)
/// — which is non-unique across the five classes (P1Audio and P2Keyframe
/// both have policy=TailDropOldest, capacity=128).
struct ClassInner {
    queue: Mutex<VecDeque<Bytes>>,
    notify: Notify,
    senders: AtomicUsize,
    capacity: usize,
    policy: DropPolicy,
    class: Class,
}

/// Producer half for a single class. Cheap to clone; multi-producer safe.
struct ClassSender {
    inner: Arc<ClassInner>,
}

impl ClassSender {
    fn send(&self, bytes: Bytes) -> SendOutcome {
        let mut q = self
            .inner
            .queue
            .lock()
            .expect("priority_queue mutex poisoned");
        if q.len() < self.inner.capacity {
            q.push_back(bytes);
            drop(q);
            self.inner.notify.notify_one();
            return SendOutcome::Sent;
        }
        match self.inner.policy {
            DropPolicy::TailDropOldest => {
                let _evicted = q.pop_front();
                q.push_back(bytes);
                drop(q);
                self.inner.notify.notify_one();
                SendOutcome::Dropped(self.inner.class, "tail_drop_oldest")
            }
            DropPolicy::HeadDropOldest => {
                drop(q);
                SendOutcome::Dropped(self.inner.class, "head_drop_new")
            }
            DropPolicy::NeverDrop => {
                drop(q);
                SendOutcome::Refused(SendError)
            }
        }
    }
}

impl Clone for ClassSender {
    fn clone(&self) -> Self {
        self.inner.senders.fetch_add(1, Ordering::AcqRel);
        ClassSender {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Drop for ClassSender {
    fn drop(&mut self) {
        // Decrement the sender count; if this was the last sender, wake any
        // pending receiver so it can observe closure and return `None`.
        let prev = self.inner.senders.fetch_sub(1, Ordering::AcqRel);
        if prev == 1 {
            self.inner.notify.notify_one();
        }
    }
}

/// Receiver half for a single class. Held by `PriorityChannels` so the
/// downstream consumer (p5-2) can `recv()` per class as part of its
/// strict-priority + fairness select loop.
pub struct ClassReceiver {
    inner: Arc<ClassInner>,
}

impl ClassReceiver {
    /// The `Class` this receiver drains.
    pub fn class(&self) -> Class {
        self.inner.class
    }

    /// Try to pop the next entry without awaiting. Returns `None` if the
    /// queue is currently empty (regardless of whether senders are still
    /// alive).
    pub fn try_recv(&mut self) -> Option<Bytes> {
        let mut q = self
            .inner
            .queue
            .lock()
            .expect("priority_queue mutex poisoned");
        q.pop_front()
    }

    /// Await the next entry. Returns `None` once all senders have dropped
    /// AND the queue is drained.
    pub async fn recv(&mut self) -> Option<Bytes> {
        loop {
            // Register intent to be notified BEFORE checking state, so we
            // don't miss a notification raced between check and await.
            let notified = self.inner.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            {
                let mut q = self
                    .inner
                    .queue
                    .lock()
                    .expect("priority_queue mutex poisoned");
                if let Some(item) = q.pop_front() {
                    return Some(item);
                }
                if self.inner.senders.load(Ordering::Acquire) == 0 {
                    return None;
                }
            }

            notified.await;
        }
    }
}

/// Five-class outbound producer. Cheap to clone; all clones share the same
/// underlying class queues.
#[derive(Clone)]
pub struct PrioritySender {
    p0: ClassSender,
    p1: ClassSender,
    p2: ClassSender,
    p3: ClassSender,
    p4: ClassSender,
}

/// Receiver bundle. Owned by the consumer task (p5-2). The five receivers
/// are exposed as named fields so the consumer can build its strict-priority
/// + fairness quantum select loop directly on top of them.
pub struct PriorityChannels {
    /// P0 Control queue receiver.
    pub p0_control: ClassReceiver,
    /// P1 Audio queue receiver.
    pub p1_audio: ClassReceiver,
    /// P2 Keyframe + base T0 video queue receiver.
    pub p2_keyframe: ClassReceiver,
    /// P3 base spatial P-frames queue receiver.
    pub p3_video_base: ClassReceiver,
    /// P4 enhancement + screenshare queue receiver.
    pub p4_enhancement: ClassReceiver,
}

impl PriorityChannels {
    /// Borrow the receiver for a specific class. Convenience for tests and
    /// for consumers that want to dispatch by `Class`.
    pub fn receiver_mut(&mut self, class: Class) -> &mut ClassReceiver {
        match class {
            Class::P0Control => &mut self.p0_control,
            Class::P1Audio => &mut self.p1_audio,
            Class::P2Keyframe => &mut self.p2_keyframe,
            Class::P3VideoBase => &mut self.p3_video_base,
            Class::P4Enhancement => &mut self.p4_enhancement,
        }
    }
}

impl PrioritySender {
    /// Construct a paired sender + channels bundle. The sender is `Clone`
    /// (multi-producer); the channels bundle is single-consumer.
    pub fn new() -> (Self, PriorityChannels) {
        let (s0, r0) = build_class(Class::P0Control);
        let (s1, r1) = build_class(Class::P1Audio);
        let (s2, r2) = build_class(Class::P2Keyframe);
        let (s3, r3) = build_class(Class::P3VideoBase);
        let (s4, r4) = build_class(Class::P4Enhancement);

        let sender = PrioritySender {
            p0: s0,
            p1: s1,
            p2: s2,
            p3: s3,
            p4: s4,
        };
        let channels = PriorityChannels {
            p0_control: r0,
            p1_audio: r1,
            p2_keyframe: r2,
            p3_video_base: r3,
            p4_enhancement: r4,
        };
        (sender, channels)
    }

    /// Send `bytes` via the queue for `class`. Non-blocking: the drop
    /// policies guarantee this call never awaits.
    pub fn send(&self, class: Class, bytes: Bytes) -> SendOutcome {
        match class {
            Class::P0Control => self.p0.send(bytes),
            Class::P1Audio => self.p1.send(bytes),
            Class::P2Keyframe => self.p2.send(bytes),
            Class::P3VideoBase => self.p3.send(bytes),
            Class::P4Enhancement => self.p4.send(bytes),
        }
    }
}

fn build_class(class: Class) -> (ClassSender, ClassReceiver) {
    let inner = Arc::new(ClassInner {
        queue: Mutex::new(VecDeque::with_capacity(class.capacity())),
        notify: Notify::new(),
        senders: AtomicUsize::new(1),
        capacity: class.capacity(),
        policy: class.drop_policy(),
        class,
    });
    let sender = ClassSender {
        inner: Arc::clone(&inner),
    };
    let receiver = ClassReceiver { inner };
    (sender, receiver)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn class_capacity_matches_table() {
        assert_eq!(Class::P0Control.capacity(), 32);
        assert_eq!(Class::P1Audio.capacity(), 128);
        assert_eq!(Class::P2Keyframe.capacity(), 128);
        assert_eq!(Class::P3VideoBase.capacity(), 256);
        assert_eq!(Class::P4Enhancement.capacity(), 256);
    }

    #[test]
    fn class_drop_policy_matches_table() {
        assert_eq!(Class::P0Control.drop_policy(), DropPolicy::NeverDrop);
        assert_eq!(Class::P1Audio.drop_policy(), DropPolicy::TailDropOldest);
        assert_eq!(Class::P2Keyframe.drop_policy(), DropPolicy::TailDropOldest);
        assert_eq!(Class::P3VideoBase.drop_policy(), DropPolicy::TailDropOldest);
        assert_eq!(
            Class::P4Enhancement.drop_policy(),
            DropPolicy::HeadDropOldest
        );
    }

    #[test]
    fn send_to_non_full_class_returns_sent() {
        let (sender, _channels) = PrioritySender::new();
        let outcome = sender.send(Class::P1Audio, Bytes::from_static(b"hello"));
        assert_eq!(outcome, SendOutcome::Sent);
    }

    #[test]
    fn tail_drop_oldest_evicts_head_and_keeps_new() {
        let (sender, mut channels) = PrioritySender::new();
        let cap = Class::P1Audio.capacity();

        // Fill to capacity with distinct sentinels "0".."127".
        for i in 0..cap {
            let outcome = sender.send(Class::P1Audio, Bytes::from(format!("{i}").into_bytes()));
            assert_eq!(outcome, SendOutcome::Sent, "fill #{i} should succeed");
        }

        // One more — should evict head and admit "new" at the tail.
        let outcome = sender.send(Class::P1Audio, Bytes::from_static(b"new"));
        match outcome {
            SendOutcome::Dropped(Class::P1Audio, reason) => {
                assert_eq!(reason, "tail_drop_oldest");
            }
            other => panic!("expected Dropped(P1Audio, tail_drop_oldest), got {other:?}"),
        }

        // Drain via try_recv and verify ordering.
        let mut drained = Vec::with_capacity(cap);
        while let Some(b) = channels.p1_audio.try_recv() {
            drained.push(b);
        }
        assert_eq!(drained.len(), cap, "queue should still hold {cap} items");
        // Original head "0" was evicted; new head is "1".
        assert_eq!(&drained[0][..], b"1");
        // New entry is at the tail.
        assert_eq!(&drained[drained.len() - 1][..], b"new");
    }

    #[test]
    fn never_drop_refuses_when_full() {
        let (sender, _channels) = PrioritySender::new();
        let cap = Class::P0Control.capacity();

        for i in 0..cap {
            let outcome = sender.send(Class::P0Control, Bytes::from(format!("{i}").into_bytes()));
            assert_eq!(outcome, SendOutcome::Sent, "fill #{i} should succeed");
        }

        let outcome = sender.send(Class::P0Control, Bytes::from_static(b"overflow"));
        assert_eq!(outcome, SendOutcome::Refused(SendError));
    }

    #[test]
    fn head_drop_oldest_drops_new_when_full() {
        let (sender, mut channels) = PrioritySender::new();
        let cap = Class::P4Enhancement.capacity();

        for i in 0..cap {
            let outcome = sender.send(
                Class::P4Enhancement,
                Bytes::from(format!("{i}").into_bytes()),
            );
            assert_eq!(outcome, SendOutcome::Sent, "fill #{i} should succeed");
        }

        let outcome = sender.send(Class::P4Enhancement, Bytes::from_static(b"new"));
        match outcome {
            SendOutcome::Dropped(Class::P4Enhancement, reason) => {
                assert_eq!(reason, "head_drop_new");
            }
            other => panic!("expected Dropped(P4Enhancement, head_drop_new), got {other:?}"),
        }

        let mut drained = Vec::with_capacity(cap);
        while let Some(b) = channels.p4_enhancement.try_recv() {
            drained.push(b);
        }
        // Queue still holds the original capacity entries, in order, with no
        // "new" present.
        assert_eq!(drained.len(), cap);
        for (i, b) in drained.iter().enumerate() {
            assert_eq!(&b[..], format!("{i}").as_bytes());
        }
        assert!(
            drained.iter().all(|b| &b[..] != b"new"),
            "head_drop policy must reject the new entry"
        );
    }

    #[tokio::test]
    async fn recv_returns_entries_in_fifo_order() {
        let (sender, mut channels) = PrioritySender::new();
        assert_eq!(
            sender.send(Class::P2Keyframe, Bytes::from_static(b"a")),
            SendOutcome::Sent
        );
        assert_eq!(
            sender.send(Class::P2Keyframe, Bytes::from_static(b"b")),
            SendOutcome::Sent
        );
        assert_eq!(&channels.p2_keyframe.recv().await.unwrap()[..], b"a");
        assert_eq!(&channels.p2_keyframe.recv().await.unwrap()[..], b"b");
    }

    #[tokio::test]
    async fn recv_returns_none_after_all_senders_drop() {
        let (sender, mut channels) = PrioritySender::new();
        drop(sender);
        assert!(channels.p1_audio.recv().await.is_none());
    }

    // --- p5-3: classify_outbound -------------------------------------------

    fn wrapper_with(pt: PacketType) -> PacketWrapper {
        let mut w = PacketWrapper::new();
        w.packet_type = pt.into();
        w
    }

    fn routing_header(is_keyframe: bool, t: u32, s: u32) -> RoutingHeader {
        let mut h = RoutingHeader::new();
        h.is_keyframe = is_keyframe;
        h.temporal_layer_id = t;
        h.spatial_layer_id = s;
        h
    }

    #[test]
    fn classify_outbound_congestion_is_p0_control() {
        let w = wrapper_with(PacketType::CONGESTION);
        assert_eq!(classify_outbound(&w, None, None), Class::P0Control);
    }

    #[test]
    fn classify_outbound_heartbeat_is_p0_control() {
        // HEARTBEAT travels as MEDIA + inner MediaType::HEARTBEAT.
        let w = wrapper_with(PacketType::MEDIA);
        assert_eq!(
            classify_outbound(&w, Some(MediaType::HEARTBEAT), None),
            Class::P0Control
        );
    }

    #[test]
    fn classify_outbound_rtt_is_p0_control() {
        let w = wrapper_with(PacketType::MEDIA);
        assert_eq!(
            classify_outbound(&w, Some(MediaType::RTT), None),
            Class::P0Control
        );
    }

    #[test]
    fn classify_outbound_session_assigned_is_p0_control() {
        let w = wrapper_with(PacketType::SESSION_ASSIGNED);
        assert_eq!(classify_outbound(&w, None, None), Class::P0Control);
    }

    #[test]
    fn classify_outbound_speaker_update_is_p0_control() {
        let w = wrapper_with(PacketType::SPEAKER_UPDATE);
        assert_eq!(classify_outbound(&w, None, None), Class::P0Control);
    }

    #[test]
    fn classify_outbound_meeting_is_p0_control() {
        // Covers MEETING_ACTIVATED / MEETING_DEACTIVATED in the bead spec —
        // both are sub-variants of the MEETING PacketType wrapper.
        let w = wrapper_with(PacketType::MEETING);
        assert_eq!(classify_outbound(&w, None, None), Class::P0Control);
    }

    #[test]
    fn classify_outbound_audio_is_p1_audio() {
        let w = wrapper_with(PacketType::MEDIA);
        assert_eq!(
            classify_outbound(&w, Some(MediaType::AUDIO), None),
            Class::P1Audio
        );
    }

    #[test]
    fn classify_outbound_media_keyframe_base_layer_is_p2_keyframe() {
        let w = wrapper_with(PacketType::MEDIA);
        let rh = routing_header(true, 0, 0);
        assert_eq!(
            classify_outbound(&w, Some(MediaType::VIDEO), Some(&rh)),
            Class::P2Keyframe
        );
    }

    #[test]
    fn classify_outbound_media_base_layer_non_keyframe_is_p3_video_base() {
        let w = wrapper_with(PacketType::MEDIA);
        let rh = routing_header(false, 0, 0);
        assert_eq!(
            classify_outbound(&w, Some(MediaType::VIDEO), Some(&rh)),
            Class::P3VideoBase
        );
    }

    #[test]
    fn classify_outbound_media_enhancement_layer_is_p4_enhancement() {
        let w = wrapper_with(PacketType::MEDIA);
        // temporal=2, spatial=0 -> T2 enhancement
        let rh = routing_header(false, 2, 0);
        assert_eq!(
            classify_outbound(&w, Some(MediaType::VIDEO), Some(&rh)),
            Class::P4Enhancement
        );
    }

    #[test]
    fn classify_outbound_media_without_routing_header_is_p3_video_base() {
        // Legacy client: MEDIA wrapper with no inner routing header. We
        // also leave `media_type` as None (e.g. encrypted inner that the
        // caller couldn't parse).
        let w = wrapper_with(PacketType::MEDIA);
        assert_eq!(classify_outbound(&w, None, None), Class::P3VideoBase);
    }

    #[test]
    fn classify_outbound_unknown_packet_type_is_p3_video_base() {
        // PACKET_TYPE_UNKNOWN does not appear in the control-class list, is
        // not MEDIA, and should hit the debug-fallback branch.
        let w = wrapper_with(PacketType::PACKET_TYPE_UNKNOWN);
        assert_eq!(classify_outbound(&w, None, None), Class::P3VideoBase);
    }

    #[test]
    fn classify_outbound_keyframe_request_is_p0_control() {
        // KEYFRAME_REQUEST is signaling, not video data — it must classify as
        // P0Control and NOT fall through to the routing-header layer check.
        let w = wrapper_with(PacketType::MEDIA);
        assert_eq!(
            classify_outbound(&w, Some(MediaType::KEYFRAME_REQUEST), None),
            Class::P0Control
        );
    }

    #[test]
    fn classify_outbound_screen_share_is_p4_enhancement() {
        // Routing header that WOULD classify VIDEO as P2Keyframe — we pick
        // is_keyframe=true, T=0, S=0 specifically to prove that SCREEN
        // overrides routing-header-based classification.
        let w = wrapper_with(PacketType::MEDIA);
        let rh = routing_header(true, 0, 0);
        assert_eq!(
            classify_outbound(&w, Some(MediaType::SCREEN), Some(&rh)),
            Class::P4Enhancement
        );
    }

    #[test]
    fn classify_outbound_subscription_update_is_p0_control() {
        let w = wrapper_with(PacketType::SUBSCRIPTION_UPDATE);
        assert_eq!(classify_outbound(&w, None, None), Class::P0Control);
    }

    #[test]
    fn classify_outbound_layer_hint_is_p0_control() {
        let w = wrapper_with(PacketType::LAYER_HINT);
        assert_eq!(classify_outbound(&w, None, None), Class::P0Control);
    }

    #[test]
    fn classify_outbound_admission_decision_is_p0_control() {
        let w = wrapper_with(PacketType::ADMISSION_DECISION);
        assert_eq!(classify_outbound(&w, None, None), Class::P0Control);
    }

    #[test]
    fn classify_outbound_capability_announce_is_p0_control() {
        let w = wrapper_with(PacketType::CAPABILITY_ANNOUNCE);
        assert_eq!(classify_outbound(&w, None, None), Class::P0Control);
    }
}
