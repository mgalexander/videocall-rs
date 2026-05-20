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

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use tokio::sync::watch;

use videocall_types::frame_marker::REFERENCES_T0;
use videocall_types::protos::media_packet::media_packet::MediaType;
use videocall_types::protos::media_packet::MediaPacket;
use videocall_types::protos::packet_wrapper::packet_wrapper::PacketType;
use videocall_types::protos::packet_wrapper::PacketWrapper;

use crate::actors::session_logic::SessionId;
use crate::metrics::sfu_drop_reason;
use crate::metrics::{
    SFU_DECIDE_LATENCY_US, SFU_DROPPED_TOTAL, SFU_FORWARDED_TOTAL, SFU_FORWARD_TOTAL,
    SFU_KEYFRAME_FORWARDED_TOTAL, SFU_ROOM_SIZE,
};
use crate::sfu::layer_selector::LayerSelector;
use crate::sfu::room_state::RoomState;
use crate::sfu::speaker::ActiveSpeakerSet;
use crate::sfu::subscription::{SubscriptionStore, MAX_VISIBLE_VIDEO};
use crate::sfu::trace;

/// Map a `PacketWrapper.packet_type` (an `EnumOrUnknown<PacketType>`) to a
/// stable lowercase string suitable for use as a Prometheus label value.
///
/// Unknown / unrecognized enum values map to `"unknown"`.
fn packet_type_label(pw: &PacketWrapper) -> &'static str {
    match pw.packet_type.enum_value_or_default() {
        PacketType::PACKET_TYPE_UNKNOWN => "unknown",
        PacketType::RSA_PUB_KEY => "rsa_pub_key",
        PacketType::AES_KEY => "aes_key",
        PacketType::MEDIA => "media",
        PacketType::CONNECTION => "connection",
        PacketType::DIAGNOSTICS => "diagnostics",
        PacketType::HEALTH => "health",
        PacketType::MEETING => "meeting",
        PacketType::SESSION_ASSIGNED => "session_assigned",
        PacketType::CONGESTION => "congestion",
        PacketType::SUBSCRIPTION_UPDATE => "subscription_update",
        PacketType::SPEAKER_UPDATE => "speaker_update",
        PacketType::LAYER_HINT => "layer_hint",
        PacketType::ADMISSION_DECISION => "admission_decision",
        PacketType::CAPABILITY_ANNOUNCE => "capability_announce",
        PacketType::HEALTH_BEACON => "health_beacon",
    }
}

/// Maximum picture_id entries retained per `(receiver, sender)` pair for
/// the recent-T0 set. The decoder reference window for VP9 SVC is small
/// (a few seconds at typical frame rates), so 64 entries is a comfortable
/// upper bound while keeping the linear scan trivially cheap.
const RECENT_T0_CAPACITY: usize = 64;

/// TTL after which an entry in the recent-T0 set is considered too stale
/// to satisfy a T1/T2 reference check. 5s is well past the longest
/// plausible decoder reference window but short enough that we don't
/// accumulate dead entries after a sender goes quiet.
const RECENT_T0_TTL: Duration = Duration::from_secs(5);

/// Per-(receiver, sender) bounded set of T0 picture_ids the SFU has
/// actually forwarded to this receiver recently.
///
/// Two eviction policies, both applied lazily on every `insert`:
///
/// * **TTL**: entries older than [`RECENT_T0_TTL`] are dropped from the
///   front of the deque.
/// * **Capacity**: after TTL eviction, if the deque still exceeds
///   [`RECENT_T0_CAPACITY`], the oldest entries are popped from the front.
///
/// `contains` is a linear scan over (worst case) 64 entries, which is
/// faster than a `HashSet` for this size and avoids a second allocation.
#[derive(Default)]
struct RecentT0Set {
    entries: VecDeque<(u64, Instant)>,
}

impl RecentT0Set {
    fn insert(&mut self, picture_id: u64, now: Instant) {
        // TTL eviction from the front (deque is ordered by insertion time
        // because `now` is monotonic across calls in the same `decide`).
        while let Some(&(_, t)) = self.entries.front() {
            if now.duration_since(t) > RECENT_T0_TTL {
                self.entries.pop_front();
            } else {
                break;
            }
        }
        self.entries.push_back((picture_id, now));
        // Capacity eviction.
        while self.entries.len() > RECENT_T0_CAPACITY {
            self.entries.pop_front();
        }
    }

    fn contains(&self, picture_id: u64, now: Instant) -> bool {
        self.entries
            .iter()
            .any(|&(pid, t)| pid == picture_id && now.duration_since(t) <= RECENT_T0_TTL)
    }
}

/// Decision returned by the [`Forwarder`] for a single (packet, receiver) pair.
///
/// The pass-through hot path no longer carries bytes: the caller already
/// has the on-wire NATS payload in scope and reuses it directly on
/// `Forward`, avoiding a per-receiver re-serialization of an identical
/// `PacketWrapper`.
pub enum ForwardDecision {
    /// Forward to the receiver. The caller is responsible for supplying
    /// the bytes (typically the original NATS payload — no re-encoding).
    Forward,
    /// Drop the packet for this receiver.
    Drop,
}

/// Per-room forwarder.
///
/// Holds shared handles to the authoritative [`RoomState`], the
/// [`SubscriptionStore`] (per-receiver declarative subscription state), and a
/// `watch::Receiver` over the room's [`ActiveSpeakerSet`]. `decide` is invoked
/// from each receiver's NATS subscription callback and takes a read lock on
/// each handle only long enough to evaluate policy — callers must keep that
/// work cheap.
///
/// Wave-3 (p3-5) consults the AllowSet to filter MEDIA packets per receiver:
/// AUDIO MediaPackets are dropped when the sender is not in `allow.audio` and
/// VIDEO/SCREEN MediaPackets are dropped when the sender is not in
/// `allow.video`. A receiver that has never sent a `SubscriptionUpdate` is
/// returned the legacy-default AllowSet by [`SubscriptionStore::resolve`]
/// (every other room member, base layer), preserving legacy fan-out for
/// clients that don't yet declare subscriptions.
///
/// Non-MEDIA packet types (CONNECTION, CONGESTION, MEETING, …) and unparseable
/// MEDIA inner payloads fall through to the pre-p3-5 pass-through behavior:
/// the only mandatory drop is the sender's own echo (self-skip).
///
/// Layer-aware filtering (skipping enhancement layers based on `LayerPref`) is
/// the P4 layer selector and is intentionally out of scope here.
pub struct Forwarder {
    room: Arc<RwLock<RoomState>>,
    subscriptions: Arc<RwLock<SubscriptionStore>>,
    speakers: watch::Receiver<ActiveSpeakerSet>,
    /// Per-room [`LayerSelector`] with a per-receiver cache. Read on every
    /// `decide` for VP9 SVC enhancement-layer drops (p4-7); written on
    /// bandwidth-estimate ingest + room membership changes, and lazily
    /// inside `decide` when the active-speaker generation moves forward.
    ///
    /// vc-wls: the previous `Arc<RwLock<LayerSelector>>` wrapper was
    /// removed. The selector's internal hot-read state is now a
    /// [`dashmap::DashMap`] (per-receiver striped locks); the slow
    /// recompute path's hysteresis state has its own internal `Mutex`.
    /// Readers of distinct receivers never contend, and bandwidth-
    /// estimate invalidation (1 Hz per receiver) no longer stalls the
    /// 1000 pps `decide` hot path.
    layer_selector: Arc<LayerSelector>,
    /// p4-9: per-`(receiver, sender)` bounded set of T0 `picture_id`s the
    /// SFU has actually forwarded to this receiver in the recent past.
    /// Consulted on every T1/T2 frame whose `frame_marker & REFERENCES_T0`
    /// bit is set, so we can drop reference-dependent frames whose
    /// reference picture was dropped upstream (e.g. by an AllowSet flip).
    ///
    /// The lock is taken in `write()` mode unconditionally inside `decide`
    /// because the critical section is a hash lookup + at most one
    /// 64-entry VecDeque push/scan — adding a read-then-upgrade dance
    /// would cost more than it saves at this size. Lock poisoning is
    /// handled like every other lock in this module (`.into_inner()`).
    recent_t0: Arc<RwLock<HashMap<(SessionId, SessionId), RecentT0Set>>>,
}

impl Forwarder {
    /// Full constructor used by the chat-server fan-out path.
    ///
    /// `speakers` is a `watch::Receiver` so reads are lock-free and synchronous
    /// — `decide` runs on every (packet, receiver) pair and must stay cheap.
    pub fn new(
        room: Arc<RwLock<RoomState>>,
        subscriptions: Arc<RwLock<SubscriptionStore>>,
        speakers: watch::Receiver<ActiveSpeakerSet>,
        layer_selector: Arc<LayerSelector>,
    ) -> Self {
        Self {
            room,
            subscriptions,
            speakers,
            layer_selector,
            recent_t0: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Shared handle to the per-room [`LayerSelector`]. Exposed so the
    /// bandwidth-estimate ingest path in `chat_server` can call
    /// [`LayerSelector::recompute_for_receiver`] without re-plumbing the
    /// handle separately.
    pub fn layer_selector(&self) -> Arc<LayerSelector> {
        self.layer_selector.clone()
    }

    /// Convenience constructor for tests and the in-crate parity helpers that
    /// don't materialise a real subscription store or speaker tick.
    ///
    /// Wires in an empty [`SubscriptionStore`] (every receiver resolves to the
    /// legacy-default AllowSet, preserving pre-p3-5 behavior) and a freshly
    /// constructed `watch` channel seeded with [`ActiveSpeakerSet::empty`].
    /// The sender half is leaked so the receiver stays open for the
    /// forwarder's lifetime — tests don't need the sender. Gated to
    /// `#[cfg(test)]` so production code paths cannot accidentally leak a
    /// `watch::Sender`; chat_server uses [`Forwarder::new`] with an
    /// actor-owned sender retained in `ChatServer::speakers`.
    #[cfg(test)]
    pub fn with_room_only(room: Arc<RwLock<RoomState>>) -> Self {
        let subscriptions = Arc::new(RwLock::new(SubscriptionStore::new()));
        let (tx, rx) = watch::channel(ActiveSpeakerSet::empty());
        // Keep the sender alive for the forwarder's lifetime so `borrow()`
        // never returns the closed-channel sentinel value. Leaking here is
        // intentional: callers of `with_room_only` are short-lived (tests).
        std::mem::forget(tx);
        Self {
            room,
            subscriptions,
            speakers: rx,
            layer_selector: Arc::new(LayerSelector::new()),
            recent_t0: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Per-receiver decision for `receiver_sid`.
    ///
    /// Order of evaluation (each drop bumps `sfu_dropped_total{reason=…}`):
    ///
    /// 1. **Self-skip** — sender == receiver → `Drop{self_skip}`.
    /// 2. **AllowSet filter** (only for `PacketWrapper.packet_type == MEDIA`
    ///    with a successfully parsed inner `MediaPacket`):
    ///    * Resolve the receiver's [`crate::sfu::subscription::AllowSet`]
    ///      from the current room membership + speaker set.
    ///    * `MediaType::AUDIO`  — drop iff sender not in `allow.audio`.
    ///    * `MediaType::VIDEO` / `SCREEN` — drop iff sender not in `allow.video`.
    ///    * Other media types (HEARTBEAT, RTT, KEYFRAME_REQUEST) and unknown
    ///      values pass through — they are control / signalling streams that
    ///      are not subject to subscription filtering.
    /// 3. **VP9 SVC layer-drop** (p4-7, tightened in p4-8) — only for MEDIA
    ///    `VIDEO`/`SCREEN` packets that carry a `RoutingHeader`:
    ///    * `routing_header.is_keyframe == true` AND `temporal_layer_id == 0`
    ///      AND `spatial_layer_id == 0` ALWAYS passes through and bumps
    ///      `sfu_keyframe_forwarded_total` — invariant 1, dropping a
    ///      base-layer keyframe breaks the entire reference chain for
    ///      every subsequent frame until the next keyframe arrives.
    ///    * Higher-layer keyframes (T>0 or S>0) are NOT load-bearing for
    ///      decode in the same way — they only restart the dependent
    ///      enhancement chain and can be dropped under budget pressure
    ///      just like P-frames.
    ///    * Consult the cached [`crate::sfu::layer_selector::LayerSelection`]
    ///      for this receiver (lazily refreshed when the active-speaker
    ///      generation moves forward).
    ///    * Sender's spatial layer not selected → drop (`layer_budget`).
    ///    * `routing_header.temporal_layer_id` exceeds the selected
    ///      `max_temporal_layer_id` → drop (`layer_budget`).
    ///    * Missing `RoutingHeader` or no cached selection → pass through
    ///      (legacy client; preserves pre-p4-7 behavior).
    /// 4. **Reference-aware drop** (p4-9) — only for MEDIA `VIDEO`/`SCREEN`
    ///    packets that carry a `RoutingHeader` and are NOT keyframes:
    ///    * If `temporal_layer_id == 0` (a T0 delta that just survived
    ///      step 3) → record its `picture_id` in the recent-T0 set for
    ///      this `(receiver, sender)` pair.
    ///    * If `frame_marker & REFERENCES_T0 != 0` (T1/T2 delta
    ///      referencing a T0) AND `picture_id` is NOT in the recent-T0
    ///      set → drop (`reference_miss`). This prevents decoder
    ///      reference errors when the referenced T0 was dropped
    ///      upstream (AllowSet flip, etc.).
    /// 5. **Anything else** — non-MEDIA wrappers or MEDIA wrappers whose
    ///    inner payload didn't parse — forwarded as-is, preserving the
    ///    tolerant pre-p3-5 behavior. The CONGESTION carve-out is enforced
    ///    one layer above (`chat_server::egress_decide_from_parsed`).
    ///
    /// All metric updates and the latency histogram observation happen before
    /// the function returns; the room's read lock is held only for the
    /// duration of the membership snapshot and gauge refresh.
    pub fn decide(
        &self,
        receiver_sid: SessionId,
        packet_wrapper: &PacketWrapper,
        media_packet: Option<&MediaPacket>,
    ) -> ForwardDecision {
        let start = std::time::Instant::now();

        // Snapshot the room membership, refresh the size gauge, and read
        // the receiver's most-recent bandwidth estimate in one critical
        // section, then drop the room read lock before doing any other
        // work. The lock is poison-safe: a panicked writer leaves the
        // table readable.
        //
        // vc-7gc: `members_snapshot` is now an `Arc<HashSet<SessionId>>`
        // shared with the authoritative `RoomState`. Cloning the `Arc` here
        // is a single refcount bump — at 20 receivers × 1000 pps that
        // replaces ~20k full `HashSet` rebuilds per second per room.
        // `SubscriptionStore::resolve` continues to accept `&HashSet`, so
        // we deref the `Arc` at the call site.
        // vc-2cx: capture `members_generation` alongside the snapshot so
        // `SubscriptionStore::resolve_cached` can detect stale cached
        // AllowSets safely (the `Arc` pointer alone is ABA-vulnerable).
        // vc-8wd: capture the room id for the targeted trace gate ONLY when
        // tracing is globally armed. With tracing OFF (default) this stays
        // `None` — no `String` clone, no extra work on the hot path. The
        // single `trace::tracing_enabled()` relaxed atomic load is the only
        // cost the gate adds per packet.
        let trace_armed = trace::tracing_enabled();
        let (members_snapshot, members_generation, receiver_bw_kbps, traced_room): (
            Arc<HashSet<SessionId>>,
            u64,
            Option<u32>,
            Option<String>,
        ) = {
            let room = match self.room.read() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            SFU_ROOM_SIZE
                .with_label_values(&[room.room_id.as_str()])
                .set(room.member_count() as f64);
            let (members, gen) = room.members_snapshot_with_generation();
            let bw = room
                .members
                .get(&receiver_sid)
                .and_then(|m| m.bandwidth_estimate.as_ref())
                .map(|est| est.estimated_downlink_kbps);
            // Only clone the room id (and only when armed AND this is the
            // configured room) so the trace path never allocates for an
            // untraced room.
            let traced = if trace_armed && trace::traced_room(&room.room_id) {
                Some(room.room_id.clone())
            } else {
                None
            };
            (members, gen, bw, traced)
        };

        // 1. Self-skip — sender is the receiver itself.
        if packet_wrapper.session_id == receiver_sid {
            SFU_DROPPED_TOTAL
                .with_label_values(&[sfu_drop_reason::SELF_SKIP])
                .inc();
            trace_forward_decision(
                &traced_room,
                &packet_wrapper.session_id,
                "drop",
                sfu_drop_reason::SELF_SKIP,
            );
            observe_decide_latency(start);
            return ForwardDecision::Drop;
        }

        // 2. AllowSet filter + layer-drop for MEDIA packets with a parsed
        // inner MediaPacket.
        let is_media = packet_wrapper.packet_type == PacketType::MEDIA.into();
        if is_media {
            if let Some(mp) = media_packet {
                let media_type = mp.media_type.enum_value_or_default();
                let needs_filter = matches!(
                    media_type,
                    MediaType::AUDIO | MediaType::VIDEO | MediaType::SCREEN
                );
                if needs_filter {
                    // Lock-free read of the current speaker set, captured
                    // ONCE — we reuse both the `top` slice for AllowSet
                    // resolution and the `generation` counter for the
                    // LayerSelector cache invalidation check below.
                    //
                    // vc-7gc: `snap.top` is `Arc<Vec<SessionId>>`; cloning
                    // it is a refcount bump rather than a full `Vec` copy.
                    // `SpeakerTick` only publishes a fresh `Arc` on actual
                    // set/order changes, so quiet ticks reuse the existing
                    // allocation across every `decide` invocation.
                    let (speakers_top, speakers_generation): (Arc<Vec<SessionId>>, u64) = {
                        let snap = self.speakers.borrow();
                        (Arc::clone(&snap.top), snap.generation)
                    };
                    // vc-2cx: cached resolve. Hit returns `Arc::clone` (zero
                    // allocations); miss recomputes once and stores. The
                    // outer read lock on `subscriptions` is sufficient — the
                    // cache itself is a `DashMap` with internal shard locks.
                    // vc-72a: capture the receiver's receive-all posture in
                    // the SAME lock acquisition as the AllowSet resolve, so a
                    // sender that physically arrived here but is NOT a local
                    // room member (cross-pod co-arrival, or the brief window
                    // before a same-pod sender's `insert_member` lands) can
                    // still be admitted when the receiver wants everyone.
                    let (allow, recv_all_audio, recv_all_video) = {
                        let store = match self.subscriptions.read() {
                            Ok(g) => g,
                            Err(poisoned) => poisoned.into_inner(),
                        };
                        let allow = store.resolve_cached(
                            receiver_sid,
                            &members_snapshot,
                            members_generation,
                            &speakers_top,
                            speakers_generation,
                        );
                        let (ra, rv) = store.receive_mode(receiver_sid);
                        (allow, ra, rv)
                    };

                    let sender_sid = packet_wrapper.session_id;
                    // vc-72a: the AllowSet is membership-bound — it only ever
                    // contains LOCAL room members. A sender that physically
                    // arrived here but is NOT a local member (cross-pod
                    // co-arrival, or the brief window before a same-pod
                    // sender's `insert_member` lands) is therefore absent from
                    // the AllowSet and would be hard-dropped, even though a
                    // "see/hear everyone" receiver wants it. We admit such a
                    // sender via the receive-all fallback.
                    //
                    // `non_member_video_admit` is set when a VIDEO/SCREEN
                    // sender is admitted ONLY by that fallback (it is not in
                    // the membership-bound `allow.video`). The downstream
                    // layer-budget stage filters on `allow.video`, so without
                    // threading this through it would re-drop every
                    // non-keyframe of an admitted non-member — leaving the
                    // receiver with periodic keyframes only (frozen video).
                    // The flag lets the budget stage evaluate the sender
                    // against an augmented AllowSet (see
                    // `should_drop_non_member_for_layer_budget`).
                    let mut non_member_video_admit = false;
                    let allowed = match media_type {
                        MediaType::AUDIO => allow.audio.contains(&sender_sid) || recv_all_audio,
                        // SCREEN rides the same allow tier as VIDEO (it's a
                        // visual stream; the subscription model has no
                        // SCREEN-specific tier today).
                        MediaType::VIDEO | MediaType::SCREEN => {
                            if allow.video.contains_key(&sender_sid) {
                                true
                            } else if recv_all_video {
                                // vc-72a cap interaction: the receive-all
                                // fallback honors MAX_VISIBLE_VIDEO, like
                                // vc-3s8's `receive_all_video` catch-all tier.
                                // The membership-bound `allow.video` already
                                // caps local members at the ceiling; a
                                // non-member is admitted only while there is
                                // leftover capacity below the cap. Local
                                // members deterministically win the cap because
                                // they always populate `allow.video` first.
                                //
                                // ACCEPTED, BOUNDED LIMITATION (keyframe
                                // over-admit): the cap is measured against
                                // `allow.video.len()` — LOCAL members only,
                                // always <= MAX_VISIBLE_VIDEO — NOT against a
                                // running count of distinct non-members
                                // admitted. So when local members number < 6
                                // the `len() < cap` test passes for an
                                // unbounded number of distinct non-members.
                                // Sustained (non-base-layer) video from those
                                // excess non-members is still bounded: it must
                                // also clear the downstream budget stage
                                // (`should_drop_non_member_for_layer_budget`),
                                // which the SVC budget caps per receiver. The
                                // residual leak is base keyframes (T0+S0):
                                // those bypass the budget stage and clear only
                                // this gate, so a receive-all receiver can
                                // receive base keyframes from >MAX_VISIBLE_VIDEO
                                // non-members when local members < 6. This is
                                // accepted because keyframes are periodic (not
                                // per-frame) — a small, bounded overhead, not a
                                // sustained stream. A fully-precise cap would
                                // require per-receiver tracking of which
                                // non-members have been admitted, state the
                                // stateless gate intentionally avoids on the
                                // hot path.
                                if allow.video.len() < MAX_VISIBLE_VIDEO as usize {
                                    non_member_video_admit = true;
                                    true
                                } else {
                                    false
                                }
                            } else {
                                false
                            }
                        }
                        _ => true,
                    };
                    if !allowed {
                        SFU_DROPPED_TOTAL
                            .with_label_values(&[sfu_drop_reason::UNSUBSCRIBED])
                            .inc();
                        trace_forward_decision(
                            &traced_room,
                            &sender_sid,
                            "drop",
                            sfu_drop_reason::UNSUBSCRIBED,
                        );
                        observe_decide_latency(start);
                        return ForwardDecision::Drop;
                    }

                    // p4-7/p4-8: VP9 SVC enhancement-layer drop. The
                    // base-layer keyframe (T0+S0) ALWAYS forwards —
                    // invariant 1 (dropping it breaks every dependent
                    // frame until the next keyframe). Higher-layer
                    // keyframes (T>0 or S>0) restart only the enhancement
                    // chain and are subject to the same budget check as
                    // P-frames. Legacy clients (no RoutingHeader) pass
                    // through. VIDEO/SCREEN only — audio has no SVC
                    // layers in this codebase today.
                    if matches!(media_type, MediaType::VIDEO | MediaType::SCREEN) {
                        if let Some(rh) = mp.routing_header.as_ref() {
                            let is_base_keyframe = rh.is_keyframe
                                && rh.temporal_layer_id == 0
                                && rh.spatial_layer_id == 0;
                            if is_base_keyframe {
                                SFU_KEYFRAME_FORWARDED_TOTAL.inc();
                            } else {
                                // vc-72a: a non-member sender admitted via the
                                // receive-all fallback is absent from the
                                // membership-bound `allow.video`, so the cached
                                // layer selection (and `ordered_senders`) would
                                // never allocate it a budget entry → every
                                // non-keyframe re-dropped. Evaluate it against
                                // an AllowSet augmented with this one sender,
                                // via a stateless `pick_layers` that neither
                                // reads nor writes the shared per-receiver
                                // selection cache (so member senders' cached
                                // budget is never poisoned by the transient
                                // augmentation).
                                let drop = if non_member_video_admit {
                                    self.should_drop_non_member_for_layer_budget(
                                        receiver_sid,
                                        sender_sid,
                                        rh.spatial_layer_id,
                                        rh.temporal_layer_id,
                                        &allow,
                                        &speakers_top,
                                        receiver_bw_kbps,
                                    )
                                } else {
                                    self.should_drop_for_layer_budget(
                                        receiver_sid,
                                        sender_sid,
                                        rh.spatial_layer_id,
                                        rh.temporal_layer_id,
                                        &allow,
                                        &speakers_top,
                                        speakers_generation,
                                        receiver_bw_kbps,
                                    )
                                };
                                if drop {
                                    SFU_DROPPED_TOTAL
                                        .with_label_values(&[sfu_drop_reason::LAYER_BUDGET])
                                        .inc();
                                    trace_forward_decision(
                                        &traced_room,
                                        &sender_sid,
                                        "drop",
                                        sfu_drop_reason::LAYER_BUDGET,
                                    );
                                    observe_decide_latency(start);
                                    return ForwardDecision::Drop;
                                }
                            }

                            // p4-9: reference-aware drop. Keyframes always
                            // pass through (they reset the reference chain
                            // — invariant 1). For everything else:
                            //   * A T0 delta that survived the layer-budget
                            //     check WILL be forwarded → record its
                            //     picture_id for this (receiver, sender).
                            //   * A T1/T2 whose `frame_marker` claims a T0
                            //     reference is dropped if that T0 is NOT
                            //     in the recent set — its reference picture
                            //     was never delivered to the decoder.
                            // p4-8 makes T0 layer-budget-drop impossible
                            // in practice, but the AllowSet flip case is
                            // still real, so this check is meaningful.
                            if !rh.is_keyframe {
                                let key = (receiver_sid, sender_sid);
                                let now = Instant::now();
                                if rh.temporal_layer_id == 0 {
                                    let mut guard = match self.recent_t0.write() {
                                        Ok(g) => g,
                                        Err(poisoned) => poisoned.into_inner(),
                                    };
                                    guard.entry(key).or_default().insert(rh.picture_id, now);
                                } else if rh.frame_marker & REFERENCES_T0 != 0 {
                                    let guard = match self.recent_t0.read() {
                                        Ok(g) => g,
                                        Err(poisoned) => poisoned.into_inner(),
                                    };
                                    let seen = guard
                                        .get(&key)
                                        .is_some_and(|s| s.contains(rh.picture_id, now));
                                    if !seen {
                                        SFU_DROPPED_TOTAL
                                            .with_label_values(&[sfu_drop_reason::REFERENCE_MISS])
                                            .inc();
                                        trace_forward_decision(
                                            &traced_room,
                                            &sender_sid,
                                            "drop",
                                            sfu_drop_reason::REFERENCE_MISS,
                                        );
                                        observe_decide_latency(start);
                                        return ForwardDecision::Drop;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // 3. Forward.
        SFU_FORWARDED_TOTAL
            .with_label_values(&[packet_type_label(packet_wrapper)])
            .inc();
        // vc-8wd Layer 1: un-labeled aggregate forward counter.
        SFU_FORWARD_TOTAL.inc();
        trace_forward_decision(&traced_room, &packet_wrapper.session_id, "forward", "ok");
        observe_decide_latency(start);
        ForwardDecision::Forward
    }

    /// Reap all per-session state held by this forwarder for `sid`.
    ///
    /// Called from the chat-server `LeaveRoom` path so that
    /// receivers and senders that join/leave a long-lived room do
    /// not accumulate ~2KB per (receiver, sender) pair in `recent_t0`
    /// plus hysteresis state in the `LayerSelector` indefinitely.
    ///
    /// Specifically:
    ///
    /// * Removes every `recent_t0` entry whose key is `(sid, *)` or
    ///   `(*, sid)` — `sid` may have been either the receiver or the
    ///   sender side of the pair.
    /// * Calls [`LayerSelector::prune_stale`] for `sid`, which drops
    ///   any hysteresis state plus the cached selection keyed by
    ///   `sid` as a receiver.
    ///
    /// Idempotent: safe to call for a `sid` that has no state.
    pub fn prune_session(&self, sid: SessionId) {
        {
            let mut guard = match self.recent_t0.write() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            guard.retain(|&(rcv, snd), _| rcv != sid && snd != sid);
        }
        self.layer_selector.prune_stale(sid);
    }

    /// Test-only accessor: number of `(receiver, sender)` pairs
    /// currently tracked in the recent-T0 map. Used to assert the
    /// pruning side-effect in unit tests.
    #[cfg(test)]
    pub fn recent_t0_pair_count(&self) -> usize {
        let guard = match self.recent_t0.read() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.len()
    }

    /// Test-only accessor: `true` iff the recent-T0 map currently
    /// holds an entry for the exact `(receiver, sender)` key.
    #[cfg(test)]
    pub fn recent_t0_contains_pair(&self, receiver: SessionId, sender: SessionId) -> bool {
        let guard = match self.recent_t0.read() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.contains_key(&(receiver, sender))
    }
}

impl Forwarder {
    /// Decide whether a VP9 SVC enhancement layer should be dropped for
    /// `receiver_sid` per the cached [`crate::sfu::layer_selector::LayerSelection`].
    ///
    /// Lazy-refresh contract:
    ///
    /// * No cached selection for the receiver → pass through (return
    ///   `false`). The bandwidth-ingest path is the authoritative place
    ///   to seed the cache; a receiver that has never reported a
    ///   bandwidth estimate gets the legacy "forward everything that
    ///   passed AllowSet" behavior.
    /// * Cached selection's `generation` matches the live speaker
    ///   generation AND `bandwidth_kbps` matches the live estimate →
    ///   consult the cache directly (lock-free `Arc` clone out of a
    ///   [`dashmap::DashMap`] shard — never blocks other receivers'
    ///   reads).
    /// * Otherwise (stale generation / bandwidth) → call
    ///   [`crate::sfu::layer_selector::LayerSelector::recompute_for_receiver`]
    ///   which acquires the hysteresis mutex briefly, runs the greedy
    ///   selection, then atomically publishes the new `CachedSelection`
    ///   into the DashMap.
    ///
    /// Returns `true` when the packet should be dropped as exceeding
    /// the receiver's layer budget. Callers are responsible for emitting
    /// the `sfu_dropped_total{reason="layer_budget"}` metric.
    #[allow(clippy::too_many_arguments)]
    fn should_drop_for_layer_budget(
        &self,
        receiver_sid: SessionId,
        sender_sid: SessionId,
        spatial_layer_id: u32,
        temporal_layer_id: u32,
        allow_set: &crate::sfu::subscription::AllowSet,
        speaker_set: &[SessionId],
        speakers_generation: u64,
        receiver_bw_kbps: Option<u32>,
    ) -> bool {
        // No bandwidth estimate yet → pass through (legacy fan-out for
        // freshly-joined receivers). Same rationale as the cache-miss
        // case below: we don't have enough information to make a
        // sensible drop decision.
        let bw_kbps = match receiver_bw_kbps {
            Some(v) => v,
            None => return false,
        };

        // Fast path: lock-free read of the cached selection. The DashMap
        // get/clone is a single shard lock that contends only with
        // writes to THIS receiver's entry (rare — ~1 Hz on bandwidth-
        // estimate refresh). Other receivers' decide() calls run
        // entirely concurrently.
        let cached = self.layer_selector.last_selection_for(receiver_sid);
        if let Some(cached) = cached.as_ref() {
            if cached.generation == speakers_generation && cached.bandwidth_kbps == bw_kbps {
                return match cached
                    .selection
                    .forward
                    .get(&(sender_sid, spatial_layer_id))
                {
                    Some(&max_t) => temporal_layer_id > max_t,
                    None => true,
                };
            }
        }

        // Slow path: cache miss / stale → recompute and publish atomically.
        // The "no cached selection at all" case is treated as a stale
        // miss here (rather than legacy pass-through) because the
        // receiver HAS a bandwidth estimate, so we have enough info to
        // make a proper decision.
        self.layer_selector.recompute_for_receiver(
            receiver_sid,
            allow_set,
            speaker_set,
            bw_kbps,
            speakers_generation,
        );
        let cached = self
            .layer_selector
            .last_selection_for(receiver_sid)
            .expect("recompute_for_receiver just inserted the entry");
        match cached
            .selection
            .forward
            .get(&(sender_sid, spatial_layer_id))
        {
            Some(&max_t) => temporal_layer_id > max_t,
            None => true,
        }
    }

    /// vc-72a: layer-budget decision for a sender admitted via the
    /// receive-all fallback that is NOT in the membership-bound `AllowSet`.
    ///
    /// The shared per-receiver layer-selection cache is keyed on
    /// `(receiver, speakers_generation, bandwidth)` and is the source of
    /// truth for the LOCAL members' budgets. Feeding it an AllowSet
    /// augmented with a transient non-member sender would poison that cache
    /// for every member sender at the same generation, so we deliberately do
    /// NOT touch it here.
    ///
    /// Instead we clone the membership-bound `AllowSet`, splice in this one
    /// sender at base-layer [`LayerPref`], and run the stateless
    /// [`crate::sfu::layer_selector::LayerSelector::pick_layers`] greedy
    /// selection (no hysteresis, no cache read/write). Within that throwaway
    /// computation the non-member is ordered by ascending `SessionId` among
    /// the non-speaker senders (see `ordered_senders`), so it consumes only
    /// the leftover budget after the senders ahead of it in that ordering —
    /// it does NOT sit at the tail and a low-id non-member can be ordered
    /// before a higher-id local member. That ordering is irrelevant to the
    /// LOCAL members' real budgets, though: each member's drop decision is
    /// computed separately via the unaugmented, cached
    /// [`Self::should_drop_for_layer_budget`] path, which never sees this
    /// transient sender. So no member is actually starved regardless of where
    /// the non-member lands in this one-off ordering — the only stream this
    /// computation governs is the non-member's own.
    ///
    /// Returns `true` when this `(spatial, temporal)` exceeds the budget the
    /// stateless selection allocated to the sender (or the sender got no
    /// budget at all). Base keyframes are handled by the caller before this
    /// is reached, so a `None` budget entry here is a genuine drop.
    ///
    /// Cost: one `AllowSet` clone + one greedy `pick_layers` pass, paid only
    /// on a non-base-layer video packet from a non-member sender to a
    /// receive-all receiver that has reported a bandwidth estimate — a rare
    /// combination relative to the steady-state member hot path.
    #[allow(clippy::too_many_arguments)]
    fn should_drop_non_member_for_layer_budget(
        &self,
        receiver_sid: SessionId,
        sender_sid: SessionId,
        spatial_layer_id: u32,
        temporal_layer_id: u32,
        allow_set: &crate::sfu::subscription::AllowSet,
        speaker_set: &[SessionId],
        receiver_bw_kbps: Option<u32>,
    ) -> bool {
        use crate::sfu::subscription::AllowSet;

        // No bandwidth estimate → pass through (legacy fan-out for a
        // freshly-joined receiver), matching `should_drop_for_layer_budget`.
        let bw_kbps = match receiver_bw_kbps {
            Some(v) => v,
            None => return false,
        };

        // Augment a clone of the membership-bound AllowSet with this sender.
        let mut augmented = AllowSet {
            audio: allow_set.audio.clone(),
            video: allow_set.video.clone(),
        };
        augmented.video.entry(sender_sid).or_default();

        let selection =
            self.layer_selector
                .pick_layers(receiver_sid, &augmented, speaker_set, bw_kbps);
        match selection.forward.get(&(sender_sid, spatial_layer_id)) {
            Some(&max_t) => temporal_layer_id > max_t,
            None => true,
        }
    }
}

fn observe_decide_latency(start: std::time::Instant) {
    let elapsed_us = start.elapsed().as_micros() as f64;
    SFU_DECIDE_LATENCY_US.observe(elapsed_us);
}

/// vc-8wd Layer 2: emit a SAMPLED structured trace for a per-packet forward
/// decision on the `sfu_trace` target.
///
/// `traced_room` is `Some(room)` ONLY when tracing is armed AND this packet
/// belongs to the configured trace room (resolved once per `decide` under
/// the room lock). When `None` — the steady-state default — this is a single
/// branch and returns immediately, doing no formatting and no allocation.
///
/// When `Some`, the 1-in-N [`trace::should_sample_forward`] sampler bounds
/// the log volume so a traced room doesn't flood. Only the sampled lines
/// touch the `tracing` macro (and thus any formatting).
#[inline]
fn trace_forward_decision(
    traced_room: &Option<String>,
    sender_sid: &SessionId,
    decision: &'static str,
    reason: &'static str,
) {
    if let Some(room) = traced_room {
        if trace::should_sample_forward() {
            tracing::debug!(
                target: "sfu_trace",
                room = %room,
                sender = sender_sid,
                decision,
                reason,
                "forward decision (sampled)"
            );
        }
    }
}
