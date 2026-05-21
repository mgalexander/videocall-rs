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

//! Authoritative per-room state for the SFU.
//!
//! `RoomState` is a plain data structure (no internal lock); the caller is
//! expected to wrap it in an `Arc<RwLock<RoomState>>` at the layer above
//! (p2-6 wires lifecycle from `chat_server`). This module only provides the
//! member table + capabilities cache the Forwarder reads.
//!
//! Capability bits are defined on the wire by the `CONNECTION` packet and
//! must match the values in `videocall-client/src/connection/connection_manager.rs`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use videocall_types::protos::diagnostics_packet::BandwidthEstimate;

use crate::actors::session_logic::SessionId;

/// Client supports the SFU routing header on media packets.
pub const CAP_SFU_ROUTING_HEADER: u32 = 1;

/// Client supports scalable video coding (SVC) layered encoding.
pub const CAP_SVC: u32 = 2;

/// Client supports the subscription model (subscribe/unsubscribe to peers).
pub const CAP_SUBSCRIPTION: u32 = 4;

/// vc-17e: absolute-kbps threshold above which a bandwidth-estimate refresh
/// is considered significant enough to invalidate the LayerSelector cache.
/// Tuned to be larger than typical BWE jitter (~tens of kbps) but small
/// relative to T0/T1/T2 step sizes so legitimate layer transitions are
/// never masked.
pub const BWE_INVALIDATE_ABS_KBPS: u32 = 50;

/// vc-54j2: maximum number of distinct cross-pod (non-local-member) MEDIA
/// publishers tracked per room in the [`RoomState::remote_publishers`]
/// registry. A federated webinar has a small fixed set of senders (≤ ~10);
/// listeners never publish, so this set is bounded by sender count, NOT by
/// receiver count. The hard cap is a DoS backstop against a misbehaving /
/// spoofing peer minting media from many fabricated session ids — past the
/// cap, the oldest (least-recently-seen) entry is evicted.
pub const MAX_REMOTE_PUBLISHERS: usize = 32;

/// vc-54j2: TTL after which a remote-publisher registry entry is reaped if it
/// has not been refreshed by a fresh MEDIA packet from that sid. A live sender
/// publishes many packets per second, so a 10s window is far past any
/// plausible inter-packet gap for an active publisher, while short enough that
/// a sender who left the federated room drops out of every local receiver's
/// AllowSet promptly.
pub const REMOTE_PUBLISHER_TTL: Duration = Duration::from_secs(10);

/// vc-54j2: minimum interval between liveness writes for an already-tracked,
/// unchanged remote publisher. On a busy spill pod EVERY inbound media packet
/// is from a non-local member, so an unthrottled `note_remote_publisher` would
/// take the room's `RwLock` in WRITE mode once per packet, contending with the
/// per-packet per-receiver `decide` read fan-out on the same lock. The
/// registry only needs liveness granularity at TTL scale, so we coalesce
/// liveness refreshes to at most one write per tracked publisher per second.
///
/// Kept safely below [`REMOTE_PUBLISHER_TTL`] (10s) so a still-publishing
/// sender's `last_seen` is refreshed long before it could be reaped. A
/// brand-new sid and an audio→video upgrade are NEVER throttled — those change
/// the resolution-relevant snapshot and must land promptly.
pub const REMOTE_PUBLISHER_LIVENESS_THROTTLE: Duration = Duration::from_secs(1);

/// vc-17e: relative threshold (in percent) used in conjunction with
/// [`BWE_INVALIDATE_ABS_KBPS`]. Either crossing triggers invalidation, so
/// an estimate update is skipped only when BOTH the absolute and relative
/// diffs are below their respective thresholds. The relative threshold
/// matters in the low-bandwidth regime (10% of 200 kbps = 20 kbps, well
/// under the absolute floor); the absolute threshold matters in the
/// high-bandwidth regime (50 kbps on a 2 Mbps link is ~2.5%).
pub const BWE_INVALIDATE_REL_PCT: u32 = 10;

/// Per-member entry tracked by the room.
///
/// Speaker-scoring fields (`last_speaker_score`, `is_speaking`) are present
/// for the layout the Speaker tracker (P3) will populate; today they default
/// to inert values. `is_observer` is a placeholder until p2-6 wires it from
/// the `JoinRoom` path.
#[derive(Debug, Clone)]
pub struct MemberEntry {
    pub session_id: SessionId,
    pub joined_at: Instant,
    /// Bitmask from the client's `CONNECTION` packet.
    pub capabilities: u32,
    /// Exponentially-weighted moving average of recent speaker scores.
    /// Populated by the speaker tracker in P3; defaults to `0.0`.
    pub last_speaker_score: f32,
    pub is_speaking: bool,
    /// Observers receive media but do not send any. Set by the JoinRoom
    /// path in p2-6.
    pub is_observer: bool,
    /// Last receiver downlink bandwidth estimate reported by this member's
    /// client via `DiagnosticsPacket.bandwidth_estimate` (p4-4), filtered
    /// to the most recent value that materially differed from the prior
    /// reading (vc-17e: see [`RoomState::update_bandwidth_estimate`]).
    /// Consumed by the LayerSelector (p4-5) to budget per-receiver layer
    /// selection. `None` until the first estimate arrives — clients may
    /// join and publish media before they have any congestion data to
    /// share.
    pub bandwidth_estimate: Option<BandwidthEstimate>,
    /// Wall-clock instant at which the client most recently reported a
    /// `DiagnosticsPacket.bandwidth_estimate`, regardless of whether that
    /// report crossed the vc-17e divergence threshold. Tracks liveness of
    /// the diagnostics uplink — use this to detect a stuck client, not
    /// the freshness of `bandwidth_estimate` as a numeric value.
    /// `None` iff `bandwidth_estimate` is `None`.
    pub bandwidth_estimate_updated_at: Option<Instant>,
}

/// vc-54j2: one tracked cross-pod publisher in the [`RoomState`] registry.
///
/// A "remote publisher" is a session id whose MEDIA the local dispatcher
/// receives over NATS federation but which is NOT a local room member (it
/// joined a different pod). The registry lets local receivers' AllowSets be
/// augmented with these senders so the forwarder admits their media instead
/// of hard-dropping it as `unsubscribed`.
#[derive(Debug, Clone)]
struct RemotePublisherEntry {
    /// Wall-clock instant of the most recent MEDIA packet seen from this sid.
    /// Drives TTL reaping ([`REMOTE_PUBLISHER_TTL`]) and oldest-first eviction
    /// when the registry is at [`MAX_REMOTE_PUBLISHERS`].
    last_seen: Instant,
    /// `true` once a VIDEO/SCREEN MediaPacket has been seen from this sid.
    /// Audio-only publishers stay `false` so they never consume a slot in the
    /// visible-video budget. Sticky: once a sender shows video it is counted
    /// as a video publisher for the rest of its registry lifetime.
    has_video: bool,
}

/// vc-54j2: immutable per-room snapshot of the remote-publisher registry,
/// shared with the forwarder hot path via an `Arc` (one refcount bump per
/// resolve, no allocation), mirroring the `members_snapshot` pattern.
///
/// * `audio` — every tracked remote publisher (all of them are audible to a
///   receive-all listener; audio is not subject to the visible-video cap).
/// * `video` — the subset that has shown a VIDEO/SCREEN packet, counted
///   against [`crate::sfu::subscription::MAX_VISIBLE_VIDEO`].
#[derive(Debug, Default)]
pub struct RemotePublishers {
    pub audio: HashSet<SessionId>,
    pub video: HashSet<SessionId>,
}

/// Authoritative per-room state for the SFU.
///
/// The struct does not own any synchronization primitive; wrap it in an
/// `Arc<RwLock<RoomState>>` (or equivalent) at the caller.
///
/// vc-7gc: alongside the [`HashMap`] of full member entries, we cache a
/// `members_snapshot: Arc<HashSet<SessionId>>` that is rebuilt atomically on
/// every membership mutation (insert / remove). The Forwarder's per-packet
/// `decide` path clones the `Arc` (one refcount bump, no allocation) instead
/// of building a fresh `HashSet` from scratch on every packet — at 20 receivers
/// × 1000 pps that previously meant ~20k allocs/sec/room of allocator churn.
/// Membership changes are rare (peer join / leave), so paying the rebuild cost
/// on the cold mutation path is a clear win.
#[derive(Debug)]
pub struct RoomState {
    pub room_id: String,
    pub members: HashMap<SessionId, MemberEntry>,
    /// Cached set of current member session ids, kept in sync with
    /// `members` by [`Self::insert_member`] and [`Self::remove_member`].
    /// Hot readers (e.g. `Forwarder::decide`) clone this `Arc` to obtain a
    /// stable snapshot without allocating a new `HashSet`.
    ///
    /// Invariant: `members_snapshot.iter().copied().collect::<HashSet<_>>()
    /// == members.keys().copied().collect::<HashSet<_>>()`.
    /// Maintained by routing every keyset-changing mutation through
    /// [`Self::rebuild_members_snapshot`].
    members_snapshot: Arc<HashSet<SessionId>>,
    /// Monotonic counter bumped on every actual keyset change (insert of
    /// a new sid, removal of an existing sid). Re-inserting an already
    /// present sid does NOT bump the generation — it preserves the
    /// `Arc::ptr_eq` "no rebuild on reconnect" contract that
    /// [`Self::rebuild_members_snapshot`] documents.
    ///
    /// Consumed by `SubscriptionStore::resolve_cached` (vc-2cx) to detect
    /// stale cached `AllowSet`s without comparing the underlying `Arc`
    /// pointer (which is vulnerable to ABA reuse if a freshly-allocated
    /// `HashSet` happens to land at the same address as a recycled one).
    members_generation: u64,
    /// vc-54j2: cross-pod publishers seen on the dispatcher MEDIA ingress that
    /// are NOT local members. Bounded by [`MAX_REMOTE_PUBLISHERS`] and reaped
    /// by [`REMOTE_PUBLISHER_TTL`]. Written on the dispatcher's once-per-
    /// inbound-message path (NOT per receiver) via
    /// [`Self::note_remote_publisher`]; read on the forwarder's resolve path
    /// via the [`Self::remote_publishers_snapshot_with_generation`] `Arc`.
    remote_publishers: HashMap<SessionId, RemotePublisherEntry>,
    /// Cached snapshot of [`Self::remote_publishers`], rebuilt only when the
    /// registry's *contents that matter to resolution* change (a new sid, a
    /// reaped sid, or an audio-only sid gaining video). Hot readers clone the
    /// `Arc`; a refresh-only `note_remote_publisher` (same sids, same video
    /// flags) does NOT rebuild, preserving cache stability.
    remote_publishers_snapshot: Arc<RemotePublishers>,
}

impl RoomState {
    /// Create a new empty room.
    pub fn new(room_id: String) -> Self {
        Self {
            room_id,
            members: HashMap::new(),
            members_snapshot: Arc::new(HashSet::new()),
            members_generation: 0,
            remote_publishers: HashMap::new(),
            remote_publishers_snapshot: Arc::new(RemotePublishers::default()),
        }
    }

    /// Rebuild the cached `members_snapshot` from the current `members` map.
    ///
    /// Called from every mutation that changes the membership keyset
    /// ([`Self::insert_member`] / [`Self::remove_member`]). The previous
    /// `Arc` is dropped only after the new one is published, so concurrent
    /// readers that already hold a clone continue to observe a consistent
    /// (if slightly stale) snapshot — exactly the contract `decide` relies
    /// on.
    ///
    /// Also bumps `members_generation` so downstream caches (e.g.
    /// `SubscriptionStore::resolve_cached`, vc-2cx) can invalidate
    /// pre-rebuild entries safely. Callers MUST only invoke this when the
    /// keyset has actually changed — the existing "no rebuild on
    /// reconnect" optimisation depends on the generation NOT bumping on
    /// no-op updates.
    fn rebuild_members_snapshot(&mut self) {
        let snapshot: HashSet<SessionId> = self.members.keys().copied().collect();
        self.members_snapshot = Arc::new(snapshot);
        self.members_generation = self.members_generation.wrapping_add(1);
    }

    /// Lock-free clone of the current members snapshot.
    ///
    /// Returns an `Arc` so the hot path can avoid allocating a fresh
    /// `HashSet` per call. The returned snapshot reflects membership as of
    /// the most recent [`Self::insert_member`] / [`Self::remove_member`]
    /// call observed by this thread.
    pub fn members_snapshot(&self) -> Arc<HashSet<SessionId>> {
        Arc::clone(&self.members_snapshot)
    }

    /// Lock-free clone of the current members snapshot together with the
    /// generation counter that identifies it.
    ///
    /// Hot callers that want to feed a downstream cache (e.g.
    /// `SubscriptionStore::resolve_cached`) need both halves to be read
    /// atomically under the same lock acquisition — otherwise a mutation
    /// between the two reads could pair a fresh snapshot with a stale
    /// generation (or vice versa) and silently serve a wrong AllowSet.
    pub fn members_snapshot_with_generation(&self) -> (Arc<HashSet<SessionId>>, u64) {
        (Arc::clone(&self.members_snapshot), self.members_generation)
    }

    /// vc-54j2: lock-free clone of the current remote-publisher snapshot.
    ///
    /// The generation returned is the SHARED `members_generation` counter —
    /// it is bumped by both membership changes AND remote-publisher registry
    /// changes, because both are inputs to `SubscriptionStore::resolve_inner`.
    /// Returning it here lets the forwarder feed
    /// `SubscriptionStore::resolve_cached` a single cache key that invalidates
    /// whenever EITHER input moves, with no extra counter to thread.
    pub fn remote_publishers_snapshot_with_generation(&self) -> (Arc<RemotePublishers>, u64) {
        (
            Arc::clone(&self.remote_publishers_snapshot),
            self.members_generation,
        )
    }

    /// vc-54j2: read-only predicate the dispatcher uses (under a READ lock) to
    /// decide whether a `note_remote_publisher` WRITE is actually required for
    /// an inbound media packet from `sid`. Lets the busy spill-pod hot path
    /// skip the write lock entirely on the overwhelmingly common case: a
    /// liveness refresh of an already-tracked publisher whose video flag has
    /// not changed and whose `last_seen` is younger than
    /// [`REMOTE_PUBLISHER_LIVENESS_THROTTLE`].
    ///
    /// Returns `true` (write needed) when:
    /// * `sid` is a brand-new remote publisher (not yet tracked) — must land
    ///   promptly so receivers' AllowSets pick it up, OR
    /// * an audio→video upgrade is observed (`is_video` and the entry is still
    ///   audio-only) — changes the resolution-relevant snapshot, must not be
    ///   throttled, OR
    /// * the tracked entry's `last_seen` is older than the throttle interval —
    ///   a liveness refresh to keep TTL reaping from evicting a still-active
    ///   sender.
    ///
    /// Returns `false` (skip the write) for a fresh, video-consistent liveness
    /// refresh. A sid that IS a local member also returns `false` — the
    /// membership path owns it and `note_remote_publisher` would self-skip
    /// anyway.
    pub fn remote_publisher_write_needed(
        &self,
        sid: SessionId,
        is_video: bool,
        now: Instant,
    ) -> bool {
        if self.members.contains_key(&sid) {
            return false;
        }
        match self.remote_publishers.get(&sid) {
            // Brand-new publisher — must register promptly.
            None => true,
            Some(entry) => {
                // Audio→video upgrade must not be throttled.
                if is_video && !entry.has_video {
                    return true;
                }
                // Otherwise only refresh liveness past the throttle window.
                now.duration_since(entry.last_seen) >= REMOTE_PUBLISHER_LIVENESS_THROTTLE
            }
        }
    }

    /// vc-54j2: rebuild the cached remote-publisher snapshot from the registry
    /// and bump the shared generation so downstream AllowSet caches invalidate.
    ///
    /// Called only when the registry's resolution-relevant contents change (a
    /// sid added/reaped, or an audio-only sid gaining video) — never on a
    /// pure liveness refresh of an already-tracked video publisher.
    fn rebuild_remote_publishers_snapshot(&mut self) {
        let mut audio = HashSet::with_capacity(self.remote_publishers.len());
        let mut video = HashSet::new();
        for (&sid, entry) in &self.remote_publishers {
            audio.insert(sid);
            if entry.has_video {
                video.insert(sid);
            }
        }
        self.remote_publishers_snapshot = Arc::new(RemotePublishers { audio, video });
        self.members_generation = self.members_generation.wrapping_add(1);
    }

    /// vc-54j2: register (or refresh) a cross-pod publisher seen on the
    /// dispatcher MEDIA ingress.
    ///
    /// Called once per inbound MEDIA message (NOT per receiver) for a sender
    /// that is NOT a local room member. Local members are the authoritative
    /// `JoinRoom` path's responsibility and are excluded here, so a sender that
    /// is (or becomes) a local member never lands in this registry — the
    /// forwarder's intra-pod path is unchanged.
    ///
    /// Semantics:
    /// * `is_video` is `true` for VIDEO/SCREEN packets, `false` for AUDIO.
    /// * A new sid is inserted and the snapshot rebuilt (generation bumps).
    /// * An existing audio-only sid that now shows video is upgraded to
    ///   `has_video = true` and the snapshot rebuilt.
    /// * A pure liveness refresh (already tracked, video flag unchanged) only
    ///   updates `last_seen` — NO snapshot rebuild, NO generation bump, so the
    ///   per-receiver AllowSet cache stays hot at steady state.
    ///
    /// Bounding + reaping (both O(registry size) ≤ [`MAX_REMOTE_PUBLISHERS`],
    /// independent of receiver count):
    /// * Stale entries (older than [`REMOTE_PUBLISHER_TTL`]) are reaped on
    ///   every call.
    /// * If, after reaping, inserting a NEW sid would exceed
    ///   [`MAX_REMOTE_PUBLISHERS`], the least-recently-seen entry is evicted.
    pub fn note_remote_publisher(&mut self, sid: SessionId, is_video: bool, now: Instant) {
        // A sender that is a local member is handled by the membership path;
        // never shadow it in the remote registry.
        if self.members.contains_key(&sid) {
            return;
        }

        // Reap stale entries first (cheap: the map is capped at
        // MAX_REMOTE_PUBLISHERS). Track whether anything changed so we only
        // rebuild the snapshot when resolution-relevant state actually moved.
        let before_len = self.remote_publishers.len();
        self.remote_publishers
            .retain(|_, e| now.duration_since(e.last_seen) <= REMOTE_PUBLISHER_TTL);
        let mut changed = self.remote_publishers.len() != before_len;

        match self.remote_publishers.get_mut(&sid) {
            Some(entry) => {
                entry.last_seen = now;
                if is_video && !entry.has_video {
                    entry.has_video = true;
                    changed = true;
                }
            }
            None => {
                // Eviction backstop: never let a spoofing peer grow the
                // registry without bound. Drop the least-recently-seen entry.
                if self.remote_publishers.len() >= MAX_REMOTE_PUBLISHERS {
                    if let Some((&oldest, _)) = self
                        .remote_publishers
                        .iter()
                        .min_by_key(|(_, e)| e.last_seen)
                    {
                        self.remote_publishers.remove(&oldest);
                    }
                }
                self.remote_publishers.insert(
                    sid,
                    RemotePublisherEntry {
                        last_seen: now,
                        has_video: is_video,
                    },
                );
                changed = true;
            }
        }

        if changed {
            self.rebuild_remote_publishers_snapshot();
        }
    }

    /// vc-54j2: drop a session from the remote-publisher registry (called from
    /// the forwarder's `prune_session` via the chat-server `LeaveRoom` path,
    /// and whenever a remote publisher becomes a local member). Idempotent.
    pub fn prune_remote_publisher(&mut self, sid: SessionId) {
        if self.remote_publishers.remove(&sid).is_some() {
            self.rebuild_remote_publishers_snapshot();
        }
    }

    /// Test-only: number of tracked remote publishers.
    #[cfg(test)]
    pub fn remote_publisher_count(&self) -> usize {
        self.remote_publishers.len()
    }

    /// Insert (or replace) a member with the given capabilities bitmask.
    ///
    /// Re-inserting an existing `session_id` overwrites the previous entry,
    /// which resets `joined_at` and clears any speaker-tracker state. This
    /// mirrors the semantics of a re-connecting peer.
    pub fn insert_member(&mut self, sid: SessionId, capabilities: u32) {
        let entry = MemberEntry {
            session_id: sid,
            joined_at: Instant::now(),
            capabilities,
            last_speaker_score: 0.0,
            is_speaking: false,
            is_observer: false,
            bandwidth_estimate: None,
            bandwidth_estimate_updated_at: None,
        };
        let was_present = self.members.insert(sid, entry).is_some();
        // Only rebuild the snapshot when the keyset actually changed.
        // Re-inserting an existing `sid` (reconnect) keeps the same key,
        // so the cached `Arc` remains correct and we save an allocation.
        if !was_present {
            self.rebuild_members_snapshot();
        }
        // vc-54j2: a sid that is now a local member must not also live in the
        // remote-publisher registry (it would be double-counted against the
        // visible-video budget and waste a registry slot). Drop it; the
        // membership path is authoritative for local senders.
        self.prune_remote_publisher(sid);
    }

    /// Update the cached bandwidth estimate for an existing member.
    ///
    /// Called from the per-room dispatcher on each inbound
    /// `DiagnosticsPacket` whose `bandwidth_estimate` field is populated
    /// (p4-4). The estimate becomes the input to the LayerSelector (p4-5)
    /// when choosing per-receiver SVC layers.
    ///
    /// No-op if `sid` is not present in the room: clients can briefly send
    /// diagnostics packets that arrive after a `LeaveRoom`/disconnect has
    /// already pruned the member from the room state. We deliberately
    /// avoid auto-inserting a phantom member entry — the JoinRoom path is
    /// the sole authority on membership.
    ///
    /// **vc-17e — divergence-gated persistence.** The stored
    /// [`MemberEntry::bandwidth_estimate`] is the cache baseline that the
    /// LayerSelector recompute fast-path (`forwarder.rs` cache-validity
    /// check) reads to decide whether its cached selection is still valid.
    /// That check is exact equality on `estimated_downlink_kbps`, so if we
    /// overwrite the stored value on every diagnostics tick the cache will
    /// miss every tick — defeating the whole point of suppressing the
    /// LayerSelector invalidate call.
    ///
    /// Therefore: when the new estimate is within noise of the stored one
    /// (below both [`BWE_INVALIDATE_ABS_KBPS`] absolute AND
    /// [`BWE_INVALIDATE_REL_PCT`] relative), we leave `bandwidth_estimate`
    /// unchanged but still bump `bandwidth_estimate_updated_at` so the
    /// liveness signal continues to reflect when the client last reported.
    /// Drift accumulates against the cached baseline and triggers a write
    /// (and invalidation) only when it crosses threshold.
    ///
    /// Returns `true` iff the caller should invalidate the LayerSelector
    /// cache for this receiver. Always `true` when there was no prior
    /// estimate; `true` when the new value diverges from the cached one
    /// by more than the absolute OR relative threshold; otherwise `false`.
    /// Spammy clients whose reports barely move the needle do not force an
    /// O(allow_set × speakers) recompute on every diagnostics tick.
    ///
    /// `#[must_use]` because dropping the return at the production callsite
    /// silently regresses the optimization (the caller would unconditionally
    /// invalidate). Test helpers that only seed state can `let _ = ...`.
    #[must_use]
    pub fn update_bandwidth_estimate(&mut self, sid: SessionId, est: &BandwidthEstimate) -> bool {
        let Some(entry) = self.members.get_mut(&sid) else {
            return false;
        };
        // Always refresh the liveness timestamp — even sub-threshold writes
        // tell us the client's diagnostics uplink is alive.
        entry.bandwidth_estimate_updated_at = Some(Instant::now());

        let prev_kbps = entry
            .bandwidth_estimate
            .as_ref()
            .map(|e| e.estimated_downlink_kbps);
        let new_kbps = est.estimated_downlink_kbps;

        let significant = match prev_kbps {
            None => true,
            Some(prev) => {
                let abs_diff = prev.abs_diff(new_kbps);
                // (abs_diff * 100) / prev > BWE_INVALIDATE_REL_PCT,
                // rearranged to avoid floating point and divide-by-zero
                // (prev==0 with new>0 yields LHS>0 > RHS=0 = true).
                abs_diff > BWE_INVALIDATE_ABS_KBPS
                    || u64::from(abs_diff) * 100
                        > u64::from(prev) * u64::from(BWE_INVALIDATE_REL_PCT)
            }
        };

        if significant {
            entry.bandwidth_estimate = Some(est.clone());
        }
        significant
    }

    /// Remove a member from the room. No-op if absent.
    pub fn remove_member(&mut self, sid: SessionId) {
        if self.members.remove(&sid).is_some() {
            self.rebuild_members_snapshot();
        }
    }

    /// Return the capabilities bitmask for the given member, if present.
    pub fn get_capabilities(&self, sid: SessionId) -> Option<u32> {
        self.members.get(&sid).map(|m| m.capabilities)
    }

    /// True iff `sid` is a member AND its capabilities have every bit set
    /// that is set in `capability_bit`.
    pub fn supports(&self, sid: SessionId, capability_bit: u32) -> bool {
        self.members
            .get(&sid)
            .map(|m| (m.capabilities & capability_bit) == capability_bit)
            .unwrap_or(false)
    }

    /// Total number of members (senders + observers).
    pub fn member_count(&self) -> usize {
        self.members.len()
    }

    /// Iterator over members that send media (i.e. non-observers).
    pub fn senders(&self) -> impl Iterator<Item = &MemberEntry> {
        self.members.values().filter(|m| !m.is_observer)
    }

    /// vc-9eh: `true` if any member is a (non-observer) sender. A `bool`
    /// convenience so a caller holding a transient lock guard need not keep the
    /// borrowing iterator returned by [`senders`](Self::senders) alive across
    /// the guard's drop.
    pub fn has_senders(&self) -> bool {
        self.senders().next().is_some()
    }
}

impl Default for RoomState {
    fn default() -> Self {
        Self::new(String::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_estimate() -> BandwidthEstimate {
        let mut est = BandwidthEstimate::new();
        est.estimated_downlink_kbps = 1200;
        est.estimated_loss_rate = 0.02;
        est.rtt_ms = 47;
        est
    }

    #[test]
    fn member_defaults_have_no_bandwidth_estimate() {
        let mut room = RoomState::new("r".into());
        room.insert_member(7, 0);
        let entry = room.members.get(&7).expect("member should be present");
        assert!(entry.bandwidth_estimate.is_none());
        assert!(entry.bandwidth_estimate_updated_at.is_none());
    }

    #[test]
    fn update_bandwidth_estimate_round_trips_value_and_sets_timestamp() {
        let mut room = RoomState::new("r".into());
        room.insert_member(42, 0);

        // Establish a "before" instant we can compare the timestamp against.
        let before = Instant::now();
        let est = sample_estimate();
        // First update (no prior estimate) must always request invalidation.
        assert!(room.update_bandwidth_estimate(42, &est));

        let entry = room.members.get(&42).expect("member should be present");
        let stored = entry
            .bandwidth_estimate
            .as_ref()
            .expect("estimate should be present");
        assert_eq!(stored.estimated_downlink_kbps, est.estimated_downlink_kbps);
        assert_eq!(stored.estimated_loss_rate, est.estimated_loss_rate);
        assert_eq!(stored.rtt_ms, est.rtt_ms);

        let ts = entry
            .bandwidth_estimate_updated_at
            .expect("timestamp should be set");
        assert!(ts >= before);
        assert!(ts <= Instant::now());
    }

    #[test]
    fn update_bandwidth_estimate_is_noop_for_absent_member() {
        let mut room = RoomState::new("r".into());
        // Note: no insert_member — sid 99 is unknown.
        assert!(!room.update_bandwidth_estimate(99, &sample_estimate()));
        assert!(
            !room.members.contains_key(&99),
            "absent sid must not be auto-inserted"
        );
        assert_eq!(room.member_count(), 0);
    }

    #[test]
    fn members_snapshot_tracks_insert_and_remove() {
        let mut room = RoomState::new("r".into());
        assert!(room.members_snapshot().is_empty());

        room.insert_member(1, 0);
        room.insert_member(2, 0);
        let snap = room.members_snapshot();
        assert_eq!(snap.len(), 2);
        assert!(snap.contains(&1));
        assert!(snap.contains(&2));

        // Reconnect (re-insert same sid) MUST NOT change the snapshot.
        let snap_before_reinsert = room.members_snapshot();
        room.insert_member(1, 7);
        let snap_after_reinsert = room.members_snapshot();
        // Pointer-equal: no rebuild happened.
        assert!(Arc::ptr_eq(&snap_before_reinsert, &snap_after_reinsert));

        // Remove of an absent id is a no-op.
        let snap_before_noop = room.members_snapshot();
        room.remove_member(999);
        let snap_after_noop = room.members_snapshot();
        assert!(Arc::ptr_eq(&snap_before_noop, &snap_after_noop));

        room.remove_member(1);
        let snap = room.members_snapshot();
        assert_eq!(snap.len(), 1);
        assert!(snap.contains(&2));
    }

    #[test]
    fn members_generation_bumps_only_on_keyset_change() {
        let mut room = RoomState::new("r".into());
        let (_, gen0) = room.members_snapshot_with_generation();

        room.insert_member(1, 0);
        let (_, gen1) = room.members_snapshot_with_generation();
        assert_ne!(gen0, gen1, "first insert must bump generation");

        // Reconnect (re-insert same sid) must NOT bump.
        room.insert_member(1, 7);
        let (_, gen2) = room.members_snapshot_with_generation();
        assert_eq!(gen1, gen2, "reconnect must not bump generation");

        // Remove of an absent id must NOT bump.
        room.remove_member(999);
        let (_, gen3) = room.members_snapshot_with_generation();
        assert_eq!(gen2, gen3, "no-op remove must not bump generation");

        room.remove_member(1);
        let (_, gen4) = room.members_snapshot_with_generation();
        assert_ne!(gen3, gen4, "remove of present sid must bump generation");
    }

    #[test]
    fn members_snapshot_clone_is_stable_across_mutations() {
        // Readers that captured a snapshot before a mutation must continue
        // to observe their snapshot's contents — the rebuild publishes a
        // new Arc rather than mutating the old one.
        let mut room = RoomState::new("r".into());
        room.insert_member(1, 0);
        let pre = room.members_snapshot();
        assert_eq!(pre.len(), 1);

        room.insert_member(2, 0);
        // The previously-captured snapshot is unchanged.
        assert_eq!(pre.len(), 1);
        assert!(pre.contains(&1));
        assert!(!pre.contains(&2));
        // The newly-fetched snapshot reflects both members.
        assert_eq!(room.members_snapshot().len(), 2);
    }

    #[test]
    fn update_bandwidth_estimate_overwrites_previous_value() {
        let mut room = RoomState::new("r".into());
        room.insert_member(1, 0);

        let mut first = BandwidthEstimate::new();
        first.estimated_downlink_kbps = 500;
        assert!(
            room.update_bandwidth_estimate(1, &first),
            "first estimate (no prior value) must invalidate"
        );

        let mut second = BandwidthEstimate::new();
        second.estimated_downlink_kbps = 2500;
        assert!(
            room.update_bandwidth_estimate(1, &second),
            "5x jump must invalidate"
        );

        let stored = room
            .members
            .get(&1)
            .and_then(|m| m.bandwidth_estimate.as_ref())
            .expect("estimate should be present");
        assert_eq!(stored.estimated_downlink_kbps, 2500);
    }

    /// vc-17e: tiny diffs within noise must NOT trigger invalidation, so a
    /// chatty client cannot force an O(allow_set × speakers) recompute on
    /// every diagnostics tick. The stored value is intentionally left at
    /// the baseline so the LayerSelector cache-validity check (exact
    /// equality on `bandwidth_kbps`) continues to hit; only the liveness
    /// timestamp moves forward.
    #[test]
    fn update_bandwidth_estimate_skips_invalidation_within_noise() {
        let mut room = RoomState::new("r".into());
        room.insert_member(1, 0);

        let mut first = BandwidthEstimate::new();
        first.estimated_downlink_kbps = 2000;
        assert!(room.update_bandwidth_estimate(1, &first));
        let ts_after_first = room
            .members
            .get(&1)
            .and_then(|m| m.bandwidth_estimate_updated_at)
            .expect("timestamp set");

        // +10 kbps: below both the 50 kbps abs and 10% rel thresholds.
        let mut second = BandwidthEstimate::new();
        second.estimated_downlink_kbps = 2010;
        assert!(
            !room.update_bandwidth_estimate(1, &second),
            "10 kbps drift (0.5%) must not invalidate"
        );

        // Stored value MUST remain the baseline so the forwarder cache hits.
        let entry = room.members.get(&1).expect("member present");
        assert_eq!(
            entry
                .bandwidth_estimate
                .as_ref()
                .expect("estimate present")
                .estimated_downlink_kbps,
            2000,
            "sub-threshold updates must not overwrite the cache baseline"
        );
        // Liveness timestamp still advances (or stays the same instant —
        // the second write happens at >= the first instant).
        let ts_after_second = entry.bandwidth_estimate_updated_at.expect("timestamp set");
        assert!(ts_after_second >= ts_after_first);
    }

    /// vc-17e: absolute threshold triggers in the high-bandwidth regime where
    /// 50 kbps is well below the relative threshold (2.5% of 2 Mbps).
    #[test]
    fn update_bandwidth_estimate_invalidates_on_absolute_diff() {
        let mut room = RoomState::new("r".into());
        room.insert_member(1, 0);

        let mut first = BandwidthEstimate::new();
        first.estimated_downlink_kbps = 2000;
        assert!(room.update_bandwidth_estimate(1, &first));

        // +60 kbps absolute, 3% relative — abs threshold crosses, rel does not.
        let mut second = BandwidthEstimate::new();
        second.estimated_downlink_kbps = 2060;
        assert!(
            room.update_bandwidth_estimate(1, &second),
            "60 kbps move must invalidate via absolute threshold"
        );
    }

    /// vc-17e: relative threshold triggers in the low-bandwidth regime where
    /// 10% is well below the 50 kbps absolute threshold.
    #[test]
    fn update_bandwidth_estimate_invalidates_on_relative_diff() {
        let mut room = RoomState::new("r".into());
        room.insert_member(1, 0);

        let mut first = BandwidthEstimate::new();
        first.estimated_downlink_kbps = 100;
        assert!(room.update_bandwidth_estimate(1, &first));

        // +30 kbps = 30% relative, below the 50 kbps absolute floor. Rel
        // threshold must catch it so low-budget receivers still react.
        let mut second = BandwidthEstimate::new();
        second.estimated_downlink_kbps = 130;
        assert!(
            room.update_bandwidth_estimate(1, &second),
            "30% move on 100 kbps must invalidate via relative threshold"
        );
    }

    /// vc-17e: prev=0 with new>0 (a client transitioning out of a "no
    /// estimate" sentinel into a real value) MUST invalidate. The
    /// relative-threshold check naturally handles this: any positive
    /// `abs_diff` produces LHS>0 strictly greater than RHS=0.
    #[test]
    fn update_bandwidth_estimate_invalidates_on_transition_from_zero() {
        let mut room = RoomState::new("r".into());
        room.insert_member(1, 0);

        let mut zero = BandwidthEstimate::new();
        zero.estimated_downlink_kbps = 0;
        assert!(room.update_bandwidth_estimate(1, &zero));

        // A small positive value (10 kbps) is below the 50 kbps absolute
        // threshold AND the relative check would divide-by-zero if naïve.
        // Both edges must converge on "invalidate".
        let mut tiny = BandwidthEstimate::new();
        tiny.estimated_downlink_kbps = 10;
        assert!(
            room.update_bandwidth_estimate(1, &tiny),
            "0 → 10 kbps transition must invalidate (no divide-by-zero short-circuit)"
        );

        // And 0 → 0 must NOT invalidate.
        room.insert_member(2, 0);
        let mut z2 = BandwidthEstimate::new();
        z2.estimated_downlink_kbps = 0;
        assert!(room.update_bandwidth_estimate(2, &z2));
        let mut z3 = BandwidthEstimate::new();
        z3.estimated_downlink_kbps = 0;
        assert!(
            !room.update_bandwidth_estimate(2, &z3),
            "0 → 0 (no change) must not invalidate"
        );
    }

    /// vc-17e: exactly-at-threshold values do NOT invalidate (strict >).
    /// Locks in the boundary so future tuning is intentional.
    #[test]
    fn update_bandwidth_estimate_at_threshold_does_not_invalidate() {
        let mut room = RoomState::new("r".into());
        room.insert_member(1, 0);

        let mut first = BandwidthEstimate::new();
        first.estimated_downlink_kbps = 1000;
        assert!(room.update_bandwidth_estimate(1, &first));

        // Exactly +50 kbps and exactly 5% — neither strictly exceeds.
        let mut second = BandwidthEstimate::new();
        second.estimated_downlink_kbps = 1050;
        assert!(
            !room.update_bandwidth_estimate(1, &second),
            "exactly-50 kbps / 5% diff must not invalidate"
        );
    }

    // ---------------- vc-54j2: remote-publisher registry ----------------

    #[test]
    fn note_remote_publisher_registers_and_snapshots() {
        let mut room = RoomState::new("r".into());
        let now = Instant::now();
        room.note_remote_publisher(2, true, now); // video publisher
        room.note_remote_publisher(3, false, now); // audio-only publisher

        let (snap, _) = room.remote_publishers_snapshot_with_generation();
        assert!(snap.audio.contains(&2), "video publisher is also audible");
        assert!(snap.audio.contains(&3));
        assert!(snap.video.contains(&2), "video publisher counted for video");
        assert!(
            !snap.video.contains(&3),
            "audio-only publisher must NOT consume a video slot"
        );
        assert_eq!(room.remote_publisher_count(), 2);
    }

    #[test]
    fn note_remote_publisher_skips_local_member() {
        let mut room = RoomState::new("r".into());
        room.insert_member(2, 0);
        room.note_remote_publisher(2, true, Instant::now());
        assert_eq!(
            room.remote_publisher_count(),
            0,
            "a local member must never be tracked as a remote publisher"
        );
    }

    #[test]
    fn audio_only_publisher_upgrades_to_video() {
        let mut room = RoomState::new("r".into());
        let now = Instant::now();
        room.note_remote_publisher(2, false, now);
        let (_, gen_after_audio) = room.remote_publishers_snapshot_with_generation();
        // Same audio refresh — no rebuild, no generation bump.
        room.note_remote_publisher(2, false, now);
        let (_, gen_after_refresh) = room.remote_publishers_snapshot_with_generation();
        assert_eq!(
            gen_after_audio, gen_after_refresh,
            "a pure liveness refresh must not bump the generation"
        );
        // First video packet upgrades — generation must bump.
        room.note_remote_publisher(2, true, now);
        let (snap, gen_after_video) = room.remote_publishers_snapshot_with_generation();
        assert_ne!(
            gen_after_refresh, gen_after_video,
            "an audio->video upgrade must bump the generation (cache invalidation)"
        );
        assert!(snap.video.contains(&2));
    }

    #[test]
    fn remote_publisher_ttl_reaped() {
        let mut room = RoomState::new("r".into());
        let t0 = Instant::now();
        room.note_remote_publisher(2, true, t0);
        // A later note for a DIFFERENT sid, past the TTL, must reap sid 2.
        let later = t0 + REMOTE_PUBLISHER_TTL + Duration::from_secs(1);
        room.note_remote_publisher(3, true, later);
        let (snap, _) = room.remote_publishers_snapshot_with_generation();
        assert!(!snap.video.contains(&2), "stale publisher 2 must be reaped");
        assert!(snap.video.contains(&3));
        assert_eq!(room.remote_publisher_count(), 1);
    }

    #[test]
    fn remote_publisher_registry_is_bounded() {
        let mut room = RoomState::new("r".into());
        let base = Instant::now();
        // Insert MAX_REMOTE_PUBLISHERS + 5 distinct sids, each newer than the
        // last (within TTL so none are reaped on age) so the oldest-eviction
        // backstop is what bounds the set.
        for i in 0..(MAX_REMOTE_PUBLISHERS as u64 + 5) {
            room.note_remote_publisher(1000 + i, true, base + Duration::from_millis(i));
        }
        assert_eq!(
            room.remote_publisher_count(),
            MAX_REMOTE_PUBLISHERS,
            "registry must never exceed MAX_REMOTE_PUBLISHERS"
        );
        let (snap, _) = room.remote_publishers_snapshot_with_generation();
        // The earliest-seen sids were evicted; the most recent survive.
        assert!(!snap.video.contains(&1000), "oldest sid must be evicted");
        assert!(snap
            .video
            .contains(&(1000 + MAX_REMOTE_PUBLISHERS as u64 + 4)));
    }

    #[test]
    fn insert_member_prunes_remote_publisher() {
        let mut room = RoomState::new("r".into());
        room.note_remote_publisher(2, true, Instant::now());
        assert_eq!(room.remote_publisher_count(), 1);
        // The same sid joins THIS pod as a local member — it must leave the
        // remote registry (membership path is authoritative).
        room.insert_member(2, 0);
        assert_eq!(
            room.remote_publisher_count(),
            0,
            "a remote publisher promoted to local member must be pruned"
        );
    }

    #[test]
    fn prune_remote_publisher_is_idempotent() {
        let mut room = RoomState::new("r".into());
        room.note_remote_publisher(2, true, Instant::now());
        room.prune_remote_publisher(2);
        assert_eq!(room.remote_publisher_count(), 0);
        // Second prune is a no-op (no panic, no generation churn surprise).
        room.prune_remote_publisher(2);
        assert_eq!(room.remote_publisher_count(), 0);
    }

    #[test]
    fn remote_publisher_write_needed_throttles_liveness_but_not_changes() {
        let mut room = RoomState::new("r".into());
        let t0 = Instant::now();

        // Brand-new sid: write needed.
        assert!(
            room.remote_publisher_write_needed(2, false, t0),
            "a brand-new remote publisher must require a write"
        );
        room.note_remote_publisher(2, false, t0); // audio-only

        // Same audio packet a moment later (within the throttle window): the
        // write must be SKIPPED — liveness granularity at TTL scale only.
        let soon = t0 + Duration::from_millis(100);
        assert!(
            !room.remote_publisher_write_needed(2, false, soon),
            "a fresh, video-consistent liveness refresh must be throttled"
        );

        // An audio→video UPGRADE within the throttle window must NOT be
        // throttled — it changes the resolution-relevant snapshot.
        assert!(
            room.remote_publisher_write_needed(2, true, soon),
            "an audio->video upgrade must require a write even within the throttle window"
        );
        room.note_remote_publisher(2, true, soon);

        // Now a video liveness refresh within the window is throttled again.
        let soon2 = soon + Duration::from_millis(100);
        assert!(
            !room.remote_publisher_write_needed(2, true, soon2),
            "a fresh video liveness refresh must be throttled"
        );

        // Past the throttle window: a liveness write IS needed (keeps the
        // sender from being TTL-reaped). The throttle is well below the TTL.
        let later = soon + REMOTE_PUBLISHER_LIVENESS_THROTTLE + Duration::from_millis(1);
        assert!(
            later.duration_since(soon) < REMOTE_PUBLISHER_TTL,
            "throttle window must stay safely below the TTL"
        );
        assert!(
            room.remote_publisher_write_needed(2, true, later),
            "a liveness refresh past the throttle window must require a write"
        );
    }

    #[test]
    fn remote_publisher_write_needed_false_for_local_member() {
        let mut room = RoomState::new("r".into());
        room.insert_member(2, 0);
        assert!(
            !room.remote_publisher_write_needed(2, true, Instant::now()),
            "a local member is owned by the membership path — never a remote write"
        );
    }
}
