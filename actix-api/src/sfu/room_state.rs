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
use std::time::Instant;

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
}

impl RoomState {
    /// Create a new empty room.
    pub fn new(room_id: String) -> Self {
        Self {
            room_id,
            members: HashMap::new(),
            members_snapshot: Arc::new(HashSet::new()),
            members_generation: 0,
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
}
