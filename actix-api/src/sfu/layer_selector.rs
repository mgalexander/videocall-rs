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

//! Per-receiver layer selection (bead vc-faj, p4-5).
//!
//! The [`LayerSelector`] runs a deterministic greedy two-pass algorithm:
//!
//!   * **Pass 1** allocates the T0 base layer for every allowed sender,
//!     up to the budget. A sender whose T0 doesn't fit is dropped
//!     entirely — partial dependency chains (e.g. T1-only) cannot decode.
//!   * **Pass 2** walks senders in priority order (active speakers first,
//!     then remaining allowed senders sorted by `SessionId`) and adds
//!     enhancement temporal layers (T1, then T2) while budget remains.
//!
//! Spatial layers are out of scope for now — VP9 L1T3 has a single
//! spatial layer (id `0`) and the selection is indexed by
//! `(sender_sid, spatial_layer_id) → max_temporal_layer_id`.
//!
//! Hysteresis (upgrade watchdog + downgrade cooldown) lands in p4-6 — see
//! [`LayerSelector::pick_with_hysteresis`].
//! Forwarder consumption of the [`LayerSelection`] lands in p4-7.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::actors::session_logic::SessionId;

use super::subscription::AllowSet;

/// Default cap on total forwarded video bitrate per receiver (kbps).
pub const DEFAULT_MAX_VIDEO_KBPS: u32 = 2000;

/// Default fraction of the measured downlink to actually fill (15% headroom).
pub const DEFAULT_BANDWIDTH_HEADROOM_PCT: f32 = 0.85;

/// VP9 L1T3 layer-bitrate table (cumulative kbps per temporal layer id).
///
/// Source: PLAN.md Phase 4 capacity model.
///   T0  = 128 kbps  (base)
///   T1  = +256 kbps  → cumulative 384
///   T2  = +512 kbps  → cumulative 896
const VP9_L1T3_CUMULATIVE_KBPS: [u32; 3] = [128, 384, 896];

/// Maximum temporal-layer id we ever consider for VP9 L1T3 (T0..=T2).
const MAX_TEMPORAL_LAYER: u32 = 2;

/// Spatial layer id for VP9 L1T3. Always 0; multi-spatial lands when
/// L3T3_KEY is added later.
const SPATIAL_LAYER_ID: u32 = 0;

/// Per-receiver layer-selection decision.
///
/// Indexed by `(sender_sid, spatial_layer_id) → max temporal_layer_id`.
/// For a sender to appear in `forward`, its T0 base layer was selected;
/// the value indicates the highest temporal id (inclusive) to forward.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LayerSelection {
    /// `(sender_sid, spatial_layer_id) → max temporal_layer_id`.
    pub forward: HashMap<(SessionId, u32), u32>,
}

impl LayerSelection {
    pub fn new() -> Self {
        Self::default()
    }

    /// Total bitrate of the current selection, in kbps.
    pub fn total_kbps(&self) -> u32 {
        self.forward
            .values()
            .map(|&t| cumulative_kbps_for(t).unwrap_or(0))
            .sum()
    }
}

/// Minimum bandwidth headroom (as fraction over the active selection's
/// bitrate) we require to even consider an upgrade. 1.20 = 20% headroom.
const UPGRADE_HEADROOM_RATIO: f32 = 1.20;

/// Continuous time the upgrade-headroom predicate must hold before we
/// trigger an upgrade. PLAN.md Phase 4: 3 seconds.
const UPGRADE_STREAK_REQUIRED: Duration = Duration::from_secs(3);

/// Cooldown after a downgrade during which upgrades are blocked. PLAN.md
/// Phase 4: > 5 seconds since last downgrade.
const DOWNGRADE_COOLDOWN: Duration = Duration::from_secs(5);

/// Per-receiver hysteresis bookkeeping for [`LayerSelector`].
///
/// Owned by the selector; one entry per receiver `SessionId`. Pruned via
/// [`LayerSelector::prune_stale`] when a receiver leaves the room.
#[derive(Debug, Clone)]
struct ReceiverHysteresis {
    /// Selection actually emitted to the forwarder on the last call.
    last_selection: LayerSelection,
    /// Start of the continuous interval during which the receiver has
    /// had `>= 20%` headroom on `last_selection`. `None` whenever the
    /// latest observation fell below threshold (streak broken).
    headroom_streak_start: Option<Instant>,
    /// When we last emitted a strictly smaller selection for this
    /// receiver. Used to enforce the post-downgrade cooldown.
    last_downgrade_at: Option<Instant>,
}

/// Direction of change between two [`LayerSelection`]s.
///
/// Conservative on mixed motion: if any sender lost ground (dropped out
/// entirely or saw its temporal layer reduced) we classify the whole
/// transition as a downgrade, even if another sender simultaneously
/// gained ground. This biases toward stability — we never starve a
/// receiver of budget for a newly-promoted sender while another is still
/// throttled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SelectionDelta {
    Identical,
    Upgrade,
    Downgrade,
}

/// Configurable greedy two-pass layer selector with per-receiver
/// upgrade/downgrade hysteresis.
///
/// `pick_layers` itself remains stateless. `pick_with_hysteresis` layers
/// on per-receiver memory: an upgrade watchdog (≥20% headroom held for
/// ≥3 s, plus a 5 s cooldown after any downgrade) and an immediate
/// downgrade path. State is keyed by receiver `SessionId` and must be
/// reaped via [`Self::prune_stale`] on `LeaveRoom`.
#[derive(Debug, Clone)]
pub struct LayerSelector {
    /// Hard cap on per-receiver forwarded video bitrate (kbps).
    pub max_video_kbps: u32,
    /// Fraction of the measured downlink we actually fill (0.0..=1.0).
    pub bandwidth_headroom_pct: f32,
    /// Per-receiver hysteresis state. Populated lazily on first
    /// `pick_with_hysteresis` call; pruned via `prune_stale`.
    receiver_state: HashMap<SessionId, ReceiverHysteresis>,
}

impl LayerSelector {
    pub fn new() -> Self {
        Self {
            max_video_kbps: DEFAULT_MAX_VIDEO_KBPS,
            bandwidth_headroom_pct: DEFAULT_BANDWIDTH_HEADROOM_PCT,
            receiver_state: HashMap::new(),
        }
    }

    /// Greedy two-pass layer selection for a single receiver.
    ///
    /// # Algorithm
    ///
    /// 1. Compute the effective budget:
    ///    `min(bandwidth_kbps * headroom_pct, max_video_kbps)`.
    /// 2. **Pass 1**: walk allowed senders in priority order. For each
    ///    sender, add its T0 base layer (128 kbps) iff it fits in the
    ///    remaining budget. Senders whose T0 doesn't fit are skipped
    ///    entirely (no partial dependency chain).
    /// 3. **Pass 2**: walk the senders that were admitted in Pass 1 in
    ///    the same priority order, upgrading temporal layers in
    ///    breadth-first rounds (everyone T1, then everyone T2) while
    ///    budget remains. This biases toward fairness over depth — the
    ///    top-priority sender does NOT consume the entire budget before
    ///    others get T1.
    ///
    /// # Priority order
    ///
    /// Speakers in `speaker_set` order come first (top scorer first,
    /// deduplicated), then remaining allowed senders sorted ascending
    /// by `SessionId` (deterministic). The receiver itself is never
    /// included — `AllowSet` already excludes self, but we filter again
    /// defensively.
    ///
    /// The `AllowSet` does not preserve the pinned-vs-slot-vs-other
    /// distinction (it collapses to `HashMap<SessionId, LayerPref>`),
    /// so we cannot replicate the full four-tier ordering described in
    /// the PLAN. Speakers-first plus sorted-by-id tail is the best
    /// deterministic approximation; surfaced in p4-6/p4-7 follow-ups if
    /// finer tiers matter.
    ///
    /// # Determinism
    ///
    /// Same inputs always produce the same `LayerSelection`. No
    /// floating-point comparisons, no `HashMap` iteration order
    /// dependencies — speaker order is taken from the input slice and
    /// the tail is sorted.
    pub fn pick_layers(
        &self,
        receiver_sid: SessionId,
        allow_set: &AllowSet,
        speaker_set: &[SessionId],
        bandwidth_kbps: u32,
    ) -> LayerSelection {
        let mut selection = LayerSelection::new();

        let budget = self.effective_budget_kbps(bandwidth_kbps);
        if budget == 0 || allow_set.video.is_empty() {
            return selection;
        }

        let ordered = ordered_senders(receiver_sid, allow_set, speaker_set);
        if ordered.is_empty() {
            return selection;
        }

        // ---- Pass 1: allocate T0 base layer for as many senders as fit. ----
        let t0_cost = VP9_L1T3_CUMULATIVE_KBPS[0];
        let mut spent: u32 = 0;
        let mut admitted: Vec<SessionId> = Vec::with_capacity(ordered.len());
        for sid in &ordered {
            if budget.saturating_sub(spent) < t0_cost {
                continue;
            }
            selection.forward.insert((*sid, SPATIAL_LAYER_ID), 0);
            admitted.push(*sid);
            spent += t0_cost;
        }

        if admitted.is_empty() {
            return selection;
        }

        // ---- Pass 2: round-robin upgrades T1, then T2 (breadth-first). ----
        for next_temporal in 1..=MAX_TEMPORAL_LAYER {
            let prev_cum = VP9_L1T3_CUMULATIVE_KBPS[(next_temporal - 1) as usize];
            let new_cum = VP9_L1T3_CUMULATIVE_KBPS[next_temporal as usize];
            let delta = new_cum - prev_cum;

            for sid in &admitted {
                let key = (*sid, SPATIAL_LAYER_ID);
                // Only consider senders currently sitting at next_temporal-1.
                if selection.forward.get(&key).copied() != Some(next_temporal - 1) {
                    continue;
                }
                if budget.saturating_sub(spent) < delta {
                    // Budget exhausted for this layer; keep walking — the
                    // next sender at this temporal layer might be cheaper
                    // (it isn't, today, but the check is uniform and
                    // future-proofs for asymmetric ladders).
                    continue;
                }
                selection.forward.insert(key, next_temporal);
                spent += delta;
            }
        }

        debug_assert!(
            spent <= budget,
            "LayerSelector overspent budget: spent={spent} budget={budget}"
        );

        selection
    }

    /// Greedy two-pass selection wrapped in per-receiver hysteresis.
    ///
    /// Computes a fresh candidate via [`Self::pick_layers`], compares it
    /// to the receiver's last-emitted selection, and applies the
    /// upgrade/downgrade rules from PLAN.md Phase 4:
    ///
    /// * **Downgrade** (selection shrinks — any sender removed or any
    ///   temporal layer reduced, including mixed motion): emit
    ///   immediately, record `last_downgrade_at`, reset the headroom
    ///   streak.
    /// * **Upgrade** (selection grows — new sender admitted or any
    ///   temporal layer raised, with no regression): emit only if all
    ///   three gates pass — (1) effective budget for `bandwidth_kbps`
    ///   is `>= last_selection.total_kbps() * 1.20` (≥ 20% headroom
    ///   over the *current* selection), (2) that headroom condition
    ///   has held continuously for `>= 3 s`, and (3) it has been
    ///   `> 5 s` since the last downgrade for this receiver (or there
    ///   has never been one). Failing any gate, the receiver's
    ///   `last_selection` is returned unchanged.
    /// * **Identical**: return `last_selection`; update the streak
    ///   tracker against the current bandwidth observation.
    ///
    /// On the very first call for a receiver, the candidate is emitted
    /// directly and the streak is seeded if the receiver already has
    /// enough headroom over the fresh selection.
    ///
    /// `now` is injected for deterministic tests; production code passes
    /// `Instant::now()`.
    ///
    /// # Concurrency
    ///
    /// This method takes `&mut self` because it mutates the per-receiver
    /// hysteresis map. Within a single room the surrounding actor model
    /// already serializes calls, so an owned `LayerSelector` is fine.
    /// Sharing one instance across rooms (e.g. via `Arc<LayerSelector>`)
    /// is **no longer safe** once hysteresis is in play — cross-room
    /// sharing requires external synchronization such as
    /// `Arc<Mutex<LayerSelector>>`.
    pub fn pick_with_hysteresis(
        &mut self,
        receiver_sid: SessionId,
        allow_set: &AllowSet,
        speaker_set: &[SessionId],
        bandwidth_kbps: u32,
        now: Instant,
    ) -> LayerSelection {
        let candidate = self.pick_layers(receiver_sid, allow_set, speaker_set, bandwidth_kbps);
        let budget = self.effective_budget_kbps(bandwidth_kbps);

        // First-ever decision for this receiver: emit and seed state.
        let Some(state) = self.receiver_state.get(&receiver_sid).cloned() else {
            let headroom_ok = has_upgrade_headroom(budget, &candidate);
            self.receiver_state.insert(
                receiver_sid,
                ReceiverHysteresis {
                    last_selection: candidate.clone(),
                    headroom_streak_start: if headroom_ok { Some(now) } else { None },
                    last_downgrade_at: None,
                },
            );
            return candidate;
        };

        match compare_selections(&state.last_selection, &candidate) {
            SelectionDelta::Identical => {
                // Selection unchanged; update streak against current bandwidth.
                let headroom_ok = has_upgrade_headroom(budget, &state.last_selection);
                let new_streak = match (headroom_ok, state.headroom_streak_start) {
                    (true, Some(start)) => Some(start),
                    (true, None) => Some(now),
                    (false, _) => None,
                };
                let entry = self
                    .receiver_state
                    .get_mut(&receiver_sid)
                    .expect("state existed above");
                entry.headroom_streak_start = new_streak;
                entry.last_selection.clone()
            }
            SelectionDelta::Downgrade => {
                // Immediate. No gates. Reset streak; record downgrade time.
                let entry = self
                    .receiver_state
                    .get_mut(&receiver_sid)
                    .expect("state existed above");
                entry.last_selection = candidate.clone();
                entry.headroom_streak_start = None;
                entry.last_downgrade_at = Some(now);
                candidate
            }
            SelectionDelta::Upgrade => {
                // Three gates. All must pass.
                let headroom_ok = has_upgrade_headroom(budget, &state.last_selection);
                let streak_satisfied = match state.headroom_streak_start {
                    Some(start) if headroom_ok => {
                        now.saturating_duration_since(start) >= UPGRADE_STREAK_REQUIRED
                    }
                    _ => false,
                };
                let cooldown_satisfied = match state.last_downgrade_at {
                    None => true,
                    Some(t) => now.saturating_duration_since(t) > DOWNGRADE_COOLDOWN,
                };

                // Always refresh the streak tracker against the prior
                // selection; if we don't upgrade now, we may upgrade on
                // a later tick.
                let new_streak = match (headroom_ok, state.headroom_streak_start) {
                    (true, Some(start)) => Some(start),
                    (true, None) => Some(now),
                    (false, _) => None,
                };

                if headroom_ok && streak_satisfied && cooldown_satisfied {
                    let entry = self
                        .receiver_state
                        .get_mut(&receiver_sid)
                        .expect("state existed above");
                    entry.last_selection = candidate.clone();
                    // Reset streak: the new (larger) selection's headroom
                    // must build up from scratch before a further upgrade.
                    entry.headroom_streak_start = None;
                    candidate
                } else {
                    let entry = self
                        .receiver_state
                        .get_mut(&receiver_sid)
                        .expect("state existed above");
                    entry.headroom_streak_start = new_streak;
                    entry.last_selection.clone()
                }
            }
        }
    }

    /// Drop any hysteresis state for `receiver_sid`.
    ///
    /// Intended to be called from the room's `LeaveRoom` handling so a
    /// rejoining receiver doesn't inherit the previous session's
    /// upgrade-streak / cooldown timers. Safe to call for receivers that
    /// have no state (no-op).
    pub fn prune_stale(&mut self, receiver_sid: SessionId) {
        self.receiver_state.remove(&receiver_sid);
    }

    /// Effective bitrate budget for this pick:
    /// `min(bandwidth_kbps * headroom_pct, max_video_kbps)`.
    fn effective_budget_kbps(&self, bandwidth_kbps: u32) -> u32 {
        let headroom = self.bandwidth_headroom_pct.clamp(0.0, 1.0);
        let after_headroom = (bandwidth_kbps as f32 * headroom).floor() as u32;
        after_headroom.min(self.max_video_kbps)
    }
}

impl Default for LayerSelector {
    fn default() -> Self {
        Self::new()
    }
}

/// Cumulative kbps to forward up to and including temporal layer `t` for
/// the VP9 L1T3 ladder. Returns `None` for unsupported temporal ids.
fn cumulative_kbps_for(t: u32) -> Option<u32> {
    VP9_L1T3_CUMULATIVE_KBPS.get(t as usize).copied()
}

/// Build the priority-ordered list of candidate senders for a receiver.
///
/// Speakers (in `speaker_set` order, deduped) precede remaining allowed
/// senders sorted ascending by `SessionId`. The receiver itself is filtered.
fn ordered_senders(
    receiver_sid: SessionId,
    allow_set: &AllowSet,
    speaker_set: &[SessionId],
) -> Vec<SessionId> {
    let mut ordered: Vec<SessionId> = Vec::with_capacity(allow_set.video.len());
    let mut seen: std::collections::HashSet<SessionId> =
        std::collections::HashSet::with_capacity(allow_set.video.len());

    // 1. Speakers, in input order, only if they're actually allowed.
    for &sid in speaker_set {
        if sid == receiver_sid {
            continue;
        }
        if !allow_set.video.contains_key(&sid) {
            continue;
        }
        if seen.insert(sid) {
            ordered.push(sid);
        }
    }

    // 2. Remaining allowed senders, sorted ascending by SessionId (deterministic).
    let mut tail: Vec<SessionId> = allow_set
        .video
        .keys()
        .copied()
        .filter(|&sid| sid != receiver_sid && !seen.contains(&sid))
        .collect();
    tail.sort_unstable();
    ordered.extend(tail);

    ordered
}

/// Classify the transition from `prev` to `next`.
///
/// Treats *any* loss (a sender that was forwarded but no longer is, or a
/// sender whose temporal layer dropped) as a [`SelectionDelta::Downgrade`]
/// — even when offset by another sender gaining ground. This conservative
/// rule keeps the hysteresis path free of pathological "mixed motion"
/// states where a gain and a loss cancel out into a no-op classification.
///
/// `Upgrade` requires at least one strict improvement (new sender or
/// raised temporal layer) and zero regressions. `Identical` means every
/// `(sender, spatial)` key matches with identical max temporal id.
fn compare_selections(prev: &LayerSelection, next: &LayerSelection) -> SelectionDelta {
    let mut any_loss = false;
    let mut any_gain = false;

    // Walk previous entries: are they gone, or did they shrink?
    for (key, &prev_t) in &prev.forward {
        match next.forward.get(key) {
            None => any_loss = true,
            Some(&next_t) if next_t < prev_t => any_loss = true,
            Some(&next_t) if next_t > prev_t => any_gain = true,
            _ => {}
        }
    }

    // Walk current entries: any brand-new senders?
    for key in next.forward.keys() {
        if !prev.forward.contains_key(key) {
            any_gain = true;
        }
    }

    if any_loss {
        SelectionDelta::Downgrade
    } else if any_gain {
        SelectionDelta::Upgrade
    } else {
        SelectionDelta::Identical
    }
}

/// Does the receiver have ≥ 20% headroom over `selection`?
///
/// Uses the same `effective_budget_kbps` lens as `pick_layers` (the caller
/// is expected to pass that pre-computed value as `budget`) so the
/// configured `bandwidth_headroom_pct` stays consistent with selection.
/// The 20% margin is applied to the *selection's* bitrate, not the raw
/// downlink. An empty selection always reports "enough headroom" (there
/// is nothing to protect against).
fn has_upgrade_headroom(budget_kbps: u32, selection: &LayerSelection) -> bool {
    let selection_kbps = selection.total_kbps();
    if selection_kbps == 0 {
        return true;
    }
    // budget >= selection * 1.20, evaluated in f32 to avoid integer
    // truncation surprises around the threshold.
    (budget_kbps as f32) >= (selection_kbps as f32) * UPGRADE_HEADROOM_RATIO
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sfu::subscription::LayerPref;

    fn allow_set_with(video: &[SessionId]) -> AllowSet {
        let mut a = AllowSet::new();
        for &sid in video {
            a.video.insert(sid, LayerPref::default());
            a.audio.insert(sid);
        }
        a
    }

    /// Acceptance #1: empty allow_set → empty selection.
    #[test]
    fn empty_allow_set_yields_empty_selection() {
        let sel = LayerSelector::new();
        let allow = AllowSet::new();
        let out = sel.pick_layers(1, &allow, &[], 10_000);
        assert!(out.forward.is_empty());
        assert_eq!(out.total_kbps(), 0);
    }

    /// Acceptance #2: single sender, generous budget → T0+T1+T2.
    #[test]
    fn single_sender_generous_budget_gets_full_ladder() {
        let sel = LayerSelector::new();
        let allow = allow_set_with(&[2]);
        let out = sel.pick_layers(1, &allow, &[], 2000);
        assert_eq!(out.forward.get(&(2, 0)), Some(&2), "should select T2");
        assert_eq!(out.forward.len(), 1, "exactly one sender entry");
        // Cumulative 896 kbps for L1T3 at T2.
        assert_eq!(out.total_kbps(), 896);
    }

    /// Acceptance #3: single sender, tight budget (200 kbps × 0.85 = 170)
    /// — only T0 (128) fits.
    #[test]
    fn single_sender_tight_budget_t0_only() {
        let sel = LayerSelector::new();
        let allow = allow_set_with(&[2]);
        let out = sel.pick_layers(1, &allow, &[], 200);
        assert_eq!(
            out.forward.get(&(2, 0)),
            Some(&0),
            "only T0 must fit in 170 kbps budget"
        );
        assert_eq!(out.total_kbps(), 128);
    }

    /// Acceptance #4: multiple senders — base fits for all, enhancements
    /// only for top-priority. With 4 senders and a budget where T0 fits
    /// for all (4 × 128 = 512) but the full T2 ladder for everyone
    /// (4 × 896 = 3584) does not, top-priority senders should be
    /// upgraded first.
    ///
    /// Budget pick: 700 kbps × 0.85 = 595. Spend after Pass 1: 512.
    /// Remaining: 83. T1 delta is 256 → no T1 fits at all. We expect
    /// every sender to be at T0 and the budget invariant to hold.
    ///
    /// Bumping the budget to 1100 (× 0.85 = 935): Pass 1 spends 512,
    /// leaving 423. Round-robin T1 upgrades cost 256 each → only the
    /// FIRST sender in priority order (the speaker) gets T1; second
    /// would need 512 total but only 423 remain, so it stays at T0.
    /// Actually 423 ≥ 256 so first speaker → T1 (spent=768), then
    /// 935 - 768 = 167 < 256 for the second. Final: speaker=T1,
    /// others=T0.
    #[test]
    fn multiple_senders_top_priority_gets_enhancement() {
        let sel = LayerSelector::new();
        let allow = allow_set_with(&[10, 11, 12, 13]);
        // Speaker order: 13 first (top-scorer), then 12.
        let out = sel.pick_layers(1, &allow, &[13, 12], 1100);

        // All four senders admitted at T0 minimum.
        for sid in [10, 11, 12, 13] {
            assert!(
                out.forward.contains_key(&(sid, 0)),
                "sender {sid} must have at least T0"
            );
        }

        // Top speaker (13) gets the only T1 upgrade.
        assert_eq!(out.forward.get(&(13, 0)), Some(&1), "top speaker upgraded");
        // Second speaker stays at T0 — budget exhausted.
        assert_eq!(out.forward.get(&(12, 0)), Some(&0));
        // Tail senders stay at T0.
        assert_eq!(out.forward.get(&(10, 0)), Some(&0));
        assert_eq!(out.forward.get(&(11, 0)), Some(&0));

        // Invariant: total ≤ effective budget (935).
        assert!(out.total_kbps() <= 935);
    }

    /// Acceptance #5: Pass-1 starvation — budget too small for ANY T0.
    /// 50 kbps × 0.85 = 42 < 128 base. Selection MUST be empty.
    /// This is the critical "no partial T1-only mistake" check.
    #[test]
    fn pass_one_starvation_yields_empty_selection() {
        let sel = LayerSelector::new();
        let allow = allow_set_with(&[2, 3, 4]);
        let out = sel.pick_layers(1, &allow, &[2, 3], 50);
        assert!(
            out.forward.is_empty(),
            "no partial dependency chain when T0 doesn't fit; got {:?}",
            out.forward
        );
    }

    /// Acceptance #6: idempotent — same inputs → same selection, 100x.
    #[test]
    fn pick_layers_is_idempotent() {
        let sel = LayerSelector::new();
        let allow = allow_set_with(&[10, 11, 12, 13, 14]);
        let speakers = [14, 12];
        let bandwidth = 1500;

        let baseline = sel.pick_layers(1, &allow, &speakers, bandwidth);
        for _ in 0..100 {
            let next = sel.pick_layers(1, &allow, &speakers, bandwidth);
            assert_eq!(
                next, baseline,
                "pick_layers must be deterministic across invocations"
            );
        }
    }

    /// Receiver itself never appears in its own selection, even if
    /// erroneously present in allow_set or speaker_set.
    #[test]
    fn receiver_excluded_defensively() {
        let sel = LayerSelector::new();
        let mut allow = allow_set_with(&[2, 3]);
        // Defensive: simulate a buggy upstream that left self in allow_set.
        allow.video.insert(1, LayerPref::default());
        let out = sel.pick_layers(1, &allow, &[1, 2], 2000);
        assert!(!out.forward.contains_key(&(1, 0)), "self must be excluded");
        assert!(out.forward.contains_key(&(2, 0)));
        assert!(out.forward.contains_key(&(3, 0)));
    }

    /// Speakers in input order precede sorted tail.
    #[test]
    fn speakers_precede_sorted_tail() {
        // Build a budget that fits exactly TWO T1 upgrades:
        // Pass 1: 4 × 128 = 512. T1 delta = 256. Two upgrades = 1024 total
        // spent. We need effective budget in [1024, 1280) so the third
        // T1 doesn't fit. 1200 × 0.85 = 1020 — that's < 1024 so only ONE
        // T1 fits. Use 1300 × 0.85 = 1105 → exactly one T1 upgrade past
        // first speaker; let's pick a budget that gives exactly two:
        // 1500 × 0.85 = 1275 → two T1s fit (1024) but third would need
        // 1280 > 1275.
        let sel = LayerSelector::new();
        let allow = allow_set_with(&[10, 20, 30, 40]);
        // Speakers: 40 (top), then 30. Tail (sorted): 10, 20.
        let out = sel.pick_layers(1, &allow, &[40, 30], 1500);

        // Both speakers get T1.
        assert_eq!(out.forward.get(&(40, 0)), Some(&1));
        assert_eq!(out.forward.get(&(30, 0)), Some(&1));
        // Tail stays at T0.
        assert_eq!(out.forward.get(&(10, 0)), Some(&0));
        assert_eq!(out.forward.get(&(20, 0)), Some(&0));
    }

    /// max_video_kbps cap applies even if bandwidth is huge.
    #[test]
    fn max_video_kbps_cap_applied() {
        let mut sel = LayerSelector::new();
        sel.max_video_kbps = 200; // very low cap
        let allow = allow_set_with(&[2, 3]);
        // 100 Mbps bandwidth — cap should bite.
        let out = sel.pick_layers(1, &allow, &[2, 3], 100_000);
        // Effective budget = min(85000, 200) = 200. Only 1 T0 fits (128).
        assert_eq!(out.forward.len(), 1);
        assert!(out.total_kbps() <= 200);
    }

    /// Zero bandwidth → empty selection.
    #[test]
    fn zero_bandwidth_yields_empty() {
        let sel = LayerSelector::new();
        let allow = allow_set_with(&[2, 3]);
        let out = sel.pick_layers(1, &allow, &[], 0);
        assert!(out.forward.is_empty());
    }

    /// Two senders, budget fits exactly 2 × T0 — neither gets T1.
    #[test]
    fn exactly_two_base_layers_fit() {
        let sel = LayerSelector::new();
        let allow = allow_set_with(&[2, 3]);
        // 302 × 0.85 = 256.7 → 256. Two T0 = 256. No T1 (needs +256).
        let out = sel.pick_layers(1, &allow, &[2, 3], 302);
        assert_eq!(out.forward.get(&(2, 0)), Some(&0));
        assert_eq!(out.forward.get(&(3, 0)), Some(&0));
        assert_eq!(out.total_kbps(), 256);
    }

    /// Pass-1 partial admission: budget fits 1.5 base layers — only one
    /// sender is admitted (no partial second). Speaker takes priority.
    #[test]
    fn pass_one_partial_admission_speaker_wins() {
        let sel = LayerSelector::new();
        let allow = allow_set_with(&[10, 20, 30]);
        // 200 × 0.85 = 170 → fits exactly one T0 (128), not two (256).
        let out = sel.pick_layers(1, &allow, &[30], 200);
        assert_eq!(out.forward.len(), 1);
        assert_eq!(out.forward.get(&(30, 0)), Some(&0), "speaker admitted");
        assert!(!out.forward.contains_key(&(10, 0)));
        assert!(!out.forward.contains_key(&(20, 0)));
    }

    // ----------------------------------------------------------------
    // Hysteresis tests (p4-6).
    // ----------------------------------------------------------------

    /// Hysteresis #1: steady-state — same inputs over 10 calls 1 s apart
    /// return the same selection every time and never re-emit.
    #[test]
    fn hysteresis_steady_state_no_change() {
        let mut sel = LayerSelector::new();
        let allow = allow_set_with(&[2]);
        let t0 = Instant::now();

        let baseline = sel.pick_with_hysteresis(1, &allow, &[], 2000, t0);
        for i in 1..=10 {
            let now = t0 + Duration::from_secs(i);
            let out = sel.pick_with_hysteresis(1, &allow, &[], 2000, now);
            assert_eq!(
                out, baseline,
                "steady-state must be stable across call #{i}"
            );
        }
    }

    /// Hysteresis #2: a brief headroom spike (< 3 s) must NOT trigger an
    /// upgrade. Receiver starts in a T0-only state at 200 kbps, then sees
    /// 2 s at 2000 kbps — not long enough to satisfy the streak gate.
    #[test]
    fn hysteresis_brief_headroom_spike_no_upgrade() {
        let mut sel = LayerSelector::new();
        let allow = allow_set_with(&[2]);
        let t0 = Instant::now();

        // Seed: 200 kbps → T0 only (cumulative 128 kbps).
        let initial = sel.pick_with_hysteresis(1, &allow, &[], 200, t0);
        assert_eq!(initial.forward.get(&(2, 0)), Some(&0));

        // 2 s of high bandwidth; must still report the seeded selection.
        let mid = sel.pick_with_hysteresis(1, &allow, &[], 2000, t0 + Duration::from_secs(2));
        assert_eq!(
            mid, initial,
            "<3s streak must not upgrade even with huge headroom"
        );

        // Bandwidth drops back; streak should be re-broken automatically.
        let after = sel.pick_with_hysteresis(1, &allow, &[], 200, t0 + Duration::from_millis(2500));
        assert_eq!(after, initial);
    }

    /// Hysteresis #3: sustained headroom for ≥ 3 s with no recent
    /// downgrade triggers the upgrade. Verifies both that pre-mark calls
    /// don't upgrade and that the at-mark call does.
    #[test]
    fn hysteresis_sustained_headroom_triggers_upgrade() {
        let mut sel = LayerSelector::new();
        let allow = allow_set_with(&[2]);
        let t0 = Instant::now();

        // Seed at T0-only with a budget tight enough that the candidate
        // *is* T0 but loose enough to satisfy 20% headroom over T0
        // (so the streak can start at t0). 200 × 0.85 = 170 ≥ 128 × 1.20
        // = 153.6, so the streak begins on this very first call.
        let initial = sel.pick_with_hysteresis(1, &allow, &[], 200, t0);
        assert_eq!(initial.forward.get(&(2, 0)), Some(&0));

        // Open up bandwidth — candidate is now T2, an upgrade, but the
        // streak is < 3 s old.
        let at_t1s = sel.pick_with_hysteresis(1, &allow, &[], 2000, t0 + Duration::from_secs(1));
        assert_eq!(at_t1s, initial, "1s in — streak too short");

        let at_t2s = sel.pick_with_hysteresis(1, &allow, &[], 2000, t0 + Duration::from_secs(2));
        assert_eq!(at_t2s, initial, "2s in — streak too short");

        // At t0 + 3 s the streak is exactly 3 s ≥ required threshold.
        // The cooldown gate passes (never downgraded), headroom holds,
        // so the upgrade fires on this call.
        let at_t3s = sel.pick_with_hysteresis(1, &allow, &[], 2000, t0 + Duration::from_secs(3));
        assert_ne!(at_t3s, initial, "streak >= 3s should have upgraded");
        // 2000 × 0.85 = 1700 budget → full L1T3 ladder for one sender (T2).
        assert_eq!(
            at_t3s.forward.get(&(2, 0)),
            Some(&2),
            "should upgrade to T2"
        );
    }

    /// Hysteresis #4: cooldown gate — after a downgrade, no upgrade can
    /// happen for > 5 s even if headroom is wide open the whole time.
    /// Polls every 500 ms across the window for broad coverage.
    #[test]
    fn hysteresis_cooldown_blocks_upgrade() {
        let mut sel = LayerSelector::new();
        let allow = allow_set_with(&[2]);
        let t0 = Instant::now();

        // Seed at T2 (full ladder) with generous budget.
        let high = sel.pick_with_hysteresis(1, &allow, &[], 2000, t0);
        assert_eq!(high.forward.get(&(2, 0)), Some(&2));

        // Bandwidth collapses → immediate downgrade to T0.
        let downgrade_at = t0 + Duration::from_millis(100);
        let downgraded = sel.pick_with_hysteresis(1, &allow, &[], 200, downgrade_at);
        assert_eq!(downgraded.forward.get(&(2, 0)), Some(&0));

        // Bandwidth springs back high; poll every 500 ms across the
        // cooldown window. Every call within the window must stay
        // blocked. The loop intentionally stops *before* the boundary
        // tick — boundary behavior is checked explicitly below.
        let mut last = downgraded.clone();
        let mut tick = 1u64;
        loop {
            let elapsed_since_downgrade = Duration::from_millis(500 * tick);
            if elapsed_since_downgrade >= DOWNGRADE_COOLDOWN {
                break;
            }
            let now = downgrade_at + elapsed_since_downgrade;
            last = sel.pick_with_hysteresis(1, &allow, &[], 2000, now);
            assert_eq!(
                last, downgraded,
                "upgrade must be blocked during cooldown at {elapsed_since_downgrade:?}"
            );
            tick += 1;
        }
        assert_eq!(last, downgraded, "loop must end with no upgrade emitted");
    }

    /// Hysteresis #4b: cooldown boundary — the cooldown gate is
    /// `now - last_downgrade_at > DOWNGRADE_COOLDOWN`, so:
    ///
    /// * At exactly `downgrade_at + DOWNGRADE_COOLDOWN` the gate must
    ///   still BLOCK (equality fails strict `>`).
    /// * At `downgrade_at + DOWNGRADE_COOLDOWN + 1 ms` the gate must
    ///   ALLOW (streak gate also satisfied — see setup below).
    ///
    /// This catches a regression that flipped `>` to `>=`.
    #[test]
    fn hysteresis_cooldown_boundary_strict() {
        let mut sel = LayerSelector::new();
        let allow = allow_set_with(&[2]);
        let t0 = Instant::now();

        // Seed at T2 with generous budget.
        let high = sel.pick_with_hysteresis(1, &allow, &[], 2000, t0);
        assert_eq!(high.forward.get(&(2, 0)), Some(&2));

        // Downgrade at t0 + 100 ms.
        let downgrade_at = t0 + Duration::from_millis(100);
        let downgraded = sel.pick_with_hysteresis(1, &allow, &[], 200, downgrade_at);
        assert_eq!(downgraded.forward.get(&(2, 0)), Some(&0));

        // Take a call early in the cooldown window so the streak begins
        // building well before the boundary. At this point the streak is
        // None (just reset by the downgrade); this call sets it to
        // Some(downgrade_at + 200 ms). By the boundary, the streak will
        // be ~4.9 s long → streak gate satisfied.
        let streak_seed = downgrade_at + Duration::from_millis(200);
        let still_blocked = sel.pick_with_hysteresis(1, &allow, &[], 2000, streak_seed);
        assert_eq!(still_blocked, downgraded, "still in cooldown shortly after");

        // Exactly at the boundary: cooldown == 5 s → strict `>` is FALSE → still blocked.
        let at_boundary = downgrade_at + DOWNGRADE_COOLDOWN;
        let boundary_call = sel.pick_with_hysteresis(1, &allow, &[], 2000, at_boundary);
        assert_eq!(
            boundary_call, downgraded,
            "at exactly downgrade + 5s the cooldown must still BLOCK"
        );

        // One millisecond past the boundary: gate flips to ALLOW.
        let past_boundary = at_boundary + Duration::from_millis(1);
        let after = sel.pick_with_hysteresis(1, &allow, &[], 2000, past_boundary);
        assert_eq!(
            after.forward.get(&(2, 0)),
            Some(&2),
            "1 ms past downgrade + 5s the upgrade must fire (T2)"
        );
    }

    /// Hysteresis #4c: state-machine — upgrade fires *immediately* once
    /// the cooldown expires, with no additional 3 s wait, provided the
    /// streak was already building during the cooldown window.
    ///
    /// This validates that the streak tracker keeps running while
    /// upgrades are gated off by cooldown — it does not require a
    /// post-cooldown rebuild from scratch.
    #[test]
    fn hysteresis_upgrade_fires_immediately_after_cooldown() {
        let mut sel = LayerSelector::new();
        let allow = allow_set_with(&[2]);
        let t0 = Instant::now();

        // Seed at T2.
        let _ = sel.pick_with_hysteresis(1, &allow, &[], 2000, t0);

        // Downgrade at t0 + 100 ms.
        let downgrade_at = t0 + Duration::from_millis(100);
        let downgraded = sel.pick_with_hysteresis(1, &allow, &[], 200, downgrade_at);
        assert_eq!(downgraded.forward.get(&(2, 0)), Some(&0));

        // Establish high bandwidth early in cooldown so the streak
        // starts building. (One call is enough; the loop below keeps
        // observations consistent at high bandwidth.)
        let _ = sel.pick_with_hysteresis(
            1,
            &allow,
            &[],
            2000,
            downgrade_at + Duration::from_millis(200),
        );

        // Hold high bandwidth across the entire cooldown window (well
        // past the 3 s streak threshold). All these calls must stay
        // blocked by cooldown alone.
        for ms in [500u64, 1000, 2000, 3000, 4000, 4900] {
            let now = downgrade_at + Duration::from_millis(ms);
            let out = sel.pick_with_hysteresis(1, &allow, &[], 2000, now);
            assert_eq!(out, downgraded, "blocked by cooldown at +{ms} ms");
        }

        // First tick past the cooldown boundary → upgrade fires
        // immediately. Streak has been Some for ~4.8 s ≫ 3 s.
        let past_boundary = downgrade_at + DOWNGRADE_COOLDOWN + Duration::from_millis(1);
        let upgraded = sel.pick_with_hysteresis(1, &allow, &[], 2000, past_boundary);
        assert_eq!(
            upgraded.forward.get(&(2, 0)),
            Some(&2),
            "upgrade must fire on the first call past the cooldown boundary, no streak rebuild"
        );
    }

    /// Hysteresis #4d: two upgrades in a row require a fresh streak.
    ///
    /// After an upgrade emits, `headroom_streak_start` resets to `None`.
    /// A subsequent strictly-larger candidate (e.g. budget rising to
    /// admit one more temporal layer) must therefore wait 3 s for the
    /// new streak to satisfy — it cannot ride the previous streak.
    #[test]
    fn hysteresis_back_to_back_upgrade_requires_streak_rebuild() {
        let mut sel = LayerSelector::new();
        let allow = allow_set_with(&[2]);
        let t0 = Instant::now();

        // Seed with a budget that fits T0 only (200 × 0.85 = 170; T1
        // cumulative = 384). Streak headroom check at this seed: budget
        // 170 ≥ T0_kbps 128 × 1.20 = 153.6 → streak starts at t0.
        let seed = sel.pick_with_hysteresis(1, &allow, &[], 200, t0);
        assert_eq!(seed.forward.get(&(2, 0)), Some(&0));

        // At t0 + 3 s open the budget enough for T1 (but NOT T2). 500 ×
        // 0.85 = 425; T1 cumulative = 384 (fits), T2 cumulative = 896
        // (doesn't). Candidate = T1, streak satisfies, cooldown clear →
        // first upgrade fires.
        let first_upgrade_at = t0 + Duration::from_secs(3);
        let first_upgrade = sel.pick_with_hysteresis(1, &allow, &[], 500, first_upgrade_at);
        assert_eq!(
            first_upgrade.forward.get(&(2, 0)),
            Some(&1),
            "first upgrade should land at T1"
        );

        // Immediately raise bandwidth to push the candidate to T2. The
        // streak was just reset by the upgrade — so the very next call,
        // even with sustained high bandwidth, must NOT upgrade.
        let just_after = first_upgrade_at + Duration::from_millis(100);
        let blocked = sel.pick_with_hysteresis(1, &allow, &[], 2000, just_after);
        assert_eq!(
            blocked, first_upgrade,
            "second upgrade must be blocked immediately after the first — streak was reset"
        );

        // Poll across the new streak window. Just under 3 s after the
        // streak restarted (streak start = just_after), still blocked.
        let almost = just_after + Duration::from_millis(2900);
        let still_blocked = sel.pick_with_hysteresis(1, &allow, &[], 2000, almost);
        assert_eq!(
            still_blocked, first_upgrade,
            "second upgrade must wait for the full 3 s streak"
        );

        // At streak_start + 3 s the second upgrade fires.
        let streak_satisfied_at = just_after + UPGRADE_STREAK_REQUIRED;
        let second_upgrade = sel.pick_with_hysteresis(1, &allow, &[], 2000, streak_satisfied_at);
        assert_eq!(
            second_upgrade.forward.get(&(2, 0)),
            Some(&2),
            "second upgrade should land at T2 once the rebuilt streak satisfies"
        );
    }

    /// Hysteresis #5: immediate downgrade. A bandwidth drop hits on the
    /// next call with no streak/cooldown delay, and `last_downgrade_at`
    /// is recorded (verified by blocking a subsequent upgrade attempt).
    #[test]
    fn hysteresis_immediate_downgrade() {
        let mut sel = LayerSelector::new();
        let allow = allow_set_with(&[2]);
        let t0 = Instant::now();

        let high = sel.pick_with_hysteresis(1, &allow, &[], 2000, t0);
        assert_eq!(high.forward.get(&(2, 0)), Some(&2));

        // Sharp drop — next call must return smaller selection.
        let now = t0 + Duration::from_millis(50);
        let dropped = sel.pick_with_hysteresis(1, &allow, &[], 200, now);
        assert_eq!(dropped.forward.get(&(2, 0)), Some(&0));

        // Verify last_downgrade_at was set: an immediate upgrade attempt
        // with huge bandwidth at t+1s must still be blocked by cooldown.
        let blocked = sel.pick_with_hysteresis(1, &allow, &[], 2000, now + Duration::from_secs(1));
        assert_eq!(blocked, dropped, "cooldown should block immediate upgrade");
    }

    /// Hysteresis #6: `prune_stale` wipes receiver state. A subsequent
    /// call after pruning must behave like a first-time call (emit the
    /// fresh candidate immediately, no streak inherited).
    #[test]
    fn hysteresis_prune_clears_state() {
        let mut sel = LayerSelector::new();
        let allow = allow_set_with(&[2]);
        let t0 = Instant::now();

        // Establish state with a low-budget seed.
        let seed = sel.pick_with_hysteresis(1, &allow, &[], 200, t0);
        assert_eq!(seed.forward.get(&(2, 0)), Some(&0));

        // Without pruning, a high-bandwidth call should NOT upgrade
        // immediately (streak hasn't been satisfied).
        let pre_prune =
            sel.pick_with_hysteresis(1, &allow, &[], 2000, t0 + Duration::from_millis(100));
        assert_eq!(
            pre_prune, seed,
            "without prune, upgrade still gated by streak"
        );

        // Prune and call again with high bandwidth — should emit the
        // full ladder immediately (first-time path).
        sel.prune_stale(1);
        let post_prune =
            sel.pick_with_hysteresis(1, &allow, &[], 2000, t0 + Duration::from_millis(200));
        assert_eq!(
            post_prune.forward.get(&(2, 0)),
            Some(&2),
            "post-prune call should emit fresh candidate (T2) directly"
        );
    }

    // ----------------------------------------------------------------
    // compare_selections unit tests — the conservative mixed-motion rule.
    // ----------------------------------------------------------------

    fn sel_from(pairs: &[(SessionId, u32)]) -> LayerSelection {
        let mut s = LayerSelection::new();
        for &(sid, t) in pairs {
            s.forward.insert((sid, 0), t);
        }
        s
    }

    #[test]
    fn compare_identical() {
        let a = sel_from(&[(2, 1), (3, 0)]);
        let b = sel_from(&[(2, 1), (3, 0)]);
        assert_eq!(compare_selections(&a, &b), SelectionDelta::Identical);
    }

    #[test]
    fn compare_pure_upgrade_new_sender() {
        let a = sel_from(&[(2, 0)]);
        let b = sel_from(&[(2, 0), (3, 0)]);
        assert_eq!(compare_selections(&a, &b), SelectionDelta::Upgrade);
    }

    #[test]
    fn compare_pure_upgrade_higher_temporal() {
        let a = sel_from(&[(2, 0), (3, 0)]);
        let b = sel_from(&[(2, 1), (3, 0)]);
        assert_eq!(compare_selections(&a, &b), SelectionDelta::Upgrade);
    }

    #[test]
    fn compare_pure_downgrade_dropped_sender() {
        let a = sel_from(&[(2, 1), (3, 0)]);
        let b = sel_from(&[(2, 1)]);
        assert_eq!(compare_selections(&a, &b), SelectionDelta::Downgrade);
    }

    #[test]
    fn compare_pure_downgrade_lower_temporal() {
        let a = sel_from(&[(2, 2)]);
        let b = sel_from(&[(2, 1)]);
        assert_eq!(compare_selections(&a, &b), SelectionDelta::Downgrade);
    }

    /// Mixed motion (one gain + one loss) is conservatively a downgrade.
    #[test]
    fn compare_mixed_motion_is_downgrade() {
        let a = sel_from(&[(2, 1), (3, 0)]);
        // Sender 2 keeps T1, sender 3 dropped, sender 4 appears at T0.
        let b = sel_from(&[(2, 1), (4, 0)]);
        assert_eq!(compare_selections(&a, &b), SelectionDelta::Downgrade);
    }

    #[test]
    fn has_upgrade_headroom_empty_selection_is_true() {
        assert!(has_upgrade_headroom(0, &LayerSelection::new()));
        assert!(has_upgrade_headroom(100, &LayerSelection::new()));
    }

    /// 20% headroom math: a 128 kbps selection needs a budget of ≥ 153.6
    /// (-> at integer budget 154 it passes; at 153 it doesn't).
    #[test]
    fn has_upgrade_headroom_threshold() {
        let s = sel_from(&[(2, 0)]); // 128 kbps
        assert!(!has_upgrade_headroom(153, &s), "153 < 128*1.20 = 153.6");
        assert!(has_upgrade_headroom(154, &s), "154 >= 153.6");
    }
}
