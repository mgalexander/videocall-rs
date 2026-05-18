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
//! Hysteresis (upgrade watchdog + downgrade cooldown) lands in p4-6.
//! Forwarder consumption of the [`LayerSelection`] lands in p4-7.

use std::collections::HashMap;

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

/// Configurable greedy two-pass layer selector.
///
/// Stateless: a single `LayerSelector` instance can be shared across all
/// receivers in a room. Configuration knobs are intended to be loaded
/// once at startup from [`super::config::SfuConfig`].
#[derive(Debug, Clone)]
pub struct LayerSelector {
    /// Hard cap on per-receiver forwarded video bitrate (kbps).
    pub max_video_kbps: u32,
    /// Fraction of the measured downlink we actually fill (0.0..=1.0).
    pub bandwidth_headroom_pct: f32,
}

impl LayerSelector {
    pub fn new() -> Self {
        Self {
            max_video_kbps: DEFAULT_MAX_VIDEO_KBPS,
            bandwidth_headroom_pct: DEFAULT_BANDWIDTH_HEADROOM_PCT,
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
}
