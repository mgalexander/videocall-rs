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

//! Bouncing-bandwidth no-thrash scenarios for the layer-selection +
//! hysteresis pipeline (bead vc-qmp, p4-12).
//!
//! These scenarios run [`LayerSelector::pick_with_hysteresis`] under
//! synthetic bandwidth traces and lock down the "no thrash" property of
//! the hysteresis state machine: brief spikes do not upgrade, brief
//! dips do downgrade immediately (no symmetrical hysteresis), the
//! cooldown gate holds for the full 5 s window after a downgrade,
//! sustained increases eventually upgrade, oscillation is absorbed by
//! streak-resets + cooldown, and small jitter within a selection's
//! headroom band does not perturb the choice.
//!
//! Timing is fully deterministic. The selector exposes an injected
//! `Instant` (see [`LayerSelector::pick_with_hysteresis`]), so the tests
//! synthesize a virtual clock by advancing `t0 + Duration::from_*`. No
//! wall sleeps, no `tokio::time::pause` plumbing required — the whole
//! file runs in well under a second.
//!
//! ## Choice of "low" bandwidth
//!
//! Where the scenario calls for a "low" bandwidth that selects T0 only,
//! we use **170 kbps** rather than 200 kbps. The selector's effective
//! budget is `bw × bandwidth_headroom_pct` (default 0.85):
//!
//!   * 200 kbps → budget 170. T0 (128) fits. The 20% upgrade-headroom
//!     check on a T0 selection **passes** (170 ≥ 128 × 1.20 = 153.6),
//!     so the streak counter *also* runs during steady-state at 200
//!     kbps. That contaminates the brief-spike / sustained-increase
//!     scenarios because any subsequent spike inherits a multi-second
//!     streak from the prelude.
//!   * 170 kbps → budget 144. T0 (128) fits. The 20% upgrade-headroom
//!     check **fails** (144 < 153.6), so the streak counter resets on
//!     every steady-state tick. A subsequent spike must then build a
//!     fresh streak from scratch — exactly the no-thrash property
//!     these scenarios are trying to lock down.
//!
//! 170 kbps is still well under the T1 cumulative bitrate (384), so the
//! forwarded selection is identical to what 200 kbps would produce
//! (T0-only). Using 170 instead of 200 does not change which layer is
//! forwarded — it only ensures the streak is broken during the "low"
//! phase, matching the spec's intent.

use std::time::{Duration, Instant};

use crate::actors::session_logic::SessionId;
use crate::sfu::layer_selector::{LayerSelection, LayerSelector};
use crate::sfu::subscription::{AllowSet, LayerPref};

/// Bandwidth that produces a T0-only candidate *and* breaks the upgrade
/// streak (no 20% headroom over T0). See module docs for derivation.
const LOW_KBPS: u32 = 170;

/// Bandwidth that produces a T1 candidate (cumulative 384 kbps fits at
/// 1000 × 0.85 = 850 budget; T2 cumulative 896 does not).
const HIGH_KBPS: u32 = 1000;

/// Cooldown applied to upgrades after any downgrade. Mirrors the
/// (private) constant in [`super::super::layer_selector`]; if the
/// production value changes, this constant must be updated alongside
/// the scenario assertions that depend on it.
const DOWNGRADE_COOLDOWN: Duration = Duration::from_secs(5);

const RECEIVER: SessionId = 1;
const SENDER: SessionId = 2;

fn allow_one() -> AllowSet {
    let mut a = AllowSet::new();
    a.video.insert(SENDER, LayerPref::default());
    a.audio.insert(SENDER);
    a
}

fn temporal(sel: &LayerSelection) -> Option<u32> {
    sel.forward.get(&(SENDER, 0)).copied()
}

/// Scenario 1: steady 1000 kbps for 60 s. Selection must be identical
/// at every 100 ms sample (denser than the 1 s tick the spec requires,
/// to catch any rounding or streak-update bug that would manifest as a
/// one-call change).
#[test]
fn s1_steady_high_bandwidth_never_changes_selection() {
    let sel = LayerSelector::new();
    let allow = allow_one();
    let t0 = Instant::now();

    let baseline = sel.pick_with_hysteresis(RECEIVER, &allow, &[], HIGH_KBPS, t0);
    assert_eq!(
        temporal(&baseline),
        Some(1),
        "1000 kbps seeds at T1 (T2 cumulative 896 > 850 budget)",
    );

    for ms in (100..=60_000).step_by(100) {
        let now = t0 + Duration::from_millis(ms);
        let out = sel.pick_with_hysteresis(RECEIVER, &allow, &[], HIGH_KBPS, now);
        assert_eq!(
            out, baseline,
            "+{ms} ms: selection must be identical under constant bandwidth",
        );
    }
}

/// Scenario 2: low for 30 s, then a 2 s spike to HIGH_KBPS, then back
/// to low. The spike is below the 3 s sustain threshold; combined with
/// the streak-resetting "low" choice, no upgrade may fire during the
/// spike or after the drop.
#[test]
fn s2_brief_two_second_spike_does_not_upgrade() {
    let sel = LayerSelector::new();
    let allow = allow_one();
    let t0 = Instant::now();

    let seed = sel.pick_with_hysteresis(RECEIVER, &allow, &[], LOW_KBPS, t0);
    assert_eq!(temporal(&seed), Some(0), "low budget seeds at T0");

    // 30 s steady low — streak broken every tick.
    for s in 1..=30 {
        let out =
            sel.pick_with_hysteresis(RECEIVER, &allow, &[], LOW_KBPS, t0 + Duration::from_secs(s));
        assert_eq!(out, seed, "+{s} s steady low must remain T0");
    }

    let spike_start = t0 + Duration::from_secs(30);

    // 2 s spike. Sample at fine granularity to catch off-by-one in the
    // streak threshold comparison.
    for &ms in &[1u64, 100, 500, 1_000, 1_500, 1_999] {
        let now = spike_start + Duration::from_millis(ms);
        let out = sel.pick_with_hysteresis(RECEIVER, &allow, &[], HIGH_KBPS, now);
        assert_eq!(out, seed, "spike +{ms} ms: streak < 3 s, must not upgrade",);
    }

    // Back to low for 5 s. Still T0; streak broken again on the drop.
    let post_spike = spike_start + Duration::from_secs(2);
    for s in 1..=5 {
        let now = post_spike + Duration::from_secs(s);
        let out = sel.pick_with_hysteresis(RECEIVER, &allow, &[], LOW_KBPS, now);
        assert_eq!(out, seed, "post-spike +{s} s low must remain T0");
    }
}

/// Scenario 3: low for 30 s, then HIGH_KBPS for 10 s. The upgrade
/// fires exactly when the fresh streak (started on the high-bw
/// transition) reaches the 3 s threshold, and exactly once — the
/// remaining 7 s do not produce a second upgrade.
#[test]
fn s3_sustained_increase_upgrades_at_three_seconds() {
    let sel = LayerSelector::new();
    let allow = allow_one();
    let t0 = Instant::now();

    let seed = sel.pick_with_hysteresis(RECEIVER, &allow, &[], LOW_KBPS, t0);
    assert_eq!(temporal(&seed), Some(0));

    for s in 1..=30 {
        let out =
            sel.pick_with_hysteresis(RECEIVER, &allow, &[], LOW_KBPS, t0 + Duration::from_secs(s));
        assert_eq!(out, seed, "+{s} s steady low must remain T0");
    }

    let high_start = t0 + Duration::from_secs(30);

    // First high call seeds the streak; candidate is T1 but streak
    // elapsed is 0 → blocked.
    let first_high = sel.pick_with_hysteresis(RECEIVER, &allow, &[], HIGH_KBPS, high_start);
    assert_eq!(
        temporal(&first_high),
        Some(0),
        "first high call: streak 0 s, upgrade must be blocked",
    );

    // 1 s and 2 s in — streak still under threshold.
    for s in [1u64, 2] {
        let out = sel.pick_with_hysteresis(
            RECEIVER,
            &allow,
            &[],
            HIGH_KBPS,
            high_start + Duration::from_secs(s),
        );
        assert_eq!(
            temporal(&out),
            Some(0),
            "+{s} s high: streak < 3 s, blocked",
        );
    }

    // At streak == 3 s the gate satisfies and the upgrade fires.
    let at_three = sel.pick_with_hysteresis(
        RECEIVER,
        &allow,
        &[],
        HIGH_KBPS,
        high_start + Duration::from_secs(3),
    );
    assert_eq!(
        temporal(&at_three),
        Some(1),
        "3 s into sustained high — streak == 3 s, upgrade must fire",
    );

    // Exactly one upgrade. The remaining 7 s of high bw must be
    // identical to the upgraded selection (T2 cumulative 896 > 850
    // budget, so a second upgrade isn't even a candidate).
    for s in 4..=10 {
        let out = sel.pick_with_hysteresis(
            RECEIVER,
            &allow,
            &[],
            HIGH_KBPS,
            high_start + Duration::from_secs(s),
        );
        assert_eq!(out, at_three, "+{s} s into high: no second upgrade");
    }
}

/// Scenario 4: HIGH_KBPS for 30 s, then a 1 s dip to LOW_KBPS, then
/// HIGH_KBPS again. The dip downgrades the selection immediately. For
/// the next 5 s the cooldown gate blocks the recovery — even with the
/// streak gate satisfied. On the first call strictly past the cooldown
/// boundary, the upgrade fires.
#[test]
fn s4_brief_dip_downgrades_immediately_then_cooldown_holds_five_seconds() {
    let sel = LayerSelector::new();
    let allow = allow_one();
    let t0 = Instant::now();

    let seed = sel.pick_with_hysteresis(RECEIVER, &allow, &[], HIGH_KBPS, t0);
    assert_eq!(temporal(&seed), Some(1));

    for s in 1..=30 {
        let out = sel.pick_with_hysteresis(
            RECEIVER,
            &allow,
            &[],
            HIGH_KBPS,
            t0 + Duration::from_secs(s),
        );
        assert_eq!(out, seed, "+{s} s high steady-state must stay T1");
    }

    // Dip: 1 s at LOW_KBPS. Downgrade is immediate on the first low call.
    let dip_at = t0 + Duration::from_secs(31);
    let downgraded = sel.pick_with_hysteresis(RECEIVER, &allow, &[], LOW_KBPS, dip_at);
    assert_eq!(
        temporal(&downgraded),
        Some(0),
        "dip must downgrade immediately to T0 (no symmetric hysteresis)",
    );

    // Recovery: 1 s after the dip onset, bw returns to HIGH_KBPS and
    // stays there.
    let recovery_at = dip_at + Duration::from_secs(1);

    // Walk the cooldown window at 200 ms granularity. Strict `>`, so a
    // call at exactly dip_at + 5 s must still be blocked. Any earlier
    // upgrade would be a regression of the cooldown gate.
    let mut tick = 0u64;
    loop {
        let elapsed_ms = 200u64 * tick;
        let now = recovery_at + Duration::from_millis(elapsed_ms);
        if now.saturating_duration_since(dip_at) > DOWNGRADE_COOLDOWN {
            break;
        }
        let out = sel.pick_with_hysteresis(RECEIVER, &allow, &[], HIGH_KBPS, now);
        assert_eq!(
            out, downgraded,
            "cooldown must block upgrade at recovery +{elapsed_ms} ms",
        );
        tick += 1;
        assert!(tick < 200, "cooldown loop bounded — invariant guard");
    }

    // First call strictly past the cooldown boundary: gate flips to
    // ALLOW. Streak has been building since recovery_at (~4 s by now),
    // well above the 3 s threshold.
    let past_boundary = dip_at + DOWNGRADE_COOLDOWN + Duration::from_millis(1);
    let upgraded = sel.pick_with_hysteresis(RECEIVER, &allow, &[], HIGH_KBPS, past_boundary);
    assert_eq!(
        temporal(&upgraded),
        Some(1),
        "1 ms past cooldown boundary: upgrade must fire (streak already built)",
    );
}

/// Scenario 5: bandwidth alternates LOW_KBPS / HIGH_KBPS every 1 s for
/// 60 s. The first dip downgrades to T0 once and triggers a 5 s
/// cooldown. After the cooldown expires, the streak gate keeps the
/// system at T0 forever — every low tick resets the streak (LOW_KBPS
/// fails the 20 % headroom check over T0), so the streak never
/// accumulates the 3 s required for upgrade. Net result: 1 downgrade,
/// 0 upgrades.
#[test]
fn s5_oscillation_yields_one_downgrade_and_no_upgrades() {
    let sel = LayerSelector::new();
    let allow = allow_one();
    let t0 = Instant::now();

    let seed = sel.pick_with_hysteresis(RECEIVER, &allow, &[], HIGH_KBPS, t0);
    assert_eq!(temporal(&seed), Some(1));

    let mut last = seed.clone();
    let mut downgrade_count = 0u32;
    let mut upgrade_count = 0u32;

    for s in 1u64..=60 {
        let bw = if s.is_multiple_of(2) {
            HIGH_KBPS
        } else {
            LOW_KBPS
        };
        let now = t0 + Duration::from_secs(s);
        let out = sel.pick_with_hysteresis(RECEIVER, &allow, &[], bw, now);
        match (temporal(&last), temporal(&out)) {
            (Some(a), Some(b)) if b < a => downgrade_count += 1,
            (Some(a), Some(b)) if b > a => upgrade_count += 1,
            _ => {}
        }
        last = out;
    }

    assert_eq!(downgrade_count, 1, "exactly one downgrade (the first dip)",);
    assert_eq!(
        upgrade_count, 0,
        "no upgrades: cooldown + streak-reset absorb the oscillation",
    );
    assert_eq!(
        temporal(&last),
        Some(0),
        "final selection must be T0 after a thrash storm",
    );
}

/// Scenario 6: bandwidth bounces inside a band that keeps T1 strictly
/// selectable. Centre = 600 kbps; bouncing ±15 % keeps the budget in
/// [433, 587] — T1 (cumulative 384) fits at both extremes, T2 (896)
/// never does. The candidate computation produces the same `T1` choice
/// on every tick → `Identical` classifier → no downgrade emitted.
#[test]
fn s6_bouncing_within_t1_headroom_band_yields_no_downgrade() {
    let sel = LayerSelector::new();
    let allow = allow_one();
    let t0 = Instant::now();

    let centre: u32 = 600;
    let seed = sel.pick_with_hysteresis(RECEIVER, &allow, &[], centre, t0);
    assert_eq!(temporal(&seed), Some(1), "centre 600 kbps seeds at T1");

    let lo = (centre as f32 * 0.85).round() as u32; // 510
    let hi = (centre as f32 * 1.15).round() as u32; // 690

    // Sanity: confirm the band edges still resolve to T1 only.
    {
        let sanity = LayerSelector::new();
        let edge_lo = sanity.pick_with_hysteresis(RECEIVER, &allow, &[], lo, t0);
        let edge_hi = sanity.pick_with_hysteresis(RECEIVER, &allow, &[], hi, t0);
        assert_eq!(
            temporal(&edge_lo),
            Some(1),
            "lo edge {lo} kbps must seed T1"
        );
        assert_eq!(
            temporal(&edge_hi),
            Some(1),
            "hi edge {hi} kbps must seed T1"
        );
    }

    for s in 1u64..=60 {
        let bw = if s.is_multiple_of(2) { lo } else { hi };
        let now = t0 + Duration::from_secs(s);
        let out = sel.pick_with_hysteresis(RECEIVER, &allow, &[], bw, now);
        assert_eq!(
            out, seed,
            "+{s} s at {bw} kbps: bouncing within the T1 band must not downgrade",
        );
    }
}
