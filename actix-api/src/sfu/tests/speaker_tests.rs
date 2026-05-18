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

//! Behavioral invariants for `sfu::speaker::SpeakerScorer` and
//! `sfu::speaker::SpeakerTick`. Companion to the inline tests inside
//! `speaker.rs`; this module covers the test-coverage matrix from bead
//! vc-980 (p3-9) and locks down the public-API contract.
//!
//! Tick-driven tests use the `pub(crate)` test seam
//! [`SpeakerTick::drive_tick_for_test`] so hysteresis windows can be
//! exercised with synthetic `Instant`s rather than wall-clock sleeps.
//! VAD-recency tests use a short `std::thread::sleep` because
//! `SpeakerScorer::observe`/`is_speaking` call `std::time::Instant::now()`
//! directly, which is NOT affected by `tokio::time::pause`. Sleep
//! durations are kept under 500ms so the whole suite stays well under
//! the 2s budget.

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;

use crate::sfu::speaker::{SpeakerScorer, SpeakerTick, MAX_SPEAKERS};

/// EWMA alpha used by `SpeakerScorer`. Mirrors the private `ALPHA`
/// constant in `speaker.rs` so the tolerance math here is self-evident.
const ALPHA: f32 = 0.3;
/// Speaking floor mirrored from `speaker.rs`.
const SPEAKING_FLOOR: f32 = 0.05;

// ---------------------------------------------------------------------------
// 1. EWMA basics (covers p3-1)
// ---------------------------------------------------------------------------

#[test]
fn ewma_all_zero_samples_stay_at_zero() {
    let mut s = SpeakerScorer::new();
    for _ in 0..50 {
        s.observe(1, 0.0, false);
    }
    assert_eq!(
        s.score(1),
        0.0,
        "EWMA of an all-zero input stream must remain exactly zero"
    );
}

#[test]
fn ewma_sustained_input_converges_toward_steady_state() {
    let mut s = SpeakerScorer::new();
    // Sustained 0.5 → EWMA converges to 0.5. After N observations,
    // ewma = 0.5 * (1 - (1-alpha)^N). For alpha=0.3, N=10:
    // 1 - 0.7^10 ≈ 1 - 0.0282 ≈ 0.9718, so ewma ≈ 0.486.
    for _ in 0..10 {
        s.observe(1, 0.5, true);
    }
    let score = s.score(1);
    assert!(
        (score - 0.5).abs() < 0.05,
        "EWMA must converge within 0.05 of steady-state after ~10 observations; got {score}"
    );
}

#[test]
fn ewma_single_spike_then_silence_decays_gradually() {
    let mut s = SpeakerScorer::new();
    // Big spike: ewma jumps to ALPHA * 1.0 = 0.3.
    s.observe(1, 1.0, true);
    let after_spike = s.score(1);
    assert!(
        (after_spike - ALPHA).abs() < 1e-6,
        "first observation should land at ALPHA * input"
    );

    // Now silence: each step multiplies ewma by (1 - alpha) = 0.7.
    s.observe(1, 0.0, false);
    let after_one_silence = s.score(1);
    let expected_one = after_spike * (1.0 - ALPHA);
    assert!(
        (after_one_silence - expected_one).abs() < 1e-6,
        "one silent step should decay by factor (1-alpha)"
    );
    assert!(
        after_one_silence < after_spike,
        "decay must be monotonic downward"
    );

    // Several more silent steps push score arbitrarily low but never < 0.
    for _ in 0..20 {
        s.observe(1, 0.0, false);
    }
    let final_score = s.score(1);
    assert!(final_score >= 0.0, "EWMA must never go negative");
    assert!(
        final_score < 0.01,
        "20 silent steps after a spike should decay below 0.01; got {final_score}"
    );
}

// ---------------------------------------------------------------------------
// 2. `is_speaking` threshold + VAD hint window (covers p3-1)
// ---------------------------------------------------------------------------

#[test]
fn is_speaking_true_when_score_above_floor_and_hint_recent() {
    let mut s = SpeakerScorer::new();
    // Push EWMA well above SPEAKING_FLOOR with hint=true; the observation
    // we just made counts as "recent" for the 400ms VAD window.
    s.observe(1, 0.9, true);
    assert!(s.score(1) > SPEAKING_FLOOR);
    assert!(
        s.is_speaking(1),
        "score above floor + fresh true-hint must read as speaking"
    );
}

#[test]
fn is_speaking_false_when_hint_is_stale_beyond_window() {
    let mut s = SpeakerScorer::new();
    s.observe(1, 0.9, true);
    assert!(s.is_speaking(1));

    // Sleep past the 400ms VAD recency window. `Instant::now()` inside
    // the scorer is wall-clock; `tokio::time::pause` cannot affect it,
    // so we use a real sleep. 450ms keeps the suite under budget.
    thread::sleep(Duration::from_millis(450));

    // A subsequent observation with hint=false keeps EWMA high but the
    // most recent true-hint Instant is now stale.
    s.observe(1, 0.9, false);
    assert!(s.score(1) > SPEAKING_FLOOR);
    assert!(
        !s.is_speaking(1),
        "stale hint (>400ms old) must disqualify the sender even with high score"
    );
}

#[test]
fn is_speaking_false_when_score_below_floor_even_with_hint() {
    let mut s = SpeakerScorer::new();
    // ewma = ALPHA * 0.05 = 0.015, well below SPEAKING_FLOOR.
    s.observe(1, 0.05, true);
    assert!(s.score(1) < SPEAKING_FLOOR);
    assert!(
        !s.is_speaking(1),
        "score below floor must NEVER read as speaking, hint notwithstanding"
    );

    // Even with continued true hints, score has to climb above the floor.
    for _ in 0..2 {
        s.observe(1, 0.05, true);
    }
    assert!(s.score(1) < SPEAKING_FLOOR);
    assert!(!s.is_speaking(1));
}

// ---------------------------------------------------------------------------
// Helpers shared by tick-driven tests below.
// ---------------------------------------------------------------------------

/// Repeatedly observe `level` so a single tick reads EWMA above floor.
/// Five observations at any level above 0.05 converge above floor for
/// alpha=0.3 (steady-state ≈ 83% of input by N=5).
fn seed_high_score(scorer: &mut SpeakerScorer, sid: u64, level: f32) {
    for _ in 0..5 {
        scorer.observe(sid, level, true);
    }
}

// ---------------------------------------------------------------------------
// 3. Hysteresis entry (covers p3-2)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hysteresis_entry_brief_spike_below_window_does_not_admit() {
    let scorer = Arc::new(RwLock::new(SpeakerScorer::new()));
    let tick = SpeakerTick::with_interval(scorer.clone(), Duration::from_millis(200), "", None);

    // Seed 7 above floor.
    {
        let mut s = scorer.write().await;
        seed_high_score(&mut s, 7, 0.8);
    }

    // First tick at t=0: marks `above_since` but window not yet elapsed.
    let t0 = Instant::now();
    tick.drive_tick_for_test(t0).await;
    let snap = tick.current().await;
    assert!(
        snap.top.is_empty(),
        "must not admit on the first above-threshold tick"
    );
    assert_eq!(snap.generation, 0);

    // Drain the sender before the entry window (200ms) elapses; only
    // 100ms later we drive another tick. The brief spike must not have
    // admitted the sender.
    {
        let mut s = scorer.write().await;
        s.forget(7);
        // Keep an unrelated low-score sender so `top_n` returns something.
        s.observe(99, 0.0, false);
    }
    tick.drive_tick_for_test(t0 + Duration::from_millis(100))
        .await;
    let snap = tick.current().await;
    assert!(
        snap.top.is_empty(),
        "brief above-threshold flash (<200ms) must NOT enter the set: {:?}",
        snap.top
    );
    assert_eq!(
        snap.generation, 0,
        "no membership change => no generation bump"
    );
}

#[tokio::test]
async fn hysteresis_entry_sustained_above_window_admits() {
    let scorer = Arc::new(RwLock::new(SpeakerScorer::new()));
    let tick = SpeakerTick::with_interval(scorer.clone(), Duration::from_millis(200), "", None);

    {
        let mut s = scorer.write().await;
        seed_high_score(&mut s, 11, 0.9);
    }

    let t0 = Instant::now();
    // Tick #1: marks above_since at t0.
    tick.drive_tick_for_test(t0).await;
    assert!(tick.current().await.top.is_empty());

    // Tick #2 at t0+200ms: entry window satisfied → admit.
    tick.drive_tick_for_test(t0 + Duration::from_millis(200))
        .await;
    let snap = tick.current().await;
    assert!(
        snap.top.contains(&11),
        "sender held above threshold for >=200ms must be admitted: {:?}",
        snap.top
    );
    assert_eq!(snap.generation, 1, "first admission bumps generation to 1");
}

// ---------------------------------------------------------------------------
// 4. Hysteresis exit (covers p3-2)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hysteresis_exit_brief_dip_below_window_does_not_evict() {
    let scorer = Arc::new(RwLock::new(SpeakerScorer::new()));
    let tick = SpeakerTick::with_interval(scorer.clone(), Duration::from_millis(200), "", None);

    // Admit sender 5 first (two ticks separated by the entry window).
    {
        let mut s = scorer.write().await;
        seed_high_score(&mut s, 5, 0.9);
    }
    let t0 = Instant::now();
    tick.drive_tick_for_test(t0).await;
    tick.drive_tick_for_test(t0 + Duration::from_millis(200))
        .await;
    let admit_snap = tick.current().await;
    assert!(admit_snap.top.contains(&5));
    let gen_after_entry = admit_snap.generation;

    // Drop sender below threshold. Drive ticks at +400ms and +600ms
    // (i.e. 200ms and 400ms into the 800ms exit window). Sender must
    // remain in the set throughout.
    {
        let mut s = scorer.write().await;
        s.forget(5);
    }
    tick.drive_tick_for_test(t0 + Duration::from_millis(400))
        .await;
    tick.drive_tick_for_test(t0 + Duration::from_millis(600))
        .await;
    let snap = tick.current().await;
    assert!(
        snap.top.contains(&5),
        "sender must persist inside the 800ms exit window: {:?}",
        snap.top
    );
    assert_eq!(
        snap.generation, gen_after_entry,
        "no eviction yet => no generation bump"
    );
}

#[tokio::test]
async fn hysteresis_exit_sustained_below_window_evicts() {
    let scorer = Arc::new(RwLock::new(SpeakerScorer::new()));
    let tick = SpeakerTick::with_interval(scorer.clone(), Duration::from_millis(200), "", None);

    {
        let mut s = scorer.write().await;
        seed_high_score(&mut s, 5, 0.9);
    }
    let t0 = Instant::now();
    tick.drive_tick_for_test(t0).await;
    tick.drive_tick_for_test(t0 + Duration::from_millis(200))
        .await;
    let gen_after_entry = tick.current().await.generation;

    // Drop and let the full 800ms exit window elapse. First below-tick
    // is at t0+400ms, so we need at least t0+1200ms to evict.
    {
        let mut s = scorer.write().await;
        s.forget(5);
    }
    tick.drive_tick_for_test(t0 + Duration::from_millis(400))
        .await;
    tick.drive_tick_for_test(t0 + Duration::from_millis(1300))
        .await;
    let snap = tick.current().await;
    assert!(
        !snap.top.contains(&5),
        "sender must be evicted once below-streak >= 800ms: {:?}",
        snap.top
    );
    assert!(
        snap.generation > gen_after_entry,
        "eviction must bump generation (was {}, now {})",
        gen_after_entry,
        snap.generation
    );
}

// ---------------------------------------------------------------------------
// 5. Generation idempotency (covers p3-2)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn generation_does_not_increment_when_set_is_stable() {
    let scorer = Arc::new(RwLock::new(SpeakerScorer::new()));
    let tick = SpeakerTick::with_interval(scorer.clone(), Duration::from_millis(200), "", None);

    {
        let mut s = scorer.write().await;
        seed_high_score(&mut s, 1, 0.9);
        seed_high_score(&mut s, 2, 0.6);
    }

    let t0 = Instant::now();
    tick.drive_tick_for_test(t0).await;
    tick.drive_tick_for_test(t0 + Duration::from_millis(200))
        .await;
    let admitted = tick.current().await;
    let gen1 = admitted.generation;
    assert!(admitted.top.contains(&1) && admitted.top.contains(&2));
    assert!(gen1 >= 1);

    // Several quiet ticks with the same scores → no membership/order
    // change, so generation must stay pinned at `gen1`.
    for k in 1..=5 {
        {
            let mut s = scorer.write().await;
            s.observe(1, 0.9, true);
            s.observe(2, 0.6, true);
        }
        tick.drive_tick_for_test(t0 + Duration::from_millis(200 + 200 * k))
            .await;
        let snap = tick.current().await;
        assert_eq!(
            snap.top, admitted.top,
            "membership/order must remain stable across quiet ticks"
        );
        assert_eq!(
            snap.generation, gen1,
            "generation must NOT bump when set is unchanged (tick {k})"
        );
    }
}

#[tokio::test]
async fn generation_increments_on_each_set_change() {
    let scorer = Arc::new(RwLock::new(SpeakerScorer::new()));
    let tick = SpeakerTick::with_interval(scorer.clone(), Duration::from_millis(200), "", None);

    // Step A: admit sender 1.
    {
        let mut s = scorer.write().await;
        seed_high_score(&mut s, 1, 0.9);
    }
    let t0 = Instant::now();
    tick.drive_tick_for_test(t0).await;
    tick.drive_tick_for_test(t0 + Duration::from_millis(200))
        .await;
    let gen_a = tick.current().await.generation;
    assert_eq!(gen_a, 1, "first admission bumps to 1");

    // Step B: add sender 2 — second membership change, bump to 2.
    {
        let mut s = scorer.write().await;
        seed_high_score(&mut s, 2, 0.6);
        // Refresh 1 so EWMA stays hot and admission isn't tugged.
        s.observe(1, 0.9, true);
    }
    tick.drive_tick_for_test(t0 + Duration::from_millis(400))
        .await;
    tick.drive_tick_for_test(t0 + Duration::from_millis(600))
        .await;
    let snap_b = tick.current().await;
    assert!(snap_b.top.contains(&2));
    let gen_b = snap_b.generation;
    assert!(
        gen_b > gen_a,
        "adding a member must bump generation (gen_a={gen_a}, gen_b={gen_b})"
    );

    // Step C: introduce sender 3 with an even higher score so it bumps
    // the *order* of the top — also a set change.
    {
        let mut s = scorer.write().await;
        seed_high_score(&mut s, 3, 0.99);
        s.observe(1, 0.9, true);
        s.observe(2, 0.6, true);
    }
    tick.drive_tick_for_test(t0 + Duration::from_millis(800))
        .await;
    tick.drive_tick_for_test(t0 + Duration::from_millis(1000))
        .await;
    let snap_c = tick.current().await;
    assert!(snap_c.top.contains(&3));
    let gen_c = snap_c.generation;
    assert!(
        gen_c > gen_b,
        "order change (new top speaker) must bump generation (gen_b={gen_b}, gen_c={gen_c})"
    );

    // Monotonicity sanity: gen_a < gen_b < gen_c.
    assert!(
        gen_a < gen_b && gen_b < gen_c,
        "generation must be monotonic"
    );
}

// ---------------------------------------------------------------------------
// 6. Top-N cap (covers p3-2)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn top_n_cap_truncates_to_max_speakers_sorted_by_score() {
    let scorer = Arc::new(RwLock::new(SpeakerScorer::new()));
    let tick = SpeakerTick::with_interval(scorer.clone(), Duration::from_millis(200), "", None);

    // 10 senders all above floor, with distinct descending scores so
    // sort order is deterministic. MAX_SPEAKERS = 4 → only the top 4
    // by score may appear; the lower 6 must be excluded.
    {
        let mut s = scorer.write().await;
        seed_high_score(&mut s, 10, 0.95);
        seed_high_score(&mut s, 20, 0.90);
        seed_high_score(&mut s, 30, 0.85);
        seed_high_score(&mut s, 40, 0.80);
        seed_high_score(&mut s, 50, 0.75);
        seed_high_score(&mut s, 60, 0.70);
        seed_high_score(&mut s, 70, 0.65);
        seed_high_score(&mut s, 80, 0.60);
        seed_high_score(&mut s, 90, 0.55);
        seed_high_score(&mut s, 100, 0.50);
    }

    let t0 = Instant::now();
    tick.drive_tick_for_test(t0).await;
    tick.drive_tick_for_test(t0 + Duration::from_millis(200))
        .await;

    let snap = tick.current().await;
    assert_eq!(
        snap.top.len(),
        MAX_SPEAKERS,
        "set must be capped at MAX_SPEAKERS={MAX_SPEAKERS}, got {:?}",
        snap.top
    );
    // The four highest-scoring senders are 10, 20, 30, 40, in that order.
    assert_eq!(
        snap.top,
        vec![10, 20, 30, 40],
        "top must be sorted by score descending and contain the 4 highest"
    );
    for excluded in [50, 60, 70, 80, 90, 100] {
        assert!(
            !snap.top.contains(&excluded),
            "lower-scored sender {excluded} must not appear in capped top"
        );
    }
}
