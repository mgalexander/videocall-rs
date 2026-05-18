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

//! Per-sender speaker scoring via EWMA over `RoutingHeader.audio_level`.
//!
//! Implements ADR-0002 p3-1: each AUDIO MediaPacket feeds the sender's EWMA
//! (α = 0.3). `is_speaking()` gates on the EWMA exceeding a floor AND a recent
//! VAD hint (`RoutingHeader.is_speaking`) within a short recency window. This
//! module is decision-pure: it does not tick, publish, or apply hysteresis —
//! those land in p3-2/p3-3.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::actors::session_logic::SessionId;

/// EWMA smoothing factor for incoming audio-level observations.
const ALPHA: f32 = 0.3;
/// Minimum EWMA below which a sender is never considered "speaking".
const SPEAKING_FLOOR: f32 = 0.05;
/// How recently the VAD hint (`is_speaking_hint == true`) must have been
/// observed for `is_speaking()` to return true.
const VAD_RECENCY: Duration = Duration::from_millis(400);

/// Per-sender state tracked by the scorer.
struct ScoreState {
    /// Smoothed audio level in `[0, 1]`.
    ewma: f32,
    /// Wall-clock time of the most recent `observe()` call.
    last_update: Instant,
    /// Raw value of `is_speaking_hint` from the most recent observation
    /// (kept for telemetry/debugging; the speaking gate uses the
    /// time-windowed `last_speaking_hint_at` below).
    last_is_speaking_hint: bool,
    /// `Instant` when `is_speaking_hint` was last observed as `true`.
    /// `None` until the first true hint is seen.
    last_speaking_hint_at: Option<Instant>,
}

/// Tracks per-sender speaker scores derived from audio-level observations.
///
/// Callers should invoke [`SpeakerScorer::observe`] for every AUDIO
/// `MediaPacket` they receive, then query [`SpeakerScorer::score`],
/// [`SpeakerScorer::is_speaking`], or [`SpeakerScorer::top_n`] to drive
/// downstream forwarding decisions.
pub struct SpeakerScorer {
    scores: HashMap<SessionId, ScoreState>,
}

impl SpeakerScorer {
    /// Create a new empty scorer.
    pub fn new() -> Self {
        Self {
            scores: HashMap::new(),
        }
    }

    /// Record an audio-level observation for `sender`.
    ///
    /// `audio_level` is expected in `[0, 1]` (matches
    /// `RoutingHeader.audio_level`); it is clamped defensively. The sender's
    /// EWMA is updated as `α * audio_level + (1 - α) * ewma_prev`.
    pub fn observe(&mut self, sender: SessionId, audio_level: f32, is_speaking_hint: bool) {
        let clamped = audio_level.clamp(0.0, 1.0);
        let now = Instant::now();
        let entry = self.scores.entry(sender).or_insert_with(|| ScoreState {
            ewma: 0.0,
            last_update: now,
            last_is_speaking_hint: false,
            last_speaking_hint_at: None,
        });
        entry.ewma = ALPHA * clamped + (1.0 - ALPHA) * entry.ewma;
        entry.last_update = now;
        entry.last_is_speaking_hint = is_speaking_hint;
        if is_speaking_hint {
            entry.last_speaking_hint_at = Some(now);
        }
    }

    /// Return the current EWMA score for `sender`, or `0.0` if unknown.
    pub fn score(&self, sender: SessionId) -> f32 {
        self.scores.get(&sender).map(|s| s.ewma).unwrap_or(0.0)
    }

    /// Return `true` iff the sender's EWMA exceeds the speaking floor AND
    /// its `is_speaking_hint` was observed as `true` within the last
    /// [`VAD_RECENCY`] window.
    pub fn is_speaking(&self, sender: SessionId) -> bool {
        let Some(state) = self.scores.get(&sender) else {
            return false;
        };
        if state.ewma <= SPEAKING_FLOOR {
            return false;
        }
        match state.last_speaking_hint_at {
            Some(t) => Instant::now().duration_since(t) <= VAD_RECENCY,
            None => false,
        }
    }

    /// Return up to `n` `(sender, score)` pairs sorted by score descending.
    pub fn top_n(&self, n: usize) -> Vec<(SessionId, f32)> {
        let mut all: Vec<(SessionId, f32)> =
            self.scores.iter().map(|(sid, s)| (*sid, s.ewma)).collect();
        // Descending by score; total_cmp avoids NaN footguns.
        all.sort_by(|a, b| b.1.total_cmp(&a.1));
        all.truncate(n);
        all
    }

    /// Drop all tracked state for `sender` (e.g., on room exit).
    pub fn forget(&mut self, sender: SessionId) {
        self.scores.remove(&sender);
    }
}

impl Default for SpeakerScorer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn observe_then_score_applies_alpha_from_zero() {
        let mut s = SpeakerScorer::new();
        s.observe(1, 0.8, true);
        // Prior EWMA is 0, so new EWMA == ALPHA * 0.8.
        let expected = ALPHA * 0.8;
        assert!((s.score(1) - expected).abs() < 1e-6);
        assert_eq!(s.score(999), 0.0);
    }

    #[test]
    fn is_speaking_respects_floor() {
        let mut s = SpeakerScorer::new();
        // Drive EWMA to just under SPEAKING_FLOOR. With ALPHA = 0.3 starting
        // from 0, a single observation of `level` yields ewma = 0.3 * level.
        // So level just-under = 0.05/0.3 - eps.
        let just_under = (SPEAKING_FLOOR / ALPHA) - 0.01;
        s.observe(1, just_under, true);
        assert!(s.score(1) < SPEAKING_FLOOR);
        assert!(!s.is_speaking(1));

        // Now push above the floor with a fresh sender.
        let just_over = (SPEAKING_FLOOR / ALPHA) + 0.05;
        s.observe(2, just_over, true);
        assert!(s.score(2) > SPEAKING_FLOOR);
        assert!(s.is_speaking(2));
    }

    #[test]
    fn is_speaking_respects_vad_recency_window() {
        let mut s = SpeakerScorer::new();
        // Push EWMA well above the floor with hint=true.
        s.observe(1, 0.9, true);
        assert!(s.is_speaking(1));

        // Wait past the 400ms VAD recency window.
        thread::sleep(Duration::from_millis(450));

        // A subsequent observation with hint=false keeps EWMA high but the
        // most recent true-hint Instant is now stale.
        s.observe(1, 0.9, false);
        assert!(s.score(1) > SPEAKING_FLOOR);
        assert!(!s.is_speaking(1));
    }

    #[test]
    fn top_n_returns_sorted_desc_and_respects_n() {
        let mut s = SpeakerScorer::new();
        s.observe(10, 0.2, false);
        s.observe(20, 0.9, false);
        s.observe(30, 0.5, false);

        let top2 = s.top_n(2);
        assert_eq!(top2.len(), 2);
        assert_eq!(top2[0].0, 20);
        assert_eq!(top2[1].0, 30);
        assert!(top2[0].1 >= top2[1].1);

        // n larger than population returns all entries.
        let top10 = s.top_n(10);
        assert_eq!(top10.len(), 3);
        assert_eq!(top10[0].0, 20);
        assert_eq!(top10[2].0, 10);

        // n = 0 returns empty.
        assert!(s.top_n(0).is_empty());
    }

    #[test]
    fn forget_removes_sender() {
        let mut s = SpeakerScorer::new();
        s.observe(1, 0.7, true);
        s.observe(2, 0.5, true);
        assert!(s.score(1) > 0.0);

        s.forget(1);
        assert_eq!(s.score(1), 0.0);

        let top = s.top_n(10);
        assert_eq!(top.len(), 1);
        assert_eq!(top[0].0, 2);
    }
}
