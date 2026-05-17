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

// TODO(p3-1): EWMA (α=0.3) per-sender scoring
/// Placeholder speaker scorer. EWMA logic lands in p3-1.
pub struct SpeakerScorer {}

impl SpeakerScorer {
    pub fn new() -> Self {
        Self {}
    }

    /// Record an audio-level observation for a sender. No-op until p3-1.
    pub fn observe(&mut self, _session_id: u64, _audio_level: u32, _is_speaking: bool) {
        // no-op
    }

    /// Return the top-N session ids by speaker score. Always empty until p3-1.
    pub fn top_speakers(&self, _n: usize) -> Vec<u64> {
        Vec::new()
    }
}

impl Default for SpeakerScorer {
    fn default() -> Self {
        Self::new()
    }
}
