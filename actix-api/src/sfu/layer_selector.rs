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

use super::subscription::AllowSet;

// TODO(p4-5): greedy two-pass layer selection
/// Placeholder layer selector. Real selection lands in p4-5.
pub struct LayerSelector {}

impl LayerSelector {
    pub fn new() -> Self {
        Self {}
    }

    /// Pick (session_id, spatial, temporal) tuples for a receiver. Empty until p4-5.
    pub fn pick_layers(&self, _receiver_sid: u64, _allow_set: &AllowSet) -> Vec<(u64, u32, u32)> {
        // (session_id, spatial, temporal) — empty until p4-5
        Vec::new()
    }
}

impl Default for LayerSelector {
    fn default() -> Self {
        Self::new()
    }
}
