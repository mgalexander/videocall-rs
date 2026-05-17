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

use std::collections::HashSet;

/// Per-receiver allow-set: which senders' audio/video may be forwarded.
pub struct AllowSet {
    pub audio: HashSet<u64>,
    pub video: HashSet<u64>,
}

impl AllowSet {
    pub fn new() -> Self {
        Self {
            audio: HashSet::new(),
            video: HashSet::new(),
        }
    }
}

impl Default for AllowSet {
    fn default() -> Self {
        Self::new()
    }
}

/// A receiver's declared subscription preferences.
///
/// Reconciliation against room state lands in p3-4.
pub struct Subscription {
    pub pinned: Vec<u64>,
    /// session ids referenced by VisibilitySlot — full slot struct lands later
    pub slots: Vec<u64>,
    pub max_video_kbps: u32,
    pub receive_all_audio: bool,
}

impl Subscription {
    pub fn new() -> Self {
        Self {
            pinned: Vec::new(),
            slots: Vec::new(),
            max_video_kbps: 0,
            receive_all_audio: false,
        }
    }
}

impl Default for Subscription {
    fn default() -> Self {
        Self::new()
    }
}
