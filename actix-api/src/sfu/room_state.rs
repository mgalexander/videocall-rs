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

use std::collections::HashMap;

/// Per-member metadata tracked by the room.
pub struct MemberInfo {
    pub session_id: u64,
    pub user_id: String,
}

/// Authoritative per-room state for the SFU.
///
/// Lifecycle methods (join/leave/update) land in p2-6.
pub struct RoomState {
    /// session_id -> member info
    pub members: HashMap<u64, MemberInfo>,
    /// session_id -> client_capabilities bitfield
    pub capabilities: HashMap<u64, u32>,
}

impl RoomState {
    pub fn new() -> Self {
        Self {
            members: HashMap::new(),
            capabilities: HashMap::new(),
        }
    }
}

impl Default for RoomState {
    fn default() -> Self {
        Self::new()
    }
}
