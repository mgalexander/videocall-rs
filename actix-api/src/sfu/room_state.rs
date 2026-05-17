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

use std::collections::HashMap;
use std::time::Instant;

use crate::actors::session_logic::SessionId;

/// Client supports the SFU routing header on media packets.
pub const CAP_SFU_ROUTING_HEADER: u32 = 1;

/// Client supports scalable video coding (SVC) layered encoding.
pub const CAP_SVC: u32 = 2;

/// Client supports the subscription model (subscribe/unsubscribe to peers).
pub const CAP_SUBSCRIPTION: u32 = 4;

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
}

/// Authoritative per-room state for the SFU.
///
/// The struct does not own any synchronization primitive; wrap it in an
/// `Arc<RwLock<RoomState>>` (or equivalent) at the caller.
#[derive(Debug)]
pub struct RoomState {
    pub room_id: String,
    pub members: HashMap<SessionId, MemberEntry>,
}

impl RoomState {
    /// Create a new empty room.
    pub fn new(room_id: String) -> Self {
        Self {
            room_id,
            members: HashMap::new(),
        }
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
        };
        self.members.insert(sid, entry);
    }

    /// Remove a member from the room. No-op if absent.
    pub fn remove_member(&mut self, sid: SessionId) {
        self.members.remove(&sid);
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
}

impl Default for RoomState {
    fn default() -> Self {
        Self::new(String::new())
    }
}
