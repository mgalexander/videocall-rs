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
 */

//! Per-receiver subscription wire emitter.
//!
//! `SfuClient` constructs and sends `SubscriptionUpdate` packets on the elected
//! transport so the SFU's per-receiver subscription store has fresh input
//! whenever client visibility/pin state changes. The packet content for
//! wave-1 is a stub (no real visibility plumbing yet); wave-3 (p3-8) will
//! populate the slot list from actual UI state.
//!
//! Mirrors the CONNECTION emission pattern from `connection_manager.rs`:
//! build a typed message, wrap in `PacketWrapper` with the correct
//! `packet_type`, and hand it to the reliable transport via the
//! `ConnectionController`.

use std::cell::RefCell;
use std::rc::Rc;

use protobuf::Message;
use videocall_types::protos::packet_wrapper::packet_wrapper::PacketType;
use videocall_types::protos::packet_wrapper::PacketWrapper;
use videocall_types::protos::subscription_packet::{SubscriptionUpdate, VisibilitySlot};

use crate::connection::ConnectionController;

/// Default `max_video_kbps` for the wave-1 stub payload. Centralised so wave-3
/// (p3-8) has one place to swap when bitrate is derived from real visibility
/// state.
pub(crate) const DEFAULT_MAX_VIDEO_KBPS: u32 = 2000;

/// Errors returned by [`SfuClient::emit_subscription_update`].
#[derive(Debug)]
pub enum SfuClientError {
    /// Protobuf serialization of the `SubscriptionUpdate` or `PacketWrapper`
    /// failed. In practice this only happens on internal protobuf bugs.
    Serialize(protobuf::Error),
    /// The transport rejected the packet (no elected connection, send queue
    /// closed, etc.).
    Transport(String),
    /// The connection controller was unavailable (not yet constructed, or
    /// the underlying `RefCell` was already borrowed mutably elsewhere).
    NoController,
}

impl std::fmt::Display for SfuClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SfuClientError::Serialize(e) => write!(f, "serialize SubscriptionUpdate: {e}"),
            SfuClientError::Transport(e) => write!(f, "transport send failure: {e}"),
            SfuClientError::NoController => write!(f, "no active ConnectionController"),
        }
    }
}

impl std::error::Error for SfuClientError {}

impl From<protobuf::Error> for SfuClientError {
    fn from(e: protobuf::Error) -> Self {
        SfuClientError::Serialize(e)
    }
}

/// Client-side emitter for SFU subscription packets.
///
/// Holds a shared handle to the same `ConnectionController` cell owned by
/// `VideoCallClient`, so it always emits on the currently-elected transport
/// (re-elections swap the controller's inner connection, not the cell).
pub struct SfuClient {
    user_id: String,
    connection_controller: Rc<RefCell<Option<ConnectionController>>>,
}

impl SfuClient {
    pub fn new(
        user_id: String,
        connection_controller: Rc<RefCell<Option<ConnectionController>>>,
    ) -> Self {
        Self {
            user_id,
            connection_controller,
        }
    }

    /// Build the `PacketWrapper` for a subscription update. Split out so
    /// tests and the send path share the same wire-format code.
    fn build_packet(
        user_id: &str,
        pinned: Vec<u64>,
        slots: Vec<VisibilitySlot>,
        max_video_kbps: u32,
        receive_all_audio: bool,
    ) -> Result<PacketWrapper, SfuClientError> {
        let update = SubscriptionUpdate {
            pinned_sessions: pinned,
            slots,
            max_video_kbps,
            receive_all_audio,
            ..Default::default()
        };
        let data = update.write_to_bytes()?;
        Ok(PacketWrapper {
            packet_type: PacketType::SUBSCRIPTION_UPDATE.into(),
            user_id: user_id.as_bytes().to_vec(),
            data,
            ..Default::default()
        })
    }

    /// Construct and send a `SubscriptionUpdate` on the elected (reliable)
    /// transport. The `async` signature is part of the wave-1 contract even
    /// though the current body has no `.await`; wave-3 will plumb async
    /// visibility state without changing callers.
    pub async fn emit_subscription_update(
        &self,
        pinned: Vec<u64>,
        slots: Vec<VisibilitySlot>,
        max_video_kbps: u32,
        receive_all_audio: bool,
    ) -> Result<(), SfuClientError> {
        let packet = Self::build_packet(
            &self.user_id,
            pinned,
            slots,
            max_video_kbps,
            receive_all_audio,
        )?;

        let cc = self
            .connection_controller
            .try_borrow()
            .map_err(|_| SfuClientError::NoController)?;
        let controller = cc.as_ref().ok_or(SfuClientError::NoController)?;
        controller
            .send_packet(packet)
            .map_err(|e| SfuClientError::Transport(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_packet_serializes_subscription_update() {
        let slot = VisibilitySlot {
            session_id: 42,
            preferred_spatial: 1,
            preferred_temporal: 2,
            ..Default::default()
        };
        let packet =
            SfuClient::build_packet("user-1", vec![7, 8], vec![slot], 2000, true).expect("build");

        assert_eq!(
            packet.packet_type.enum_value(),
            Ok(PacketType::SUBSCRIPTION_UPDATE)
        );
        assert_eq!(packet.user_id, b"user-1".to_vec());
        assert!(!packet.data.is_empty());

        let bytes = packet.write_to_bytes().expect("wrapper serializes");
        assert!(!bytes.is_empty());

        // Round-trip the inner payload to confirm field layout matches proto.
        let parsed = SubscriptionUpdate::parse_from_bytes(&packet.data).expect("parse inner");
        assert_eq!(parsed.pinned_sessions, vec![7, 8]);
        assert_eq!(parsed.slots.len(), 1);
        assert_eq!(parsed.slots[0].session_id, 42);
        assert_eq!(parsed.slots[0].preferred_spatial, 1);
        assert_eq!(parsed.slots[0].preferred_temporal, 2);
        assert_eq!(parsed.max_video_kbps, 2000);
        assert!(parsed.receive_all_audio);
    }

    // Covers the exact wave-1 stub payload emitted by VideoCallClient so any
    // future proto field rename is caught immediately on the on-the-wire shape.
    #[test]
    fn build_packet_round_trips_empty_wave1_stub() {
        let packet = SfuClient::build_packet("u", vec![], vec![], DEFAULT_MAX_VIDEO_KBPS, true)
            .expect("build");

        let parsed = SubscriptionUpdate::parse_from_bytes(&packet.data).expect("parse inner");
        assert!(parsed.pinned_sessions.is_empty());
        assert!(parsed.slots.is_empty());
        assert_eq!(parsed.max_video_kbps, DEFAULT_MAX_VIDEO_KBPS);
        assert!(parsed.receive_all_audio);
    }
}
