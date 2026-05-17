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

use std::sync::{Arc, RwLock};

use bytes::Bytes;
use protobuf::Message;
use videocall_types::protos::media_packet::RoutingHeader;
use videocall_types::protos::packet_wrapper::PacketWrapper;

use crate::actors::session_logic::SessionId;
use crate::sfu::room_state::RoomState;

/// Decision returned by the [`Forwarder`] for a single (packet, receiver) pair.
pub enum ForwardDecision {
    /// Forward the serialized packet bytes to the receiver.
    Forward(Bytes),
    /// Drop the packet for this receiver.
    Drop,
}

/// Per-room forwarder.
///
/// Holds a shared handle to the authoritative [`RoomState`]. `decide` is
/// invoked from each receiver's NATS subscription callback and takes a
/// read lock on the room for the duration of the decision — callers must
/// keep that work cheap.
///
/// Wave-3 implements *pass-through* semantics: every packet is forwarded
/// except for the sender's own echo (self-skip). Real per-receiver
/// filtering (subscription, allow-set, layer selection) lands in later
/// phases.
pub struct Forwarder {
    room: Arc<RwLock<RoomState>>,
}

impl Forwarder {
    pub fn new(room: Arc<RwLock<RoomState>>) -> Self {
        Self { room }
    }

    /// Pass-through decision for `receiver_sid`.
    ///
    /// Returns [`ForwardDecision::Drop`] iff the packet's sender is the
    /// receiver itself (self-skip — preserves legacy fanout semantics).
    /// Otherwise re-serializes the packet and returns
    /// [`ForwardDecision::Forward`] with the bytes.
    pub fn decide(
        &self,
        receiver_sid: SessionId,
        packet_wrapper: &PacketWrapper,
        _routing_header: Option<&RoutingHeader>,
    ) -> ForwardDecision {
        // Hold a read lock for the duration of the decision so future
        // phases can layer subscription / capability / speaker checks
        // here without changing the call site. The lock is poison-safe:
        // a panicked writer leaves the room readable.
        let _room = match self.room.read() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };

        if packet_wrapper.session_id == receiver_sid {
            return ForwardDecision::Drop;
        }

        match packet_wrapper.write_to_bytes() {
            Ok(bytes) => ForwardDecision::Forward(Bytes::from(bytes)),
            Err(_) => ForwardDecision::Drop,
        }
    }
}
