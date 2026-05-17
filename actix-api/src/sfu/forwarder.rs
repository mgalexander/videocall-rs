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

use bytes::Bytes;
use protobuf::Message;
use videocall_types::protos::media_packet::RoutingHeader;
use videocall_types::protos::packet_wrapper::PacketWrapper;

/// Decision returned by the [`Forwarder`] for a single (packet, receiver) pair.
pub enum ForwardDecision {
    /// Forward the serialized packet bytes to the receiver.
    Forward(Bytes),
    /// Drop the packet for this receiver.
    Drop,
}

/// Placeholder forwarder. Real forwarding logic lands in p2-3.
pub struct Forwarder {}

impl Forwarder {
    pub fn new() -> Self {
        Self {}
    }

    /// Pass-through decision: re-serializes the packet and returns it as bytes.
    ///
    /// Real per-receiver filtering (subscription, allow-set, layer selection) is
    /// not implemented yet — see p2-3 / p3-4 / p4-5.
    pub fn decide(
        &self,
        _receiver_sid: u64,
        packet_wrapper: &PacketWrapper,
        _routing_header: Option<&RoutingHeader>,
    ) -> ForwardDecision {
        match packet_wrapper.write_to_bytes() {
            Ok(bytes) => ForwardDecision::Forward(Bytes::from(bytes)),
            Err(_) => ForwardDecision::Drop,
        }
    }
}

impl Default for Forwarder {
    fn default() -> Self {
        Self::new()
    }
}
