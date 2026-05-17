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

use videocall_types::protos::media_packet::RoutingHeader;
use videocall_types::protos::packet_wrapper::packet_wrapper::PacketType;
use videocall_types::protos::packet_wrapper::PacketWrapper;

use crate::actors::session_logic::SessionId;
use crate::metrics::{
    SFU_DECIDE_LATENCY_US, SFU_DROPPED_TOTAL, SFU_FORWARDED_TOTAL, SFU_ROOM_SIZE,
};
use crate::sfu::room_state::RoomState;

/// Map a `PacketWrapper.packet_type` (an `EnumOrUnknown<PacketType>`) to a
/// stable lowercase string suitable for use as a Prometheus label value.
///
/// Unknown / unrecognized enum values map to `"unknown"`.
fn packet_type_label(pw: &PacketWrapper) -> &'static str {
    match pw.packet_type.enum_value_or_default() {
        PacketType::PACKET_TYPE_UNKNOWN => "unknown",
        PacketType::RSA_PUB_KEY => "rsa_pub_key",
        PacketType::AES_KEY => "aes_key",
        PacketType::MEDIA => "media",
        PacketType::CONNECTION => "connection",
        PacketType::DIAGNOSTICS => "diagnostics",
        PacketType::HEALTH => "health",
        PacketType::MEETING => "meeting",
        PacketType::SESSION_ASSIGNED => "session_assigned",
        PacketType::CONGESTION => "congestion",
        PacketType::SUBSCRIPTION_UPDATE => "subscription_update",
        PacketType::SPEAKER_UPDATE => "speaker_update",
        PacketType::LAYER_HINT => "layer_hint",
        PacketType::ADMISSION_DECISION => "admission_decision",
        PacketType::CAPABILITY_ANNOUNCE => "capability_announce",
    }
}

/// Decision returned by the [`Forwarder`] for a single (packet, receiver) pair.
///
/// The pass-through hot path no longer carries bytes: the caller already
/// has the on-wire NATS payload in scope and reuses it directly on
/// `Forward`, avoiding a per-receiver re-serialization of an identical
/// `PacketWrapper`.
pub enum ForwardDecision {
    /// Forward to the receiver. The caller is responsible for supplying
    /// the bytes (typically the original NATS payload — no re-encoding).
    Forward,
    /// Drop the packet for this receiver.
    Drop,
}

/// Per-room forwarder.
///
/// Holds a shared handle to the authoritative [`RoomState`]. `decide` is
/// invoked from each receiver's NATS subscription callback and takes a
/// read lock on the room only long enough to evaluate policy — callers
/// must keep that work cheap.
///
/// Wave-3 implements *pass-through* semantics: every packet is forwarded
/// except for the sender's own echo (self-skip). Real per-receiver
/// filtering (subscription, allow-set, layer selection) lands in later
/// phases. The decision carries no bytes — the call site reuses the
/// original NATS payload, so the pass-through path performs zero
/// per-receiver serialization.
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
    /// Otherwise returns [`ForwardDecision::Forward`]; the caller is
    /// expected to write the original on-wire bytes to the receiver — no
    /// re-encoding occurs in this function.
    pub fn decide(
        &self,
        receiver_sid: SessionId,
        packet_wrapper: &PacketWrapper,
        _routing_header: Option<&RoutingHeader>,
    ) -> ForwardDecision {
        let start = std::time::Instant::now();

        // Tight read-lock scope: only the policy decision (self-skip) and
        // a room-size gauge refresh happen under the lock. Counters and
        // the latency histogram are updated *after* the guard is dropped
        // to keep the critical section as small as possible. The lock is
        // poison-safe: a panicked writer leaves the room readable.
        let is_self = {
            let room = match self.room.read() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };

            // Refresh room-size gauge under the read lock.
            SFU_ROOM_SIZE
                .with_label_values(&[room.room_id.as_str()])
                .set(room.member_count() as f64);

            packet_wrapper.session_id == receiver_sid
        };

        let decision = if is_self {
            SFU_DROPPED_TOTAL.with_label_values(&["self_skip"]).inc();
            ForwardDecision::Drop
        } else {
            SFU_FORWARDED_TOTAL
                .with_label_values(&[packet_type_label(packet_wrapper)])
                .inc();
            ForwardDecision::Forward
        };

        let elapsed_us = start.elapsed().as_micros() as f64;
        SFU_DECIDE_LATENCY_US.observe(elapsed_us);

        decision
    }
}
