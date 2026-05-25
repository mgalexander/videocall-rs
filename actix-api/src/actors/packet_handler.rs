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

//! Shared packet handling logic for session actors.
//!
//! This module provides common packet classification and processing
//! used by both `WsChatSession` and `WtChatSession`.

use protobuf::Message as ProtobufMessage;
use videocall_types::protos::connection_packet::ConnectionPacket;
use videocall_types::protos::media_packet::media_packet::MediaType;
use videocall_types::protos::media_packet::{MediaPacket, RoutingHeader};
use videocall_types::protos::packet_wrapper::packet_wrapper::PacketType;
use videocall_types::protos::packet_wrapper::PacketWrapper;

use crate::constants::{KEYFRAME_REQUEST_MAX_PER_SEC, KEYFRAME_REQUEST_WINDOW_MS};
use crate::sfu::layer_selector::LayerSelection;
use std::time::Instant;

use super::session_logic::SessionId;

/// Classification of an incoming packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketKind {
    /// RTT (Round-Trip Time) packet - should be echoed back to sender
    Rtt,
    /// Health diagnostics packet - should be processed for metrics
    Health,
    /// Normal data packet - should be forwarded to ChatServer
    Data,
    /// Packet that should be silently dropped (e.g., client-originated CONGESTION)
    Dropped,
    /// KEYFRAME_REQUEST packet - subject to per-session rate limiting
    KeyframeRequest,
    /// SUBSCRIPTION_UPDATE packet - a server-local control packet that is NOT
    /// broadcast to NATS. It must be applied to the per-room `SubscriptionStore`
    /// on the `ChatServer` actor thread, so it is routed through the actor
    /// rather than the off-actor media-publish fast path (vc-ud6o E3).
    SubscriptionUpdate,
}

/// Result of classifying an inbound packet, with optional parsed inner
/// payloads useful for observability without forcing the caller to re-parse.
///
/// `media_packet` is populated for any MEDIA `PacketWrapper` whose inner
/// `MediaPacket` parsed successfully (encrypted-payload parse failures yield
/// `None` and still classify as [`PacketKind::Data`]).
///
/// `connection_packet` is populated for any successfully parsed inner
/// `ConnectionPacket` carried by a CONNECTION `PacketWrapper`. CONNECTION
/// packets currently classify as [`PacketKind::Data`] (i.e. they are still
/// forwarded unchanged); this field exists purely so the caller can log the
/// advertised `client_capabilities` once per connection.
pub struct ClassifiedPacket {
    pub kind: PacketKind,
    pub media_packet: Option<MediaPacket>,
    pub connection_packet: Option<ConnectionPacket>,
}

/// Classify a packet and surface any inner payload that was already parsed
/// in the process, so callers can read fields like `routing_header` or
/// `client_capabilities` without re-parsing the bytes.
///
/// This is the production entry point used by `SessionLogic::handle_inbound`.
/// `classify_packet` is preserved as a thin wrapper that drops the extra
/// fields, used by the existing security property tests.
pub fn classify_and_inspect(data: &[u8]) -> ClassifiedPacket {
    let packet_wrapper = match PacketWrapper::parse_from_bytes(data) {
        Ok(pw) => pw,
        Err(_) => {
            // Unparseable, treat as opaque data.
            return ClassifiedPacket {
                kind: PacketKind::Data,
                media_packet: None,
                connection_packet: None,
            };
        }
    };

    // Drop client-originated CONGESTION packets.
    // CONGESTION signals must only originate from the server's CongestionTracker,
    // never from clients. A malicious client could craft a CONGESTION packet with
    // a victim's session_id to force them to degrade video quality.
    if packet_wrapper.packet_type == PacketType::CONGESTION.into() {
        return ClassifiedPacket {
            kind: PacketKind::Dropped,
            media_packet: None,
            connection_packet: None,
        };
    }

    // Check if it's a MEDIA packet (RTT, keyframe request, or regular media).
    if packet_wrapper.packet_type == PacketType::MEDIA.into() {
        // Try to parse inner MediaPacket to distinguish control sub-types.
        // For encrypted payloads this parse will fail, correctly falling
        // through to PacketKind::Data.
        let media_packet = MediaPacket::parse_from_bytes(&packet_wrapper.data).ok();
        let kind = match &media_packet {
            Some(mp) if mp.media_type == MediaType::RTT.into() => PacketKind::Rtt,
            Some(mp) if mp.media_type == MediaType::KEYFRAME_REQUEST.into() => {
                PacketKind::KeyframeRequest
            }
            _ => PacketKind::Data,
        };
        return ClassifiedPacket {
            kind,
            media_packet,
            connection_packet: None,
        };
    }

    // Check health packet.
    if packet_wrapper.packet_type == PacketType::HEALTH.into() {
        return ClassifiedPacket {
            kind: PacketKind::Health,
            media_packet: None,
            connection_packet: None,
        };
    }

    // SUBSCRIPTION_UPDATE is a server-local control packet (vc-ud6o E3): it is
    // intercepted on the `ChatServer` actor and applied to the per-room
    // `SubscriptionStore` — never broadcast to NATS. Classify it distinctly so
    // the transport's `Forward` path routes it through the actor mailbox
    // instead of the off-actor media-publish fast path. Pre-vc-ud6o this fell
    // through to `PacketKind::Data`; the actor-side `ClientMessage` handler
    // still re-checks `packet_type == SUBSCRIPTION_UPDATE` so the interception
    // semantics are unchanged.
    if packet_wrapper.packet_type == PacketType::SUBSCRIPTION_UPDATE.into() {
        return ClassifiedPacket {
            kind: PacketKind::SubscriptionUpdate,
            media_packet: None,
            connection_packet: None,
        };
    }

    // Check CONNECTION packet. CONNECTION packets are still forwarded as
    // opaque Data so peers receive the join notification; we just attempt
    // to parse the inner ConnectionPacket to expose `client_capabilities`
    // for observability. A parse failure is benign — classification still
    // falls through to PacketKind::Data.
    if packet_wrapper.packet_type == PacketType::CONNECTION.into() {
        let connection_packet = ConnectionPacket::parse_from_bytes(&packet_wrapper.data).ok();
        return ClassifiedPacket {
            kind: PacketKind::Data,
            media_packet: None,
            connection_packet,
        };
    }

    ClassifiedPacket {
        kind: PacketKind::Data,
        media_packet: None,
        connection_packet: None,
    }
}

/// Classify a packet based on its contents.
///
/// Parses the `PacketWrapper` exactly once and uses the `packet_type` field
/// to classify the packet. For MEDIA packets, the inner `MediaPacket` is
/// parsed at most once to distinguish RTT and KEYFRAME_REQUEST from regular
/// media data.
///
/// This is a thin wrapper around [`classify_and_inspect`] that discards the
/// parsed inner payloads. Production code uses [`classify_and_inspect`]
/// directly so the inner `MediaPacket` / `ConnectionPacket` can be logged
/// without re-parsing.
///
/// # Arguments
/// * `data` - Raw packet bytes
///
/// # Returns
/// The classification of the packet
pub fn classify_packet(data: &[u8]) -> PacketKind {
    classify_and_inspect(data).kind
}

/// Parsed `PacketWrapper` plus (optionally) its inner `MediaPacket`, surfaced
/// for the egress fan-out path in `ChatServer::handle_msg`.
///
/// Unlike [`ClassifiedPacket`], this struct does NOT carry a [`PacketKind`]
/// and the parser does NOT apply the client-CONGESTION drop. That drop is an
/// *ingress-only* security check enforced by [`classify_and_inspect`]: it
/// prevents a malicious client from spoofing a peer's congestion signal. By
/// the time a packet reaches egress fan-out (a per-receiver NATS subscription
/// closure), it has already passed the ingress filter — and the server itself
/// publishes CONGESTION packets on the participant's own subject so peers can
/// back off. Re-dropping CONGESTION at egress would black-hole those
/// legitimate server-published signals. See the CRITICAL carve-out in
/// `chat_server::handle_msg` (around the `SfuMode::Sfu` match arm) for the
/// matching policy on the receive side.
pub struct ParsedPacket {
    /// The outer wrapper as parsed from the wire.
    pub wrapper: PacketWrapper,
    /// Inner [`MediaPacket`], populated for MEDIA wrappers whose payload
    /// parsed successfully. Encrypted MediaPacket payloads correctly yield
    /// `None` here without affecting `wrapper`, so callers can still inspect
    /// the wrapper's packet type / session id.
    pub media_packet: Option<MediaPacket>,
}

impl ParsedPacket {
    /// Convenience accessor for the routing header, if present.
    ///
    /// Returns `None` for non-MEDIA wrappers, MEDIA wrappers whose inner
    /// payload failed to parse (e.g. encrypted), and MEDIA wrappers whose
    /// `MediaPacket` simply does not set a routing header.
    pub fn routing_header(&self) -> Option<&RoutingHeader> {
        self.media_packet
            .as_ref()
            .and_then(|mp| mp.routing_header.as_ref())
    }
}

/// Parse a packet for egress fan-out, surfacing the inner `MediaPacket` once
/// so per-receiver closures don't re-parse.
///
/// This is the egress counterpart to [`classify_and_inspect`]. It deliberately
/// performs *less* work:
///
/// - No [`PacketKind`] classification — egress fan-out does not need to
///   distinguish RTT / HEALTH / KEYFRAME_REQUEST from regular data.
/// - No inner `ConnectionPacket` parse — egress does not read
///   `client_capabilities`.
/// - **No client-CONGESTION drop.** The CONGESTION-as-Dropped rule in
///   [`classify_and_inspect`] is an ingress-only security check that prevents
///   clients from spoofing peer congestion. At egress, the server's
///   `CongestionTracker` legitimately publishes CONGESTION on a session's own
///   subject so peers can degrade quality; dropping those here would defeat
///   the feature. See the `is_congestion` carve-out in
///   `chat_server::handle_msg` for the matching receive-side policy.
///
/// Returns `None` only when the outer `PacketWrapper` itself fails to parse.
/// Encrypted MediaPacket payloads return `Some` with `media_packet: None`.
pub fn parse_and_inspect(data: &[u8]) -> Option<ParsedPacket> {
    let wrapper = PacketWrapper::parse_from_bytes(data).ok()?;
    let media_packet = if wrapper.packet_type == PacketType::MEDIA.into() {
        // Encrypted payloads parse-fail here and correctly yield `None`
        // without affecting the wrapper.
        MediaPacket::parse_from_bytes(&wrapper.data).ok()
    } else {
        None
    };
    Some(ParsedPacket {
        wrapper,
        media_packet,
    })
}

/// Layer-aware KEYFRAME_REQUEST (KFR) routing filter (p4-10).
///
/// A KFR triggers the named target sender to blast a fresh (~1.5 MB) keyframe.
/// If the requesting receiver is not currently subscribed to any layer of that
/// sender — per its cached [`LayerSelection`] — the KFR is wasted work and, on
/// a bandwidth-constrained downlink, can wedge the receiver's egress queue
/// behind a keyframe burst it will never consume.
///
/// Returns `true` when the KFR should be **dropped** before the NATS publish.
///
/// Decision rules (all "absence = pass-through", matching
/// [`crate::sfu::forwarder::Forwarder::should_drop_for_layer_budget`]):
///
/// * Empty `target_user_id_bytes` → forward (legacy KFRs without a populated
///   `MediaPacket.user_id` must not regress).
/// * `selection.is_none()` → forward (no cached selection yet; freshly-joined
///   receivers haven't reported a bandwidth estimate and still expect legacy
///   fan-out).
/// * Target user is not present in `members` → forward (target not in room;
///   the KFR will die naturally at the NATS layer — don't add a new failure
///   mode).
/// * Any matching sender `SessionId` for the target user_id appears in
///   `selection.forward` at any spatial layer → forward (receiver IS getting
///   at least T0 of this sender; the KFR is legitimately needed).
/// * Otherwise → drop (receiver is not subscribed to any layer of this
///   sender).
///
/// A receiver may briefly have multiple `SessionId`s in the room across a
/// reconnection grace window; we treat "ANY matching session in forward" as
/// subscribed.
pub fn should_drop_kfr_for_layer_selection(
    target_user_id_bytes: &[u8],
    _requester_sid: SessionId,
    members: &[(SessionId, String, String)],
    selection: Option<&LayerSelection>,
) -> bool {
    // Empty target → legacy pass-through.
    if target_user_id_bytes.is_empty() {
        return false;
    }
    // No cached selection → legacy pass-through (the same absence-rule
    // documented on `should_drop_for_layer_budget`).
    let selection = match selection {
        Some(s) => s,
        None => return false,
    };
    // Resolve target user_id → matching sender SessionId(s).
    let target_user_id = match std::str::from_utf8(target_user_id_bytes) {
        Ok(s) => s,
        Err(_) => return false,
    };
    let mut any_match = false;
    let mut any_in_forward = false;
    for (sid, user_id, _display) in members {
        if user_id == target_user_id {
            any_match = true;
            // Scan forward keys for this sender SID at any spatial layer.
            // The HashMap is small (one entry per (sender, spatial)); a linear
            // scan is cheaper than constructing a per-sender index.
            if selection
                .forward
                .keys()
                .any(|(sender_sid, _spatial)| sender_sid == sid)
            {
                any_in_forward = true;
                break;
            }
        }
    }
    if !any_match {
        // Target not in room → forward (let it die naturally).
        return false;
    }
    // Drop iff target is in the room but no matching session appears in the
    // current forward set.
    !any_in_forward
}

/// Per-session rate limiter for KEYFRAME_REQUEST packets.
///
/// Tracks the number of KEYFRAME_REQUEST packets forwarded within a sliding
/// window and drops excess requests to prevent abuse.
pub struct KeyframeRequestLimiter {
    /// Number of requests forwarded in the current window.
    count: u32,
    /// Start of the current counting window.
    window_start: Instant,
}

impl Default for KeyframeRequestLimiter {
    fn default() -> Self {
        Self::new()
    }
}

impl KeyframeRequestLimiter {
    pub fn new() -> Self {
        Self {
            count: 0,
            window_start: Instant::now(),
        }
    }

    /// Check whether a KEYFRAME_REQUEST should be allowed through.
    ///
    /// Returns `true` if the request is within the rate limit, `false` if it
    /// should be dropped. Automatically resets the window when it expires.
    pub fn allow(&mut self) -> bool {
        let now = Instant::now();
        let window = std::time::Duration::from_millis(KEYFRAME_REQUEST_WINDOW_MS);

        if now.duration_since(self.window_start) > window {
            self.count = 0;
            self.window_start = now;
        }

        if self.count < KEYFRAME_REQUEST_MAX_PER_SEC {
            self.count += 1;
            true
        } else {
            false
        }
    }
}

/// Maximum payload size for WebTransport datagrams (bytes).
///
/// Datagrams are used for control packets (heartbeats, RTT probes,
/// diagnostics) that are periodic and expendable. Media packets always use
/// reliable unidirectional streams. Control packets larger than this limit
/// also fall back to reliable streams.
///
/// Must match the client-side `DATAGRAM_MAX_SIZE` constant.
pub const DATAGRAM_MAX_SIZE: usize = 1200;

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // Test-only helper functions
    //
    // These standalone is_* functions are used only by their own unit tests.
    // Production code uses `classify_packet()` instead.
    // =========================================================================

    /// Check if a packet is a CONGESTION packet (test-only helper).
    fn is_congestion_packet(data: &[u8]) -> bool {
        if let Ok(packet_wrapper) = PacketWrapper::parse_from_bytes(data) {
            return packet_wrapper.packet_type == PacketType::CONGESTION.into();
        }
        false
    }

    /// Check if a packet is an RTT measurement packet (test-only helper).
    fn is_rtt_packet(data: &[u8]) -> bool {
        if let Ok(packet_wrapper) = PacketWrapper::parse_from_bytes(data) {
            if packet_wrapper.packet_type == PacketType::MEDIA.into() {
                if let Ok(media_packet) = MediaPacket::parse_from_bytes(&packet_wrapper.data) {
                    return media_packet.media_type == MediaType::RTT.into();
                }
            }
        }
        false
    }

    /// Check if a MEDIA packet contains a KEYFRAME_REQUEST (test-only helper).
    fn is_keyframe_request(data: &[u8]) -> bool {
        if let Ok(packet_wrapper) = PacketWrapper::parse_from_bytes(data) {
            if packet_wrapper.packet_type == PacketType::MEDIA.into() {
                if let Ok(media_packet) = MediaPacket::parse_from_bytes(&packet_wrapper.data) {
                    return media_packet.media_type == MediaType::KEYFRAME_REQUEST.into();
                }
            }
        }
        false
    }

    /// Test-only helper that replicates the datagram routing logic from
    /// `WtChatSession::send_auto`. Control packets (non-media) that fit
    /// within the datagram MTU use datagrams; media packets always use
    /// reliable streams. Empty/unparseable inputs are never routed via
    /// datagram.
    fn should_use_datagram(data: &[u8]) -> bool {
        if data.is_empty() {
            return false;
        }
        if let Ok(pw) = PacketWrapper::parse_from_bytes(data) {
            let is_media = pw.packet_type == PacketType::MEDIA.into();
            return !is_media && data.len() <= DATAGRAM_MAX_SIZE;
        }
        false
    }

    #[test]
    fn test_classify_empty_packet_as_data() {
        assert_eq!(classify_packet(&[]), PacketKind::Data);
    }

    #[test]
    fn test_classify_garbage_as_data() {
        assert_eq!(classify_packet(&[1, 2, 3, 4, 5]), PacketKind::Data);
    }

    /// vc-ud6o E3: SUBSCRIPTION_UPDATE must classify as its own control kind
    /// (NOT `Data`) so the transport routes it through the ChatServer actor
    /// rather than the off-actor media-publish fast path. The actor applies it
    /// to the per-room SubscriptionStore on the single-writer thread.
    #[test]
    fn test_classify_subscription_update_is_control_kind() {
        let wrapper = PacketWrapper {
            packet_type: PacketType::SUBSCRIPTION_UPDATE.into(),
            data: vec![1, 2, 3],
            ..Default::default()
        };
        let bytes = wrapper.write_to_bytes().unwrap();
        assert_eq!(classify_packet(&bytes), PacketKind::SubscriptionUpdate);
    }

    /// MEDIA AUDIO/VIDEO/SCREEN payloads keep classifying as `Data` so they
    /// take the off-actor publish fast path (the vc-ud6o E3 hot path).
    #[test]
    fn test_classify_opaque_media_remains_data() {
        // An encrypted MEDIA payload whose inner MediaPacket fails to parse
        // classifies as Data (the AUDIO/VIDEO/SCREEN fast path).
        let wrapper = PacketWrapper {
            packet_type: PacketType::MEDIA.into(),
            data: vec![0xff, 0xff, 0xff, 0xff],
            ..Default::default()
        };
        let bytes = wrapper.write_to_bytes().unwrap();
        assert_eq!(classify_packet(&bytes), PacketKind::Data);
    }

    #[test]
    fn test_is_rtt_packet_with_garbage() {
        assert!(!is_rtt_packet(&[1, 2, 3, 4, 5]));
    }

    #[test]
    fn test_is_rtt_packet_with_empty() {
        assert!(!is_rtt_packet(&[]));
    }

    #[test]
    fn test_should_use_datagram_empty() {
        assert!(!should_use_datagram(&[]));
    }

    #[test]
    fn test_should_use_datagram_garbage() {
        assert!(!should_use_datagram(&[1, 2, 3, 4, 5]));
    }

    #[test]
    fn test_should_use_datagram_media_packet() {
        // MEDIA packets always use reliable streams (avoids artifacts)
        let wrapper = PacketWrapper {
            packet_type: PacketType::MEDIA.into(),
            data: vec![1, 2, 3], // small payload
            ..Default::default()
        };
        let bytes = wrapper.write_to_bytes().unwrap();
        assert!(bytes.len() <= DATAGRAM_MAX_SIZE);
        assert!(!should_use_datagram(&bytes));
    }

    #[test]
    fn test_should_use_datagram_oversized_media_packet() {
        // Oversized MEDIA packets also use reliable streams
        let wrapper = PacketWrapper {
            packet_type: PacketType::MEDIA.into(),
            data: vec![0u8; DATAGRAM_MAX_SIZE + 100], // exceeds MTU
            ..Default::default()
        };
        let bytes = wrapper.write_to_bytes().unwrap();
        assert!(!should_use_datagram(&bytes));
    }

    #[test]
    fn test_should_use_datagram_non_media_packet() {
        // Small AES_KEY packets use datagrams (control, expendable)
        let wrapper = PacketWrapper {
            packet_type: PacketType::AES_KEY.into(),
            data: vec![1, 2, 3],
            ..Default::default()
        };
        let bytes = wrapper.write_to_bytes().unwrap();
        assert!(should_use_datagram(&bytes));
    }

    #[test]
    fn test_should_use_datagram_diagnostics_packet() {
        // Small DIAGNOSTICS packets use datagrams (periodic, expendable)
        let wrapper = PacketWrapper {
            packet_type: PacketType::DIAGNOSTICS.into(),
            data: vec![1, 2, 3],
            ..Default::default()
        };
        let bytes = wrapper.write_to_bytes().unwrap();
        assert!(should_use_datagram(&bytes));
    }

    #[test]
    fn test_should_use_datagram_health_packet() {
        // Small HEALTH packets use datagrams (periodic, expendable)
        let wrapper = PacketWrapper {
            packet_type: PacketType::HEALTH.into(),
            data: vec![1, 2, 3],
            ..Default::default()
        };
        let bytes = wrapper.write_to_bytes().unwrap();
        assert!(should_use_datagram(&bytes));
    }

    #[test]
    fn test_should_use_datagram_oversized_control_packet() {
        // Control packets exceeding DATAGRAM_MAX_SIZE fall back to reliable stream
        let wrapper = PacketWrapper {
            packet_type: PacketType::DIAGNOSTICS.into(),
            data: vec![0u8; DATAGRAM_MAX_SIZE + 100],
            ..Default::default()
        };
        let bytes = wrapper.write_to_bytes().unwrap();
        assert!(!should_use_datagram(&bytes));
    }

    // -------------------------------------------------------------------------
    // SECURITY PROPERTY: server-only packet types must be dropped when sent
    // by a client.
    //
    // A `PacketType` is "server-only" when it carries a control signal that
    // the *server* is the authority on — emitting it from a client either
    // bypasses a security control (e.g. CONGESTION → forced encoder
    // step-down on a victim) or impersonates the server in ways that mislead
    // peers. The SFU refactor adds more such types (SPEAKER_UPDATE,
    // LAYER_HINT, ADMISSION_DECISION); each must be added to
    // `SERVER_ONLY_PACKET_TYPES` below as it lands, and the test below
    // enforces classify_packet() drops it. See sfu-update/GAP-ANALYSIS.md
    // S-P0-2 (packet direction discipline).
    //
    // The original threat (from the existing CONGESTION case) is documented
    // in classify_packet() above: "A malicious client could craft a
    // CONGESTION packet with a victim's session_id to force them to degrade
    // video quality."
    //
    // Adding a new server-only PacketType WITHOUT extending this list is a
    // security regression. The list is intentionally explicit (not a "wildcard
    // catch") so reviewers see the addition.
    const SERVER_ONLY_PACKET_TYPES: &[PacketType] = &[
        PacketType::CONGESTION,
        // SFU refactor additions land here as their PacketTypes are added
        // to packet_wrapper.proto:
        //   - PacketType::SPEAKER_UPDATE,
        //   - PacketType::LAYER_HINT,
        //   - PacketType::ADMISSION_DECISION,
        // Each addition must also extend classify_packet() to return
        // PacketKind::Dropped for that type.
    ];

    #[test]
    fn test_classify_congestion_packet_as_dropped() {
        // Preserve the original property check for the legacy case.
        let wrapper = PacketWrapper {
            packet_type: PacketType::CONGESTION.into(),
            data: vec![1, 2, 3],
            ..Default::default()
        };
        let bytes = wrapper.write_to_bytes().unwrap();
        assert_eq!(classify_packet(&bytes), PacketKind::Dropped);
    }

    #[test]
    fn test_classify_all_server_only_packet_types_as_dropped() {
        // Property test: every PacketType listed in SERVER_ONLY_PACKET_TYPES
        // must be dropped by classify_packet, regardless of payload content.
        // When a new server-only PacketType is added to packet_wrapper.proto,
        // adding it to SERVER_ONLY_PACKET_TYPES enrolls it in this test
        // automatically. classify_packet() must also be extended to actually
        // drop it.
        for &server_type in SERVER_ONLY_PACKET_TYPES {
            for payload in [vec![], vec![0u8; 3], vec![0u8; 64], vec![0u8; 1500]] {
                let wrapper = PacketWrapper {
                    packet_type: server_type.into(),
                    data: payload.clone(),
                    session_id: 12345, // attacker-claimed victim session
                    ..Default::default()
                };
                let bytes = wrapper.write_to_bytes().unwrap();
                assert_eq!(
                    classify_packet(&bytes),
                    PacketKind::Dropped,
                    "server-only PacketType {server_type:?} with {}B payload \
                     must be dropped when sent from a client",
                    payload.len(),
                );
            }
        }
    }

    #[test]
    fn test_classify_keyframe_request() {
        let media = MediaPacket {
            media_type: MediaType::KEYFRAME_REQUEST.into(),
            ..Default::default()
        };
        let wrapper = PacketWrapper {
            packet_type: PacketType::MEDIA.into(),
            data: media.write_to_bytes().unwrap(),
            ..Default::default()
        };
        let bytes = wrapper.write_to_bytes().unwrap();
        assert_eq!(classify_packet(&bytes), PacketKind::KeyframeRequest);
    }

    /// vc-jgj: classification is **kind-driven**, not size-driven.
    ///
    /// The previous fan-out gate in `chat_server.rs` skipped the inner
    /// `MediaPacket` parse for any MEDIA wrapper larger than 256 B,
    /// assuming real KFRs were always tiny. That heuristic would have
    /// silently misclassified an oversized KFR as `PacketKind::Data`
    /// and bypassed the layer-selection drop policy. The plumbed
    /// `PacketKind` from `classify_and_inspect` must continue to
    /// identify a KEYFRAME_REQUEST as such regardless of payload size.
    #[test]
    fn test_classify_oversized_keyframe_request_is_kind_driven() {
        // 1 KiB of arbitrary bytes — well above the old 256-byte size gate.
        let padding = vec![0xABu8; 1024];
        let media = MediaPacket {
            media_type: MediaType::KEYFRAME_REQUEST.into(),
            user_id: b"target-user".to_vec(),
            // `data` is unused by KFRs in production, but a real attacker /
            // misbehaving client could attach arbitrary bytes. The test
            // intentionally exceeds the old 256 B size gate.
            data: padding,
            ..Default::default()
        };
        let wrapper = PacketWrapper {
            packet_type: PacketType::MEDIA.into(),
            data: media.write_to_bytes().unwrap(),
            ..Default::default()
        };
        let bytes = wrapper.write_to_bytes().unwrap();
        assert!(
            wrapper.data.len() > 256,
            "test invariant: inner MediaPacket must exceed the old size gate (got {} B)",
            wrapper.data.len()
        );

        // The new kind-driven branch must still classify this as a KFR,
        // proving the chat_server fan-out path will enter the
        // layer-selection drop branch rather than the AUDIO/VIDEO/SCREEN
        // forwarding path.
        let classified = classify_and_inspect(&bytes);
        assert_eq!(
            classified.kind,
            PacketKind::KeyframeRequest,
            "oversized KFR must be classified by media_type, not by wrapper size"
        );

        // Negative case: the SAME oversized payload but with media_type=VIDEO
        // classifies as Data — proving the gate is media_type-driven, not size.
        let video = MediaPacket {
            media_type: MediaType::VIDEO.into(),
            user_id: b"target-user".to_vec(),
            data: vec![0xABu8; 1024],
            ..Default::default()
        };
        let video_wrapper = PacketWrapper {
            packet_type: PacketType::MEDIA.into(),
            data: video.write_to_bytes().unwrap(),
            ..Default::default()
        };
        let video_bytes = video_wrapper.write_to_bytes().unwrap();
        assert_eq!(
            classify_and_inspect(&video_bytes).kind,
            PacketKind::Data,
            "oversized VIDEO must not be classified as KeyframeRequest"
        );
    }

    #[test]
    fn test_classify_rtt_packet() {
        let media = MediaPacket {
            media_type: MediaType::RTT.into(),
            ..Default::default()
        };
        let wrapper = PacketWrapper {
            packet_type: PacketType::MEDIA.into(),
            data: media.write_to_bytes().unwrap(),
            ..Default::default()
        };
        let bytes = wrapper.write_to_bytes().unwrap();
        assert_eq!(classify_packet(&bytes), PacketKind::Rtt);
    }

    #[test]
    fn test_classify_health_packet() {
        let wrapper = PacketWrapper {
            packet_type: PacketType::HEALTH.into(),
            data: vec![1, 2, 3],
            ..Default::default()
        };
        let bytes = wrapper.write_to_bytes().unwrap();
        assert_eq!(classify_packet(&bytes), PacketKind::Health);
    }

    #[test]
    fn test_classify_regular_media_as_data() {
        let media = MediaPacket {
            media_type: MediaType::VIDEO.into(),
            ..Default::default()
        };
        let wrapper = PacketWrapper {
            packet_type: PacketType::MEDIA.into(),
            data: media.write_to_bytes().unwrap(),
            ..Default::default()
        };
        let bytes = wrapper.write_to_bytes().unwrap();
        assert_eq!(classify_packet(&bytes), PacketKind::Data);
    }

    #[test]
    fn test_is_congestion_packet_true() {
        let wrapper = PacketWrapper {
            packet_type: PacketType::CONGESTION.into(),
            ..Default::default()
        };
        let bytes = wrapper.write_to_bytes().unwrap();
        assert!(is_congestion_packet(&bytes));
    }

    #[test]
    fn test_is_congestion_packet_false_for_media() {
        let wrapper = PacketWrapper {
            packet_type: PacketType::MEDIA.into(),
            ..Default::default()
        };
        let bytes = wrapper.write_to_bytes().unwrap();
        assert!(!is_congestion_packet(&bytes));
    }

    #[test]
    fn test_is_keyframe_request_with_valid_packet() {
        let media = MediaPacket {
            media_type: MediaType::KEYFRAME_REQUEST.into(),
            ..Default::default()
        };
        let wrapper = PacketWrapper {
            packet_type: PacketType::MEDIA.into(),
            data: media.write_to_bytes().unwrap(),
            ..Default::default()
        };
        let bytes = wrapper.write_to_bytes().unwrap();
        assert!(is_keyframe_request(&bytes));
    }

    #[test]
    fn test_is_keyframe_request_false_for_video() {
        let media = MediaPacket {
            media_type: MediaType::VIDEO.into(),
            ..Default::default()
        };
        let wrapper = PacketWrapper {
            packet_type: PacketType::MEDIA.into(),
            data: media.write_to_bytes().unwrap(),
            ..Default::default()
        };
        let bytes = wrapper.write_to_bytes().unwrap();
        assert!(!is_keyframe_request(&bytes));
    }

    #[test]
    fn test_is_keyframe_request_false_for_non_media() {
        let wrapper = PacketWrapper {
            packet_type: PacketType::AES_KEY.into(),
            ..Default::default()
        };
        let bytes = wrapper.write_to_bytes().unwrap();
        assert!(!is_keyframe_request(&bytes));
    }

    #[test]
    fn test_keyframe_request_limiter_allows_within_limit() {
        let mut limiter = KeyframeRequestLimiter::new();
        assert!(limiter.allow());
        assert!(limiter.allow());
    }

    // -------------------------------------------------------------------------
    // classify_and_inspect: surfaces parsed inner payloads for observability
    // -------------------------------------------------------------------------

    #[test]
    fn test_inspect_media_packet_with_routing_header() {
        use videocall_types::protos::media_packet::RoutingHeader;

        let routing_header = RoutingHeader {
            is_keyframe: true,
            temporal_layer_id: 2,
            spatial_layer_id: 1,
            audio_level: 0.75,
            is_speaking: true,
            ..Default::default()
        };
        let media = MediaPacket {
            media_type: MediaType::VIDEO.into(),
            routing_header: ::protobuf::MessageField::some(routing_header),
            ..Default::default()
        };
        let wrapper = PacketWrapper {
            packet_type: PacketType::MEDIA.into(),
            data: media.write_to_bytes().unwrap(),
            ..Default::default()
        };
        let bytes = wrapper.write_to_bytes().unwrap();

        let classified = classify_and_inspect(&bytes);
        assert_eq!(classified.kind, PacketKind::Data);
        let mp = classified
            .media_packet
            .expect("MediaPacket should be surfaced for MEDIA wrappers");
        let rh = mp
            .routing_header
            .as_ref()
            .expect("routing_header should be present");
        assert!(rh.is_keyframe);
        assert_eq!(rh.temporal_layer_id, 2);
        assert_eq!(rh.spatial_layer_id, 1);
        assert!((rh.audio_level - 0.75).abs() < f32::EPSILON);
        assert!(rh.is_speaking);
        assert!(classified.connection_packet.is_none());
    }

    #[test]
    fn test_inspect_connection_packet_with_capabilities() {
        let conn = ConnectionPacket {
            meeting_id: "room-42".to_string(),
            client_capabilities: Some(5),
            ..Default::default()
        };
        let wrapper = PacketWrapper {
            packet_type: PacketType::CONNECTION.into(),
            data: conn.write_to_bytes().unwrap(),
            ..Default::default()
        };
        let bytes = wrapper.write_to_bytes().unwrap();

        let classified = classify_and_inspect(&bytes);
        // CONNECTION packets must still be forwarded as Data so peers see them.
        assert_eq!(classified.kind, PacketKind::Data);
        let cp = classified
            .connection_packet
            .expect("ConnectionPacket should be surfaced for CONNECTION wrappers");
        assert_eq!(cp.meeting_id, "room-42");
        assert_eq!(cp.client_capabilities, Some(5));
        assert!(classified.media_packet.is_none());
    }

    // -------------------------------------------------------------------------
    // parse_and_inspect: egress-side parser, NO client-CONGESTION drop
    // -------------------------------------------------------------------------

    #[test]
    fn test_parse_and_inspect_returns_none_for_garbage() {
        assert!(parse_and_inspect(&[1, 2, 3, 4, 5]).is_none());
    }

    #[test]
    fn test_parse_and_inspect_returns_none_for_empty() {
        // An empty input *is* a valid empty PacketWrapper for protobuf, so we
        // still get Some — but `wrapper.packet_type` defaults to RELIABLE
        // (the proto default), and `media_packet` is None. Document that
        // here: `None` only occurs for truly malformed wire bytes, not for
        // empty input.
        let parsed = parse_and_inspect(&[]);
        assert!(parsed.is_some());
        assert!(parsed.unwrap().media_packet.is_none());
    }

    #[test]
    fn test_parse_and_inspect_non_media_wrapper_has_no_media_packet() {
        for pt in [PacketType::HEALTH, PacketType::AES_KEY] {
            let wrapper = PacketWrapper {
                packet_type: pt.into(),
                data: vec![1, 2, 3],
                ..Default::default()
            };
            let bytes = wrapper.write_to_bytes().unwrap();
            let parsed = parse_and_inspect(&bytes).expect("wrapper parses");
            assert_eq!(parsed.wrapper.packet_type, pt.into());
            assert!(parsed.media_packet.is_none());
            assert!(parsed.routing_header().is_none());
        }
    }

    #[test]
    fn test_parse_and_inspect_media_with_routing_header() {
        use videocall_types::protos::media_packet::RoutingHeader;

        let routing_header = RoutingHeader {
            is_keyframe: true,
            temporal_layer_id: 2,
            spatial_layer_id: 1,
            audio_level: 0.5,
            is_speaking: true,
            ..Default::default()
        };
        let media = MediaPacket {
            media_type: MediaType::VIDEO.into(),
            routing_header: ::protobuf::MessageField::some(routing_header),
            ..Default::default()
        };
        let wrapper = PacketWrapper {
            packet_type: PacketType::MEDIA.into(),
            data: media.write_to_bytes().unwrap(),
            ..Default::default()
        };
        let bytes = wrapper.write_to_bytes().unwrap();

        let parsed = parse_and_inspect(&bytes).expect("wrapper parses");
        assert_eq!(parsed.wrapper.packet_type, PacketType::MEDIA.into());
        let mp = parsed
            .media_packet
            .as_ref()
            .expect("inner MediaPacket parses");
        assert_eq!(mp.media_type, MediaType::VIDEO.into());
        let rh = parsed
            .routing_header()
            .expect("routing_header surfaced via accessor");
        assert!(rh.is_keyframe);
        assert_eq!(rh.temporal_layer_id, 2);
        assert_eq!(rh.spatial_layer_id, 1);
        assert!((rh.audio_level - 0.5).abs() < f32::EPSILON);
        assert!(rh.is_speaking);
    }

    #[test]
    fn test_parse_and_inspect_media_with_unparseable_inner_payload() {
        // Encrypted-payload analog: outer MEDIA wrapper parses, inner bytes
        // are not a valid MediaPacket. The wrapper must still surface;
        // `media_packet` is None; `routing_header()` is None.
        //
        // We construct unparseable inner bytes by using a leading byte that
        // is not a valid protobuf tag for MediaPacket fields (0xff repeated).
        let wrapper = PacketWrapper {
            packet_type: PacketType::MEDIA.into(),
            data: vec![0xff, 0xff, 0xff, 0xff],
            ..Default::default()
        };
        let bytes = wrapper.write_to_bytes().unwrap();
        let parsed = parse_and_inspect(&bytes).expect("outer wrapper parses");
        assert_eq!(parsed.wrapper.packet_type, PacketType::MEDIA.into());
        assert!(
            parsed.media_packet.is_none(),
            "encrypted/unparseable inner payload must yield media_packet: None"
        );
        assert!(parsed.routing_header().is_none());
    }

    #[test]
    fn test_parse_and_inspect_congestion_is_not_dropped() {
        // CRITICAL contract: at egress, CONGESTION must reach receivers.
        // The CONGESTION-as-Dropped rule in classify_and_inspect is an
        // *ingress-only* security check — it stops a client from spoofing a
        // peer's congestion signal. The server's CongestionTracker
        // legitimately publishes CONGESTION on a session's own subject so
        // peers can back off. parse_and_inspect runs at egress and MUST NOT
        // apply the client-CONGESTION drop, or those server-published
        // signals would never reach the wire.
        //
        // See `chat_server::handle_msg` for the matching CONGESTION
        // carve-out on the receive side.
        let wrapper = PacketWrapper {
            packet_type: PacketType::CONGESTION.into(),
            data: vec![1, 2, 3],
            session_id: 99,
            ..Default::default()
        };
        let bytes = wrapper.write_to_bytes().unwrap();
        let parsed = parse_and_inspect(&bytes).expect("wrapper parses");
        assert_eq!(parsed.wrapper.packet_type, PacketType::CONGESTION.into());
        assert!(parsed.media_packet.is_none());
    }

    #[test]
    fn test_keyframe_request_limiter_blocks_over_limit() {
        let mut limiter = KeyframeRequestLimiter::new();
        // Exhaust the limit
        for _ in 0..KEYFRAME_REQUEST_MAX_PER_SEC {
            assert!(limiter.allow());
        }
        // Next one should be blocked
        assert!(!limiter.allow());
    }

    // -------------------------------------------------------------------------
    // should_drop_kfr_for_layer_selection (p4-10)
    // -------------------------------------------------------------------------

    fn member(sid: SessionId, user_id: &str) -> (SessionId, String, String) {
        (sid, user_id.to_string(), user_id.to_string())
    }

    fn selection_with(pairs: &[(SessionId, u32, u32)]) -> LayerSelection {
        let mut s = LayerSelection::new();
        for &(sid, spatial, max_t) in pairs {
            s.forward.insert((sid, spatial), max_t);
        }
        s
    }

    #[test]
    fn test_drop_kfr_empty_target_passes_through() {
        let sel = LayerSelection::new();
        assert!(!should_drop_kfr_for_layer_selection(
            b"",
            42,
            &[member(1, "alice")],
            Some(&sel),
        ));
    }

    #[test]
    fn test_drop_kfr_no_selection_passes_through() {
        assert!(!should_drop_kfr_for_layer_selection(
            b"alice",
            42,
            &[member(1, "alice")],
            None,
        ));
    }

    #[test]
    fn test_drop_kfr_target_not_in_members_passes_through() {
        let sel = selection_with(&[(1, 0, 1)]);
        assert!(!should_drop_kfr_for_layer_selection(
            b"ghost",
            42,
            &[member(1, "alice")],
            Some(&sel),
        ));
    }

    #[test]
    fn test_drop_kfr_target_in_forward_t0_only_forwards() {
        // T0-only counts: any presence in forward (at any spatial layer)
        // means "subscribed".
        let sel = selection_with(&[(1, 0, 0)]); // T0 only on spatial 0
        assert!(!should_drop_kfr_for_layer_selection(
            b"alice",
            42,
            &[member(1, "alice")],
            Some(&sel),
        ));
    }

    #[test]
    fn test_drop_kfr_target_in_forward_t0_t1_forwards() {
        let sel = selection_with(&[(1, 0, 1)]); // T0+T1
        assert!(!should_drop_kfr_for_layer_selection(
            b"alice",
            42,
            &[member(1, "alice")],
            Some(&sel),
        ));
    }

    #[test]
    fn test_drop_kfr_target_not_in_forward_drops() {
        // alice (sid=1) is in the room but NOT in forward.
        // bob (sid=2) is in forward.
        let sel = selection_with(&[(2, 0, 1)]);
        assert!(should_drop_kfr_for_layer_selection(
            b"alice",
            42,
            &[member(1, "alice"), member(2, "bob")],
            Some(&sel),
        ));
    }

    #[test]
    fn test_drop_kfr_multiple_sids_one_in_forward_forwards() {
        // During reconnection grace, a single user may have two SIDs in
        // room_members. If ANY of them appears in forward, treat as
        // subscribed and let the KFR through.
        let sel = selection_with(&[(2, 0, 0)]);
        assert!(!should_drop_kfr_for_layer_selection(
            b"alice",
            42,
            &[member(1, "alice"), member(2, "alice")],
            Some(&sel),
        ));
    }

    #[test]
    fn test_drop_kfr_invalid_utf8_target_passes_through() {
        // Defensive: non-UTF8 user_id bytes (malformed client) must not
        // panic or wrongly drop. Treat as "can't match" → forward.
        let sel = selection_with(&[(1, 0, 0)]);
        let bad = [0xff, 0xfe, 0xfd];
        assert!(!should_drop_kfr_for_layer_selection(
            &bad,
            42,
            &[member(1, "alice")],
            Some(&sel),
        ));
    }
}
