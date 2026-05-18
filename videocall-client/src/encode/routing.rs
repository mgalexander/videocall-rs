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

use videocall_types::protos::media_packet::RoutingHeader;
use web_sys::{EncodedVideoChunk, EncodedVideoChunkMetadata, EncodedVideoChunkType};

// frame_marker bitfield constants — see ADR-0001.
pub(crate) const START_OF_FRAME: u32 = 1;
pub(crate) const END_OF_FRAME: u32 = 2;
pub(crate) const REFERENCES_T0: u32 = 4;

/// Compute the `frame_marker` bitfield for a single WebCodecs-produced frame.
///
/// Each WebCodecs chunk is one complete frame, so `START_OF_FRAME | END_OF_FRAME`
/// is always set. The `REFERENCES_T0` bit indicates that the frame depends on
/// the T0 base layer chain:
///
///   * Keyframes — never set (self-contained, no inter-frame deps).
///   * SVC delta (L1T3, `is_svc=true`):
///       - T0 (`temporal_layer_id == 0`): NOT set — these frames *are* the T0
///         chain, they reference prior T0 frames implicitly.
///       - T1/T2 (`temporal_layer_id > 0`): set — these frames are dropped by
///         the SFU when a receiver subsumes a lower temporal layer, so they
///         must be flagged as depending on the T0 chain.
///   * Non-SVC delta (L1T1, `is_svc=false`): always set — every delta references
///     the prior T0/key frame, matching pre-SVC behavior (bead p1-7).
///
/// This is a pure-Rust helper to keep the bit logic unit-testable without a
/// WebCodecs runtime.
pub(crate) fn compute_frame_marker(is_keyframe: bool, temporal_layer_id: u8, is_svc: bool) -> u32 {
    let base = START_OF_FRAME | END_OF_FRAME;
    if is_keyframe {
        base
    } else if is_svc {
        if temporal_layer_id > 0 {
            base | REFERENCES_T0
        } else {
            base
        }
    } else {
        // L1T1: every delta references T0 — preserve p1-7 semantics.
        base | REFERENCES_T0
    }
}

/// Build a `RoutingHeader` from already-decoded scalar inputs.
///
/// Pure-Rust helper so unit tests can exercise the full header construction
/// without a real `EncodedVideoChunk`.
pub(crate) fn build_routing_header(
    is_keyframe: bool,
    temporal_layer_id: u8,
    is_svc: bool,
    sequence: u64,
) -> RoutingHeader {
    RoutingHeader {
        is_keyframe,
        temporal_layer_id: temporal_layer_id as u32,
        // L1T3 is single-spatial-layer; no spatial scalability today.
        spatial_layer_id: 0,
        frame_marker: compute_frame_marker(is_keyframe, temporal_layer_id, is_svc),
        picture_id: sequence,
        ..Default::default()
    }
}

/// Build a `RoutingHeader` for a single WebCodecs `EncodedVideoChunk`.
///
/// `temporal_layer_id` is extracted by the caller from the VideoEncoder output
/// callback's `EncodedVideoChunkMetadata.svc.temporalLayerId` field (callers
/// that don't enable SVC pass 0). `is_svc=true` means the producing encoder
/// is configured with a scalability mode such as `L1T3` (camera); `is_svc=false`
/// means single-layer (screen-share today).
pub(crate) fn build_routing_header_from_chunk(
    chunk: &EncodedVideoChunk,
    temporal_layer_id: u8,
    is_svc: bool,
    sequence: u64,
) -> RoutingHeader {
    let is_keyframe = chunk.type_() == EncodedVideoChunkType::Key;
    build_routing_header(is_keyframe, temporal_layer_id, is_svc, sequence)
}

/// Best-effort extraction of `metadata.svc.temporalLayerId` from a WebCodecs
/// VideoEncoder `output` callback's metadata argument.
///
/// Returns 0 when the field is missing (e.g., L1T1 encoder configuration or
/// browser builds that omit SVC metadata). This is the safe default — it
/// degrades cleanly to "everything is T0", which preserves correctness even
/// if the SFU later applies temporal-layer filtering.
pub(crate) fn extract_temporal_layer_id(metadata: &EncodedVideoChunkMetadata) -> u8 {
    metadata
        .get_svc()
        .and_then(|svc| svc.get_temporal_layer_id())
        // Spec values are 0..=2 for L1T3; clamp defensively in case a UA emits
        // an out-of-range value rather than silently truncating bits.
        .map(|v| v.min(u8::MAX as u32) as u8)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    //! Unit tests for the pure-Rust routing-header helpers (bead vc-2nh).
    //!
    //! These deliberately avoid `web_sys`/`EncodedVideoChunk` so the bit-field
    //! logic is exercised without a WebCodecs runtime. They follow the same
    //! `#[cfg(test)]` pattern as `audio_level_tests` in `microphone_encoder.rs`.
    use super::{
        build_routing_header, compute_frame_marker, END_OF_FRAME, REFERENCES_T0, START_OF_FRAME,
    };

    const BOTH_ENDS: u32 = START_OF_FRAME | END_OF_FRAME;

    #[test]
    fn keyframe_never_references_t0() {
        // Both SVC and non-SVC keyframes are self-contained.
        assert_eq!(compute_frame_marker(true, 0, true), BOTH_ENDS);
        assert_eq!(compute_frame_marker(true, 0, false), BOTH_ENDS);
        // Even if an upstream weirdly tagged a key as T2, treat it as self-contained.
        assert_eq!(compute_frame_marker(true, 2, true), BOTH_ENDS);
    }

    #[test]
    fn svc_t0_delta_does_not_reference_t0_bit() {
        // T0 delta frames *are* the T0 chain — no REFERENCES_T0 bit.
        let m = compute_frame_marker(false, 0, true);
        assert_eq!(m, BOTH_ENDS);
        assert_eq!(m & REFERENCES_T0, 0);
    }

    #[test]
    fn svc_t1_and_t2_delta_set_references_t0() {
        for tl in [1u8, 2u8] {
            let m = compute_frame_marker(false, tl, true);
            assert_eq!(m & REFERENCES_T0, REFERENCES_T0, "tl={tl} missing bit");
            assert_eq!(m & BOTH_ENDS, BOTH_ENDS);
        }
    }

    #[test]
    fn non_svc_delta_always_sets_references_t0() {
        // L1T1 path (screen-share): preserve p1-7 behavior.
        let m = compute_frame_marker(false, 0, false);
        assert_eq!(m, BOTH_ENDS | REFERENCES_T0);
    }

    #[test]
    fn build_header_for_t2_delta_populates_all_fields() {
        // Bead vc-2nh acceptance: T2 delta frame must surface temporal_layer_id=2
        // and REFERENCES_T0 in frame_marker so the SFU can drop it.
        let h = build_routing_header(false, 2, true, 42);
        assert!(!h.is_keyframe);
        assert_eq!(h.temporal_layer_id, 2);
        assert_eq!(h.spatial_layer_id, 0);
        assert_eq!(h.picture_id, 42);
        assert_eq!(h.frame_marker & REFERENCES_T0, REFERENCES_T0);
        assert_eq!(h.frame_marker & START_OF_FRAME, START_OF_FRAME);
        assert_eq!(h.frame_marker & END_OF_FRAME, END_OF_FRAME);
    }

    #[test]
    fn build_header_for_svc_t0_delta_omits_references_t0() {
        let h = build_routing_header(false, 0, true, 7);
        assert_eq!(h.temporal_layer_id, 0);
        assert_eq!(h.frame_marker & REFERENCES_T0, 0);
    }

    #[test]
    fn build_header_for_keyframe() {
        let h = build_routing_header(true, 0, true, 1);
        assert!(h.is_keyframe);
        assert_eq!(h.temporal_layer_id, 0);
        assert_eq!(h.frame_marker, START_OF_FRAME | END_OF_FRAME);
    }
}
