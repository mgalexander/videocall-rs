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
use web_sys::{EncodedVideoChunk, EncodedVideoChunkType};

// frame_marker bitfield constants — see ADR-0001.
pub(crate) const START_OF_FRAME: u32 = 1;
pub(crate) const END_OF_FRAME: u32 = 2;
pub(crate) const REFERENCES_T0: u32 = 4;

/// Build a `RoutingHeader` for a single WebCodecs `EncodedVideoChunk`.
///
/// Each WebCodecs chunk is one complete frame, so `START_OF_FRAME | END_OF_FRAME`
/// is always set. In L1T1 (single temporal layer) delta frames always reference T0.
/// Both camera and screen-share paths share these semantics today.
pub(crate) fn build_routing_header_from_chunk(
    chunk: &EncodedVideoChunk,
    sequence: u64,
) -> RoutingHeader {
    let is_keyframe = chunk.type_() == EncodedVideoChunkType::Key;
    let frame_marker = if is_keyframe {
        START_OF_FRAME | END_OF_FRAME
    } else {
        START_OF_FRAME | END_OF_FRAME | REFERENCES_T0
    };
    RoutingHeader {
        is_keyframe,
        temporal_layer_id: 0,
        spatial_layer_id: 0,
        frame_marker,
        picture_id: sequence,
        ..Default::default()
    }
}
