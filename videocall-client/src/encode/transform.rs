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

use super::super::wrappers::EncodedVideoChunkTypeWrapper;
use crate::constants::get_video_codec;
use crate::crypto::aes::Aes128State;
use js_sys::Uint8Array;
use protobuf::Message;
use std::rc::Rc;
use videocall_types::protos::{
    media_packet::{media_packet::MediaType, MediaPacket, RoutingHeader, VideoMetadata},
    packet_wrapper::{packet_wrapper::PacketType, PacketWrapper},
};
use web_sys::{EncodedVideoChunk, EncodedVideoChunkType};

pub fn buffer_to_uint8array(buf: &mut [u8]) -> Uint8Array {
    // Convert &mut [u8] to a Uint8Array
    unsafe { Uint8Array::view_mut_raw(buf.as_mut_ptr(), buf.len()) }
}

// frame_marker bitfield constants — see ADR-0001.
const START_OF_FRAME: u32 = 1;
const END_OF_FRAME: u32 = 2;
const REFERENCES_T0: u32 = 4;

/// Build a `RoutingHeader` for a single WebCodecs `EncodedVideoChunk`.
///
/// Each WebCodecs chunk is one complete frame, so `START_OF_FRAME | END_OF_FRAME`
/// is always set. In L1T1 (single temporal layer) delta frames always reference T0.
/// Both camera and screen-share paths share these semantics today.
fn build_routing_header_from_chunk(chunk: &EncodedVideoChunk, sequence: u64) -> RoutingHeader {
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

pub fn transform_video_chunk(
    chunk: EncodedVideoChunk,
    sequence: u64,
    buffer: &mut [u8],
    user_id: &str,
    aes: Rc<Aes128State>,
) -> PacketWrapper {
    let byte_length = chunk.byte_length() as usize;
    if let Err(e) = chunk.copy_to_with_u8_array(&buffer_to_uint8array(buffer)) {
        log::error!("Error copying video chunk: {e:?}");
    }
    let routing_header = build_routing_header_from_chunk(&chunk, sequence);
    let mut media_packet: MediaPacket = MediaPacket {
        data: buffer[0..byte_length].to_vec(),
        frame_type: EncodedVideoChunkTypeWrapper(chunk.type_()).to_string(),
        user_id: Vec::new(),
        media_type: MediaType::VIDEO.into(),
        timestamp: chunk.timestamp(),
        video_metadata: Some(VideoMetadata {
            sequence,
            codec: get_video_codec().into(),
            ..Default::default()
        })
        .into(),
        routing_header: Some(routing_header).into(),
        ..Default::default()
    };
    if let Some(duration0) = chunk.duration() {
        media_packet.duration = duration0;
    }
    let data = media_packet.write_to_bytes().unwrap();
    let data = aes.encrypt(&data).unwrap();
    PacketWrapper {
        data,
        user_id: user_id.as_bytes().to_vec(),
        packet_type: PacketType::MEDIA.into(),
        ..Default::default()
    }
}

pub fn transform_screen_chunk(
    chunk: EncodedVideoChunk,
    sequence: u64,
    buffer: &mut [u8],
    user_id: &str,
    aes: Rc<Aes128State>,
) -> PacketWrapper {
    let byte_length = chunk.byte_length() as usize;
    if let Err(e) = chunk.copy_to_with_u8_array(&buffer_to_uint8array(buffer)) {
        log::error!("Error copying video chunk: {e:?}");
    }
    let routing_header = build_routing_header_from_chunk(&chunk, sequence);
    let mut media_packet: MediaPacket = MediaPacket {
        user_id: Vec::new(),
        data: buffer[0..byte_length].to_vec(),
        frame_type: EncodedVideoChunkTypeWrapper(chunk.type_()).to_string(),
        media_type: MediaType::SCREEN.into(),
        timestamp: chunk.timestamp(),
        video_metadata: Some(VideoMetadata {
            sequence,
            codec: get_video_codec().into(),
            ..Default::default()
        })
        .into(),
        routing_header: Some(routing_header).into(),
        ..Default::default()
    };
    if let Some(duration0) = chunk.duration() {
        media_packet.duration = duration0;
    }
    let data = media_packet.write_to_bytes().unwrap();
    let data = aes.encrypt(&data).unwrap();
    PacketWrapper {
        data,
        user_id: user_id.as_bytes().to_vec(),
        packet_type: PacketType::MEDIA.into(),
        ..Default::default()
    }
}
