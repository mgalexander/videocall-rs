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
use super::routing::build_routing_header_from_chunk;
use crate::constants::get_video_codec;
use crate::crypto::aes::Aes128State;
use js_sys::Uint8Array;
use protobuf::Message;
use std::rc::Rc;
use videocall_types::protos::{
    media_packet::{media_packet::MediaType, MediaPacket, VideoMetadata},
    packet_wrapper::{packet_wrapper::PacketType, PacketWrapper},
};
use web_sys::EncodedVideoChunk;

pub fn buffer_to_uint8array(buf: &mut [u8]) -> Uint8Array {
    // Convert &mut [u8] to a Uint8Array
    unsafe { Uint8Array::view_mut_raw(buf.as_mut_ptr(), buf.len()) }
}

/// Camera path: VP9 SVC (`L1T3`) — `temporal_layer_id` is extracted by the
/// caller from the VideoEncoder output callback's metadata (see
/// [`crate::encode::routing::extract_temporal_layer_id`]). `is_svc=true` so
/// REFERENCES_T0 is set only for T1/T2 deltas, enabling per-receiver temporal
/// dropping at the SFU (bead vc-2nh / p4-2).
pub fn transform_video_chunk(
    chunk: EncodedVideoChunk,
    sequence: u64,
    temporal_layer_id: u8,
    buffer: &mut [u8],
    user_id: &str,
    aes: Rc<Aes128State>,
) -> PacketWrapper {
    let byte_length = chunk.byte_length() as usize;
    if let Err(e) = chunk.copy_to_with_u8_array(&buffer_to_uint8array(buffer)) {
        log::error!("Error copying video chunk: {e:?}");
    }
    let routing_header = build_routing_header_from_chunk(
        &chunk,
        temporal_layer_id,
        /* is_svc */ true,
        sequence,
    );
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

/// Screen-share path: L1T1 (single temporal layer; no `scalability_mode` set
/// on the encoder config). `is_svc=false` preserves the p1-7 invariant that
/// every delta frame sets REFERENCES_T0.
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
    let routing_header = build_routing_header_from_chunk(
        &chunk, /* temporal_layer_id */ 0, /* is_svc */ false, sequence,
    );
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
