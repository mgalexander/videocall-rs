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

use crate::stats::BotStats;
use crate::video_encoder::VideoEncoderBuilder;
use image::imageops::FilterType;
use image::{ImageBuffer, ImageReader, Rgb};
use protobuf::Message;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::Sender;
use tracing::{debug, error, info, trace, warn};
use videocall_types::protos::media_packet::media_packet::MediaType;
use videocall_types::protos::media_packet::{MediaPacket, VideoCodec, VideoMetadata};
use videocall_types::protos::packet_wrapper::packet_wrapper::PacketType;
use videocall_types::protos::packet_wrapper::PacketWrapper;

// Real VP9 encoder - exactly same approach as videocall-cli

pub struct VideoProducer {
    #[allow(dead_code)]
    user_id: String,
    quit: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl VideoProducer {
    /// Spawn the video producer thread.
    ///
    /// `force_keyframe` is a shared flag the bot's inbound consumer flips to
    /// `true` when it observes an inbound `KEYFRAME_REQUEST` targeted at this
    /// sender (vc-7zjq). The producer checks-and-clears it each iteration and,
    /// when set, forces a keyframe on the next encode. Pass a fresh
    /// `Arc::new(AtomicBool::new(false))` if KFR honoring is not wired up.
    pub fn from_image_sequence(
        user_id: String,
        image_dir: &str,
        packet_sender: Sender<Vec<u8>>,
        stats: Option<Arc<BotStats>>,
        verify_integrity: bool,
        force_keyframe: Arc<AtomicBool>,
    ) -> anyhow::Result<Self> {
        let quit = Arc::new(AtomicBool::new(false));
        let quit_clone = quit.clone();
        let user_id_clone = user_id.clone();
        let image_dir = image_dir.to_string();

        let handle = thread::spawn(move || {
            if let Err(e) = Self::video_loop(
                user_id_clone,
                &image_dir,
                packet_sender,
                quit_clone,
                stats,
                verify_integrity,
                force_keyframe,
            ) {
                error!("Video producer error: {}", e);
            }
        });

        Ok(VideoProducer {
            user_id,
            quit,
            handle: Some(handle),
        })
    }

    fn video_loop(
        user_id: String,
        image_dir: &str,
        packet_sender: Sender<Vec<u8>>,
        quit: Arc<AtomicBool>,
        stats: Option<Arc<BotStats>>,
        verify_integrity: bool,
        force_keyframe: Arc<AtomicBool>,
    ) -> anyhow::Result<()> {
        // Video configuration - targeting 30fps (~33ms packets)
        let width = 1280u32;
        let height = 720u32;
        let framerate = 30u32;
        let packet_interval = Duration::from_millis(1000 / framerate as u64);

        info!(
            "Video producer started for {} ({}x{} @ {}fps)",
            user_id, width, height, framerate
        );

        // Load image sequence (using videocall-cli pattern)
        let mut frames = Vec::new();
        for i in 120..125 {
            let path = format!("{image_dir}/output_{i}.jpg");
            match std::fs::read(&path) {
                Ok(img_data) => {
                    let img = ImageReader::new(std::io::Cursor::new(img_data))
                        .with_guessed_format()?
                        .decode()?;

                    // Resize and convert to I420 format
                    let img = img.resize_exact(width, height, FilterType::Nearest);
                    let img = img.to_rgb8();
                    let i420_data = rgb_to_i420(&img);
                    frames.push(i420_data);
                    debug!("Loaded frame: {}", path);
                }
                Err(e) => {
                    warn!("Failed to load frame {}: {}", path, e);
                }
            }
        }

        if frames.is_empty() {
            return Err(anyhow::anyhow!("No frames loaded from {image_dir}"));
        }

        info!("Loaded {} frames for {}", frames.len(), user_id);

        // Initialize VP9 encoder (exactly same as videocall-cli)
        let mut video_encoder = VideoEncoderBuilder::new(framerate, 5) // cpu_used=5 like videocall-cli
            .set_resolution(width, height)
            .build()?;
        video_encoder.update_bitrate_kbps(500)?; // 500kbps default like videocall-cli

        let mut frame_iterator = frames.into_iter().cycle();
        // `sequence` is the per-EMITTED-packet monotonic counter. It must be
        // 1:1 with MediaPackets on the wire so the integrity instrument can do
        // honest completeness accounting (`expected = max - min + 1`). A single
        // `encode()` call can yield more than one compressed packet (VP9
        // alt-ref / invisible frames), so we increment this once per emitted
        // frame, NOT once per source frame (vc-1re).
        let mut sequence = 0u64;
        // `pts` is the presentation timestamp fed to the encoder, advanced once
        // per source frame. Kept separate from `sequence` so the encoder still
        // sees monotonic per-source-frame timestamps regardless of how many
        // packets each encode produces.
        let mut pts = 0i64;

        loop {
            if quit.load(Ordering::Relaxed) {
                info!("Video producer stopping for {}", user_id);
                break;
            }

            // Get next frame
            let frame_data = frame_iterator.next().unwrap();

            // vc-7zjq: honor an inbound KEYFRAME_REQUEST. The inbound consumer
            // (see `WebTransportClient::with_keyframe_signal`) flips this flag
            // when it parses a KFR targeting our user_id. We check-and-clear it
            // atomically (`swap`) so exactly one encode forces a keyframe per
            // request. The periodic `kf_max_dist=150` cadence remains the
            // always-on fallback.
            let force_kf = force_keyframe.swap(false, Ordering::Relaxed);

            // Encode to VP9 (exactly same as videocall-cli)
            let frames_result = video_encoder.encode(pts, &frame_data, force_kf)?;

            // Send each encoded frame (exactly same as videocall-cli)
            for frame in frames_result {
                // vc-1re: when integrity verification is on, append a fixed
                // `[magic][seq][crc32]` trailer to the codec payload. We do
                // NOT set a RoutingHeader — that would flip the SFU off the
                // legacy passthrough path onto the untested P4 layer-drop
                // branch, making integrity runs incomparable to baseline.
                // The seq reuses the VideoMetadata.sequence semantics.
                let mut data = frame.data.to_vec(); // Real VP9 encoded data!
                if verify_integrity {
                    crate::integrity::append_trailer(&mut data, sequence);
                }
                let media_packet = MediaPacket {
                    media_type: MediaType::VIDEO.into(),
                    data,
                    user_id: user_id.clone().into_bytes(),
                    frame_type: if frame.key { "key" } else { "delta" }.to_string(),
                    timestamp: get_timestamp_ms(),
                    duration: (1000.0 / framerate as f64),
                    video_metadata: Some(VideoMetadata {
                        sequence,
                        codec: VideoCodec::VP9_PROFILE0_LEVEL10_8BIT.into(),
                        ..Default::default()
                    })
                    .into(),
                    ..Default::default()
                };

                // Wrap in packet wrapper
                let packet_wrapper = PacketWrapper {
                    packet_type: PacketType::MEDIA.into(),
                    user_id: user_id.clone().into_bytes(),
                    data: media_packet.write_to_bytes()?,
                    ..Default::default()
                };

                // vc-7zjq: keyframe-aware backpressure. Keyframes must NEVER
                // be dropped on the way to the writer — a single dropped
                // keyframe poisons the GOP for every mid-stream joiner until
                // the next one (which is also likely to be dropped). P-frames
                // keep the original non-blocking try_send drop semantics
                // (dropping a P-frame only loses a single frame of motion).
                // See `enqueue_packet` for the survival strategy and why we
                // keep `tx_packets_enqueued` / `tx_drops_channel_full`
                // semantics intact.
                let packet_data = packet_wrapper.write_to_bytes()?;
                match enqueue_packet(&packet_sender, packet_data, frame.key) {
                    EnqueueOutcome::Enqueued => {
                        if let Some(s) = &stats {
                            s.record_tx_packet_enqueued();
                        }
                        trace!(
                            "Sent VP9 frame {} ({} bytes, {}) for {}",
                            sequence,
                            frame.data.len(),
                            if frame.key { "key" } else { "delta" },
                            user_id
                        );
                    }
                    EnqueueOutcome::DroppedFull => {
                        if let Some(s) = &stats {
                            s.record_tx_drop_channel_full();
                        }
                        debug!(
                            "video producer dropped frame (channel full) for {}",
                            user_id
                        );
                    }
                    EnqueueOutcome::Closed => {
                        warn!("video producer channel closed for {}; stopping", user_id);
                        return Ok(());
                    }
                }

                // One distinct seq per EMITTED packet so the trailer seq and
                // VideoMetadata.sequence stay 1:1 with packets-on-wire (vc-1re).
                sequence += 1;
            }

            pts += 1;
            thread::sleep(packet_interval);
        }

        Ok(())
    }

    pub fn stop(&mut self) {
        self.quit.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for VideoProducer {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Outcome of an [`enqueue_packet`] attempt. Maps 1:1 onto the producer-side
/// stat buckets the orchestrate summary depends on (vc-xpf): `Enqueued` ->
/// `tx_packets_enqueued`, `DroppedFull` -> `tx_drops_channel_full`, `Closed`
/// stops the producer. Kept as a small enum (rather than `Result`) so the
/// keyframe-survival logic is testable without a real WebTransport session.
#[derive(Debug, PartialEq, Eq)]
enum EnqueueOutcome {
    Enqueued,
    DroppedFull,
    Closed,
}

/// Total wall-clock budget a keyframe may block waiting for room on the
/// producer→writer channel before we give up (vc-7zjq). A keyframe is far more
/// valuable than realtime cadence for a mid-stream joiner, so we are willing to
/// stall the producer thread briefly to land it. At 30fps the source-frame
/// interval is ~33ms; 250ms tolerates a short writer hiccup (a few frames of
/// jitter) while still bounding the worst case so a permanently-wedged writer
/// can't hang the producer forever. The producer runs on its own std thread,
/// so blocking here does not stall the async runtime.
const KEYFRAME_BLOCK_BUDGET: Duration = Duration::from_millis(250);

/// Enqueue one outbound packet onto the bounded producer→writer channel with
/// keyframe-aware backpressure (vc-7zjq).
///
/// - **Delta / P-frames (`is_keyframe == false`)**: non-blocking `try_send`.
///   On a full channel the frame is dropped (`DroppedFull`) exactly as before
///   — losing a P-frame only drops a single frame of motion and the next
///   frame supersedes it.
/// - **Keyframes (`is_keyframe == true`)**: a keyframe must NEVER be dropped
///   because of a full channel — a lost keyframe leaves every mid-stream
///   joiner stuck on `decode_errors` until the next periodic keyframe (which
///   is itself likely to be dropped under the same backpressure). So on a full
///   channel we retry `try_send` with a short backoff, parking the producer's
///   std thread until the writer drains a slot (or the budget elapses). Only if
///   the writer makes no progress for the entire [`KEYFRAME_BLOCK_BUDGET`] do
///   we concede and report `DroppedFull` — a pathological case (a wedged
///   writer) that the periodic `kf_max_dist` cadence will retry on the next
///   boundary. Under a *persistently* wedged writer this means repeated
///   per-keyframe stalls are possible, but each is intentionally bounded by
///   `KEYFRAME_BLOCK_BUDGET` (≤250ms) and backstopped by the periodic cadence,
///   so the producer can never hang indefinitely.
///
/// Audio safety: audio and video SHARE this one bounded mpsc (the same
/// `packet_tx` is cloned into both producers, capacity 100). The keyframe stall
/// does NOT block audio: audio is enqueued with non-blocking `try_send` from
/// its own tokio task (see `audio_producer.rs`), which is never parked by the
/// video keyframe block — the block happens on the *video std thread*. Audio
/// simply contends for freed channel slots exactly as it did before this
/// change; its drop-under-backpressure behaviour is unchanged. We also do NOT
/// add eviction of queued P-frames here: the `tokio::mpsc::Sender` API exposes
/// no peek/evict, and bounded-retry achieves the same "keyframe survives"
/// guarantee without a second channel or custom queue, keeping the `tx_*` stat
/// semantics byte-for-byte identical.
///
/// Implementation note: we do not use `Sender::blocking_send` because it
/// blocks unboundedly (a wedged writer would hang the producer forever) and
/// panics if a runtime is entered. Instead we poll `try_send` with a short
/// backoff up to [`KEYFRAME_BLOCK_BUDGET`]. The producer owns a dedicated std
/// thread, so sleeping here never stalls the tokio runtime.
fn enqueue_packet(sender: &Sender<Vec<u8>>, packet: Vec<u8>, is_keyframe: bool) -> EnqueueOutcome {
    enqueue_packet_with_clock(sender, packet, is_keyframe, KEYFRAME_BLOCK_BUDGET, || {
        thread::sleep(Duration::from_millis(2))
    })
}

/// Core of [`enqueue_packet`], parameterised on the keyframe block budget and
/// a `wait` closure so unit tests can drive the retry loop deterministically
/// (e.g. draining a slot between polls) without real sleeps (vc-7zjq).
fn enqueue_packet_with_clock(
    sender: &Sender<Vec<u8>>,
    packet: Vec<u8>,
    is_keyframe: bool,
    budget: Duration,
    mut wait: impl FnMut(),
) -> EnqueueOutcome {
    let mut packet = match sender.try_send(packet) {
        Ok(()) => return EnqueueOutcome::Enqueued,
        Err(TrySendError::Closed(_)) => return EnqueueOutcome::Closed,
        Err(TrySendError::Full(p)) => p,
    };

    if !is_keyframe {
        // P-frame: preserve the original drop-on-full behaviour.
        return EnqueueOutcome::DroppedFull;
    }

    // Keyframe: retry within the budget. A keyframe is never dropped while the
    // writer is making progress; only a fully wedged writer (no slot frees for
    // the whole budget) concedes to `DroppedFull`, and the periodic-keyframe
    // cadence then retries on the next boundary.
    let deadline = Instant::now() + budget;
    loop {
        wait();
        packet = match sender.try_send(packet) {
            Ok(()) => return EnqueueOutcome::Enqueued,
            Err(TrySendError::Closed(_)) => return EnqueueOutcome::Closed,
            Err(TrySendError::Full(p)) => p,
        };
        if Instant::now() >= deadline {
            return EnqueueOutcome::DroppedFull;
        }
    }
}

// VP9 encoder implemented using exact same approach as videocall-cli

// Convert RGB image to I420 format (same as videocall-cli)
fn rgb_to_i420(image: &ImageBuffer<Rgb<u8>, Vec<u8>>) -> Vec<u8> {
    let width = image.width() as usize;
    let height = image.height() as usize;
    let mut i420_data = vec![0u8; width * height * 3 / 2];

    let rgb = image.as_raw();
    let (y_plane, uv_planes) = i420_data.split_at_mut(width * height);
    let (u_plane, v_plane) = uv_planes.split_at_mut(width * height / 4);

    for y in 0..height {
        for x in 0..width {
            let rgb_index = (y * width + x) * 3;
            let r = rgb[rgb_index] as f32;
            let g = rgb[rgb_index + 1] as f32;
            let b = rgb[rgb_index + 2] as f32;

            // Calculate Y, U, V components
            let y_value = (0.257 * r + 0.504 * g + 0.098 * b + 16.0).round() as u8;
            let u_value = (-0.148 * r - 0.291 * g + 0.439 * b + 128.0).round() as u8;
            let v_value = (0.439 * r - 0.368 * g - 0.071 * b + 128.0).round() as u8;

            y_plane[y * width + x] = y_value;

            if y % 2 == 0 && x % 2 == 0 {
                let uv_index = (y / 2) * (width / 2) + (x / 2);
                u_plane[uv_index] = u_value;
                v_plane[uv_index] = v_value;
            }
        }
    }

    i420_data
}

fn get_timestamp_ms() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as f64
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    /// vc-7zjq: under a full channel a P-frame is dropped (`DroppedFull`),
    /// preserving the original non-blocking try_send semantics.
    #[tokio::test]
    async fn pframe_dropped_when_channel_full() {
        let (tx, _rx) = mpsc::channel::<Vec<u8>>(1);
        // Fill the single slot.
        assert_eq!(
            enqueue_packet(&tx, vec![1], false),
            EnqueueOutcome::Enqueued
        );
        // Next P-frame finds the channel full and is dropped immediately.
        assert_eq!(
            enqueue_packet(&tx, vec![2], false),
            EnqueueOutcome::DroppedFull
        );
    }

    /// vc-7zjq (PRIMARY acceptance): a keyframe must NOT be dropped under
    /// backpressure as long as the writer eventually drains a slot. We fill the
    /// channel, then drive the retry loop with a `wait` closure that drains one
    /// slot on the first poll — simulating the writer making progress. The
    /// keyframe must land (`Enqueued`), NOT drop.
    #[tokio::test]
    async fn keyframe_survives_backpressure_when_writer_drains() {
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(1);
        // Fill the single slot with a queued P-frame.
        assert_eq!(
            enqueue_packet(&tx, vec![1], false),
            EnqueueOutcome::Enqueued
        );

        // Keyframe arrives while the channel is full. The `wait` closure drains
        // one slot, modelling the writer dequeuing the queued P-frame between
        // polls. The budget is generous; the keyframe must be enqueued.
        let mut drained_once = false;
        let outcome =
            enqueue_packet_with_clock(&tx, vec![0xAA], true, Duration::from_secs(5), || {
                if !drained_once {
                    // Writer makes progress: free a slot.
                    let _ = rx.try_recv();
                    drained_once = true;
                }
            });
        assert_eq!(
            outcome,
            EnqueueOutcome::Enqueued,
            "keyframe must survive backpressure once the writer drains a slot"
        );
    }

    /// vc-7zjq: if the writer is fully wedged (no slot ever frees), a keyframe
    /// concedes to `DroppedFull` once the budget elapses — bounding the worst
    /// case so a permanently-stuck writer cannot hang the producer forever. The
    /// periodic-keyframe cadence retries on the next boundary.
    #[tokio::test]
    async fn keyframe_concedes_after_budget_when_writer_wedged() {
        let (tx, _rx) = mpsc::channel::<Vec<u8>>(1);
        assert_eq!(
            enqueue_packet(&tx, vec![1], false),
            EnqueueOutcome::Enqueued
        );

        // `wait` does nothing (writer never drains); a zero budget makes the
        // loop concede on the first deadline check without real sleeping.
        let outcome =
            enqueue_packet_with_clock(&tx, vec![2], true, Duration::from_millis(0), || {});
        assert_eq!(outcome, EnqueueOutcome::DroppedFull);
    }

    /// vc-7zjq: a closed channel surfaces `Closed` for both frame kinds so the
    /// producer stops cleanly (matches the original `TrySendError::Closed`
    /// arm).
    #[tokio::test]
    async fn closed_channel_reports_closed() {
        let (tx, rx) = mpsc::channel::<Vec<u8>>(1);
        drop(rx);
        assert_eq!(enqueue_packet(&tx, vec![1], false), EnqueueOutcome::Closed);
        assert_eq!(enqueue_packet(&tx, vec![1], true), EnqueueOutcome::Closed);
    }
}
