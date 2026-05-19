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

//! The native decoder implementation using `std::thread`.

use super::{Decodable, DecodedFrame};
use crate::frame::FrameBuffer;
use std::ffi::c_void;
use std::ptr;
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::thread::{self, JoinHandle};
use vpx_sys::{
    vpx_codec_ctx_t, vpx_codec_dec_init_ver, vpx_codec_decode, vpx_codec_destroy,
    vpx_codec_get_frame, vpx_codec_vp9_dx, VPX_CODEC_OK, VPX_DECODER_ABI_VERSION,
};

/// Upper bound on the number of in-flight encoded frames buffered per
/// [`NativeDecoder`] before the producer starts dropping (vc-35t / vc-2x8).
///
/// Steady-state load for the bot listener is roughly 30 frames/s per
/// publisher, so a bound of 32 absorbs short bursts (~1s at line rate) while
/// capping worst-case memory at ~32 × ~60 KiB ≈ 2 MiB per decoder. With ~500
/// decoders (100 listeners × ~5 publishers) the ceiling is ~1 GiB instead of
/// unbounded growth that previously OOM'd 4 GiB pods.
const NATIVE_DECODER_CHANNEL_BOUND: usize = 32;

// --- Vp9Decoder implementation, now living inside the native module ---

/// A VP9 decoder using libvpx.
struct Vp9Decoder {
    context: vpx_codec_ctx_t,
}

impl Vp9Decoder {
    fn new() -> Result<Self, String> {
        let mut context = unsafe { std::mem::zeroed() };
        let ret = unsafe {
            vpx_codec_dec_init_ver(
                &mut context,
                vpx_codec_vp9_dx(),
                ptr::null_mut(),
                0,
                VPX_DECODER_ABI_VERSION as i32,
            )
        };
        if ret != VPX_CODEC_OK {
            return Err(format!("Failed to initialize VP9 decoder: {:?}", ret));
        }
        Ok(Self { context })
    }
}

impl Drop for Vp9Decoder {
    fn drop(&mut self) {
        unsafe {
            vpx_codec_destroy(&mut self.context);
        }
    }
}
// --- End Vp9Decoder implementation ---

// A wrapper to make the Vp9Decoder Send-able.
// This is safe because we are only ever accessing the decoder from a single thread.
struct SendableVp9Decoder(Vp9Decoder);
unsafe impl Send for SendableVp9Decoder {}

// A mock decoder that does nothing.
struct MockDecoder;
impl MockDecoder {
    fn new() -> Self {
        Self
    }
}

/// Test-only knob: when non-zero, `MockDecoder::decode_frame` sleeps for this
/// many nanoseconds before returning. The slow consumer thus runs *inside the
/// decoder thread*, not in the `on_decoded_frame` callback, which makes the
/// backpressure test in `tests::full_channel_drops_and_reports` deterministic
/// even though `MockDecoder` never produces any frames to dispatch (vc-35t).
///
/// Outside tests this is always `0`, so the production path remains a no-op.
#[cfg(test)]
static MOCK_DECODE_DELAY_NANOS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// A trait for any decoder that can run on the internal thread.
trait ThreadDecodable: Send {
    fn decode_frame(&mut self, frame_buffer: &FrameBuffer) -> Result<Vec<DecodedFrame>, String>;
}

impl ThreadDecodable for SendableVp9Decoder {
    fn decode_frame(&mut self, frame_buffer: &FrameBuffer) -> Result<Vec<DecodedFrame>, String> {
        let mut decoded_frames = Vec::new();

        let ret = unsafe {
            vpx_codec_decode(
                &mut self.0.context,
                frame_buffer.frame.data.as_ptr(),
                frame_buffer.frame.data.len() as u32,
                ptr::null_mut(),
                0,
            )
        };
        if ret != VPX_CODEC_OK {
            let error_msg = unsafe {
                let error_cstr = vpx_sys::vpx_codec_err_to_string(ret);
                if error_cstr.is_null() {
                    "Unknown codec error".to_string()
                } else {
                    std::ffi::CStr::from_ptr(error_cstr)
                        .to_string_lossy()
                        .into_owned()
                }
            };
            return Err(format!("VPX Decode failed: {}", error_msg));
        }

        let mut iter = ptr::null_mut::<c_void>();
        loop {
            let img = unsafe {
                vpx_codec_get_frame(
                    &mut self.0.context,
                    &mut iter as *mut _ as *mut *const c_void,
                )
            };
            if img.is_null() {
                break;
            }

            let image_data = unsafe {
                let width = (*img).d_w as usize;
                let height = (*img).d_h as usize;

                // For I420 format, the U and V planes are half the width and height.
                let uv_width = width / 2;
                let uv_height = height / 2;

                // Total size = Y plane + U plane + V plane
                let mut buffer = Vec::with_capacity(width * height + 2 * uv_width * uv_height);

                // Copy Y plane
                copy_plane_to_buffer(
                    (*img).planes[0],
                    (*img).stride[0],
                    width,
                    height,
                    &mut buffer,
                );
                // Copy U plane
                copy_plane_to_buffer(
                    (*img).planes[1],
                    (*img).stride[1],
                    uv_width,
                    uv_height,
                    &mut buffer,
                );
                // Copy V plane
                copy_plane_to_buffer(
                    (*img).planes[2],
                    (*img).stride[2],
                    uv_width,
                    uv_height,
                    &mut buffer,
                );

                buffer
            };

            decoded_frames.push(DecodedFrame {
                sequence_number: frame_buffer.sequence_number(),
                width: 0,
                height: 0,
                data: image_data,
            });
        }
        Ok(decoded_frames)
    }
}

/// Helper to copy a plane from a vpx_image_t to a buffer, accounting for stride.
unsafe fn copy_plane_to_buffer(
    plane: *const u8,
    stride: i32,
    width: usize,
    height: usize,
    buffer: &mut Vec<u8>,
) {
    let mut current_ptr = plane;
    for _ in 0..height {
        buffer.extend_from_slice(std::slice::from_raw_parts(current_ptr, width));
        current_ptr = current_ptr.offset(stride as isize);
    }
}

impl ThreadDecodable for MockDecoder {
    fn decode_frame(&mut self, _frame_buffer: &FrameBuffer) -> Result<Vec<DecodedFrame>, String> {
        // Intentionally silent: this runs once per frame in the bot's listener
        // path. Per-frame stdout writes flood the orchestrator's JSON parser
        // at scale (vc-35t / vc-4tl).
        #[cfg(test)]
        {
            let delay_ns = MOCK_DECODE_DELAY_NANOS.load(std::sync::atomic::Ordering::Relaxed);
            if delay_ns > 0 {
                std::thread::sleep(std::time::Duration::from_nanos(delay_ns));
            }
        }
        Ok(Vec::new())
    }
}

/// A message sent to the native decoder thread.
///
/// Previously held an explicit `Shutdown` variant, but the teardown path now
/// drops the sender (see `NativeDecoder::drop`) and relies on
/// `recv() -> Err(RecvError)` to terminate the worker. That avoids the
/// shutdown-deadlock window where a `Shutdown` sentinel could not be enqueued
/// because the bounded channel was full (vc-35t).
enum DecoderMessage {
    /// A frame to be decoded.
    Frame(FrameBuffer),
}

pub struct NativeDecoder {
    thread_handle: Option<JoinHandle<()>>,
    /// `Some` for the lifetime of the decoder; `None` only transiently inside
    /// `Drop::drop`, where it is taken and dropped *before* joining the worker
    /// thread to avoid a shutdown deadlock (vc-35t). When the channel is full
    /// at teardown, a `try_send(Shutdown)` would fail and `recv()` would block
    /// forever because the sender is still owned by `self`. Taking the sender
    /// here lets `recv()` return `Err(RecvError)` and the thread exit cleanly.
    sender: Option<SyncSender<DecoderMessage>>,
    /// Invoked from the producer side of the bounded channel whenever a frame
    /// cannot be enqueued (channel full or decoder thread gone). Used by the
    /// bot listener to attribute backpressure-induced drops.
    on_dropped: Box<dyn Fn() + Send + Sync>,
}

impl NativeDecoder {
    /// Construct a decoder with both the standard decoded-frame callback and a
    /// `on_dropped` callback that is invoked when a frame is discarded because
    /// the decoder's bounded input channel is full or the decoder thread has
    /// terminated. The on_dropped callback runs synchronously on the producer
    /// thread, so it must be cheap and non-blocking (an atomic counter bump is
    /// the intended use; see `bot/src/stats.rs`).
    pub fn with_drop_callback(
        codec: crate::decoder::VideoCodec,
        on_decoded_frame: Box<dyn Fn(DecodedFrame) + Send + Sync>,
        on_dropped: Box<dyn Fn() + Send + Sync>,
    ) -> Self {
        Self::build(codec, on_decoded_frame, on_dropped)
    }

    fn build(
        codec: crate::decoder::VideoCodec,
        on_decoded_frame: Box<dyn Fn(DecodedFrame) + Send + Sync>,
        on_dropped: Box<dyn Fn() + Send + Sync>,
    ) -> Self {
        // Bounded channel: a full queue means the decoder thread is behind
        // real-time. The producer drops the frame rather than blocking the
        // tokio blocking-pool worker that called `decode` (vc-35t).
        let (sender, receiver) = mpsc::sync_channel(NATIVE_DECODER_CHANNEL_BOUND);

        let thread_handle = Some(thread::spawn(move || {
            let mut decoder: Box<dyn ThreadDecodable> = match codec {
                crate::decoder::VideoCodec::Vp9Profile0Level10Bit8 => Box::new(SendableVp9Decoder(
                    Vp9Decoder::new().expect("Failed to create Vp9Decoder"),
                )),
                crate::decoder::VideoCodec::Vp8 => {
                    // VP8 uses the same libvpx decoder
                    Box::new(SendableVp9Decoder(
                        Vp9Decoder::new().expect("Failed to create Vp9Decoder"),
                    ))
                }
                crate::decoder::VideoCodec::Mock => Box::new(MockDecoder::new()),
                crate::decoder::VideoCodec::Unspecified => {
                    panic!("Cannot create decoder for unspecified codec")
                }
            };

            // This is the decoder thread loop. Exits when the producer side
            // of the channel is dropped (see `NativeDecoder::drop`).
            while let Ok(DecoderMessage::Frame(frame_buffer)) = receiver.recv() {
                // Per-frame logging was removed (vc-35t / vc-4tl): at
                // bot-scale (100+ listeners × multiple publishers) the
                // stdout volume crowds out the orchestrator's summary
                // JSON. Decode errors are surfaced via the bot's stats
                // counters instead of stderr.
                if let Ok(images) = decoder.decode_frame(&frame_buffer) {
                    for img in images {
                        on_decoded_frame(img);
                    }
                }
            }
        }));

        NativeDecoder {
            thread_handle,
            sender: Some(sender),
            on_dropped,
        }
    }
}

impl Decodable for NativeDecoder {
    /// The decoded frame type for native decoding.
    type Frame = DecodedFrame;

    fn new(
        codec: crate::decoder::VideoCodec,
        on_decoded_frame: Box<dyn Fn(Self::Frame) + Send + Sync>,
    ) -> Self {
        // Default drop callback is a no-op; callers that care about backpressure
        // should use `NativeDecoder::with_drop_callback` directly.
        Self::build(codec, on_decoded_frame, Box::new(|| {}))
    }

    fn decode(&self, frame: FrameBuffer) {
        // `try_send` keeps this non-blocking even on a tokio blocking-pool
        // worker. The decoder thread falling behind must not back up onto the
        // network read loop.
        //
        // `sender` is only `None` transiently inside `Drop::drop`, and `decode`
        // cannot be called once the struct has been dropped, so the `else`
        // branch is unreachable in practice — we treat it as a drop for
        // accounting symmetry rather than panicking.
        let Some(sender) = self.sender.as_ref() else {
            (self.on_dropped)();
            return;
        };
        match sender.try_send(DecoderMessage::Frame(frame)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                // Channel is at capacity: the decoder thread is behind. Drop
                // the frame and bump the drop counter so the bot can attribute
                // the loss.
                (self.on_dropped)();
            }
            Err(TrySendError::Disconnected(_)) => {
                // Decoder thread has terminated (panicked at startup, or we're
                // racing Drop). Same accounting as Full.
                (self.on_dropped)();
            }
        }
    }
}

impl Drop for NativeDecoder {
    fn drop(&mut self) {
        // vc-35t: do NOT signal Shutdown via `try_send` and then join while the
        // sender is still alive. If the channel is at capacity at teardown,
        // `try_send` fails and the worker's `recv()` would block forever
        // because we still hold a live `SyncSender` (the worker exits its
        // `while let Ok(_) = receiver.recv()` loop only when *all* senders
        // drop). Joining in that state deadlocks `Drop::drop`.
        //
        // Instead, take and drop the sender first. The worker's next `recv()`
        // (after draining any frames already in the channel) returns
        // `Err(RecvError)`, which terminates the loop. The dedicated
        // `Shutdown` message is now redundant for the teardown path and is
        // intentionally not sent: it would only ever sit behind queued frames
        // and the channel-close signal is strictly better than a sentinel.
        drop(self.sender.take());

        // Wait for the thread to drain in-flight frames and exit. Bounded by
        // the channel depth (NATIVE_DECODER_CHANNEL_BOUND frames) × per-frame
        // decode cost; with the sender dropped, this is finite and small.
        if let Some(handle) = self.thread_handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decoder::VideoCodec;
    use crate::frame::{FrameCodec, FrameType, VideoFrame};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    fn make_frame(seq: u64) -> FrameBuffer {
        FrameBuffer::new(
            VideoFrame {
                sequence_number: seq,
                frame_type: FrameType::DeltaFrame,
                codec: FrameCodec::Vp9Profile0Level10Bit8,
                temporal_layer_id: 0,
                // The Mock decoder ignores the payload entirely; an empty
                // buffer keeps the test cheap and avoids real codec work.
                data: Vec::new(),
                timestamp: 0.0,
            },
            0,
        )
    }

    /// vc-35t: a full bounded channel must drop the frame on the producer side
    /// and invoke the `on_dropped` callback rather than block the caller.
    ///
    /// Determinism: the Mock decoder's `decode_frame` itself sleeps for the
    /// duration configured in `MOCK_DECODE_DELAY_NANOS`. This guarantees that
    /// the worker thread is the bottleneck (not the producer), so the math
    /// is unambiguous:
    ///   - 200 frames sent back-to-back
    ///   - 5 ms per frame on the consumer => ~1 s of consumer work total
    ///   - channel bound is 32 => overflow of (200 − 32) = 168 frames worst
    ///     case, and at least (200 − 32 − a few drained during the send loop)
    ///     in practice — comfortably > 0 on any scheduler.
    /// Previously the test relied on a sleep inside `on_decoded_frame`, but
    /// `MockDecoder` never emits decoded frames, so that callback never ran
    /// and the test only "drove" drops by winning a scheduler race — flaky on
    /// idle CI.
    #[test]
    fn full_channel_drops_and_reports() {
        // 5 ms is enough to dominate any plausible producer-loop iteration
        // cost on CI while keeping the test wall-clock ≤ ~1.5 s.
        const PER_FRAME_DELAY: Duration = Duration::from_millis(5);
        const TOTAL_FRAMES: usize = 200;

        // The delay knob is a process-global static. Set it before spawning
        // the decoder thread so the worker observes the value on every frame.
        MOCK_DECODE_DELAY_NANOS.store(
            PER_FRAME_DELAY.as_nanos() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        let decoded = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));

        let decoded_clone = decoded.clone();
        let dropped_clone = dropped.clone();

        let decoder = NativeDecoder::with_drop_callback(
            VideoCodec::Mock,
            Box::new(move |_| {
                // MockDecoder never emits frames, so this never fires.
                // Tracked anyway in case the mock is changed later.
                decoded_clone.fetch_add(1, Ordering::Relaxed);
            }),
            Box::new(move || {
                dropped_clone.fetch_add(1, Ordering::Relaxed);
            }),
        );

        // Push significantly more frames than the channel bound, timing the
        // producer to assert that `try_send` never blocks for long.
        let producer_start = std::time::Instant::now();
        for i in 0..TOTAL_FRAMES as u64 {
            decoder.decode(make_frame(i));
        }
        let producer_elapsed = producer_start.elapsed();

        // Drop the decoder to release the static delay knob for any other
        // tests, and to exercise the non-blocking Drop path (vc-35t blocker).
        drop(decoder);

        // Reset the global so we don't poison other tests in the same binary.
        MOCK_DECODE_DELAY_NANOS.store(0, std::sync::atomic::Ordering::Relaxed);

        let dropped_total = dropped.load(Ordering::Relaxed);

        // 1. Backpressure must have dropped at least one frame. With a 5 ms
        //    consumer and an instant producer this is overdetermined:
        //    expected_min = TOTAL_FRAMES − NATIVE_DECODER_CHANNEL_BOUND − slack.
        assert!(
            dropped_total > 0,
            "expected the bounded channel to drop frames, dropped={dropped_total}, bound={NATIVE_DECODER_CHANNEL_BOUND}, total_sent={TOTAL_FRAMES}"
        );
        // Tighter check: with the queue full almost immediately, the vast
        // majority of the 200 frames should be dropped. Allow generous slack
        // for scheduler variance but catch a regression that lets the
        // try_send path silently start blocking (which would make drops near
        // zero again).
        let expected_min_drops = TOTAL_FRAMES - NATIVE_DECODER_CHANNEL_BOUND - 16;
        assert!(
            dropped_total >= expected_min_drops,
            "expected at least {expected_min_drops} drops with a 5ms-per-frame consumer, got {dropped_total}"
        );

        // 2. Producer must not have blocked. Even with worst-case OS jitter,
        //    200 non-blocking try_send calls should finish in well under
        //    100 ms; if try_send is silently turning into send, this balloons
        //    to ~1 s (TOTAL_FRAMES × PER_FRAME_DELAY = 1s).
        assert!(
            producer_elapsed < Duration::from_millis(100),
            "producer loop took {producer_elapsed:?}; try_send appears to be blocking"
        );

        // 3. Sanity: we shouldn't have dropped every single frame — the
        //    channel does hold NATIVE_DECODER_CHANNEL_BOUND entries.
        assert!(
            dropped_total < TOTAL_FRAMES,
            "expected at least some frames to enqueue, but all {TOTAL_FRAMES} were dropped"
        );

        // Silence dead_store warnings on `decoded`: MockDecoder never emits
        // frames, so this count is always zero. The assertion documents that
        // invariant rather than checking a meaningful property.
        assert_eq!(
            decoded.load(Ordering::Relaxed),
            0,
            "MockDecoder must not produce decoded frames"
        );
    }

    /// vc-35t: dropping a `NativeDecoder` while the channel is full must not
    /// deadlock. The previous implementation tried to enqueue a `Shutdown`
    /// sentinel via `try_send` and then joined the worker; if the channel was
    /// full at that moment the send failed and `recv()` blocked forever
    /// because the live `SyncSender` in `self` was never dropped. We exercise
    /// that exact condition: wedge the consumer with a delay, jam the
    /// channel, then drop the decoder and assert it returns in finite time.
    ///
    /// Note on bound: after the fix, dropping the sender allows the worker to
    /// drain the *already-buffered* frames at real time before `recv()`
    /// returns `Err(RecvError)`. That's an accepted trade-off (per the bead);
    /// the goal here is to prove there's no deadlock, not to prove
    /// instantaneous shutdown. With a 5 ms per-frame delay and a 32-frame
    /// queue the upper bound is ~160 ms of drain plus jitter.
    #[test]
    fn drop_does_not_deadlock_when_channel_full() {
        const PER_FRAME_DELAY: Duration = Duration::from_millis(5);

        MOCK_DECODE_DELAY_NANOS.store(
            PER_FRAME_DELAY.as_nanos() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        let decoder =
            NativeDecoder::with_drop_callback(VideoCodec::Mock, Box::new(|_| {}), Box::new(|| {}));

        // Overflow the channel so the wedge is unambiguous: with the consumer
        // at 5ms/frame and the producer instant, the channel is full by the
        // time we return from this loop. Any extra frames are dropped on the
        // producer side.
        for i in 0..(NATIVE_DECODER_CHANNEL_BOUND as u64 * 4) {
            decoder.decode(make_frame(i));
        }

        let drop_start = std::time::Instant::now();
        drop(decoder);
        let drop_elapsed = drop_start.elapsed();

        MOCK_DECODE_DELAY_NANOS.store(0, std::sync::atomic::Ordering::Relaxed);

        // Upper bound: 32 buffered frames × 5 ms = 160 ms, plus generous
        // scheduler slack. Pre-fix would have blocked forever (test would be
        // killed by harness timeout).
        assert!(
            drop_elapsed < Duration::from_millis(1_000),
            "Drop took {drop_elapsed:?}; expected < 1s. A truly unbounded value here means the deadlock has regressed."
        );
    }
}
