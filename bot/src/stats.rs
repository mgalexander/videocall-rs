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

//! Shared per-bot statistics for load-test orchestration.
//!
//! `BotStats` is a lock-free, atomically-updated counter set that bot
//! components (currently the [`WebTransportClient`](crate::webtransport_client::WebTransportClient))
//! mutate while running. The orchestrator collects a snapshot at the end of
//! the run via [`BotStats::snapshot`] and aggregates totals across all bots.
//!
//! All counters are `u64` because at 200 bots * 300 s they comfortably fit
//! and atomic loads/stores are cheap.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use serde::Serialize;

/// Role of a bot in the load test. Used solely for the summary JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BotRole {
    Sender,
    Listener,
}

/// Counters mutated by a single bot's background tasks.
///
/// Wrap in `Arc` and clone freely; all fields are atomic.
#[derive(Debug, Default)]
pub struct BotStats {
    /// Logical bot identifier (e.g. `sender-0`, `listener-12`).
    pub user_id: String,
    /// Role assigned by the orchestrator.
    pub role: Option<BotRole>,
    /// `true` once the WebTransport session is established.
    pub connected: AtomicBool,
    /// Total inbound unistreams successfully read end-to-end. We treat each
    /// inbound unistream as one media packet from the SFU's perspective.
    pub packets_received: AtomicU64,
    /// Total inbound bytes successfully read.
    pub bytes_received: AtomicU64,
    /// Inbound stream read errors (the stream was accepted but failed to
    /// drain). Used as a proxy for "drops".
    pub drops: AtomicU64,
    /// Unix-millis when the bot connected; `0` if never connected.
    pub connected_at_ms: AtomicU64,
    /// Failover-test bookkeeping (p6-11): unix-millis of the last successfully
    /// drained inbound packet. `0` until the first packet arrives.
    pub last_packet_at_ms: AtomicU64,
    /// Failover-test bookkeeping (p6-11): unix-millis at which the bot first
    /// detected an inbound gap (session close, accept-uni error, or stream
    /// drain error). `0` if no gap has been observed in this run. Sticky:
    /// only the first gap of the run is recorded.
    pub disconnect_at_ms: AtomicU64,
    /// Failover-test bookkeeping (p6-11): unix-millis of the first inbound
    /// packet drained after `disconnect_at_ms`. `0` if no successful packet
    /// has arrived post-gap. Sticky: only the first reconnect is recorded.
    pub reconnect_at_ms: AtomicU64,
    /// Listener-decode bookkeeping (vc-86j): total VP9 video frames the
    /// listener successfully decoded. `0` for senders and for listeners with
    /// decode disabled.
    pub video_frames_decoded: AtomicU64,
    /// Listener-decode bookkeeping (vc-86j): total Opus audio frames the
    /// listener successfully decoded.
    pub audio_frames_decoded: AtomicU64,
    /// Listener-decode bookkeeping (vc-86j): wrapper/media parse failures and
    /// codec decode errors observed while servicing the inbound stream.
    pub decode_errors: AtomicU64,
    /// Listener-feedback bookkeeping (vc-dwc): total `DiagnosticsPacket`
    /// frames the listener emitted back to the SFU (per-publisher × per-
    /// media-type at ~2Hz, matching the real client cadence). `0` for
    /// senders and for listeners with decode disabled.
    pub diagnostics_sent: AtomicU64,
    /// Listener-feedback bookkeeping (vc-dwc): total KEYFRAME_REQUEST
    /// MediaPackets the listener emitted (rate-limited per publisher to
    /// match `KEYFRAME_REQUEST_MIN_INTERVAL_MS`).
    pub keyframe_requests_sent: AtomicU64,
}

impl BotStats {
    /// Construct an empty stats handle for a bot with the given id and role.
    pub fn new(user_id: String, role: BotRole) -> Arc<Self> {
        Arc::new(Self {
            user_id,
            role: Some(role),
            ..Self::default()
        })
    }

    /// Mark the bot as connected and record the timestamp.
    pub fn mark_connected(&self, now_ms: u64) {
        self.connected.store(true, Ordering::Relaxed);
        self.connected_at_ms.store(now_ms, Ordering::Relaxed);
    }

    /// Record one successfully drained inbound stream.
    pub fn record_packet(&self, bytes: u64) {
        self.packets_received.fetch_add(1, Ordering::Relaxed);
        self.bytes_received.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record one successfully drained inbound stream and update the
    /// failover-test bookkeeping (`last_packet_at_ms` and, if this is the
    /// first packet after a recorded disconnect, `reconnect_at_ms`).
    ///
    /// `now_ms` is the wall-clock unix-millis the caller observed when the
    /// packet finished draining. Centralising the timestamp here keeps the
    /// "first-post-gap" detection branch atomic-free in the hot path.
    pub fn record_packet_at(&self, bytes: u64, now_ms: u64) {
        self.record_packet(bytes);
        self.last_packet_at_ms.store(now_ms, Ordering::Relaxed);
        // Sticky reconnect timestamp: only set the first time we see a packet
        // after a disconnect was observed. CAS from 0 -> now_ms keeps this
        // race-free across the per-stream tasks.
        if self.disconnect_at_ms.load(Ordering::Relaxed) != 0 {
            let _ = self.reconnect_at_ms.compare_exchange(
                0,
                now_ms,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
        }
    }

    /// Mark the first observed disconnect for this run. Sticky: subsequent
    /// calls are no-ops (CAS 0 -> now_ms).
    pub fn mark_disconnected_at(&self, now_ms: u64) {
        let _ =
            self.disconnect_at_ms
                .compare_exchange(0, now_ms, Ordering::Relaxed, Ordering::Relaxed);
        self.connected.store(false, Ordering::Relaxed);
    }

    /// Record a failed-to-drain inbound stream.
    pub fn record_drop(&self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one successfully decoded VP9 video frame.
    pub fn record_video_decoded(&self) {
        self.video_frames_decoded.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one successfully decoded Opus audio frame.
    pub fn record_audio_decoded(&self) {
        self.audio_frames_decoded.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a parse or decode failure observed on the inbound path.
    pub fn record_decode_error(&self) {
        self.decode_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one outbound `DiagnosticsPacket` emission (vc-dwc).
    pub fn record_diagnostics_sent(&self) {
        self.diagnostics_sent.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one outbound KEYFRAME_REQUEST `MediaPacket` emission (vc-dwc).
    pub fn record_keyframe_request_sent(&self) {
        self.keyframe_requests_sent.fetch_add(1, Ordering::Relaxed);
    }

    /// Capture a serializable snapshot of the current counters.
    pub fn snapshot(&self, duration_s: f64) -> BotStatsSnapshot {
        let bytes = self.bytes_received.load(Ordering::Relaxed);
        let avg_bandwidth_bps = if duration_s > 0.0 {
            (bytes as f64 * 8.0 / duration_s).round() as u64
        } else {
            0
        };
        let disconnect_at_ms = self.disconnect_at_ms.load(Ordering::Relaxed);
        let reconnect_at_ms = self.reconnect_at_ms.load(Ordering::Relaxed);
        let downtime_ms = match (disconnect_at_ms, reconnect_at_ms) {
            (0, _) => None,
            (_, 0) => None,
            (d, r) if r >= d => Some(r - d),
            // Clock skew / out-of-order observation: clamp to zero rather
            // than reporting a negative downtime.
            _ => Some(0),
        };
        let video_frames_decoded = self.video_frames_decoded.load(Ordering::Relaxed);
        let audio_frames_decoded = self.audio_frames_decoded.load(Ordering::Relaxed);
        let decode_errors = self.decode_errors.load(Ordering::Relaxed);
        let diagnostics_sent = self.diagnostics_sent.load(Ordering::Relaxed);
        let keyframe_requests_sent = self.keyframe_requests_sent.load(Ordering::Relaxed);
        BotStatsSnapshot {
            user_id: self.user_id.clone(),
            role: self.role,
            connected: self.connected.load(Ordering::Relaxed),
            packets_received: self.packets_received.load(Ordering::Relaxed),
            bytes_received: bytes,
            drops: self.drops.load(Ordering::Relaxed),
            avg_bandwidth_bps,
            disconnect_at_ms: if disconnect_at_ms == 0 {
                None
            } else {
                Some(disconnect_at_ms)
            },
            reconnect_at_ms: if reconnect_at_ms == 0 {
                None
            } else {
                Some(reconnect_at_ms)
            },
            downtime_ms,
            video_frames_decoded: if video_frames_decoded == 0 {
                None
            } else {
                Some(video_frames_decoded)
            },
            audio_frames_decoded: if audio_frames_decoded == 0 {
                None
            } else {
                Some(audio_frames_decoded)
            },
            decode_errors: if decode_errors == 0 {
                None
            } else {
                Some(decode_errors)
            },
            diagnostics_sent: if diagnostics_sent == 0 {
                None
            } else {
                Some(diagnostics_sent)
            },
            keyframe_requests_sent: if keyframe_requests_sent == 0 {
                None
            } else {
                Some(keyframe_requests_sent)
            },
        }
    }
}

/// Serializable per-bot snapshot included in the summary JSON.
///
/// `disconnect_at_ms`, `reconnect_at_ms`, and `downtime_ms` are only populated
/// in failover-test mode; they remain `None` for ordinary orchestrate runs so
/// the existing JSON schema is forward-compatible (consumers see absent
/// fields when serialized with `serde_json` and `Option::None` -> `null`).
#[derive(Debug, Clone, Serialize)]
pub struct BotStatsSnapshot {
    pub user_id: String,
    pub role: Option<BotRole>,
    pub connected: bool,
    pub packets_received: u64,
    pub bytes_received: u64,
    pub drops: u64,
    pub avg_bandwidth_bps: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disconnect_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reconnect_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub downtime_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub video_frames_decoded: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio_frames_decoded: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decode_errors: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diagnostics_sent: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keyframe_requests_sent: Option<u64>,
}
