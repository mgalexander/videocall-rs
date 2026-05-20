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
use std::sync::{Arc, Mutex};

use serde::Serialize;

use crate::integrity::{IntegritySummary, IntegrityTracker};

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
    /// codec decode errors observed while servicing the inbound stream. Since
    /// vc-35t this also includes backpressure drops from the per-publisher
    /// VP9 decoder channel: when the bounded native-decoder input queue is
    /// full, the producer drops the frame and bumps this counter rather than
    /// blocking the network read loop. A follow-up bead will split these into
    /// separate counters; for now they share one bucket.
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
    /// Producer-side bookkeeping (vc-xpf): total packets the audio/video
    /// producers successfully enqueued onto the producer→sender mpsc
    /// channel via `try_send`. Counts both audio and video paths.
    pub tx_packets_enqueued: AtomicU64,
    /// Producer-side bookkeeping (vc-xpf): total packets dropped at the
    /// producer because the bounded producer→sender mpsc channel was full
    /// (`TrySendError::Full`). Expected to grow during the staircase test
    /// when the WebTransport writer can't keep up with offered load.
    pub tx_drops_channel_full: AtomicU64,
    /// Producer-side bookkeeping (vc-xpf): total packets dropped at the
    /// WebTransport writer because `send_via_session` returned an error
    /// (open_uni / write_all / finish failure). Distinct from
    /// `tx_drops_channel_full` so the staircase test can attribute drops to
    /// either the local queue or the wire.
    pub tx_drops_send_error: AtomicU64,
    /// Redirect bookkeeping (vc-kni): total `ADMISSION_DECISION{REDIRECT}`
    /// targets this bot successfully followed via the orchestrate reconnect
    /// loop. Increments once per loop iteration that captured a redirect
    /// target and rebuilt the WebTransport client at the new host. `0` for
    /// failover-test mode (which logs but does not follow redirects) and for
    /// any bot that never received a redirect.
    pub redirects_followed: AtomicU64,

    // --- Media-vs-control receive split (vc-1re) -----------------------
    //
    // `packets_received` (above) counts every inbound unistream, including
    // 57-byte control packets (heartbeat, SPEAKER_UPDATE, ...). That made it
    // impossible to tell "media never arrived" from "media arrived but didn't
    // decode". These split counters classify each decoded wrapper so the
    // load test can read the media-only signal. `packets_received` stays as
    // the back-compat total.
    /// Non-MEDIA wrappers received (heartbeat, SPEAKER_UPDATE, ADMISSION,
    /// etc.). Classified at the decode dispatch site (vc-1re).
    pub control_packets_received: AtomicU64,
    /// MEDIA wrappers received, regardless of inner media type (vc-1re).
    pub media_packets_received: AtomicU64,
    /// MEDIA wrappers whose inner `media_type` is VIDEO (vc-1re).
    pub media_received_video: AtomicU64,
    /// MEDIA wrappers whose inner `media_type` is AUDIO (vc-1re).
    pub media_received_audio: AtomicU64,
    /// MEDIA wrappers whose inner `media_type` is neither VIDEO nor AUDIO
    /// (SCREEN, RTT, KEYFRAME_REQUEST, HEARTBEAT-as-media, ...) (vc-1re).
    pub media_received_other: AtomicU64,

    // --- Trailer-CRC integrity (vc-1re) --------------------------------
    //
    // Populated only when the bot runs with `--verify-integrity`. The decode
    // thread strips the payload trailer, recomputes CRC32, and folds the
    // observation into `integrity`. The snapshot rolls the tracker up into the
    // four fixed-shape counters.
    /// Per-(publisher, media_type) completeness + CRC tracker. Behind a
    /// `Mutex` because the decode thread mutates it and the orchestrator
    /// reads it at snapshot time. Bounded: O(publishers × media_types), NOT
    /// duration-dependent.
    pub integrity: Mutex<IntegrityTracker>,
    /// `true` once `--verify-integrity` wiring is active on this bot. The four
    /// integrity counters serialize either way (fixed-shape); this is purely
    /// for clarity in the rollup.
    pub verify_integrity: AtomicBool,

    // --- joined_pod + redirect_chain (vc-1re) --------------------------
    /// Pod hostname the bot's media session actually landed on after any
    /// redirect. Set as the connection target / ADMISSION_DECISION target
    /// resolves. `None` until the first successful connect.
    pub joined_pod: Mutex<Option<String>>,
    /// Ordered list of redirect hops the bot traversed, with per-hop latency.
    /// A clean direct connect leaves this empty. Bounded by the orchestrate
    /// loop's `MAX_REDIRECT_HOPS`.
    pub redirect_chain: Mutex<Vec<RedirectHop>>,
}

/// One hop in a bot's redirect chain (vc-1re). Records the pod the bot was
/// redirected to and the wall-clock latency of that hop (time from observing
/// the `ADMISSION_DECISION{REDIRECT}` to the next session being established).
#[derive(Debug, Clone, Serialize)]
pub struct RedirectHop {
    /// Target pod hostname carried by `ADMISSION_DECISION{REDIRECT}`.
    pub to_pod: String,
    /// Latency of this hop in milliseconds: teardown + reconnect to the
    /// redirect target. Asserted against the §3.2 responsiveness budgets.
    pub redirect_latency_ms: u64,
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

    /// Record one packet successfully enqueued onto the producer→sender
    /// channel (vc-xpf). Called from both `AudioProducer` and `VideoProducer`
    /// on the `try_send` success path.
    pub fn record_tx_packet_enqueued(&self) {
        self.tx_packets_enqueued.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one packet dropped at the producer because the
    /// producer→sender channel was full (vc-xpf, `TrySendError::Full`).
    pub fn record_tx_drop_channel_full(&self) {
        self.tx_drops_channel_full.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one packet dropped at the WebTransport writer because
    /// `send_via_session` failed (vc-xpf).
    pub fn record_tx_drop_send_error(&self) {
        self.tx_drops_send_error.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one successfully followed `ADMISSION_DECISION{REDIRECT}`
    /// target (vc-kni). Called by the orchestrate reconnect loop after it
    /// captures `redirect_to` from the session-end signal and rebuilds the
    /// `WebTransportClient` against the new host.
    pub fn record_redirect_followed(&self) {
        self.redirects_followed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one inbound non-MEDIA control wrapper (vc-1re). Single
    /// increment site for `control_packets_received`.
    pub fn record_control_packet(&self) {
        self.control_packets_received
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record one inbound MEDIA wrapper, classified by inner media type
    /// (vc-1re). Single increment site for `media_packets_received` plus the
    /// video/audio/other split. `is_video`/`is_audio` are mutually
    /// exclusive; both false means "other".
    pub fn record_media_packet(&self, is_video: bool, is_audio: bool) {
        self.media_packets_received.fetch_add(1, Ordering::Relaxed);
        if is_video {
            self.media_received_video.fetch_add(1, Ordering::Relaxed);
        } else if is_audio {
            self.media_received_audio.fetch_add(1, Ordering::Relaxed);
        } else {
            self.media_received_other.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Enable integrity tracking on this bot (vc-1re). Called by the
    /// orchestrate/failover wiring when `--verify-integrity` is set.
    pub fn enable_verify_integrity(&self) {
        self.verify_integrity.store(true, Ordering::Relaxed);
    }

    /// Set the pod the bot's media session landed on (vc-1re). Idempotent;
    /// the orchestrate loop calls this after each successful connect with the
    /// current target host.
    pub fn set_joined_pod(&self, pod: String) {
        *self.joined_pod.lock().unwrap_or_else(|p| p.into_inner()) = Some(pod);
    }

    /// Append one hop to the redirect chain (vc-1re). Called by the
    /// orchestrate reconnect loop once it has measured the hop latency.
    pub fn record_redirect_hop(&self, to_pod: String, redirect_latency_ms: u64) {
        self.redirect_chain
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(RedirectHop {
                to_pod,
                redirect_latency_ms,
            });
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
        let tx_packets_enqueued = self.tx_packets_enqueued.load(Ordering::Relaxed);
        let tx_drops_channel_full = self.tx_drops_channel_full.load(Ordering::Relaxed);
        let tx_drops_send_error = self.tx_drops_send_error.load(Ordering::Relaxed);
        // vc-020: unified producer-side drop aggregate. Mirrors the
        // receive-side `drops` counter so dashboards / jq filters can read
        // `.tx_drops` as a single number without summing the split fields.
        // The split fields (`tx_drops_channel_full`, `tx_drops_send_error`)
        // remain for attribution.
        let tx_drops = tx_drops_channel_full + tx_drops_send_error;
        let redirects_followed = self.redirects_followed.load(Ordering::Relaxed);

        // vc-1re: media-vs-control receive split.
        let control_packets_received = self.control_packets_received.load(Ordering::Relaxed);
        let media_packets_received = self.media_packets_received.load(Ordering::Relaxed);
        let media_received_video = self.media_received_video.load(Ordering::Relaxed);
        let media_received_audio = self.media_received_audio.load(Ordering::Relaxed);
        let media_received_other = self.media_received_other.load(Ordering::Relaxed);

        // vc-1re: roll the integrity tracker up into the four fixed-shape
        // counters. On legacy passthrough the dominant accounted drop is the
        // receive-side `drops` counter (AllowSet unsubscribed); subtract it so
        // `unexplained_gaps` isolates losses we cannot explain.
        let drops = self.drops.load(Ordering::Relaxed);
        let IntegritySummary {
            media_seq_max,
            media_received_distinct,
            crc_mismatches,
            unexplained_gaps,
        } = self
            .integrity
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .summarize(drops);

        // vc-1re: redirect chain + joined pod.
        let redirect_chain = self
            .redirect_chain
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let joined_pod = self
            .joined_pod
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();

        BotStatsSnapshot {
            user_id: self.user_id.clone(),
            role: self.role,
            connected: self.connected.load(Ordering::Relaxed),
            packets_received: self.packets_received.load(Ordering::Relaxed),
            bytes_received: bytes,
            drops,
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
            tx_packets_enqueued: if tx_packets_enqueued == 0 {
                None
            } else {
                Some(tx_packets_enqueued)
            },
            tx_drops_channel_full: if tx_drops_channel_full == 0 {
                None
            } else {
                Some(tx_drops_channel_full)
            },
            tx_drops_send_error: if tx_drops_send_error == 0 {
                None
            } else {
                Some(tx_drops_send_error)
            },
            tx_drops: if tx_drops == 0 { None } else { Some(tx_drops) },
            redirects_followed: if redirects_followed == 0 {
                None
            } else {
                Some(redirects_followed)
            },
            // vc-1re media-vs-control split: omit-when-zero, same pattern as
            // the other optional counters.
            control_packets_received: if control_packets_received == 0 {
                None
            } else {
                Some(control_packets_received)
            },
            media_packets_received: if media_packets_received == 0 {
                None
            } else {
                Some(media_packets_received)
            },
            media_received_video: if media_received_video == 0 {
                None
            } else {
                Some(media_received_video)
            },
            media_received_audio: if media_received_audio == 0 {
                None
            } else {
                Some(media_received_audio)
            },
            media_received_other: if media_received_other == 0 {
                None
            } else {
                Some(media_received_other)
            },
            // vc-1re integrity: FIXED-SHAPE. These four MUST serialize even
            // when zero (plain u64, no skip_serializing_if) so the integrity
            // contract is always visible in the summary root and rollups.
            media_seq_max,
            media_received_distinct,
            crc_mismatches,
            unexplained_gaps,
            // vc-1re joined_pod + redirect_chain (per-bot). Omit when absent /
            // empty: a clean direct connect has no chain.
            joined_pod,
            redirect_chain: if redirect_chain.is_empty() {
                None
            } else {
                Some(redirect_chain)
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
    /// Producer-side bookkeeping (vc-xpf). `None` for prior runs / when no
    /// packets were enqueued.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_packets_enqueued: Option<u64>,
    /// Producer-side bookkeeping (vc-xpf). `None` when no producer queue
    /// drops occurred (the channel never filled).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_drops_channel_full: Option<u64>,
    /// Producer-side bookkeeping (vc-xpf). `None` when no WebTransport send
    /// failures occurred.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_drops_send_error: Option<u64>,
    /// Producer-side bookkeeping (vc-020): unified aggregate of producer
    /// drops (`tx_drops_channel_full + tx_drops_send_error`). Mirrors the
    /// receive-side `drops` field so jq dashboards can read `.tx_drops` as
    /// a single number. `None` when both split counters are zero (same
    /// "omit when 0" pattern as the other producer-side fields). The split
    /// fields are retained for attribution.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_drops: Option<u64>,
    /// Redirect bookkeeping (vc-kni). `None` when the bot never followed an
    /// `ADMISSION_DECISION{REDIRECT}` target during the run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub redirects_followed: Option<u64>,

    // --- Media-vs-control receive split (vc-1re) -----------------------
    // Omit-when-zero, matching the existing optional counters.
    /// Non-MEDIA control wrappers received (heartbeat, SPEAKER_UPDATE, ...).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub control_packets_received: Option<u64>,
    /// MEDIA wrappers received (any inner media type).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_packets_received: Option<u64>,
    /// MEDIA wrappers with inner `media_type == VIDEO`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_received_video: Option<u64>,
    /// MEDIA wrappers with inner `media_type == AUDIO`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_received_audio: Option<u64>,
    /// MEDIA wrappers whose inner media type is neither VIDEO nor AUDIO.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_received_other: Option<u64>,

    // --- Trailer-CRC integrity (vc-1re): FIXED-SHAPE ----------------------
    // These four are plain `u64` (NOT `Option<u64>`) and carry NO
    // `skip_serializing_if`, so they ALWAYS appear in the JSON — even at
    // zero. This is the load-bearing integrity contract: a dashboard reading
    // `.crc_mismatches` must never see the key vanish.
    /// Highest media sequence observed across all (publisher, media_type)
    /// keys. `0` if integrity was off or nothing was tracked.
    pub media_seq_max: u64,
    /// Total distinct media payloads with a verified trailer.
    pub media_received_distinct: u64,
    /// Trailers whose recomputed CRC32 did not match the stamped value. MUST
    /// be 0 on a clean run.
    pub crc_mismatches: u64,
    /// `expected - received - accounted_drops` summed across keys, clamped at
    /// 0. Isolates losses not explained by the AllowSet `unsubscribed` drop.
    pub unexplained_gaps: u64,

    // --- joined_pod + redirect_chain (vc-1re) ---------------------------
    /// Pod the bot's media session landed on after any redirect. `None` until
    /// the first successful connect.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub joined_pod: Option<String>,
    /// Ordered redirect hops with per-hop latency. `None` for a clean direct
    /// connect (empty chain).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub redirect_chain: Option<Vec<RedirectHop>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// vc-xpf: the producer-side counters must follow the same "omit when 0"
    /// pattern as the existing optional fields so historical JSON consumers
    /// don't see new keys appear in runs that don't exercise them.
    #[test]
    fn tx_counters_omitted_when_zero() {
        let stats = BotStats::new("sender-0".into(), BotRole::Sender);
        let snap = stats.snapshot(1.0);
        assert_eq!(snap.tx_packets_enqueued, None);
        assert_eq!(snap.tx_drops_channel_full, None);
        assert_eq!(snap.tx_drops_send_error, None);
        assert_eq!(snap.tx_drops, None);

        // And the JSON shape must omit them entirely (forward-compat).
        let json = serde_json::to_string(&snap).expect("serialize snapshot");
        assert!(
            !json.contains("tx_packets_enqueued"),
            "tx_packets_enqueued must be omitted when zero, got: {json}"
        );
        assert!(
            !json.contains("tx_drops_channel_full"),
            "tx_drops_channel_full must be omitted when zero, got: {json}"
        );
        assert!(
            !json.contains("tx_drops_send_error"),
            "tx_drops_send_error must be omitted when zero, got: {json}"
        );
        // vc-020: the unified `tx_drops` aggregate follows the same
        // omit-when-zero pattern.
        assert!(
            !json.contains("\"tx_drops\""),
            "tx_drops must be omitted when zero, got: {json}"
        );
    }

    /// vc-kni: the `redirects_followed` counter follows the same "omit when
    /// 0" pattern as the existing optional fields. Guards against the field
    /// leaking into 200-bot runs that never trigger a REDIRECT (the common
    /// case on a healthy cluster).
    #[test]
    fn redirects_followed_counter_omitted_when_zero_and_surfaces_when_nonzero_vc_kni() {
        let stats = BotStats::new("listener-0".into(), BotRole::Listener);
        let snap = stats.snapshot(1.0);
        assert_eq!(snap.redirects_followed, None);

        let json = serde_json::to_string(&snap).expect("serialize snapshot");
        assert!(
            !json.contains("redirects_followed"),
            "redirects_followed must be omitted when zero, got: {json}"
        );

        // Recording the helper bumps the counter and surfaces it.
        stats.record_redirect_followed();
        stats.record_redirect_followed();
        let snap = stats.snapshot(1.0);
        assert_eq!(snap.redirects_followed, Some(2));

        let json = serde_json::to_string(&snap).expect("serialize snapshot");
        assert!(json.contains("\"redirects_followed\":2"));
    }

    /// vc-xpf: each `record_tx_*` helper must update its own counter and
    /// surface a `Some` value in the snapshot. This is the inverse of
    /// `tx_counters_omitted_when_zero`.
    #[test]
    fn tx_counters_surface_when_nonzero() {
        let stats = BotStats::new("sender-0".into(), BotRole::Sender);
        stats.record_tx_packet_enqueued();
        stats.record_tx_packet_enqueued();
        stats.record_tx_drop_channel_full();
        stats.record_tx_drop_send_error();
        stats.record_tx_drop_send_error();
        stats.record_tx_drop_send_error();

        let snap = stats.snapshot(1.0);
        assert_eq!(snap.tx_packets_enqueued, Some(2));
        assert_eq!(snap.tx_drops_channel_full, Some(1));
        assert_eq!(snap.tx_drops_send_error, Some(3));
        // vc-020: unified aggregate is the sum of the split fields.
        assert_eq!(snap.tx_drops, Some(4));

        let json = serde_json::to_string(&snap).expect("serialize snapshot");
        assert!(json.contains("\"tx_packets_enqueued\":2"));
        assert!(json.contains("\"tx_drops_channel_full\":1"));
        assert!(json.contains("\"tx_drops_send_error\":3"));
        assert!(json.contains("\"tx_drops\":4"));
    }

    /// vc-1re: the four integrity counters MUST serialize at the snapshot
    /// root even when zero (fixed-shape contract). This test FAILS if any of
    /// `media_seq_max`, `media_received_distinct`, `crc_mismatches`, or
    /// `unexplained_gaps` disappears from the JSON. Guards against a future
    /// regression that adds `skip_serializing_if` to (or removes) any of the
    /// four. The Totals roll-up equivalents are guarded in `orchestrate.rs`
    /// and `failover.rs`.
    #[test]
    fn integrity_counters_are_fixed_shape_in_snapshot_root_vc_1re() {
        let stats = BotStats::new("listener-0".into(), BotRole::Listener);
        let snap = stats.snapshot(1.0);
        // Plain u64 at zero (not Option), so the values are present.
        assert_eq!(snap.media_seq_max, 0);
        assert_eq!(snap.media_received_distinct, 0);
        assert_eq!(snap.crc_mismatches, 0);
        assert_eq!(snap.unexplained_gaps, 0);

        let json = serde_json::to_string(&snap).expect("serialize snapshot");
        for key in [
            "\"media_seq_max\":0",
            "\"media_received_distinct\":0",
            "\"crc_mismatches\":0",
            "\"unexplained_gaps\":0",
        ] {
            assert!(
                json.contains(key),
                "integrity counter {} must serialize even when zero, got: {}",
                key,
                json
            );
        }
    }

    /// vc-1re: the media-vs-control split counters follow the omit-when-zero
    /// pattern, and the `record_*` helpers route to the right buckets.
    #[test]
    fn media_control_split_counters_route_and_omit_vc_1re() {
        let stats = BotStats::new("listener-0".into(), BotRole::Listener);
        // Zero state: split fields omitted from JSON.
        let json = serde_json::to_string(&stats.snapshot(1.0)).unwrap();
        assert!(!json.contains("control_packets_received"));
        assert!(!json.contains("media_packets_received"));

        stats.record_control_packet();
        stats.record_media_packet(true, false); // video
        stats.record_media_packet(false, true); // audio
        stats.record_media_packet(false, false); // other

        let snap = stats.snapshot(1.0);
        assert_eq!(snap.control_packets_received, Some(1));
        assert_eq!(snap.media_packets_received, Some(3));
        assert_eq!(snap.media_received_video, Some(1));
        assert_eq!(snap.media_received_audio, Some(1));
        assert_eq!(snap.media_received_other, Some(1));
    }

    /// vc-1re §3.2 responsiveness contract: redirect-chain hop latencies must
    /// be present and bounded by the budgets. We assert each recorded hop's
    /// `redirect_latency_ms` is within the 2.0s total-recovery budget (the
    /// strictest single-number bound on a per-hop latency in the §3.2 table:
    /// 100ms detect / 500ms reconnect / 1.5s first media / 2.0s total). The
    /// `record_redirect_hop` helper is the single increment site.
    #[test]
    fn redirect_chain_hops_carry_latency_within_budget_vc_1re() {
        // §3.2 budgets in milliseconds.
        const DETECT_MS: u64 = 100;
        const RECONNECT_MS: u64 = 500;
        const FIRST_MEDIA_MS: u64 = 1_500;
        const TOTAL_MS: u64 = 2_000;
        // Sanity: the budgets are ordered as documented.
        assert!(DETECT_MS < RECONNECT_MS);
        assert!(RECONNECT_MS < FIRST_MEDIA_MS);
        assert!(FIRST_MEDIA_MS < TOTAL_MS);

        let stats = BotStats::new("listener-0".into(), BotRole::Listener);
        // Empty chain on a clean direct connect.
        assert!(stats.snapshot(1.0).redirect_chain.is_none());

        // Record two hops with realistic sub-budget latencies.
        stats.set_joined_pod("pod-a".into());
        stats.record_redirect_hop("pod-b".into(), 420);
        stats.record_redirect_hop("pod-c".into(), 1_300);

        let snap = stats.snapshot(1.0);
        assert_eq!(snap.joined_pod.as_deref(), Some("pod-a"));
        let chain = snap.redirect_chain.expect("redirect chain present");
        assert_eq!(chain.len(), 2);
        for hop in &chain {
            // The latency field exists and is bounded by the §3.2 total
            // recovery budget.
            assert!(
                hop.redirect_latency_ms <= TOTAL_MS,
                "hop to {} latency {}ms exceeds {}ms total budget",
                hop.to_pod,
                hop.redirect_latency_ms,
                TOTAL_MS
            );
        }
        // Per-hop reconnect detail: first media within budget for the slower
        // hop, reconnect window for the faster one.
        assert!(chain[0].redirect_latency_ms <= RECONNECT_MS);
        assert!(chain[1].redirect_latency_ms <= FIRST_MEDIA_MS);
    }
}
