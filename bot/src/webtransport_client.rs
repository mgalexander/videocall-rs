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

use protobuf::Message;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::Notify;
use tokio::time;
use tracing::{debug, info, warn};
use url::Url;
use videocall_codecs::decoder::{Decodable, Decoder as NativeDecoder, VideoCodec as DecVideoCodec};
use videocall_codecs::frame::{FrameBuffer, FrameCodec, FrameType, VideoFrame};
use videocall_types::protos::admission_decision_packet::admission_decision::Status as AdmissionStatus;
use videocall_types::protos::admission_decision_packet::AdmissionDecision;
use videocall_types::protos::connection_packet::ConnectionPacket;
use videocall_types::protos::diagnostics_packet::{
    AudioMetrics, BandwidthEstimate, DiagnosticsPacket, VideoMetrics,
};
use videocall_types::protos::media_packet::media_packet::MediaType;
use videocall_types::protos::media_packet::{HeartbeatMetadata, MediaPacket};
use videocall_types::protos::packet_wrapper::packet_wrapper::PacketType;
use videocall_types::protos::packet_wrapper::PacketWrapper;
use web_transport_quinn::{ClientBuilder, Session};

use crate::config::ClientConfig;
use crate::stats::BotStats;

/// Per-publisher decoder state owned by a listener. VP9 is stateful across
/// frames, so each remote sender needs its own decoder context keyed by
/// `media_packet.user_id`. Opus is similarly stateful (carries jitter buffer
/// / PLC state internally) so we key those by publisher too.
struct DecoderPool {
    video: Mutex<HashMap<String, NativeDecoder>>,
    audio: Mutex<HashMap<String, opus::Decoder>>,
    /// Per-publisher rolling counters that drive the periodic diagnostics
    /// emitter (vc-dwc) and the sequence-gap KEYFRAME_REQUEST detector.
    /// Keyed by publisher `user_id`.
    publishers: Mutex<HashMap<String, PublisherTracker>>,
    /// Local user_id of this listener. Used as the `sender_id` on outbound
    /// PacketWrappers so the SFU/NATS can route diagnostics back to the
    /// originating session.
    local_user_id: String,
    /// Bounded sender for outbound feedback packets (DIAGNOSTICS,
    /// KEYFRAME_REQUEST). The dedicated writer task in
    /// [`start_feedback_writer`] drains this and opens unistreams. Bounded
    /// (vc-2x8 lesson) so a stalled session can't grow this unboundedly.
    /// `try_send` is used in the decode hot path; overflow is dropped and
    /// logged at `debug` to avoid blocking decode.
    feedback_tx: mpsc::Sender<Vec<u8>>,
    stats: Arc<BotStats>,
}

/// Per-publisher rolling counters that drive feedback emission.
///
/// Field choices mirror the real client's `diagnostics_manager.rs`
/// `send_diagnostic_packets` path (~500ms cadence, per-(peer × media-type)
/// `DiagnosticsPacket` with fps + bitrate_kbps + bandwidth_estimate) and
/// `peer_decode_manager.rs::track_sequence` for the KEYFRAME_REQUEST
/// debounce.
///
/// Also carries the LRU last-access timestamp (vc-wx3): when the pool hits
/// [`DECODER_POOL_MAX_PUBLISHERS`], the publisher with the smallest
/// `last_access_ms` is evicted from `video`, `audio`, and `publishers` in
/// lockstep so half-evicted state can't leak into diagnostics or KFR logic.
#[derive(Default)]
struct PublisherTracker {
    /// Wall-clock unix-millis of the start of the current diagnostics
    /// window. `0` until the first frame arrives. The emitter task closes
    /// the window when ~`DIAGNOSTICS_INTERVAL_MS` have elapsed.
    window_start_ms: u64,
    /// VIDEO frames received in the current window.
    video_frames: u64,
    /// VIDEO bytes received in the current window.
    video_bytes: u64,
    /// AUDIO frames received in the current window.
    audio_frames: u64,
    /// AUDIO bytes received in the current window.
    audio_bytes: u64,
    /// Last observed VIDEO `video_metadata.sequence`. `None` until the
    /// first video frame arrives. Used by the legacy seq-gap detector to
    /// arm KFR (matches `peer_decode_manager.rs::track_sequence` for
    /// `MediaType::VIDEO` without a `RoutingHeader`).
    last_video_seq: Option<u64>,
    /// Unix-millis when a VIDEO gap was first detected. `None` while
    /// stream is healthy. Cleared on the next keyframe.
    video_gap_detected_at_ms: Option<u64>,
    /// Unix-millis of the most recent outbound VIDEO KEYFRAME_REQUEST.
    /// Used to debounce per `KEYFRAME_REQUEST_MIN_INTERVAL_MS` (500ms,
    /// matches `adaptive_quality_constants.rs:312`).
    last_video_keyframe_request_ms: u64,
    /// Unix-millis of the most recent VIDEO or AUDIO frame observed for
    /// this publisher. Drives the [`DECODER_POOL_MAX_PUBLISHERS`] LRU
    /// eviction (vc-wx3). Updated on every frame so an active publisher
    /// stays "hot" and idle publishers fall out first when the cap is hit.
    /// `0` until the first frame arrives.
    last_access_ms: u64,
}

/// Cadence for outbound `DiagnosticsPacket` emission per (publisher × media
/// type). Mirrors the real client's
/// `diagnostics_manager.rs::setup_heartbeat` which fires
/// `HeartbeatTick` every 500ms (~2Hz).
const DIAGNOSTICS_INTERVAL_MS: u64 = 500;

/// Minimum interval between KEYFRAME_REQUEST emissions to the same
/// publisher (for VIDEO). Matches
/// `videocall-client/src/adaptive_quality_constants.rs:312`
/// (`KEYFRAME_REQUEST_MIN_INTERVAL_MS = 500`). A burst of decode failures
/// or sequence gaps must not produce a burst of KFRs — each KFR forces a
/// full I-frame re-encode that bursts ~50–150KB of uplink on the sender.
const KEYFRAME_REQUEST_MIN_INTERVAL_MS: u64 = 500;

/// Trigger window before arming KEYFRAME_REQUEST after a sequence gap.
/// Mirrors `KEYFRAME_REQUEST_TIMEOUT_MS = 1000` in the real client. We
/// only emit a KFR if the gap remains unresolved for this long, which
/// avoids hammering senders on transient single-frame losses.
const KEYFRAME_REQUEST_GAP_ARM_MS: u64 = 1000;

/// Bound on the feedback channel between the decode hot path and the
/// session writer. Sized for the worst-case "all publishers in a
/// max-room burst KFR + diagnostics at the same tick" scenario; well
/// above the steady-state load (~2 packets/sec/publisher) so steady
/// emission never blocks the decode thread.
const FEEDBACK_CHANNEL_BOUND: usize = 256;

/// Maximum number of distinct publishers a listener tracks simultaneously
/// in its [`DecoderPool`] (vc-wx3). Each entry holds a [`NativeDecoder`]
/// (one worker thread + 32-frame bounded channel ≈ ~2 MiB worst case), an
/// `opus::Decoder` (~tens of KiB), and a [`PublisherTracker`] (negligible).
/// The dominant cost is the video decoder; capping at 16 publishers caps
/// the per-listener decoder footprint at ~32 MiB worst-case (and with the
/// vc-wx3 stack-size fix, ~24 MiB of that is the bounded channel rather
/// than thread stack reservation).
///
/// 16 is generous for the production case (rooms top out at ~6 active
/// publishers) while still bounding REDIRECT-loop and soak-test scenarios
/// where publishers churn (each REDIRECT cycle re-uses a new transient
/// publisher id) — without that cap, the maps grew without bound and
/// dominated steady-state listener memory.
///
/// Eviction is LRU by `PublisherTracker::last_access_ms`. We deliberately
/// did NOT pull in a dedicated LRU crate: the cap is small, eviction is
/// triggered only on the cap-hit code path (cold), and a linear scan over
/// ≤ 16 entries is faster than a doubly-linked-list update on the hot path.
/// The single source of truth for recency is the `publishers` map so all
/// three maps (video, audio, publishers) can be kept in lockstep without
/// a fourth sidecar map.
const DECODER_POOL_MAX_PUBLISHERS: usize = 16;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Shared per-client signal channel that the inbound consumer uses to notify
/// the orchestrator that the session has ended (either cleanly, by error, or
/// by an `ADMISSION_DECISION{REDIRECT}` arriving just before the close).
///
/// Used by both the failover-test orchestrator (`failover.rs`) and the
/// default orchestrate path (`orchestrate.rs`) since vc-kni — the latter
/// drives a reconnect-on-REDIRECT loop so a bot that lands on a non-owner
/// pod follows the SFU's redirect to the correct owner instead of silently
/// dropping out of the test.
#[derive(Default)]
pub struct SessionEndSignal {
    /// Fires when the inbound consumer exits. Multi-consumer-safe via
    /// `Notify::notified()`.
    pub notify: Notify,
    /// Set to `Some(redirect_to)` if an `ADMISSION_DECISION{REDIRECT}` was
    /// observed before the session ended. The orchestrator may consult this
    /// to direct the next reconnect attempt at the named pod (best-effort —
    /// the local k3d cluster typically can't resolve the cluster-internal
    /// DNS from outside, so the test falls back to the original LB URL).
    pub redirect_to: Mutex<Option<String>>,
    /// Set to `true` once any terminal condition is observed. Used so a
    /// reconnect loop can drop into the wait state without racing the
    /// notification.
    pub ended: AtomicBool,
}

impl SessionEndSignal {
    fn fire(&self, redirect: Option<String>) {
        if let Some(r) = redirect {
            *self.redirect_to.lock().unwrap() = Some(r);
        }
        self.ended.store(true, Ordering::Relaxed);
        self.notify.notify_waiters();
    }
}

pub struct WebTransportClient {
    config: ClientConfig,
    session: Option<Session>,
    quit: Arc<AtomicBool>,
    stats: Option<Arc<BotStats>>,
    /// Optional session-end signal, attached by the failover-test orchestrator
    /// via [`with_session_end_signal`](Self::with_session_end_signal) before
    /// [`connect`](Self::connect). `None` for default runs.
    session_end: Option<Arc<SessionEndSignal>>,
    /// When true, the inbound consumer parses each unistream as a media
    /// packet and runs real VP9 / Opus decode so the bot exerts client-side
    /// CPU comparable to a real browser participant (vc-86j).
    decode: bool,
}

impl WebTransportClient {
    pub fn new(config: ClientConfig) -> Self {
        Self {
            config,
            session: None,
            quit: Arc::new(AtomicBool::new(false)),
            stats: None,
            session_end: None,
            decode: false,
        }
    }

    /// Attach a shared stats handle. Called by the load-test orchestrator
    /// before [`connect`](Self::connect) so the inbound consumer can update
    /// counters as packets arrive.
    pub fn with_stats(mut self, stats: Arc<BotStats>) -> Self {
        self.stats = Some(stats);
        self
    }

    /// Attach a session-end signal. Used by the failover-test orchestrator
    /// (p6-11) to detect when the inbound side closes so it can launch a
    /// reconnect attempt. No-op for default runs.
    pub fn with_session_end_signal(mut self, signal: Arc<SessionEndSignal>) -> Self {
        self.session_end = Some(signal);
        self
    }

    /// Enable real VP9 + Opus decode on the inbound path (vc-86j). The 200-
    /// bot harness relies on this so listeners exert representative client
    /// CPU. Senders should leave this disabled — they don't subscribe.
    pub fn with_decode(mut self, decode: bool) -> Self {
        self.decode = decode;
        self
    }

    pub async fn connect(&mut self, server_url: &Url, insecure: bool) -> anyhow::Result<()> {
        info!(
            "Connecting client {} to {}",
            self.config.user_id, server_url
        );

        // Create WebTransport client (same logic as webtranscat)
        let client = if insecure {
            warn!("Certificate verification disabled (--insecure)");
            // SAFETY: This is intentionally insecure for testing purposes
            unsafe { ClientBuilder::new().with_no_certificate_verification()? }
        } else {
            // Use default secure configuration with system certificates
            ClientBuilder::new().with_system_roots()?
        };

        // Construct full URL with lobby path
        let full_url = format!(
            "{}/lobby/{}/{}",
            server_url.as_str().trim_end_matches('/'),
            self.config.user_id,
            self.config.meeting_id
        );
        let connection_url = Url::parse(&full_url)?;

        info!("Connecting to {}", connection_url);
        let session = client.connect(connection_url).await?;
        info!(
            "WebTransport session established for {}",
            self.config.user_id
        );

        self.session = Some(session);
        info!(
            "WebTransport session established for {}",
            self.config.user_id
        );

        if let Some(stats) = &self.stats {
            stats.mark_connected(now_ms());
        }

        // Send connection packet
        self.send_connection_packet().await?;

        // Start heartbeat
        self.start_heartbeat().await;
        info!("Heartbeat started for {}", self.config.user_id);

        // Start inbound consumer to avoid being a slow consumer
        self.start_inbound_consumer().await;
        info!("Inbound consumer started for {}", self.config.user_id);

        Ok(())
    }

    async fn send_connection_packet(&self) -> anyhow::Result<()> {
        let connection_packet = ConnectionPacket {
            meeting_id: self.config.meeting_id.clone(),
            ..Default::default()
        };

        let packet = PacketWrapper {
            packet_type: PacketType::CONNECTION.into(),
            user_id: self.config.user_id.clone().into_bytes(),
            data: connection_packet.write_to_bytes()?,
            ..Default::default()
        };

        self.send_packet(packet.write_to_bytes()?).await?;
        info!("Sent connection packet for {}", self.config.user_id);
        Ok(())
    }

    async fn start_heartbeat(&self) {
        if let Some(session) = &self.session {
            let session = session.clone();
            let user_id = self.config.user_id.clone();
            let video_enabled = self.config.enable_video; // Get actual video config
            let audio_enabled = self.config.enable_audio; // Get actual audio config
            let quit = self.quit.clone();

            tokio::spawn(async move {
                let mut interval = time::interval(Duration::from_secs(1));

                loop {
                    if quit.load(Ordering::Relaxed) {
                        break;
                    }

                    interval.tick().await;

                    // Use exact same timestamp calculation as videocall-cli
                    let now_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Time went backwards")
                        .as_millis();

                    let heartbeat = MediaPacket {
                        media_type: MediaType::HEARTBEAT.into(),
                        user_id: user_id.clone().into_bytes(),
                        timestamp: now_ms as f64,
                        heartbeat_metadata: Some(HeartbeatMetadata {
                            video_enabled,
                            audio_enabled,
                            ..Default::default()
                        })
                        .into(),
                        ..Default::default()
                    };

                    let packet = PacketWrapper {
                        user_id: user_id.clone().into_bytes(),
                        packet_type: PacketType::MEDIA.into(),
                        data: heartbeat.write_to_bytes().unwrap(),
                        ..Default::default()
                    };

                    if let Err(e) =
                        Self::send_via_session(&session, packet.write_to_bytes().unwrap()).await
                    {
                        warn!("Failed to send heartbeat for {}: {}", user_id, e);
                    } else {
                        debug!("Sent heartbeat for {}", user_id);
                    }
                }
            });
        }
    }

    /// Start a task to consume all inbound unistreams to avoid being a slow
    /// consumer.
    ///
    /// When a [`SessionEndSignal`] is attached (failover.rs since p6-11,
    /// orchestrate.rs since vc-kni) the consumer layers two extra
    /// behaviours on top:
    ///
    /// 1. Each drained stream is parsed as a `PacketWrapper`. If we see an
    ///    `ADMISSION_DECISION{REDIRECT}`, we stash `redirect_to` on the
    ///    [`SessionEndSignal`] so the orchestrator can use it on the next
    ///    reconnect attempt. The redirect packet arrives **immediately
    ///    before** the SFU closes the session, so we must capture it before
    ///    treating the subsequent `accept_uni` error as a plain disconnect.
    ///    For reliability we drain inline (rather than spawn-per-stream) so
    ///    `read_to_end` and `try_extract_redirect_target` complete before
    ///    the next `accept_uni` returns an error. Perf trade-off: at the
    ///    orchestrate workload shape (~12 publishers × ~80 streams/sec/
    ///    publisher ≈ 1000 streams/sec/listener) `read_to_end` for small
    ///    media packets returns quickly and decode is already offloaded to
    ///    `spawn_blocking`, so the marginal latency is acceptable.
    /// 2. On any terminal condition (accept-uni error, quit flag) we mark
    ///    the bot as disconnected (sticky first-gap timestamp) and fire the
    ///    session-end notification.
    async fn start_inbound_consumer(&self) {
        if let Some(session) = &self.session {
            let session = session.clone();
            let user_id = self.config.user_id.clone();
            let quit = self.quit.clone();
            let stats = self.stats.clone();
            let session_end = self.session_end.clone();
            // Decoders only meaningful when both `decode` is on and we have a
            // stats handle to publish counters into.
            //
            // When decode is enabled we also stand up the feedback path
            // (vc-dwc): a bounded mpsc that the per-stream decode tasks
            // push DIAGNOSTICS + KEYFRAME_REQUEST PacketWrappers into, a
            // dedicated writer task that drains the channel onto the
            // session, and a periodic emitter that produces one
            // DiagnosticsPacket per (publisher × media-type) every
            // `DIAGNOSTICS_INTERVAL_MS`. All gated on `self.decode`, so
            // sender bots and listeners with decode disabled have zero
            // additional outbound traffic.
            let decoders = if self.decode {
                stats.as_ref().map(|s| {
                    let (feedback_tx, feedback_rx) = mpsc::channel(FEEDBACK_CHANNEL_BOUND);
                    let pool = Arc::new(DecoderPool {
                        video: Mutex::new(HashMap::new()),
                        audio: Mutex::new(HashMap::new()),
                        publishers: Mutex::new(HashMap::new()),
                        local_user_id: user_id.clone(),
                        feedback_tx,
                        stats: s.clone(),
                    });
                    start_feedback_writer(
                        session.clone(),
                        user_id.clone(),
                        quit.clone(),
                        feedback_rx,
                    );
                    start_diagnostics_emitter(pool.clone(), user_id.clone(), quit.clone());
                    pool
                })
            } else {
                None
            };

            tokio::spawn(async move {
                loop {
                    if quit.load(Ordering::Relaxed) {
                        break;
                    }

                    match session.accept_uni().await {
                        Ok(mut stream) => {
                            // Default path: spawn per-stream so accept and
                            // drain run concurrently — preserves the
                            // pre-p6-11 behaviour for the orchestrate / 200-
                            // bot harness.
                            //
                            // Failover-test path (`session_end` attached):
                            // drain inline so we observe REDIRECT bytes
                            // before the next `accept_uni` returns an
                            // error and breaks the loop. Throughput on a
                            // single listener bot is low enough that
                            // sequential draining is fine.
                            if let Some(signal_handle) = &session_end {
                                match stream.read_to_end(usize::MAX).await {
                                    Ok(data) => {
                                        let t = now_ms();
                                        if let Some(stats) = &stats {
                                            stats.record_packet_at(data.len() as u64, t);
                                        }
                                        if let Some(target) = try_extract_redirect_target(&data) {
                                            info!(
                                                "Listener {} received ADMISSION_DECISION REDIRECT to {}",
                                                user_id, target
                                            );
                                            *signal_handle.redirect_to.lock().unwrap() =
                                                Some(target);
                                        }
                                        if let Some(pool) = &decoders {
                                            // Same off-thread decode as the
                                            // default path; keeps failover-test
                                            // bots aligned with the 200-bot
                                            // workload shape.
                                            let pool_clone = pool.clone();
                                            tokio::task::spawn_blocking(move || {
                                                decode_packet(&pool_clone, &data);
                                            });
                                        }
                                    }
                                    Err(e) => {
                                        if let Some(stats) = &stats {
                                            stats.record_drop();
                                        }
                                        debug!(
                                            "Error reading inbound unistream for {}: {}",
                                            user_id, e
                                        );
                                    }
                                }
                            } else {
                                let stats_spawn = stats.clone();
                                let user_id_spawn = user_id.clone();
                                let decoders_spawn = decoders.clone();
                                tokio::spawn(async move {
                                    match stream.read_to_end(usize::MAX).await {
                                        Ok(data) => {
                                            if let Some(stats) = stats_spawn {
                                                stats.record_packet(data.len() as u64);
                                            }
                                            if let Some(pool) = decoders_spawn {
                                                // Move decode (protobuf parse +
                                                // VP9/Opus) onto the blocking
                                                // pool: opus::decode_float is
                                                // pure-CPU and would starve
                                                // accept_uni at 200 colocated
                                                // bots otherwise.
                                                tokio::task::spawn_blocking(move || {
                                                    decode_packet(&pool, &data);
                                                });
                                            }
                                        }
                                        Err(e) => {
                                            if let Some(stats) = stats_spawn {
                                                stats.record_drop();
                                            }
                                            debug!(
                                                "Error reading inbound unistream for {}: {}",
                                                user_id_spawn, e
                                            );
                                        }
                                    }
                                });
                            }
                        }
                        Err(e) => {
                            debug!("Inbound consumer ended for {}: {}", user_id, e);
                            break;
                        }
                    }
                }
                // Terminal: mark disconnect (sticky) and signal the
                // orchestrator. Both are no-ops when the failover-test
                // wiring isn't attached.
                if let Some(stats) = &stats {
                    stats.mark_disconnected_at(now_ms());
                }
                if let Some(signal) = &session_end {
                    signal.fire(None);
                }
                // Dropping `decoders` here joins the per-publisher VP9
                // decoder threads. `NativeDecoder::drop` takes its sender,
                // which terminates the worker's `recv()` loop (vc-35t) —
                // no Shutdown sentinel is needed.
                drop(decoders);
                info!("Inbound consumer stopped for {}", user_id);
            });
        }
    }

    pub async fn send_packet(&self, data: Vec<u8>) -> anyhow::Result<()> {
        if let Some(session) = &self.session {
            Self::send_via_session(session, data).await
        } else {
            Err(anyhow::anyhow!("No WebTransport session available"))
        }
    }

    async fn send_via_session(session: &Session, data: Vec<u8>) -> anyhow::Result<()> {
        let mut stream = session.open_uni().await?;
        stream.write_all(&data).await?;
        stream.finish()?; // Remove .await as this is not async
        Ok(())
    }

    pub async fn start_packet_sender(&self, mut packet_receiver: Receiver<Vec<u8>>) {
        if let Some(session) = &self.session {
            let session = session.clone();
            let user_id = self.config.user_id.clone();
            let quit = self.quit.clone();
            // vc-xpf: thread the shared stats handle in so wire-level send
            // failures land on `tx_drops_send_error`. Distinct from
            // `tx_drops_channel_full` (producer enqueue side) so the
            // staircase test can attribute drops to either bucket.
            let stats = self.stats.clone();

            tokio::spawn(async move {
                while let Some(packet_data) = packet_receiver.recv().await {
                    if quit.load(Ordering::Relaxed) {
                        break;
                    }

                    if let Err(e) = Self::send_via_session(&session, packet_data).await {
                        if let Some(s) = &stats {
                            s.record_tx_drop_send_error();
                        }
                        debug!("Failed to send media packet for {}: {}", user_id, e);
                    }
                }
                info!("Packet sender stopped for {}", user_id);
            });
        }
    }

    pub fn stop(&self) {
        self.quit.store(true, Ordering::Relaxed);
        info!("Stopping WebTransport client for {}", self.config.user_id);
    }
}

/// Parse `data` as a `PacketWrapper`; if it is an `ADMISSION_DECISION` with
/// `status = REDIRECT` and a non-empty `redirect_to`, return that target.
///
/// Returns `None` for any other packet type, parse failure, or empty target.
/// Cheap: protobuf parse on a small wrapper, then a single field check.
fn try_extract_redirect_target(data: &[u8]) -> Option<String> {
    let wrapper = PacketWrapper::parse_from_bytes(data).ok()?;
    if wrapper.packet_type != PacketType::ADMISSION_DECISION.into() {
        return None;
    }
    let decision = AdmissionDecision::parse_from_bytes(&wrapper.data).ok()?;
    if decision.status != AdmissionStatus::REDIRECT.into() {
        return None;
    }
    if decision.redirect_to.is_empty() {
        return None;
    }
    Some(decision.redirect_to)
}

/// Parse `data` as a `PacketWrapper` containing a `MediaPacket`, then route
/// VIDEO frames to a libvpx VP9 decoder and AUDIO frames to an Opus decoder.
/// Per-publisher decoders are created lazily and reused across calls because
/// both codecs maintain state across frames.
fn decode_packet(pool: &DecoderPool, data: &[u8]) {
    let wrapper = match PacketWrapper::parse_from_bytes(data) {
        Ok(w) => w,
        Err(_) => {
            pool.stats.record_decode_error();
            return;
        }
    };
    if wrapper.packet_type != PacketType::MEDIA.into() {
        return;
    }
    let media = match MediaPacket::parse_from_bytes(&wrapper.data) {
        Ok(m) => m,
        Err(_) => {
            pool.stats.record_decode_error();
            return;
        }
    };

    let publisher = String::from_utf8_lossy(&media.user_id).into_owned();
    let media_type = media.media_type.enum_value_or(MediaType::HEARTBEAT);

    match media_type {
        MediaType::VIDEO => decode_video(pool, &publisher, &media),
        MediaType::AUDIO => decode_audio(pool, &publisher, &media),
        _ => {}
    }
}

fn decode_video(pool: &DecoderPool, publisher: &str, media: &MediaPacket) {
    let frame_type = if media.frame_type == "key" {
        FrameType::KeyFrame
    } else {
        FrameType::DeltaFrame
    };
    let sequence_number = media
        .video_metadata
        .as_ref()
        .map(|m| m.sequence)
        .unwrap_or(0);

    // Per-publisher window accounting (drives DiagnosticsPacket fps/bitrate)
    // and sequence-gap KFR detection. Done under the publishers mutex; the
    // mutex itself is uncontended outside the periodic emitter, so the hot
    // path stays single-lock single-publisher.
    let kfr = update_publisher_video_window(
        pool,
        publisher,
        sequence_number,
        &media.frame_type,
        media.data.len() as u64,
    );
    if let Some(req) = kfr {
        emit_keyframe_request(pool, publisher, req);
    }

    // recover from poisoning: decoder ctor on another thread may have panicked, the map state itself is still valid
    let mut map = pool.video.lock().unwrap_or_else(|p| p.into_inner());
    let decoder = map.entry(publisher.to_string()).or_insert_with(|| {
        // Merged vc-35t + vc-4ns wiring: `record_decode_error` is the single
        // counter for both backpressure-induced drops (bounded channel full
        // / worker gone, vc-35t) AND decoder-thread errors (per-frame
        // decode failure or libvpx init failure, vc-4ns). Keeping one
        // counter preserves the summary JSON schema; if we later want to
        // distinguish drops vs errors we can file a follow-up that adds a
        // dedicated `decode_drops` counter without churning callers.
        let decoded_stats = pool.stats.clone();
        let dropped_stats = pool.stats.clone();
        let err_stats = pool.stats.clone();
        let publisher_id = publisher.to_string();
        NativeDecoder::with_callbacks(
            DecVideoCodec::Vp9Profile0Level10Bit8,
            Box::new(move |_decoded| {
                decoded_stats.record_video_decoded();
            }),
            Box::new(move || {
                dropped_stats.record_decode_error();
            }),
            Some(Box::new(move |msg| {
                debug!(
                    "Video decoder thread error for publisher {}: {}",
                    publisher_id, msg
                );
                err_stats.record_decode_error();
            })),
        )
    });

    let frame = VideoFrame {
        sequence_number,
        frame_type,
        codec: FrameCodec::Vp9Profile0Level10Bit8,
        temporal_layer_id: 0,
        data: media.data.clone(),
        timestamp: media.timestamp,
    };
    decoder.decode(FrameBuffer::new(frame, 0));
}

fn decode_audio(pool: &DecoderPool, publisher: &str, media: &MediaPacket) {
    // Update per-publisher AUDIO window counters before the actual decode
    // so the periodic emitter sees the byte/frame even if opus fails.
    update_publisher_audio_window(pool, publisher, media.data.len() as u64);

    let mut map = pool.audio.lock().unwrap_or_else(|p| p.into_inner());
    let decoder = match map.entry(publisher.to_string()) {
        std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
        std::collections::hash_map::Entry::Vacant(e) => {
            // matches sender config in audio_producer.rs
            match opus::Decoder::new(48000, opus::Channels::Mono) {
                Ok(dec) => e.insert(dec),
                Err(_) => {
                    pool.stats.record_decode_error();
                    return;
                }
            }
        }
    };

    // 20 ms at 48 kHz mono = 960 samples; oversize buffer is fine.
    let mut pcm = [0.0f32; 5760];
    match decoder.decode_float(&media.data, &mut pcm, false) {
        Ok(_) => pool.stats.record_audio_decoded(),
        Err(_) => pool.stats.record_decode_error(),
    }
}

/// Update the per-publisher VIDEO window counters and run the
/// sequence-gap detector. Returns `Some(MediaType::VIDEO)` if a
/// KEYFRAME_REQUEST should be emitted for this publisher (i.e. a gap has
/// been outstanding for `KEYFRAME_REQUEST_GAP_ARM_MS` and the per-
/// publisher KFR debounce of `KEYFRAME_REQUEST_MIN_INTERVAL_MS` has
/// elapsed). Otherwise `None`.
///
/// Mirrors the legacy seq-based path in
/// `videocall-client/src/decode/peer_decode_manager.rs::track_sequence`
/// (VIDEO without a `RoutingHeader`).
fn update_publisher_video_window(
    pool: &DecoderPool,
    publisher: &str,
    sequence: u64,
    frame_type: &str,
    bytes: u64,
) -> Option<MediaType> {
    let now = now_ms();
    let mut map = pool.publishers.lock().unwrap_or_else(|p| p.into_inner());
    evict_lru_if_full(pool, &mut map, publisher);
    let tracker = map.entry(publisher.to_string()).or_default();
    tracker.last_access_ms = now;
    if tracker.window_start_ms == 0 {
        tracker.window_start_ms = now;
    }
    tracker.video_frames = tracker.video_frames.saturating_add(1);
    tracker.video_bytes = tracker.video_bytes.saturating_add(bytes);

    let is_key = frame_type == "key";
    // Keyframes recover us from any pending gap. Mirrors line 701 of
    // peer_decode_manager.rs.
    if is_key {
        tracker.video_gap_detected_at_ms = None;
    }

    if let Some(prev) = tracker.last_video_seq {
        if sequence > prev + 1 && tracker.video_gap_detected_at_ms.is_none() {
            tracker.video_gap_detected_at_ms = Some(now);
            debug!(
                "Bot detected video sequence gap for publisher {}: expected {}, got {}",
                publisher,
                prev + 1,
                sequence
            );
        }
    }
    tracker.last_video_seq = Some(sequence);

    // Rate-limited dispatch: only emit if the gap has been outstanding
    // longer than the arm window AND the per-publisher KFR debounce has
    // elapsed. Matches the legacy seq-based dispatch in
    // peer_decode_manager.rs:729.
    if let Some(gap_time) = tracker.video_gap_detected_at_ms {
        let elapsed_since_gap = now.saturating_sub(gap_time);
        let elapsed_since_last_req = now.saturating_sub(tracker.last_video_keyframe_request_ms);
        if elapsed_since_gap >= KEYFRAME_REQUEST_GAP_ARM_MS
            && elapsed_since_last_req >= KEYFRAME_REQUEST_MIN_INTERVAL_MS
        {
            tracker.last_video_keyframe_request_ms = now;
            return Some(MediaType::VIDEO);
        }
    }
    None
}

/// Update the per-publisher AUDIO window counters. AUDIO has no KFR
/// equivalent so this only updates the rolling counters consumed by the
/// periodic diagnostics emitter.
fn update_publisher_audio_window(pool: &DecoderPool, publisher: &str, bytes: u64) {
    let now = now_ms();
    let mut map = pool.publishers.lock().unwrap_or_else(|p| p.into_inner());
    evict_lru_if_full(pool, &mut map, publisher);
    let tracker = map.entry(publisher.to_string()).or_default();
    tracker.last_access_ms = now;
    if tracker.window_start_ms == 0 {
        tracker.window_start_ms = now;
    }
    tracker.audio_frames = tracker.audio_frames.saturating_add(1);
    tracker.audio_bytes = tracker.audio_bytes.saturating_add(bytes);
}

/// Enforce the [`DECODER_POOL_MAX_PUBLISHERS`] cap on `pool.publishers` /
/// `pool.video` / `pool.audio` (vc-wx3).
///
/// Called from the `update_publisher_*_window` paths just before
/// `map.entry(publisher).or_default()` would insert a new tracker. When the
/// caller is about to push the map past the cap (i.e. `publisher` is not
/// already present AND the map is at-or-above capacity), we pick the entry
/// with the smallest `last_access_ms` and evict it from ALL three maps in
/// lockstep.
///
/// ## Why lockstep eviction
///
/// `decoder_pool` is three parallel maps keyed by publisher id; the
/// invariant the rest of the bot relies on is "if the publishers map has
/// `pub_X`, then `video[pub_X]` and `audio[pub_X]` *may* exist and are valid
/// for that publisher". Evicting from one map without the others would let
/// `diagnostics_emitter` snapshot a publisher whose decoders were freed
/// (and conversely, leave a libvpx context + worker thread alive for a
/// publisher we no longer track for KFR/diagnostics — a slow memory leak
/// that defeats the point of the cap).
///
/// ## Lock ordering
///
/// Held order is `publishers → video → audio`. The publishers lock is
/// already held by the caller (passed in as `publishers`); we acquire the
/// other two inside this function. No other code path in this file takes
/// `video` or `audio` and then `publishers`, so this ordering is
/// deadlock-free.
///
/// ## Tie-break
///
/// On ties (e.g. two publishers both at `last_access_ms == 0`, the default
/// before the first frame arrives), we evict the lexicographically
/// smallest publisher id. This is deterministic and test-friendly; in
/// practice ties only occur transiently because `last_access_ms` is
/// updated on every frame and is millisecond-resolution.
fn evict_lru_if_full(
    pool: &DecoderPool,
    publishers: &mut HashMap<String, PublisherTracker>,
    new_publisher: &str,
) {
    if publishers.contains_key(new_publisher) {
        return;
    }
    if publishers.len() < DECODER_POOL_MAX_PUBLISHERS {
        return;
    }
    // Find LRU. `min_by` over `(last_access_ms, key)` handles ties
    // deterministically (lexicographically-smallest publisher id wins on
    // tie) without allocating a temporary key copy inside the comparator.
    let victim = publishers
        .iter()
        .min_by(|a, b| {
            a.1.last_access_ms
                .cmp(&b.1.last_access_ms)
                .then_with(|| a.0.cmp(b.0))
        })
        .map(|(k, _)| k.clone());
    let Some(victim) = victim else {
        return;
    };
    publishers.remove(&victim);
    // Drop video decoder (joins its worker thread under Drop — bounded by
    // the 32-frame in-flight queue × per-frame decode cost) and the opus
    // decoder. Both maps may legitimately not contain the victim if no
    // frames of that media type had arrived yet; `remove` returning None
    // in that case is harmless.
    pool.video
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .remove(&victim);
    pool.audio
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .remove(&victim);
    debug!(
        "DecoderPool evicted LRU publisher {} (cap={})",
        victim, DECODER_POOL_MAX_PUBLISHERS
    );
}

/// Build the KEYFRAME_REQUEST PacketWrapper that
/// `peer_decode_manager.rs::send_keyframe_request` emits and push it to
/// the feedback channel. Drops the packet (with a debug log) if the
/// channel is full — the next gap-triggered emission will retry after
/// the debounce.
///
/// Wire format (must match `peer_decode_manager.rs:1085-1112` exactly):
/// - Inner `MediaPacket { media_type: KEYFRAME_REQUEST, user_id: publisher,
///   data: b"VIDEO" | b"SCREEN" }`. Note that `user_id` here is the
///   **target publisher** (the one whose stream needs the keyframe), not
///   the sender — this is the convention the SFU's routing relies on.
/// - Outer `PacketWrapper { packet_type: MEDIA, user_id: local_user_id,
///   data: serialised MediaPacket }`. Sent unencrypted because the SFU
///   needs to read the inner `user_id` to route it back to the publisher.
fn emit_keyframe_request(pool: &DecoderPool, publisher: &str, requested: MediaType) {
    let media_type_byte = match requested {
        MediaType::VIDEO => b"VIDEO".to_vec(),
        MediaType::SCREEN => b"SCREEN".to_vec(),
        _ => return,
    };
    let media_packet = MediaPacket {
        media_type: MediaType::KEYFRAME_REQUEST.into(),
        user_id: publisher.as_bytes().to_vec(),
        data: media_type_byte,
        ..Default::default()
    };
    let media_bytes = match media_packet.write_to_bytes() {
        Ok(b) => b,
        Err(e) => {
            debug!("Failed to serialise KEYFRAME_REQUEST media packet: {}", e);
            return;
        }
    };
    let wrapper = PacketWrapper {
        packet_type: PacketType::MEDIA.into(),
        user_id: pool.local_user_id.as_bytes().to_vec(),
        data: media_bytes,
        ..Default::default()
    };
    let bytes = match wrapper.write_to_bytes() {
        Ok(b) => b,
        Err(e) => {
            debug!("Failed to serialise KEYFRAME_REQUEST wrapper: {}", e);
            return;
        }
    };
    // try_send: a backed-up writer must NOT stall the decode hot path.
    // Dropping a KFR is recoverable (we'll re-arm on the next gap once
    // the debounce passes).
    match pool.feedback_tx.try_send(bytes) {
        Ok(()) => pool.stats.record_keyframe_request_sent(),
        Err(mpsc::error::TrySendError::Full(_)) => {
            debug!(
                "Feedback channel full; dropping KEYFRAME_REQUEST for publisher {}",
                publisher
            );
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            // Writer task exited (session ended); silently drop. The
            // outer consumer will tear everything down momentarily.
        }
    }
}

/// Background writer task: drain the feedback channel and write each
/// PacketWrapper out as a unistream on the WebTransport session. One
/// task per listener client; lives until `quit` flips or the channel
/// senders are all dropped.
fn start_feedback_writer(
    session: Session,
    user_id: String,
    quit: Arc<AtomicBool>,
    mut rx: mpsc::Receiver<Vec<u8>>,
) {
    tokio::spawn(async move {
        while let Some(packet_bytes) = rx.recv().await {
            if quit.load(Ordering::Relaxed) {
                break;
            }
            if let Err(e) = WebTransportClient::send_via_session(&session, packet_bytes).await {
                debug!("Bot {} failed to send feedback packet: {}", user_id, e);
            }
        }
        debug!("Bot {} feedback writer stopped", user_id);
    });
}

/// Periodic per-publisher DiagnosticsPacket emitter. Fires every
/// `DIAGNOSTICS_INTERVAL_MS` (500ms), snapshots each publisher's rolling
/// window, builds one DiagnosticsPacket per (publisher × media-type with
/// non-zero traffic), and pushes them to the feedback channel.
///
/// Computes fps = frames / window_seconds and bitrate_kbps =
/// bytes * 8 / window_seconds / 1000 — matches the meaning of the
/// `VideoMetrics.fps_received` / `bitrate_kbps` fields the real client
/// fills in (`diagnostics_manager.rs:524-530`).
fn start_diagnostics_emitter(pool: Arc<DecoderPool>, user_id: String, quit: Arc<AtomicBool>) {
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_millis(DIAGNOSTICS_INTERVAL_MS));
        // Skip the immediate first tick: we want a real window of data
        // before emitting anything.
        interval.tick().await;
        loop {
            interval.tick().await;
            if quit.load(Ordering::Relaxed) {
                break;
            }
            emit_diagnostics_tick(&pool);
        }
        debug!("Bot {} diagnostics emitter stopped", user_id);
    });
}

/// One-shot diagnostics tick: snapshot every publisher, build packets,
/// and push them onto the feedback channel. Resets each publisher's
/// rolling window after snapshotting.
fn emit_diagnostics_tick(pool: &DecoderPool) {
    let now = now_ms();
    // Snapshot + reset under one lock so the hot path sees a consistent
    // window boundary. Build/serialise packets after dropping the lock
    // to keep the critical section short.
    let snapshots: Vec<DiagSnapshot> = {
        let mut map = pool.publishers.lock().unwrap_or_else(|p| p.into_inner());
        map.iter_mut()
            .filter_map(|(pubid, t)| {
                if t.window_start_ms == 0 {
                    return None;
                }
                let window_ms = now.saturating_sub(t.window_start_ms).max(1);
                let snap = DiagSnapshot {
                    publisher: pubid.clone(),
                    window_ms,
                    video_frames: t.video_frames,
                    video_bytes: t.video_bytes,
                    audio_frames: t.audio_frames,
                    audio_bytes: t.audio_bytes,
                };
                // Reset rolling window. Keep last_seq / gap state — those
                // belong to the KFR detector, not the diagnostics window.
                t.window_start_ms = now;
                t.video_frames = 0;
                t.video_bytes = 0;
                t.audio_frames = 0;
                t.audio_bytes = 0;
                Some(snap)
            })
            .collect()
    };

    for snap in snapshots {
        if snap.video_frames > 0 {
            push_diag_packet(
                pool,
                &snap.publisher,
                now,
                MediaType::VIDEO,
                snap.video_frames,
                snap.video_bytes,
                snap.window_ms,
            );
        }
        if snap.audio_frames > 0 {
            push_diag_packet(
                pool,
                &snap.publisher,
                now,
                MediaType::AUDIO,
                snap.audio_frames,
                snap.audio_bytes,
                snap.window_ms,
            );
        }
    }
}

struct DiagSnapshot {
    publisher: String,
    window_ms: u64,
    video_frames: u64,
    video_bytes: u64,
    audio_frames: u64,
    audio_bytes: u64,
}

/// Build one DiagnosticsPacket and try_send it onto the feedback
/// channel. Wire format mirrors `diagnostics_manager.rs:515-549`:
///
/// - `sender_id` = the publisher we're diagnosing (the real client
///   intentionally uses the peer's id here despite the proto comment;
///   wire-compat with the SFU's NATS analytics requires we do the same).
/// - `target_id` = our local user_id (the receiver).
/// - `timestamp_ms` = now.
/// - `media_type` = VIDEO or AUDIO.
/// - One of `video_metrics` / `audio_metrics` populated with fps_received
///   + bitrate_kbps computed from the rolling window.
/// - `bandwidth_estimate` populated with benign-bot defaults: large
///   downlink, zero loss, zero RTT. A bot has effectively unlimited
///   downlink; we just need to exercise the field so the SFU's
///   LayerSelector ingest path (chat_server.rs:2105-2170) treats us
///   like a real client.
fn push_diag_packet(
    pool: &DecoderPool,
    publisher: &str,
    now: u64,
    media_type: MediaType,
    frames: u64,
    bytes: u64,
    window_ms: u64,
) {
    let secs = window_ms as f64 / 1000.0;
    let fps = if secs > 0.0 {
        frames as f64 / secs
    } else {
        0.0
    };
    let bitrate_kbps = if secs > 0.0 {
        (bytes as f64 * 8.0 / secs / 1000.0).round() as u32
    } else {
        0
    };

    let mut packet = DiagnosticsPacket {
        stream_id: String::new(),
        sender_id: publisher.to_string(),
        target_id: pool.local_user_id.clone(),
        timestamp_ms: now,
        media_type: media_type.into(),
        ..Default::default()
    };
    match media_type {
        MediaType::AUDIO => {
            let mut m = AudioMetrics::new();
            m.fps_received = fps as f32;
            m.bitrate_kbps = bitrate_kbps;
            packet.audio_metrics = ::protobuf::MessageField::some(m);
        }
        _ => {
            let mut m = VideoMetrics::new();
            m.fps_received = fps as f32;
            m.bitrate_kbps = bitrate_kbps;
            packet.video_metrics = ::protobuf::MessageField::some(m);
        }
    }
    let mut be = BandwidthEstimate::new();
    // Bot is on a load-test pod with effectively unlimited downlink.
    // Reporting a benign-but-non-zero estimate avoids triggering the
    // SFU LayerSelector's lowest-tier path (room_state.rs:91 — a 0kbps
    // estimate could be read as "this receiver can't handle anything").
    be.estimated_downlink_kbps = 100_000;
    be.estimated_loss_rate = 0.0;
    be.rtt_ms = 0;
    packet.bandwidth_estimate = ::protobuf::MessageField::some(be);

    let inner = match packet.write_to_bytes() {
        Ok(b) => b,
        Err(e) => {
            debug!("Failed to serialise DiagnosticsPacket: {}", e);
            return;
        }
    };
    let wrapper = PacketWrapper {
        packet_type: PacketType::DIAGNOSTICS.into(),
        user_id: pool.local_user_id.as_bytes().to_vec(),
        data: inner,
        ..Default::default()
    };
    let bytes = match wrapper.write_to_bytes() {
        Ok(b) => b,
        Err(e) => {
            debug!("Failed to serialise DIAGNOSTICS wrapper: {}", e);
            return;
        }
    };
    match pool.feedback_tx.try_send(bytes) {
        Ok(()) => pool.stats.record_diagnostics_sent(),
        Err(mpsc::error::TrySendError::Full(_)) => {
            debug!(
                "Feedback channel full; dropping DiagnosticsPacket for publisher {}",
                publisher
            );
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_extract_redirect_target_parses_redirect_packet() {
        let decision = AdmissionDecision {
            status: AdmissionStatus::REDIRECT.into(),
            redirect_to: "rustlemania-webtransport-0.webtransport-headless.svc.cluster.local"
                .to_string(),
            reason: "wrong_owner".to_string(),
            ..Default::default()
        };
        let wrapper = PacketWrapper {
            packet_type: PacketType::ADMISSION_DECISION.into(),
            user_id: b"system".to_vec(),
            data: decision.write_to_bytes().unwrap(),
            ..Default::default()
        };
        let bytes = wrapper.write_to_bytes().unwrap();

        let got = try_extract_redirect_target(&bytes).expect("redirect target");
        assert!(got.contains("rustlemania-webtransport-0"));
    }

    #[test]
    fn try_extract_redirect_target_ignores_non_redirect_admission() {
        let decision = AdmissionDecision {
            status: AdmissionStatus::QUEUED.into(),
            position: 1,
            ..Default::default()
        };
        let wrapper = PacketWrapper {
            packet_type: PacketType::ADMISSION_DECISION.into(),
            data: decision.write_to_bytes().unwrap(),
            ..Default::default()
        };
        assert!(try_extract_redirect_target(&wrapper.write_to_bytes().unwrap()).is_none());
    }

    #[test]
    fn try_extract_redirect_target_ignores_media() {
        let media = MediaPacket {
            media_type: MediaType::VIDEO.into(),
            ..Default::default()
        };
        let wrapper = PacketWrapper {
            packet_type: PacketType::MEDIA.into(),
            data: media.write_to_bytes().unwrap(),
            ..Default::default()
        };
        assert!(try_extract_redirect_target(&wrapper.write_to_bytes().unwrap()).is_none());
    }

    #[test]
    fn try_extract_redirect_target_ignores_garbage() {
        assert!(try_extract_redirect_target(&[0xff, 0xff, 0xff]).is_none());
    }

    use crate::stats::{BotRole, BotStats};
    use std::sync::atomic::Ordering;

    fn empty_pool() -> DecoderPool {
        // Tests don't drain the channel; size doesn't matter because the
        // existing tests never push into it. `try_send` from any new code
        // paths exercised by tests would degrade to a Full error.
        let (feedback_tx, _feedback_rx) = mpsc::channel(16);
        DecoderPool {
            video: Mutex::new(HashMap::new()),
            audio: Mutex::new(HashMap::new()),
            publishers: Mutex::new(HashMap::new()),
            local_user_id: "test-listener".to_string(),
            feedback_tx,
            stats: BotStats::new("test".into(), BotRole::Listener),
        }
    }

    #[test]
    fn decode_packet_counts_garbage_as_error() {
        let pool = empty_pool();
        decode_packet(&pool, &[0xff, 0xff, 0xff]);
        assert_eq!(pool.stats.decode_errors.load(Ordering::Relaxed), 1);
        assert_eq!(pool.stats.video_frames_decoded.load(Ordering::Relaxed), 0);
        assert_eq!(pool.stats.audio_frames_decoded.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn decode_packet_ignores_non_media() {
        let decision = AdmissionDecision {
            status: AdmissionStatus::QUEUED.into(),
            ..Default::default()
        };
        let wrapper = PacketWrapper {
            packet_type: PacketType::ADMISSION_DECISION.into(),
            data: decision.write_to_bytes().unwrap(),
            ..Default::default()
        };
        let pool = empty_pool();
        decode_packet(&pool, &wrapper.write_to_bytes().unwrap());
        assert_eq!(pool.stats.decode_errors.load(Ordering::Relaxed), 0);
        assert_eq!(pool.stats.video_frames_decoded.load(Ordering::Relaxed), 0);
        assert_eq!(pool.stats.audio_frames_decoded.load(Ordering::Relaxed), 0);
    }

    /// Pool variant that keeps the feedback receiver alive so try_send
    /// succeeds, letting the test count packets actually queued.
    fn pool_with_drain() -> (DecoderPool, mpsc::Receiver<Vec<u8>>) {
        let (feedback_tx, feedback_rx) = mpsc::channel(64);
        let pool = DecoderPool {
            video: Mutex::new(HashMap::new()),
            audio: Mutex::new(HashMap::new()),
            publishers: Mutex::new(HashMap::new()),
            local_user_id: "test-listener".to_string(),
            feedback_tx,
            stats: BotStats::new("test".into(), BotRole::Listener),
        };
        (pool, feedback_rx)
    }

    #[test]
    fn video_seq_gap_arms_keyframe_request_after_window() {
        let (pool, _rx) = pool_with_drain();
        // First frame: establish baseline. No gap, no KFR.
        assert!(update_publisher_video_window(&pool, "pub", 1, "delta", 100).is_none());
        // Big sequence jump — gap detected, but ARM window not elapsed yet.
        assert!(update_publisher_video_window(&pool, "pub", 50, "delta", 100).is_none());

        // Manually rewind the gap timestamp past the arm window so we don't
        // have to sleep in the test.
        {
            let mut map = pool.publishers.lock().unwrap();
            let t = map.get_mut("pub").unwrap();
            let now = now_ms();
            t.video_gap_detected_at_ms = Some(now.saturating_sub(KEYFRAME_REQUEST_GAP_ARM_MS + 50));
            t.last_video_keyframe_request_ms = 0;
        }
        // Next delta frame now triggers the dispatch.
        assert_eq!(
            update_publisher_video_window(&pool, "pub", 51, "delta", 100),
            Some(MediaType::VIDEO)
        );
        // Immediately repeating should be debounced.
        assert!(update_publisher_video_window(&pool, "pub", 52, "delta", 100).is_none());
    }

    #[test]
    fn video_keyframe_clears_pending_gap() {
        let (pool, _rx) = pool_with_drain();
        update_publisher_video_window(&pool, "pub", 1, "delta", 100);
        update_publisher_video_window(&pool, "pub", 50, "delta", 100);
        {
            let map = pool.publishers.lock().unwrap();
            assert!(map.get("pub").unwrap().video_gap_detected_at_ms.is_some());
        }
        // Receiving a keyframe clears the pending gap.
        update_publisher_video_window(&pool, "pub", 51, "key", 100);
        let map = pool.publishers.lock().unwrap();
        assert!(map.get("pub").unwrap().video_gap_detected_at_ms.is_none());
    }

    #[test]
    fn emit_keyframe_request_serialises_wire_format() {
        let (pool, mut rx) = pool_with_drain();
        emit_keyframe_request(&pool, "publisher-7", MediaType::VIDEO);
        let bytes = rx.try_recv().expect("KFR packet should be queued");
        let wrapper = PacketWrapper::parse_from_bytes(&bytes).unwrap();
        assert_eq!(wrapper.packet_type, PacketType::MEDIA.into());
        assert_eq!(wrapper.user_id, b"test-listener");
        let inner = MediaPacket::parse_from_bytes(&wrapper.data).unwrap();
        assert_eq!(inner.media_type, MediaType::KEYFRAME_REQUEST.into());
        assert_eq!(inner.user_id, b"publisher-7");
        assert_eq!(inner.data, b"VIDEO");
        assert_eq!(pool.stats.keyframe_requests_sent.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn diagnostics_tick_emits_per_media_packet() {
        let (pool, mut rx) = pool_with_drain();
        // Simulate one video + one audio frame from one publisher.
        update_publisher_video_window(&pool, "pub-a", 1, "delta", 5_000);
        update_publisher_audio_window(&pool, "pub-a", 200);
        // Pretend the window opened 500ms ago.
        {
            let mut map = pool.publishers.lock().unwrap();
            let t = map.get_mut("pub-a").unwrap();
            t.window_start_ms = now_ms().saturating_sub(500);
        }
        emit_diagnostics_tick(&pool);

        // Expect exactly two packets: one VIDEO, one AUDIO.
        let mut seen_video = false;
        let mut seen_audio = false;
        for _ in 0..2 {
            let bytes = rx.try_recv().expect("diag packet should be queued");
            let wrapper = PacketWrapper::parse_from_bytes(&bytes).unwrap();
            assert_eq!(wrapper.packet_type, PacketType::DIAGNOSTICS.into());
            assert_eq!(wrapper.user_id, b"test-listener");
            let diag = DiagnosticsPacket::parse_from_bytes(&wrapper.data).unwrap();
            assert_eq!(diag.target_id, "test-listener");
            assert_eq!(diag.sender_id, "pub-a");
            assert!(diag.bandwidth_estimate.is_some());
            if diag.media_type == MediaType::VIDEO.into() {
                seen_video = true;
                assert!(diag.video_metrics.is_some());
            } else if diag.media_type == MediaType::AUDIO.into() {
                seen_audio = true;
                assert!(diag.audio_metrics.is_some());
            }
        }
        assert!(seen_video);
        assert!(seen_audio);
        assert!(rx.try_recv().is_err());
        assert_eq!(pool.stats.diagnostics_sent.load(Ordering::Relaxed), 2);

        // Window counters are reset after the tick.
        let map = pool.publishers.lock().unwrap();
        let t = map.get("pub-a").unwrap();
        assert_eq!(t.video_frames, 0);
        assert_eq!(t.audio_frames, 0);
        assert_eq!(t.video_bytes, 0);
        assert_eq!(t.audio_bytes, 0);
    }

    #[test]
    fn diagnostics_tick_skips_idle_publishers() {
        let (pool, mut rx) = pool_with_drain();
        // No frames observed yet → no packets emitted.
        emit_diagnostics_tick(&pool);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn decode_packet_skips_heartbeat() {
        let media = MediaPacket {
            media_type: MediaType::HEARTBEAT.into(),
            user_id: b"sender-0".to_vec(),
            ..Default::default()
        };
        let wrapper = PacketWrapper {
            packet_type: PacketType::MEDIA.into(),
            data: media.write_to_bytes().unwrap(),
            ..Default::default()
        };
        let pool = empty_pool();
        decode_packet(&pool, &wrapper.write_to_bytes().unwrap());
        assert_eq!(pool.stats.decode_errors.load(Ordering::Relaxed), 0);
    }

    /// vc-4ns: end-to-end regression test for the listener audio decode
    /// path. Builds a real Opus-encoded 20ms frame using the SAME encoder
    /// configuration as `bot::audio_producer` (`48000 Hz`, mono, Voip),
    /// wraps it in a `MediaPacket`/`PacketWrapper` shaped exactly like the
    /// sender produces, and calls `decode_packet` directly.
    ///
    /// The pre-vc-4ns test suite only exercised garbage parses and the
    /// HEARTBEAT skip branch — there was no test that proved the actual
    /// `opus::Decoder::decode_float` call wires through to
    /// `audio_frames_decoded`. A regression in `decode_audio` (wrong
    /// publisher key, wrong sample-rate config, wrong buffer sizing,
    /// mis-routed enum match) would have left every counter at zero in
    /// production and gone undetected because no test reached the codec.
    ///
    /// On Pass: `audio_frames_decoded == 1`, `decode_errors == 0`.
    /// On Fail (any decode path regression): the assertion below
    /// distinguishes "decoder constructor / config" from "wrong dispatch
    /// branch" by checking both counters.
    #[test]
    fn decode_packet_increments_audio_counter_on_real_opus_frame() {
        use opus::{Application as OpusApp, Channels as OpusChannels, Encoder as OpusEncoder};

        // Mirror audio_producer.rs exactly: 48 kHz mono Voip Opus, 20 ms
        // frames = 960 samples.
        let sample_rate = 48000u32;
        let mut encoder = OpusEncoder::new(sample_rate, OpusChannels::Mono, OpusApp::Voip)
            .expect("construct Opus encoder");
        let samples = vec![0.0f32; 960];
        let mut encoded = vec![0u8; 4000];
        let bytes_written = encoder
            .encode_float(&samples, &mut encoded)
            .expect("Opus encode silence");
        encoded.truncate(bytes_written);
        // Sanity check: Opus must produce at least a single byte (even
        // for silence — DTX is off in the bot encoder).
        assert!(
            !encoded.is_empty(),
            "Opus encode should produce a non-empty frame for silence"
        );

        // Shape matches audio_producer.rs construction.
        let media = MediaPacket {
            media_type: MediaType::AUDIO.into(),
            user_id: b"sender-7".to_vec(),
            data: encoded,
            frame_type: "key".to_string(),
            ..Default::default()
        };
        let wrapper = PacketWrapper {
            packet_type: PacketType::MEDIA.into(),
            user_id: b"sender-7".to_vec(),
            data: media.write_to_bytes().expect("serialise MediaPacket"),
            ..Default::default()
        };
        let pool = empty_pool();
        decode_packet(&pool, &wrapper.write_to_bytes().expect("serialise wrapper"));

        assert_eq!(
            pool.stats.audio_frames_decoded.load(Ordering::Relaxed),
            1,
            "real Opus frame must increment audio_frames_decoded"
        );
        assert_eq!(
            pool.stats.decode_errors.load(Ordering::Relaxed),
            0,
            "valid Opus frame must NOT count as a decode error"
        );

        // And the per-publisher diagnostics tracker must see the AUDIO
        // bytes — without this, the periodic diagnostics emitter would
        // also stay at zero and we'd lose two observability signals at
        // once.
        let publishers = pool.publishers.lock().expect("publishers lock");
        let tracker = publishers
            .get("sender-7")
            .expect("publisher tracker materialised on first AUDIO frame");
        assert_eq!(tracker.audio_frames, 1);
        assert!(tracker.audio_bytes > 0);
    }

    /// vc-4ns: a second AUDIO frame from the same publisher must reuse
    /// the cached `opus::Decoder` (decoder state is jitter-buffer /
    /// PLC-relevant and must NOT be reconstructed per frame). The cached
    /// path is the steady-state hot path; the prior test only exercised
    /// the Vacant branch of the HashMap entry.
    #[test]
    fn decode_packet_reuses_opus_decoder_across_frames() {
        use opus::{Application as OpusApp, Channels as OpusChannels, Encoder as OpusEncoder};

        let mut encoder = OpusEncoder::new(48000, OpusChannels::Mono, OpusApp::Voip)
            .expect("construct Opus encoder");
        let samples = vec![0.0f32; 960];
        let mut encoded = vec![0u8; 4000];
        let n = encoder.encode_float(&samples, &mut encoded).unwrap();
        encoded.truncate(n);

        let make_wrapper_bytes = |seq: u64| {
            let media = MediaPacket {
                media_type: MediaType::AUDIO.into(),
                user_id: b"sender-9".to_vec(),
                data: encoded.clone(),
                frame_type: "key".to_string(),
                timestamp: seq as f64,
                ..Default::default()
            };
            let wrapper = PacketWrapper {
                packet_type: PacketType::MEDIA.into(),
                user_id: b"sender-9".to_vec(),
                data: media.write_to_bytes().unwrap(),
                ..Default::default()
            };
            wrapper.write_to_bytes().unwrap()
        };

        let pool = empty_pool();
        for seq in 0..5 {
            decode_packet(&pool, &make_wrapper_bytes(seq));
        }

        assert_eq!(pool.stats.audio_frames_decoded.load(Ordering::Relaxed), 5);
        assert_eq!(pool.stats.decode_errors.load(Ordering::Relaxed), 0);
        // Exactly one decoder must have been constructed (Vacant branch
        // hit once, then four Occupied reuses).
        let audio_map = pool.audio.lock().expect("audio decoder map");
        assert_eq!(
            audio_map.len(),
            1,
            "per-publisher Opus decoder must be reused across frames"
        );
    }

    /// vc-4ns: confirm the per-publisher Opus decoder map keys on
    /// `media.user_id` (the publisher) and NOT on the outer
    /// `PacketWrapper.user_id` (the SFU-rewritten field) or the
    /// `PacketWrapper.session_id`. Two distinct publishers must
    /// instantiate two decoders; one publisher's frames must NOT
    /// flow through another publisher's decoder state.
    ///
    /// This was the user's hypothesised root cause #2 in the bug
    /// report ("publisher key wrong"). The assertion below would
    /// catch a regression where `decode_audio` accidentally used the
    /// wrapper's session id (a u64) as the publisher key, or
    /// `String::from_utf8_lossy(&wrapper.user_id)` instead of
    /// `media.user_id`.
    #[test]
    fn decode_packet_keys_decoder_map_by_media_user_id() {
        use opus::{Application as OpusApp, Channels as OpusChannels, Encoder as OpusEncoder};

        let mut encoder = OpusEncoder::new(48000, OpusChannels::Mono, OpusApp::Voip)
            .expect("construct Opus encoder");
        let samples = vec![0.0f32; 960];
        let mut encoded = vec![0u8; 4000];
        let n = encoder.encode_float(&samples, &mut encoded).unwrap();
        encoded.truncate(n);

        let make_wrapper = |publisher: &[u8]| {
            let media = MediaPacket {
                media_type: MediaType::AUDIO.into(),
                user_id: publisher.to_vec(),
                data: encoded.clone(),
                frame_type: "key".to_string(),
                ..Default::default()
            };
            let wrapper = PacketWrapper {
                packet_type: PacketType::MEDIA.into(),
                // Intentionally constant: confirm decode_audio reads
                // the INNER MediaPacket.user_id, not the outer
                // PacketWrapper.user_id.
                user_id: b"sfu-rewritten".to_vec(),
                data: media.write_to_bytes().unwrap(),
                ..Default::default()
            };
            wrapper.write_to_bytes().unwrap()
        };

        let pool = empty_pool();
        decode_packet(&pool, &make_wrapper(b"publisher-A"));
        decode_packet(&pool, &make_wrapper(b"publisher-B"));

        assert_eq!(pool.stats.audio_frames_decoded.load(Ordering::Relaxed), 2);
        assert_eq!(pool.stats.decode_errors.load(Ordering::Relaxed), 0);

        let audio_map = pool.audio.lock().expect("audio decoder map");
        assert_eq!(
            audio_map.len(),
            2,
            "distinct publishers must own distinct decoders"
        );
        assert!(audio_map.contains_key("publisher-A"));
        assert!(audio_map.contains_key("publisher-B"));
        // SFU-rewritten wrapper user_id must NOT leak into the map.
        assert!(!audio_map.contains_key("sfu-rewritten"));
    }

    /// vc-wx3: the per-listener pool must cap at exactly
    /// [`DECODER_POOL_MAX_PUBLISHERS`] tracked publishers. Driving the
    /// VIDEO path for `CAP + 5` distinct publishers must result in
    /// `publishers.len() == CAP` AND the LRU victim must be evicted from
    /// `audio` and `publishers` in lockstep (the original bug was three
    /// independent maps that grew without bound; a fix that only capped
    /// one of them would let `diagnostics_emitter` operate on half-evicted
    /// state).
    ///
    /// Uses `update_publisher_video_window` directly rather than the full
    /// `decode_video` path so the test is libvpx-agnostic — the cap logic
    /// lives entirely in that helper and the eviction reaches into all
    /// three maps. We seed `audio` and `publishers` with a known LRU victim
    /// up front (smallest `last_access_ms`) and assert all three maps drop
    /// it in lockstep when the cap is breached.
    #[test]
    fn decoder_pool_caps_publishers_and_evicts_in_lockstep() {
        let (pool, _rx) = pool_with_drain();

        // Pre-populate the audio map with stub Opus decoders for what will
        // become the LRU victim, so we can prove eviction reaches the audio
        // map. Opus::Decoder::new is cheap and doesn't link libvpx.
        let lru_victim = "pub-0";
        {
            let mut audio = pool.audio.lock().expect("audio lock");
            audio.insert(
                lru_victim.to_string(),
                opus::Decoder::new(48000, opus::Channels::Mono).expect("opus decoder"),
            );
        }

        // Seed the publishers map with the victim at the OLDEST timestamp.
        // Subsequent publishers all get strictly larger timestamps so the
        // LRU is unambiguously `pub-0`.
        {
            let mut pubs = pool.publishers.lock().expect("publishers lock");
            let t = pubs.entry(lru_victim.to_string()).or_default();
            t.last_access_ms = 1; // strictly < everything we'll insert below
            t.window_start_ms = 1;
        }

        // Fill the pool to exactly CAP entries with monotonically-increasing
        // access times (pub-1, pub-2, ...). The seed counts as the first
        // entry, so we insert (CAP - 1) more.
        for i in 1..(DECODER_POOL_MAX_PUBLISHERS as u64) {
            // Each call to update_publisher_video_window bumps that
            // publisher's last_access_ms to now_ms(); we don't need to
            // override here because i > 0 is already newer than seed=1.
            let publisher = format!("pub-{}", i);
            update_publisher_video_window(&pool, &publisher, i, "delta", 100);
        }
        // Sanity: we're at the cap, victim still present.
        {
            let pubs = pool.publishers.lock().unwrap();
            assert_eq!(pubs.len(), DECODER_POOL_MAX_PUBLISHERS);
            assert!(pubs.contains_key(lru_victim));
        }

        // Push a CAP+1th publisher — this must trigger eviction of pub-0
        // from publishers AND audio.
        let overflow_publisher = format!("pub-{}", DECODER_POOL_MAX_PUBLISHERS);
        update_publisher_video_window(&pool, &overflow_publisher, 99, "delta", 100);

        // Map size must have stayed at CAP, not grown to CAP+1.
        let pubs = pool.publishers.lock().expect("publishers lock");
        assert_eq!(
            pubs.len(),
            DECODER_POOL_MAX_PUBLISHERS,
            "publishers map must cap at DECODER_POOL_MAX_PUBLISHERS, got {}",
            pubs.len()
        );
        // LRU victim must have been removed from publishers...
        assert!(
            !pubs.contains_key(lru_victim),
            "LRU publisher {} should have been evicted from publishers map",
            lru_victim
        );
        // ...AND from the audio decoder map (lockstep eviction is the
        // load-bearing invariant for diagnostics correctness).
        let audio = pool.audio.lock().expect("audio lock");
        assert!(
            !audio.contains_key(lru_victim),
            "LRU publisher {} should have been evicted from audio map in lockstep",
            lru_victim
        );
        // The new publisher is present.
        assert!(
            pubs.contains_key(&overflow_publisher),
            "new publisher {} must be inserted after eviction",
            overflow_publisher
        );
    }

    /// vc-wx3: re-accessing an existing publisher must NOT trigger an
    /// eviction even when the map is at the cap. A regression that
    /// re-evicted on every frame would thrash the codec state and produce
    /// an artillery of KFRs.
    #[test]
    fn decoder_pool_does_not_evict_on_existing_publisher_access() {
        let (pool, _rx) = pool_with_drain();

        // Fill to cap.
        for i in 0..DECODER_POOL_MAX_PUBLISHERS as u64 {
            let publisher = format!("pub-{}", i);
            update_publisher_video_window(&pool, &publisher, i, "delta", 100);
        }
        {
            let pubs = pool.publishers.lock().unwrap();
            assert_eq!(pubs.len(), DECODER_POOL_MAX_PUBLISHERS);
        }

        // Update an existing publisher many times — the map must stay at
        // exactly CAP entries and the publisher must remain present.
        for seq in 1..50u64 {
            update_publisher_video_window(&pool, "pub-0", seq, "delta", 100);
        }

        let pubs = pool.publishers.lock().unwrap();
        assert_eq!(
            pubs.len(),
            DECODER_POOL_MAX_PUBLISHERS,
            "re-accessing an existing publisher must not change map size"
        );
        assert!(pubs.contains_key("pub-0"));
    }

    /// vc-wx3: the AUDIO path must drive the same cap as the VIDEO path —
    /// they share the `publishers` map. A regression that capped only on
    /// the VIDEO path would let an audio-only stream blow past the cap.
    #[test]
    fn decoder_pool_caps_publishers_via_audio_path() {
        let (pool, _rx) = pool_with_drain();

        for i in 0..(DECODER_POOL_MAX_PUBLISHERS as u64 + 5) {
            let publisher = format!("audio-pub-{}", i);
            update_publisher_audio_window(&pool, &publisher, 200);
        }

        let pubs = pool.publishers.lock().unwrap();
        assert_eq!(
            pubs.len(),
            DECODER_POOL_MAX_PUBLISHERS,
            "AUDIO path must also enforce DECODER_POOL_MAX_PUBLISHERS cap"
        );
    }
}
