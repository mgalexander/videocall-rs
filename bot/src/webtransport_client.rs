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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::Receiver;
use tokio::sync::Notify;
use tokio::time;
use tracing::{debug, info, warn};
use url::Url;
use videocall_types::protos::admission_decision_packet::admission_decision::Status as AdmissionStatus;
use videocall_types::protos::admission_decision_packet::AdmissionDecision;
use videocall_types::protos::connection_packet::ConnectionPacket;
use videocall_types::protos::media_packet::media_packet::MediaType;
use videocall_types::protos::media_packet::{HeartbeatMetadata, MediaPacket};
use videocall_types::protos::packet_wrapper::packet_wrapper::PacketType;
use videocall_types::protos::packet_wrapper::PacketWrapper;
use web_transport_quinn::{ClientBuilder, Session};

use crate::config::ClientConfig;
use crate::stats::BotStats;

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
/// Used only in failover-test mode. Ordinary `--orchestrate` runs never look
/// at this; the bot loops via `std::future::pending` and is task-aborted at
/// duration end.
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
}

impl WebTransportClient {
    pub fn new(config: ClientConfig) -> Self {
        Self {
            config,
            session: None,
            quit: Arc::new(AtomicBool::new(false)),
            stats: None,
            session_end: None,
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
    /// Failover-test mode (p6-11) layers two extra behaviours on top:
    ///
    /// 1. Each drained stream is parsed as a `PacketWrapper`. If we see an
    ///    `ADMISSION_DECISION{REDIRECT}`, we stash `redirect_to` on the
    ///    [`SessionEndSignal`] so the orchestrator can use it on the next
    ///    reconnect attempt. The redirect packet arrives **immediately
    ///    before** the SFU closes the session, so we must capture it before
    ///    treating the subsequent `accept_uni` error as a plain disconnect.
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
                                tokio::spawn(async move {
                                    match stream.read_to_end(usize::MAX).await {
                                        Ok(data) => {
                                            if let Some(stats) = stats_spawn {
                                                stats.record_packet(data.len() as u64);
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

            tokio::spawn(async move {
                while let Some(packet_data) = packet_receiver.recv().await {
                    if quit.load(Ordering::Relaxed) {
                        break;
                    }

                    if let Err(e) = Self::send_via_session(&session, packet_data).await {
                        warn!("Failed to send media packet for {}: {}", user_id, e);
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
}
