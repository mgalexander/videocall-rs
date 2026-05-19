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

//! Failover-test orchestration for the bot crate (bead vc-607 / p6-11).
//!
//! This is an **opt-in** sibling of [`crate::orchestrate`]: it spawns the
//! same shape of sender + listener bots, but each listener is wrapped in a
//! reconnect loop. When the SFU session ends — typically because the owner
//! pod was killed by `kubectl delete pod` — the listener bots loop on
//! reconnect at ~500ms cadence until packets resume or the run duration
//! elapses.
//!
//! The orchestrator measures per-listener downtime as
//! `reconnect_at_ms - disconnect_at_ms` from [`crate::stats::BotStats`] (the
//! sticky first-gap and first-post-gap timestamps) and emits an aggregate
//! `max_downtime_ms` field in the JSON summary, which the e2e shell script
//! at `scripts/sfu_p6_failover_test.sh` asserts against the <15s budget.
//!
//! Senders are NOT wrapped in a reconnect loop: the bead only requires
//! listener-side recovery measurement, and senders going down during the
//! kill window is expected (they were on the killed pod). Adding sender
//! reconnect would muddy the listener downtime measurement (no inbound
//! traffic to mark recovery against).

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time;
use tracing::{error, info, warn};
use url::Url;

use crate::audio_producer::AudioProducer;
use crate::config::ClientConfig;
use crate::stats::{BotRole, BotStats, BotStatsSnapshot};
use crate::video_producer::VideoProducer;
use crate::webtransport_client::{SessionEndSignal, WebTransportClient};

/// Parameters for a failover-test run.
#[derive(Debug, Clone)]
pub struct FailoverConfig {
    pub room: String,
    pub senders: usize,
    pub listeners: usize,
    pub duration: Duration,
    pub server_url: Url,
    pub insecure: bool,
    pub audio_path: String,
    pub image_dir: String,
    /// Delay between reconnect attempts inside the listener loop. Defaults
    /// to 500ms in the CLI; exposed here so unit/integration tests can
    /// override.
    pub reconnect_interval: Duration,
    /// String prefix prepended to every generated user_id. Used to shard
    /// multiple driver invocations against the same room without colliding
    /// user IDs. Empty string preserves the original
    /// `sender-{i}` / `listener-{j}` naming.
    pub user_id_prefix: String,
    /// Non-negative integer added to the bot index when forming the
    /// user_id (e.g. with `index_offset = 100` the first listener becomes
    /// `listener-100`).
    pub index_offset: usize,
}

/// Per-bot snapshot plus the aggregate failover metrics.
#[derive(Debug, Serialize)]
struct FailoverSummary {
    senders: usize,
    listeners: usize,
    duration_s: u64,
    room: String,
    server_url: String,
    /// Maximum `downtime_ms` observed across all listeners that ever
    /// observed a disconnect. `None` if no listener observed a gap (e.g.
    /// the kill never happened or the test exited before the kill).
    max_downtime_ms: Option<u64>,
    /// Count of listeners that observed at least one gap during the run.
    listeners_with_gap: usize,
    /// Count of listeners that observed a gap AND recovered (received at
    /// least one packet post-gap before the run ended).
    listeners_recovered: usize,
    per_bot: Vec<BotStatsSnapshot>,
}

/// Entry point for failover-test mode. Spawns bots, runs for `duration`,
/// then prints the summary JSON on stdout.
pub async fn run(cfg: FailoverConfig) -> anyhow::Result<()> {
    let total_bots = cfg.senders + cfg.listeners;
    info!(
        "Failover-test starting: {} senders + {} listeners = {} bots in room '{}' for {}s",
        cfg.senders,
        cfg.listeners,
        total_bots,
        cfg.room,
        cfg.duration.as_secs()
    );

    let mut stats_handles: Vec<Arc<BotStats>> = Vec::with_capacity(total_bots);
    let mut join_handles: Vec<JoinHandle<()>> = Vec::with_capacity(total_bots);

    // Senders: same as orchestrate mode, no reconnect logic. Producers will
    // stop when the task is aborted at duration-end.
    for i in 0..cfg.senders {
        let user_id = format!("{}sender-{}", cfg.user_id_prefix, i + cfg.index_offset);
        let stats = BotStats::new(user_id.clone(), BotRole::Sender);
        stats_handles.push(stats.clone());

        let client_cfg = ClientConfig {
            user_id,
            meeting_id: cfg.room.clone(),
            enable_audio: true,
            enable_video: true,
        };
        let server_url = cfg.server_url.clone();
        let insecure = cfg.insecure;
        let audio_path = cfg.audio_path.clone();
        let image_dir = cfg.image_dir.clone();

        join_handles.push(tokio::spawn(async move {
            if let Err(e) = run_sender(
                client_cfg, server_url, insecure, stats, audio_path, image_dir,
            )
            .await
            {
                error!("Sender failed: {}", e);
            }
        }));
    }

    // Listeners: wrapped in a reconnect loop.
    for j in 0..cfg.listeners {
        let user_id = format!("{}listener-{}", cfg.user_id_prefix, j + cfg.index_offset);
        let stats = BotStats::new(user_id.clone(), BotRole::Listener);
        stats_handles.push(stats.clone());

        let client_cfg = ClientConfig {
            user_id,
            meeting_id: cfg.room.clone(),
            enable_audio: false,
            enable_video: false,
        };
        let server_url = cfg.server_url.clone();
        let insecure = cfg.insecure;
        let reconnect_interval = cfg.reconnect_interval;

        join_handles.push(tokio::spawn(async move {
            if let Err(e) = run_listener_with_reconnect(
                client_cfg,
                server_url,
                insecure,
                stats,
                reconnect_interval,
            )
            .await
            {
                error!("Listener failed: {}", e);
            }
        }));
    }

    info!(
        "All {} bots spawned, running for {}s",
        total_bots,
        cfg.duration.as_secs()
    );
    time::sleep(cfg.duration).await;
    info!("Duration elapsed, aborting bot tasks and collecting stats");

    for handle in &join_handles {
        handle.abort();
    }
    for handle in join_handles {
        let _ = handle.await;
    }

    let duration_s = cfg.duration.as_secs_f64();
    let per_bot: Vec<BotStatsSnapshot> = stats_handles
        .iter()
        .map(|s| s.snapshot(duration_s))
        .collect();

    let (max_downtime_ms, listeners_with_gap, listeners_recovered) = aggregate_failover(&per_bot);

    let summary = FailoverSummary {
        senders: cfg.senders,
        listeners: cfg.listeners,
        duration_s: cfg.duration.as_secs(),
        room: cfg.room,
        server_url: cfg.server_url.to_string(),
        max_downtime_ms,
        listeners_with_gap,
        listeners_recovered,
        per_bot,
    };

    let json = serde_json::to_string_pretty(&summary)?;
    println!("{json}");
    Ok(())
}

fn aggregate_failover(per_bot: &[BotStatsSnapshot]) -> (Option<u64>, usize, usize) {
    let mut max_dt: Option<u64> = None;
    let mut with_gap = 0usize;
    let mut recovered = 0usize;
    for snap in per_bot {
        if snap.role != Some(BotRole::Listener) {
            continue;
        }
        if snap.disconnect_at_ms.is_some() {
            with_gap += 1;
        }
        if let Some(dt) = snap.downtime_ms {
            recovered += 1;
            max_dt = Some(max_dt.map_or(dt, |m| m.max(dt)));
        }
    }
    (max_dt, with_gap, recovered)
}

async fn run_sender(
    config: ClientConfig,
    server_url: Url,
    insecure: bool,
    stats: Arc<BotStats>,
    audio_path: String,
    image_dir: String,
) -> anyhow::Result<()> {
    let user_id = config.user_id.clone();
    info!("Initialising sender {} (failover-test)", user_id);

    let mut client = WebTransportClient::new(config.clone()).with_stats(stats);
    client.connect(&server_url, insecure).await?;

    let (packet_tx, packet_rx) = mpsc::channel::<Vec<u8>>(100);
    client.start_packet_sender(packet_rx).await;

    let _audio = match AudioProducer::from_wav_file(user_id.clone(), &audio_path, packet_tx.clone())
    {
        Ok(p) => Some(p),
        Err(e) => {
            warn!("Sender {} failed to start audio producer: {}", user_id, e);
            None
        }
    };

    let _video =
        match VideoProducer::from_image_sequence(user_id.clone(), &image_dir, packet_tx.clone()) {
            Ok(p) => Some(p),
            Err(e) => {
                warn!("Sender {} failed to start video producer: {}", user_id, e);
                None
            }
        };

    std::future::pending::<()>().await;
    Ok(())
}

/// Listener with reconnect-on-disconnect loop.
///
/// On each iteration: build a fresh [`WebTransportClient`] wired with a
/// [`SessionEndSignal`], connect, then await the session-end notification.
/// When it fires (inbound consumer exited — usually because the pod was
/// killed), sleep `reconnect_interval` and loop. Repeat until the parent
/// task is aborted at duration-end.
///
/// Per-listener downtime measurement happens inside [`crate::stats::BotStats`]:
/// the inbound consumer stamps `last_packet_at_ms` on every drained packet
/// and `disconnect_at_ms` (sticky) on its first terminal exit, and the
/// post-reconnect inbound consumer stamps `reconnect_at_ms` (sticky) on the
/// first packet it drains while `disconnect_at_ms` is non-zero. The aggregate
/// `downtime_ms` is computed at snapshot time.
async fn run_listener_with_reconnect(
    config: ClientConfig,
    server_url: Url,
    insecure: bool,
    stats: Arc<BotStats>,
    reconnect_interval: Duration,
) -> anyhow::Result<()> {
    let user_id = config.user_id.clone();
    info!(
        "Initialising listener {} (failover-test reconnect mode)",
        user_id
    );

    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        let signal = Arc::new(SessionEndSignal::default());

        let mut client = WebTransportClient::new(config.clone())
            .with_stats(stats.clone())
            .with_session_end_signal(signal.clone());

        match client.connect(&server_url, insecure).await {
            Ok(()) => {
                info!(
                    "Listener {} connected (attempt {}) at {}ms",
                    user_id,
                    attempt,
                    now_ms()
                );
            }
            Err(e) => {
                // First attempt failing is a hard failure; subsequent
                // failures are expected during the kill window and just
                // back off.
                if attempt == 1 {
                    return Err(e.context(format!("listener {} failed initial connect", user_id)));
                }
                warn!(
                    "Listener {} reconnect attempt {} failed: {}; backing off {}ms",
                    user_id,
                    attempt,
                    e,
                    reconnect_interval.as_millis()
                );
                time::sleep(reconnect_interval).await;
                continue;
            }
        }

        // Wait for the session to end. The inbound consumer fires
        // `signal.notify` on terminal exit; if it has already fired (race
        // between connect and inbound startup) we observe the `ended` flag
        // and bail without sleeping on the notify.
        let notified = signal.notify.notified();
        tokio::pin!(notified);
        if !signal.ended.load(std::sync::atomic::Ordering::Relaxed) {
            notified.await;
        }
        info!(
            "Listener {} session ended (attempt {}); will retry in {}ms",
            user_id,
            attempt,
            reconnect_interval.as_millis()
        );

        // We do nothing with the redirect target captured in the signal:
        // local k3d tests typically can't resolve the in-cluster headless
        // DNS from outside, so we just reconnect to the LB URL and let
        // jump-hash route us. We log it for diagnostics.
        if let Some(target) = signal.redirect_to.lock().unwrap().clone() {
            info!(
                "Listener {} received redirect to {} (logging only; reconnecting via LB)",
                user_id, target
            );
        }

        // Stop any heartbeat / packet-sender tasks still holding session
        // clones from the dead connection so they don't loop forever on
        // send errors. Drop the client at end of iteration; the new
        // iteration constructs a fresh one.
        client.stop();
        drop(client);

        time::sleep(reconnect_interval).await;
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
