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

//! Load-test orchestration mode for the bot crate.
//!
//! Spawns `senders` publishing bots and `listeners` subscribe-only bots
//! against a single room, lets them run for `duration` seconds, then emits
//! an aggregated JSON summary to stdout.
//!
//! Invoked from `main.rs` when the `--orchestrate` flag is set.

use std::sync::Arc;
use std::time::Duration;

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
use crate::webtransport_client::WebTransportClient;

/// Orchestration parameters. Built from the CLI flags in `main.rs`.
#[derive(Debug, Clone)]
pub struct OrchestrationConfig {
    pub room: String,
    pub senders: usize,
    pub listeners: usize,
    pub duration: Duration,
    pub server_url: Url,
    pub insecure: bool,
    /// Optional path to the WAV file senders should publish. Defaults to
    /// `BundyBests2.wav` in the working directory.
    pub audio_path: String,
    /// Optional path to the directory containing `output_120.jpg`..`output_124.jpg`.
    pub image_dir: String,
    /// String prefix prepended to every generated user_id. Used to shard
    /// multiple driver invocations against the same room without colliding
    /// user IDs. Empty string preserves the original
    /// `sender-{i}` / `listener-{j}` naming.
    pub user_id_prefix: String,
    /// Non-negative integer added to the bot index when forming the
    /// user_id (e.g. with `index_offset = 100` the first sender becomes
    /// `sender-100`).
    pub index_offset: usize,
}

/// Aggregate totals across every bot in the run.
#[derive(Debug, Serialize)]
struct Totals {
    connected: u64,
    packets_received: u64,
    bytes_received: u64,
    drops: u64,
    avg_bandwidth_bps: u64,
}

/// Final summary JSON emitted to stdout.
#[derive(Debug, Serialize)]
struct OrchestrationSummary {
    senders: usize,
    listeners: usize,
    duration_s: u64,
    room: String,
    server_url: String,
    totals: Totals,
    sender_totals: Totals,
    listener_totals: Totals,
    per_bot: Vec<BotStatsSnapshot>,
}

/// Entry point for orchestration mode. Spawns bots, waits `duration`, then
/// prints a JSON summary on stdout and returns.
pub async fn run(cfg: OrchestrationConfig) -> anyhow::Result<()> {
    let total_bots = cfg.senders + cfg.listeners;
    info!(
        "Orchestration starting: {} senders + {} listeners = {} bots in room '{}' for {}s",
        cfg.senders,
        cfg.listeners,
        total_bots,
        cfg.room,
        cfg.duration.as_secs()
    );

    let mut stats_handles: Vec<Arc<BotStats>> = Vec::with_capacity(total_bots);
    let mut join_handles: Vec<JoinHandle<()>> = Vec::with_capacity(total_bots);

    // Spawn senders first so the room has publishers before listeners attach.
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

        join_handles.push(tokio::spawn(async move {
            if let Err(e) = run_listener(client_cfg, server_url, insecure, stats).await {
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

    // Abort every bot task. Each bot owns its WebTransport session and
    // producer threads; dropping them on abort triggers clean shutdown via
    // `Drop` impls for AudioProducer/VideoProducer.
    for handle in &join_handles {
        handle.abort();
    }
    // Best-effort wait so producer threads stop before we emit the report.
    for handle in join_handles {
        let _ = handle.await;
    }

    let duration_s = cfg.duration.as_secs_f64();
    let per_bot: Vec<BotStatsSnapshot> = stats_handles
        .iter()
        .map(|s| s.snapshot(duration_s))
        .collect();

    let (sender_totals, listener_totals, totals) = aggregate(&per_bot, duration_s);

    let summary = OrchestrationSummary {
        senders: cfg.senders,
        listeners: cfg.listeners,
        duration_s: cfg.duration.as_secs(),
        room: cfg.room,
        server_url: cfg.server_url.to_string(),
        totals,
        sender_totals,
        listener_totals,
        per_bot,
    };

    let json = serde_json::to_string_pretty(&summary)?;
    println!("{json}");
    Ok(())
}

fn aggregate(per_bot: &[BotStatsSnapshot], duration_s: f64) -> (Totals, Totals, Totals) {
    let mut sender_totals = empty_totals();
    let mut listener_totals = empty_totals();
    let mut all = empty_totals();

    for snap in per_bot {
        let target = match snap.role {
            Some(BotRole::Sender) => &mut sender_totals,
            Some(BotRole::Listener) => &mut listener_totals,
            None => &mut all,
        };
        accumulate(target, snap);
        accumulate(&mut all, snap);
    }

    finalise_avg(&mut sender_totals, duration_s);
    finalise_avg(&mut listener_totals, duration_s);
    finalise_avg(&mut all, duration_s);

    (sender_totals, listener_totals, all)
}

fn empty_totals() -> Totals {
    Totals {
        connected: 0,
        packets_received: 0,
        bytes_received: 0,
        drops: 0,
        avg_bandwidth_bps: 0,
    }
}

fn accumulate(t: &mut Totals, snap: &BotStatsSnapshot) {
    if snap.connected {
        t.connected += 1;
    }
    t.packets_received += snap.packets_received;
    t.bytes_received += snap.bytes_received;
    t.drops += snap.drops;
}

fn finalise_avg(t: &mut Totals, duration_s: f64) {
    if duration_s > 0.0 {
        t.avg_bandwidth_bps = (t.bytes_received as f64 * 8.0 / duration_s).round() as u64;
    }
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
    info!("Initialising sender {}", user_id);

    let mut client = WebTransportClient::new(config.clone()).with_stats(stats);
    client.connect(&server_url, insecure).await?;

    let (packet_tx, packet_rx) = mpsc::channel::<Vec<u8>>(100);
    client.start_packet_sender(packet_rx).await;

    // Audio
    let _audio = match AudioProducer::from_wav_file(user_id.clone(), &audio_path, packet_tx.clone())
    {
        Ok(p) => Some(p),
        Err(e) => {
            warn!("Sender {} failed to start audio producer: {}", user_id, e);
            None
        }
    };

    // Video
    let _video =
        match VideoProducer::from_image_sequence(user_id.clone(), &image_dir, packet_tx.clone()) {
            Ok(p) => Some(p),
            Err(e) => {
                warn!("Sender {} failed to start video producer: {}", user_id, e);
                None
            }
        };

    // Block forever; the orchestrator aborts this task when the duration
    // elapses. The `Drop` impls on the producers stop their background work.
    std::future::pending::<()>().await;
    Ok(())
}

async fn run_listener(
    config: ClientConfig,
    server_url: Url,
    insecure: bool,
    stats: Arc<BotStats>,
) -> anyhow::Result<()> {
    let user_id = config.user_id.clone();
    info!("Initialising listener {}", user_id);

    let mut client = WebTransportClient::new(config).with_stats(stats);
    client.connect(&server_url, insecure).await?;

    // Listeners don't run audio/video producers. The inbound consumer started
    // by `connect` records packets_received for us.
    std::future::pending::<()>().await;
    Ok(())
}
