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
    video_frames_decoded: u64,
    audio_frames_decoded: u64,
    decode_errors: u64,
    diagnostics_sent: u64,
    keyframe_requests_sent: u64,
    /// vc-xpf: aggregate of `tx_packets_enqueued` across all bots — total
    /// successful producer→sender enqueues this run.
    tx_packets_enqueued: u64,
    /// vc-xpf: aggregate of `tx_drops_channel_full` — drops attributed to a
    /// full producer→sender channel.
    tx_drops_channel_full: u64,
    /// vc-xpf: aggregate of `tx_drops_send_error` — drops attributed to a
    /// WebTransport send failure.
    tx_drops_send_error: u64,
}

/// Final summary JSON emitted to stdout.
#[derive(Debug, Serialize)]
struct OrchestrationSummary {
    senders: usize,
    listeners: usize,
    duration_s: u64,
    room: String,
    server_url: String,
    /// vc-8pl: aggregate of `tx_packets_enqueued` across all bots, surfaced
    /// at the top level so dashboards / jq filters can use the same path
    /// (`.tx_packets_enqueued`) against both orchestrate and failover
    /// summaries. Mirrors the matching field on
    /// [`crate::failover::FailoverSummary`]. The value is also present
    /// inside `totals` / `sender_totals` / `listener_totals` for the
    /// per-role split; this top-level copy exists for schema parity.
    /// Always serialized — never gated by `skip_serializing_if`.
    tx_packets_enqueued: u64,
    /// vc-8pl: aggregate of `tx_drops_channel_full` across all bots. See
    /// `tx_packets_enqueued` above for the schema-parity rationale and
    /// [`crate::failover::FailoverSummary`] for the matching field.
    tx_drops_channel_full: u64,
    /// vc-8pl: aggregate of `tx_drops_send_error` across all bots. See
    /// `tx_packets_enqueued` above for the schema-parity rationale and
    /// [`crate::failover::FailoverSummary`] for the matching field.
    tx_drops_send_error: u64,
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

    // vc-8pl: copy the producer-side aggregates out before moving `totals`
    // into the struct literal so the top-level fields stay in lock-step with
    // the totals sub-object. u64 is `Copy`, but the explicit bindings make
    // the ordering obvious and mirror `failover::FailoverSummary`.
    let tx_packets_enqueued = totals.tx_packets_enqueued;
    let tx_drops_channel_full = totals.tx_drops_channel_full;
    let tx_drops_send_error = totals.tx_drops_send_error;

    let summary = OrchestrationSummary {
        senders: cfg.senders,
        listeners: cfg.listeners,
        duration_s: cfg.duration.as_secs(),
        room: cfg.room,
        server_url: cfg.server_url.to_string(),
        tx_packets_enqueued,
        tx_drops_channel_full,
        tx_drops_send_error,
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
        video_frames_decoded: 0,
        audio_frames_decoded: 0,
        decode_errors: 0,
        diagnostics_sent: 0,
        keyframe_requests_sent: 0,
        tx_packets_enqueued: 0,
        tx_drops_channel_full: 0,
        tx_drops_send_error: 0,
    }
}

fn accumulate(t: &mut Totals, snap: &BotStatsSnapshot) {
    if snap.connected {
        t.connected += 1;
    }
    t.packets_received += snap.packets_received;
    t.bytes_received += snap.bytes_received;
    t.drops += snap.drops;
    t.video_frames_decoded += snap.video_frames_decoded.unwrap_or(0);
    t.audio_frames_decoded += snap.audio_frames_decoded.unwrap_or(0);
    t.decode_errors += snap.decode_errors.unwrap_or(0);
    t.diagnostics_sent += snap.diagnostics_sent.unwrap_or(0);
    t.keyframe_requests_sent += snap.keyframe_requests_sent.unwrap_or(0);
    t.tx_packets_enqueued += snap.tx_packets_enqueued.unwrap_or(0);
    t.tx_drops_channel_full += snap.tx_drops_channel_full.unwrap_or(0);
    t.tx_drops_send_error += snap.tx_drops_send_error.unwrap_or(0);
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

    let mut client = WebTransportClient::new(config.clone()).with_stats(stats.clone());
    client.connect(&server_url, insecure).await?;

    let (packet_tx, packet_rx) = mpsc::channel::<Vec<u8>>(100);
    client.start_packet_sender(packet_rx).await;

    // Audio
    let _audio = match AudioProducer::from_wav_file(
        user_id.clone(),
        &audio_path,
        packet_tx.clone(),
        Some(stats.clone()),
    ) {
        Ok(p) => Some(p),
        Err(e) => {
            warn!("Sender {} failed to start audio producer: {}", user_id, e);
            None
        }
    };

    // Video
    let _video = match VideoProducer::from_image_sequence(
        user_id.clone(),
        &image_dir,
        packet_tx.clone(),
        Some(stats.clone()),
    ) {
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

    // Listeners decode by default (vc-86j) so the 200-bot harness exerts
    // representative client CPU; senders never decode.
    let mut client = WebTransportClient::new(config)
        .with_stats(stats)
        .with_decode(true);
    client.connect(&server_url, insecure).await?;

    // Listeners don't run audio/video producers. The inbound consumer started
    // by `connect` records packets_received for us.
    std::future::pending::<()>().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// vc-8pl: the orchestrate summary must surface `tx_packets_enqueued`,
    /// `tx_drops_channel_full`, and `tx_drops_send_error` at the JSON root
    /// even when every aggregate is zero. This mirrors
    /// `failover::FailoverSummary` so dashboards / jq filters get a stable
    /// schema across both modes. Guards against a future regression where
    /// someone adds `skip_serializing_if = "..."` to (or outright removes)
    /// the top-level fields.
    #[test]
    fn orchestration_summary_serializes_zero_tx_totals_at_root_vc_8pl() {
        // One zero-counter listener snapshot drives the aggregate to all
        // zeros. The listener role means none of the tx_* recorders fire.
        let stats = BotStats::new("listener-0".to_string(), BotRole::Listener);
        let per_bot = vec![stats.snapshot(1.0)];
        let (sender_totals, listener_totals, totals) = aggregate(&per_bot, 1.0);

        assert_eq!(totals.tx_packets_enqueued, 0);
        assert_eq!(totals.tx_drops_channel_full, 0);
        assert_eq!(totals.tx_drops_send_error, 0);

        let tx_packets_enqueued = totals.tx_packets_enqueued;
        let tx_drops_channel_full = totals.tx_drops_channel_full;
        let tx_drops_send_error = totals.tx_drops_send_error;

        let summary = OrchestrationSummary {
            senders: 0,
            listeners: 1,
            duration_s: 1,
            room: "room-a".into(),
            server_url: "https://example".into(),
            tx_packets_enqueued,
            tx_drops_channel_full,
            tx_drops_send_error,
            totals,
            sender_totals,
            listener_totals,
            per_bot,
        };

        let json = serde_json::to_string(&summary).expect("serialize summary");

        // Root-level keys: present and zero. The `totals` sub-object also
        // contains the same keys, but the assertions below only require
        // *at least one* occurrence per key, which is sufficient to prove
        // the top-level fields are emitted (the totals copy alone would
        // satisfy the substring check, so we additionally verify the JSON
        // contains the field after `server_url` — i.e. at the root).
        assert!(
            json.contains("\"tx_packets_enqueued\":0"),
            "tx_packets_enqueued must be present even when zero, got: {json}"
        );
        assert!(
            json.contains("\"tx_drops_channel_full\":0"),
            "tx_drops_channel_full must be present even when zero, got: {json}"
        );
        assert!(
            json.contains("\"tx_drops_send_error\":0"),
            "tx_drops_send_error must be present even when zero, got: {json}"
        );

        // Stronger guarantee: ensure the top-level fields appear before the
        // `totals` sub-object key in the serialized JSON. Field order in
        // serde_json matches struct declaration order, so this catches
        // someone deleting the top-level fields and leaving only the
        // totals-nested copies.
        let root_tx_idx = json
            .find("\"tx_packets_enqueued\"")
            .expect("tx_packets_enqueued missing");
        let totals_idx = json.find("\"totals\"").expect("totals key missing");
        assert!(
            root_tx_idx < totals_idx,
            "tx_packets_enqueued must appear at the root (before \"totals\"), got: {json}"
        );
    }
}
