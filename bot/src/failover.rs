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
    /// vc-1re: enable byte-fidelity integrity verification. Off by default.
    pub verify_integrity: bool,
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
    /// vc-6hu: aggregate of `tx_packets_enqueued` across all bots — total
    /// successful producer→sender enqueues this run. Mirrors the
    /// `Totals.tx_packets_enqueued` field on [`crate::orchestrate`] so the
    /// failover summary exposes the same producer-side visibility at the
    /// top level (per-bot values are `Option<u64>` and disappear when zero,
    /// which broke staircase dashboards that grep the summary for
    /// `tx_drops_*` regardless of whether any bot tripped a drop).
    tx_packets_enqueued: u64,
    /// vc-6hu: aggregate of `tx_drops_channel_full` — drops attributed to a
    /// full producer→sender channel. Always present so dashboards / jq
    /// filters get a stable schema.
    tx_drops_channel_full: u64,
    /// vc-6hu: aggregate of `tx_drops_send_error` — drops attributed to a
    /// WebTransport send failure. Always present.
    tx_drops_send_error: u64,
    /// vc-020: unified producer-side drop aggregate
    /// (`tx_drops_channel_full + tx_drops_send_error`), surfaced at the root
    /// so jq dashboards can read `.tx_drops` as a single number — mirroring
    /// the receive-side `.drops` field on each `BotStatsSnapshot`. The split
    /// fields above are retained for attribution. Always serialized — never
    /// gated by `skip_serializing_if`.
    tx_drops: u64,
    /// vc-1re: highest media sequence observed across all bots. FIXED-SHAPE
    /// — always serialized, even at zero.
    media_seq_max: u64,
    /// vc-1re: total distinct verified media payloads. FIXED-SHAPE.
    media_received_distinct: u64,
    /// vc-1re: total CRC mismatches across all bots. MUST be 0 on a clean
    /// integrity run. FIXED-SHAPE.
    crc_mismatches: u64,
    /// vc-1re: total unexplained sequence gaps across all bots. FIXED-SHAPE.
    unexplained_gaps: u64,
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
        let verify_integrity = cfg.verify_integrity;

        join_handles.push(tokio::spawn(async move {
            if let Err(e) = run_sender(
                client_cfg,
                server_url,
                insecure,
                stats,
                audio_path,
                image_dir,
                verify_integrity,
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
    let tx_totals = aggregate_tx_totals(&per_bot);

    let summary = FailoverSummary {
        senders: cfg.senders,
        listeners: cfg.listeners,
        duration_s: cfg.duration.as_secs(),
        room: cfg.room,
        server_url: cfg.server_url.to_string(),
        max_downtime_ms,
        listeners_with_gap,
        listeners_recovered,
        tx_packets_enqueued: tx_totals.tx_packets_enqueued,
        tx_drops_channel_full: tx_totals.tx_drops_channel_full,
        tx_drops_send_error: tx_totals.tx_drops_send_error,
        tx_drops: tx_totals.tx_drops,
        media_seq_max: tx_totals.media_seq_max,
        media_received_distinct: tx_totals.media_received_distinct,
        crc_mismatches: tx_totals.crc_mismatches,
        unexplained_gaps: tx_totals.unexplained_gaps,
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

/// vc-6hu: per-bot tx_* counters collapsed into a single run-wide tuple.
///
/// The per-bot snapshot stores `tx_packets_enqueued`, `tx_drops_channel_full`,
/// and `tx_drops_send_error` as `Option<u64>` (omitted from JSON when zero,
/// matching the orchestrate-side schema), so dashboards scraping the failover
/// summary cannot rely on the per-bot keys being present. The aggregator
/// produces top-level plain `u64`s that are always serialized.
#[derive(Debug, Default, PartialEq, Eq)]
struct TxTotals {
    tx_packets_enqueued: u64,
    tx_drops_channel_full: u64,
    tx_drops_send_error: u64,
    /// vc-020: unified aggregate (`tx_drops_channel_full + tx_drops_send_error`)
    /// surfaced at the failover summary root so jq dashboards can read
    /// `.tx_drops` as a single number.
    tx_drops: u64,
    /// vc-1re: integrity rollup — highest media sequence observed (a max, not
    /// a sum). FIXED-SHAPE on the summary root.
    media_seq_max: u64,
    /// vc-1re: total distinct verified media payloads. FIXED-SHAPE.
    media_received_distinct: u64,
    /// vc-1re: total CRC mismatches. MUST be 0 on a clean integrity run.
    /// FIXED-SHAPE.
    crc_mismatches: u64,
    /// vc-1re: total unexplained sequence gaps. FIXED-SHAPE.
    unexplained_gaps: u64,
}

fn aggregate_tx_totals(per_bot: &[BotStatsSnapshot]) -> TxTotals {
    let mut totals = TxTotals::default();
    for snap in per_bot {
        totals.tx_packets_enqueued += snap.tx_packets_enqueued.unwrap_or(0);
        totals.tx_drops_channel_full += snap.tx_drops_channel_full.unwrap_or(0);
        totals.tx_drops_send_error += snap.tx_drops_send_error.unwrap_or(0);
        // vc-020: derive from the split fields so the aggregate stays
        // self-consistent regardless of whether `snap.tx_drops` was set
        // (today it always is, but this avoids coupling to that invariant).
        totals.tx_drops +=
            snap.tx_drops_channel_full.unwrap_or(0) + snap.tx_drops_send_error.unwrap_or(0);
        // vc-1re integrity rollup. `media_seq_max` is a max; the rest sum.
        totals.media_seq_max = totals.media_seq_max.max(snap.media_seq_max);
        totals.media_received_distinct += snap.media_received_distinct;
        totals.crc_mismatches += snap.crc_mismatches;
        totals.unexplained_gaps += snap.unexplained_gaps;
    }
    totals
}

async fn run_sender(
    config: ClientConfig,
    server_url: Url,
    insecure: bool,
    stats: Arc<BotStats>,
    audio_path: String,
    image_dir: String,
    verify_integrity: bool,
) -> anyhow::Result<()> {
    let user_id = config.user_id.clone();
    info!("Initialising sender {} (failover-test)", user_id);
    if verify_integrity {
        stats.enable_verify_integrity();
    }

    let mut client = WebTransportClient::new(config.clone())
        .with_stats(stats.clone())
        .with_verify_integrity(verify_integrity);
    client.connect(&server_url, insecure).await?;
    // vc-1re: record the pod this sender landed on. Failover senders are not
    // wrapped in a redirect loop, so the chain stays empty (direct connect).
    stats.set_joined_pod(server_url.host_str().unwrap_or("unknown").to_string());

    let (packet_tx, packet_rx) = mpsc::channel::<Vec<u8>>(100);
    client.start_packet_sender(packet_rx).await;

    let _audio = match AudioProducer::from_wav_file(
        user_id.clone(),
        &audio_path,
        packet_tx.clone(),
        Some(stats.clone()),
        verify_integrity,
    ) {
        Ok(p) => Some(p),
        Err(e) => {
            warn!("Sender {} failed to start audio producer: {}", user_id, e);
            None
        }
    };

    let _video = match VideoProducer::from_image_sequence(
        user_id.clone(),
        &image_dir,
        packet_tx.clone(),
        Some(stats.clone()),
        verify_integrity,
    ) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::{BotRole, BotStats};

    /// Build a per-bot snapshot with a configurable tx_* counter mix. Uses
    /// the real `BotStats` recorder helpers (rather than constructing the
    /// snapshot literal) so the test stays robust to future field additions
    /// on `BotStatsSnapshot`.
    fn snap_with_tx(
        user_id: &str,
        role: BotRole,
        enqueued: u64,
        ch_full: u64,
        send_err: u64,
    ) -> BotStatsSnapshot {
        let stats = BotStats::new(user_id.to_string(), role);
        for _ in 0..enqueued {
            stats.record_tx_packet_enqueued();
        }
        for _ in 0..ch_full {
            stats.record_tx_drop_channel_full();
        }
        for _ in 0..send_err {
            stats.record_tx_drop_send_error();
        }
        stats.snapshot(1.0)
    }

    /// vc-6hu: the aggregator must sum across every bot, treating `None`
    /// (omitted-when-zero) as 0 and including senders, listeners, and the
    /// rare unroled snapshot. The result is what gets surfaced at the top
    /// level of the failover JSON.
    #[test]
    fn aggregate_tx_totals_sums_mixed_some_and_none() {
        let per_bot = vec![
            // Sender with all three counters active.
            snap_with_tx("sender-0", BotRole::Sender, 100, 5, 2),
            // Sender that never tripped channel-full nor send-error -> None.
            snap_with_tx("sender-1", BotRole::Sender, 200, 0, 0),
            // Listener: producers don't run, so all three are None.
            snap_with_tx("listener-0", BotRole::Listener, 0, 0, 0),
            // Sender with only send-errors.
            snap_with_tx("sender-2", BotRole::Sender, 50, 0, 7),
        ];

        let totals = aggregate_tx_totals(&per_bot);
        assert_eq!(
            totals,
            TxTotals {
                tx_packets_enqueued: 350,
                tx_drops_channel_full: 5,
                tx_drops_send_error: 9,
                // vc-020: unified aggregate = channel_full + send_error.
                tx_drops: 14,
                // vc-1re: no integrity activity in this fixture.
                media_seq_max: 0,
                media_received_distinct: 0,
                crc_mismatches: 0,
                unexplained_gaps: 0,
            }
        );
    }

    /// vc-6hu: when no bot tripped any tx_* counter, the aggregate must be
    /// all zeros — and crucially, the fields must still be present in the
    /// JSON (unlike the per-bot `Option<u64>` fields). This guards against
    /// a future regression where someone adds `skip_serializing_if = "..."`
    /// to the FailoverSummary fields.
    #[test]
    fn failover_summary_serializes_zero_tx_totals() {
        let per_bot = vec![snap_with_tx("listener-0", BotRole::Listener, 0, 0, 0)];
        let totals = aggregate_tx_totals(&per_bot);
        assert_eq!(totals, TxTotals::default());

        let summary = FailoverSummary {
            senders: 0,
            listeners: 1,
            duration_s: 30,
            room: "room-a".into(),
            server_url: "https://example".into(),
            max_downtime_ms: None,
            listeners_with_gap: 0,
            listeners_recovered: 0,
            tx_packets_enqueued: totals.tx_packets_enqueued,
            tx_drops_channel_full: totals.tx_drops_channel_full,
            tx_drops_send_error: totals.tx_drops_send_error,
            tx_drops: totals.tx_drops,
            media_seq_max: totals.media_seq_max,
            media_received_distinct: totals.media_received_distinct,
            crc_mismatches: totals.crc_mismatches,
            unexplained_gaps: totals.unexplained_gaps,
            per_bot,
        };

        let json = serde_json::to_string(&summary).expect("serialize summary");
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
        // vc-020: the unified `tx_drops` must also be present at the root,
        // even when zero. The per-bot snapshot uses `Option<u64>` and
        // disappears when zero, so this top-level field is the schema-stable
        // entry point for jq dashboards.
        assert!(
            json.contains("\"tx_drops\":0"),
            "tx_drops must be present even when zero, got: {json}"
        );
        // Stronger guarantee: `tx_drops` must appear at the root, before the
        // `per_bot` array. Field order in serde_json matches struct
        // declaration order, so this catches a future regression that
        // accidentally pushes `tx_drops` into `per_bot` snapshots only.
        let tx_drops_idx = json
            .find("\"tx_drops\"")
            .expect("tx_drops missing from JSON");
        let per_bot_idx = json.find("\"per_bot\"").expect("per_bot key missing");
        assert!(
            tx_drops_idx < per_bot_idx,
            "tx_drops must appear at the root (before \"per_bot\"), got: {json}"
        );
    }

    /// vc-1re: the four integrity counters MUST be present at the failover
    /// summary root even when zero. This is the failover-side half of the
    /// fixed-shape contract (the orchestrate roll-ups are covered in
    /// `orchestrate.rs`). FAILS if any of the four disappears from the root.
    #[test]
    fn failover_summary_integrity_counters_fixed_shape_vc_1re() {
        let per_bot = vec![snap_with_tx("listener-0", BotRole::Listener, 0, 0, 0)];
        let totals = aggregate_tx_totals(&per_bot);
        // Aggregator zeros for all four integrity fields.
        assert_eq!(totals.media_seq_max, 0);
        assert_eq!(totals.media_received_distinct, 0);
        assert_eq!(totals.crc_mismatches, 0);
        assert_eq!(totals.unexplained_gaps, 0);

        let summary = FailoverSummary {
            senders: 0,
            listeners: 1,
            duration_s: 30,
            room: "room-a".into(),
            server_url: "https://example".into(),
            max_downtime_ms: None,
            listeners_with_gap: 0,
            listeners_recovered: 0,
            tx_packets_enqueued: totals.tx_packets_enqueued,
            tx_drops_channel_full: totals.tx_drops_channel_full,
            tx_drops_send_error: totals.tx_drops_send_error,
            tx_drops: totals.tx_drops,
            media_seq_max: totals.media_seq_max,
            media_received_distinct: totals.media_received_distinct,
            crc_mismatches: totals.crc_mismatches,
            unexplained_gaps: totals.unexplained_gaps,
            per_bot,
        };

        let value: serde_json::Value =
            serde_json::to_value(&summary).expect("serialize failover summary");
        for f in [
            "media_seq_max",
            "media_received_distinct",
            "crc_mismatches",
            "unexplained_gaps",
        ] {
            assert!(
                value.get(f).and_then(|v| v.as_u64()) == Some(0),
                "failover root must carry fixed-shape integrity field {}, got: {}",
                f,
                value
            );
        }
    }
}
