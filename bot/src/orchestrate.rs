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

use std::sync::atomic::Ordering;
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
use crate::webtransport_client::{SessionEndSignal, WebTransportClient};

/// Maximum number of `ADMISSION_DECISION{REDIRECT}` hops a single bot will
/// follow in one orchestrate run before falling back to idle (vc-kni). The
/// wave-3 jump-hash room→ordinal mapping is deterministic, so a healthy
/// cluster needs at most one redirect per bot; the cap exists purely to
/// defend against pathological redirect loops (e.g. a misconfigured
/// affinity map). On exceeded, we log a warning and `pending` until the
/// orchestrator aborts the task at duration-end.
const MAX_REDIRECT_HOPS: u32 = 5;

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
    /// vc-1re: enable byte-fidelity integrity verification. Senders append a
    /// trailer; listeners strip + verify it. Off by default.
    pub verify_integrity: bool,
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
    /// vc-020: unified aggregate of producer-side drops
    /// (`tx_drops_channel_full + tx_drops_send_error`) across all bots.
    /// Mirrors the receive-side `drops` field so jq dashboards reading
    /// `.totals.tx_drops` (and `.tx_drops` at root) get a single number
    /// without summing the split fields. The split fields are retained for
    /// attribution.
    tx_drops: u64,
    /// vc-kni: aggregate of `redirects_followed` across all bots — total
    /// `ADMISSION_DECISION{REDIRECT}` hops successfully followed during the
    /// run. Non-zero values indicate at least one bot landed on a
    /// non-owner pod under the room→ordinal jump-hash mapping and migrated
    /// to the correct owner. Stays at `0` for healthy single-pod runs.
    redirects_followed: u64,
    /// vc-1re: highest media sequence observed across the bots in this
    /// rollup. FIXED-SHAPE — always serialized, even at zero.
    media_seq_max: u64,
    /// vc-1re: total distinct verified media payloads across the rollup.
    /// FIXED-SHAPE.
    media_received_distinct: u64,
    /// vc-1re: total CRC mismatches across the rollup. MUST be 0 on a clean
    /// integrity run. FIXED-SHAPE.
    crc_mismatches: u64,
    /// vc-1re: total unexplained sequence gaps across the rollup.
    /// FIXED-SHAPE.
    unexplained_gaps: u64,
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
    /// vc-020: unified producer-side drop aggregate
    /// (`tx_drops_channel_full + tx_drops_send_error`), surfaced at the root
    /// so jq dashboards can read `.tx_drops` as a single number — mirroring
    /// the receive-side `.drops` field. The split fields above are retained
    /// for attribution. Always serialized — never gated by
    /// `skip_serializing_if`.
    tx_drops: u64,
    /// vc-kni: aggregate of `redirects_followed` across all bots, surfaced
    /// at the top level so dashboards / jq filters can use the same path
    /// (`.redirects_followed`) without descending into `totals`. Non-zero
    /// indicates at least one bot followed an `ADMISSION_DECISION{REDIRECT}`
    /// to a different owner pod during the run. Always serialized — never
    /// gated by `skip_serializing_if`.
    redirects_followed: u64,
    /// vc-1re: highest media sequence observed across all bots, surfaced at
    /// the root. FIXED-SHAPE — always serialized, even at zero, so a
    /// dashboard reading `.media_seq_max` never sees the key vanish.
    media_seq_max: u64,
    /// vc-1re: total distinct verified media payloads across all bots.
    /// FIXED-SHAPE root field.
    media_received_distinct: u64,
    /// vc-1re: total CRC mismatches across all bots. MUST be 0 on a clean
    /// integrity run. FIXED-SHAPE root field.
    crc_mismatches: u64,
    /// vc-1re: total unexplained sequence gaps across all bots. FIXED-SHAPE
    /// root field.
    unexplained_gaps: u64,
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
        let verify_integrity = cfg.verify_integrity;

        join_handles.push(tokio::spawn(async move {
            if let Err(e) =
                run_listener(client_cfg, server_url, insecure, stats, verify_integrity).await
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
    // vc-020: unified producer-drop aggregate copied alongside the split
    // fields so dashboards reading `.tx_drops` get a single number that
    // mirrors the receive-side `.drops`.
    let tx_drops = totals.tx_drops;
    let redirects_followed = totals.redirects_followed;
    // vc-1re: hoist the integrity aggregates out before moving `totals` so
    // the root-level fixed-shape fields stay in lock-step with the totals
    // sub-object.
    let media_seq_max = totals.media_seq_max;
    let media_received_distinct = totals.media_received_distinct;
    let crc_mismatches = totals.crc_mismatches;
    let unexplained_gaps = totals.unexplained_gaps;

    let summary = OrchestrationSummary {
        senders: cfg.senders,
        listeners: cfg.listeners,
        duration_s: cfg.duration.as_secs(),
        room: cfg.room,
        server_url: cfg.server_url.to_string(),
        tx_packets_enqueued,
        tx_drops_channel_full,
        tx_drops_send_error,
        tx_drops,
        redirects_followed,
        media_seq_max,
        media_received_distinct,
        crc_mismatches,
        unexplained_gaps,
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
        tx_drops: 0,
        redirects_followed: 0,
        media_seq_max: 0,
        media_received_distinct: 0,
        crc_mismatches: 0,
        unexplained_gaps: 0,
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
    // vc-020: derived from the split fields (NOT `snap.tx_drops`) so the
    // aggregate is consistent even if a future change loosens the snapshot
    // invariant. Equivalent to `snap.tx_drops.unwrap_or(0)` today.
    t.tx_drops += snap.tx_drops_channel_full.unwrap_or(0) + snap.tx_drops_send_error.unwrap_or(0);
    t.redirects_followed += snap.redirects_followed.unwrap_or(0);
    // vc-1re integrity rollup. `media_seq_max` is a max (highest observed
    // sequence), not a sum; the other three are sums. All four are
    // fixed-shape `u64` so they always serialize.
    t.media_seq_max = t.media_seq_max.max(snap.media_seq_max);
    t.media_received_distinct += snap.media_received_distinct;
    t.crc_mismatches += snap.crc_mismatches;
    t.unexplained_gaps += snap.unexplained_gaps;
}

fn finalise_avg(t: &mut Totals, duration_s: f64) {
    if duration_s > 0.0 {
        t.avg_bandwidth_bps = (t.bytes_received as f64 * 8.0 / duration_s).round() as u64;
    }
}

/// Replace the host of `original` with `redirect_target`, preserving the
/// scheme, port, and path (vc-kni).
///
/// The SFU's `ADMISSION_DECISION{REDIRECT}` payload carries only the host
/// (e.g. `rustlemania-webtransport-2.webtransport-headless.svc.cluster.local`),
/// not a full URL. The orchestrate reconnect loop needs the full URL the
/// caller passed in for the original connect attempt with just the host
/// swapped — same scheme (https), same port (8443), same path prefix the
/// `WebTransportClient::connect` consumer adds (`/lobby/...`).
///
/// Returns `Err` if the target string is not a valid host. Empty targets
/// are rejected by `Url::set_host`'s validation.
pub(crate) fn compute_redirect_url(original: &Url, redirect_target: &str) -> anyhow::Result<Url> {
    let mut url = original.clone();
    url.set_host(Some(redirect_target))
        .map_err(|e| anyhow::anyhow!("invalid redirect host {redirect_target:?}: {e}"))?;
    Ok(url)
}

/// Wall-clock unix-millis. Mirrors the helper in `failover.rs` /
/// `webtransport_client.rs`; kept local so orchestrate doesn't depend on
/// either module's private fn.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// vc-1re: record which pod the bot's media session landed on, and — if this
/// connect closes out a pending redirect — append the hop to the redirect
/// chain with its measured latency.
///
/// `pending_redirect_at_ms` is the timestamp captured when the bot observed
/// the `ADMISSION_DECISION{REDIRECT}`. It is `Some` only between observing a
/// redirect and the next successful connect; this fn clears it. A direct
/// (first) connect leaves the chain empty and only sets `joined_pod`.
fn record_landing(stats: &BotStats, current_url: &Url, pending_redirect_at_ms: &mut Option<u64>) {
    let pod = current_url.host_str().unwrap_or("unknown").to_string();
    stats.set_joined_pod(pod.clone());
    if let Some(started) = pending_redirect_at_ms.take() {
        let latency = now_ms().saturating_sub(started);
        stats.record_redirect_hop(pod, latency);
    }
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
    info!("Initialising sender {}", user_id);
    if verify_integrity {
        stats.enable_verify_integrity();
    }

    // vc-kni: drive connect+publish inside a reconnect-on-REDIRECT loop. The
    // SFU emits `ADMISSION_DECISION{REDIRECT}` when this bot lands on a
    // non-owner pod under the room→ordinal jump-hash mapping; without
    // following it the bot silently drops out of the test and corrupts
    // staircase shard ratios. We follow it by tearing down the client +
    // producers (Drop stops the producer threads) and reconnecting at the
    // redirect target, capped at `MAX_REDIRECT_HOPS` to defend against
    // pathological loops.
    let original_url = server_url.clone();
    let mut current_url = server_url;
    let mut hops: u32 = 0;
    // vc-1re: wall-clock ms at which we observed a REDIRECT, so the next
    // successful connect can record the hop latency. `None` on the first
    // (direct) connect.
    let mut pending_redirect_at_ms: Option<u64> = None;
    loop {
        let signal = Arc::new(SessionEndSignal::default());
        let mut client = WebTransportClient::new(config.clone())
            .with_stats(stats.clone())
            .with_session_end_signal(signal.clone())
            .with_verify_integrity(verify_integrity);
        client.connect(&current_url, insecure).await?;
        // vc-1re: record the pod we actually landed on, and close out any
        // pending redirect hop with its measured latency.
        record_landing(&stats, &current_url, &mut pending_redirect_at_ms);

        let (packet_tx, packet_rx) = mpsc::channel::<Vec<u8>>(100);
        client.start_packet_sender(packet_rx).await;

        // Audio
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

        // Video
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

        // Wait for the inbound consumer to signal session-end. If it has
        // already fired (race between connect and the signal firing) we
        // observe the sticky `ended` flag and skip the notify. Mirrors the
        // failover.rs reconnect loop.
        let notified = signal.notify.notified();
        tokio::pin!(notified);
        if !signal.ended.load(Ordering::Relaxed) {
            notified.await;
        }

        // Was this a REDIRECT? If yes, follow it; otherwise stay idle for
        // the rest of the run (orchestrate aborts the task at duration-end).
        let redirect = signal.redirect_to.lock().unwrap().clone();
        match redirect {
            Some(target) if hops < MAX_REDIRECT_HOPS => {
                hops += 1;
                match compute_redirect_url(&original_url, &target) {
                    Ok(next) => {
                        info!(
                            "Sender {} following ADMISSION_DECISION REDIRECT to {} (hop {}/{})",
                            user_id, next, hops, MAX_REDIRECT_HOPS
                        );
                        stats.record_redirect_followed();
                        // vc-1re: start the hop stopwatch. The latency is
                        // closed out (and the chain entry appended) when the
                        // next connect lands, in `record_landing`.
                        pending_redirect_at_ms = Some(now_ms());
                        current_url = next;
                        client.stop();
                        // Drop client + producers explicitly: producers stop
                        // on Drop (see audio_producer.rs / video_producer.rs)
                        // so their background threads exit before we open a
                        // fresh session.
                        drop(_audio);
                        drop(_video);
                        drop(client);
                        continue;
                    }
                    Err(e) => {
                        warn!(
                            "Sender {} could not parse redirect target {:?}: {}; staying idle",
                            user_id, target, e
                        );
                        break;
                    }
                }
            }
            Some(target) => {
                warn!(
                    "Sender {} exhausted redirect budget ({} hops); last target was {}; staying idle",
                    user_id, MAX_REDIRECT_HOPS, target
                );
                break;
            }
            None => {
                // Plain disconnect (not a REDIRECT). Preserve the
                // pre-vc-kni behaviour: do NOT reconnect — fall through to
                // `pending` and let the orchestrator abort us at
                // duration-end. Introducing reconnect-on-plain-disconnect
                // is out of scope for vc-kni.
                break;
            }
        }
    }

    std::future::pending::<()>().await;
    Ok(())
}

async fn run_listener(
    config: ClientConfig,
    server_url: Url,
    insecure: bool,
    stats: Arc<BotStats>,
    verify_integrity: bool,
) -> anyhow::Result<()> {
    let user_id = config.user_id.clone();
    info!("Initialising listener {}", user_id);
    if verify_integrity {
        stats.enable_verify_integrity();
    }

    // vc-kni: same reconnect-on-REDIRECT loop as `run_sender`. Listeners
    // decode by default (vc-86j) so each iteration also re-arms the per-
    // publisher decoder pool inside the new `WebTransportClient`. Plain
    // disconnects keep the pre-vc-kni "stay idle" behaviour.
    let original_url = server_url.clone();
    let mut current_url = server_url;
    let mut hops: u32 = 0;
    let mut pending_redirect_at_ms: Option<u64> = None;
    loop {
        let signal = Arc::new(SessionEndSignal::default());
        let mut client = WebTransportClient::new(config.clone())
            .with_stats(stats.clone())
            .with_session_end_signal(signal.clone())
            .with_decode(true)
            .with_verify_integrity(verify_integrity);
        client.connect(&current_url, insecure).await?;
        record_landing(&stats, &current_url, &mut pending_redirect_at_ms);

        let notified = signal.notify.notified();
        tokio::pin!(notified);
        if !signal.ended.load(Ordering::Relaxed) {
            notified.await;
        }

        let redirect = signal.redirect_to.lock().unwrap().clone();
        match redirect {
            Some(target) if hops < MAX_REDIRECT_HOPS => {
                hops += 1;
                match compute_redirect_url(&original_url, &target) {
                    Ok(next) => {
                        info!(
                            "Listener {} following ADMISSION_DECISION REDIRECT to {} (hop {}/{})",
                            user_id, next, hops, MAX_REDIRECT_HOPS
                        );
                        stats.record_redirect_followed();
                        pending_redirect_at_ms = Some(now_ms());
                        current_url = next;
                        client.stop();
                        drop(client);
                        continue;
                    }
                    Err(e) => {
                        warn!(
                            "Listener {} could not parse redirect target {:?}: {}; staying idle",
                            user_id, target, e
                        );
                        break;
                    }
                }
            }
            Some(target) => {
                warn!(
                    "Listener {} exhausted redirect budget ({} hops); last target was {}; staying idle",
                    user_id, MAX_REDIRECT_HOPS, target
                );
                break;
            }
            None => break,
        }
    }

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
        // vc-020: unified aggregate is zero when both split counters are zero.
        assert_eq!(totals.tx_drops, 0);

        let tx_packets_enqueued = totals.tx_packets_enqueued;
        let tx_drops_channel_full = totals.tx_drops_channel_full;
        let tx_drops_send_error = totals.tx_drops_send_error;
        let tx_drops = totals.tx_drops;
        let redirects_followed = totals.redirects_followed;
        let media_seq_max = totals.media_seq_max;
        let media_received_distinct = totals.media_received_distinct;
        let crc_mismatches = totals.crc_mismatches;
        let unexplained_gaps = totals.unexplained_gaps;

        let summary = OrchestrationSummary {
            senders: 0,
            listeners: 1,
            duration_s: 1,
            room: "room-a".into(),
            server_url: "https://example".into(),
            tx_packets_enqueued,
            tx_drops_channel_full,
            tx_drops_send_error,
            tx_drops,
            redirects_followed,
            media_seq_max,
            media_received_distinct,
            crc_mismatches,
            unexplained_gaps,
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
        // vc-020: the unified aggregate must also be present at root.
        assert!(
            json.contains("\"tx_drops\":0"),
            "tx_drops must be present even when zero, got: {json}"
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
        // vc-020: the unified `tx_drops` must also appear at the root, NOT
        // only inside the `totals` sub-object.
        let root_tx_drops_idx = json
            .find("\"tx_drops\"")
            .expect("tx_drops missing from root");
        assert!(
            root_tx_drops_idx < totals_idx,
            "tx_drops must appear at the root (before \"totals\"), got: {json}"
        );
    }

    /// vc-kni: swapping the host of the orchestrate URL with the SFU's
    /// redirect target must preserve the scheme, port, and path. This is
    /// the path the orchestrate reconnect loop uses to point the next
    /// `WebTransportClient::connect` at the owner pod identified by
    /// `ADMISSION_DECISION{REDIRECT}.redirect_to`.
    #[test]
    fn compute_redirect_url_swaps_host_and_preserves_rest() {
        let original = Url::parse("https://lb-host:8443/").expect("parse original");
        let target = "rustlemania-webtransport-2.webtransport-headless.svc.cluster.local";
        let got = compute_redirect_url(&original, target).expect("compute redirect");
        assert_eq!(got.scheme(), "https");
        assert_eq!(got.host_str(), Some(target));
        assert_eq!(got.port(), Some(8443));
        assert_eq!(got.path(), "/");
        assert_eq!(
            got.as_str(),
            "https://rustlemania-webtransport-2.webtransport-headless.svc.cluster.local:8443/"
        );
    }

    /// vc-kni: a URL that already carries a non-trivial path (e.g. the
    /// caller passed in a base URL including a prefix) must survive the
    /// host swap unchanged. The orchestrate consumer appends
    /// `/lobby/<user>/<room>` inside `WebTransportClient::connect`, so a
    /// loss of path data would manifest as a 404 / lobby mismatch on the
    /// reconnect attempt.
    #[test]
    fn compute_redirect_url_preserves_path_and_default_port() {
        let original = Url::parse("https://lb-host/api").expect("parse original");
        let target = "owner-pod.cluster.local";
        let got = compute_redirect_url(&original, target).expect("compute redirect");
        assert_eq!(got.host_str(), Some(target));
        assert_eq!(got.scheme(), "https");
        assert_eq!(got.path(), "/api");
        // Default https port was implicit in the source; it stays implicit.
        assert_eq!(got.port(), None);
    }

    /// vc-kni: an empty or syntactically invalid target must NOT crash the
    /// bot — the orchestrate loop instead logs a warning and falls through
    /// to `pending`. Empty hosts are rejected by `Url::set_host`, which
    /// gives us this behaviour for free.
    #[test]
    fn compute_redirect_url_rejects_empty_target() {
        let original = Url::parse("https://lb-host:8443/").unwrap();
        assert!(compute_redirect_url(&original, "").is_err());
    }

    /// vc-1re: the four integrity counters MUST be present at the summary
    /// root AND in all three Totals roll-ups (`totals`, `sender_totals`,
    /// `listener_totals`), even when every value is zero. This test FAILS if
    /// any of the four fixed-shape fields disappears from any of those four
    /// JSON objects. Together with the failover-side test, this fulfils the
    /// vc-1re counter contract: one increment site (BotStats), one snapshot
    /// field (BotStatsSnapshot), one rollup+root serializer entry.
    #[test]
    fn integrity_counters_fixed_shape_at_root_and_all_totals_vc_1re() {
        let stats = BotStats::new("listener-0".to_string(), BotRole::Listener);
        let per_bot = vec![stats.snapshot(1.0)];
        let (sender_totals, listener_totals, totals) = aggregate(&per_bot, 1.0);

        let summary = OrchestrationSummary {
            senders: 0,
            listeners: 1,
            duration_s: 1,
            room: "room-a".into(),
            server_url: "https://example".into(),
            tx_packets_enqueued: totals.tx_packets_enqueued,
            tx_drops_channel_full: totals.tx_drops_channel_full,
            tx_drops_send_error: totals.tx_drops_send_error,
            tx_drops: totals.tx_drops,
            redirects_followed: totals.redirects_followed,
            media_seq_max: totals.media_seq_max,
            media_received_distinct: totals.media_received_distinct,
            crc_mismatches: totals.crc_mismatches,
            unexplained_gaps: totals.unexplained_gaps,
            totals,
            sender_totals,
            listener_totals,
            per_bot,
        };

        let value: serde_json::Value =
            serde_json::to_value(&summary).expect("serialize summary to value");
        let fields = [
            "media_seq_max",
            "media_received_distinct",
            "crc_mismatches",
            "unexplained_gaps",
        ];
        // Root.
        for f in fields {
            assert!(
                value.get(f).and_then(|v| v.as_u64()) == Some(0),
                "root must carry fixed-shape integrity field {}, got: {}",
                f,
                value
            );
        }
        // All three Totals roll-ups.
        for rollup in ["totals", "sender_totals", "listener_totals"] {
            let obj = value.get(rollup).expect("rollup object present");
            for f in fields {
                assert!(
                    obj.get(f).and_then(|v| v.as_u64()) == Some(0),
                    "{} rollup must carry fixed-shape integrity field {}, got: {}",
                    rollup,
                    f,
                    obj
                );
            }
        }
    }

    /// vc-1re: the integrity rollup must take the MAX of `media_seq_max`
    /// across bots (not the sum) and SUM the other three. Proves the
    /// accumulate path is wired with the correct reduction per field.
    #[test]
    fn integrity_rollup_maxes_seq_and_sums_the_rest_vc_1re() {
        let mut a = empty_totals();
        let mut snap_lo = BotStats::new("s0".into(), BotRole::Sender).snapshot(1.0);
        let mut snap_hi = BotStats::new("s1".into(), BotRole::Sender).snapshot(1.0);
        snap_lo.media_seq_max = 10;
        snap_lo.media_received_distinct = 11;
        snap_lo.crc_mismatches = 1;
        snap_lo.unexplained_gaps = 2;
        snap_hi.media_seq_max = 99;
        snap_hi.media_received_distinct = 100;
        snap_hi.crc_mismatches = 3;
        snap_hi.unexplained_gaps = 4;
        accumulate(&mut a, &snap_lo);
        accumulate(&mut a, &snap_hi);
        assert_eq!(a.media_seq_max, 99, "media_seq_max must be a max");
        assert_eq!(a.media_received_distinct, 111);
        assert_eq!(a.crc_mismatches, 4);
        assert_eq!(a.unexplained_gaps, 6);
    }
}
