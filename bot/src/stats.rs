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

    /// Record a failed-to-drain inbound stream.
    pub fn record_drop(&self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }

    /// Capture a serializable snapshot of the current counters.
    pub fn snapshot(&self, duration_s: f64) -> BotStatsSnapshot {
        let bytes = self.bytes_received.load(Ordering::Relaxed);
        let avg_bandwidth_bps = if duration_s > 0.0 {
            (bytes as f64 * 8.0 / duration_s).round() as u64
        } else {
            0
        };
        BotStatsSnapshot {
            user_id: self.user_id.clone(),
            role: self.role,
            connected: self.connected.load(Ordering::Relaxed),
            packets_received: self.packets_received.load(Ordering::Relaxed),
            bytes_received: bytes,
            drops: self.drops.load(Ordering::Relaxed),
            avg_bandwidth_bps,
        }
    }
}

/// Serializable per-bot snapshot included in the summary JSON.
#[derive(Debug, Clone, Serialize)]
pub struct BotStatsSnapshot {
    pub user_id: String,
    pub role: Option<BotRole>,
    pub connected: bool,
    pub packets_received: u64,
    pub bytes_received: u64,
    pub drops: u64,
    pub avg_bandwidth_bps: u64,
}
