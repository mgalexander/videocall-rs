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

//! vc-zf8k (Bead B): forwarding-liveness signal for `/healthz`.
//!
//! Companion to the fail-fast panic hook installed in
//! `webtransport_server::main`. The hook guarantees that a panic on ANY task
//! crashes the process; this module guarantees that a *non-panicking* but
//! stalled forwarding pipeline (e.g. a wedged subscription that never fires a
//! panic) is still observable from outside the process, so a k8s
//! `livenessProbe` hitting `/healthz` can restart the pod.
//!
//! ## Why a process-global singleton
//!
//! The per-room dispatcher tasks ([`crate::actors::chat_server::spawn_room_dispatcher`])
//! are free functions spawned by the actor; the health responder is an
//! independent Actix `App` factory in `main`. Threading an `Arc` through every
//! dispatcher spawn site AND into the health `App` closure would be invasive
//! and error-prone. A `static` with relaxed atomics is the minimal coupling:
//! the dispatcher hot path does a single relaxed store per forwarded message
//! batch, and the health responder does a few relaxed loads per probe.
//!
//! ## What is recorded
//!
//! Two monotonic-millis-since-process-start timestamps:
//!
//!   * [`ForwardingHealth::note_forward`] — stamped on the per-room dispatcher
//!     fan-out hot path whenever at least one parsed NATS message is forwarded
//!     to at least one receiver. This is the positive "forwarding is alive"
//!     heartbeat.
//!   * [`ForwardingHealth::note_should_forward`] — stamped by the per-room
//!     liveness watchdog tick whenever it observes a room that *should* be
//!     forwarding (`has_receivers && has_publishers`, exactly the vc-9eh
//!     [`crate::actors::chat_server::watchdog_should_resubscribe`] gate inputs).
//!     This is the "we expect forwarding to be happening" signal.
//!
//! The health DECISION ([`forwarding_health_decision`]) is a pure function so
//! it is unit-testable without spinning up NATS or the actor.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Default forwarding-silence threshold. If a room that SHOULD be forwarding
/// has been observed within this window but NO forward has happened within it,
/// `/healthz` reports unhealthy.
///
/// Tuned for real-world networks per the Change Impact Policy: this is well
/// above the vc-9eh base silence window (750ms) and its escalation steps so a
/// brief NATS stall, a single slow-consumer trip, or 200ms+ RTT jitter does
/// NOT trip a pod restart. It is aligned with the vc-9eh
/// [`crate::actors::chat_server::WATCHDOG_SILENCE_CAP`] (30s) — the watchdog's
/// own ceiling for a wedged-but-recoverable subscription — plus headroom, so
/// the in-place resubscribe machinery gets multiple full attempts to self-heal
/// before the liveness probe escalates to a process restart.
pub const DEFAULT_FORWARDING_SILENCE_THRESHOLD: Duration = Duration::from_secs(45);

/// Env var to override [`DEFAULT_FORWARDING_SILENCE_THRESHOLD`] (milliseconds).
pub const FORWARDING_SILENCE_THRESHOLD_ENV: &str = "SFU_FORWARDING_SILENCE_MS";

/// Resolve the configured forwarding-silence threshold.
///
/// Reads [`FORWARDING_SILENCE_THRESHOLD_ENV`] as a millisecond count; falls
/// back to [`DEFAULT_FORWARDING_SILENCE_THRESHOLD`] when unset or unparseable.
/// A configured value of `0` is treated as "disabled" by the caller (see
/// [`forwarding_health_decision`]); we still return it verbatim so the
/// decision function owns that policy.
pub fn forwarding_silence_threshold() -> Duration {
    match std::env::var(FORWARDING_SILENCE_THRESHOLD_ENV) {
        Ok(v) => match v.trim().parse::<u64>() {
            Ok(ms) => Duration::from_millis(ms),
            Err(_) => DEFAULT_FORWARDING_SILENCE_THRESHOLD,
        },
        Err(_) => DEFAULT_FORWARDING_SILENCE_THRESHOLD,
    }
}

/// Sentinel meaning "never recorded". `0` is a safe sentinel because a real
/// timestamp is millis-since-process-start and the very first recorded value
/// is taken strictly after the epoch is initialized (and we saturate to at
/// least `1` on store, see [`ForwardingHealth::stamp`]).
const NEVER: u64 = 0;

/// Process-global forwarding-liveness state. See the module docs.
pub struct ForwardingHealth {
    /// Millis-since-process-start of the most recent successful forward.
    last_forward_ms: AtomicU64,
    /// Millis-since-process-start of the most recent watchdog observation of a
    /// room that should be forwarding (receivers + publishers present).
    last_should_forward_ms: AtomicU64,
}

impl ForwardingHealth {
    const fn new() -> Self {
        Self {
            last_forward_ms: AtomicU64::new(NEVER),
            last_should_forward_ms: AtomicU64::new(NEVER),
        }
    }

    /// Store `now_ms`, saturating away the [`NEVER`] sentinel so a genuine
    /// "happened at process-start tick 0" can never be misread as "never".
    #[inline]
    fn stamp(slot: &AtomicU64, now_ms: u64) {
        slot.store(now_ms.max(1), Ordering::Relaxed);
    }

    /// Record that forwarding just happened. Called from the dispatcher hot
    /// path; intentionally a single relaxed store (no fence, no RMW) so the
    /// per-forward cost is negligible.
    #[inline]
    pub fn note_forward(&self) {
        Self::stamp(&self.last_forward_ms, now_millis());
    }

    /// Record that a room that SHOULD be forwarding was just observed. Called
    /// from the per-room watchdog tick (one timer per room), not per join.
    #[inline]
    pub fn note_should_forward(&self) {
        Self::stamp(&self.last_should_forward_ms, now_millis());
    }

    /// Snapshot for the health responder / tests.
    pub fn snapshot(&self) -> ForwardingHealthSnapshot {
        ForwardingHealthSnapshot {
            last_forward_ms: self.last_forward_ms.load(Ordering::Relaxed),
            last_should_forward_ms: self.last_should_forward_ms.load(Ordering::Relaxed),
        }
    }
}

/// A point-in-time read of [`ForwardingHealth`].
#[derive(Clone, Copy, Debug)]
pub struct ForwardingHealthSnapshot {
    pub last_forward_ms: u64,
    pub last_should_forward_ms: u64,
}

/// The process-global instance. Updated by the dispatcher + watchdog, read by
/// the `/healthz` responder.
pub static FORWARDING_HEALTH: ForwardingHealth = ForwardingHealth::new();

/// Convenience accessor for the global instance.
pub fn global() -> &'static ForwardingHealth {
    &FORWARDING_HEALTH
}

/// Monotonic millis since process start. Backed by a lazily-initialized
/// [`std::time::Instant`] epoch so the values are immune to wall-clock jumps
/// (NTP steps, suspend) — the same monotonic basis the rest of the SFU uses.
pub fn now_millis() -> u64 {
    use std::sync::OnceLock;
    use std::time::Instant;
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    let epoch = EPOCH.get_or_init(Instant::now);
    epoch.elapsed().as_millis() as u64
}

/// Result of the forwarding-liveness decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HealthStatus {
    /// `/healthz` should return 200.
    Healthy,
    /// `/healthz` should return 503 — the SFU should be forwarding but isn't.
    ForwardingStalled,
}

impl HealthStatus {
    /// `true` for [`HealthStatus::Healthy`].
    pub fn is_healthy(self) -> bool {
        matches!(self, HealthStatus::Healthy)
    }
}

/// Pure forwarding-liveness decision (unit-testable; no globals, no clock).
///
/// All times are millis-since-process-start; `0` means "never recorded".
///
/// Returns [`HealthStatus::ForwardingStalled`] IFF **both**:
///   1. A room that should be forwarding was observed within `threshold` of
///      `now_ms` (i.e. forwarding SHOULD be happening right now), AND
///   2. No forward has happened within `threshold` of `now_ms`.
///
/// Otherwise [`HealthStatus::Healthy`]. In particular:
///   * An idle / empty SFU (no room ever observed as should-forward, or the
///     last such observation is older than `threshold`) is always healthy —
///     there is nothing to forward, so a stale forward timestamp is expected
///     and must NOT trip a 503 (no false-positive restart of a quiet pod).
///   * A `threshold` of `0` disables the check entirely (always healthy).
///
/// Reusing the watchdog's `has_receivers && has_publishers` gate (folded into
/// `last_should_forward_ms`) means this shares the vc-9eh liveness semantics
/// rather than inventing a parallel heuristic.
pub fn forwarding_health_decision(
    now_ms: u64,
    last_forward_ms: u64,
    last_should_forward_ms: u64,
    threshold: Duration,
) -> HealthStatus {
    let threshold_ms = threshold.as_millis() as u64;
    // Disabled / no-window configured.
    if threshold_ms == 0 {
        return HealthStatus::Healthy;
    }

    // (1) Do we currently expect forwarding? Only if a should-forward room was
    // observed recently. If we have NEVER observed one, or the last observation
    // is stale, the SFU is idle/empty → healthy.
    let should_forward_recently = last_should_forward_ms != NEVER
        && now_ms.saturating_sub(last_should_forward_ms) <= threshold_ms;
    if !should_forward_recently {
        return HealthStatus::Healthy;
    }

    // (2) We expect forwarding. Healthy iff a forward happened within the
    // threshold. A never-recorded forward (NEVER) while we expect one is
    // unhealthy.
    let forwarded_recently =
        last_forward_ms != NEVER && now_ms.saturating_sub(last_forward_ms) <= threshold_ms;
    if forwarded_recently {
        HealthStatus::Healthy
    } else {
        HealthStatus::ForwardingStalled
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const THRESH: Duration = Duration::from_secs(45);

    #[test]
    fn idle_empty_sfu_is_healthy() {
        // Nothing ever observed: no should-forward, no forward.
        let s = forwarding_health_decision(100_000, NEVER, NEVER, THRESH);
        assert_eq!(s, HealthStatus::Healthy);
        assert!(s.is_healthy());
    }

    #[test]
    fn idle_with_stale_should_forward_is_healthy() {
        // A room WAS populated long ago, but not within the threshold — the
        // pod is now quiet/empty. A stale forward timestamp must not trip 503.
        let now = 1_000_000;
        let last_forward = 10_000; // ancient
        let last_should = 20_000; // also ancient (> threshold ago)
        assert_eq!(
            forwarding_health_decision(now, last_forward, last_should, THRESH),
            HealthStatus::Healthy
        );
    }

    #[test]
    fn forwarding_recently_is_healthy() {
        // Both fresh: we expect forwarding and it is happening.
        let now = 1_000_000;
        let last_forward = now - 5_000;
        let last_should = now - 1_000;
        assert_eq!(
            forwarding_health_decision(now, last_forward, last_should, THRESH),
            HealthStatus::Healthy
        );
    }

    #[test]
    fn silent_past_threshold_with_receivers_and_publishers_is_unhealthy() {
        // We observed a should-forward room recently, but no forward has
        // happened within the threshold → forwarding is dead while it should
        // be alive.
        let now = 1_000_000;
        let last_should = now - 1_000; // expect forwarding now
        let last_forward = now - 60_000; // but last forward was 60s ago (> 45s)
        assert_eq!(
            forwarding_health_decision(now, last_forward, last_should, THRESH),
            HealthStatus::ForwardingStalled
        );
    }

    #[test]
    fn expecting_but_never_forwarded_is_unhealthy() {
        // Watchdog sees a populated room, but forwarding has NEVER produced a
        // single forward — a wedged subscription from the start.
        let now = 1_000_000;
        let last_should = now - 500;
        assert_eq!(
            forwarding_health_decision(now, NEVER, last_should, THRESH),
            HealthStatus::ForwardingStalled
        );
    }

    #[test]
    fn boundary_exactly_at_threshold_is_healthy() {
        // `<= threshold` is inclusive: a forward exactly `threshold` ago still
        // counts as recent, so we do not flap at the exact boundary.
        let now = 1_000_000;
        let last_should = now - 100;
        let last_forward = now - THRESH.as_millis() as u64;
        assert_eq!(
            forwarding_health_decision(now, last_forward, last_should, THRESH),
            HealthStatus::Healthy
        );
    }

    #[test]
    fn zero_threshold_disables_check() {
        // Even with a clearly-stalled signal, a 0 threshold means "always
        // healthy" (escape hatch / disable).
        let now = 1_000_000;
        let last_should = now - 1;
        let last_forward = NEVER;
        assert_eq!(
            forwarding_health_decision(now, last_forward, last_should, Duration::ZERO),
            HealthStatus::Healthy
        );
    }

    #[test]
    fn threshold_env_falls_back_on_garbage() {
        // Unset and garbage both yield the default; this exercises the parse
        // fallback without mutating global env in parallel-test-unsafe ways
        // for the unset case (env var name is unique to this test).
        std::env::set_var(FORWARDING_SILENCE_THRESHOLD_ENV, "not-a-number");
        assert_eq!(
            forwarding_silence_threshold(),
            DEFAULT_FORWARDING_SILENCE_THRESHOLD
        );
        std::env::set_var(FORWARDING_SILENCE_THRESHOLD_ENV, "12000");
        assert_eq!(
            forwarding_silence_threshold(),
            Duration::from_millis(12_000)
        );
        std::env::remove_var(FORWARDING_SILENCE_THRESHOLD_ENV);
    }

    #[test]
    fn global_singleton_records_and_snapshots() {
        let h = global();
        h.note_should_forward();
        h.note_forward();
        let snap = h.snapshot();
        // Both stamped to a real (non-NEVER) value.
        assert_ne!(snap.last_forward_ms, NEVER);
        assert_ne!(snap.last_should_forward_ms, NEVER);
    }
}
