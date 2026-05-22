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

/// Default inbound-saturation window (bead vc-m7k6). If a room that SHOULD be
/// forwarding has been observed within this window AND the process-global
/// inbound-drop counter advanced within this window, `/healthz` reports
/// unhealthy.
///
/// This is the SATURATION case (distinct from the SILENCE case above): under a
/// publisher storm async-nats silently drops inbound messages and fires a
/// connection-global `Event::SlowConsumer` WITHOUT closing the subscription, so
/// the dispatcher keeps draining the messages that DID get through —
/// `last_forward_ms` keeps advancing and the silence decision stays healthy
/// even though late joiners are being starved. We surface that by treating a
/// RECENT increase in the drop counter (while forwarding is expected) as
/// unhealthy.
///
/// Tuned for real-world networks per the Change Impact Policy: kept SHORTER
/// than [`DEFAULT_FORWARDING_SILENCE_THRESHOLD`] so genuine saturation escalates
/// to a restart faster than the slow-silence path, but still well above the
/// vc-9eh watchdog base window and above any plausible single-tick jitter on a
/// 200ms+ RTT link — a one-off SlowConsumer trip during a brief burst ages out
/// of this window long before it can wedge `/healthz` at 503. Crucially the
/// decision ALSO gates on the drop counter having INCREASED within the window
/// (not merely being non-zero), so a single historical drop can never pin the
/// pod unhealthy forever.
pub const DEFAULT_INBOUND_SATURATION_THRESHOLD: Duration = Duration::from_secs(15);

/// Env var to override [`DEFAULT_INBOUND_SATURATION_THRESHOLD`] (milliseconds).
pub const INBOUND_SATURATION_THRESHOLD_ENV: &str = "SFU_INBOUND_SATURATION_MS";

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

/// Resolve the configured inbound-saturation threshold (bead vc-m7k6).
///
/// Reads [`INBOUND_SATURATION_THRESHOLD_ENV`] as a millisecond count; falls
/// back to [`DEFAULT_INBOUND_SATURATION_THRESHOLD`] when unset or unparseable.
/// A configured value of `0` is treated as "disabled" by
/// [`saturation_health_decision`] (the saturation check is skipped, leaving the
/// silence check as the sole liveness signal).
pub fn inbound_saturation_threshold() -> Duration {
    match std::env::var(INBOUND_SATURATION_THRESHOLD_ENV) {
        Ok(v) => match v.trim().parse::<u64>() {
            Ok(ms) => Duration::from_millis(ms),
            Err(_) => DEFAULT_INBOUND_SATURATION_THRESHOLD,
        },
        Err(_) => DEFAULT_INBOUND_SATURATION_THRESHOLD,
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
    /// vc-m7k6: millis-since-process-start of the most recent inbound
    /// slow-consumer drop (an `async_nats::Event::SlowConsumer`). This is the
    /// saturation heartbeat: the `nats_connect` event handler stamps it on
    /// every drop event. The decision treats it as "drops increased recently"
    /// purely by age — a drop that is older than the saturation threshold ages
    /// out, so a single historical drop can NEVER wedge `/healthz` at 503.
    last_inbound_drop_ms: AtomicU64,
}

impl ForwardingHealth {
    const fn new() -> Self {
        Self {
            last_forward_ms: AtomicU64::new(NEVER),
            last_should_forward_ms: AtomicU64::new(NEVER),
            last_inbound_drop_ms: AtomicU64::new(NEVER),
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

    /// vc-m7k6: record that an inbound slow-consumer drop just happened.
    /// Called from the shared `nats_connect` event handler on every
    /// `async_nats::Event::SlowConsumer`. A single relaxed store (negligible
    /// cost; the event is rare relative to the media hot path).
    #[inline]
    pub fn note_inbound_drop(&self) {
        Self::stamp(&self.last_inbound_drop_ms, now_millis());
    }

    /// Snapshot for the health responder / tests.
    pub fn snapshot(&self) -> ForwardingHealthSnapshot {
        ForwardingHealthSnapshot {
            last_forward_ms: self.last_forward_ms.load(Ordering::Relaxed),
            last_should_forward_ms: self.last_should_forward_ms.load(Ordering::Relaxed),
            last_inbound_drop_ms: self.last_inbound_drop_ms.load(Ordering::Relaxed),
        }
    }
}

/// A point-in-time read of [`ForwardingHealth`].
#[derive(Clone, Copy, Debug)]
pub struct ForwardingHealthSnapshot {
    pub last_forward_ms: u64,
    pub last_should_forward_ms: u64,
    /// vc-m7k6: millis-since-process-start of the most recent inbound
    /// slow-consumer drop, or [`NEVER`] (`0`) if none has been observed.
    pub last_inbound_drop_ms: u64,
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

/// vc-m7k6: pure inbound-SATURATION decision (unit-testable; no globals, no
/// clock).
///
/// This is the companion to [`forwarding_health_decision`] for the
/// invisible-saturation failure mode: under a publisher storm async-nats
/// silently drops inbound messages and fires a connection-global SlowConsumer
/// WITHOUT closing the subscription, so the dispatcher keeps forwarding the
/// messages that DID arrive — `last_forward_ms` keeps advancing and the
/// silence decision stays healthy while late joiners are starved.
///
/// All times are millis-since-process-start; `0` ([`NEVER`]) means "never
/// recorded".
///
/// Returns [`HealthStatus::ForwardingStalled`] IFF **all** of:
///   1. The saturation check is enabled (`threshold != 0`), AND
///   2. A room that should be forwarding was observed within `threshold` of
///      `now_ms` (receivers + publishers present right now), AND
///   3. An inbound slow-consumer drop was recorded within `threshold` of
///      `now_ms` (drops are happening RIGHT NOW, not historically).
///
/// Otherwise [`HealthStatus::Healthy`]. The three gates together give NO false
/// positives at low load:
///   * No drops ever (`last_inbound_drop_ms == NEVER`) → healthy.
///   * Only OLD drops (last drop older than `threshold`) → healthy. This is the
///     critical anti-wedge property: a single historical SlowConsumer ages out
///     of the window and can never pin the pod at 503 forever.
///   * Recent drops but NO room currently expects forwarding (idle/empty pod,
///     or a low-volume non-media subscriber on the shared connection tripped
///     SlowConsumer) → healthy. We only restart when receivers are actually
///     being starved.
///   * A `threshold` of `0` disables the saturation check entirely.
///
/// Gate (2) reuses the exact `last_should_forward_ms` signal the silence
/// decision uses (the vc-9eh `has_receivers && has_publishers` watchdog gate),
/// so saturation and silence share liveness semantics.
pub fn saturation_health_decision(
    now_ms: u64,
    last_should_forward_ms: u64,
    last_inbound_drop_ms: u64,
    threshold: Duration,
) -> HealthStatus {
    let threshold_ms = threshold.as_millis() as u64;
    // Disabled.
    if threshold_ms == 0 {
        return HealthStatus::Healthy;
    }

    // (2) Do we currently expect forwarding? If not, a drop somewhere on the
    // shared connection is not starving any receiver → healthy.
    let should_forward_recently = last_should_forward_ms != NEVER
        && now_ms.saturating_sub(last_should_forward_ms) <= threshold_ms;
    if !should_forward_recently {
        return HealthStatus::Healthy;
    }

    // (3) Have inbound drops happened RECENTLY? A never-recorded or stale drop
    // is healthy (anti-wedge: a single historical drop ages out).
    let dropped_recently = last_inbound_drop_ms != NEVER
        && now_ms.saturating_sub(last_inbound_drop_ms) <= threshold_ms;
    if dropped_recently {
        HealthStatus::ForwardingStalled
    } else {
        HealthStatus::Healthy
    }
}

/// vc-m7k6: combined `/healthz` liveness decision over BOTH failure modes.
///
/// Returns [`HealthStatus::ForwardingStalled`] if EITHER the silence decision
/// ([`forwarding_health_decision`]) OR the saturation decision
/// ([`saturation_health_decision`]) reports stalled; otherwise
/// [`HealthStatus::Healthy`]. This is the single entry point the SFU health
/// responders call so both binaries (WebTransport + WebSocket) share identical
/// semantics.
pub fn combined_health_decision(
    now_ms: u64,
    snap: ForwardingHealthSnapshot,
    silence_threshold: Duration,
    saturation_threshold: Duration,
) -> HealthStatus {
    let silence = forwarding_health_decision(
        now_ms,
        snap.last_forward_ms,
        snap.last_should_forward_ms,
        silence_threshold,
    );
    if !silence.is_healthy() {
        return silence;
    }
    saturation_health_decision(
        now_ms,
        snap.last_should_forward_ms,
        snap.last_inbound_drop_ms,
        saturation_threshold,
    )
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
        h.note_inbound_drop();
        let snap = h.snapshot();
        // All three stamped to a real (non-NEVER) value.
        assert_ne!(snap.last_forward_ms, NEVER);
        assert_ne!(snap.last_should_forward_ms, NEVER);
        assert_ne!(snap.last_inbound_drop_ms, NEVER);
    }

    // ===== vc-m7k6 inbound-saturation decision =====

    const SAT: Duration = Duration::from_secs(15);

    #[test]
    fn saturation_no_drops_ever_is_healthy() {
        // A populated, actively-forwarding room with NO drops ever recorded.
        // CRITICAL no-false-positive case at normal load.
        let now = 1_000_000;
        let last_should = now - 500;
        assert_eq!(
            saturation_health_decision(now, last_should, NEVER, SAT),
            HealthStatus::Healthy
        );
    }

    #[test]
    fn saturation_old_drops_is_healthy() {
        // A single historical SlowConsumer (e.g. one brief burst minutes ago).
        // It has aged out of the window → must NOT wedge /healthz at 503. This
        // is the anti-wedge property.
        let now = 1_000_000;
        let last_should = now - 500; // forwarding expected now
        let last_drop = now - 60_000; // drop was 60s ago (> 15s window)
        assert_eq!(
            saturation_health_decision(now, last_should, last_drop, SAT),
            HealthStatus::Healthy
        );
    }

    #[test]
    fn saturation_recent_drops_with_receivers_is_unhealthy() {
        // The target case: receivers+publishers present (should-forward recent)
        // AND drops are happening right now → late joiners are being starved
        // invisibly → 503 so the pod restarts.
        let now = 1_000_000;
        let last_should = now - 500;
        let last_drop = now - 1_000; // within the 15s window
        assert_eq!(
            saturation_health_decision(now, last_should, last_drop, SAT),
            HealthStatus::ForwardingStalled
        );
    }

    #[test]
    fn saturation_recent_drops_no_receivers_is_healthy() {
        // Drops are recent, but NO room currently expects forwarding (idle pod,
        // or a low-volume non-media subscriber on the shared connection tripped
        // SlowConsumer). Nobody is being starved → healthy.
        let now = 1_000_000;
        let last_should = now - 60_000; // stale: no room expects forwarding now
        let last_drop = now - 1_000; // drop is recent
        assert_eq!(
            saturation_health_decision(now, last_should, last_drop, SAT),
            HealthStatus::Healthy
        );
    }

    #[test]
    fn saturation_never_should_forward_is_healthy() {
        // Brand-new idle pod: a drop on a control subscriber, no forwarding
        // ever expected.
        let now = 1_000_000;
        let last_drop = now - 100;
        assert_eq!(
            saturation_health_decision(now, NEVER, last_drop, SAT),
            HealthStatus::Healthy
        );
    }

    #[test]
    fn saturation_zero_threshold_disables_check() {
        // Even a clear saturation signal yields healthy when disabled.
        let now = 1_000_000;
        let last_should = now - 100;
        let last_drop = now - 100;
        assert_eq!(
            saturation_health_decision(now, last_should, last_drop, Duration::ZERO),
            HealthStatus::Healthy
        );
    }

    #[test]
    fn saturation_boundary_exactly_at_threshold_is_unhealthy() {
        // `<= threshold` is inclusive on both gates: a drop exactly `threshold`
        // ago with a should-forward exactly `threshold` ago still counts as
        // recent, so we do not flap right at the boundary.
        let now = 1_000_000;
        let last_should = now - SAT.as_millis() as u64;
        let last_drop = now - SAT.as_millis() as u64;
        assert_eq!(
            saturation_health_decision(now, last_should, last_drop, SAT),
            HealthStatus::ForwardingStalled
        );
    }

    #[test]
    fn saturation_threshold_env_falls_back_on_garbage() {
        std::env::set_var(INBOUND_SATURATION_THRESHOLD_ENV, "not-a-number");
        assert_eq!(
            inbound_saturation_threshold(),
            DEFAULT_INBOUND_SATURATION_THRESHOLD
        );
        std::env::set_var(INBOUND_SATURATION_THRESHOLD_ENV, "9000");
        assert_eq!(inbound_saturation_threshold(), Duration::from_millis(9_000));
        std::env::remove_var(INBOUND_SATURATION_THRESHOLD_ENV);
    }

    // ===== vc-m7k6 combined decision =====

    fn snap(forward: u64, should: u64, drop: u64) -> ForwardingHealthSnapshot {
        ForwardingHealthSnapshot {
            last_forward_ms: forward,
            last_should_forward_ms: should,
            last_inbound_drop_ms: drop,
        }
    }

    #[test]
    fn combined_healthy_when_forwarding_and_no_drops() {
        let now = 1_000_000;
        let s = snap(now - 1_000, now - 500, NEVER);
        assert_eq!(
            combined_health_decision(now, s, THRESH, SAT),
            HealthStatus::Healthy
        );
    }

    #[test]
    fn combined_unhealthy_on_silence() {
        // Silence path trips even with no drops.
        let now = 1_000_000;
        let s = snap(now - 60_000, now - 500, NEVER);
        assert_eq!(
            combined_health_decision(now, s, THRESH, SAT),
            HealthStatus::ForwardingStalled
        );
    }

    #[test]
    fn combined_unhealthy_on_saturation_while_silence_healthy() {
        // The vc-m7k6 invisible-saturation case: forwarding is happening
        // (silence is HEALTHY because the dispatcher keeps draining the
        // messages that got through) yet inbound is being dropped right now.
        // The combined decision catches it via the saturation arm.
        let now = 1_000_000;
        let s = snap(now - 500, now - 500, now - 1_000);
        assert_eq!(
            forwarding_health_decision(now, s.last_forward_ms, s.last_should_forward_ms, THRESH),
            HealthStatus::Healthy,
            "silence path alone must be blind to this (the whole point of vc-m7k6)"
        );
        assert_eq!(
            combined_health_decision(now, s, THRESH, SAT),
            HealthStatus::ForwardingStalled
        );
    }
}
