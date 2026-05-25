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

//! Opt-in, targeted SFU decision tracing (bead vc-8wd, Layer 2).
//!
//! This module is the cheap gate that guards the structured `tracing`
//! events emitted at the join → subscribe → forward decision points. It
//! exists so that, **with tracing OFF (the default)**, the forward hot
//! path pays *at most one relaxed atomic load* per packet and does **no**
//! per-packet allocation or string formatting.
//!
//! ## Two-stage gate
//!
//! 1. [`tracing_enabled`] — a single [`AtomicBool`] relaxed load. When the
//!    operator has not set `SFU_TRACE_ROOM`, this is `false` and every
//!    decision-point call site early-returns *before* touching the room
//!    string, formatting anything, or allocating. This is the only cost
//!    on the steady-state hot path.
//! 2. [`traced_room`] — only reached once stage 1 is `true`. Compares the
//!    candidate room id against the configured room (and, for sessions,
//!    [`traced_session`] against the optional configured session). The
//!    comparison reads an `Arc<TraceConfig>` snapshot via [`ArcSwap`]; no
//!    allocation, just borrows + `str` equality.
//!
//! ## Why this ordering is zero-cost when OFF
//!
//! The `AtomicBool` is set once at [`init`] time. When it is `false` the
//! compiler/CPU evaluates a single relaxed load and the `&&`/`if` short-
//! circuits — `traced_room` is never called, the `ArcSwap` is never
//! loaded, and the `format!`/`tracing::debug!` macro arguments are never
//! evaluated (Rust does not evaluate macro args behind a `false` guard).
//! There is no per-packet `String`, no `Arc` clone, no lock.
//!
//! ## Configuration (read once at startup)
//!
//! * `SFU_TRACE_ROOM=<room_id>` — enables tracing for exactly this room.
//!   Unset (default) ⇒ tracing globally OFF.
//! * `SFU_TRACE_SESSION=<session_id>` — optional further narrowing; when
//!   set, session-scoped events also require a session match.
//!
//! The env vars are read **once** in [`init`], called from server
//! startup. A live refresh on `SIGHUP` is intentionally NOT wired here:
//! the SFU is restarted/rolled to change trace targeting in practice, and
//! a startup-only read keeps the gate a plain `AtomicBool` with no signal-
//! handler machinery. Operators enable tracing by setting the env var and
//! restarting the pod (or a single canary pod) — see `docs/SFU_TRACING.md`.
//!
//! ## Per-packet sampling
//!
//! Even within a traced room the forward decision fires up to ~1000×/s per
//! receiver; logging every one would flood. [`should_sample_forward`]
//! provides a 1-in-N atomic counter so a traced room emits a bounded
//! trickle of forward/drop lines while still surfacing the decision mix.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::OnceLock;

use arc_swap::ArcSwap;

/// Resolved trace targeting, published once at [`init`].
#[derive(Debug, Default)]
pub struct TraceConfig {
    /// Room id to trace. `None` ⇒ tracing disabled.
    room: Option<String>,
    /// Optional session id to additionally narrow on. `None` ⇒ all
    /// sessions in the matched room are traced.
    session: Option<String>,
}

/// Fast global "is any tracing on at all?" flag.
///
/// Set once in [`init`]. The hot path reads this with [`Ordering::Relaxed`]
/// — a single uncontended load — before doing anything else. When `false`
/// the rest of the gate is never touched.
static TRACE_ENABLED: AtomicBool = AtomicBool::new(false);

/// 1-in-N sample divisor for the per-packet forward decision. Set in
/// [`init`] from `SFU_TRACE_FORWARD_SAMPLE` (default [`DEFAULT_FORWARD_SAMPLE`]).
static FORWARD_SAMPLE_N: AtomicU64 = AtomicU64::new(DEFAULT_FORWARD_SAMPLE);

/// Monotonic counter feeding the 1-in-N sampler.
static FORWARD_SAMPLE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Default sample divisor: emit roughly 1 forward-decision line per this
/// many packets within a traced room. Chosen so a 1000 pps stream yields
/// ~5 lines/s — enough to see the decision mix without flooding.
const DEFAULT_FORWARD_SAMPLE: u64 = 200;

/// Holds the published `TraceConfig`. `ArcSwap` so a future SIGHUP refresh
/// can swap a new config in without a lock; today it is written once.
fn config() -> &'static ArcSwap<TraceConfig> {
    static CONFIG: OnceLock<ArcSwap<TraceConfig>> = OnceLock::new();
    CONFIG.get_or_init(|| ArcSwap::from_pointee(TraceConfig::default()))
}

/// Read `SFU_TRACE_ROOM` / `SFU_TRACE_SESSION` / `SFU_TRACE_FORWARD_SAMPLE`
/// once and publish the resulting gate state.
///
/// Idempotent and cheap to call; intended to run exactly once from each
/// SFU server binary's `main` after the tracing subscriber is installed.
/// Logs a one-line summary so operators can confirm the gate is armed.
pub fn init() {
    let room = std::env::var("SFU_TRACE_ROOM")
        .ok()
        .filter(|s| !s.is_empty());
    let session = std::env::var("SFU_TRACE_SESSION")
        .ok()
        .filter(|s| !s.is_empty());

    if let Some(n) = std::env::var("SFU_TRACE_FORWARD_SAMPLE")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&n| n > 0)
    {
        FORWARD_SAMPLE_N.store(n, Ordering::Relaxed);
    }

    let enabled = room.is_some();
    config().store(std::sync::Arc::new(TraceConfig {
        room: room.clone(),
        session: session.clone(),
    }));
    // Publish the flag AFTER the config so a reader that observes
    // `true` always sees the matching config (single-threaded at
    // startup, but keep the ordering honest).
    TRACE_ENABLED.store(enabled, Ordering::Release);

    if enabled {
        tracing::info!(
            target: "sfu_trace",
            room = ?room,
            session = ?session,
            forward_sample = FORWARD_SAMPLE_N.load(Ordering::Relaxed),
            "SFU targeted tracing ARMED (set RUST_LOG=sfu_trace=debug to emit)"
        );
    }
}

/// `true` iff any SFU tracing is enabled at all.
///
/// This is THE hot-path guard: a single relaxed [`AtomicBool`] load. Every
/// decision-point macro is wrapped in `if trace::tracing_enabled() { … }`
/// so that, when OFF, no room string is borrowed and no macro arguments
/// are evaluated.
#[inline(always)]
pub fn tracing_enabled() -> bool {
    TRACE_ENABLED.load(Ordering::Relaxed)
}

/// `true` iff `room` is the configured trace target.
///
/// Only meaningful after [`tracing_enabled`] returned `true`; callers must
/// gate on that first to keep the common (disabled) path free of the
/// `ArcSwap` load. Allocation-free: borrows the published config and does
/// a single `str` comparison.
#[inline]
pub fn traced_room(room: &str) -> bool {
    if !tracing_enabled() {
        return false;
    }
    let cfg = config().load();
    cfg.room.as_deref() == Some(room)
}

/// `true` iff `session` matches the configured `SFU_TRACE_SESSION`, or no
/// session filter was configured (in which case all sessions match).
///
/// Like [`traced_room`], assumes the caller already passed
/// [`tracing_enabled`]. Allocation-free.
#[inline]
pub fn traced_session(session: &str) -> bool {
    let cfg = config().load();
    match cfg.session.as_deref() {
        None => true,
        Some(s) => s == session,
    }
}

/// 1-in-N sampler for the per-packet forward decision.
///
/// Returns `true` on roughly one call in `SFU_TRACE_FORWARD_SAMPLE`. Uses a
/// single relaxed fetch-add + modulo; no allocation, no lock. Callers MUST
/// have already passed [`traced_room`] so this never runs for untraced
/// rooms (and therefore never perturbs the global counter on the steady
/// hot path).
#[inline]
pub fn should_sample_forward() -> bool {
    let n = FORWARD_SAMPLE_N.load(Ordering::Relaxed).max(1);
    let c = FORWARD_SAMPLE_COUNTER.fetch_add(1, Ordering::Relaxed);
    c.is_multiple_of(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// With tracing disabled (default), `traced_room` short-circuits to
    /// `false` without consulting the config.
    #[test]
    fn disabled_gate_returns_false() {
        // TRACE_ENABLED defaults to false and `init` is not called here.
        assert!(!tracing_enabled());
        assert!(!traced_room("any-room"));
    }

    /// The sampler emits at the configured cadence and never panics on the
    /// default divisor.
    ///
    /// `#[serial]`: both sampler tests mutate the shared `FORWARD_SAMPLE_N`
    /// / `FORWARD_SAMPLE_COUNTER` process-global statics, so they must not
    /// run concurrently or they race each other's stores.
    #[test]
    #[serial_test::serial]
    fn sampler_fires_one_in_n() {
        FORWARD_SAMPLE_N.store(10, Ordering::Relaxed);
        FORWARD_SAMPLE_COUNTER.store(0, Ordering::Relaxed);
        let fired = (0..100).filter(|_| should_sample_forward()).count();
        // 100 / 10 == 10 hits (counter starts at 0 → indices 0,10,20,…,90).
        assert_eq!(fired, 10);
    }

    /// A zero/garbage divisor is clamped to 1 (sample everything) rather
    /// than dividing by zero.
    #[test]
    #[serial_test::serial]
    fn sampler_clamps_zero_divisor() {
        FORWARD_SAMPLE_N.store(0, Ordering::Relaxed);
        FORWARD_SAMPLE_COUNTER.store(0, Ordering::Relaxed);
        assert!(should_sample_forward());
    }
}
