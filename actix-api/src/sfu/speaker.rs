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

//! Per-sender speaker scoring via EWMA over `RoutingHeader.audio_level`.
//!
//! Implements ADR-0002 p3-1: each AUDIO MediaPacket feeds the sender's EWMA
//! (α = 0.3). `is_speaking()` gates on the EWMA exceeding a floor AND a recent
//! VAD hint (`RoutingHeader.is_speaking`) within a short recency window.
//!
//! p3-2 layers a 200ms periodic tick on top of the decision-pure scorer:
//! [`SpeakerTick`] maintains the current `ActiveSpeakerSet` (top-N=4) with
//! entry/exit hysteresis and a monotonic `generation` counter, fanning out
//! snapshots over a `tokio::sync::watch` channel for downstream consumers
//! (p3-3 NATS publisher, p3-5 forwarder integration).

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use protobuf::Message as ProtoMessage;
use tokio::sync::{watch, RwLock};
use tokio::task::JoinHandle;
use tracing::warn;
use videocall_types::protos::packet_wrapper::packet_wrapper::PacketType;
use videocall_types::protos::packet_wrapper::PacketWrapper;
use videocall_types::protos::speaker_update_packet::{SpeakerEntry, SpeakerUpdate};
use videocall_types::user_id::to_user_id_bytes;
use videocall_types::SYSTEM_USER_ID;

use crate::actors::session_logic::SessionId;

/// EWMA smoothing factor for incoming audio-level observations.
const ALPHA: f32 = 0.3;
/// Minimum EWMA below which a sender is never considered "speaking".
const SPEAKING_FLOOR: f32 = 0.05;
/// How recently the VAD hint (`is_speaking_hint == true`) must have been
/// observed for `is_speaking()` to return true.
const VAD_RECENCY: Duration = Duration::from_millis(400);

/// Per-sender state tracked by the scorer.
struct ScoreState {
    /// Smoothed audio level in `[0, 1]`.
    ewma: f32,
    /// Wall-clock time of the most recent `observe()` call.
    last_update: Instant,
    /// Raw value of `is_speaking_hint` from the most recent observation
    /// (kept for telemetry/debugging; the speaking gate uses the
    /// time-windowed `last_speaking_hint_at` below).
    last_is_speaking_hint: bool,
    /// `Instant` when `is_speaking_hint` was last observed as `true`.
    /// `None` until the first true hint is seen.
    last_speaking_hint_at: Option<Instant>,
}

/// Tracks per-sender speaker scores derived from audio-level observations.
///
/// Callers should invoke [`SpeakerScorer::observe`] for every AUDIO
/// `MediaPacket` they receive, then query [`SpeakerScorer::score`],
/// [`SpeakerScorer::is_speaking`], or [`SpeakerScorer::top_n`] to drive
/// downstream forwarding decisions.
pub struct SpeakerScorer {
    scores: HashMap<SessionId, ScoreState>,
}

impl SpeakerScorer {
    /// Create a new empty scorer.
    pub fn new() -> Self {
        Self {
            scores: HashMap::new(),
        }
    }

    /// Record an audio-level observation for `sender`.
    ///
    /// `audio_level` is expected in `[0, 1]` (matches
    /// `RoutingHeader.audio_level`); it is clamped defensively. The sender's
    /// EWMA is updated as `α * audio_level + (1 - α) * ewma_prev`.
    pub fn observe(&mut self, sender: SessionId, audio_level: f32, is_speaking_hint: bool) {
        let clamped = audio_level.clamp(0.0, 1.0);
        let now = Instant::now();
        let entry = self.scores.entry(sender).or_insert_with(|| ScoreState {
            ewma: 0.0,
            last_update: now,
            last_is_speaking_hint: false,
            last_speaking_hint_at: None,
        });
        entry.ewma = ALPHA * clamped + (1.0 - ALPHA) * entry.ewma;
        entry.last_update = now;
        entry.last_is_speaking_hint = is_speaking_hint;
        if is_speaking_hint {
            entry.last_speaking_hint_at = Some(now);
        }
    }

    /// Return the current EWMA score for `sender`, or `0.0` if unknown.
    pub fn score(&self, sender: SessionId) -> f32 {
        self.scores.get(&sender).map(|s| s.ewma).unwrap_or(0.0)
    }

    /// Return `true` iff the sender's EWMA exceeds the speaking floor AND
    /// its `is_speaking_hint` was observed as `true` within the last
    /// [`VAD_RECENCY`] window.
    pub fn is_speaking(&self, sender: SessionId) -> bool {
        let Some(state) = self.scores.get(&sender) else {
            return false;
        };
        if state.ewma <= SPEAKING_FLOOR {
            return false;
        }
        match state.last_speaking_hint_at {
            Some(t) => Instant::now().duration_since(t) <= VAD_RECENCY,
            None => false,
        }
    }

    /// Return up to `n` `(sender, score)` pairs sorted by score descending.
    pub fn top_n(&self, n: usize) -> Vec<(SessionId, f32)> {
        let mut all: Vec<(SessionId, f32)> =
            self.scores.iter().map(|(sid, s)| (*sid, s.ewma)).collect();
        // Descending by score; total_cmp avoids NaN footguns.
        all.sort_by(|a, b| b.1.total_cmp(&a.1));
        all.truncate(n);
        all
    }

    /// Drop all tracked state for `sender` (e.g., on room exit).
    pub fn forget(&mut self, sender: SessionId) {
        self.scores.remove(&sender);
    }
}

impl Default for SpeakerScorer {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// p3-2: 200ms tick + hysteresis + generation counter
// ---------------------------------------------------------------------------

/// Maximum number of senders in the published active speaker set.
pub const MAX_SPEAKERS: usize = 4;

/// Tick cadence for the speaker selector.
pub const TICK_INTERVAL: Duration = Duration::from_millis(200);

/// Minimum time a sender's score must stay above [`SPEAKING_FLOOR`]
/// before being admitted to the active set (one full tick).
pub const ENTRY_WINDOW: Duration = Duration::from_millis(200);

/// Minimum time a sender's score must stay below [`SPEAKING_FLOOR`]
/// before being evicted from the active set (four consecutive ticks).
pub const EXIT_WINDOW: Duration = Duration::from_millis(800);

/// Snapshot of the currently-selected speakers for a room.
///
/// `top` is sorted by EWMA score descending; ordering changes count as a
/// set change and bump [`Self::generation`] (see ADR-0002 §5).
#[derive(Debug, Clone)]
pub struct ActiveSpeakerSet {
    /// Up to [`MAX_SPEAKERS`] active speakers, sorted by score descending.
    pub top: Vec<SessionId>,
    /// Monotonic counter; bumped on every membership or order change.
    pub generation: u64,
    /// Wall-clock time of the most recent change to `top`.
    pub last_change: Instant,
}

impl ActiveSpeakerSet {
    /// Construct the initial empty set (`top = []`, `generation = 0`).
    ///
    /// Used by [`SpeakerTick`] for its starting `watch::channel` value and by
    /// `ChatServer` (p3-5) when materialising a brand-new room before the
    /// per-room tick has fired even once.
    pub fn empty() -> Self {
        Self {
            top: Vec::new(),
            generation: 0,
            last_change: Instant::now(),
        }
    }
}

/// Per-sender candidacy timing used by the hysteresis state machine.
#[derive(Debug, Clone, Copy, Default)]
struct CandidateState {
    /// First tick at which the sender was observed above
    /// [`SPEAKING_FLOOR`] in the current "rising" streak, or `None`
    /// if the most recent observation was below.
    above_since: Option<Instant>,
    /// First tick at which the sender was observed below
    /// [`SPEAKING_FLOOR`] in the current "falling" streak, or `None`
    /// if the most recent observation was above.
    below_since: Option<Instant>,
}

/// Sink for SpeakerUpdate broadcasts, abstracted so unit tests can swap a
/// real `async_nats::Client` for a collecting fake. Fire-and-forget by
/// design: implementations spawn their own async work and must not block
/// the tick loop. Failures are an implementation concern (typically logged
/// and dropped) — losing a single SpeakerUpdate is preferable to stalling
/// the 200ms cadence.
pub trait SpeakerPublisher: Send + Sync + fmt::Debug {
    /// Publish `payload` on `subject`. Must not block; implementations
    /// spawn the async send themselves.
    fn publish(&self, subject: String, payload: Vec<u8>);
}

/// Production [`SpeakerPublisher`] backed by an `async_nats::Client`.
///
/// Cloning the inner `async_nats::Client` is cheap (it's `Arc`-wrapped),
/// so each `publish` call spawns a tokio task with its own handle and
/// returns immediately.
#[derive(Clone)]
pub struct NatsSpeakerPublisher {
    client: async_nats::Client,
}

impl NatsSpeakerPublisher {
    pub fn new(client: async_nats::Client) -> Self {
        Self { client }
    }
}

impl fmt::Debug for NatsSpeakerPublisher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NatsSpeakerPublisher").finish()
    }
}

impl SpeakerPublisher for NatsSpeakerPublisher {
    fn publish(&self, subject: String, payload: Vec<u8>) {
        let client = self.client.clone();
        tokio::spawn(async move {
            if let Err(e) = client.publish(subject.clone(), payload.into()).await {
                warn!(
                    target: "sfu_speaker",
                    subject = %subject,
                    error = %e,
                    "SpeakerUpdate publish failed"
                );
            }
        });
    }
}

/// Cancels the background tick task when dropped.
///
/// Returned by [`SpeakerTick::run`]; aborts the underlying tokio task on
/// `Drop` so callers cannot leak the loop.
#[derive(Debug)]
pub struct TickHandle {
    join: JoinHandle<()>,
}

impl Drop for TickHandle {
    fn drop(&mut self) {
        self.join.abort();
    }
}

/// 200ms periodic tick that maintains an [`ActiveSpeakerSet`] with hysteresis.
///
/// Construct via [`SpeakerTick::new`] with an `Arc<RwLock<SpeakerScorer>>`
/// shared with the packet-ingest path. Subscribe to snapshots via
/// [`SpeakerTick::subscribe`], then drive the loop with [`SpeakerTick::run`]
/// (which moves `self`). The returned [`TickHandle`] aborts the task on drop.
pub struct SpeakerTick {
    scorer: Arc<RwLock<SpeakerScorer>>,
    state: Arc<RwLock<TickState>>,
    tx: watch::Sender<ActiveSpeakerSet>,
    rx: watch::Receiver<ActiveSpeakerSet>,
    interval: Duration,
    /// Sanitized room id used as the middle segment of
    /// `room.{room_id}.system`. Empty when no publisher is attached.
    room_id: String,
    /// p3-3: optional sink for SpeakerUpdate broadcasts. `None` means
    /// "compute set transitions but do not announce them" — useful for
    /// the hysteresis-only unit tests inherited from p3-2.
    publisher: Option<Arc<dyn SpeakerPublisher>>,
}

/// Internal mutable state used by the tick task and `current()` accessor.
#[derive(Debug)]
struct TickState {
    candidates: HashMap<SessionId, CandidateState>,
    current: ActiveSpeakerSet,
}

impl SpeakerTick {
    /// Create a new speaker tick over `scorer` with the default 200ms cadence,
    /// announcing every generation change to `room.{room_id}.system` via
    /// `publisher`. Production callers (chat_server) pass a
    /// [`NatsSpeakerPublisher`] wrapping the shared `async_nats::Client`.
    pub fn new(
        scorer: Arc<RwLock<SpeakerScorer>>,
        room_id: impl Into<String>,
        publisher: Arc<dyn SpeakerPublisher>,
    ) -> Self {
        Self::with_interval(scorer, TICK_INTERVAL, room_id, Some(publisher))
    }

    /// Create a tick with a custom interval and optional publisher
    /// (primarily for tests). When `publisher` is `None`, generation
    /// changes still update the `watch` channel but no SpeakerUpdate is
    /// broadcast — the p3-2 hysteresis tests rely on this.
    pub fn with_interval(
        scorer: Arc<RwLock<SpeakerScorer>>,
        interval: Duration,
        room_id: impl Into<String>,
        publisher: Option<Arc<dyn SpeakerPublisher>>,
    ) -> Self {
        let initial = ActiveSpeakerSet::empty();
        let (tx, rx) = watch::channel(initial.clone());
        Self {
            scorer,
            state: Arc::new(RwLock::new(TickState {
                candidates: HashMap::new(),
                current: initial,
            })),
            tx,
            rx,
            interval,
            room_id: room_id.into(),
            publisher,
        }
    }

    /// Subscribe to active-speaker-set updates.
    ///
    /// The initial value is the empty set with `generation = 0`.
    pub fn subscribe(&self) -> watch::Receiver<ActiveSpeakerSet> {
        self.rx.clone()
    }

    /// Current snapshot, suitable for synchronous callers that don't need
    /// the change notifications from the watch channel.
    pub async fn current(&self) -> ActiveSpeakerSet {
        self.state.read().await.current.clone()
    }

    /// Spawn the background tick task. Returns a handle that aborts the
    /// task on drop.
    pub fn run(self) -> TickHandle {
        let scorer = self.scorer;
        let state = self.state;
        let tx = self.tx;
        let interval = self.interval;
        let room_id = self.room_id;
        let publisher = self.publisher;
        let join = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            // Skip the immediate first-tick fire so the loop body actually
            // waits one `interval` before the first evaluation. Without
            // this, MissedTickBehavior::Burst would otherwise also let
            // ticks bunch up under load.
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // Consume the initial immediate tick so the first scoring pass
            // happens at `t = interval`, matching the ADR's worst-case
            // "200ms detection floor" wording.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                // Use tokio's `Instant` (converted to `std::time::Instant`)
                // so the hysteresis windows respect `tokio::time::pause`
                // in tests; in production both clocks advance identically.
                let now = tokio::time::Instant::now().into_std();
                Self::tick_once(&scorer, &state, &tx, now, &room_id, publisher.as_deref()).await;
            }
        });
        TickHandle { join }
    }

    /// Drive a single deterministic scoring pass at the supplied virtual
    /// `now`. Test-only seam so unit tests can exercise hysteresis windows
    /// without scheduling a real interval loop. Wraps the private
    /// [`Self::tick_once`] over `self`'s shared state.
    #[cfg(test)]
    pub(crate) async fn drive_tick_for_test(&self, now: Instant) {
        Self::tick_once(
            &self.scorer,
            &self.state,
            &self.tx,
            now,
            &self.room_id,
            self.publisher.as_deref(),
        )
        .await;
    }

    /// One scoring pass. Extracted so tests can drive ticks deterministically
    /// by supplying a synthetic `now`.
    async fn tick_once(
        scorer: &Arc<RwLock<SpeakerScorer>>,
        state: &Arc<RwLock<TickState>>,
        tx: &watch::Sender<ActiveSpeakerSet>,
        now: Instant,
        room_id: &str,
        publisher: Option<&dyn SpeakerPublisher>,
    ) {
        // Lift the scorer read into a snapshot so we can drop the lock
        // before mutating tick state (the scorer also serves the ingest
        // hot path; minimise contention).
        let top = {
            let guard = scorer.read().await;
            // Pull more than MAX_SPEAKERS so a noisy 5th sender doesn't
            // displace the legitimate 4th — we still cap at MAX_SPEAKERS
            // after hysteresis filtering.
            guard.top_n(MAX_SPEAKERS * 4)
        };

        let mut st = state.write().await;
        // Existing members get their slot kept until the exit window
        // elapses; anyone whose score is currently above threshold must
        // ALSO have crossed the entry window to be admitted.
        let prev_members: HashSet<SessionId> = st.current.top.iter().copied().collect();

        // Update per-sender above/below streaks against this tick's scores.
        // Senders not present in `top` are implicitly below threshold.
        let observed: HashMap<SessionId, f32> = top.iter().copied().collect();

        // Refresh streaks for everyone we know about — both observed
        // senders and any existing candidates/members not in this tick.
        let known: HashSet<SessionId> = observed
            .keys()
            .copied()
            .chain(st.candidates.keys().copied())
            .chain(prev_members.iter().copied())
            .collect();

        for sid in &known {
            let score = observed.get(sid).copied().unwrap_or(0.0);
            let cand = st.candidates.entry(*sid).or_default();
            if score > SPEAKING_FLOOR {
                if cand.above_since.is_none() {
                    cand.above_since = Some(now);
                }
                cand.below_since = None;
            } else {
                if cand.below_since.is_none() {
                    cand.below_since = Some(now);
                }
                cand.above_since = None;
            }
        }

        // Build the next set:
        //   - sender admitted iff above-streak duration >= ENTRY_WINDOW
        //   - existing member retained until below-streak >= EXIT_WINDOW
        // Then cap at MAX_SPEAKERS by score descending.
        let mut eligible: Vec<(SessionId, f32)> = Vec::new();
        for (sid, score) in &top {
            let cand = st.candidates.get(sid).copied().unwrap_or_default();
            let above_long_enough = cand
                .above_since
                .map(|t| now.duration_since(t) >= ENTRY_WINDOW)
                .unwrap_or(false);
            let was_member = prev_members.contains(sid);
            let below_long_enough = cand
                .below_since
                .map(|t| now.duration_since(t) >= EXIT_WINDOW)
                .unwrap_or(false);
            if above_long_enough || (was_member && !below_long_enough) {
                eligible.push((*sid, *score));
            }
        }
        // Also keep prior members whose score dropped to zero (not even in
        // `top`) but who haven't yet hit the exit window.
        for sid in &prev_members {
            if observed.contains_key(sid) {
                continue;
            }
            let cand = st.candidates.get(sid).copied().unwrap_or_default();
            let below_long_enough = cand
                .below_since
                .map(|t| now.duration_since(t) >= EXIT_WINDOW)
                .unwrap_or(false);
            if !below_long_enough {
                eligible.push((*sid, 0.0));
            }
        }

        eligible.sort_by(|a, b| b.1.total_cmp(&a.1));
        eligible.truncate(MAX_SPEAKERS);
        let next_top: Vec<SessionId> = eligible.into_iter().map(|(sid, _)| sid).collect();

        // Set change detection: membership-or-order change bumps generation.
        let snapshot_for_publish = if next_top != st.current.top {
            let new_gen = st.current.generation.wrapping_add(1);
            st.current = ActiveSpeakerSet {
                top: next_top,
                generation: new_gen,
                last_change: now,
            };
            // `send` only fails if there are zero receivers; we always hold
            // one ourselves in `self.rx`, so this is infallible in practice.
            let _ = tx.send(st.current.clone());
            Some(st.current.clone())
        } else {
            None
        };

        // Garbage-collect candidates that are no longer relevant: not in
        // the current set, not in the latest observation, and either have
        // never been above or have exceeded the exit window below.
        let live: HashSet<SessionId> = st
            .current
            .top
            .iter()
            .copied()
            .chain(observed.keys().copied())
            .collect();
        st.candidates.retain(|sid, cand| {
            if live.contains(sid) {
                return true;
            }
            // Keep until the below-streak passes EXIT_WINDOW so a recent
            // member can still graduate to "exited" cleanly next tick.
            match cand.below_since {
                Some(t) => now.duration_since(t) < EXIT_WINDOW,
                None => false,
            }
        });
        drop(st);

        // p3-3: on every generation bump, broadcast the new active set to
        // `room.{room_id}.system` so spill pods + clients can react
        // without polling. No-op when no publisher is wired (test config)
        // or `room_id` is empty (also test config).
        if let (Some(snap), Some(pub_)) = (snapshot_for_publish, publisher) {
            if !room_id.is_empty() {
                let payload = Self::build_speaker_update_payload(scorer, &snap).await;
                let subject = format!("room.{}.system", room_id);
                pub_.publish(subject, payload);
            }
        }
    }

    /// Serialize an [`ActiveSpeakerSet`] snapshot as a `SpeakerUpdate`
    /// wrapped in a `PacketWrapper` (PacketType::SPEAKER_UPDATE,
    /// user_id = SYSTEM_USER_ID), matching the wire shape that p3-5
    /// (forwarder) and p3-6 (client) decode.
    ///
    /// Re-acquires the scorer read lock to fill in `score` and
    /// `is_speaking` per entry; the lock is held only for the duration
    /// of the snapshot copy.
    async fn build_speaker_update_payload(
        scorer: &Arc<RwLock<SpeakerScorer>>,
        snap: &ActiveSpeakerSet,
    ) -> Vec<u8> {
        let top_speakers: Vec<SpeakerEntry> = {
            let guard = scorer.read().await;
            snap.top
                .iter()
                .map(|sid| SpeakerEntry {
                    session_id: *sid,
                    score: guard.score(*sid),
                    is_speaking: guard.is_speaking(*sid),
                    ..Default::default()
                })
                .collect()
        };
        let update = SpeakerUpdate {
            top_speakers,
            generation: snap.generation,
            ..Default::default()
        };
        let data = update.write_to_bytes().unwrap_or_default();
        let wrapper = PacketWrapper {
            packet_type: PacketType::SPEAKER_UPDATE.into(),
            user_id: to_user_id_bytes(SYSTEM_USER_ID),
            data,
            ..Default::default()
        };
        wrapper.write_to_bytes().unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn observe_then_score_applies_alpha_from_zero() {
        let mut s = SpeakerScorer::new();
        s.observe(1, 0.8, true);
        // Prior EWMA is 0, so new EWMA == ALPHA * 0.8.
        let expected = ALPHA * 0.8;
        assert!((s.score(1) - expected).abs() < 1e-6);
        assert_eq!(s.score(999), 0.0);
    }

    #[test]
    fn is_speaking_respects_floor() {
        let mut s = SpeakerScorer::new();
        // Drive EWMA to just under SPEAKING_FLOOR. With ALPHA = 0.3 starting
        // from 0, a single observation of `level` yields ewma = 0.3 * level.
        // So level just-under = 0.05/0.3 - eps.
        let just_under = (SPEAKING_FLOOR / ALPHA) - 0.01;
        s.observe(1, just_under, true);
        assert!(s.score(1) < SPEAKING_FLOOR);
        assert!(!s.is_speaking(1));

        // Now push above the floor with a fresh sender.
        let just_over = (SPEAKING_FLOOR / ALPHA) + 0.05;
        s.observe(2, just_over, true);
        assert!(s.score(2) > SPEAKING_FLOOR);
        assert!(s.is_speaking(2));
    }

    #[test]
    fn is_speaking_respects_vad_recency_window() {
        let mut s = SpeakerScorer::new();
        // Push EWMA well above the floor with hint=true.
        s.observe(1, 0.9, true);
        assert!(s.is_speaking(1));

        // Wait past the 400ms VAD recency window.
        thread::sleep(Duration::from_millis(450));

        // A subsequent observation with hint=false keeps EWMA high but the
        // most recent true-hint Instant is now stale.
        s.observe(1, 0.9, false);
        assert!(s.score(1) > SPEAKING_FLOOR);
        assert!(!s.is_speaking(1));
    }

    #[test]
    fn top_n_returns_sorted_desc_and_respects_n() {
        let mut s = SpeakerScorer::new();
        s.observe(10, 0.2, false);
        s.observe(20, 0.9, false);
        s.observe(30, 0.5, false);

        let top2 = s.top_n(2);
        assert_eq!(top2.len(), 2);
        assert_eq!(top2[0].0, 20);
        assert_eq!(top2[1].0, 30);
        assert!(top2[0].1 >= top2[1].1);

        // n larger than population returns all entries.
        let top10 = s.top_n(10);
        assert_eq!(top10.len(), 3);
        assert_eq!(top10[0].0, 20);
        assert_eq!(top10[2].0, 10);

        // n = 0 returns empty.
        assert!(s.top_n(0).is_empty());
    }

    #[test]
    fn forget_removes_sender() {
        let mut s = SpeakerScorer::new();
        s.observe(1, 0.7, true);
        s.observe(2, 0.5, true);
        assert!(s.score(1) > 0.0);

        s.forget(1);
        assert_eq!(s.score(1), 0.0);

        let top = s.top_n(10);
        assert_eq!(top.len(), 1);
        assert_eq!(top[0].0, 2);
    }

    // -----------------------------------------------------------------
    // p3-2: SpeakerTick tests
    // -----------------------------------------------------------------

    /// Drive a single deterministic tick at the supplied virtual time
    /// against a freshly-constructed tick. Avoids the live spawn loop so
    /// hysteresis windows can be exercised in microseconds of wall time.
    async fn drive_tick(tick: &SpeakerTick, now: Instant) {
        SpeakerTick::tick_once(
            &tick.scorer,
            &tick.state,
            &tick.tx,
            now,
            &tick.room_id,
            tick.publisher.as_deref(),
        )
        .await;
    }

    /// Test-only [`SpeakerPublisher`] that collects every publish call into
    /// a shared `Vec` so assertions can introspect the bytes (and decode
    /// them as `PacketWrapper`/`SpeakerUpdate`).
    type PublishedLog = Arc<std::sync::Mutex<Vec<(String, Vec<u8>)>>>;

    #[derive(Debug, Default, Clone)]
    struct FakePublisher {
        published: PublishedLog,
    }

    impl FakePublisher {
        fn new() -> Self {
            Self::default()
        }
        fn drain(&self) -> Vec<(String, Vec<u8>)> {
            std::mem::take(&mut *self.published.lock().unwrap())
        }
    }

    impl SpeakerPublisher for FakePublisher {
        fn publish(&self, subject: String, payload: Vec<u8>) {
            self.published.lock().unwrap().push((subject, payload));
        }
    }

    /// Build a scorer that already has a high stable EWMA for `sid` by
    /// repeatedly observing `level` so a single tick reads `> SPEAKING_FLOOR`.
    fn seed_high_score(scorer: &mut SpeakerScorer, sid: SessionId, level: f32) {
        // Five observations at the same level converge above floor for any
        // level above 0.05 (since EWMA reaches ~83% of step input by tick 5).
        for _ in 0..5 {
            scorer.observe(sid, level, true);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn tick_fires_periodically_and_observes_scorer() {
        let scorer = Arc::new(RwLock::new(SpeakerScorer::new()));
        let tick = SpeakerTick::with_interval(scorer.clone(), Duration::from_millis(200), "", None);
        let mut rx = tick.subscribe();

        // Seed a dominant speaker BEFORE the tick task starts so the very
        // first scoring pass already sees it.
        {
            let mut s = scorer.write().await;
            seed_high_score(&mut s, 42, 0.9);
        }

        let _handle = tick.run();
        // Let the spawned task initialise its interval and consume the
        // immediate first tick before we advance virtual time.
        tokio::task::yield_now().await;

        // Advance ~600ms — three ticks at 200ms cadence. The entry window
        // is 200ms (one full tick above threshold), so after multiple
        // ticks the speaker set must include 42.
        for _ in 0..6 {
            tokio::time::advance(Duration::from_millis(120)).await;
            tokio::task::yield_now().await;
        }

        // Drain the watch channel to the latest value.
        let latest = rx.borrow_and_update().clone();
        assert!(
            latest.top.contains(&42),
            "speaker 42 should be in the published set after multiple ticks: {:?}",
            latest.top
        );
        assert!(latest.generation >= 1, "generation must have advanced");
    }

    #[tokio::test]
    async fn hysteresis_entry_requires_full_window() {
        let scorer = Arc::new(RwLock::new(SpeakerScorer::new()));
        let tick = SpeakerTick::with_interval(scorer.clone(), Duration::from_millis(200), "", None);

        // Sender 7 has a high EWMA but we only give it one tick above —
        // less than the 200ms entry window required for admission.
        {
            let mut s = scorer.write().await;
            seed_high_score(&mut s, 7, 0.8);
        }

        // First tick at t=0: above_since recorded, but duration is 0.
        let t0 = Instant::now();
        drive_tick(&tick, t0).await;
        let snap = tick.current().await;
        assert!(
            snap.top.is_empty(),
            "must not admit on the first tick (entry window not elapsed)"
        );
        assert_eq!(snap.generation, 0, "generation stays 0 with no change");

        // Sender drops below threshold before 200ms elapses. Drive a tick
        // 100ms later with the scorer drained back down. We forge a fresh
        // scorer-state by inserting a low-EWMA sender.
        {
            let mut s = scorer.write().await;
            s.forget(7);
            // Observe at zero to leave 7 entirely out of top_n.
            s.observe(99, 0.0, false);
        }
        let t1 = t0 + Duration::from_millis(100);
        drive_tick(&tick, t1).await;
        let snap = tick.current().await;
        assert!(
            snap.top.is_empty(),
            "brief flash below 200ms must NOT enter the set"
        );
        assert_eq!(snap.generation, 0);
    }

    #[tokio::test]
    async fn hysteresis_exit_requires_full_window() {
        let scorer = Arc::new(RwLock::new(SpeakerScorer::new()));
        let tick = SpeakerTick::with_interval(scorer.clone(), Duration::from_millis(200), "", None);

        // Bring sender 5 into the set: high score across an entry window.
        {
            let mut s = scorer.write().await;
            seed_high_score(&mut s, 5, 0.9);
        }
        let t0 = Instant::now();
        // Tick #1 marks above_since.
        drive_tick(&tick, t0).await;
        // Tick #2 200ms later — entry window satisfied → admitted.
        drive_tick(&tick, t0 + Duration::from_millis(200)).await;
        let snap = tick.current().await;
        assert!(
            snap.top.contains(&5),
            "sender must be admitted after entry window elapses: {:?}",
            snap.top
        );
        let gen_after_entry = snap.generation;
        assert!(gen_after_entry >= 1);

        // Now drop the sender below threshold. We drive ticks at +400ms
        // and +600ms (i.e. 200ms and 400ms after the admission tick) —
        // both inside the 800ms exit window, so the sender must persist.
        {
            let mut s = scorer.write().await;
            s.forget(5);
        }
        drive_tick(&tick, t0 + Duration::from_millis(400)).await;
        drive_tick(&tick, t0 + Duration::from_millis(600)).await;
        let snap = tick.current().await;
        assert!(
            snap.top.contains(&5),
            "sender must remain inside the 800ms exit window: {:?}",
            snap.top
        );
        assert_eq!(
            snap.generation, gen_after_entry,
            "generation must not bump while set is unchanged"
        );

        // Past the exit window (>= 800ms below threshold) → eviction.
        // First below-tick was at t0+400ms, so we need t0+1200ms+.
        drive_tick(&tick, t0 + Duration::from_millis(1300)).await;
        let snap = tick.current().await;
        assert!(
            !snap.top.contains(&5),
            "sender must be evicted after exit window elapses"
        );
        assert!(snap.generation > gen_after_entry, "exit bumps generation");
    }

    #[tokio::test]
    async fn generation_increments_only_on_set_change() {
        let scorer = Arc::new(RwLock::new(SpeakerScorer::new()));
        let tick = SpeakerTick::with_interval(scorer.clone(), Duration::from_millis(200), "", None);

        // Seed two stable speakers above threshold.
        {
            let mut s = scorer.write().await;
            seed_high_score(&mut s, 1, 0.9);
            seed_high_score(&mut s, 2, 0.6);
        }

        let t0 = Instant::now();
        drive_tick(&tick, t0).await; // marks above_since
        drive_tick(&tick, t0 + Duration::from_millis(200)).await; // admits
        let snap = tick.current().await;
        let admitted_top = snap.top.clone();
        let gen1 = snap.generation;
        assert!(admitted_top.contains(&1) && admitted_top.contains(&2));
        assert!(gen1 >= 1);

        // Several more ticks with the same scorer state → no change.
        // We re-observe to keep EWMA fresh but the relative order stays.
        for k in 1..=4 {
            {
                let mut s = scorer.write().await;
                s.observe(1, 0.9, true);
                s.observe(2, 0.6, true);
            }
            drive_tick(&tick, t0 + Duration::from_millis(200 + 200 * k)).await;
            let snap = tick.current().await;
            assert_eq!(snap.top, admitted_top, "membership/order must be stable");
            assert_eq!(snap.generation, gen1, "generation must not bump");
        }

        // Introduce a new dominant speaker; let it traverse the entry
        // window so it's admitted. Order should change (new top), bumping
        // generation exactly once.
        {
            let mut s = scorer.write().await;
            seed_high_score(&mut s, 3, 0.99);
            // Keep 1 and 2 alive too so they're not evicted.
            s.observe(1, 0.9, true);
            s.observe(2, 0.6, true);
        }
        drive_tick(&tick, t0 + Duration::from_millis(1200)).await; // marks above_since for 3
        drive_tick(&tick, t0 + Duration::from_millis(1400)).await; // entry window for 3 elapses
        let snap = tick.current().await;
        assert!(snap.top.contains(&3), "new speaker admitted");
        assert!(
            snap.generation > gen1,
            "generation must bump when set changes (got {} vs {})",
            snap.generation,
            gen1
        );
    }

    #[tokio::test]
    async fn top_n_cap_respected_with_excess_speakers() {
        let scorer = Arc::new(RwLock::new(SpeakerScorer::new()));
        let tick = SpeakerTick::with_interval(scorer.clone(), Duration::from_millis(200), "", None);

        // Seed 6 speakers all above threshold, with distinct scores so
        // sort ordering is deterministic. MAX_SPEAKERS = 4 → the lowest
        // two must be excluded.
        {
            let mut s = scorer.write().await;
            seed_high_score(&mut s, 10, 0.95);
            seed_high_score(&mut s, 20, 0.90);
            seed_high_score(&mut s, 30, 0.85);
            seed_high_score(&mut s, 40, 0.80);
            seed_high_score(&mut s, 50, 0.75);
            seed_high_score(&mut s, 60, 0.70);
        }

        let t0 = Instant::now();
        drive_tick(&tick, t0).await;
        drive_tick(&tick, t0 + Duration::from_millis(200)).await;

        let snap = tick.current().await;
        assert_eq!(
            snap.top.len(),
            MAX_SPEAKERS,
            "top must be capped at MAX_SPEAKERS, got {:?}",
            snap.top
        );
        // The four highest scores are 10, 20, 30, 40 (in that order).
        assert_eq!(snap.top, vec![10, 20, 30, 40]);
        assert!(!snap.top.contains(&50));
        assert!(!snap.top.contains(&60));
    }

    // -----------------------------------------------------------------
    // p3-3: SpeakerUpdate NATS publication
    // -----------------------------------------------------------------

    /// On every generation change, the tick must publish exactly one
    /// `PacketWrapper<SpeakerUpdate>` to `room.{room_id}.system`. A
    /// no-op tick (set unchanged) must NOT publish.
    #[tokio::test]
    async fn publishes_speaker_update_on_generation_change() {
        use protobuf::Message as ProtoMessage;

        let scorer = Arc::new(RwLock::new(SpeakerScorer::new()));
        let publisher = Arc::new(FakePublisher::new());
        let tick = SpeakerTick::with_interval(
            scorer.clone(),
            Duration::from_millis(200),
            "room-42",
            Some(publisher.clone() as Arc<dyn SpeakerPublisher>),
        );

        // Seed a dominant speaker and let it traverse the entry window so
        // the second tick promotes it → generation bumps from 0 → 1.
        {
            let mut s = scorer.write().await;
            seed_high_score(&mut s, 99, 0.9);
        }
        let t0 = Instant::now();
        drive_tick(&tick, t0).await; // marks above_since, no change yet
        assert!(publisher.drain().is_empty(), "no publish before admission",);

        drive_tick(&tick, t0 + Duration::from_millis(200)).await; // admit
        let snap = tick.current().await;
        assert!(snap.top.contains(&99), "speaker admitted: {:?}", snap.top);
        assert_eq!(snap.generation, 1);

        let published = publisher.drain();
        assert_eq!(
            published.len(),
            1,
            "exactly one SpeakerUpdate published on admission"
        );
        let (subject, payload) = &published[0];
        assert_eq!(
            subject, "room.room-42.system",
            "publish targets room.{{room}}.system"
        );

        // The payload must round-trip as a PacketWrapper(SPEAKER_UPDATE)
        // carrying a SpeakerUpdate with the expected generation + entries.
        let wrapper = PacketWrapper::parse_from_bytes(payload).expect("decode PacketWrapper");
        assert_eq!(
            wrapper.packet_type,
            PacketType::SPEAKER_UPDATE.into(),
            "wrapper type must be SPEAKER_UPDATE"
        );
        let update = SpeakerUpdate::parse_from_bytes(&wrapper.data).expect("decode SpeakerUpdate");
        assert_eq!(update.generation, 1);
        assert_eq!(update.top_speakers.len(), 1);
        assert_eq!(update.top_speakers[0].session_id, 99);
        assert!(update.top_speakers[0].score > SPEAKING_FLOOR);

        // A no-op tick must NOT publish even though set membership is
        // stable (regression guard for unconditional publishing).
        {
            let mut s = scorer.write().await;
            // Re-observe to keep EWMA fresh without changing order.
            s.observe(99, 0.9, true);
        }
        drive_tick(&tick, t0 + Duration::from_millis(400)).await;
        assert!(
            publisher.drain().is_empty(),
            "no publish when generation is unchanged",
        );
    }

    /// When constructed without a publisher (the p3-2 hysteresis-test
    /// path), generation changes must still update internal state but
    /// must NOT panic or attempt to publish.
    #[tokio::test]
    async fn no_publisher_means_no_publish() {
        let scorer = Arc::new(RwLock::new(SpeakerScorer::new()));
        let tick = SpeakerTick::with_interval(scorer.clone(), Duration::from_millis(200), "", None);
        {
            let mut s = scorer.write().await;
            seed_high_score(&mut s, 1, 0.9);
        }
        let t0 = Instant::now();
        drive_tick(&tick, t0).await;
        drive_tick(&tick, t0 + Duration::from_millis(200)).await;
        // If we got here without panicking, the None-publisher branch is
        // exercised. Sanity-check the state still advanced.
        let snap = tick.current().await;
        assert!(snap.top.contains(&1));
        assert_eq!(snap.generation, 1);
    }
}
