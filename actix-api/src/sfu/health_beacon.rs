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

//! Owner-pod 5s health beacons on `room.{room}.system` (P6 wave-2 bead vc-kol / p6-7).
//!
//! Every owner pod (per [`crate::sfu::affinity::is_owner`]) runs one background
//! task per owned room. Every [`BEACON_INTERVAL`] the task assembles a
//! [`HealthBeaconPacket`] from the live [`RoomState`] plus a cheap CPU-load
//! estimate, wraps it in a [`PacketWrapper`] with `packet_type = HEALTH_BEACON`
//! and `user_id = SYSTEM_USER_ID`, and publishes it to `room.{room_id}.system`.
//!
//! Spill pods consume this stream (p6-8 — wave 3) to decide whether to accept
//! additional joiners for the room.
//!
//! Lifecycle:
//!
//! * Spawn on the first `JoinRoom` for a room, alongside the
//!   [`crate::sfu::speaker::SpeakerTick`]. Only spawn when this pod is the
//!   room's owner; non-owner pods stay silent.
//! * The task re-checks ownership on every tick — replica scale changes
//!   during runtime are rare but possible, and a former owner must stop
//!   emitting once the room migrates.
//! * Tear down by dropping the [`BeaconHandle`], which aborts the task. The
//!   chat-server holds the handle in `health_beacon_ticks`, mirroring the
//!   `speaker_ticks` map.

use std::fmt;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use protobuf::Message as ProtoMessage;
use tokio::task::JoinHandle;
use tracing::warn;
use videocall_types::protos::health_beacon_packet::HealthBeaconPacket;
use videocall_types::protos::packet_wrapper::packet_wrapper::PacketType;
use videocall_types::protos::packet_wrapper::PacketWrapper;
use videocall_types::user_id::to_user_id_bytes;
use videocall_types::SYSTEM_USER_ID;

use crate::sfu::room_state::RoomState;

/// Cadence at which an owner pod emits a health beacon per owned room.
///
/// Matches the value documented in PLAN.md Phase 6: "each pod publishes
/// 5s health beacons on `room.{room}.system`".
pub const BEACON_INTERVAL: Duration = Duration::from_secs(5);

/// Sink for owner-pod health beacons. Mirrors
/// [`crate::sfu::speaker::SpeakerPublisher`] so unit tests can swap a real
/// `async_nats::Client` for a collecting fake. Fire-and-forget by design:
/// implementations spawn their own async work and must not block the
/// beacon loop. Failures are an implementation concern (typically logged
/// and dropped) — losing a single beacon is preferable to stalling the
/// 5s cadence and starving the spill controller's view of the next tick.
pub trait HealthBeaconPublisher: Send + Sync + fmt::Debug {
    /// Publish `payload` on `subject`. Must not block; implementations
    /// spawn the async send themselves.
    fn publish(&self, subject: String, payload: Vec<u8>);
}

/// Production [`HealthBeaconPublisher`] backed by an `async_nats::Client`.
///
/// Cloning the inner `async_nats::Client` is cheap (it's `Arc`-wrapped), so
/// each `publish` call spawns a tokio task with its own handle and returns
/// immediately.
#[derive(Clone)]
pub struct NatsHealthBeaconPublisher {
    client: async_nats::Client,
}

impl NatsHealthBeaconPublisher {
    pub fn new(client: async_nats::Client) -> Self {
        Self { client }
    }
}

impl fmt::Debug for NatsHealthBeaconPublisher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NatsHealthBeaconPublisher").finish()
    }
}

impl HealthBeaconPublisher for NatsHealthBeaconPublisher {
    fn publish(&self, subject: String, payload: Vec<u8>) {
        let client = self.client.clone();
        tokio::spawn(async move {
            if let Err(e) = client.publish(subject.clone(), payload.into()).await {
                warn!(
                    target: "sfu_health_beacon",
                    subject = %subject,
                    error = %e,
                    "HealthBeacon publish failed"
                );
            }
        });
    }
}

/// Returns the current CPU load on a Linux host as a value in `[0, 1]`.
///
/// Sources `/proc/loadavg` (cheap, no external crate) for the 1-minute
/// load-average and normalises by the online CPU count from
/// `/proc/cpuinfo`. The published `cpu_load` is `min(1.0, loadavg / ncpus)`
/// so the spill controller can compare it directly against an `0..1`
/// threshold. Returns `0.0` on any read or parse failure — a missing
/// `/proc` (non-Linux) or transient read error must not stall the beacon.
///
/// Why `min(1.0, ...)`? Loadavg can briefly exceed `ncpus` under contention;
/// clamping keeps the wire contract `[0, 1]` so consumers can use simple
/// arithmetic without defensive bounds checks.
pub fn linux_cpu_load_estimate() -> f32 {
    let load_1m = match std::fs::read_to_string("/proc/loadavg") {
        Ok(s) => s
            .split_whitespace()
            .next()
            .and_then(|w| w.parse::<f32>().ok())
            .unwrap_or(0.0),
        Err(_) => return 0.0,
    };
    let ncpus = num_online_cpus().max(1) as f32;
    (load_1m / ncpus).clamp(0.0, 1.0)
}

/// Count the online CPUs from `/proc/cpuinfo`. Returns 1 if unavailable so
/// downstream division never divides by zero.
///
/// Cached in a process-wide [`OnceLock`]: the CPU count does not change at
/// runtime on the pods we deploy to, and parsing `/proc/cpuinfo` line-by-line
/// on every beacon tick is wasted I/O at the per-room cadence (200 owned
/// rooms × 1 tick / 5 s = 40 reads/s before caching).
fn num_online_cpus() -> usize {
    static CACHED: OnceLock<usize> = OnceLock::new();
    *CACHED.get_or_init(|| match std::fs::read_to_string("/proc/cpuinfo") {
        Ok(s) => {
            let n = s.lines().filter(|l| l.starts_with("processor")).count();
            n.max(1)
        }
        Err(_) => 1,
    })
}

/// Format the NATS subject for the per-room system topic.
///
/// Matches the normalisation applied at every other publish/subscribe site
/// in `chat_server.rs` (search for `replace(' ', "_")`): NATS subjects may
/// not contain whitespace, so room ids that contain spaces must have them
/// rewritten before the subject is constructed. Without this, beacons
/// published from a room id like `"foo bar"` would land on the literal
/// subject `room.foo bar.system`, which the spill controller's
/// `room.*.system` subscription cannot match in the same way the rest of
/// the codebase has been written to expect.
pub(crate) fn system_subject(room_id: &str) -> String {
    format!("room.{}.system", room_id.replace(' ', "_"))
}

/// Build the wire payload for a single beacon tick.
///
/// Pure function: takes the per-tick inputs (room id, member count, cpu
/// load, wall-clock instant) and returns the serialized
/// `PacketWrapper(HealthBeaconPacket)` bytes. Extracted so unit tests can
/// assert the bytes round-trip without standing up the full task loop.
pub fn build_health_beacon_payload(
    room_id: &str,
    participant_count: u32,
    cpu_load: f32,
    reported_at: SystemTime,
) -> Vec<u8> {
    let reported_at_ms = reported_at
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        // Pre-UNIX-epoch clocks are pathological; emit 0 rather than
        // failing the beacon. Consumers can detect this via the rest of
        // the payload (room_id, participant_count) staying valid.
        .unwrap_or(0);
    let beacon = HealthBeaconPacket {
        room_id: room_id.to_string(),
        participant_count,
        cpu_load: cpu_load.clamp(0.0, 1.0),
        reported_at_ms,
        ..Default::default()
    };
    let data = beacon.write_to_bytes().unwrap_or_default();
    let wrapper = PacketWrapper {
        packet_type: PacketType::HEALTH_BEACON.into(),
        user_id: to_user_id_bytes(SYSTEM_USER_ID),
        data,
        ..Default::default()
    };
    wrapper.write_to_bytes().unwrap_or_default()
}

/// Owner-pod predicate used by the beacon loop. Trait so tests can pin
/// ownership independent of process env vars. The production impl wraps
/// [`crate::sfu::affinity::is_owner`].
pub trait OwnerCheck: Send + Sync + 'static {
    /// Return `true` if this pod currently owns `room_id`.
    fn is_owner(&self, room_id: &str) -> bool;
}

/// Production owner check. Calls into [`crate::sfu::affinity::is_owner`] on
/// every tick, so a runtime replica-count change picks up the new
/// ownership decision on the next beacon.
#[derive(Debug, Clone, Copy, Default)]
pub struct EnvOwnerCheck;

impl OwnerCheck for EnvOwnerCheck {
    fn is_owner(&self, room_id: &str) -> bool {
        crate::sfu::affinity::is_owner(room_id)
    }
}

/// Read the live CPU load. Trait so tests can pin the value independent
/// of `/proc/loadavg`. Production impl is [`LinuxCpuLoad`].
pub trait CpuLoadSource: Send + Sync + 'static {
    /// Return the current load estimate in `[0, 1]`.
    fn load(&self) -> f32;
}

/// Production CPU-load source backed by [`linux_cpu_load_estimate`].
#[derive(Debug, Clone, Copy, Default)]
pub struct LinuxCpuLoad;

impl CpuLoadSource for LinuxCpuLoad {
    fn load(&self) -> f32 {
        linux_cpu_load_estimate()
    }
}

/// Cancels the background beacon task when dropped.
///
/// Returned by [`spawn_health_beacon_loop`]; aborts the underlying tokio
/// task on `Drop` so callers cannot leak the loop.
#[derive(Debug)]
pub struct BeaconHandle {
    join: JoinHandle<()>,
}

impl BeaconHandle {
    /// Test-only accessor for the underlying join handle, used to assert
    /// the task is still running.
    #[cfg(test)]
    pub(crate) fn is_finished(&self) -> bool {
        self.join.is_finished()
    }
}

impl Drop for BeaconHandle {
    fn drop(&mut self) {
        self.join.abort();
    }
}

/// Spawn the per-room beacon loop with the production interval.
///
/// Convenience wrapper around [`spawn_health_beacon_loop_with_interval`].
pub fn spawn_health_beacon_loop(
    room_id: String,
    room_state: Arc<RwLock<RoomState>>,
    owner_check: Arc<dyn OwnerCheck>,
    cpu_load: Arc<dyn CpuLoadSource>,
    publisher: Arc<dyn HealthBeaconPublisher>,
) -> BeaconHandle {
    spawn_health_beacon_loop_with_interval(
        room_id,
        room_state,
        owner_check,
        cpu_load,
        publisher,
        BEACON_INTERVAL,
    )
}

/// Spawn the per-room beacon loop with a custom interval (primarily for tests).
///
/// The first beacon fires after `interval` (not immediately) so the tick
/// cadence matches the documented "every 5 seconds" contract from the
/// moment the loop starts. On each tick:
///
/// 1. Re-check [`OwnerCheck::is_owner`]. Non-owners skip the publish
///    silently — keeps the task safe to spawn unconditionally if needed,
///    and tolerates runtime replica scale changes that may transfer
///    ownership away from this pod.
/// 2. Snapshot the room's `member_count` under a short read lock.
///    Capturing the count once per tick (rather than per field) keeps the
///    lock window tiny — there is no per-member iteration in the beacon
///    hot path.
/// 3. Read CPU load via [`CpuLoadSource::load`] and build the payload.
/// 4. Publish to `room.{room_id}.system`.
///
/// Returns a [`BeaconHandle`] that aborts the task on drop. The chat-server
/// stores this handle alongside its `speaker_ticks` and drops both on room
/// drain.
pub fn spawn_health_beacon_loop_with_interval(
    room_id: String,
    room_state: Arc<RwLock<RoomState>>,
    owner_check: Arc<dyn OwnerCheck>,
    cpu_load: Arc<dyn CpuLoadSource>,
    publisher: Arc<dyn HealthBeaconPublisher>,
    interval: Duration,
) -> BeaconHandle {
    let join = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        // `Delay` (rather than the default `Burst`) prevents missed ticks
        // under load from bunching the beacon stream — at most one per
        // interval, in line with the documented 5s cadence.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Consume the immediate first tick so the first beacon publishes
        // at `t = interval`, not `t = 0`. Matches the SpeakerTick pattern
        // and gives joiners a moment to register before being counted.
        ticker.tick().await;
        // Whitespace-safe subject — matches every other `room.{room}.system`
        // publish/subscribe site in `chat_server.rs` (vc-kol follow-up).
        let subject = system_subject(&room_id);
        loop {
            ticker.tick().await;
            if !owner_check.is_owner(&room_id) {
                // Pod is no longer the owner (e.g., replica scale-out
                // moved this room elsewhere). Stay alive — ownership may
                // come back — but emit nothing.
                continue;
            }
            // Tiny critical section: only the count is read, no per-member
            // iteration. Survives a panicked writer (poison) by treating
            // the poisoned state as readable — we are not mutating.
            let participant_count = match room_state.read() {
                Ok(g) => g.member_count() as u32,
                Err(poisoned) => poisoned.into_inner().member_count() as u32,
            };
            let cpu = cpu_load.load();
            let payload =
                build_health_beacon_payload(&room_id, participant_count, cpu, SystemTime::now());
            publisher.publish(subject.clone(), payload);
        }
    });
    BeaconHandle { join }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    type PublishedLog = Arc<Mutex<Vec<(String, Vec<u8>)>>>;

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

    impl HealthBeaconPublisher for FakePublisher {
        fn publish(&self, subject: String, payload: Vec<u8>) {
            self.published.lock().unwrap().push((subject, payload));
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct AlwaysOwner;
    impl OwnerCheck for AlwaysOwner {
        fn is_owner(&self, _room_id: &str) -> bool {
            true
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct NeverOwner;
    impl OwnerCheck for NeverOwner {
        fn is_owner(&self, _room_id: &str) -> bool {
            false
        }
    }

    /// Owner toggles between owner and non-owner across calls — used to
    /// verify the loop respects ownership on every tick.
    #[derive(Debug, Default)]
    struct ToggleOwner {
        calls: Mutex<Vec<bool>>,
        outcomes: Vec<bool>,
    }
    impl ToggleOwner {
        fn new(outcomes: Vec<bool>) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                outcomes,
            }
        }
    }
    impl OwnerCheck for ToggleOwner {
        fn is_owner(&self, _room_id: &str) -> bool {
            let mut g = self.calls.lock().unwrap();
            let idx = g.len();
            let out = self.outcomes.get(idx).copied().unwrap_or(false);
            g.push(out);
            out
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct FixedCpu(f32);
    impl CpuLoadSource for FixedCpu {
        fn load(&self) -> f32 {
            self.0
        }
    }

    /// Decode the wire payload back into its component parts so assertions
    /// can read out the `HealthBeaconPacket` fields.
    fn decode(payload: &[u8]) -> HealthBeaconPacket {
        let wrapper = PacketWrapper::parse_from_bytes(payload).expect("decode wrapper");
        assert_eq!(
            wrapper.packet_type,
            PacketType::HEALTH_BEACON.into(),
            "wrapper type must be HEALTH_BEACON"
        );
        HealthBeaconPacket::parse_from_bytes(&wrapper.data).expect("decode beacon")
    }

    /// `build_health_beacon_payload` round-trips room id, count, and cpu.
    #[test]
    fn payload_round_trips_fields() {
        let payload = build_health_beacon_payload(
            "demo-room",
            7,
            0.42,
            UNIX_EPOCH + Duration::from_millis(1_700_000_000_000),
        );
        let beacon = decode(&payload);
        assert_eq!(beacon.room_id, "demo-room");
        assert_eq!(beacon.participant_count, 7);
        assert!((beacon.cpu_load - 0.42).abs() < 1e-6);
        assert_eq!(beacon.reported_at_ms, 1_700_000_000_000);
    }

    /// Out-of-range CPU load is clamped to `[0, 1]` on the wire.
    #[test]
    fn payload_clamps_cpu_load() {
        let high = build_health_beacon_payload("r", 0, 2.5, UNIX_EPOCH);
        assert!((decode(&high).cpu_load - 1.0).abs() < 1e-6);

        let low = build_health_beacon_payload("r", 0, -0.7, UNIX_EPOCH);
        assert!(decode(&low).cpu_load.abs() < 1e-6);
    }

    /// vc-kol acceptance test: a room with 5 members produces a beacon
    /// whose `participant_count` decodes to 5.
    #[tokio::test(start_paused = true)]
    async fn beacon_reports_participant_count_for_five_members() {
        let room_state = Arc::new(RwLock::new(RoomState::new("room-5".into())));
        {
            let mut g = room_state.write().unwrap();
            for sid in 1..=5 {
                g.insert_member(sid, 0);
            }
            assert_eq!(g.member_count(), 5);
        }
        let publisher = Arc::new(FakePublisher::new());
        let handle = spawn_health_beacon_loop_with_interval(
            "room-5".into(),
            room_state.clone(),
            Arc::new(AlwaysOwner),
            Arc::new(FixedCpu(0.25)),
            publisher.clone(),
            Duration::from_millis(100),
        );
        // Let the spawned task initialise its interval.
        tokio::task::yield_now().await;
        // Advance well past the first interval boundary.
        tokio::time::advance(Duration::from_millis(120)).await;
        tokio::task::yield_now().await;

        let published = publisher.drain();
        assert!(
            !published.is_empty(),
            "at least one beacon should have been published after one interval"
        );
        let (subject, payload) = &published[0];
        assert_eq!(subject, "room.room-5.system");
        let beacon = decode(payload);
        assert_eq!(beacon.participant_count, 5);
        assert_eq!(beacon.room_id, "room-5");
        assert!((beacon.cpu_load - 0.25).abs() < 1e-6);
        assert!(beacon.reported_at_ms > 0, "reported_at_ms should be set");

        drop(handle);
    }

    /// Cadence test: ~5s cadence in production translates to one beacon
    /// per `interval`. We use a short interval with paused virtual time
    /// to keep the test fast.
    #[tokio::test(start_paused = true)]
    async fn beacons_fire_at_interval_cadence() {
        let room_state = Arc::new(RwLock::new(RoomState::new("room-c".into())));
        {
            let mut g = room_state.write().unwrap();
            g.insert_member(1, 0);
        }
        let publisher = Arc::new(FakePublisher::new());
        let interval = Duration::from_millis(50);
        let handle = spawn_health_beacon_loop_with_interval(
            "room-c".into(),
            room_state,
            Arc::new(AlwaysOwner),
            Arc::new(FixedCpu(0.1)),
            publisher.clone(),
            interval,
        );
        tokio::task::yield_now().await;

        // Advance four intervals — expect roughly four beacons.
        for _ in 0..4 {
            tokio::time::advance(interval).await;
            // Yield twice: the ticker fires, and then the loop body runs.
            tokio::task::yield_now().await;
            tokio::task::yield_now().await;
        }
        let count = publisher.drain().len();
        assert!(
            (3..=5).contains(&count),
            "expected ~4 beacons after 4 intervals, got {count}"
        );

        drop(handle);
    }

    /// Non-owner pods stay quiet — the task survives but emits nothing.
    #[tokio::test(start_paused = true)]
    async fn non_owner_pod_emits_no_beacons() {
        let room_state = Arc::new(RwLock::new(RoomState::new("room-q".into())));
        {
            let mut g = room_state.write().unwrap();
            g.insert_member(1, 0);
        }
        let publisher = Arc::new(FakePublisher::new());
        let handle = spawn_health_beacon_loop_with_interval(
            "room-q".into(),
            room_state,
            Arc::new(NeverOwner),
            Arc::new(FixedCpu(0.5)),
            publisher.clone(),
            Duration::from_millis(50),
        );
        tokio::task::yield_now().await;
        for _ in 0..3 {
            tokio::time::advance(Duration::from_millis(50)).await;
            tokio::task::yield_now().await;
            tokio::task::yield_now().await;
        }
        assert!(
            publisher.drain().is_empty(),
            "non-owner pod must not publish beacons"
        );
        // Task must still be alive — ownership might come back later.
        assert!(
            !handle.is_finished(),
            "loop must persist across non-owner ticks"
        );

        drop(handle);
    }

    /// Runtime ownership transition: a pod that starts as owner and later
    /// loses ownership emits while owner, then stops.
    #[tokio::test(start_paused = true)]
    async fn beacons_pause_when_ownership_lost() {
        let room_state = Arc::new(RwLock::new(RoomState::new("room-t".into())));
        {
            let mut g = room_state.write().unwrap();
            g.insert_member(1, 0);
            g.insert_member(2, 0);
        }
        // Owner for the first two ticks, then not.
        let owner = Arc::new(ToggleOwner::new(vec![true, true, false, false]));
        let publisher = Arc::new(FakePublisher::new());
        let handle = spawn_health_beacon_loop_with_interval(
            "room-t".into(),
            room_state,
            owner.clone(),
            Arc::new(FixedCpu(0.0)),
            publisher.clone(),
            Duration::from_millis(50),
        );
        tokio::task::yield_now().await;
        // First two ticks publish.
        for _ in 0..2 {
            tokio::time::advance(Duration::from_millis(50)).await;
            tokio::task::yield_now().await;
            tokio::task::yield_now().await;
        }
        let after_owner_ticks = publisher.drain().len();
        assert!(
            after_owner_ticks >= 1,
            "must publish while owning, got {after_owner_ticks}"
        );

        // Next two ticks do not.
        for _ in 0..2 {
            tokio::time::advance(Duration::from_millis(50)).await;
            tokio::task::yield_now().await;
            tokio::task::yield_now().await;
        }
        assert!(
            publisher.drain().is_empty(),
            "no publishes after ownership lost"
        );

        drop(handle);
    }

    /// Dropping the [`BeaconHandle`] aborts the task: subsequent virtual
    /// time advances produce no new publishes.
    #[tokio::test(start_paused = true)]
    async fn dropping_handle_aborts_loop() {
        let room_state = Arc::new(RwLock::new(RoomState::new("room-d".into())));
        {
            let mut g = room_state.write().unwrap();
            g.insert_member(1, 0);
        }
        let publisher = Arc::new(FakePublisher::new());
        let handle = spawn_health_beacon_loop_with_interval(
            "room-d".into(),
            room_state,
            Arc::new(AlwaysOwner),
            Arc::new(FixedCpu(0.0)),
            publisher.clone(),
            Duration::from_millis(50),
        );
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(50)).await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        let pre_drop = publisher.drain().len();
        assert!(pre_drop >= 1, "expected ≥1 publish before drop");

        drop(handle);
        // Give the runtime a chance to actually abort the task.
        tokio::task::yield_now().await;

        for _ in 0..3 {
            tokio::time::advance(Duration::from_millis(50)).await;
            tokio::task::yield_now().await;
            tokio::task::yield_now().await;
        }
        assert!(
            publisher.drain().is_empty(),
            "no publishes after BeaconHandle drop"
        );
    }

    /// `linux_cpu_load_estimate` returns a value in `[0, 1]` on any platform
    /// (returns 0 on missing /proc/loadavg).
    #[test]
    fn linux_cpu_load_estimate_in_range() {
        let v = linux_cpu_load_estimate();
        assert!((0.0..=1.0).contains(&v), "got {v}");
    }

    /// `system_subject` rewrites whitespace in the room id so the published
    /// subject is wire-legal and matches the rest of the codebase's
    /// `room.{room}.system` convention.
    ///
    /// vc-kol follow-up: every other site that builds the system subject in
    /// `chat_server.rs` calls `.replace(' ', "_")` before formatting; the
    /// beacon publisher must do the same so the spill controller (p6-8) sees
    /// beacons for rooms whose ids contain spaces.
    #[test]
    fn system_subject_replaces_spaces_with_underscores() {
        assert_eq!(system_subject("foo bar"), "room.foo_bar.system");
        // Multiple spaces are all rewritten.
        assert_eq!(system_subject("a b c"), "room.a_b_c.system");
        // Rooms without spaces are unchanged.
        assert_eq!(system_subject("plain-room"), "room.plain-room.system");
    }

    /// End-to-end: a room id with a space publishes its beacon on the
    /// normalised subject (matches `chat_server.rs`'s convention).
    ///
    /// Without the `.replace(' ', "_")` fix, the beacon loop would publish
    /// to `room.foo bar.system` (subject contains a space), which is both
    /// invalid as a NATS subject and would never match the spill
    /// controller's subscription pattern.
    #[tokio::test(start_paused = true)]
    async fn beacon_publishes_on_normalised_subject() {
        let room_id = "foo bar".to_string();
        let room_state = Arc::new(RwLock::new(RoomState::new(room_id.clone())));
        {
            let mut g = room_state.write().unwrap();
            g.insert_member(1, 0);
        }
        let publisher = Arc::new(FakePublisher::new());
        let handle = spawn_health_beacon_loop_with_interval(
            room_id,
            room_state,
            Arc::new(AlwaysOwner),
            Arc::new(FixedCpu(0.0)),
            publisher.clone(),
            Duration::from_millis(50),
        );
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(60)).await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        let published = publisher.drain();
        assert!(
            !published.is_empty(),
            "expected at least one beacon after first interval"
        );
        let (subject, _payload) = &published[0];
        assert_eq!(
            subject, "room.foo_bar.system",
            "subject must normalise the space in 'foo bar' to 'foo_bar'"
        );

        drop(handle);
    }
}
