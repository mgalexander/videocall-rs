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
//! Every owner pod (per [`crate::sfu::affinity::is_owner`]) runs a single
//! background task — the [`BeaconHub`] — that fans out beacons for every
//! owned room. Every [`BEACON_INTERVAL`] the hub iterates over its registry,
//! assembles a [`HealthBeaconPacket`] from each live [`RoomState`] plus a
//! single pod-wide CPU-load estimate, wraps it in a [`PacketWrapper`] with
//! `packet_type = HEALTH_BEACON` and `user_id = SYSTEM_USER_ID`, and publishes
//! it to `room.{room_id}.system`.
//!
//! Spill pods consume this stream (p6-8 — wave 3) to decide whether to accept
//! additional joiners for the room.
//!
//! Lifecycle:
//!
//! * The hub is spawned once at [`crate::actors::chat_server::ChatServer::new`]
//!   and runs for the lifetime of the process.
//! * On the first `JoinRoom` for a room (alongside the
//!   [`crate::sfu::speaker::SpeakerTick`]) the chat-server calls
//!   [`BeaconHub::register`] when this pod is the room's owner.
//! * The hub re-checks ownership on every tick — replica scale changes
//!   during runtime are rare but possible, and a former owner must stop
//!   emitting once the room migrates.
//! * On room drain the chat-server calls [`BeaconHub::unregister`].
//! * Dropping the [`BeaconHub`] aborts the underlying task.
//!
//! Collapsing N per-room tasks into one hub (vc-c6l) eliminates per-room
//! timer state and per-room `Arc<dyn ...>` clones; at 200 owned rooms this
//! is the difference between 200 tokio tasks and one.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, OnceLock, RwLock};
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
/// on every beacon tick is wasted I/O.
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
/// Pure function: takes the per-tick inputs (member count, cpu load,
/// wall-clock instant) and returns the serialized
/// `PacketWrapper(HealthBeaconPacket)` bytes. The room id is encoded in
/// the NATS subject (`room.{room_id}.system`) — embedding it in the
/// payload would double wire size for no benefit (vc-4le). Extracted so
/// unit tests can assert the bytes round-trip without standing up the
/// full task loop.
pub fn build_health_beacon_payload(
    participant_count: u32,
    cpu_load: f32,
    reported_at: SystemTime,
) -> Vec<u8> {
    let reported_at_ms = reported_at
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        // Pre-UNIX-epoch clocks are pathological; emit 0 rather than
        // failing the beacon. Consumers can detect this via the rest of
        // the payload (participant_count) staying valid.
        .unwrap_or(0);
    let beacon = HealthBeaconPacket {
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

/// Shared registry of owned rooms whose beacons the hub task should emit
/// on each tick. Keyed by room id, values are the same
/// `Arc<RwLock<RoomState>>` the chat-server stores in its `room_states`
/// map — sharing the Arc means the hub sees live `member_count` updates
/// without any extra synchronisation.
type Registry = Arc<Mutex<HashMap<String, Arc<RwLock<RoomState>>>>>;

/// Single owner-pod beacon task that emits beacons for all registered
/// rooms (vc-c6l).
///
/// Replaces the per-room [`tokio::task`] model: one timer, one CPU-load
/// read per tick, one small mutex on the registry. Drop aborts the task.
#[derive(Debug)]
pub struct BeaconHub {
    rooms: Registry,
    join: JoinHandle<()>,
}

impl BeaconHub {
    /// Add `room_id` to the registry. Called by the chat-server on the
    /// first `JoinRoom` for an owned room, alongside the speaker tick.
    /// Idempotent: re-registering the same room replaces the stored
    /// `Arc<RwLock<RoomState>>` (callers always pass the same pointer, but
    /// this keeps the invariant robust against future refactors).
    pub fn register(&self, room_id: String, state: Arc<RwLock<RoomState>>) {
        let mut g = match self.rooms.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        g.insert(room_id, state);
    }

    /// Remove `room_id` from the registry. Called by the chat-server on
    /// room drain. Safe no-op if the room was never registered (non-owner
    /// pods never register).
    pub fn unregister(&self, room_id: &str) {
        let mut g = match self.rooms.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        g.remove(room_id);
    }

    /// Test-only accessor: returns whether the hub's background task has
    /// already terminated. Production code does not need this; only the
    /// abort-on-drop test relies on it.
    #[cfg(test)]
    pub(crate) fn is_finished(&self) -> bool {
        self.join.is_finished()
    }
}

impl Drop for BeaconHub {
    fn drop(&mut self) {
        self.join.abort();
    }
}

/// Spawn the single owner-pod beacon hub with the production interval.
///
/// Convenience wrapper around [`spawn_beacon_hub_with_interval`].
pub fn spawn_beacon_hub(
    owner_check: Arc<dyn OwnerCheck>,
    cpu_load: Arc<dyn CpuLoadSource>,
    publisher: Arc<dyn HealthBeaconPublisher>,
) -> BeaconHub {
    spawn_beacon_hub_with_interval(owner_check, cpu_load, publisher, BEACON_INTERVAL)
}

/// Spawn the beacon hub with a custom interval (primarily for tests).
///
/// The first beacon fires after `interval` (not immediately) so the tick
/// cadence matches the documented "every 5 seconds" contract from the
/// moment the hub starts. On each tick:
///
/// 1. Take a brief lock on the registry and clone out
///    `(room_id, Arc<RwLock<RoomState>>)` pairs. The lock is released
///    before any publish work happens — registry mutations from the
///    chat-server actor must not be blocked by I/O.
/// 2. Read CPU load via [`CpuLoadSource::load`] **once** for the tick.
///    All rooms in this tick share the same pod-wide value.
/// 3. For each registered room: re-check [`OwnerCheck::is_owner`] and
///    skip the publish silently if false — tolerates runtime replica
///    scale changes that may transfer ownership away from this pod.
/// 4. Snapshot the room's `member_count` under a short read lock and
///    publish to `room.{room_id}.system`.
///
/// Returns a [`BeaconHub`] that aborts the task on drop. The chat-server
/// holds the hub for the lifetime of the process.
pub fn spawn_beacon_hub_with_interval(
    owner_check: Arc<dyn OwnerCheck>,
    cpu_load: Arc<dyn CpuLoadSource>,
    publisher: Arc<dyn HealthBeaconPublisher>,
    interval: Duration,
) -> BeaconHub {
    let rooms: Registry = Arc::new(Mutex::new(HashMap::new()));
    let rooms_for_task = rooms.clone();
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
        loop {
            ticker.tick().await;
            // Snapshot the registry into a small Vec so the lock window
            // is bounded by the registry size, not by the publish work
            // that follows.
            let snapshot: Vec<(String, Arc<RwLock<RoomState>>)> = {
                let g = match rooms_for_task.lock() {
                    Ok(g) => g,
                    Err(poisoned) => poisoned.into_inner(),
                };
                g.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
            };
            if snapshot.is_empty() {
                continue;
            }
            // One CPU-load read per tick, pod-wide. Owner pods that own
            // 200 rooms used to read this 200×/tick; now it's 1×/tick.
            let cpu = cpu_load.load();
            let now = SystemTime::now();
            for (room_id, state) in snapshot {
                if !owner_check.is_owner(&room_id) {
                    // Pod is no longer the owner (e.g., replica scale-out
                    // moved this room elsewhere). Stay registered —
                    // ownership may come back — but emit nothing.
                    continue;
                }
                // Tiny critical section: only the count is read, no
                // per-member iteration. Survives a panicked writer
                // (poison) by treating the poisoned state as readable —
                // we are not mutating.
                let participant_count = match state.read() {
                    Ok(g) => g.member_count() as u32,
                    Err(poisoned) => poisoned.into_inner().member_count() as u32,
                };
                let subject = system_subject(&room_id);
                let payload = build_health_beacon_payload(participant_count, cpu, now);
                publisher.publish(subject, payload);
            }
        }
    });
    BeaconHub { rooms, join }
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
    /// verify the hub respects ownership on every tick.
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

    /// Build a populated `RoomState` for `room_id` with `n` members.
    fn make_room(room_id: &str, n: usize) -> Arc<RwLock<RoomState>> {
        let state = Arc::new(RwLock::new(RoomState::new(room_id.into())));
        {
            let mut g = state.write().unwrap();
            for sid in 1..=n {
                g.insert_member(sid as u64, 0);
            }
            assert_eq!(g.member_count(), n);
        }
        state
    }

    /// Advance the paused runtime by `step` and yield enough times that
    /// the hub task wakes, takes the registry snapshot, and runs its
    /// publish loop.
    async fn advance_one_tick(step: Duration) {
        tokio::time::advance(step).await;
        // The ticker fires, then the loop body runs. A handful of yields
        // is more than enough — the body is short and synchronous.
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
    }

    /// `build_health_beacon_payload` round-trips count, cpu, and timestamp.
    #[test]
    fn payload_round_trips_fields() {
        let payload = build_health_beacon_payload(
            7,
            0.42,
            UNIX_EPOCH + Duration::from_millis(1_700_000_000_000),
        );
        let beacon = decode(&payload);
        assert_eq!(beacon.participant_count, 7);
        assert!((beacon.cpu_load - 0.42).abs() < 1e-6);
        assert_eq!(beacon.reported_at_ms, 1_700_000_000_000);
    }

    /// Out-of-range CPU load is clamped to `[0, 1]` on the wire.
    #[test]
    fn payload_clamps_cpu_load() {
        let high = build_health_beacon_payload(0, 2.5, UNIX_EPOCH);
        assert!((decode(&high).cpu_load - 1.0).abs() < 1e-6);

        let low = build_health_beacon_payload(0, -0.7, UNIX_EPOCH);
        assert!(decode(&low).cpu_load.abs() < 1e-6);
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
    #[test]
    fn system_subject_replaces_spaces_with_underscores() {
        assert_eq!(system_subject("foo bar"), "room.foo_bar.system");
        assert_eq!(system_subject("a b c"), "room.a_b_c.system");
        assert_eq!(system_subject("plain-room"), "room.plain-room.system");
    }

    /// Hub publishes a beacon for a single registered room on each tick.
    #[tokio::test(start_paused = true)]
    async fn hub_emits_for_single_registered_room() {
        let publisher = Arc::new(FakePublisher::new());
        let hub = spawn_beacon_hub_with_interval(
            Arc::new(AlwaysOwner),
            Arc::new(FixedCpu(0.25)),
            publisher.clone(),
            Duration::from_millis(50),
        );
        hub.register("room-5".into(), make_room("room-5", 5));
        tokio::task::yield_now().await;
        advance_one_tick(Duration::from_millis(60)).await;

        let published = publisher.drain();
        assert!(
            !published.is_empty(),
            "at least one beacon expected after the first interval"
        );
        let (subject, payload) = &published[0];
        assert_eq!(subject, "room.room-5.system");
        let beacon = decode(payload);
        assert_eq!(beacon.participant_count, 5);
        assert!((beacon.cpu_load - 0.25).abs() < 1e-6);
        assert!(beacon.reported_at_ms > 0);

        drop(hub);
    }

    /// Hub fans out beacons for MULTIPLE registered rooms in a single
    /// tick — the perf-relevant case the refactor enables.
    #[tokio::test(start_paused = true)]
    async fn hub_emits_for_multiple_registered_rooms() {
        let publisher = Arc::new(FakePublisher::new());
        let hub = spawn_beacon_hub_with_interval(
            Arc::new(AlwaysOwner),
            Arc::new(FixedCpu(0.1)),
            publisher.clone(),
            Duration::from_millis(50),
        );
        hub.register("room-a".into(), make_room("room-a", 2));
        hub.register("room-b".into(), make_room("room-b", 3));
        hub.register("room-c".into(), make_room("room-c", 4));
        tokio::task::yield_now().await;
        advance_one_tick(Duration::from_millis(60)).await;

        let published = publisher.drain();
        // Each tick must produce exactly one beacon per registered room.
        let mut by_subject: HashMap<String, HealthBeaconPacket> = HashMap::new();
        for (subject, payload) in &published {
            by_subject.insert(subject.clone(), decode(payload));
        }
        assert_eq!(
            by_subject.len(),
            3,
            "expected one beacon per room, got {} ({published:?})",
            by_subject.len()
        );
        assert_eq!(
            by_subject
                .get("room.room-a.system")
                .unwrap()
                .participant_count,
            2
        );
        assert_eq!(
            by_subject
                .get("room.room-b.system")
                .unwrap()
                .participant_count,
            3
        );
        assert_eq!(
            by_subject
                .get("room.room-c.system")
                .unwrap()
                .participant_count,
            4
        );

        drop(hub);
    }

    /// Hub skips rooms on ticks where `is_owner` returns false (preserves
    /// the prior `ToggleOwner` semantics for runtime ownership transitions).
    #[tokio::test(start_paused = true)]
    async fn hub_skips_rooms_when_not_owner_on_tick() {
        let publisher = Arc::new(FakePublisher::new());
        // Two outcomes per tick (one per room). Tick 1: both owners.
        // Tick 2: neither.
        let owner = Arc::new(ToggleOwner::new(vec![true, true, false, false]));
        let hub = spawn_beacon_hub_with_interval(
            owner.clone(),
            Arc::new(FixedCpu(0.0)),
            publisher.clone(),
            Duration::from_millis(50),
        );
        hub.register("room-t1".into(), make_room("room-t1", 1));
        hub.register("room-t2".into(), make_room("room-t2", 2));
        tokio::task::yield_now().await;

        advance_one_tick(Duration::from_millis(50)).await;
        let owned = publisher.drain();
        assert!(
            !owned.is_empty(),
            "expected publishes on first tick while owning, got {}",
            owned.len()
        );

        advance_one_tick(Duration::from_millis(50)).await;
        assert!(
            publisher.drain().is_empty(),
            "no publishes once ownership lost"
        );
        // Hub task must still be alive — ownership may return.
        assert!(
            !hub.is_finished(),
            "hub task must persist across non-owner ticks"
        );

        drop(hub);
    }

    /// A non-owner pod (every owner-check returns false) emits nothing,
    /// but the hub task itself stays alive.
    #[tokio::test(start_paused = true)]
    async fn hub_emits_nothing_for_non_owner_pod() {
        let publisher = Arc::new(FakePublisher::new());
        let hub = spawn_beacon_hub_with_interval(
            Arc::new(NeverOwner),
            Arc::new(FixedCpu(0.5)),
            publisher.clone(),
            Duration::from_millis(50),
        );
        hub.register("room-q".into(), make_room("room-q", 1));
        tokio::task::yield_now().await;
        for _ in 0..3 {
            advance_one_tick(Duration::from_millis(50)).await;
        }
        assert!(
            publisher.drain().is_empty(),
            "non-owner pod must not publish beacons"
        );
        assert!(
            !hub.is_finished(),
            "hub task must persist even when nothing is published"
        );

        drop(hub);
    }

    /// `unregister` removes a room from the rotation — no further beacons
    /// fire for it.
    #[tokio::test(start_paused = true)]
    async fn hub_honors_unregister() {
        let publisher = Arc::new(FakePublisher::new());
        let hub = spawn_beacon_hub_with_interval(
            Arc::new(AlwaysOwner),
            Arc::new(FixedCpu(0.0)),
            publisher.clone(),
            Duration::from_millis(50),
        );
        hub.register("room-u".into(), make_room("room-u", 1));
        tokio::task::yield_now().await;

        advance_one_tick(Duration::from_millis(50)).await;
        let pre = publisher.drain();
        assert!(
            pre.iter().any(|(s, _)| s == "room.room-u.system"),
            "expected a beacon for room-u before unregister"
        );

        hub.unregister("room-u");
        for _ in 0..3 {
            advance_one_tick(Duration::from_millis(50)).await;
        }
        let post = publisher.drain();
        assert!(
            post.iter().all(|(s, _)| s != "room.room-u.system"),
            "no beacons for room-u after unregister, got {post:?}"
        );

        drop(hub);
    }

    /// Dropping the [`BeaconHub`] aborts the task: subsequent virtual
    /// time advances produce no new publishes.
    #[tokio::test(start_paused = true)]
    async fn dropping_hub_aborts_task() {
        let publisher = Arc::new(FakePublisher::new());
        let hub = spawn_beacon_hub_with_interval(
            Arc::new(AlwaysOwner),
            Arc::new(FixedCpu(0.0)),
            publisher.clone(),
            Duration::from_millis(50),
        );
        hub.register("room-d".into(), make_room("room-d", 1));
        tokio::task::yield_now().await;
        advance_one_tick(Duration::from_millis(50)).await;
        let pre_drop = publisher.drain().len();
        assert!(pre_drop >= 1, "expected at least 1 publish before drop");

        drop(hub);
        // Give the runtime a chance to actually abort the task.
        tokio::task::yield_now().await;

        for _ in 0..3 {
            advance_one_tick(Duration::from_millis(50)).await;
        }
        assert!(
            publisher.drain().is_empty(),
            "no publishes after BeaconHub drop"
        );
    }

    /// End-to-end: a room id with a space publishes its beacon on the
    /// normalised subject (matches `chat_server.rs`'s convention).
    #[tokio::test(start_paused = true)]
    async fn hub_publishes_on_normalised_subject() {
        let publisher = Arc::new(FakePublisher::new());
        let hub = spawn_beacon_hub_with_interval(
            Arc::new(AlwaysOwner),
            Arc::new(FixedCpu(0.0)),
            publisher.clone(),
            Duration::from_millis(50),
        );
        hub.register("foo bar".into(), make_room("foo bar", 1));
        tokio::task::yield_now().await;
        advance_one_tick(Duration::from_millis(60)).await;

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

        drop(hub);
    }
}
