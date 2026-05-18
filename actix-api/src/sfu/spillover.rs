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

//! Spillover acceptance state (P6 wave-3 bead vc-d55 / p6-8).
//!
//! Consumes the owner-pod health beacons published by
//! [`crate::sfu::health_beacon`] on `room.*.system` and exposes a
//! per-room snapshot — most recent `participant_count`, `cpu_load`, and
//! freshness timestamp — that the JoinRoom path will use to decide
//! whether a non-owner pod should accept a spillover joiner instead of
//! redirecting them to the owner.
//!
//! ## Design
//!
//! * **Storage:** a single process-wide [`dashmap::DashMap`] keyed by
//!   the *normalized* `room_id` (spaces rewritten to underscores, as
//!   produced by [`crate::sfu::health_beacon::system_subject`]).
//!   Sharded internal locks keep the JoinRoom hot path off the
//!   beacon-ingest hot path; one beacon write per room every 5 seconds
//!   is trivially serviceable.
//! * **Ingest:** a single background task subscribes to `room.*.system`
//!   and updates the map on every received `HEALTH_BEACON` packet. The
//!   task runs independently of the per-room dispatcher tasks so packet
//!   forwarding can never be blocked by spill-state updates.
//!   The room id is recovered from the NATS subject (`room.<id>.system`)
//!   rather than the protobuf payload — vc-4le dropped `room_id` from
//!   the `HealthBeaconPacket` wire shape to keep the beacon small.
//! * **Read API:** [`SpilloverStore::is_spilled_over`] is the public
//!   predicate the JoinRoom handler will call. It enforces the
//!   acceptance thresholds (>180 participants OR >80% CPU) and the 15s
//!   freshness window required by the bead. Callers may pass the raw
//!   room id (possibly containing spaces); the store normalizes the
//!   lookup key to match the subject-derived storage key.
//!
//! ## Why a separate module (not an extension of `RoomState`)
//!
//! `RoomState` is the authoritative *local* per-room model owned by the
//! room's pod. The spillover snapshot is the *remote* owner-pod's view,
//! consumed by non-owner pods. Mixing the two would (a) require every
//! pod to allocate a `RoomState` for every room it has merely seen a
//! beacon for — unbounded — and (b) confuse the meaning of
//! `member_count`. Keeping them in distinct structures preserves the
//! invariant that `RoomState` covers only rooms with local members.
//!
//! ## Bounding memory
//!
//! Rooms come and go. To avoid unbounded growth across long process
//! lifetimes, [`SpilloverStore::is_spilled_over`] only returns `true`
//! when `last_seen.elapsed() < 15s`; stale entries are functionally
//! inert. A separate prune call ([`SpilloverStore::prune_stale`]) drops
//! entries older than a configurable horizon so the map itself stays
//! small; callers may invoke this periodically or rely on the ingest
//! task to do so. The ingest task currently prunes opportunistically on
//! every beacon write.
//!
//! ## Integration scope (p6-8 vs p6-5)
//!
//! This bead delivers the store, the ingest task, and the public read
//! API. It deliberately does **not** wire `is_spilled_over` into the
//! `JoinRoom` handler — the sibling bead p6-5 owns the join/redirect
//! decision logic and will call into this module from there.

use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use futures::StreamExt;
use protobuf::Message as ProtoMessage;
use tokio::task::JoinHandle;
use tracing::{debug, error, warn};
use videocall_types::protos::health_beacon_packet::HealthBeaconPacket;
use videocall_types::protos::packet_wrapper::packet_wrapper::PacketType;
use videocall_types::protos::packet_wrapper::PacketWrapper;

/// Participant-count threshold above which the owner pod is considered
/// "full enough" to spill new joiners onto a non-owner pod.
///
/// Matches the documented p6-8 acceptance criterion: `count > 180`.
pub const SPILLOVER_PARTICIPANT_THRESHOLD: u32 = 180;

/// CPU-load threshold (in the wire `[0, 1]` range) above which the
/// owner pod is considered "hot enough" to spill. Matches the documented
/// p6-8 acceptance criterion: `cpu_load > 0.80`.
pub const SPILLOVER_CPU_THRESHOLD: f32 = 0.80;

/// Maximum age of an owner-pod beacon for it to be considered actionable.
///
/// At the documented [`crate::sfu::health_beacon::BEACON_INTERVAL`] of
/// 5 seconds, a 15s freshness window tolerates two missed beacons before
/// we fall back to "not spilled over" (safer default: route to the owner
/// and let it redirect us if it really is full). Returning `false` on
/// stale data is the conservative choice — false negatives just send the
/// joiner to the owner, while false positives could send them to a
/// non-owner pod that itself has no real picture of the room.
pub const SPILLOVER_FRESHNESS_WINDOW: Duration = Duration::from_secs(15);

/// NATS subject pattern the ingest task subscribes to.
///
/// Matches the publish subject built by
/// [`crate::sfu::health_beacon::system_subject`]: `room.{room}.system`.
/// Using the per-room wildcard means a single subscription covers every
/// room without any per-room handshake.
pub const SPILLOVER_SUBJECT_PATTERN: &str = "room.*.system";

/// Per-room snapshot of the owner pod's most recent health beacon.
///
/// `last_seen` is a monotonic [`Instant`] (not the beacon's wall-clock
/// `reported_at_ms`) so freshness checks survive NTP corrections on
/// either side. The owner's clock is only used to populate the wire
/// payload's `reported_at_ms`, not the consumer's freshness window.
#[derive(Debug, Clone, Copy)]
pub struct RoomSpilloverState {
    /// Participant count reported by the owner pod in the most recent
    /// beacon for this room.
    pub owner_count: u32,
    /// CPU load reported by the owner pod, in the wire `[0, 1]` range.
    pub owner_cpu: f32,
    /// Monotonic instant at which this snapshot was ingested. Used to
    /// gate `is_spilled_over` on freshness rather than the owner's
    /// wall-clock timestamp.
    pub last_seen: Instant,
}

impl RoomSpilloverState {
    /// `true` when the owner pod is fresh **and** over either the
    /// participant or CPU threshold.
    ///
    /// Stale beacons (older than [`SPILLOVER_FRESHNESS_WINDOW`]) return
    /// `false`: without a recent picture, we conservatively assume the
    /// owner can still take the joiner. This is the documented p6-8
    /// behaviour.
    pub fn is_spilled_over(&self) -> bool {
        self.last_seen.elapsed() < SPILLOVER_FRESHNESS_WINDOW
            && (self.owner_count > SPILLOVER_PARTICIPANT_THRESHOLD
                || self.owner_cpu > SPILLOVER_CPU_THRESHOLD)
    }
}

/// Process-wide store of per-room owner-pod health snapshots.
///
/// `Clone` is cheap (only the inner `Arc<DashMap>` is bumped) so callers
/// can hand copies to background tasks without ceremony. All mutations
/// happen via `&self`; the inner `DashMap` provides sharded locking so a
/// JoinRoom read and a beacon write never serialize on each other unless
/// they hit the same shard.
///
/// Keys are stored normalized (spaces rewritten to underscores) to match
/// the subject-derived id the ingest task records. Reader methods
/// normalize their input transparently so callers can pass raw room ids
/// unchanged.
#[derive(Debug, Clone, Default)]
pub struct SpilloverStore {
    inner: Arc<DashMap<String, RoomSpilloverState>>,
}

impl SpilloverStore {
    /// Construct an empty store. Equivalent to [`Default::default`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Public predicate the JoinRoom handler (p6-5) will call.
    ///
    /// Returns `true` only when a fresh beacon exists for `room_id` and
    /// the owner is over threshold. Unknown rooms — no beacon ever seen
    /// — return `false`, mirroring the stale-beacon behaviour: route to
    /// the owner and let it decide.
    ///
    /// The raw `room_id` is normalized (spaces → underscores) to match
    /// the storage key produced by the ingest task, which derives its
    /// key from the NATS subject built by
    /// [`crate::sfu::health_beacon::system_subject`].
    pub fn is_spilled_over(&self, room_id: &str) -> bool {
        let key = normalize_room_id(room_id);
        self.inner
            .get(key.as_ref())
            .map(|s| s.is_spilled_over())
            .unwrap_or(false)
    }

    /// Look up the raw snapshot for a room. Primarily for diagnostics
    /// and tests; production callers should prefer [`Self::is_spilled_over`].
    ///
    /// Like [`Self::is_spilled_over`], the raw `room_id` is normalized
    /// before lookup so callers can pass ids verbatim.
    pub fn snapshot(&self, room_id: &str) -> Option<RoomSpilloverState> {
        let key = normalize_room_id(room_id);
        self.inner.get(key.as_ref()).map(|s| *s)
    }

    /// Insert / overwrite the snapshot for `room_id`. Last writer wins;
    /// the beacon cadence (5s) is far slower than any conceivable JoinRoom
    /// rate, so we do not need monotonic version checks here.
    ///
    /// Intended to be called by the ingest task with an
    /// already-normalized id (extracted from the NATS subject). It does
    /// **not** re-normalize, so callers writing directly must use the
    /// same convention as the publisher.
    pub fn record(&self, room_id: &str, state: RoomSpilloverState) {
        self.inner.insert(room_id.to_string(), state);
    }

    /// Drop entries whose `last_seen` is older than `max_age`.
    ///
    /// Keeps the map bounded across long process lifetimes — rooms whose
    /// owner pod has gone silent (room torn down, pod restart) age out
    /// rather than accumulating forever. Returns the number of entries
    /// removed for observability.
    pub fn prune_stale(&self, max_age: Duration) -> usize {
        let mut to_remove = Vec::new();
        for entry in self.inner.iter() {
            if entry.value().last_seen.elapsed() >= max_age {
                to_remove.push(entry.key().clone());
            }
        }
        let n = to_remove.len();
        for k in to_remove {
            self.inner.remove(&k);
        }
        n
    }

    /// Test-only access to the entry count.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.inner.len()
    }
}

/// Normalize a raw `room_id` to the storage-key form used by the ingest
/// task (which derives keys from the NATS subject built by
/// [`crate::sfu::health_beacon::system_subject`]).
///
/// Returns a `Cow` to avoid the allocation in the common case where the
/// id contains no spaces. The single rule mirrored here — `' '` → `'_'`
/// — is the same rewrite applied everywhere else in the codebase that
/// names rooms in NATS subjects (see `lobby.rs`, `webtransport/mod.rs`,
/// etc.). Keep this private; the public API hides the normalization.
fn normalize_room_id(room_id: &str) -> std::borrow::Cow<'_, str> {
    if room_id.contains(' ') {
        std::borrow::Cow::Owned(room_id.replace(' ', "_"))
    } else {
        std::borrow::Cow::Borrowed(room_id)
    }
}

/// Extract the room id from a `room.<id>.system` NATS subject.
///
/// Returns `None` if either bookend is missing or `<id>` is empty.
/// The id is returned verbatim — the publisher
/// ([`crate::sfu::health_beacon::system_subject`]) has already applied
/// the space-to-underscore rewrite, so no further normalization is
/// required here. We deliberately do not try to handle ids containing
/// `.`: the rest of the SFU assumes the canonical shape and would
/// itself misbehave on such ids.
fn room_id_from_subject(subject: &str) -> Option<&str> {
    let after_prefix = subject.strip_prefix("room.")?;
    let id = after_prefix.strip_suffix(".system")?;
    if id.is_empty() {
        None
    } else {
        Some(id)
    }
}

/// Cancels the background ingest task when dropped.
///
/// Mirrors the [`crate::sfu::health_beacon::BeaconHandle`] pattern:
/// callers retain this handle for the lifetime of the SFU process and
/// drop it on shutdown to abort the task.
#[derive(Debug)]
pub struct SpilloverIngestHandle {
    join: JoinHandle<()>,
}

impl Drop for SpilloverIngestHandle {
    fn drop(&mut self) {
        self.join.abort();
    }
}

/// Spawn the NATS subscriber that feeds [`SpilloverStore`] from
/// `room.*.system` health beacons.
///
/// The subscriber runs on its own tokio task and never blocks the
/// packet-forwarding hot path. On every received message it parses the
/// outer `PacketWrapper`, drops anything that isn't a `HEALTH_BEACON`,
/// extracts the `room_id` from the message's NATS subject, then parses
/// the inner `HealthBeaconPacket` and records the snapshot.
///
/// Subscription failures (initial subscribe error, mid-stream closure)
/// are logged but do not panic; on closure the task exits and the
/// returned [`SpilloverIngestHandle`] becomes finished. Callers that
/// want resilience to NATS reconnects should respawn the task when this
/// handle finishes.
///
/// The ingest task also opportunistically prunes entries older than
/// `prune_horizon` on each successfully-recorded beacon, so memory stays
/// bounded without a separate scheduler.
pub fn spawn_spillover_ingest(
    nc: async_nats::Client,
    store: SpilloverStore,
) -> SpilloverIngestHandle {
    spawn_spillover_ingest_with_horizon(nc, store, Duration::from_secs(300))
}

/// Variant of [`spawn_spillover_ingest`] with an explicit prune horizon.
///
/// Exposed primarily so unit tests can verify the prune-on-write path
/// with a short horizon; production callers should use the default
/// 5-minute horizon via [`spawn_spillover_ingest`].
pub fn spawn_spillover_ingest_with_horizon(
    nc: async_nats::Client,
    store: SpilloverStore,
    prune_horizon: Duration,
) -> SpilloverIngestHandle {
    let join = tokio::spawn(async move {
        let mut sub = match nc.subscribe(SPILLOVER_SUBJECT_PATTERN).await {
            Ok(s) => s,
            Err(e) => {
                error!(
                    target: "sfu_spillover",
                    subject = SPILLOVER_SUBJECT_PATTERN,
                    error = %e,
                    "Spillover ingest failed to subscribe; task exiting"
                );
                return;
            }
        };
        debug!(
            target: "sfu_spillover",
            subject = SPILLOVER_SUBJECT_PATTERN,
            "Spillover ingest subscribed"
        );
        while let Some(msg) = sub.next().await {
            match decode_beacon(msg.subject.as_str(), &msg.payload) {
                Ok(Some((room_id, count, cpu))) => {
                    store.record(
                        &room_id,
                        RoomSpilloverState {
                            owner_count: count,
                            owner_cpu: cpu,
                            last_seen: Instant::now(),
                        },
                    );
                    // Opportunistic prune: O(rooms-seen) on the ingest
                    // task only, never on the JoinRoom hot path.
                    store.prune_stale(prune_horizon);
                }
                Ok(None) => {
                    // Non-HEALTH_BEACON packet on `room.*.system`, or a
                    // subject we couldn't parse a room id from — ignore.
                }
                Err(e) => {
                    warn!(
                        target: "sfu_spillover",
                        subject = %msg.subject,
                        error = %e,
                        "Failed to decode spillover beacon"
                    );
                }
            }
        }
        debug!(
            target: "sfu_spillover",
            "Spillover ingest subscription closed; task exiting"
        );
    });
    SpilloverIngestHandle { join }
}

/// Decode a `room.*.system` payload into `(room_id, participant_count, cpu_load)`.
///
/// Returns `Ok(None)` for non-`HEALTH_BEACON` wrappers (e.g. the
/// active-speaker broadcast also published on this subject family) and
/// for messages whose `subject` does not match the expected
/// `room.<id>.system` shape, so the ingest task can ignore them
/// silently. Returns `Err` only on genuine protobuf parse failures.
///
/// The `room_id` is extracted from the NATS `subject` argument rather
/// than the protobuf payload: vc-4le dropped `room_id` from the
/// `HealthBeaconPacket` wire shape to keep the beacon small, and the
/// subject already carries an authoritative, publisher-normalized id.
fn decode_beacon(
    subject: &str,
    payload: &[u8],
) -> Result<Option<(String, u32, f32)>, protobuf::Error> {
    let wrapper = PacketWrapper::parse_from_bytes(payload)?;
    if wrapper.packet_type != PacketType::HEALTH_BEACON.into() {
        return Ok(None);
    }
    let beacon = HealthBeaconPacket::parse_from_bytes(&wrapper.data)?;
    let Some(room_id) = room_id_from_subject(subject) else {
        return Ok(None);
    };
    Ok(Some((
        room_id.to_string(),
        beacon.participant_count,
        beacon.cpu_load,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sfu::health_beacon::{build_health_beacon_payload, system_subject};
    use std::time::{SystemTime, UNIX_EPOCH};

    /// p6-8 acceptance criterion (1): `count = 200` → `is_spilled_over = true`.
    #[test]
    fn count_over_threshold_trips_spillover() {
        let state = RoomSpilloverState {
            owner_count: 200,
            owner_cpu: 0.10,
            last_seen: Instant::now(),
        };
        assert!(
            state.is_spilled_over(),
            "owner_count=200 must trip spillover (threshold is 180)"
        );
    }

    /// p6-8 acceptance criterion (4): `cpu_load = 0.85` → spillover.
    #[test]
    fn cpu_over_threshold_trips_spillover() {
        let state = RoomSpilloverState {
            owner_count: 10,
            owner_cpu: 0.85,
            last_seen: Instant::now(),
        };
        assert!(
            state.is_spilled_over(),
            "owner_cpu=0.85 must trip spillover (threshold is 0.80)"
        );
    }

    /// p6-8 acceptance criterion (2): a beacon older than the freshness
    /// window must NOT trip spillover, even with a count well over
    /// threshold. Stale data is treated as no data.
    #[test]
    fn stale_beacon_does_not_trip_spillover() {
        let state = RoomSpilloverState {
            owner_count: 500,
            owner_cpu: 0.99,
            // 16s old: outside the 15s freshness window. We can't
            // construct an arbitrary-old Instant directly on stable
            // Rust, so subtract from `now()` via `checked_sub`.
            last_seen: Instant::now()
                .checked_sub(Duration::from_secs(16))
                .expect("Instant arithmetic should succeed in tests"),
        };
        assert!(
            !state.is_spilled_over(),
            "stale beacon (16s old) must not trip spillover"
        );
    }

    /// p6-8 acceptance criterion (3): a fresh, under-threshold beacon
    /// means the owner accepts normally — no spillover.
    #[test]
    fn fresh_under_threshold_does_not_trip_spillover() {
        let state = RoomSpilloverState {
            owner_count: 50,
            owner_cpu: 0.20,
            last_seen: Instant::now(),
        };
        assert!(
            !state.is_spilled_over(),
            "fresh beacon with count=50, cpu=0.2 must not trip spillover"
        );
    }

    /// Exact-threshold boundary (`count == 180` and `cpu == 0.80`) is
    /// NOT spillover. The bead specifies strict `>`, so equality with
    /// the threshold stays on the owner. Documents the boundary
    /// behaviour explicitly so a future tweak to `>=` is a conscious
    /// choice, not an accident.
    #[test]
    fn at_threshold_does_not_trip_spillover() {
        let at_count = RoomSpilloverState {
            owner_count: SPILLOVER_PARTICIPANT_THRESHOLD, // == 180
            owner_cpu: 0.0,
            last_seen: Instant::now(),
        };
        assert!(
            !at_count.is_spilled_over(),
            "count == threshold (180) must NOT trip spillover"
        );

        let at_cpu = RoomSpilloverState {
            owner_count: 0,
            owner_cpu: SPILLOVER_CPU_THRESHOLD, // == 0.80
            last_seen: Instant::now(),
        };
        assert!(
            !at_cpu.is_spilled_over(),
            "cpu == threshold (0.80) must NOT trip spillover"
        );
    }

    /// `SpilloverStore::is_spilled_over` for an unknown room returns
    /// `false` — same conservative default as for stale data.
    #[test]
    fn store_unknown_room_returns_false() {
        let store = SpilloverStore::new();
        assert!(
            !store.is_spilled_over("never-seen"),
            "unknown rooms must default to not spilled over"
        );
    }

    /// Round-trip through the store: record → snapshot → is_spilled_over.
    #[test]
    fn store_round_trips_snapshot() {
        let store = SpilloverStore::new();
        store.record(
            "room-x",
            RoomSpilloverState {
                owner_count: 200,
                owner_cpu: 0.30,
                last_seen: Instant::now(),
            },
        );
        let snap = store.snapshot("room-x").expect("snapshot present");
        assert_eq!(snap.owner_count, 200);
        assert!((snap.owner_cpu - 0.30).abs() < 1e-6);
        assert!(store.is_spilled_over("room-x"));
    }

    /// Last-writer-wins on repeated records for the same room.
    #[test]
    fn store_record_overwrites() {
        let store = SpilloverStore::new();
        store.record(
            "r",
            RoomSpilloverState {
                owner_count: 200,
                owner_cpu: 0.0,
                last_seen: Instant::now(),
            },
        );
        store.record(
            "r",
            RoomSpilloverState {
                owner_count: 5,
                owner_cpu: 0.0,
                last_seen: Instant::now(),
            },
        );
        assert!(
            !store.is_spilled_over("r"),
            "second record (count=5) must replace first (count=200)"
        );
    }

    /// `prune_stale` removes only entries older than the horizon and
    /// returns the count removed. Fresh entries survive.
    #[test]
    fn prune_stale_drops_only_old_entries() {
        let store = SpilloverStore::new();
        // Fresh entry: under the horizon.
        store.record(
            "fresh",
            RoomSpilloverState {
                owner_count: 10,
                owner_cpu: 0.0,
                last_seen: Instant::now(),
            },
        );
        // Stale entry: well past the horizon.
        store.record(
            "stale",
            RoomSpilloverState {
                owner_count: 10,
                owner_cpu: 0.0,
                last_seen: Instant::now()
                    .checked_sub(Duration::from_secs(60))
                    .expect("Instant arithmetic in test"),
            },
        );
        assert_eq!(store.len(), 2);

        let removed = store.prune_stale(Duration::from_secs(30));
        assert_eq!(removed, 1, "exactly one stale entry should be pruned");
        assert_eq!(store.len(), 1);
        assert!(store.snapshot("fresh").is_some());
        assert!(store.snapshot("stale").is_none());
    }

    /// `decode_beacon` round-trips a real wire payload produced by
    /// `build_health_beacon_payload`. This is the integration point
    /// between the publisher (p6-7) and the consumer (p6-8) — if the
    /// wire shape or subject format ever changes, this test fails first.
    ///
    /// vc-4le dropped `room_id` from the protobuf payload; the room id
    /// now arrives via the NATS subject built by `system_subject`. We
    /// construct that subject the same way the publisher does to keep
    /// this test honest.
    #[test]
    fn decode_beacon_round_trips_publisher_payload() {
        let payload = build_health_beacon_payload(
            200,
            0.85,
            UNIX_EPOCH + Duration::from_millis(1_700_000_000_000),
        );
        let subject = system_subject("demo");
        let decoded = decode_beacon(&subject, &payload)
            .expect("decode ok")
            .expect("is a beacon");
        assert_eq!(decoded.0, "demo");
        assert_eq!(decoded.1, 200);
        assert!((decoded.2 - 0.85).abs() < 1e-6);
    }

    /// Non-HEALTH_BEACON wrappers (e.g. the active-speaker broadcast
    /// also published on `room.*.system`) are silently ignored — the
    /// ingest task must not panic or spam logs on them.
    #[test]
    fn decode_beacon_ignores_non_health_beacon_wrappers() {
        let other = PacketWrapper {
            packet_type: PacketType::MEDIA.into(),
            data: vec![1, 2, 3],
            ..Default::default()
        };
        let bytes = other.write_to_bytes().expect("encode wrapper");
        let decoded = decode_beacon("room.demo.system", &bytes).expect("decode ok");
        assert!(
            decoded.is_none(),
            "non-HEALTH_BEACON wrapper must decode to None, not Err"
        );
    }

    /// Garbage bytes return `Err`, not panic.
    #[test]
    fn decode_beacon_errors_on_garbage() {
        // Protobuf parsers tolerate empty input as a default message,
        // so use bytes that look like a length-delimited field with an
        // impossible wire type to force a parse failure.
        let garbage: &[u8] = &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff];
        let result = decode_beacon("room.x.system", garbage);
        assert!(result.is_err(), "garbage input must return Err");
    }

    /// `room_id_from_subject` correctly parses the canonical shape and
    /// rejects malformed inputs. The final assertion documents the
    /// publisher↔consumer round-trip: ids containing spaces are
    /// rewritten by [`system_subject`] before publication, and come
    /// back out of the subject already-normalized.
    #[test]
    fn room_id_from_subject_parses_and_rejects() {
        assert_eq!(room_id_from_subject("room.foo_bar.system"), Some("foo_bar"));
        assert_eq!(room_id_from_subject("room..system"), None, "empty id");
        assert_eq!(
            room_id_from_subject("foo_bar.system"),
            None,
            "missing prefix"
        );
        assert_eq!(
            room_id_from_subject("room.foo_bar"),
            None,
            "missing .system suffix"
        );
        // Round-trip via the publisher's subject builder: a raw id with
        // spaces normalizes to underscores, then survives unchanged
        // through the parser.
        let subject = system_subject("foo bar");
        assert_eq!(subject, "room.foo_bar.system");
        assert_eq!(room_id_from_subject(&subject), Some("foo_bar"));
    }

    /// Lookup-side normalization: callers pass the raw room id (possibly
    /// containing spaces) to [`SpilloverStore::is_spilled_over`] but the
    /// ingest task stores the subject-derived (underscore-normalized)
    /// key. The store must bridge the two transparently.
    #[test]
    fn store_lookup_normalizes_spaces() {
        let store = SpilloverStore::new();
        // Ingest task would record with the subject-derived id:
        store.record(
            "foo_bar",
            RoomSpilloverState {
                owner_count: 200,
                owner_cpu: 0.0,
                last_seen: Instant::now(),
            },
        );
        // JoinRoom caller queries with the raw, pre-normalization id:
        assert!(
            store.is_spilled_over("foo bar"),
            "is_spilled_over must normalize spaces to match the storage key"
        );
        assert!(
            store.snapshot("foo bar").is_some(),
            "snapshot must normalize spaces to match the storage key"
        );
    }

    /// Use `SystemTime` import to suppress unused-import warnings when
    /// tests grow / shrink. The decode round-trip test references
    /// `UNIX_EPOCH` directly; this keeps the import block ergonomic.
    #[test]
    fn system_time_import_is_live() {
        let _ = SystemTime::now();
    }
}
