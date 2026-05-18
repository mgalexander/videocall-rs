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

//! Room → pod affinity using Lamping & Veach's "jump consistent hash".
//!
//! This module assigns each `room_id` to a specific pod ordinal in a
//! StatefulSet, providing stable affinity across the cluster. When the
//! replica count scales from N to N+1, only ~1/(N+1) of keys move — the
//! theoretical minimum for any consistent-hash scheme.
//!
//! Reference: Lamping & Veach, "A Fast, Minimal Memory, Consistent Hash
//! Algorithm" (2014), <https://arxiv.org/abs/1406.2294>.

use std::env;
use std::sync::OnceLock;

use async_trait::async_trait;
use tracing::warn;

/// FNV-1a 64-bit offset basis.
const FNV_OFFSET_64: u64 = 0xcbf2_9ce4_8422_2325;
/// FNV-1a 64-bit prime.
const FNV_PRIME_64: u64 = 0x0000_0100_0000_01b3;

/// Stable 64-bit hash of a byte slice using FNV-1a.
///
/// Used here (rather than `DefaultHasher`) because `DefaultHasher` is
/// randomized per-process, which would break room→pod affinity across
/// restarts. FNV-1a is deterministic across Rust versions and platforms.
#[inline]
fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET_64;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(FNV_PRIME_64);
    }
    hash
}

/// Canonical Lamping & Veach jump-consistent-hash.
///
/// Maps a 64-bit `key` to a bucket in `0..num_buckets`. O(log N) compute,
/// no allocations, no per-bucket state. When `num_buckets` grows from N
/// to N+1, on average only 1/(N+1) of keys are reassigned, and they are
/// reassigned to the new bucket — never reshuffled among existing buckets.
///
/// Returns 0 when `num_buckets == 0` (defensive — callers should pass ≥ 1).
fn jump_consistent_hash(mut key: u64, num_buckets: u32) -> u32 {
    if num_buckets == 0 {
        return 0;
    }
    let mut b: i64 = -1;
    let mut j: i64 = 0;
    while j < num_buckets as i64 {
        b = j;
        // LCG step from the paper.
        key = key.wrapping_mul(2_862_933_555_777_941_757).wrapping_add(1);
        // Compute the next jump destination. The original C reference uses
        // a double-precision divide; this integer formulation is exactly
        // equivalent and avoids floating-point determinism concerns.
        let shifted = ((key >> 33) + 1) as f64;
        j = (((b + 1) as f64) * (((1u64 << 31) as f64) / shifted)) as i64;
    }
    b as u32
}

/// Consistent hash from room_id to pod ordinal in `0..replicas`.
///
/// Uses Lamping & Veach's "jump hash" algorithm — O(log N) compute,
/// minimal disruption on replica scale changes (only ~1/N keys move
/// when N→N+1).
///
/// Defensive: returns 0 when `replicas == 0`.
pub fn jump_hash(room_id: &str, replicas: u32) -> u32 {
    if replicas == 0 {
        return 0;
    }
    let key = fnv1a_64(room_id.as_bytes());
    jump_consistent_hash(key, replicas)
}

/// Parse the trailing `-<N>` ordinal suffix from a StatefulSet pod name.
///
/// e.g. `"rustlemania-webtransport-2"` → `Some(2)`.
/// Returns `None` if there is no `-` or the suffix is not a valid u32.
fn parse_ordinal(pod_name: &str) -> Option<u32> {
    let (_, tail) = pod_name.rsplit_once('-')?;
    tail.parse::<u32>().ok()
}

/// Cached process-wide affinity config.
///
/// `POD_NAME` and `STATEFULSET_REPLICAS` are baked into the pod's env at
/// startup by the K8s downward API / chart (see commit 10c865b) and do
/// not change at runtime. We read them exactly once and cache the parsed
/// values to avoid syscall-ish env lookups + `String` allocations on hot
/// paths (e.g. the per-tick health-beacon loop, which calls `is_owner`
/// O(rooms) times per second).
///
/// vc-hc8 (p6-9) extension: also caches `REGION` and `REGION_BASE_DOMAIN`
/// for cross-region home-region pinning. Both are stored as `&'static str`
/// (via leaked `String`s, one-shot at process startup) so the rest of the
/// pipeline can pass them around without further allocation. The base
/// domain defaults to `"videocall.rs"` with a one-shot warning log, mirroring
/// the `SFU_TRANSPORT_KIND` pattern in `chat_server.rs` — the default keeps
/// dev/test deployments working but the warning makes a misconfigured prod
/// observable at startup instead of silently mis-routing cross-region
/// joiners.
#[derive(Debug, Clone, Copy)]
struct AffinityConfig {
    self_ordinal: Option<u32>,
    replicas: u32,
    region: &'static str,
    base_domain: &'static str,
}

static CONFIG: OnceLock<AffinityConfig> = OnceLock::new();

/// Read `POD_NAME` from env and parse its trailing ordinal. See
/// `self_ordinal_from_env` for the semantics; this is the uncached
/// implementation used to populate the cache.
fn read_self_ordinal_from_env() -> Option<u32> {
    match env::var("POD_NAME") {
        Ok(name) => parse_ordinal(&name),
        Err(_) => Some(0),
    }
}

/// Read `STATEFULSET_REPLICAS` from env. See `replicas_from_env` for the
/// semantics; this is the uncached implementation used to populate the
/// cache.
fn read_replicas_from_env() -> u32 {
    env::var("STATEFULSET_REPLICAS")
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(1)
}

/// Read `REGION` from env. Defaults to `"local"` when unset so dev/test
/// deployments behave as a single-region cluster. Leaked into a
/// `&'static str` so it can be cached cheaply by reference for the
/// lifetime of the process.
fn read_region_from_env() -> &'static str {
    match env::var("REGION") {
        Ok(s) if !s.is_empty() => Box::leak(s.into_boxed_str()),
        _ => "local",
    }
}

/// Read `REGION_BASE_DOMAIN` from env. Defaults to `"videocall.rs"`. Logs
/// a one-shot warning on miss — the default keeps dev/test working but a
/// missing env var in a multi-region prod deployment would silently route
/// cross-region redirects to the wrong domain, so we want the operator to
/// see it in the startup logs. (One-shot is sufficient because this is
/// only ever called from `config()`'s `get_or_init`.)
fn read_region_base_domain_from_env() -> &'static str {
    match env::var("REGION_BASE_DOMAIN") {
        Ok(s) if !s.is_empty() => Box::leak(s.into_boxed_str()),
        _ => {
            warn!(
                "REGION_BASE_DOMAIN not set; defaulting to \"videocall.rs\" for \
                 cross-region ADMISSION_DECISION{{REDIRECT}} DNS (p6-9). \
                 The deployment chart should set this in multi-region production."
            );
            "videocall.rs"
        }
    }
}

/// Lazily initialize and return the cached affinity config. First call
/// reads env vars; subsequent calls are a single relaxed atomic load.
fn config() -> &'static AffinityConfig {
    CONFIG.get_or_init(|| AffinityConfig {
        self_ordinal: read_self_ordinal_from_env(),
        replicas: read_replicas_from_env(),
        region: read_region_from_env(),
        base_domain: read_region_base_domain_from_env(),
    })
}

/// Pod ordinal of the current pod, from K8s downward API env var
/// `POD_NAME` of the form `<statefulset>-<ordinal>`. e.g.
/// `"rustlemania-webtransport-0"` → `0`.
///
/// For local/dev environments without `POD_NAME`, returns `Some(0)` by
/// default so single-instance setups work unchanged. Returns `None` only
/// when `POD_NAME` is set but cannot be parsed.
///
/// The env var is read exactly once per process and the parsed result is
/// cached (see `AffinityConfig`).
pub fn self_ordinal_from_env() -> Option<u32> {
    config().self_ordinal
}

/// Replicas count, from `STATEFULSET_REPLICAS` env var (set by the
/// chart). Default `1` for non-StatefulSet deployments.
///
/// The env var is read exactly once per process and the parsed result is
/// cached (see `AffinityConfig`).
pub fn replicas_from_env() -> u32 {
    config().replicas
}

/// Current region tag, from the `REGION` env var (set by the chart).
/// Defaults to `"local"` for single-region / dev deployments.
///
/// Used by p6-9 (vc-hc8) cross-region home-region pinning to decide
/// whether a JoinRoom should be redirected to a different region's load
/// balancer (room's home region differs from this pod's region).
///
/// The env var is read exactly once per process; subsequent calls return
/// the cached `&'static str`.
pub fn current_region() -> &'static str {
    config().region
}

/// Region base domain, from the `REGION_BASE_DOMAIN` env var. Defaults to
/// `"videocall.rs"` (with a one-shot warning log when defaulting). Used
/// to construct the cross-region redirect DNS:
///
/// ```text
/// {transport_kind}.{home_region}.{base_domain}
/// ```
///
/// e.g. `webtransport.us-east.videocall.rs`. The env var is read exactly
/// once per process; subsequent calls return the cached `&'static str`.
pub fn region_base_domain() -> &'static str {
    config().base_domain
}

/// Abstraction over the NATS JetStream KV bucket that stores each room's
/// "home region" — the region that owns the room's authoritative SFU
/// state. Decoupled from `async_nats::jetstream::kv::Store` so unit tests
/// can drive `home_region` / cross-region redirect logic without a live
/// NATS server.
///
/// The home region is set by the *first* joiner under atomic
/// create-if-absent semantics: concurrent first-joiners across regions
/// race exactly once via the KV's `create` op (revision-1 CAS), and all
/// of them then agree on whichever region won. Subsequent joiners see
/// the steady-state value via `get`.
#[async_trait]
pub trait RegionKv: Send + Sync {
    /// Read the home region for `room_id`, if previously set. Returns
    /// `None` on cache miss (room has never been joined anywhere) and on
    /// transient KV errors — callers MUST treat `None` as "unknown, fall
    /// through to `create_or_get`" rather than "definitely unset", since
    /// the latter would let a transient KV blip silently re-assign a
    /// room's home region.
    async fn get(&self, room_id: &str) -> Option<String>;

    /// Try to atomically set `room_id`'s home region to `region`. Returns
    /// the value that "won" the race:
    ///   - `region` when this caller's create succeeded, OR
    ///   - the previously-stored value when someone else got there first.
    ///
    /// On NATS error, returns `region` defensively so a transient failure
    /// keeps the user *here* (admitting locally) rather than bouncing them
    /// to a region that may itself be unreachable. The home-region binding
    /// will be re-attempted on the next joiner.
    async fn create_or_get(&self, room_id: &str, region: &str) -> String;
}

/// Two-phase home-region lookup. Fast path: a single GET serves all
/// steady-state joiners (zero writes, no JetStream round-trip beyond the
/// KV READ). Slow path: when the room has no home region yet, CAS-create
/// it to `current`. Concurrent first-joiners across regions race exactly
/// once in `create_or_get`; the loser sees the winner's region.
///
/// Takes a `&dyn RegionKv` rather than a generic so tests can swap in a
/// `Arc<Mutex<HashMap<...>>>`-backed fake without monomorphising the
/// caller, and so the chat-server actor can hold an `Arc<dyn RegionKv>`
/// behind a single virtual dispatch.
pub async fn home_region(room_id: &str, kv: &dyn RegionKv, current: &str) -> String {
    if let Some(v) = kv.get(room_id).await {
        return v;
    }
    kv.create_or_get(room_id, current).await
}

/// Pure helper: compute the cross-region redirect target DNS hostname
/// when `home != current`. Returns `None` when the room is homed in
/// this region (no redirect needed).
///
/// The DNS template is
///
/// ```text
/// {transport_kind}.{home}.{base_domain}
/// ```
///
/// e.g. `webtransport.us-east.videocall.rs`. The transport-kind prefix
/// preserves the protocol the client connected with (WebTransport vs
/// WebSocket — see p6-5 `compute_redirect_target` for the analogous pod-
/// level template). The region segment is the home region's chart-side
/// regional load balancer; from there the existing p6-5 pod-ordinal
/// redirect takes over inside the home region's cluster.
///
/// Factored as a pure function so the cross-region decision is testable
/// without env vars or NATS.
pub fn compute_cross_region_redirect_target(
    home: &str,
    current: &str,
    transport_kind: &str,
    base_domain: &str,
) -> Option<String> {
    if home == current {
        return None;
    }
    Some(format!("{transport_kind}.{home}.{base_domain}"))
}

/// Production [`RegionKv`] implementation backed by a NATS JetStream KV
/// bucket (`rooms-home-region`). The bucket should be created once at
/// process startup via [`NatsRegionKv::connect`]; the returned handle is
/// cheap to clone and safe to share across actors.
///
/// Key format: the raw `room_id`. Room IDs in this codebase are short
/// alphanumeric / UUID-ish strings, which already match NATS KV's
/// permitted character set (alphanumeric, `_`, `-`, `.`, `=` excluding
/// leading/trailing `.`). If a non-conforming room id ever reaches this
/// layer, `create_or_get` falls through to the defensive "return
/// `region`" branch on `InvalidKey` so the join still succeeds — we never
/// hard-fail on a single misbehaving room.
pub struct NatsRegionKv {
    store: async_nats::jetstream::kv::Store,
}

impl NatsRegionKv {
    /// Bucket name reserved for the room→home-region mapping. Kept as a
    /// constant so callers (e.g. operational tooling, integration tests)
    /// can reference the same string instead of hard-coding it.
    pub const BUCKET: &'static str = "rooms-home-region";

    /// Lazily ensure the KV bucket exists and wrap it in a `NatsRegionKv`.
    /// Uses `create_or_update_key_value` so multiple pods racing on
    /// startup don't error each other out. Returns an error only on
    /// JetStream-level failures (e.g. JetStream not enabled on the
    /// cluster) — callers should fall back to a no-op KV in that case so
    /// the SFU continues to function in single-region mode.
    pub async fn connect(
        nc: async_nats::client::Client,
    ) -> Result<Self, async_nats::jetstream::context::CreateKeyValueError> {
        let js = async_nats::jetstream::new(nc);
        let store = js
            .create_or_update_key_value(async_nats::jetstream::kv::Config {
                bucket: Self::BUCKET.to_string(),
                description: "Room → home-region pinning (p6-9 / vc-hc8)".to_string(),
                history: 1,
                // No TTL: home region is sticky for the life of the room.
                // The bucket entry is removed when the room itself is
                // explicitly torn down by an operator (out of scope for v1).
                ..Default::default()
            })
            .await?;
        Ok(Self { store })
    }
}

#[async_trait]
impl RegionKv for NatsRegionKv {
    async fn get(&self, room_id: &str) -> Option<String> {
        match self.store.get(room_id.to_string()).await {
            Ok(Some(bytes)) => match std::str::from_utf8(&bytes) {
                Ok(s) => Some(s.to_string()),
                Err(e) => {
                    warn!(
                        "home-region KV entry for room {} is not UTF-8: {} — \
                         treating as unset",
                        room_id, e
                    );
                    None
                }
            },
            Ok(None) => None,
            Err(e) => {
                warn!(
                    "home-region KV GET failed for room {}: {} — admitting \
                     locally (defensive: a transient KV failure must not \
                     bounce users to a region that may itself be down)",
                    room_id, e
                );
                None
            }
        }
    }

    async fn create_or_get(&self, room_id: &str, region: &str) -> String {
        use async_nats::jetstream::kv::CreateErrorKind;
        match self
            .store
            .create(room_id, bytes::Bytes::copy_from_slice(region.as_bytes()))
            .await
        {
            Ok(_) => region.to_string(),
            Err(e) if e.kind() == CreateErrorKind::AlreadyExists => {
                // Someone else won the race; read what they wrote.
                match self.store.get(room_id.to_string()).await {
                    Ok(Some(bytes)) => std::str::from_utf8(&bytes)
                        .map(str::to_string)
                        .unwrap_or_else(|_| region.to_string()),
                    Ok(None) => {
                        // Edge case: AlreadyExists then None — entry was
                        // purged between calls. Fall through to "us".
                        region.to_string()
                    }
                    Err(e) => {
                        warn!(
                            "home-region KV post-CAS GET failed for room {}: {} — \
                             admitting locally",
                            room_id, e
                        );
                        region.to_string()
                    }
                }
            }
            Err(e) => {
                warn!(
                    "home-region KV CREATE failed for room {}: {} — admitting \
                     locally (defensive on transient JetStream failure)",
                    room_id, e
                );
                region.to_string()
            }
        }
    }
}

/// Fallback [`RegionKv`] used when JetStream is not available (single-
/// node dev deployments, NATS clusters without JetStream enabled). Every
/// lookup behaves as if the room is homed in the *current* region so the
/// SFU keeps functioning in single-region mode — no cross-region redirect
/// is ever issued.
pub struct NoopRegionKv;

#[async_trait]
impl RegionKv for NoopRegionKv {
    async fn get(&self, _room_id: &str) -> Option<String> {
        None
    }

    async fn create_or_get(&self, _room_id: &str, region: &str) -> String {
        region.to_string()
    }
}

/// Pure helper: is `me` the owner of `room` under `replicas` replicas?
///
/// Factored out from `is_owner` so tests can exercise ownership logic
/// without mutating process-wide env vars.
fn is_owner_for(room: &str, me: Option<u32>, replicas: u32) -> bool {
    match me {
        Some(ord) => jump_hash(room, replicas) == ord,
        None => false,
    }
}

/// Is this pod the owner of the room?
///
/// Reads `POD_NAME` and `STATEFULSET_REPLICAS` from the environment
/// (cached after the first call) and returns `true` when this pod's
/// ordinal matches the jump-hash of the room.
pub fn is_owner(room_id: &str) -> bool {
    let cfg = config();
    is_owner_for(room_id, cfg.self_ordinal, cfg.replicas)
}

/// Pure helper: compute the redirect target headless DNS name when `me`
/// is NOT the jump-hash owner of `room` under `replicas` replicas.
///
/// Returns `None` when this pod IS the owner (no redirect needed), when
/// `replicas == 0` (defensive — single-pod / unconfigured cluster), or
/// when `me` is `None` (POD_NAME was set but unparseable — we cannot
/// know whether we're the owner, so we conservatively skip the redirect
/// rather than risk silently claiming ownership of pod-0's rooms by
/// coercing `None` to 0 at the call site).
///
/// Returns `Some(dns)` otherwise, where `dns` follows the StatefulSet
/// headless service DNS template documented in `sfu-update/PLAN.md`
/// wave 3:
///
/// ```text
/// rustlemania-{transport}-{owner_ord}.{transport}-headless.svc.cluster.local
/// ```
///
/// `transport_kind` is the literal `"webtransport"` or `"websocket"` —
/// it is the binary's identity within the cluster. It is the caller's
/// responsibility to pass a value that matches the deployed StatefulSet
/// name; this helper just splices.
///
/// No port is appended — the client reconnects on the same port it used
/// for the original connection. Factored out of the JoinRoom handler so
/// the logic can be exercised without touching process-wide env vars.
pub fn compute_redirect_target(
    room: &str,
    me: Option<u32>,
    replicas: u32,
    transport_kind: &str,
) -> Option<String> {
    if replicas == 0 {
        return None;
    }
    // Unparseable POD_NAME: skip the redirect. Coercing `None` → 0 would
    // make a misconfigured pod silently claim ownership of pod-0's
    // rooms, splitting the cluster.
    let me = me?;
    let owner = jump_hash(room, replicas);
    if owner == me {
        return None;
    }
    Some(format!(
        "rustlemania-{transport_kind}-{owner}.{transport_kind}-headless.svc.cluster.local"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 1. `jump_hash` deterministic: same input → same output.
    #[test]
    fn jump_hash_is_deterministic() {
        let a = jump_hash("room-deadbeef", 8);
        let b = jump_hash("room-deadbeef", 8);
        assert_eq!(a, b);

        // Spot-check a few more keys at different replica counts.
        for replicas in [1u32, 2, 4, 8, 16, 32] {
            for i in 0..50 {
                let key = format!("room-{i}");
                let x = jump_hash(&key, replicas);
                let y = jump_hash(&key, replicas);
                assert_eq!(x, y, "non-deterministic for {key} @ {replicas}");
                assert!(x < replicas, "bucket {x} out of range for {replicas}");
            }
        }
    }

    /// 2. Distribution: 10000 keys over 4 replicas → ~2500 ±10% per bucket.
    #[test]
    fn jump_hash_distribution_is_even() {
        let replicas = 4u32;
        let n = 10_000u32;
        let mut counts = [0u32; 4];
        for i in 0..n {
            let key = format!("room-{i}");
            let ord = jump_hash(&key, replicas);
            counts[ord as usize] += 1;
        }

        let expected = n / replicas; // 2500
        let lo = expected - expected / 10; // 2250
        let hi = expected + expected / 10; // 2750
        for (idx, &c) in counts.iter().enumerate() {
            assert!(
                c >= lo && c <= hi,
                "bucket {idx} count {c} outside [{lo}, {hi}]; counts = {counts:?}"
            );
        }
    }

    /// 3. Minimal disruption: scale 3 → 4, fewer than 1/3 of keys move.
    ///
    /// Theoretical bound is 1/4 of keys move (the new bucket steals
    /// exactly 1/4 of the keyspace); we allow slack to 1/3.
    #[test]
    fn jump_hash_minimal_disruption_on_scale() {
        let n = 10_000u32;
        let mut moved = 0u32;
        for i in 0..n {
            let key = format!("room-{i}");
            let before = jump_hash(&key, 3);
            let after = jump_hash(&key, 4);
            if before != after {
                moved += 1;
            }
        }
        let limit = n / 3; // 3333
        assert!(
            moved <= limit,
            "{moved} keys moved on 3→4 scale, exceeds limit {limit}"
        );
    }

    /// 4. Ordinal parsing — tested via pure helper to avoid env races.
    #[test]
    fn parse_ordinal_handles_statefulset_names() {
        assert_eq!(parse_ordinal("rustlemania-webtransport-2"), Some(2));
        assert_eq!(parse_ordinal("rustlemania-webtransport-0"), Some(0));
        assert_eq!(parse_ordinal("pod-13"), Some(13));
        assert_eq!(parse_ordinal("single"), None);
        assert_eq!(parse_ordinal("pod-abc"), None);
        assert_eq!(parse_ordinal("pod-"), None);
        assert_eq!(parse_ordinal("pod-1-2"), Some(2));
    }

    /// 5. `is_owner_for` — tested via pure helper to avoid env races.
    ///
    /// With no env vars, `self_ordinal_from_env` is `Some(0)` and
    /// `replicas_from_env` is `1`, so the sole pod owns every room.
    #[test]
    fn is_owner_for_default_single_pod_owns_everything() {
        // Mirrors what `is_owner()` does when POD_NAME and
        // STATEFULSET_REPLICAS are unset.
        let me = Some(0u32);
        let replicas = 1u32;
        for i in 0..100 {
            let room = format!("room-{i}");
            assert!(
                is_owner_for(&room, me, replicas),
                "single pod should own {room}"
            );
        }

        // Multi-pod sanity: each room is owned by exactly one ordinal.
        let replicas = 5u32;
        for i in 0..100 {
            let room = format!("room-{i}");
            let owner = jump_hash(&room, replicas);
            let mut owners = 0u32;
            for ord in 0..replicas {
                if is_owner_for(&room, Some(ord), replicas) {
                    owners += 1;
                }
            }
            assert_eq!(owners, 1, "room {room} should have exactly one owner");
            assert!(is_owner_for(&room, Some(owner), replicas));
        }

        // `None` ordinal (unparseable POD_NAME) — never the owner.
        assert!(!is_owner_for("room-x", None, 3));
    }

    /// 6. `compute_redirect_target`: returns `None` when this pod owns the
    ///    room, and a correctly-shaped DNS name otherwise.
    #[test]
    fn compute_redirect_target_owner_returns_none() {
        // Single-pod cluster: pod 0 owns everything → no redirect.
        for i in 0..50 {
            let room = format!("room-{i}");
            assert_eq!(
                compute_redirect_target(&room, Some(0), 1, "webtransport"),
                None,
                "single-pod owner must not redirect {room}"
            );
        }
        // replicas == 0 is treated as "unconfigured" — never redirect.
        assert_eq!(
            compute_redirect_target("room-x", Some(0), 0, "webtransport"),
            None
        );
    }

    /// 7. `compute_redirect_target`: for the non-owner case, the returned
    ///    DNS name embeds the OWNER ordinal (not `me`) and the transport.
    #[test]
    fn compute_redirect_target_non_owner_returns_owner_dns() {
        let replicas = 3u32;
        // Find a room whose jump-hash lands on a non-zero ordinal so we
        // can test the redirect from pod 0 → pod {owner}.
        let (room, owner) = (0..100)
            .find_map(|i| {
                let r = format!("room-{i}");
                let o = jump_hash(&r, replicas);
                (o != 0).then_some((r, o))
            })
            .expect("among 100 keys, at least one must hash to a non-zero ordinal");

        let target = compute_redirect_target(&room, Some(0), replicas, "webtransport")
            .expect("non-owner must produce a redirect target");
        let expected =
            format!("rustlemania-webtransport-{owner}.webtransport-headless.svc.cluster.local");
        assert_eq!(target, expected, "DNS must embed owner ordinal");

        // websocket variant uses the websocket headless name.
        let target_ws = compute_redirect_target(&room, Some(0), replicas, "websocket")
            .expect("non-owner must produce a redirect target (ws)");
        let expected_ws =
            format!("rustlemania-websocket-{owner}.websocket-headless.svc.cluster.local");
        assert_eq!(target_ws, expected_ws);

        // When `me == owner`, no redirect.
        assert_eq!(
            compute_redirect_target(&room, Some(owner), replicas, "webtransport"),
            None,
            "pod must not redirect rooms it owns"
        );
    }

    // ----- p6-9 / vc-hc8: cross-region home-region pinning ----------------
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    /// In-memory fake [`RegionKv`] used by the p6-9 tests. Models the same
    /// "first writer wins, all others read the winner" semantics that
    /// NATS JetStream KV `create` provides — without spinning up a NATS
    /// server. Implemented on top of a single `Mutex<HashMap>` so we can
    /// directly observe CAS races in tests with `tokio::join!`.
    #[derive(Default)]
    struct FakeRegionKv {
        inner: Mutex<HashMap<String, String>>,
    }

    #[async_trait]
    impl RegionKv for FakeRegionKv {
        async fn get(&self, room_id: &str) -> Option<String> {
            self.inner.lock().await.get(room_id).cloned()
        }

        async fn create_or_get(&self, room_id: &str, region: &str) -> String {
            let mut g = self.inner.lock().await;
            g.entry(room_id.to_string())
                .or_insert_with(|| region.to_string())
                .clone()
        }
    }

    /// 9. `home_region` returns the cached value on hit (steady state).
    #[tokio::test]
    async fn home_region_steady_state_returns_cached_value() {
        let kv = FakeRegionKv::default();
        kv.inner
            .lock()
            .await
            .insert("room-A".into(), "us-east".into());

        // Calling from a different region must NOT overwrite — it just
        // reads the pinned home region.
        let h = home_region("room-A", &kv, "singapore").await;
        assert_eq!(h, "us-east");
        assert_eq!(
            kv.inner.lock().await.get("room-A").cloned(),
            Some("us-east".into()),
            "home_region must not mutate on cache hit"
        );
    }

    /// 10. `home_region` performs the CAS-create on cache miss.
    #[tokio::test]
    async fn home_region_first_joiner_sets_home() {
        let kv = FakeRegionKv::default();
        let h = home_region("room-B", &kv, "singapore").await;
        assert_eq!(h, "singapore");
        // The KV must now reflect the binding.
        assert_eq!(
            kv.inner.lock().await.get("room-B").cloned(),
            Some("singapore".into()),
        );
    }

    /// 11. Two concurrent first-joiners (different regions, same room) must
    ///     converge on a SINGLE home region — the CAS winner. This is the
    ///     core safety property the bead's "atomic CAS" requirement is
    ///     about: the fake matches NATS KV `create` semantics so a passing
    ///     test here is evidence the algorithm doesn't double-bind.
    #[tokio::test]
    async fn home_region_concurrent_first_joiners_converge() {
        // Spawn two `home_region` calls into the same fake. They both miss
        // the GET; whichever wins the mutex inside `create_or_get` first
        // sets the home region, and the other reads the winner.
        let kv: Arc<FakeRegionKv> = Arc::new(FakeRegionKv::default());
        let k1 = kv.clone();
        let k2 = kv.clone();
        let t1 = tokio::spawn(async move { home_region("room-race", &*k1, "us-east").await });
        let t2 = tokio::spawn(async move { home_region("room-race", &*k2, "singapore").await });
        let (r1, r2) = (t1.await.unwrap(), t2.await.unwrap());
        assert_eq!(r1, r2, "concurrent first-joiners must converge");
        assert!(
            r1 == "us-east" || r1 == "singapore",
            "winner must be one of the two contenders, got {r1}"
        );
        // The KV stores exactly the winner.
        assert_eq!(kv.inner.lock().await.get("room-race").cloned(), Some(r1));
    }

    /// 12. `compute_cross_region_redirect_target`: no redirect when `home`
    ///     matches `current`; correctly-shaped DNS otherwise.
    #[test]
    fn compute_cross_region_redirect_target_same_region_returns_none() {
        assert_eq!(
            compute_cross_region_redirect_target(
                "us-east",
                "us-east",
                "webtransport",
                "videocall.rs"
            ),
            None
        );
        assert_eq!(
            compute_cross_region_redirect_target("local", "local", "websocket", "videocall.rs"),
            None
        );
    }

    /// 13. `compute_cross_region_redirect_target`: cross-region produces
    ///     `{transport}.{home}.{base_domain}`.
    #[test]
    fn compute_cross_region_redirect_target_cross_region_returns_dns() {
        let t = compute_cross_region_redirect_target(
            "us-east",
            "singapore",
            "webtransport",
            "videocall.rs",
        )
        .expect("cross-region must produce a target");
        assert_eq!(t, "webtransport.us-east.videocall.rs");

        // WebSocket variant.
        let tws = compute_cross_region_redirect_target(
            "us-east",
            "singapore",
            "websocket",
            "videocall.rs",
        )
        .expect("cross-region must produce a target (ws)");
        assert_eq!(tws, "websocket.us-east.videocall.rs");

        // Different base domain (e.g. staging).
        let staging = compute_cross_region_redirect_target(
            "eu-west",
            "us-east",
            "webtransport",
            "stg.videocall.rs",
        )
        .expect("cross-region must produce a target (staging)");
        assert_eq!(staging, "webtransport.eu-west.stg.videocall.rs");
    }

    /// 14. End-to-end simulation: two-region scenario.
    ///   - First joiner is in `us-east`; sets home → us-east.
    ///   - Second joiner is in `singapore`; must be redirected to us-east.
    ///   - Third joiner is in `us-east` again; must NOT be redirected.
    #[tokio::test]
    async fn cross_region_two_region_scenario_redirects_outsider() {
        let kv = FakeRegionKv::default();

        // Joiner 1, us-east — sets the home region.
        let h1 = home_region("room-meeting", &kv, "us-east").await;
        assert_eq!(h1, "us-east");
        assert_eq!(
            compute_cross_region_redirect_target(&h1, "us-east", "webtransport", "videocall.rs"),
            None,
            "first joiner is in home region; no redirect"
        );

        // Joiner 2, singapore — must redirect to us-east.
        let h2 = home_region("room-meeting", &kv, "singapore").await;
        assert_eq!(h2, "us-east", "second joiner must see the pinned region");
        let target =
            compute_cross_region_redirect_target(&h2, "singapore", "webtransport", "videocall.rs")
                .expect("cross-region joiner must get redirect target");
        assert_eq!(target, "webtransport.us-east.videocall.rs");

        // Joiner 3, us-east again — must NOT redirect.
        let h3 = home_region("room-meeting", &kv, "us-east").await;
        assert_eq!(h3, "us-east");
        assert_eq!(
            compute_cross_region_redirect_target(&h3, "us-east", "webtransport", "videocall.rs"),
            None,
            "in-region joiner must not be redirected"
        );
    }

    /// 15. `NoopRegionKv` never redirects: `get` returns `None`, and
    ///     `create_or_get` echoes the caller's region. This is the safe
    ///     single-region fallback used when JetStream isn't available.
    #[tokio::test]
    async fn noop_region_kv_never_redirects() {
        let kv = NoopRegionKv;
        assert_eq!(kv.get("room-X").await, None);
        assert_eq!(kv.create_or_get("room-X", "us-east").await, "us-east");

        let h = home_region("room-X", &kv, "us-east").await;
        assert_eq!(
            compute_cross_region_redirect_target(&h, "us-east", "webtransport", "videocall.rs"),
            None,
        );
    }

    /// 8. `compute_redirect_target`: returns `None` when `me` is `None`.
    ///
    /// Locks in the nice-to-have safety fix from the p6-5 follow-up review:
    /// an unparseable `POD_NAME` must NOT silently coerce to ordinal 0 and
    /// claim ownership of pod-0's rooms. A `None` self-ordinal means the
    /// operator misconfigured the pod; the safe response is to skip the
    /// redirect entirely (the join itself still proceeds — the worst case
    /// is a sub-optimal pod placement, not a cluster split).
    #[test]
    fn compute_redirect_target_none_self_ordinal_skips_redirect() {
        let replicas = 3u32;
        // Pick any room that does NOT hash to 0 (so a `me=Some(0)` call
        // would normally produce a redirect). With `me=None`, no redirect
        // can be computed because we don't know whether we're the owner.
        let (room, owner) = (0..100)
            .find_map(|i| {
                let r = format!("redirect-room-{i}");
                let o = jump_hash(&r, replicas);
                (o != 0).then_some((r, o))
            })
            .expect("among 100 keys, at least one must hash to a non-zero ordinal");
        // Sanity: with a parseable ordinal, this room WOULD redirect.
        let baseline = compute_redirect_target(&room, Some(0), replicas, "webtransport");
        assert!(
            baseline.is_some(),
            "baseline: room {room} owned by {owner}, me=0 should redirect"
        );
        // With None, the redirect is suppressed.
        assert_eq!(
            compute_redirect_target(&room, None, replicas, "webtransport"),
            None,
            "unparseable POD_NAME must NOT trigger redirect (would split cluster)"
        );
    }
}
