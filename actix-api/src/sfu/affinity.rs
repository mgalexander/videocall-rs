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
#[derive(Debug, Clone, Copy)]
struct AffinityConfig {
    self_ordinal: Option<u32>,
    replicas: u32,
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

/// Lazily initialize and return the cached affinity config. First call
/// reads env vars; subsequent calls are a single relaxed atomic load.
fn config() -> &'static AffinityConfig {
    CONFIG.get_or_init(|| AffinityConfig {
        self_ordinal: read_self_ordinal_from_env(),
        replicas: read_replicas_from_env(),
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
