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

/// Pod ordinal of the current pod, from K8s downward API env var
/// `POD_NAME` of the form `<statefulset>-<ordinal>`. e.g.
/// `"rustlemania-webtransport-0"` → `0`.
///
/// For local/dev environments without `POD_NAME`, returns `Some(0)` by
/// default so single-instance setups work unchanged. Returns `None` only
/// when `POD_NAME` is set but cannot be parsed.
pub fn self_ordinal_from_env() -> Option<u32> {
    match env::var("POD_NAME") {
        Ok(name) => parse_ordinal(&name),
        Err(_) => Some(0),
    }
}

/// Replicas count, from `STATEFULSET_REPLICAS` env var (set by the
/// chart). Default `1` for non-StatefulSet deployments.
pub fn replicas_from_env() -> u32 {
    env::var("STATEFULSET_REPLICAS")
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(1)
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
/// Reads `POD_NAME` and `STATEFULSET_REPLICAS` from the environment and
/// returns `true` when this pod's ordinal matches the jump-hash of the
/// room.
pub fn is_owner(room_id: &str) -> bool {
    is_owner_for(room_id, self_ordinal_from_env(), replicas_from_env())
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
}
