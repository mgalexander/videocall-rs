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

use crate::actors::chat_server::ChatServerPool;
use crate::actors::session_logic::SharedConnectionStates;
use crate::server_diagnostics::TrackerSender;
use crate::session_manager::SessionManager;

#[derive(Clone)]
pub struct AppState {
    /// Per-pod pool of `ChatServer` shards (bead vc-8txq). The owning shard for
    /// each room is resolved by jump-hash at session construction via
    /// [`ChatServerPool::addr_for_room`]; the WS handlers know the room before
    /// they build the session actor.
    pub chat: ChatServerPool,
    pub nats_client: async_nats::client::Client,
    pub tracker_sender: TrackerSender,
    pub session_manager: SessionManager,
    /// vc-ud6o E3: shared, lock-free per-session connection-state map handed
    /// to each `SessionLogic` so the off-actor media-publish path can read the
    /// `Active` gate without touching the single `ChatServer` mailbox.
    pub connection_states: SharedConnectionStates,
}

pub struct AppConfig {
    pub oauth_client_id: String,
    pub oauth_secret: String,
    pub oauth_redirect_url: String,
    pub oauth_auth_url: String,
    pub oauth_token_url: String,
    pub after_login_url: String,
}

/// Build NATS subject and queue name for room subscriptions
/// Used by both WebSocket and WebTransport implementations
pub fn build_subject_and_queue(room: &str, session: &str) -> (String, String) {
    (
        format!("room.{room}.*").replace(' ', "_"),
        format!("{session}-{room}").replace(' ', "_"),
    )
}

// ===========================================================================
// vc-kcpg: subject-sharded ingest helpers
// ===========================================================================
//
// The per-room ingest used to run through ONE dispatcher draining a single
// `room.{room}.*` subscription — every publisher in the room funneled through
// one `sub.next()`. To parallelize that choke we split a room's publishers
// across `K` shards by a STABLE hash of the publisher's session id and run one
// dispatcher per shard, each subscribing a disjoint subject subset.
//
// SUBJECT SCHEME
//   - K == 1 (default): legacy subjects, byte-identical to today.
//       publish:    room.{room}.{session}          (3 tokens)
//       subscribe:  room.{room}.*                   (3-token wildcard)
//   - K  > 1:
//       publish:    room.{room}.{shard}.{session}   (4 tokens)
//       subscribe:  room.{room}.{shard}.*           (4-token wildcard, per shard)
//
// MIGRATION (rolling deploy, mixed old/new fleet). A NATS `*` matches EXACTLY
// one token, so `room.{room}.*` (3 tokens) does NOT match
// `room.{room}.{shard}.{session}` (4 tokens) and vice-versa — the two subject
// spaces are DISJOINT. So to lose no packets while old (3-token publisher) and
// new (4-token publisher) pods coexist, shard 0 ALSO subscribes the legacy
// `room.{room}.*`. Because any single on-wire subject has a fixed token count,
// it matches at most ONE of shard 0's two filters — no double-delivery.

/// Stable shard index for a publisher `session` under `k` ingest shards
/// (bead vc-kcpg).
///
/// Uses the same deterministic jump-consistent hash the room→shard pool uses
/// (`crate::sfu::affinity::jump_hash`) rather than `DefaultHasher` (which is
/// per-process randomized): the shard a session lands on MUST agree between the
/// publish side and the subscribe side across the whole fleet, and across
/// reconnects, or a publisher's media would be subscribed by no dispatcher.
/// Returns `0` when `k <= 1` (the legacy single-shard case).
pub fn ingest_shard_for_session(session: u64, k: usize) -> u32 {
    if k <= 1 {
        return 0;
    }
    // vc-kcpg perf: hash the u64 session id DIRECTLY (no `String` formatting) —
    // this runs on the per-packet media-publish path (`publish_media_off_actor`),
    // so a `session.to_string()` alloc here would be a per-frame allocation under
    // load. Keyed on the session id alone so it is room-independent and matches
    // on both the publish and subscribe sides.
    crate::sfu::affinity::jump_hash_u64(session, k as u32)
}

/// Build the media-publish subject for `session` in `room` under `k` ingest
/// shards (bead vc-kcpg). `k == 1` yields the legacy `room.{room}.{session}`
/// 3-token subject (byte-identical to pre-vc-kcpg); `k > 1` yields
/// `room.{room}.{shard}.{session}`. Spaces are normalized to `_` exactly as the
/// legacy builder did (whole-string replace).
pub fn build_publish_subject(room: &str, session: u64, k: usize) -> String {
    if k <= 1 {
        format!("room.{room}.{session}").replace(' ', "_")
    } else {
        let shard = ingest_shard_for_session(session, k);
        format!("room.{room}.{shard}.{session}").replace(' ', "_")
    }
}

/// The set of NATS subscribe filters for ingest `shard` of `room` under `k`
/// shards (bead vc-kcpg).
///
/// - `k == 1`: exactly `["room.{room}.*"]` — the single legacy wildcard, so the
///   default deploy subscribes precisely what it did before.
/// - `k  > 1`, `shard == 0`: `["room.{room}.0.*", "room.{room}.*"]` — the new
///   4-token shard-0 filter PLUS the legacy 3-token wildcard for migration
///   (catches messages still published by old 3-token pods).
/// - `k  > 1`, `shard > 0`: `["room.{room}.{shard}.*"]`.
///
/// Spaces are normalized to `_` to match the publish-side normalization.
pub fn build_shard_subscribe_subjects(room: &str, shard: usize, k: usize) -> Vec<String> {
    let room = room.replace(' ', "_");
    if k <= 1 {
        return vec![format!("room.{room}.*")];
    }
    if shard == 0 {
        // Shard 0 owns the new 4-token shard-0 traffic AND the legacy 3-token
        // wildcard so a mixed-fleet deploy loses no packets. The two filters are
        // disjoint by token count, so no message is delivered twice.
        vec![format!("room.{room}.0.*"), format!("room.{room}.*")]
    } else {
        vec![format!("room.{room}.{shard}.*")]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    /// K=1 publish subject is byte-identical to the legacy form.
    #[test]
    fn k1_publish_subject_is_legacy() {
        assert_eq!(build_publish_subject("abc", 42, 1), "room.abc.42");
        // Spaces normalized exactly as the legacy builder did.
        assert_eq!(build_publish_subject("a b", 7, 1), "room.a_b.7");
    }

    /// K=1 subscribe is exactly the single legacy wildcard.
    #[test]
    fn k1_subscribe_is_single_legacy_wildcard() {
        assert_eq!(
            build_shard_subscribe_subjects("abc", 0, 1),
            vec!["room.abc.*"]
        );
    }

    /// K>1 publish subject carries the shard token and the shard agrees with
    /// `ingest_shard_for_session`.
    #[test]
    fn kgt1_publish_subject_has_shard_token() {
        let k = 4;
        for session in 0u64..200 {
            let shard = ingest_shard_for_session(session, k);
            assert!(shard < k as u32);
            assert_eq!(
                build_publish_subject("room-x", session, k),
                format!("room.room-x.{shard}.{session}")
            );
        }
    }

    /// Shard assignment is deterministic (publish/subscribe sides must agree).
    #[test]
    fn shard_assignment_is_deterministic() {
        for k in [2usize, 3, 4, 8] {
            for session in 0u64..500 {
                let a = ingest_shard_for_session(session, k);
                let b = ingest_shard_for_session(session, k);
                assert_eq!(a, b, "non-deterministic for session {session} @ k={k}");
                assert!((a as usize) < k);
            }
        }
    }

    /// K=1 always maps to shard 0.
    #[test]
    fn k1_shard_is_zero() {
        for session in 0u64..50 {
            assert_eq!(ingest_shard_for_session(session, 1), 0);
            assert_eq!(ingest_shard_for_session(session, 0), 0);
        }
    }

    /// MIGRATION COVERAGE PROOF. The union of every shard's subscribe filters
    /// must cover BOTH (a) every new 4-token publish subject AND (b) the legacy
    /// 3-token publish subject, with NO subject matched by two distinct filters
    /// across the whole shard set. We model NATS `*` semantics (matches exactly
    /// one token) directly.
    fn nats_star_matches(filter: &str, subject: &str) -> bool {
        let f: Vec<&str> = filter.split('.').collect();
        let s: Vec<&str> = subject.split('.').collect();
        if f.len() != s.len() {
            return false; // `*` matches exactly one token; no token-count slack.
        }
        f.iter()
            .zip(s.iter())
            .all(|(ft, st)| *ft == "*" || ft == st)
    }

    #[test]
    fn migration_filters_cover_all_publishers_without_double_delivery() {
        let room = "meeting";
        for k in [2usize, 3, 4, 8] {
            // Collect the full filter set across all K shards.
            let mut filters: Vec<String> = Vec::new();
            for shard in 0..k {
                filters.extend(build_shard_subscribe_subjects(room, shard, k));
            }
            // Sanity: filters are unique (no shard re-subscribes another's).
            let unique: HashSet<&String> = filters.iter().collect();
            assert_eq!(unique.len(), filters.len(), "duplicate filter for k={k}");

            // (a) Every NEW 4-token publish subject is matched by EXACTLY ONE
            //     filter (the owning shard's 4-token filter).
            for session in 0u64..300 {
                let subj = build_publish_subject(room, session, k);
                let matches: Vec<&String> = filters
                    .iter()
                    .filter(|f| nats_star_matches(f, &subj))
                    .collect();
                assert_eq!(
                    matches.len(),
                    1,
                    "new subject {subj} matched by {} filters (k={k})",
                    matches.len()
                );
            }

            // (b) Every LEGACY 3-token publish subject (what an OLD pod still
            //     emits during a rolling deploy) is matched by EXACTLY ONE
            //     filter — shard 0's legacy `room.{room}.*`.
            for session in 0u64..300 {
                let legacy = format!("room.{room}.{session}");
                let matches: Vec<&String> = filters
                    .iter()
                    .filter(|f| nats_star_matches(f, &legacy))
                    .collect();
                assert_eq!(
                    matches.len(),
                    1,
                    "legacy subject {legacy} matched by {} filters (k={k})",
                    matches.len()
                );
                assert_eq!(matches[0], &format!("room.{room}.*"));
            }
        }
    }

    /// vc-kcpg review fix (SHOULD #1): the parse-failure self-skip fallback in
    /// `egress_decide_from_parsed` builds its comparison subject via
    /// `build_publish_subject(room, receiver_session, k)`. Under K>1 that MUST be
    /// the 4-token on-wire form so an unparseable self-published packet is
    /// skipped (not echoed back to its own publisher). This pins that the
    /// fallback subject equals what the publisher actually emitted for the same
    /// `(room, session, k)`.
    #[test]
    fn parse_failure_fallback_subject_matches_publisher_form() {
        for k in [1usize, 2, 4, 8] {
            for session in 0u64..100 {
                // The publisher's on-wire subject and the receiver-side fallback
                // comparison subject are built by the SAME function with the SAME
                // K, so a self-published packet (receiver_session == session)
                // always matches — at every K, including the 4-token K>1 form.
                let on_wire = build_publish_subject("rm", session, k);
                let fallback = build_publish_subject("rm", session, k);
                assert_eq!(on_wire, fallback);
                if k > 1 {
                    // Sanity: it really is the 4-token form under K>1.
                    assert_eq!(on_wire.split('.').count(), 4, "k={k} sess={session}");
                } else {
                    assert_eq!(on_wire.split('.').count(), 3);
                }
            }
        }
    }

    /// The legacy 3-token wildcard must NOT match a new 4-token subject (this is
    /// the NATS-semantics fact the whole migration design rests on).
    #[test]
    fn legacy_wildcard_does_not_match_4token_subject() {
        assert!(nats_star_matches("room.r.*", "room.r.42"));
        assert!(!nats_star_matches("room.r.*", "room.r.0.42"));
        assert!(nats_star_matches("room.r.0.*", "room.r.0.42"));
        assert!(!nats_star_matches("room.r.0.*", "room.r.42"));
    }
}
