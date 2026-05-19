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

//! Per-receiver subscription store and AllowSet resolver (bead vc-va1, p3-4).
//!
//! Receivers send declarative `SubscriptionUpdate` messages describing which
//! senders they care about (pins + visibility slots + audio policy). The
//! [`SubscriptionStore`] keeps the latest declared state per receiver and
//! resolves it against the current room membership + speaker set into an
//! [`AllowSet`] that the forwarder consults to decide what to forward.
//!
//! Resolution is deterministic (sorted within tiers) so test assertions are
//! stable. Forwarder integration lands in p3-5; this module is pure logic.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use dashmap::DashMap;

use videocall_types::protos::subscription_packet::{SubscriptionUpdate, VisibilitySlot};

use crate::actors::session_logic::SessionId;

/// Maximum number of senders whose video may be forwarded to a single receiver.
pub const MAX_VISIBLE_VIDEO: u32 = 6;

/// Maximum number of pre-join pinned session ids buffered per receiver.
///
/// Receivers may pin a session id before that participant has actually joined
/// the room (e.g. layout was restored from local storage). We buffer these
/// `pending` entries and promote them on the next `apply_update`, capped to
/// avoid unbounded memory growth from a misbehaving client.
pub const PENDING_CAP: usize = 50;

/// Preferred spatial/temporal layer for a single forwarded video stream.
///
/// Values of `(0, 0)` mean "base layer" — the legacy default before clients
/// start emitting `SubscriptionUpdate` packets with explicit slot preferences.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LayerPref {
    pub preferred_spatial: u32,
    pub preferred_temporal: u32,
}

/// Per-receiver allow-set: which senders' audio/video may be forwarded to it.
///
/// `video` carries per-stream layer preferences so the layer selector (p4-5)
/// can honor the receiver's declared slot preferences.
#[derive(Debug, Clone, Default)]
pub struct AllowSet {
    pub audio: HashSet<SessionId>,
    pub video: HashMap<SessionId, LayerPref>,
}

impl AllowSet {
    pub fn new() -> Self {
        Self::default()
    }
}

/// A receiver's most-recently declared subscription state, post-reconciliation.
///
/// Stale entries (sessions not in `current_members` at apply time) have already
/// been filtered out — every id here is known-good as of the last update.
#[derive(Debug, Clone, Default)]
pub struct ReceiverSubscription {
    pub pinned: HashSet<SessionId>,
    pub slots: Vec<VisibilitySlot>,
    pub max_video_kbps: u32,
    pub receive_all_audio: bool,
    /// vc-3s8: when true, video fans out to every current and future room
    /// member (minus self), capped at [`MAX_VISIBLE_VIDEO`]. Mirrors
    /// `receive_all_audio` for video so a receiver that wants to "see
    /// everyone" doesn't get locked out of senders that join AFTER its
    /// subscription is applied (webinar first-joiner bug).
    pub receive_all_video: bool,
}

/// Cached `AllowSet` for one receiver, keyed by the three generation counters
/// that fully determine its contents (vc-2cx).
///
/// On lookup, all three counters must match the live state for the cache to be
/// safe to serve. A mismatch in ANY counter is a stale entry — we recompute
/// and overwrite.
#[derive(Debug, Clone)]
struct CachedAllow {
    /// Shared, immutable result. Hot path returns `Arc::clone(&allow)`.
    allow: Arc<AllowSet>,
    /// Receiver's per-subscription version when this entry was computed.
    sub_version: u64,
    /// Room membership generation when this entry was computed.
    members_generation: u64,
    /// Active-speaker set generation when this entry was computed.
    speakers_generation: u64,
}

/// Tracks declarative subscription state for every receiver in a room.
///
/// Each [`SubscriptionUpdate`] from a receiver fully replaces its prior state
/// (declarative semantics). Resolution against the current speaker set + room
/// membership produces an [`AllowSet`] used by the forwarder.
///
/// vc-2cx: the forwarder hot path resolves the same `AllowSet` once per packet
/// per receiver. Membership / subscription / speaker set all change rarely
/// relative to packet rate, so we memoise the result in [`Self::cache`] keyed
/// by `(receiver, sub_version, members_generation, speakers_generation)`.
/// Hits return an `Arc<AllowSet>` with zero allocations.
#[derive(Debug, Default)]
pub struct SubscriptionStore {
    /// Per-receiver subscription state. Declarative: server replaces the prior
    /// state on each `SubscriptionUpdate`.
    per_receiver: HashMap<SessionId, ReceiverSubscription>,
    /// Pinned ids that referenced senders not yet in the room. Cleared / promoted
    /// on subsequent `apply_update` calls. Capped at [`PENDING_CAP`] per receiver.
    pending: HashMap<SessionId, Vec<SessionId>>,
    /// Per-receiver monotonic version, bumped on every `apply_update` and
    /// removed on `forget`. Receivers that never sent an update have no entry
    /// and default to 0 — that 0 is a stable cache key for the legacy
    /// default-fan-out path.
    sub_version: HashMap<SessionId, u64>,
    /// Resolved-AllowSet cache, keyed by receiver. Sharded `DashMap` so the
    /// forwarder can hold the outer `RwLock<SubscriptionStore>` read-only and
    /// still mutate the cache (one shard lock per write).
    cache: DashMap<SessionId, CachedAllow>,
}

impl SubscriptionStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply a `SubscriptionUpdate` from `receiver`, replacing its prior state.
    ///
    /// - Pinned ids and slot session ids that aren't currently in the room are
    ///   silently dropped from the stored state.
    /// - Pinned ids that weren't yet in the room are appended to a per-receiver
    ///   `pending` buffer (cap [`PENDING_CAP`], drop-oldest on overflow).
    /// - Any previously-pending ids that have since joined are promoted into
    ///   this update's pinned set automatically.
    ///
    /// On overflow, oldest pending entries are dropped first (carried-over
    /// pending entries are evicted before newly-submitted ids).
    pub fn apply_update(
        &mut self,
        receiver: SessionId,
        update: SubscriptionUpdate,
        current_members: &HashSet<SessionId>,
    ) {
        // Promote any previously-pending ids that have now joined the room.
        let (promoted, still_pending): (Vec<_>, Vec<_>) = self
            .pending
            .remove(&receiver)
            .unwrap_or_default()
            .into_iter()
            .partition(|sid| current_members.contains(sid));

        // Reconcile incoming pinned set against room membership.
        let SubscriptionUpdate {
            pinned_sessions,
            slots,
            max_video_kbps,
            receive_all_audio,
            receive_all_video,
            ..
        } = update;

        let mut pinned: HashSet<SessionId> = HashSet::new();
        // Pending ids (carried-over + newly-unknown) still awaiting their sender to join.
        let mut new_pending: Vec<SessionId> = still_pending;

        for sid in pinned_sessions.into_iter().chain(promoted.into_iter()) {
            if current_members.contains(&sid) {
                pinned.insert(sid);
            } else {
                new_pending.push(sid);
            }
        }

        // Cap pending buffer (drop-oldest).
        if new_pending.len() > PENDING_CAP {
            let overflow = new_pending.len() - PENDING_CAP;
            new_pending.drain(0..overflow);
        }
        if !new_pending.is_empty() {
            self.pending.insert(receiver, new_pending);
        }

        // Reconcile slots against room membership — drop stale, keep the rest
        // in their declared order so deterministic resolution can rely on them.
        let kept_slots: Vec<VisibilitySlot> = slots
            .into_iter()
            .filter(|slot| current_members.contains(&slot.session_id))
            .collect();

        self.per_receiver.insert(
            receiver,
            ReceiverSubscription {
                pinned,
                slots: kept_slots,
                max_video_kbps,
                receive_all_audio,
                receive_all_video,
            },
        );

        // Bump the per-receiver version and evict the cached entry. The
        // version bump alone is sufficient for correctness — `resolve_cached`
        // re-checks all three generations on every lookup — but evicting
        // here ensures we don't carry a stale entry around indefinitely for
        // a receiver that has since left the cache hot path.
        let v = self.sub_version.entry(receiver).or_insert(0);
        *v = v.wrapping_add(1);
        self.cache.remove(&receiver);
    }

    /// Resolve the receiver's [`AllowSet`] from stored subscription + live state.
    ///
    /// - If the receiver has never sent a `SubscriptionUpdate`, returns a
    ///   default AllowSet covering all room members (minus the receiver
    ///   itself) with base-layer preferences. This preserves legacy fan-out
    ///   semantics for clients that haven't been upgraded yet.
    /// - Otherwise: `pinned ∪ slot_sessions ∪ speaker_set`, intersected with
    ///   `current_members`, minus the receiver itself, capped at
    ///   [`MAX_VISIBLE_VIDEO`]. Tier order for capping (deterministic):
    ///   pinned → slots → speakers, sorted by `SessionId` within each tier.
    /// - Audio follows video unless `receive_all_audio` is set, in which case
    ///   audio is the full membership minus the receiver.
    pub fn resolve(
        &self,
        receiver: SessionId,
        current_members: &HashSet<SessionId>,
        speaker_set: &[SessionId],
    ) -> AllowSet {
        self.resolve_inner(receiver, current_members, speaker_set)
    }

    /// Cached variant of [`Self::resolve`] (vc-2cx).
    ///
    /// On a cache hit (per-receiver `sub_version` plus the supplied
    /// `members_generation` and `speakers_generation` all match the cached
    /// entry), returns a cloned `Arc<AllowSet>` — zero allocations on the
    /// hot path.
    ///
    /// On a miss (no entry, or any of the three generations differs), the
    /// `AllowSet` is computed from scratch via [`Self::resolve_inner`],
    /// wrapped in an `Arc`, inserted, and returned.
    ///
    /// `&self`-only: the inner cache is a [`DashMap`], so the caller may
    /// continue to hold the outer `Arc<RwLock<SubscriptionStore>>` read-only.
    pub fn resolve_cached(
        &self,
        receiver: SessionId,
        current_members: &Arc<HashSet<SessionId>>,
        members_generation: u64,
        speaker_set: &Arc<Vec<SessionId>>,
        speakers_generation: u64,
    ) -> Arc<AllowSet> {
        let sub_version = self.sub_version.get(&receiver).copied().unwrap_or(0);

        // Fast path: lock-free shard read; on a full-match return the Arc clone.
        if let Some(entry) = self.cache.get(&receiver) {
            if entry.sub_version == sub_version
                && entry.members_generation == members_generation
                && entry.speakers_generation == speakers_generation
            {
                return Arc::clone(&entry.allow);
            }
        }

        // Miss: compute, store, return.
        let allow = Arc::new(self.resolve_inner(receiver, current_members, speaker_set));
        self.cache.insert(
            receiver,
            CachedAllow {
                allow: Arc::clone(&allow),
                sub_version,
                members_generation,
                speakers_generation,
            },
        );
        allow
    }

    /// Compute a fresh `AllowSet` (no caching). Used by both [`Self::resolve`]
    /// and the miss path of [`Self::resolve_cached`].
    fn resolve_inner(
        &self,
        receiver: SessionId,
        current_members: &HashSet<SessionId>,
        speaker_set: &[SessionId],
    ) -> AllowSet {
        let Some(sub) = self.per_receiver.get(&receiver) else {
            // Legacy default: forward everyone (minus self) at base layer.
            let mut audio = HashSet::with_capacity(current_members.len());
            let mut video = HashMap::with_capacity(current_members.len());
            for &sid in current_members {
                if sid == receiver {
                    continue;
                }
                audio.insert(sid);
                video.insert(sid, LayerPref::default());
            }
            return AllowSet { audio, video };
        };

        // Build candidate set in deterministic tier order: pinned → slots → speakers.
        // Each tier is sorted by SessionId; dedupe via `seen` so a stable
        // first-seen ordering survives the MAX_VISIBLE_VIDEO cap.
        let in_room = |sid: SessionId| sid != receiver && current_members.contains(&sid);

        let mut pinned_sorted: Vec<SessionId> =
            sub.pinned.iter().copied().filter(|&s| in_room(s)).collect();
        pinned_sorted.sort_unstable();

        let mut slot_sorted: Vec<&VisibilitySlot> = sub
            .slots
            .iter()
            .filter(|slot| in_room(slot.session_id))
            .collect();
        slot_sorted.sort_unstable_by_key(|slot| slot.session_id);

        let mut speaker_sorted: Vec<SessionId> = speaker_set
            .iter()
            .copied()
            .filter(|&s| in_room(s))
            .collect();
        speaker_sorted.sort_unstable();

        // Layer prefs keyed by sender — slot wins; non-slot ids fall back to (0,0).
        let mut slot_prefs: HashMap<SessionId, LayerPref> = HashMap::new();
        for slot in &slot_sorted {
            slot_prefs.entry(slot.session_id).or_insert(LayerPref {
                preferred_spatial: slot.preferred_spatial,
                preferred_temporal: slot.preferred_temporal,
            });
        }

        let cap = MAX_VISIBLE_VIDEO as usize;
        let mut video: HashMap<SessionId, LayerPref> = HashMap::new();
        let mut seen: HashSet<SessionId> = HashSet::new();

        let push = |sid: SessionId,
                    video: &mut HashMap<SessionId, LayerPref>,
                    seen: &mut HashSet<SessionId>|
         -> bool {
            if video.len() >= cap {
                return false;
            }
            if seen.insert(sid) {
                let pref = slot_prefs.get(&sid).copied().unwrap_or_default();
                video.insert(sid, pref);
            }
            true
        };

        for sid in pinned_sorted {
            if !push(sid, &mut video, &mut seen) {
                break;
            }
        }
        if video.len() < cap {
            for slot in slot_sorted {
                if !push(slot.session_id, &mut video, &mut seen) {
                    break;
                }
            }
        }
        if video.len() < cap {
            for sid in speaker_sorted {
                if !push(sid, &mut video, &mut seen) {
                    break;
                }
            }
        }

        // vc-3s8: `receive_all_video` is a fourth tier that fans out to every
        // current room member. Runs after pinned/slots/speakers so explicit
        // tiers win when the cap is tight. Sorted by SessionId for
        // determinism, just like the other tiers.
        if sub.receive_all_video && video.len() < cap {
            let mut all_sorted: Vec<SessionId> = current_members
                .iter()
                .copied()
                .filter(|&s| in_room(s) && !seen.contains(&s))
                .collect();
            all_sorted.sort_unstable();
            for sid in all_sorted {
                if !push(sid, &mut video, &mut seen) {
                    break;
                }
            }
        }

        let audio: HashSet<SessionId> = if sub.receive_all_audio {
            current_members
                .iter()
                .copied()
                .filter(|&s| s != receiver)
                .collect()
        } else {
            video.keys().copied().collect()
        };

        AllowSet { audio, video }
    }

    /// Drop all state associated with `receiver` (called on disconnect).
    pub fn forget(&mut self, receiver: SessionId) {
        self.per_receiver.remove(&receiver);
        self.pending.remove(&receiver);
        self.sub_version.remove(&receiver);
        self.cache.remove(&receiver);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn members(ids: &[SessionId]) -> HashSet<SessionId> {
        ids.iter().copied().collect()
    }

    fn slot(session_id: SessionId, spatial: u32, temporal: u32) -> VisibilitySlot {
        let mut s = VisibilitySlot::new();
        s.session_id = session_id;
        s.preferred_spatial = spatial;
        s.preferred_temporal = temporal;
        s
    }

    fn update(
        pinned: &[SessionId],
        slots: Vec<VisibilitySlot>,
        receive_all_audio: bool,
    ) -> SubscriptionUpdate {
        let mut u = SubscriptionUpdate::new();
        u.pinned_sessions = pinned.to_vec();
        u.slots = slots;
        u.max_video_kbps = 0;
        u.receive_all_audio = receive_all_audio;
        u.receive_all_video = false;
        u
    }

    /// vc-3s8: builder for opt-in "see everyone" subscriptions.
    fn update_all(
        pinned: &[SessionId],
        slots: Vec<VisibilitySlot>,
        receive_all_audio: bool,
        receive_all_video: bool,
    ) -> SubscriptionUpdate {
        let mut u = update(pinned, slots, receive_all_audio);
        u.receive_all_video = receive_all_video;
        u
    }

    /// Acceptance #1: a single pinned sender lands in the AllowSet with base-layer prefs.
    #[test]
    fn pin_only_resolves_to_pinned_sender() {
        let mut store = SubscriptionStore::new();
        let room = members(&[1, 2, 3]);
        store.apply_update(1, update(&[2], vec![], false), &room);

        let allow = store.resolve(1, &room, &[]);
        assert_eq!(allow.video.len(), 1);
        assert_eq!(allow.video.get(&2), Some(&LayerPref::default()));
        assert_eq!(allow.audio, [2].into_iter().collect::<HashSet<_>>());
    }

    /// Acceptance #2: a single slot lands with its declared LayerPref.
    #[test]
    fn slot_only_resolves_with_layer_pref() {
        let mut store = SubscriptionStore::new();
        let room = members(&[1, 2, 3]);
        store.apply_update(1, update(&[], vec![slot(3, 2, 1)], false), &room);

        let allow = store.resolve(1, &room, &[]);
        assert_eq!(allow.video.len(), 1);
        assert_eq!(
            allow.video.get(&3),
            Some(&LayerPref {
                preferred_spatial: 2,
                preferred_temporal: 1,
            })
        );
        assert_eq!(allow.audio, [3].into_iter().collect::<HashSet<_>>());
    }

    /// Acceptance #3: union of pin + slot + speaker; slot LayerPref wins.
    #[test]
    fn union_pin_slot_speaker_with_slot_pref_winning() {
        let mut store = SubscriptionStore::new();
        let room = members(&[1, 2, 3, 4]);
        // sender 2 is BOTH pinned and slotted: slot prefs must win.
        store.apply_update(1, update(&[2, 3], vec![slot(2, 1, 2)], false), &room);

        let allow = store.resolve(1, &room, &[4]);
        assert_eq!(allow.video.len(), 3);
        assert_eq!(
            allow.video.get(&2),
            Some(&LayerPref {
                preferred_spatial: 1,
                preferred_temporal: 2,
            }),
            "slot pref must win over pinned default"
        );
        assert_eq!(allow.video.get(&3), Some(&LayerPref::default()));
        assert_eq!(allow.video.get(&4), Some(&LayerPref::default()));
    }

    /// Acceptance #4: stale entry (not in room at apply time) is silently dropped.
    #[test]
    fn stale_entry_dropped_on_apply() {
        let mut store = SubscriptionStore::new();
        let room = members(&[1, 2]); // 99 is not in the room
        store.apply_update(1, update(&[2], vec![slot(99, 3, 3)], false), &room);

        let allow = store.resolve(1, &room, &[]);
        assert!(!allow.video.contains_key(&99));
        assert_eq!(allow.video.len(), 1);
        assert_eq!(allow.video.get(&2), Some(&LayerPref::default()));
    }

    /// Acceptance #5: a pre-join pin is promoted once the sender joins and
    /// a new SubscriptionUpdate arrives.
    #[test]
    fn pre_join_pin_promoted_after_member_joins() {
        let mut store = SubscriptionStore::new();
        let room_before = members(&[1, 2]); // 5 not yet joined
        store.apply_update(1, update(&[5], vec![], false), &room_before);

        // 5 is parked in pending; not in the initial resolve.
        let allow = store.resolve(1, &room_before, &[]);
        assert!(!allow.video.contains_key(&5));

        // 5 joins; receiver sends a fresh update (empty pin list is fine —
        // pending gets merged in automatically).
        let room_after = members(&[1, 2, 5]);
        store.apply_update(1, update(&[], vec![], false), &room_after);

        let allow = store.resolve(1, &room_after, &[]);
        assert!(
            allow.video.contains_key(&5),
            "pending pin must be promoted once sender joins"
        );
    }

    /// Acceptance #6: 10 pinned ids cap to MAX_VISIBLE_VIDEO=6 for video.
    /// When `receive_all_audio=true`, audio covers all 10.
    #[test]
    fn oversize_pinned_caps_video_audio_follows_policy() {
        let mut store = SubscriptionStore::new();
        let pins: Vec<SessionId> = (10..20).collect(); // 10 sessions
        let mut all = pins.clone();
        all.push(1); // receiver
        let room = members(&all);

        // receive_all_audio=false: audio mirrors capped video.
        store.apply_update(1, update(&pins, vec![], false), &room);
        let allow = store.resolve(1, &room, &[]);
        assert_eq!(allow.video.len(), MAX_VISIBLE_VIDEO as usize);
        assert_eq!(allow.audio.len(), MAX_VISIBLE_VIDEO as usize);
        // Deterministic: lowest 6 SessionIds win (sorted within pinned tier).
        let mut got: Vec<SessionId> = allow.video.keys().copied().collect();
        got.sort_unstable();
        assert_eq!(got, vec![10, 11, 12, 13, 14, 15]);

        // receive_all_audio=true: audio covers all 10 senders (minus self).
        store.apply_update(1, update(&pins, vec![], true), &room);
        let allow = store.resolve(1, &room, &[]);
        assert_eq!(allow.video.len(), MAX_VISIBLE_VIDEO as usize);
        assert_eq!(allow.audio.len(), 10);
    }

    /// Acceptance #7: receiver with no update gets default AllowSet over all members.
    #[test]
    fn no_update_yields_legacy_default_allowset() {
        let store = SubscriptionStore::new();
        let room = members(&[1, 2, 3, 4]);
        let allow = store.resolve(1, &room, &[]);
        assert_eq!(allow.video.len(), 3);
        for sid in [2, 3, 4] {
            assert_eq!(allow.video.get(&sid), Some(&LayerPref::default()));
        }
        assert_eq!(allow.audio, [2, 3, 4].into_iter().collect::<HashSet<_>>());
    }

    /// Acceptance #8: >50 unknown pinned sessions cap pending to PENDING_CAP.
    #[test]
    fn pending_cap_enforced() {
        let mut store = SubscriptionStore::new();
        let room = members(&[1]); // only the receiver itself
        let pins: Vec<SessionId> = (1000..1100).collect(); // 100 unknown ids
        store.apply_update(1, update(&pins, vec![], false), &room);

        let pending_len = store.pending.get(&1).map(|v| v.len()).unwrap_or(0);
        assert_eq!(pending_len, PENDING_CAP);
    }

    /// Acceptance #9: receiver id in pinned/slots/speakers never appears in its own AllowSet.
    #[test]
    fn receiver_excluded_from_own_allowset() {
        let mut store = SubscriptionStore::new();
        let room = members(&[1, 2, 3]);
        // Receiver 1 tries to pin/slot/speaker themself.
        store.apply_update(1, update(&[1, 2], vec![slot(1, 2, 2)], true), &room);

        let allow = store.resolve(1, &room, &[1, 3]);
        assert!(!allow.video.contains_key(&1), "self-video must be excluded");
        assert!(!allow.audio.contains(&1), "self-audio must be excluded");
        // Sanity: 2 (pinned) and 3 (speaker) still make it through.
        assert!(allow.video.contains_key(&2));
        assert!(allow.video.contains_key(&3));
    }

    /// Mixed-tier overflow: pins consume cap slots first, then declared slots,
    /// and speakers fill any leftover capacity sorted ascending.
    #[test]
    fn mixed_tier_cap_pins_take_precedence_over_slots_and_speakers() {
        let mut store = SubscriptionStore::new();
        // pinned: 3 ids; slots: 2 disjoint ids; speakers: 10 disjoint ids.
        let pinned: Vec<SessionId> = vec![100, 101, 102];
        let slot_ids: Vec<SessionId> = vec![200, 201];
        let speakers: Vec<SessionId> = (300..310).collect();

        let mut all: Vec<SessionId> = Vec::new();
        all.push(1); // receiver
        all.extend(&pinned);
        all.extend(&slot_ids);
        all.extend(&speakers);
        let room = members(&all);

        let slots = vec![slot(200, 1, 1), slot(201, 2, 2)];
        store.apply_update(1, update(&pinned, slots, false), &room);

        let allow = store.resolve(1, &room, &speakers);
        assert_eq!(allow.video.len(), MAX_VISIBLE_VIDEO as usize);

        // All 3 pinned ids present (base-layer prefs).
        for sid in &pinned {
            assert_eq!(
                allow.video.get(sid),
                Some(&LayerPref::default()),
                "pinned sender {sid} must be present at base layer"
            );
        }

        // Both slot ids present with their declared LayerPref.
        assert_eq!(
            allow.video.get(&200),
            Some(&LayerPref {
                preferred_spatial: 1,
                preferred_temporal: 1,
            })
        );
        assert_eq!(
            allow.video.get(&201),
            Some(&LayerPref {
                preferred_spatial: 2,
                preferred_temporal: 2,
            })
        );

        // Exactly one speaker — the lowest sorted (300) — fills the final slot.
        assert!(allow.video.contains_key(&300));
        for sid in 301..310 {
            assert!(
                !allow.video.contains_key(&sid),
                "speaker {sid} must NOT appear (cap exhausted by pins+slots+lowest speaker)"
            );
        }
    }

    /// Declarative-replace contract: an empty update clears prior pins/slots so
    /// resolve() returns an empty AllowSet (no fallback to legacy default).
    #[test]
    fn empty_update_clears_prior_state() {
        let mut store = SubscriptionStore::new();
        let room = members(&[1, 10, 11]);

        // Seed with pins so we know there is prior state to clear.
        store.apply_update(1, update(&[10, 11], vec![], false), &room);
        let seeded = store.resolve(1, &room, &[]);
        assert_eq!(seeded.video.len(), 2);

        // Apply an empty update: no pinned, no slots, receive_all_audio=false.
        store.apply_update(1, update(&[], vec![], false), &room);

        let allow = store.resolve(1, &room, &[]);
        assert!(
            allow.video.is_empty(),
            "video must be empty after declarative-replace with empty update"
        );
        assert!(
            allow.audio.is_empty(),
            "audio must be empty (receive_all_audio=false, no video)"
        );
    }

    /// vc-3s8 regression: a receiver that opts in to "see everyone" via
    /// `receive_all_video=true` must, after a NEW sender joins, see that
    /// sender's video — not just audio. Webinar listeners hit this path when
    /// the client's `SubscriptionCoalescer` ships its initial empty update
    /// (default: `receive_all_audio:true`, `receive_all_video:true`) before
    /// the first peer becomes visible.
    #[test]
    fn vc_3s8_late_joiner_visible_when_receive_all_video() {
        let mut store = SubscriptionStore::new();
        // Step 1: only the listener is in the room.
        let room_before = members(&[1]);
        // Listener sends an empty SubscriptionUpdate with
        // receive_all_audio=true AND receive_all_video=true — the fix-side
        // contract that mirrors audio's existing semantics for video.
        store.apply_update(1, update_all(&[], vec![], true, true), &room_before);

        // Step 2: sender 2 joins later. Membership generation bumps.
        let room_after = members(&[1, 2]);
        let allow = store.resolve(1, &room_after, &[]);

        assert!(
            allow.audio.contains(&2),
            "audio: late-joining sender 2 must be audible to listener 1"
        );
        assert!(
            allow.video.contains_key(&2),
            "video: late-joining sender 2 must be visible to listener 1 \
             (vc-3s8 fix: receive_all_video=true mirrors receive_all_audio \
             for late-joining publishers)"
        );
    }

    /// vc-3s8: `receive_all_video=true` honors the MAX_VISIBLE_VIDEO cap so
    /// a fan-out subscription cannot DoS the layer selector. Deterministic
    /// tie-break by SessionId (sorted ascending), matching the other tiers.
    #[test]
    fn vc_3s8_receive_all_video_caps_at_max_visible() {
        let mut store = SubscriptionStore::new();
        // Receiver 1 + 10 senders (10..20). receive_all_video=true with no
        // explicit pins/slots/speakers should cover senders 10..16 (the
        // lowest MAX_VISIBLE_VIDEO=6 by SessionId).
        let mut all: Vec<SessionId> = (10..20).collect();
        all.push(1);
        let room = members(&all);
        store.apply_update(1, update_all(&[], vec![], false, true), &room);

        let allow = store.resolve(1, &room, &[]);
        assert_eq!(allow.video.len(), MAX_VISIBLE_VIDEO as usize);
        let mut got: Vec<SessionId> = allow.video.keys().copied().collect();
        got.sort_unstable();
        assert_eq!(got, vec![10, 11, 12, 13, 14, 15]);
    }

    /// vc-3s8: explicit tiers (pinned, slots, speakers) win over the
    /// receive_all_video catch-all when the cap is tight. The catch-all only
    /// fills leftover capacity.
    #[test]
    fn vc_3s8_explicit_tiers_win_over_receive_all_video() {
        let mut store = SubscriptionStore::new();
        // Room: receiver 1, plus 8 senders (10..18). Receiver pins 17 (a
        // high-numbered id), enables receive_all_video. Cap=6 must include
        // the pinned 17 plus the 5 lowest-by-id non-receiver members
        // (10..15) — NOT 16 (which sorts before 17 but is bumped by the
        // tier ordering: pinned drains capacity first, then catch-all fills
        // ascending without revisiting 17).
        let mut all: Vec<SessionId> = (10..18).collect();
        all.push(1);
        let room = members(&all);
        store.apply_update(1, update_all(&[17], vec![], false, true), &room);

        let allow = store.resolve(1, &room, &[]);
        assert_eq!(allow.video.len(), MAX_VISIBLE_VIDEO as usize);
        // Pinned must be present.
        assert!(allow.video.contains_key(&17));
        // Catch-all fills the remaining 5 slots with the lowest sids.
        for sid in [10, 11, 12, 13, 14] {
            assert!(allow.video.contains_key(&sid), "catch-all must admit {sid}");
        }
        // 15, 16 are squeezed out — pin + the 5 lowest already exhausted cap.
        assert!(!allow.video.contains_key(&15));
        assert!(!allow.video.contains_key(&16));
    }

    /// vc-3s8: toggling `receive_all_video` from true → false must shrink the
    /// AllowSet on the next resolve. Cache eviction is provided by the
    /// `sub_version` bump in `apply_update`; this test locks in the wire-level
    /// contract for clients that opt out (e.g., bandwidth-constrained
    /// receivers flipping the catch-all off once their UI has materialised
    /// the visible tiles).
    #[test]
    fn vc_3s8_receive_all_video_dynamic_toggle_shrinks_allowset() {
        let mut store = SubscriptionStore::new();
        let room = members(&[1, 10, 11, 12]);

        // Step 1: receive_all_video=true with no explicit pins/slots → AllowSet
        // covers every non-receiver.
        store.apply_update(1, update_all(&[], vec![], false, true), &room);
        let allow_on = store.resolve(1, &room, &[]);
        assert_eq!(allow_on.video.len(), 3);
        for sid in [10, 11, 12] {
            assert!(allow_on.video.contains_key(&sid));
        }

        // Step 2: receive_all_video=false with the same (empty) explicit
        // tiers → AllowSet collapses to empty. Declarative-replace contract
        // is preserved; the catch-all does not "stick".
        store.apply_update(1, update_all(&[], vec![], false, false), &room);
        let allow_off = store.resolve(1, &room, &[]);
        assert!(
            allow_off.video.is_empty(),
            "video must collapse when receive_all_video flips false with no \
             explicit pins/slots — declarative-replace contract"
        );
    }

    /// vc-3s8: a sender that departs the room while `receive_all_video=true`
    /// is in effect must drop out of the AllowSet on the next resolve.
    /// Membership is supplied by the caller (room state), so this verifies
    /// the resolver consumes `current_members` correctly rather than caching
    /// a stale snapshot internally.
    #[test]
    fn vc_3s8_receive_all_video_drops_departed_member() {
        let mut store = SubscriptionStore::new();

        // Step 1: room has {1, 10, 11}. receive_all_video=true covers both
        // non-receivers.
        let room_before = members(&[1, 10, 11]);
        store.apply_update(1, update_all(&[], vec![], false, true), &room_before);
        let allow_before = store.resolve(1, &room_before, &[]);
        assert!(allow_before.video.contains_key(&10));
        assert!(allow_before.video.contains_key(&11));

        // Step 2: member 10 leaves. Caller hands the resolver the updated
        // membership snapshot. The catch-all must NOT smuggle 10 back in.
        let room_after = members(&[1, 11]);
        let allow_after = store.resolve(1, &room_after, &[]);
        assert!(
            !allow_after.video.contains_key(&10),
            "departed member 10 must not appear in catch-all AllowSet"
        );
        assert!(allow_after.video.contains_key(&11));
    }

    /// vc-7wi (symmetric counterpart to vc-3s8): a receiver that joins AFTER
    /// an existing publisher and never sends a SubscriptionUpdate must, on
    /// first resolve, see the publisher via the legacy-default AllowSet path.
    ///
    /// This guards the publisher-first direction at the resolver layer. The
    /// integration test pins the dispatcher fan-out side of the same path
    /// (the new listener has to land in the per-room receivers map before
    /// the next NATS message arrives); this test pins the resolver side.
    #[test]
    fn vc_7wi_late_listener_no_update_sees_existing_publisher() {
        let store = SubscriptionStore::new();
        // Room state at the moment the listener joins: publisher 2 is
        // already a member; listener is 1.
        let room = members(&[1, 2]);
        let allow = store.resolve(1, &room, &[]);

        assert!(
            allow.video.contains_key(&2),
            "video: an existing publisher (2) must be visible to a fresh \
             listener (1) that has never sent a SubscriptionUpdate \
             (legacy-default AllowSet path)"
        );
        assert!(
            allow.audio.contains(&2),
            "audio: an existing publisher (2) must be audible to a fresh \
             listener (1) that has never sent a SubscriptionUpdate"
        );
    }

    /// vc-7wi: a receiver that joins AFTER an existing publisher and then
    /// sends the SubscriptionCoalescer's opening empty update
    /// (`receive_all_audio=true`, `receive_all_video=true`) must, on the
    /// resolve immediately following the update, see the publisher.
    ///
    /// Unlike `vc_3s8_late_joiner_visible_when_receive_all_video`, where the
    /// listener applied its update BEFORE the publisher joined (so the
    /// catch-all kicked in via a later `members_generation` bump), this test
    /// applies the update when the publisher is already in
    /// `current_members`. The catch-all must materialise the publisher into
    /// the AllowSet on the very first resolve.
    #[test]
    fn vc_7wi_late_listener_with_empty_receive_all_sees_existing_publisher() {
        let mut store = SubscriptionStore::new();
        // Publisher 2 is already a member when the listener applies its
        // opening empty update.
        let room = members(&[1, 2]);
        store.apply_update(1, update_all(&[], vec![], true, true), &room);

        let allow = store.resolve(1, &room, &[]);
        assert!(
            allow.video.contains_key(&2),
            "video: existing publisher (2) must be in the AllowSet on the \
             first resolve after an empty receive_all_video=true update"
        );
        assert!(
            allow.audio.contains(&2),
            "audio: existing publisher (2) must be in the AllowSet on the \
             first resolve after an empty receive_all_audio=true update"
        );
    }

    /// vc-3s8: when speakers + catch-all together would exceed cap, speakers
    /// drain capacity ahead of the catch-all. Locks in the tier order
    /// pinned → slots → speakers → receive_all_video so future tuning is
    /// intentional rather than accidental.
    #[test]
    fn vc_3s8_speakers_win_over_receive_all_video_at_cap() {
        let mut store = SubscriptionStore::new();
        // Room: receiver 1 + 8 senders (10..18). Speakers explicitly include
        // 16, 17 (high-numbered). receive_all_video=true catch-all fills
        // remaining capacity from the lowest sids upward.
        let mut all: Vec<SessionId> = (10..18).collect();
        all.push(1);
        let room = members(&all);
        store.apply_update(1, update_all(&[], vec![], false, true), &room);

        let allow = store.resolve(1, &room, &[16, 17]);
        assert_eq!(allow.video.len(), MAX_VISIBLE_VIDEO as usize);

        // Speakers must be present (they drained capacity ahead of the
        // catch-all).
        assert!(allow.video.contains_key(&16));
        assert!(allow.video.contains_key(&17));
        // Catch-all fills the remaining 4 slots with the lowest non-speaker
        // sids: 10, 11, 12, 13.
        for sid in [10, 11, 12, 13] {
            assert!(allow.video.contains_key(&sid), "catch-all must admit {sid}");
        }
        // 14, 15 squeezed out — speakers + the 4 lowest exhausted cap.
        assert!(!allow.video.contains_key(&14));
        assert!(!allow.video.contains_key(&15));
    }

    /// Sanity: forget() wipes both per_receiver and pending.
    #[test]
    fn forget_removes_all_state() {
        let mut store = SubscriptionStore::new();
        let room = members(&[1, 2]);
        store.apply_update(1, update(&[2, 99], vec![], false), &room);
        assert!(store.per_receiver.contains_key(&1));
        assert!(store.pending.contains_key(&1));

        store.forget(1);
        assert!(!store.per_receiver.contains_key(&1));
        assert!(!store.pending.contains_key(&1));
    }

    // ---------------- vc-2cx: resolve_cached cache invariants ----------------

    /// Cache hit returns the SAME `Arc` allocation on a repeated call with
    /// matching generations — zero new allocations on the hot path.
    #[test]
    fn resolve_cached_hit_returns_shared_arc() {
        let mut store = SubscriptionStore::new();
        let room = Arc::new(members(&[1, 2, 3]));
        let speakers: Arc<Vec<SessionId>> = Arc::new(vec![]);
        store.apply_update(1, update(&[2], vec![], false), &room);

        let a = store.resolve_cached(1, &room, 5, &speakers, 7);
        let b = store.resolve_cached(1, &room, 5, &speakers, 7);
        assert!(
            Arc::ptr_eq(&a, &b),
            "second resolve_cached must return the same Arc (cache hit)"
        );
        assert_eq!(a.video.len(), 1);
        assert!(a.video.contains_key(&2));
    }

    /// Bumping `sub_version` via `apply_update` invalidates the cache: the
    /// next `resolve_cached` returns a fresh Arc whose contents reflect the
    /// new subscription.
    #[test]
    fn resolve_cached_invalidates_on_apply_update() {
        let mut store = SubscriptionStore::new();
        let room = Arc::new(members(&[1, 2, 3]));
        let speakers: Arc<Vec<SessionId>> = Arc::new(vec![]);

        store.apply_update(1, update(&[2], vec![], false), &room);
        let a = store.resolve_cached(1, &room, 0, &speakers, 0);

        // New subscription: pin 3 instead of 2.
        store.apply_update(1, update(&[3], vec![], false), &room);
        let b = store.resolve_cached(1, &room, 0, &speakers, 0);

        assert!(!Arc::ptr_eq(&a, &b), "apply_update must invalidate cache");
        assert!(a.video.contains_key(&2));
        assert!(!a.video.contains_key(&3));
        assert!(b.video.contains_key(&3));
        assert!(!b.video.contains_key(&2));
    }

    /// A different `members_generation` must produce a fresh Arc, even if
    /// the sub state and the speaker generation are unchanged.
    #[test]
    fn resolve_cached_invalidates_on_members_generation() {
        let mut store = SubscriptionStore::new();
        let room = Arc::new(members(&[1, 2, 3]));
        let speakers: Arc<Vec<SessionId>> = Arc::new(vec![]);
        store.apply_update(1, update(&[2], vec![], false), &room);

        let a = store.resolve_cached(1, &room, 1, &speakers, 0);
        let b = store.resolve_cached(1, &room, 2, &speakers, 0);
        assert!(
            !Arc::ptr_eq(&a, &b),
            "members_generation change must invalidate cache"
        );
    }

    /// A different `speakers_generation` must produce a fresh Arc, even if
    /// the sub state and members generation are unchanged.
    #[test]
    fn resolve_cached_invalidates_on_speakers_generation() {
        let mut store = SubscriptionStore::new();
        let room = Arc::new(members(&[1, 2, 3]));
        let speakers: Arc<Vec<SessionId>> = Arc::new(vec![]);
        store.apply_update(1, update(&[2], vec![], false), &room);

        let a = store.resolve_cached(1, &room, 0, &speakers, 1);
        let b = store.resolve_cached(1, &room, 0, &speakers, 2);
        assert!(
            !Arc::ptr_eq(&a, &b),
            "speakers_generation change must invalidate cache"
        );
    }

    /// Legacy default path (receiver never sent an update) is also cached —
    /// hits return the same Arc.
    #[test]
    fn resolve_cached_legacy_default_path_is_cached() {
        let store = SubscriptionStore::new();
        let room = Arc::new(members(&[1, 2, 3, 4]));
        let speakers: Arc<Vec<SessionId>> = Arc::new(vec![]);

        let a = store.resolve_cached(1, &room, 0, &speakers, 0);
        let b = store.resolve_cached(1, &room, 0, &speakers, 0);
        assert!(Arc::ptr_eq(&a, &b), "default-path resolve must cache");
        // Same legacy semantics: forward everyone (minus self).
        assert_eq!(a.video.len(), 3);
    }

    /// `forget` must drop the cached entry so a fresh subscription post-
    /// forget cannot read a stale value through the cache.
    #[test]
    fn forget_evicts_cache_entry() {
        let mut store = SubscriptionStore::new();
        let room = Arc::new(members(&[1, 2, 3]));
        let speakers: Arc<Vec<SessionId>> = Arc::new(vec![]);
        store.apply_update(1, update(&[2], vec![], false), &room);

        let _ = store.resolve_cached(1, &room, 0, &speakers, 0);
        assert!(store.cache.contains_key(&1));

        store.forget(1);
        assert!(!store.cache.contains_key(&1));
        assert!(!store.sub_version.contains_key(&1));
    }

    /// `apply_update` on one receiver must NOT evict another receiver's
    /// cached entry — locks in per-receiver eviction granularity.
    #[test]
    fn apply_update_on_other_receiver_preserves_cache() {
        let mut store = SubscriptionStore::new();
        let room = Arc::new(members(&[1, 2, 3]));
        let speakers: Arc<Vec<SessionId>> = Arc::new(vec![]);
        store.apply_update(1, update(&[2], vec![], false), &room);
        store.apply_update(2, update(&[3], vec![], false), &room);

        let a = store.resolve_cached(1, &room, 0, &speakers, 0);
        // Mutating receiver 2 must not invalidate receiver 1's entry.
        store.apply_update(2, update(&[1], vec![], false), &room);
        let b = store.resolve_cached(1, &room, 0, &speakers, 0);
        assert!(
            Arc::ptr_eq(&a, &b),
            "receiver 1's cache must survive receiver 2's apply_update"
        );
    }

    /// A lower (older) generation must miss the cache — the invariant is
    /// equality, not "any prior key", so a hash-based key swap could not
    /// silently sneak in.
    #[test]
    fn resolve_cached_lower_generation_misses() {
        let mut store = SubscriptionStore::new();
        let room = Arc::new(members(&[1, 2, 3]));
        let speakers: Arc<Vec<SessionId>> = Arc::new(vec![]);
        store.apply_update(1, update(&[2], vec![], false), &room);

        let a = store.resolve_cached(1, &room, 5, &speakers, 0);
        let b = store.resolve_cached(1, &room, 3, &speakers, 0);
        assert!(
            !Arc::ptr_eq(&a, &b),
            "lower members_generation must miss (equality, not monotonic-or-greater)"
        );
    }
}
