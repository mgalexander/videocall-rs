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
use crate::sfu::room_state::RemotePublishers;

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
        self.resolve_inner(
            receiver,
            current_members,
            speaker_set,
            &RemotePublishers::default(),
        )
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
    ///
    /// vc-54j2: `remote_publishers` is the cross-pod publisher snapshot. It is
    /// folded into the `members_generation` (see
    /// `RoomState::remote_publishers_snapshot_with_generation`), so a change to
    /// the registry invalidates the cache via the SAME counter — no extra
    /// cache-key dimension is needed.
    pub fn resolve_cached(
        &self,
        receiver: SessionId,
        current_members: &Arc<HashSet<SessionId>>,
        members_generation: u64,
        speaker_set: &Arc<Vec<SessionId>>,
        speakers_generation: u64,
        remote_publishers: &RemotePublishers,
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
        let allow =
            Arc::new(self.resolve_inner(receiver, current_members, speaker_set, remote_publishers));
        // vc-8wd Layer 1: observe the resolved AllowSet size only on the
        // actual (re)compute — NOT on cache hits — so the histogram tracks
        // resolve events without per-packet cost. Catches empty-AllowSet
        // regressions (a pile-up at bucket 0 = receivers seeing nobody).
        crate::metrics::SFU_ALLOWSET_SIZE.observe(allow.video.len() as f64);
        // vc-8wd Layer 2: targeted AllowSet trace for the configured room.
        // Gated on the cheap atomic load first. The receiver session is the
        // natural session key here; the room id is not in scope at this
        // layer, so we gate on session only (room gating happens upstream in
        // the forwarder/join paths).
        if crate::sfu::trace::tracing_enabled()
            && crate::sfu::trace::traced_session(&receiver.to_string())
        {
            tracing::debug!(
                target: "sfu_trace",
                receiver,
                audio_len = allow.audio.len(),
                video_len = allow.video.len(),
                members_generation,
                speakers_generation,
                "AllowSet resolved"
            );
        }
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

    /// vc-zexm: rewarm every *currently-cached* receiver's `AllowSet` against a
    /// fresh `(members, speakers, remote_publishers)` snapshot, OFF the media
    /// hot path.
    ///
    /// ## Problem this solves
    ///
    /// The per-receiver cache key folds the GLOBAL `members_generation` counter
    /// (see [`CachedAllow`]). A single join bumps that counter, so the NEXT
    /// media packet for EVERY one of the room's `R` receivers misses the cache
    /// and rebuilds via [`Self::resolve_inner`] (O(members) each) — an O(R²)
    /// recompute storm executed INSIDE the dispatcher's fan-out barrier. That
    /// barrier gates inbound drain, so the storm throttles ingest and the
    /// upstream async-nats subscription silently drops, starving late joiners of
    /// media (the late-joiner root cause).
    ///
    /// ## The fix
    ///
    /// Membership changes are rare (cold path) relative to the media packet
    /// rate. So when membership (or the remote-publisher registry) changes, the
    /// caller rebuilds the affected cache entries HERE — on the cold
    /// `JoinRoom` / `Leave` path — using the post-change snapshot + generation.
    /// The subsequent media packets then HIT the cache (matching generation),
    /// so no `resolve_inner` runs inside the fan-out barrier. The per-join cost
    /// is O(hot-receivers × members) paid once, off the media-drain barrier —
    /// NOT O(R²) per packet on it.
    ///
    /// ## Why this is always correct (cannot serve a stale AllowSet)
    ///
    /// This method is a pure *performance* warm-up. It only ever writes entries
    /// stamped with the supplied generations. [`Self::resolve_cached`] STILL
    /// validates all three generations (`sub_version`, `members_generation`,
    /// `speakers_generation`) on every lookup. So if the warmed snapshot is
    /// already stale by the time a media packet arrives (a second membership
    /// change raced in between), the media path simply misses and recomputes —
    /// degrading to the pre-fix behavior for that one packet, never serving a
    /// wrong answer. The visible-tile cap, cross-pod publisher folding, and
    /// pin/slot/speaker/receive-all resolution are all unchanged because the
    /// warm-up calls the SAME [`Self::resolve_inner`] the hot path would.
    ///
    /// ## Scope: only ALREADY-cached receivers
    ///
    /// We iterate the existing cache keys, not `per_receiver` and not the full
    /// membership. A receiver with no cache entry is "cold" — its first media
    /// packet computes once and caches, exactly as before (a one-time
    /// O(members), not a storm). Warming a cold receiver here would be wasted
    /// work for a receiver that may never receive another packet. Receivers
    /// whose `sub_version` no longer matches a live subscription are skipped via
    /// the same equality the hot path uses.
    ///
    /// `&self`-only: the cache is a [`DashMap`]; `per_receiver` / `sub_version`
    /// are read immutably, so a caller holding the outer `RwLock` read-only can
    /// invoke this. (The membership-change callers happen to hold it for write,
    /// which is also fine.)
    pub fn rewarm_cache(
        &self,
        current_members: &Arc<HashSet<SessionId>>,
        members_generation: u64,
        speaker_set: &Arc<Vec<SessionId>>,
        speakers_generation: u64,
        remote_publishers: &RemotePublishers,
    ) {
        // Snapshot the cached receiver ids first so we are not iterating the
        // DashMap while inserting back into it (which would deadlock on the
        // same shard). The hot-receiver set is bounded by room size.
        let receivers: Vec<SessionId> = self.cache.iter().map(|e| *e.key()).collect();
        for receiver in receivers {
            let sub_version = self.sub_version.get(&receiver).copied().unwrap_or(0);
            // Skip entries that are already fresh for all three generations —
            // nothing to rebuild (e.g. a join that did not actually change this
            // receiver's resolved set still needs the stamp refreshed, so we
            // only skip on an exact generation match).
            if let Some(entry) = self.cache.get(&receiver) {
                if entry.sub_version == sub_version
                    && entry.members_generation == members_generation
                    && entry.speakers_generation == speakers_generation
                {
                    continue;
                }
            }
            let allow = Arc::new(self.resolve_inner(
                receiver,
                current_members,
                speaker_set,
                remote_publishers,
            ));
            self.cache.insert(
                receiver,
                CachedAllow {
                    allow,
                    sub_version,
                    members_generation,
                    speakers_generation,
                },
            );
        }
    }

    /// Compute a fresh `AllowSet` (no caching). Used by both [`Self::resolve`]
    /// and the miss path of [`Self::resolve_cached`].
    fn resolve_inner(
        &self,
        receiver: SessionId,
        current_members: &HashSet<SessionId>,
        speaker_set: &[SessionId],
        remote_publishers: &RemotePublishers,
    ) -> AllowSet {
        let Some(sub) = self.per_receiver.get(&receiver) else {
            // Legacy default: forward everyone (minus self) at base layer.
            //
            // Local members are added UNCAPPED here, exactly as before — the
            // owner-pod fan-out is unchanged. The visible-video cap is enforced
            // downstream (the forwarder's receive-all fallback + the layer
            // selector budget); the legacy default never capped allow.video and
            // we deliberately do not start, to avoid a non-deterministic
            // owner-pod regression.
            //
            // vc-54j2: cross-pod VIDEO publishers (delivered over NATS but
            // absent from this pod's membership) are folded into allow.video so
            // the forwarder admits them via the `allow.video.contains_key`
            // branch instead of dropping their media as `unsubscribed`. On a
            // spill pod, `current_members` is dozens of non-publishing
            // listeners and the real senders are remote — without this they are
            // never in allow.video and every packet is dropped. All remote
            // publishers (audio-only included) are also audible. Local members
            // shadow any same-id remote entry (the membership path is
            // authoritative), so there is no double-insert.
            let mut audio = HashSet::with_capacity(current_members.len());
            let mut video: HashMap<SessionId, LayerPref> = HashMap::new();
            for &sid in current_members {
                if sid == receiver {
                    continue;
                }
                audio.insert(sid);
                video.insert(sid, LayerPref::default());
            }
            for &sid in &remote_publishers.video {
                if sid != receiver && !current_members.contains(&sid) {
                    video.entry(sid).or_default();
                }
            }
            for &sid in remote_publishers
                .audio
                .iter()
                .chain(&remote_publishers.video)
            {
                if sid != receiver && !current_members.contains(&sid) {
                    audio.insert(sid);
                }
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
        //
        // vc-54j2: a `receive_all_video` receiver wants EVERY publisher,
        // including cross-pod ones that are not local members. Fold the remote
        // video publishers in alongside the local members, with remote
        // publishers ordered FIRST so they are not starved by local listeners
        // under a tight cap (same rationale as the legacy-default path above).
        if sub.receive_all_video && video.len() < cap {
            let mut remote_video: Vec<SessionId> = remote_publishers
                .video
                .iter()
                .copied()
                .filter(|&s| s != receiver && !current_members.contains(&s) && !seen.contains(&s))
                .collect();
            remote_video.sort_unstable();
            for sid in remote_video {
                if !push(sid, &mut video, &mut seen) {
                    break;
                }
            }
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

        let mut audio: HashSet<SessionId> = if sub.receive_all_audio {
            current_members
                .iter()
                .copied()
                .filter(|&s| s != receiver)
                .collect()
        } else {
            video.keys().copied().collect()
        };

        // vc-54j2: a `receive_all_audio` receiver hears every remote publisher
        // too. A receiver that did NOT opt into receive-all audio keeps audio
        // mirroring its (capped) video allow-set — including any remote video
        // publisher that won a slot above — but is not force-fed audio-only
        // remote senders it never asked for.
        if sub.receive_all_audio {
            for &sid in remote_publishers
                .audio
                .iter()
                .chain(&remote_publishers.video)
            {
                if sid != receiver && !current_members.contains(&sid) {
                    audio.insert(sid);
                }
            }
        }

        AllowSet { audio, video }
    }

    /// Whether `receiver` is in a "receive everyone" posture for audio and
    /// video respectively, returned as `(receive_all_audio, receive_all_video)`.
    ///
    /// vc-72a: the [`AllowSet`] produced by [`Self::resolve_inner`] is built
    /// from the LOCAL room-member snapshot. In a multi-pod deployment a
    /// sender that joined a *different* pod is never in this pod's
    /// `current_members`, so it can never appear in the AllowSet — even
    /// though its media physically arrives here over NATS. The same gap
    /// shows up for a brief window during same-pod co-arrival, before the
    /// sender's `insert_member` lands. Either way the forwarder would
    /// hard-drop the sender's media as "unsubscribed" and the receiver gets
    /// zero packets for the whole run.
    ///
    /// This predicate lets the forwarder recover the receiver's intent
    /// independently of local membership: a receiver that wants to see/hear
    /// everyone should be forwarded any sender whose media actually reached
    /// this pod, regardless of whether that sender is a *local* member.
    ///
    /// Semantics:
    /// * **No `SubscriptionUpdate` ever applied** (the bot / legacy-client
    ///   path) → `(true, true)`. The receiver implicitly wants everyone, so
    ///   both tiers fall back to receive-all. This mirrors the legacy-default
    ///   AllowSet, which fans out to every member.
    /// * **Explicit subscription present** → `(receive_all_audio,
    ///   receive_all_video)` exactly as declared. A receiver that declared a
    ///   restrictive subscription (both flags false) gets `(false, false)`
    ///   and the membership-bound AllowSet remains authoritative for it.
    pub fn receive_mode(&self, receiver: SessionId) -> (bool, bool) {
        match self.per_receiver.get(&receiver) {
            // Legacy-default receiver: implicitly "see + hear everyone".
            None => (true, true),
            Some(sub) => (sub.receive_all_audio, sub.receive_all_video),
        }
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

        let a = store.resolve_cached(1, &room, 5, &speakers, 7, &RemotePublishers::default());
        let b = store.resolve_cached(1, &room, 5, &speakers, 7, &RemotePublishers::default());
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
        let a = store.resolve_cached(1, &room, 0, &speakers, 0, &RemotePublishers::default());

        // New subscription: pin 3 instead of 2.
        store.apply_update(1, update(&[3], vec![], false), &room);
        let b = store.resolve_cached(1, &room, 0, &speakers, 0, &RemotePublishers::default());

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

        let a = store.resolve_cached(1, &room, 1, &speakers, 0, &RemotePublishers::default());
        let b = store.resolve_cached(1, &room, 2, &speakers, 0, &RemotePublishers::default());
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

        let a = store.resolve_cached(1, &room, 0, &speakers, 1, &RemotePublishers::default());
        let b = store.resolve_cached(1, &room, 0, &speakers, 2, &RemotePublishers::default());
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

        let a = store.resolve_cached(1, &room, 0, &speakers, 0, &RemotePublishers::default());
        let b = store.resolve_cached(1, &room, 0, &speakers, 0, &RemotePublishers::default());
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

        let _ = store.resolve_cached(1, &room, 0, &speakers, 0, &RemotePublishers::default());
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

        let a = store.resolve_cached(1, &room, 0, &speakers, 0, &RemotePublishers::default());
        // Mutating receiver 2 must not invalidate receiver 1's entry.
        store.apply_update(2, update(&[1], vec![], false), &room);
        let b = store.resolve_cached(1, &room, 0, &speakers, 0, &RemotePublishers::default());
        assert!(
            Arc::ptr_eq(&a, &b),
            "receiver 1's cache must survive receiver 2's apply_update"
        );
    }

    // ---------------- vc-72a: receive_mode posture ----------------

    /// vc-72a: a receiver that never sent a `SubscriptionUpdate` is in the
    /// implicit "see + hear everyone" posture so the forwarder can admit a
    /// publisher that is not a local member (cross-pod co-arrival).
    #[test]
    fn receive_mode_defaults_to_all_for_no_update_receiver() {
        let store = SubscriptionStore::new();
        assert_eq!(store.receive_mode(1), (true, true));
    }

    /// vc-72a: an explicit subscription reports its declared receive-all
    /// flags verbatim. The `SubscriptionCoalescer`'s opening empty flush
    /// (`receive_all_audio=true`, `receive_all_video=true`) → `(true, true)`.
    #[test]
    fn receive_mode_reports_declared_flags() {
        let mut store = SubscriptionStore::new();
        let room = members(&[1, 2]);

        store.apply_update(1, update_all(&[], vec![], true, true), &room);
        assert_eq!(store.receive_mode(1), (true, true));

        // Audio-only fan-out.
        store.apply_update(1, update_all(&[], vec![], true, false), &room);
        assert_eq!(store.receive_mode(1), (true, false));

        // Restrictive: both flags false.
        store.apply_update(1, update_all(&[], vec![], false, false), &room);
        assert_eq!(store.receive_mode(1), (false, false));
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

        let a = store.resolve_cached(1, &room, 5, &speakers, 0, &RemotePublishers::default());
        let b = store.resolve_cached(1, &room, 3, &speakers, 0, &RemotePublishers::default());
        assert!(
            !Arc::ptr_eq(&a, &b),
            "lower members_generation must miss (equality, not monotonic-or-greater)"
        );
    }

    // ---------------- vc-54j2: remote-publisher folding ----------------

    fn remote(audio: &[SessionId], video: &[SessionId]) -> RemotePublishers {
        RemotePublishers {
            audio: audio.iter().copied().collect(),
            video: video.iter().copied().collect(),
        }
    }

    /// vc-54j2 core: a spill-pod listener (no SubscriptionUpdate → legacy
    /// default) with DOZENS of fellow local listeners must still admit a
    /// cross-pod publisher's audio AND video. Before the fix, the legacy
    /// default fan-out filled all MAX_VISIBLE_VIDEO slots with non-publishing
    /// listeners and the cross-pod publisher was dropped as `unsubscribed`.
    #[test]
    fn spill_listener_admits_cross_pod_publisher_over_listeners() {
        let store = SubscriptionStore::new();
        // Receiver 1 plus 20 fellow LISTENERS (none publish). The cross-pod
        // publisher 999 is NOT a local member.
        let mut all: Vec<SessionId> = (1..=21).collect();
        let room: HashSet<SessionId> = all.drain(..).collect();
        let pubs = remote(&[999], &[999]);

        let allow = store.resolve_inner(1, &room, &[], &pubs);
        assert!(
            allow.audio.contains(&999),
            "cross-pod publisher audio must be admitted to a legacy-default listener"
        );
        assert!(
            allow.video.contains_key(&999),
            "cross-pod publisher video must be present so the forwarder admits it \
             via allow.video.contains_key (not dropped as unsubscribed)"
        );
    }

    /// vc-54j2: multiple cross-pod video publishers (≤ cap) all land in
    /// allow.video so the forwarder admits each of them.
    #[test]
    fn multiple_cross_pod_video_publishers_admitted() {
        let store = SubscriptionStore::new();
        let mut all: Vec<SessionId> = (1..=20).collect();
        let room: HashSet<SessionId> = all.drain(..).collect();
        // 3 cross-pod video publishers.
        let pubs = remote(&[901, 902, 903], &[901, 902, 903]);

        let allow = store.resolve_inner(1, &room, &[], &pubs);
        for p in [901, 902, 903] {
            assert!(
                allow.video.contains_key(&p),
                "every cross-pod video publisher must be in allow.video ({p})"
            );
        }
    }

    /// vc-54j2: a `receive_all_video` listener also admits cross-pod video
    /// publishers (the structured-path counterpart to the legacy default).
    #[test]
    fn receive_all_video_admits_cross_pod_publisher() {
        let mut store = SubscriptionStore::new();
        let room = members(&[1]);
        store.apply_update(1, update_all(&[], vec![], true, true), &room);
        let pubs = remote(&[999], &[999]);

        let allow = store.resolve_inner(1, &room, &[], &pubs);
        assert!(allow.video.contains_key(&999));
        assert!(allow.audio.contains(&999));
    }

    /// vc-54j2 regression guard: a RESTRICTIVE subscription (both receive-all
    /// flags false, no pins/slots) must NOT be force-fed cross-pod publishers.
    #[test]
    fn restrictive_subscription_ignores_cross_pod_publishers() {
        let mut store = SubscriptionStore::new();
        let room = members(&[1, 2]);
        store.apply_update(1, update_all(&[], vec![], false, false), &room);
        let pubs = remote(&[999], &[999]);

        let allow = store.resolve_inner(1, &room, &[], &pubs);
        assert!(
            !allow.video.contains_key(&999),
            "restrictive subscription must not admit a cross-pod video publisher"
        );
        assert!(
            !allow.audio.contains(&999),
            "restrictive subscription must not admit a cross-pod audio publisher"
        );
    }

    /// vc-54j2: an audio-only cross-pod publisher is audible to a legacy
    /// listener but must NOT consume a video slot.
    #[test]
    fn audio_only_cross_pod_publisher_not_in_video() {
        let store = SubscriptionStore::new();
        let room = members(&[1]);
        let pubs = remote(&[999], &[]); // audio only

        let allow = store.resolve_inner(1, &room, &[], &pubs);
        assert!(allow.audio.contains(&999));
        assert!(!allow.video.contains_key(&999));
    }

    // ---------------- vc-zexm: rewarm_cache (off-hot-path warm-up) ----------------

    /// vc-zexm core: a join bumps `members_generation`. AFTER `rewarm_cache`
    /// against the new generation, the next `resolve_cached` for an
    /// already-cached receiver is a CACHE HIT at the new generation (same Arc),
    /// and its contents reflect the new member. This is the property that keeps
    /// the post-join media packets off the `resolve_inner` recompute path.
    #[test]
    fn rewarm_makes_post_join_resolve_a_hit_with_fresh_contents() {
        let store = SubscriptionStore::new();
        // Receiver 1 is a legacy-default receiver (no SubscriptionUpdate).
        let room_g0 = Arc::new(members(&[1, 2]));
        let speakers: Arc<Vec<SessionId>> = Arc::new(vec![]);

        // Prime the cache at generation 0.
        let a = store.resolve_cached(1, &room_g0, 0, &speakers, 0, &RemotePublishers::default());
        assert_eq!(a.video.len(), 1);
        assert!(a.video.contains_key(&2));

        // Member 3 joins → new snapshot, generation bumps to 1. Re-warm the
        // cache off the hot path (what the JoinRoom handler does).
        let room_g1 = Arc::new(members(&[1, 2, 3]));
        store.rewarm_cache(&room_g1, 1, &speakers, 0, &RemotePublishers::default());

        // The very next resolve at the NEW generation must be a HIT (the rewarm
        // already stamped generation 1) AND must include the new member.
        let warmed =
            store.resolve_cached(1, &room_g1, 1, &speakers, 0, &RemotePublishers::default());
        let again =
            store.resolve_cached(1, &room_g1, 1, &speakers, 0, &RemotePublishers::default());
        assert!(
            Arc::ptr_eq(&warmed, &again),
            "post-rewarm resolve must hit the warmed entry (no recompute)"
        );
        assert!(
            warmed.video.contains_key(&3),
            "warmed AllowSet must include the newly-joined member 3"
        );
        assert!(warmed.video.contains_key(&2));
    }

    /// vc-zexm: `rewarm_cache` only touches receivers that ALREADY have a cache
    /// entry. A cold receiver (never resolved) is not warmed — its first packet
    /// computes once and caches, exactly as before.
    #[test]
    fn rewarm_skips_cold_receivers() {
        let store = SubscriptionStore::new();
        let room = Arc::new(members(&[1, 2, 3]));
        let speakers: Arc<Vec<SessionId>> = Arc::new(vec![]);

        // Only receiver 1 is primed (has a cache entry); 2 and 3 are cold.
        let _ = store.resolve_cached(1, &room, 0, &speakers, 0, &RemotePublishers::default());

        store.rewarm_cache(&room, 1, &speakers, 0, &RemotePublishers::default());

        assert!(
            store.cache.contains_key(&1),
            "primed receiver must be warmed"
        );
        assert!(
            !store.cache.contains_key(&2),
            "cold receiver must NOT be warmed by rewarm_cache"
        );
        assert!(!store.cache.contains_key(&3));
    }

    /// vc-zexm correctness invariant: even if `rewarm_cache` warmed a snapshot
    /// that is ALREADY stale (a second join raced in), `resolve_cached` still
    /// validates the generation and recomputes — it never serves the stale
    /// warmed entry for a different generation.
    #[test]
    fn rewarm_never_serves_stale_entry_for_a_newer_generation() {
        let store = SubscriptionStore::new();
        let room_g1 = Arc::new(members(&[1, 2]));
        let speakers: Arc<Vec<SessionId>> = Arc::new(vec![]);

        // Prime + warm at generation 1.
        let _ = store.resolve_cached(1, &room_g1, 1, &speakers, 0, &RemotePublishers::default());
        store.rewarm_cache(&room_g1, 1, &speakers, 0, &RemotePublishers::default());

        // A second join raced in: live generation is now 2 with a bigger room.
        // The forwarder reads generation 2, so resolve_cached must MISS the
        // generation-1 warmed entry and recompute against the real membership.
        let room_g2 = Arc::new(members(&[1, 2, 3]));
        let fresh =
            store.resolve_cached(1, &room_g2, 2, &speakers, 0, &RemotePublishers::default());
        assert!(
            fresh.video.contains_key(&3),
            "resolve_cached must recompute (not serve the stale gen-1 entry) when \
             the live generation moved past the warmed one"
        );
    }

    /// vc-zexm: re-warming preserves the visible-tile cap for a
    /// `receive_all_video` receiver. After a low-id member joins, the warmed
    /// capped set must reflect the new membership-derived selection (sorted by
    /// SessionId), proving the warm-up runs the SAME resolution logic as the
    /// hot path.
    #[test]
    fn rewarm_preserves_visible_tile_cap_for_receive_all_video() {
        let mut store = SubscriptionStore::new();
        let speakers: Arc<Vec<SessionId>> = Arc::new(vec![]);

        // Room: receiver 1 + senders 20..26 (6 senders) → exactly fills the cap.
        let mut all: Vec<SessionId> = (20..26).collect();
        all.push(1);
        let room_g0 = Arc::new(members(&all));
        store.apply_update(1, update_all(&[], vec![], false, true), &room_g0);
        let before =
            store.resolve_cached(1, &room_g0, 0, &speakers, 0, &RemotePublishers::default());
        let mut got_before: Vec<SessionId> = before.video.keys().copied().collect();
        got_before.sort_unstable();
        assert_eq!(got_before, vec![20, 21, 22, 23, 24, 25]);

        // A LOW-id member (5) joins. It must displace the highest-id member
        // (25) from the capped visible set after the re-warm.
        let mut all2 = all.clone();
        all2.push(5);
        let room_g1 = Arc::new(members(&all2));
        store.rewarm_cache(&room_g1, 1, &speakers, 0, &RemotePublishers::default());

        let after =
            store.resolve_cached(1, &room_g1, 1, &speakers, 0, &RemotePublishers::default());
        assert_eq!(after.video.len(), MAX_VISIBLE_VIDEO as usize);
        let mut got_after: Vec<SessionId> = after.video.keys().copied().collect();
        got_after.sort_unstable();
        assert_eq!(
            got_after,
            vec![5, 20, 21, 22, 23, 24],
            "low-id joiner must enter the capped visible set, displacing the highest id"
        );
    }

    /// vc-zexm: re-warming a legacy-default receiver folds in cross-pod
    /// publishers exactly as the hot path does (vc-54j2 preserved).
    #[test]
    fn rewarm_preserves_cross_pod_publisher_folding() {
        let store = SubscriptionStore::new();
        let speakers: Arc<Vec<SessionId>> = Arc::new(vec![]);
        let room = Arc::new(members(&[1, 2]));

        // Prime at gen 0 with no remote publishers.
        let _ = store.resolve_cached(1, &room, 0, &speakers, 0, &RemotePublishers::default());

        // A cross-pod publisher appears → registry change bumps the shared
        // generation to 1. Re-warm with the publisher in the snapshot.
        let pubs = remote(&[999], &[999]);
        store.rewarm_cache(&room, 1, &speakers, 1, &pubs);

        let warmed = store.resolve_cached(1, &room, 1, &speakers, 1, &pubs);
        assert!(
            warmed.video.contains_key(&999),
            "cross-pod video publisher must be folded into the warmed AllowSet"
        );
        assert!(warmed.audio.contains(&999));
    }

    /// vc-zexm: an explicit (non-receive-all) subscription is warmed using its
    /// real `sub_version`, so its pin/slot selection is preserved across the
    /// re-warm and the next resolve is a hit.
    #[test]
    fn rewarm_preserves_explicit_subscription() {
        let mut store = SubscriptionStore::new();
        let speakers: Arc<Vec<SessionId>> = Arc::new(vec![]);
        let room_g0 = Arc::new(members(&[1, 2, 3]));
        store.apply_update(1, update(&[2], vec![], false), &room_g0);
        let _ = store.resolve_cached(1, &room_g0, 0, &speakers, 0, &RemotePublishers::default());

        // Member 4 joins (not pinned by receiver 1). Re-warm.
        let room_g1 = Arc::new(members(&[1, 2, 3, 4]));
        store.rewarm_cache(&room_g1, 1, &speakers, 0, &RemotePublishers::default());

        let warmed =
            store.resolve_cached(1, &room_g1, 1, &speakers, 0, &RemotePublishers::default());
        let again =
            store.resolve_cached(1, &room_g1, 1, &speakers, 0, &RemotePublishers::default());
        assert!(
            Arc::ptr_eq(&warmed, &again),
            "explicit receiver must hit post-rewarm"
        );
        // Pin-only subscription: only 2 is visible — the unrelated joiner 4 is
        // NOT smuggled in.
        assert_eq!(warmed.video.len(), 1);
        assert!(warmed.video.contains_key(&2));
        assert!(!warmed.video.contains_key(&4));
    }

    /// vc-zexm: `rewarm_cache` on an empty cache is a cheap no-op (the common
    /// case for the first joiner, whose cache is still cold).
    #[test]
    fn rewarm_empty_cache_is_noop() {
        let store = SubscriptionStore::new();
        let room = Arc::new(members(&[1, 2]));
        let speakers: Arc<Vec<SessionId>> = Arc::new(vec![]);
        store.rewarm_cache(&room, 1, &speakers, 0, &RemotePublishers::default());
        assert!(store.cache.is_empty());
    }
}
