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

use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use tokio::sync::watch;

use videocall_types::protos::media_packet::media_packet::MediaType;
use videocall_types::protos::media_packet::MediaPacket;
use videocall_types::protos::packet_wrapper::packet_wrapper::PacketType;
use videocall_types::protos::packet_wrapper::PacketWrapper;

use crate::actors::session_logic::SessionId;
use crate::metrics::{
    SFU_DECIDE_LATENCY_US, SFU_DROPPED_TOTAL, SFU_FORWARDED_TOTAL, SFU_ROOM_SIZE,
};
use crate::sfu::room_state::RoomState;
use crate::sfu::speaker::ActiveSpeakerSet;
use crate::sfu::subscription::SubscriptionStore;

/// Map a `PacketWrapper.packet_type` (an `EnumOrUnknown<PacketType>`) to a
/// stable lowercase string suitable for use as a Prometheus label value.
///
/// Unknown / unrecognized enum values map to `"unknown"`.
fn packet_type_label(pw: &PacketWrapper) -> &'static str {
    match pw.packet_type.enum_value_or_default() {
        PacketType::PACKET_TYPE_UNKNOWN => "unknown",
        PacketType::RSA_PUB_KEY => "rsa_pub_key",
        PacketType::AES_KEY => "aes_key",
        PacketType::MEDIA => "media",
        PacketType::CONNECTION => "connection",
        PacketType::DIAGNOSTICS => "diagnostics",
        PacketType::HEALTH => "health",
        PacketType::MEETING => "meeting",
        PacketType::SESSION_ASSIGNED => "session_assigned",
        PacketType::CONGESTION => "congestion",
        PacketType::SUBSCRIPTION_UPDATE => "subscription_update",
        PacketType::SPEAKER_UPDATE => "speaker_update",
        PacketType::LAYER_HINT => "layer_hint",
        PacketType::ADMISSION_DECISION => "admission_decision",
        PacketType::CAPABILITY_ANNOUNCE => "capability_announce",
    }
}

/// Decision returned by the [`Forwarder`] for a single (packet, receiver) pair.
///
/// The pass-through hot path no longer carries bytes: the caller already
/// has the on-wire NATS payload in scope and reuses it directly on
/// `Forward`, avoiding a per-receiver re-serialization of an identical
/// `PacketWrapper`.
pub enum ForwardDecision {
    /// Forward to the receiver. The caller is responsible for supplying
    /// the bytes (typically the original NATS payload — no re-encoding).
    Forward,
    /// Drop the packet for this receiver.
    Drop,
}

/// Per-room forwarder.
///
/// Holds shared handles to the authoritative [`RoomState`], the
/// [`SubscriptionStore`] (per-receiver declarative subscription state), and a
/// `watch::Receiver` over the room's [`ActiveSpeakerSet`]. `decide` is invoked
/// from each receiver's NATS subscription callback and takes a read lock on
/// each handle only long enough to evaluate policy — callers must keep that
/// work cheap.
///
/// Wave-3 (p3-5) consults the AllowSet to filter MEDIA packets per receiver:
/// AUDIO MediaPackets are dropped when the sender is not in `allow.audio` and
/// VIDEO/SCREEN MediaPackets are dropped when the sender is not in
/// `allow.video`. A receiver that has never sent a `SubscriptionUpdate` is
/// returned the legacy-default AllowSet by [`SubscriptionStore::resolve`]
/// (every other room member, base layer), preserving legacy fan-out for
/// clients that don't yet declare subscriptions.
///
/// Non-MEDIA packet types (CONNECTION, CONGESTION, MEETING, …) and unparseable
/// MEDIA inner payloads fall through to the pre-p3-5 pass-through behavior:
/// the only mandatory drop is the sender's own echo (self-skip).
///
/// Layer-aware filtering (skipping enhancement layers based on `LayerPref`) is
/// the P4 layer selector and is intentionally out of scope here.
pub struct Forwarder {
    room: Arc<RwLock<RoomState>>,
    subscriptions: Arc<RwLock<SubscriptionStore>>,
    speakers: watch::Receiver<ActiveSpeakerSet>,
}

impl Forwarder {
    /// Full constructor used by the chat-server fan-out path.
    ///
    /// `speakers` is a `watch::Receiver` so reads are lock-free and synchronous
    /// — `decide` runs on every (packet, receiver) pair and must stay cheap.
    pub fn new(
        room: Arc<RwLock<RoomState>>,
        subscriptions: Arc<RwLock<SubscriptionStore>>,
        speakers: watch::Receiver<ActiveSpeakerSet>,
    ) -> Self {
        Self {
            room,
            subscriptions,
            speakers,
        }
    }

    /// Convenience constructor for tests and the in-crate parity helpers that
    /// don't materialise a real subscription store or speaker tick.
    ///
    /// Wires in an empty [`SubscriptionStore`] (every receiver resolves to the
    /// legacy-default AllowSet, preserving pre-p3-5 behavior) and a freshly
    /// constructed `watch` channel seeded with [`ActiveSpeakerSet::empty`].
    /// The sender half is leaked so the receiver stays open for the
    /// forwarder's lifetime — tests don't need the sender. Gated to
    /// `#[cfg(test)]` so production code paths cannot accidentally leak a
    /// `watch::Sender`; chat_server uses [`Forwarder::new`] with an
    /// actor-owned sender retained in `ChatServer::speakers`.
    #[cfg(test)]
    pub fn with_room_only(room: Arc<RwLock<RoomState>>) -> Self {
        let subscriptions = Arc::new(RwLock::new(SubscriptionStore::new()));
        let (tx, rx) = watch::channel(ActiveSpeakerSet::empty());
        // Keep the sender alive for the forwarder's lifetime so `borrow()`
        // never returns the closed-channel sentinel value. Leaking here is
        // intentional: callers of `with_room_only` are short-lived (tests).
        std::mem::forget(tx);
        Self {
            room,
            subscriptions,
            speakers: rx,
        }
    }

    /// Per-receiver decision for `receiver_sid`.
    ///
    /// Order of evaluation (each drop bumps `sfu_dropped_total{reason=…}`):
    ///
    /// 1. **Self-skip** — sender == receiver → `Drop{self_skip}`.
    /// 2. **MEDIA filtering** (only for `PacketWrapper.packet_type == MEDIA`
    ///    with a successfully parsed inner `MediaPacket`):
    ///    * Resolve the receiver's [`crate::sfu::subscription::AllowSet`]
    ///      from the current room membership + speaker set.
    ///    * `MediaType::AUDIO`  — drop iff sender not in `allow.audio`.
    ///    * `MediaType::VIDEO` / `SCREEN` — drop iff sender not in `allow.video`.
    ///    * Other media types (HEARTBEAT, RTT, KEYFRAME_REQUEST) and unknown
    ///      values pass through — they are control / signalling streams that
    ///      are not subject to subscription filtering.
    /// 3. **Anything else** — non-MEDIA wrappers or MEDIA wrappers whose
    ///    inner payload didn't parse — forwarded as-is, preserving the
    ///    tolerant pre-p3-5 behavior. The CONGESTION carve-out is enforced
    ///    one layer above (`chat_server::egress_decide_from_parsed`).
    ///
    /// All metric updates and the latency histogram observation happen before
    /// the function returns; the room's read lock is held only for the
    /// duration of the membership snapshot and gauge refresh.
    pub fn decide(
        &self,
        receiver_sid: SessionId,
        packet_wrapper: &PacketWrapper,
        media_packet: Option<&MediaPacket>,
    ) -> ForwardDecision {
        let start = std::time::Instant::now();

        // Snapshot the room membership and refresh the size gauge in one
        // critical section, then drop the room read lock before doing any
        // other work. The lock is poison-safe: a panicked writer leaves the
        // table readable.
        let members_snapshot: HashSet<SessionId> = {
            let room = match self.room.read() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            SFU_ROOM_SIZE
                .with_label_values(&[room.room_id.as_str()])
                .set(room.member_count() as f64);
            room.members.keys().copied().collect()
        };

        // 1. Self-skip — sender is the receiver itself.
        if packet_wrapper.session_id == receiver_sid {
            SFU_DROPPED_TOTAL.with_label_values(&["self_skip"]).inc();
            observe_decide_latency(start);
            return ForwardDecision::Drop;
        }

        // 2. AllowSet filter for MEDIA packets with a parsed inner MediaPacket.
        let is_media = packet_wrapper.packet_type == PacketType::MEDIA.into();
        if is_media {
            if let Some(mp) = media_packet {
                let media_type = mp.media_type.enum_value_or_default();
                let needs_filter = matches!(
                    media_type,
                    MediaType::AUDIO | MediaType::VIDEO | MediaType::SCREEN
                );
                if needs_filter {
                    // Lock-free read of the current speaker set.
                    let speakers_top: Vec<SessionId> = self.speakers.borrow().top.clone();
                    let allow = {
                        let store = match self.subscriptions.read() {
                            Ok(g) => g,
                            Err(poisoned) => poisoned.into_inner(),
                        };
                        store.resolve(receiver_sid, &members_snapshot, &speakers_top)
                    };

                    let sender_sid = packet_wrapper.session_id;
                    let allowed = match media_type {
                        MediaType::AUDIO => allow.audio.contains(&sender_sid),
                        // SCREEN rides the same allow tier as VIDEO (it's a
                        // visual stream; the subscription model has no
                        // SCREEN-specific tier today).
                        MediaType::VIDEO | MediaType::SCREEN => {
                            allow.video.contains_key(&sender_sid)
                        }
                        _ => true,
                    };
                    if !allowed {
                        SFU_DROPPED_TOTAL.with_label_values(&["unsubscribed"]).inc();
                        observe_decide_latency(start);
                        return ForwardDecision::Drop;
                    }
                }
            }
        }

        // 3. Forward.
        SFU_FORWARDED_TOTAL
            .with_label_values(&[packet_type_label(packet_wrapper)])
            .inc();
        observe_decide_latency(start);
        ForwardDecision::Forward
    }
}

fn observe_decide_latency(start: std::time::Instant) {
    let elapsed_us = start.elapsed().as_micros() as f64;
    SFU_DECIDE_LATENCY_US.observe(elapsed_us);
}
