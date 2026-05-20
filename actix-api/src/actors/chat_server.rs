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

use crate::{
    constants::{
        MAX_PARTICIPANTS_ENV, MAX_PARTICIPANTS_PER_ROOM, RECONNECT_GRACE_PERIOD,
        WAITING_ROOM_THRESHOLD, WAITING_ROOM_THRESHOLD_ENV,
    },
    messages::{
        server::{ActivateConnection, ClientMessage, Connect, Disconnect, JoinRoom, Leave},
        session::Message,
    },
    models::build_subject_and_queue,
    session_manager::{SessionEndResult, SessionManager},
};

use actix::{
    Actor, AsyncContext, Context, Handler, Message as ActixMessage, MessageResult, Recipient,
    SpawnHandle,
};
use futures::StreamExt;
use protobuf::Message as ProtobufMessage;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, trace, warn};
use videocall_types::protos::admission_decision_packet::admission_decision::Status as AdmissionStatus;
use videocall_types::protos::diagnostics_packet::DiagnosticsPacket;
use videocall_types::protos::packet_wrapper::packet_wrapper::PacketType;
use videocall_types::protos::packet_wrapper::PacketWrapper;
use videocall_types::SYSTEM_USER_ID;

use super::packet_handler::{
    parse_and_inspect, should_drop_kfr_for_layer_selection, PacketKind, ParsedPacket,
};
use super::session_logic::{ConnectionState, SessionId};
use crate::sfu::forwarder::{ForwardDecision, Forwarder};
use crate::sfu::health_beacon::{
    spawn_beacon_hub, BeaconHub, EnvOwnerCheck, LinuxCpuLoad, NatsHealthBeaconPublisher,
};
use crate::sfu::layer_selector::LayerSelector;
use crate::sfu::room_state::RoomState;
use crate::sfu::speaker::{NatsSpeakerPublisher, SpeakerScorer, SpeakerTick, TickHandle};
use crate::sfu::spillover::{spawn_spillover_ingest, SpilloverIngestHandle, SpilloverStore};
use crate::sfu::subscription::SubscriptionStore;
use crate::sfu::{SfuConfig, SfuMode};
use tokio::sync::RwLock as TokioRwLock;
use videocall_types::protos::media_packet::media_packet::MediaType;
use videocall_types::protos::media_packet::MediaPacket;
use videocall_types::protos::subscription_packet::SubscriptionUpdate;

/// Cached transport-family identifier for the wave-3 ADMISSION_DECISION
/// {REDIRECT} DNS template (bead vc-8oa / p6-5).
///
/// Resolved exactly once from the `SFU_TRANSPORT_KIND` env var on first
/// JoinRoom (after the binary's startup sets it; see
/// `actix-api/src/bin/{webtransport,websocket}_server.rs`). Stored in an
/// `OnceLock<String>` so subsequent JoinRoom handlers never re-touch the
/// env. Without caching, every join paid an `env::var` lookup and the
/// default-on-miss silently masked a misconfigured deployment — the
/// startup log makes the warning visible at the right time instead.
static SFU_TRANSPORT_KIND_CACHE: OnceLock<String> = OnceLock::new();

/// Resolve the cached `SFU_TRANSPORT_KIND`, initialising it from the env
/// on first call. Defaults to `"webtransport"` if the env var is missing —
/// the binaries unconditionally set this at startup so the default only
/// fires in tests / misconfigured deployments. The unset-at-runtime branch
/// emits a one-shot warning so the misconfiguration is observable in logs
/// without spamming every JoinRoom.
fn sfu_transport_kind() -> &'static str {
    SFU_TRANSPORT_KIND_CACHE
        .get_or_init(|| match std::env::var("SFU_TRANSPORT_KIND") {
            Ok(v) => v,
            Err(_) => {
                warn!(
                    "SFU_TRANSPORT_KIND not set; defaulting to \"webtransport\" for \
                     ADMISSION_DECISION{{REDIRECT}} DNS. The transport binaries set \
                     this at startup — if you see this in production, the env wiring \
                     is broken."
                );
                "webtransport".to_string()
            }
        })
        .as_str()
}

/// Internal message sent via `notify_later` after the reconnection grace period
/// expires. If the user has not reconnected by the time this message is handled,
/// the actual `leave_rooms()` + PARTICIPANT_LEFT flow executes.
#[derive(ActixMessage)]
#[rtype(result = "()")]
struct ExecutePendingDeparture {
    session: SessionId,
    room: String,
    user_id: String,
    display_name: String,
}

/// State stored while a departure is pending (waiting for possible reconnection).
struct PendingDepartureState {
    /// Handle returned by `ctx.notify_later()`, used to cancel the delayed
    /// `ExecutePendingDeparture` message if the user reconnects in time.
    spawn_handle: SpawnHandle,
    /// The old session ID that disconnected — used for cleanup.
    old_session: SessionId,
    /// Whether the disconnecting session had been activated (Testing -> Active).
    /// Only Active sessions should trigger PARTICIPANT_LEFT, because only Active
    /// sessions had their PARTICIPANT_JOINED broadcast. Testing sessions (e.g.,
    /// the losing connection during RTT election) never announced themselves.
    was_active: bool,
}

/// Per-room demux state (vc-q0v).
///
/// Replaces the pre-vc-q0v *per-session* NATS subscription model. A single
/// queue-less `subscribe` runs against `room.<room>.*` for the lifetime of the
/// room. Every inbound message is parsed exactly once via
/// [`parse_and_inspect`] and fanned out to each receiver in `receivers` via
/// [`egress_decide_from_parsed`]. Joins update `receivers` under a write lock;
/// the demux task takes a read lock per inbound message.
///
/// When the last receiver leaves, the dispatcher task is aborted and the
/// `RoomDispatch` entry is removed. This mirrors the lifecycle of
/// `room_states` / `forwarders` so per-room SFU state and per-room NATS
/// subscriptions tear down together.
struct RoomDispatch {
    /// Live receivers in the room, keyed by `SessionId`. The demux task
    /// snapshots this map per-message so writes (join/leave) only block
    /// briefly under a write lock.
    receivers: Arc<RwLock<HashMap<SessionId, Recipient<Message>>>>,
    /// JoinHandle for the per-room subscription loop. Aborted when the
    /// room drains so the task exits at its next `.await` and drops its
    /// `Arc<Forwarder>` reference.
    ///
    /// Must be `.abort()`-ed before drop: tokio `JoinHandle` *detaches*
    /// on drop, it does not cancel. Replacing or removing this field
    /// without first aborting the prior handle leaks the task.
    task: JoinHandle<()>,
}

/// Internal message: posted by the per-room demux task when its
/// subscription loop exits unexpectedly (initial subscribe failed or
/// `sub.next()` returned `None` because NATS closed the subscription).
/// Normal teardown via [`ChatServer::drop_room_receiver`] aborts the task
/// *before* sending — the handler distinguishes the two cases by checking
/// whether `room_dispatch` still holds the entry.
///
/// Without this signal, a dispatcher dying mid-flight would leave a
/// `RoomDispatch` entry in `room_dispatch` whose receivers map is still
/// populated; subsequent joiners would register into that orphan map and
/// silently receive zero media for the lifetime of the room. The handler
/// recovers by respawning the dispatcher when receivers are still present.
#[derive(ActixMessage)]
#[rtype(result = "()")]
struct RoomDispatcherExited {
    room: String,
}

/// Internal message: posted by the spawned NATS-KV lookup task once a
/// room's home region has been resolved (bead vc-hc8 / p6-9). Carries the
/// information needed for the actor to either:
///   - populate the synchronous `home_region_cache` and stop (in-region), OR
///   - emit `ADMISSION_DECISION{REDIRECT}` to the session and force a
///     `Disconnect`, because the room's home region is elsewhere.
///
/// We send this message back to the actor (rather than mutating cache
/// directly from the spawned task) so all `home_region_cache` writes
/// happen on a single thread of execution — keeping the cache mutation
/// path identical to the rest of `ChatServer`'s state and avoiding any
/// need for interior locking on what is otherwise plain actor state.
#[derive(ActixMessage)]
#[rtype(result = "()")]
struct HomeRegionResolved {
    /// Room the lookup was performed for.
    room: String,
    /// Resolved home region (`region` if we won the CAS, else the
    /// previously-stored value).
    home_region: String,
    /// Session that triggered the lookup. If still registered AND the home
    /// region differs from this pod's region, the handler emits the
    /// REDIRECT packet and disconnects this session. If the session is
    /// gone (already disconnected) the handler just populates the cache.
    session: SessionId,
    /// User id from the original JoinRoom — used for log breadcrumbs and
    /// to send a `Disconnect` after the redirect packet so the pending-
    /// departure / leave path runs identically to a normal disconnect.
    user_id: String,
    /// Display name from the original JoinRoom (mirrors user_id semantics).
    display_name: String,
}

pub struct ChatServer {
    nats_connection: async_nats::client::Client,
    sessions: HashMap<SessionId, Recipient<Message>>,
    /// Sessions that have completed `JoinRoom`. Used solely for the
    /// duplicate-join short-circuit in the `JoinRoom` handler; the actual
    /// receiver-side mailbox lives in [`RoomDispatch::receivers`].
    joined_sessions: HashSet<SessionId>,
    session_manager: SessionManager,
    connection_states: HashMap<SessionId, ConnectionState>,
    /// Track which sessions are in which room, with their user_id and display_name.
    /// Used to send PARTICIPANT_JOINED for existing peers to new joiners.
    room_members: HashMap<String, Vec<(SessionId, String, String)>>,
    /// Pending departures keyed by `(room_id, user_id)`. When a session disconnects
    /// we defer the PARTICIPANT_LEFT broadcast by [`RECONNECT_GRACE_PERIOD`]. If the
    /// same user reconnects before the timer fires, the departure is cancelled
    /// silently — no PARTICIPANT_LEFT or PARTICIPANT_JOINED is sent.
    pending_departures: HashMap<(String, String), PendingDepartureState>,
    /// Sessions that should NOT have PARTICIPANT_JOINED broadcast at activation.
    /// This is used for reconnection sessions: the user never "left" from peers'
    /// perspective, so announcing a "join" would be misleading.
    suppress_join_broadcast: std::collections::HashSet<SessionId>,
    /// SFU runtime configuration, snapshotted from `SFU_MODE` at actor
    /// construction. The startup binaries log the mode separately; reading
    /// the env again here is intentional so the actor owns its own copy.
    sfu_config: SfuConfig,
    /// Per-room authoritative SFU state. Lazily inserted in `JoinRoom` and
    /// removed in the same code paths that clean up `room_members`. Wrapped
    /// in `Arc<RwLock<_>>` so the per-receiver `Forwarder::decide` calls
    /// running inside the NATS subscriber task can read it concurrently
    /// while p2-6 adds member-table mutations from the actor thread.
    room_states: HashMap<String, Arc<RwLock<RoomState>>>,
    /// Per-room forwarder, cheaply cloned (via `Arc`) into the per-room
    /// dispatcher task so the forwarder outlives the actor handler's stack
    /// frame.
    forwarders: HashMap<String, Arc<Forwarder>>,
    /// Per-room declarative subscription state (p3-4). Receivers publish
    /// `SubscriptionUpdate` packets which the `ClientMessage` handler intercepts
    /// before NATS publish and applies via `SubscriptionStore::apply_update`.
    /// The forwarder reads this store on every `decide()` to determine which
    /// senders' MEDIA may be forwarded to each receiver.
    subscriptions: HashMap<String, Arc<RwLock<SubscriptionStore>>>,
    /// Per-room shared scorer (p3-11 wiring).
    ///
    /// The per-room dispatcher task observes each inbound AUDIO
    /// `MediaPacket`'s `RoutingHeader.audio_level` / `is_speaking` hint into
    /// this scorer; the per-room [`SpeakerTick`] reads it on its 200ms
    /// cadence to compute the [`ActiveSpeakerSet`] snapshot consumed by the
    /// forwarder and broadcast to clients on `room.{room}.system`.
    ///
    /// Stored alongside `speaker_ticks` (one-to-one), but kept in its own
    /// map so the dispatcher can clone an `Arc` without touching the tick
    /// handle's lifecycle.
    speaker_scorers: HashMap<String, Arc<TokioRwLock<SpeakerScorer>>>,
    /// Per-room speaker tick. Holds the join handle returned by
    /// [`SpeakerTick::run`]; dropping the handle aborts the background
    /// task. Torn down in the same code paths that remove `speaker_scorers`
    /// and `room_states` when a room drains.
    ///
    /// The tick is also responsible for publishing `SpeakerUpdate`
    /// `PacketWrapper`s on `room.{room}.system` via its embedded
    /// [`NatsSpeakerPublisher`]. The tick internally owns the
    /// `watch::Sender<ActiveSpeakerSet>` that the forwarder's
    /// `watch::Receiver` reads from, so the channel stays open for as long
    /// as the tick handle is retained here.
    speaker_ticks: HashMap<String, TickHandle>,
    /// Single owner-pod health-beacon hub (vc-kol / p6-7, refactored in
    /// vc-c6l from N per-room tasks to one). Registered with the room id
    /// alongside the speaker tick, but ONLY when this pod is the room's
    /// owner per [`crate::sfu::affinity::is_owner`]. Unregistered in the
    /// same code paths that remove `speaker_ticks`; the `Drop` impl on
    /// [`BeaconHub`] aborts the background task on shutdown.
    ///
    /// Non-owner pods never register any rooms — they silently consume
    /// the beacons the owner publishes on `room.{room}.system` (p6-8,
    /// wave 3).
    beacon_hub: BeaconHub,
    /// Per-room demux state (one NATS subscription per room, fanned out to
    /// all local receivers). Lazily created on the first `JoinRoom` for a
    /// room and torn down when the room drains. See [`RoomDispatch`].
    room_dispatch: HashMap<String, RoomDispatch>,
    /// Synchronous in-actor cache of room → home-region (bead vc-hc8 / p6-9).
    ///
    /// On JoinRoom: if a cached entry exists, the cross-region redirect
    /// decision is made synchronously up-front. On miss, a background
    /// task performs the NATS-KV CAS and posts a [`HomeRegionResolved`]
    /// message back to the actor which then populates this cache (and,
    /// if the home region differs, emits a REDIRECT and disconnects the
    /// session).
    ///
    /// Race window (acceptable per v1 / "accept 250ms RTT penalty" in the
    /// bead): the very first joiner for a room already homed in another
    /// region pays a brief admission-then-redirect instead of an
    /// up-front redirect. The client handles the REDIRECT packet the same
    /// way (see `ConnectionManager` p6-6 / vc-mv3); the visible difference
    /// is one extra round-trip and a short admit window.
    home_region_cache: HashMap<String, String>,
    /// Backing store for [`Self::home_region_cache`] population. Either a
    /// [`crate::sfu::affinity::NatsRegionKv`] when JetStream is reachable
    /// at startup, or a [`crate::sfu::affinity::NoopRegionKv`] fallback —
    /// the noop never returns a foreign region, so a JetStream outage
    /// silently degrades to single-region behaviour rather than failing
    /// every JoinRoom.
    home_region_kv: Arc<dyn crate::sfu::affinity::RegionKv>,
    /// Process-wide snapshot of every room's owner-pod health beacon
    /// (bead vc-85p / p6-5 wiring of the p6-8 store). Populated by the
    /// background ingest task spawned in [`ChatServer::new`], which
    /// subscribes to `room.*.system` and records the most recent
    /// `participant_count` + `cpu_load` per room.
    ///
    /// Consulted on the non-owner JoinRoom path: when
    /// [`SpilloverStore::is_spilled_over`] is `true` for the joined room
    /// (owner over threshold AND a fresh beacon), the non-owner pod admits
    /// the joiner LOCALLY (spill) instead of redirecting to the owner.
    /// Otherwise behaviour is unchanged — redirect to the owner.
    spillover_store: SpilloverStore,
    /// Retains the [`spawn_spillover_ingest`] task for the actor's
    /// lifetime. The handle's `Drop` aborts the background subscription on
    /// shutdown; holding it here (rather than dropping the return value)
    /// keeps the ingest task alive so the store stays populated. Never read
    /// directly — its sole purpose is lifetime ownership.
    _spillover_ingest: SpilloverIngestHandle,
}

impl ChatServer {
    pub async fn new(nats_connection: async_nats::client::Client) -> Self {
        // vc-c6l: a single owner-pod hub replaces the previous N per-room
        // tasks. The hub always runs (1 task, 1 timer) and stays empty on
        // non-owner pods. Eager-init avoids special-casing `ChatServer::new`
        // for the rare case of a pod that never owns a room.
        let beacon_publisher = Arc::new(NatsHealthBeaconPublisher::new(nats_connection.clone()));
        let beacon_hub = spawn_beacon_hub(
            Arc::new(EnvOwnerCheck),
            Arc::new(LinuxCpuLoad),
            beacon_publisher,
        );
        // vc-hc8 (p6-9): try to attach to the JetStream KV bucket that
        // tracks each room's home region. If JetStream isn't enabled on
        // this NATS cluster (single-node dev, or a misconfigured prod),
        // fall back to a no-op KV that effectively disables cross-region
        // redirects — the SFU still functions in single-region mode.
        // A failure here MUST NOT block actor startup: ChatServer is the
        // root of the system, and refusing to start because cross-region
        // pinning isn't available would be a strict regression vs. p6-8.
        let home_region_kv: Arc<dyn crate::sfu::affinity::RegionKv> =
            match crate::sfu::affinity::NatsRegionKv::connect(nats_connection.clone()).await {
                Ok(kv) => Arc::new(kv),
                Err(e) => {
                    warn!(
                        "p6-9 home-region KV unavailable ({}); cross-region \
                         redirects DISABLED. Single-region behaviour preserved.",
                        e
                    );
                    Arc::new(crate::sfu::affinity::NoopRegionKv)
                }
            };
        // vc-85p (p6-5): spawn the spillover beacon-ingest task so the
        // non-owner JoinRoom path can consult fresh owner-pod health. The
        // task subscribes to `room.*.system`, decodes `HEALTH_BEACON`
        // packets, and populates `spillover_store`. It runs on its own
        // tokio task and never blocks the packet-forwarding hot path. We
        // retain the returned handle so the task is not aborted when the
        // return value would otherwise be dropped. Reusing the actor's
        // existing NATS client keeps a single connection per pod.
        let spillover_store = SpilloverStore::new();
        let spillover_ingest =
            spawn_spillover_ingest(nats_connection.clone(), spillover_store.clone());
        ChatServer {
            nats_connection,
            joined_sessions: HashSet::new(),
            sessions: HashMap::new(),
            session_manager: SessionManager::new(),
            connection_states: HashMap::new(),
            room_members: HashMap::new(),
            pending_departures: HashMap::new(),
            suppress_join_broadcast: HashSet::new(),
            sfu_config: SfuConfig::from_env(),
            room_states: HashMap::new(),
            forwarders: HashMap::new(),
            subscriptions: HashMap::new(),
            speaker_scorers: HashMap::new(),
            speaker_ticks: HashMap::new(),
            beacon_hub,
            room_dispatch: HashMap::new(),
            home_region_cache: HashMap::new(),
            home_region_kv,
            spillover_store,
            _spillover_ingest: spillover_ingest,
        }
    }

    pub fn leave_rooms(
        &mut self,
        session_id: &SessionId,
        room: Option<&str>,
        user_id: Option<&str>,
        display_name: Option<&str>,
        observer: bool,
        was_active: bool,
    ) {
        // Drop the session marker. The session's receiver entry in the
        // per-room demux is removed below (we need the room id for that).
        self.joined_sessions.remove(session_id);

        // Remove from room_members tracking
        if let Some(room_id) = room {
            // p2-6: drop the SFU member entry first. We do this before the
            // room_members emptiness check below because the room_states
            // map entry may itself be removed in the same pass.
            if let Some(state) = self.room_states.get(room_id) {
                let mut guard = match state.write() {
                    Ok(g) => g,
                    Err(poisoned) => poisoned.into_inner(),
                };
                guard.remove_member(*session_id);
            }
            // vc-q0v: remove this session from the per-room demux receiver
            // map. When the room's receiver map empties, abort the
            // dispatcher task so its `Arc<Forwarder>` reference is dropped
            // and the per-room subscription closes.
            self.drop_room_receiver(room_id, session_id);

            // Drop the receiver's declarative subscription state (if any) so
            // a future session reusing this `SessionId` starts from the
            // legacy default. Best-effort: missing entries are silently
            // ignored.
            if let Some(store) = self.subscriptions.get(room_id) {
                let mut guard = match store.write() {
                    Ok(g) => g,
                    Err(poisoned) => poisoned.into_inner(),
                };
                guard.forget(*session_id);
            }
            let room_torn_down = if let Some(members) = self.room_members.get_mut(room_id) {
                members.retain(|(sid, _, _)| sid != session_id);
                if members.is_empty() {
                    self.room_members.remove(room_id);
                    // Mirror room_members lifecycle for SFU state.
                    self.forwarders.remove(room_id);
                    self.room_states.remove(room_id);
                    self.subscriptions.remove(room_id);
                    // p3-11: dropping the TickHandle aborts the speaker
                    // tick task, which in turn drops its `watch::Sender`
                    // and closes the channel the (already-removed)
                    // forwarder's receiver was watching. Then drop the
                    // shared scorer.
                    self.speaker_ticks.remove(room_id);
                    self.speaker_scorers.remove(room_id);
                    // vc-kol / p6-7 (vc-c6l): unregister the room from the
                    // shared owner-pod beacon hub. Non-owner pods never
                    // registered; `unregister` is a safe no-op there.
                    self.beacon_hub.unregister(room_id);
                    true
                } else {
                    false
                }
            } else {
                false
            };
            // p4-7: a member leaving invalidates every cached layer
            // selection in the room (the departed member was a candidate
            // sender for other receivers and may have been pinned/admitted
            // in someone's selection). Skip when the room was torn down
            // above — the forwarder is already gone, so invalidating its
            // cache is dead work.
            //
            // vc-78q: additionally reap the departing session's per-pair
            // state from the forwarder — recent_t0 entries keyed by
            // (sid, *) / (*, sid) and the LayerSelector hysteresis +
            // cached selection keyed by `sid` as a receiver. Without
            // this, long-lived rooms accumulate ~2KB per pair indefinitely
            // as receivers and senders churn.
            if !room_torn_down {
                if let Some(fwd) = self.forwarders.get(room_id) {
                    // vc-wls: no lock to acquire — interior mutability
                    // is handled inside the selector (DashMap shard
                    // locks for `last_selections`, mutex for hysteresis).
                    fwd.layer_selector().invalidate_all();
                    fwd.prune_session(*session_id);
                }
            }
        }

        // End session using SessionManager
        if let (Some(room_id), Some(uid)) = (room, user_id) {
            let room_id = room_id.to_string();
            let user_id = uid.to_string();
            let display_name = display_name.unwrap_or(uid).to_string();
            let session_manager = self.session_manager.clone();
            let nc = self.nats_connection.clone();
            let session_id_val = *session_id;

            // Observer sessions (waiting room) should not publish PARTICIPANT_LEFT
            // since they were never real participants in the meeting.
            if observer {
                info!(
                    "Observer session {} for {} leaving room {} - skipping PARTICIPANT_LEFT",
                    session_id_val, user_id, room_id
                );
                tokio::spawn(async move {
                    if let Err(e) = session_manager.end_session(&room_id, &user_id).await {
                        error!("Error ending observer session for room {}: {}", room_id, e);
                    }
                });
                return;
            }

            // vc-9g7 follow-up: gate on the caller-supplied `was_active`
            // snapshot. The previous in-band lookup against
            // `self.connection_states` was racy: every caller in this file
            // removes the session's `connection_states` entry BEFORE
            // invoking `leave_rooms` (e.g. `Handler<Disconnect>` at the top
            // of its body), so the lookup always returned `None` and the
            // gate became a no-op — letting PARTICIPANT_LEFT fire for
            // sessions that never reached `Active`. Callers now pass the
            // value they captured before mutating state.
            if !was_active {
                info!(
                    "Skipping PARTICIPANT_LEFT for non-active session {}",
                    session_id
                );
                return;
            }

            tokio::spawn(async move {
                match session_manager.end_session(&room_id, &user_id).await {
                    Ok(SessionEndResult::HostEndedMeeting) => {
                        info!(
                            "Host {} left room {} - ending meeting for all",
                            user_id, room_id
                        );
                        // Notify all participants using MEETING packet (protobuf)
                        let bytes = SessionManager::build_meeting_ended_packet(
                            &room_id,
                            "The host has ended the meeting",
                        );
                        let subject = format!("room.{}.system", room_id.replace(' ', "_"));
                        if let Err(e) = nc.publish(subject, bytes.into()).await {
                            error!("Error publishing MEETING_ENDED: {}", e);
                        }
                    }
                    Ok(SessionEndResult::LastParticipantLeft) => {
                        info!("Last participant {} left room {}", user_id, room_id);
                    }
                    Ok(SessionEndResult::MeetingContinues { remaining_count }) => {
                        info!(
                            "Participant {} left room {}, {} remaining",
                            user_id, room_id, remaining_count
                        );
                        // Notify remaining peers about the departed session
                        let bytes = SessionManager::build_peer_left_packet(
                            &room_id,
                            &user_id,
                            session_id_val,
                            &display_name,
                        );
                        let subject = format!("room.{}.system", room_id.replace(' ', "_"));
                        if let Err(e) = nc.publish(subject, bytes.into()).await {
                            error!("Error publishing PARTICIPANT_LEFT: {}", e);
                        }
                    }
                    Err(e) => {
                        error!("Error ending session for room {}: {}", room_id, e);
                    }
                }
            });
        }
    }

    /// Get the session manager (for use by chat_session)
    pub fn session_manager(&self) -> &SessionManager {
        &self.session_manager
    }

    /// Apply a `SubscriptionUpdate` payload from `receiver` in `room` (p3-5).
    ///
    /// Decodes `payload` as a [`SubscriptionUpdate`] and applies it to the
    /// room's [`SubscriptionStore`] against the current member set. Silently
    /// returns on:
    ///   * malformed payloads (parse error)
    ///   * unknown rooms (no `SubscriptionStore` materialised yet)
    ///   * missing room state (no member snapshot available)
    ///
    /// These are best-effort updates: a malformed packet does not break the
    /// existing subscription state, and a missing store cannot exist in
    /// practice because both are materialised together in `JoinRoom`.
    fn apply_subscription_update(&self, room: &str, receiver: SessionId, payload: &[u8]) {
        let update = match SubscriptionUpdate::parse_from_bytes(payload) {
            Ok(u) => u,
            Err(e) => {
                warn!(
                    "Dropping malformed SubscriptionUpdate from session {} in room {}: {}",
                    receiver, room, e
                );
                return;
            }
        };

        let store = match self.subscriptions.get(room) {
            Some(s) => s,
            None => return,
        };
        // vc-7gc: read the cached `Arc<HashSet>` instead of allocating a
        // fresh `HashSet` from `members.keys()`. The lock is released the
        // moment the read scope ends.
        let members = match self.room_states.get(room) {
            Some(state) => {
                let guard = match state.read() {
                    Ok(g) => g,
                    Err(poisoned) => poisoned.into_inner(),
                };
                guard.members_snapshot()
            }
            None => return,
        };
        let mut guard = match store.write() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.apply_update(receiver, update, &members);
        // p4-7 follow-up: a SubscriptionUpdate may change which senders this
        // receiver wants without bumping the speaker generation or arriving
        // alongside a bandwidth-estimate refresh. Invalidate the receiver's
        // cached layer selection so the next `decide()` call recomputes
        // against the new `AllowSet`.
        if let Some(fwd) = self.forwarders.get(room) {
            // vc-wls: lock-free per-key invalidation via DashMap.
            fwd.layer_selector().invalidate_for_receiver(receiver);
        }
    }

    /// Remove a session from the per-room demux receiver map (vc-q0v).
    ///
    /// If the room's receiver map becomes empty as a result, the dispatcher
    /// task is aborted and the `RoomDispatch` entry is dropped. Idempotent:
    /// safe to call when the room or session is already gone.
    fn drop_room_receiver(&mut self, room_id: &str, session_id: &SessionId) {
        let now_empty = {
            let dispatch = match self.room_dispatch.get(room_id) {
                Some(d) => d,
                None => return,
            };
            let mut guard = match dispatch.receivers.write() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            guard.remove(session_id);
            guard.is_empty()
        };
        if now_empty {
            if let Some(dispatch) = self.room_dispatch.remove(room_id) {
                dispatch.task.abort();
            }
        }
    }
}

impl Actor for ChatServer {
    type Context = Context<Self>;
}

impl Handler<Connect> for ChatServer {
    type Result = ();

    fn handle(&mut self, msg: Connect, _ctx: &mut Self::Context) -> Self::Result {
        let Connect { id, addr } = msg;
        self.sessions.insert(id, addr);
        self.connection_states.insert(id, ConnectionState::Testing);
    }
}

impl Handler<Disconnect> for ChatServer {
    type Result = ();

    fn handle(
        &mut self,
        Disconnect {
            session,
            room,
            user_id,
            display_name,
            observer,
            redirect,
        }: Disconnect,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        // Clean up session-level state immediately — the transport is gone.
        // Capture whether the session was Active before removing the state,
        // so we can store it in PendingDepartureState for the grace period.
        let was_active = self
            .connection_states
            .get(&session)
            .map(|s| *s == ConnectionState::Active)
            .unwrap_or(false);
        let _ = self.sessions.remove(&session);
        let _ = self.connection_states.remove(&session);
        let _ = self.suppress_join_broadcast.remove(&session);

        // Observers and non-active sessions bypass the grace period — they
        // never triggered PARTICIPANT_JOINED, so there is nothing to defer.
        if observer {
            // Observers short-circuit inside `leave_rooms` before the
            // `was_active` gate is consulted, so the value is academic
            // here — pass the real captured value for consistency rather
            // than a hard-coded literal.
            self.leave_rooms(
                &session,
                Some(&room),
                Some(&user_id),
                Some(&display_name),
                true,
                was_active,
            );
            return;
        }

        // vc-9g7 (p6-9 follow-up): cross-region redirect synthesizes a
        // Disconnect addressed to ourselves so the normal leave path runs.
        // The redirected client is being told to reconnect *to a different
        // pod in a different region* — it will NOT reconnect to this pod,
        // so the RECONNECT_GRACE_PERIOD deferral is pure dead time that
        // produces a ~2.25s ghost-participant window for cross-region peers
        // federated via NATS (PARTICIPANT_JOINED → PARTICIPANT_LEFT pair).
        //
        // Mirror what the deferred path does on entry (drop joined_sessions
        // and the per-room receiver) and then call `leave_rooms` directly
        // instead of going through ExecutePendingDeparture. The
        // PARTICIPANT_LEFT broadcast inside `leave_rooms` correctly gates
        // on whether the session ever reached Active: if it never did
        // (likely, given the KV-roundtrip window), the broadcast is
        // suppressed and no JOINED→LEFT pair exists; if it did, the LEFT
        // fires immediately rather than after the grace period. Either way
        // the ghost window collapses.
        //
        // `observer=false` because the joiner was admitted as a real
        // participant; the redirect flag is independent of observer-ness.
        if redirect {
            self.joined_sessions.remove(&session);
            self.drop_room_receiver(&room, &session);
            // Pass the `was_active` snapshot captured at the top of this
            // handler. In the realistic post-cache-miss timing the KV
            // roundtrip resolves before the client's CONNECTION packet
            // triggers `ActivateConnection`, so `was_active` is false and
            // `leave_rooms` correctly suppresses the spurious
            // PARTICIPANT_LEFT for a participant nobody saw join.
            self.leave_rooms(
                &session,
                Some(&room),
                Some(&user_id),
                Some(&display_name),
                false,
                was_active,
            );
            return;
        }

        // vc-q0v: drop this session from the per-room demux receiver map
        // immediately so the old (dead) recipient stops being targeted for
        // try_send fan-out. Keep room_members intact for now — they will be
        // cleaned up either on reconnection or when the grace period
        // expires. The per-room dispatcher task itself stays alive as long
        // as any receivers remain (and is aborted when the map empties).
        self.joined_sessions.remove(&session);
        self.drop_room_receiver(&room, &session);

        // If there is already a pending departure for this (room, user_id),
        // cancel the old timer and replace it. This handles the edge case of
        // rapid disconnect-reconnect-disconnect cycles.
        let key = (room.clone(), user_id.clone());
        if let Some(old) = self.pending_departures.remove(&key) {
            ctx.cancel_future(old.spawn_handle);
            info!(
                "Replaced existing pending departure for user {} in room {} (old session {})",
                user_id, room, old.old_session
            );
        }

        info!(
            "Deferring PARTICIPANT_LEFT for user {} (session {}) in room {} — \
             grace period {:?}",
            user_id, session, room, RECONNECT_GRACE_PERIOD
        );

        let handle = ctx.notify_later(
            ExecutePendingDeparture {
                session,
                room: room.clone(),
                user_id: user_id.clone(),
                display_name,
            },
            RECONNECT_GRACE_PERIOD,
        );

        self.pending_departures.insert(
            key,
            PendingDepartureState {
                spawn_handle: handle,
                old_session: session,
                was_active,
            },
        );
    }
}

impl Handler<Leave> for ChatServer {
    type Result = ();

    fn handle(
        &mut self,
        Leave {
            session,
            room,
            user_id,
        }: Leave,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        // vc-9g7 follow-up: capture `was_active` BEFORE any state mutation
        // (here it happens to be safe because Leave doesn't touch
        // `connection_states`, but reading up-front preserves the contract
        // expected by `leave_rooms`). Clients that issue an explicit Leave
        // before activating (e.g. very fast Testing-state departures) will
        // have `was_active == false` and PARTICIPANT_LEFT will be elided,
        // matching the prior in-band behaviour.
        let was_active = self
            .connection_states
            .get(&session)
            .map(|s| *s == ConnectionState::Active)
            .unwrap_or(false);

        // Cancel any pending departure for this (room, user_id) to avoid a
        // duplicate PARTICIPANT_LEFT when the grace-period timer fires later.
        // We don't need ctx.cancel_future() because ExecutePendingDeparture::handle
        // already checks whether the entry exists in pending_departures — once
        // removed, the timer becomes a no-op.
        let key = (room.clone(), user_id.clone());
        if self.pending_departures.remove(&key).is_some() {
            info!(
                "Cancelled pending departure for user {} in room {} — explicit Leave received",
                user_id, room
            );
        }

        // Leave is always a real participant, never an observer.
        // No display_name available from Leave message; leave_rooms will
        // fall back to user_id.
        self.leave_rooms(
            &session,
            Some(&room),
            Some(&user_id),
            None,
            false,
            was_active,
        );
    }
}

impl Handler<ActivateConnection> for ChatServer {
    type Result = ();

    fn handle(&mut self, msg: ActivateConnection, ctx: &mut Self::Context) -> Self::Result {
        let ActivateConnection { session } = msg;
        let was_testing = if let Some(state) = self.connection_states.get_mut(&session) {
            if *state == ConnectionState::Testing {
                *state = ConnectionState::Active;
                info!("Session {} activated (Testing -> Active)", session);
                true
            } else {
                false
            }
        } else {
            self.connection_states
                .insert(session, ConnectionState::Active);
            info!(
                "Session {} activated (state was missing, created as Active)",
                session
            );
            // Treat missing state as a Testing -> Active transition so we
            // still broadcast PARTICIPANT_JOINED.
            true
        };

        // Broadcast PARTICIPANT_JOINED now that this connection is confirmed
        // as the elected/active one. During JoinRoom, the broadcast was deferred
        // to avoid ghost join events from Testing connections (e.g., the losing
        // connection during RTT election).
        //
        // Skip the broadcast for sessions marked in suppress_join_broadcast
        // (reconnection sessions and observer sessions).
        let suppressed = self.suppress_join_broadcast.remove(&session);
        if was_testing && !suppressed {
            // Look up the session's room, user_id, and display_name from room_members.
            let mut found: Option<(String, String, String)> = None;
            for (room_id, members) in &self.room_members {
                for (sid, uid, dname) in members {
                    if *sid == session {
                        found = Some((room_id.clone(), uid.clone(), dname.clone()));
                        break;
                    }
                }
                if found.is_some() {
                    break;
                }
            }

            if let Some((room_id, user_id, display_name)) = found {
                let bytes = SessionManager::build_peer_joined_packet(
                    &room_id,
                    &user_id,
                    session,
                    &display_name,
                );
                let subject = format!("room.{}.system", room_id.replace(' ', "_"));
                info!(
                    "Publishing deferred PARTICIPANT_JOINED for {} (display={}, session={}) to {}",
                    user_id, display_name, session, subject
                );
                let nc = self.nats_connection.clone();
                let fut = async move {
                    if let Err(e) = nc.publish(subject, bytes.into()).await {
                        error!("Error publishing deferred PARTICIPANT_JOINED: {}", e);
                    }
                };
                let fut = actix::fut::wrap_future::<_, Self>(fut);
                ctx.spawn(fut);
            } else {
                // This can happen for observer sessions (not tracked in room_members)
                // or if the session was cleaned up before activation. Not an error.
                info!(
                    "Session {} activated but not found in room_members — \
                     skipping PARTICIPANT_JOINED (likely observer or already cleaned up)",
                    session
                );
            }
        }
    }
}

/// Handler for deferred departure execution.
/// Runs after [`RECONNECT_GRACE_PERIOD`] unless cancelled by a reconnection.
impl Handler<ExecutePendingDeparture> for ChatServer {
    type Result = ();

    fn handle(
        &mut self,
        ExecutePendingDeparture {
            session,
            room,
            user_id,
            display_name,
        }: ExecutePendingDeparture,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let key = (room.clone(), user_id.clone());

        // Only execute if this departure is still pending. It may have been
        // cancelled by a reconnection or replaced by a newer disconnect.
        if let Some(pending) = self.pending_departures.remove(&key) {
            if pending.old_session != session {
                // A newer disconnect replaced this one — do nothing, the newer
                // timer will handle it.
                info!(
                    "Stale pending departure for user {} in room {} (session {} != {}), skipping",
                    user_id, room, session, pending.old_session
                );
                // Re-insert the newer pending state.
                self.pending_departures.insert(key, pending);
                return;
            }

            // Only broadcast PARTICIPANT_LEFT if the session was Active when it
            // disconnected. Testing sessions (e.g., the losing connection during
            // RTT election) never had PARTICIPANT_JOINED broadcast, so emitting
            // PARTICIPANT_LEFT would cause ghost leave events for other participants.
            if !pending.was_active {
                info!(
                    "Grace period expired for user {} (session {}) in room {} — \
                     skipping PARTICIPANT_LEFT (session was never activated)",
                    user_id, session, room
                );
                // p2-6: drop the SFU member entry before the room_states
                // map entry may be evicted below.
                if let Some(state) = self.room_states.get(&room) {
                    let mut guard = match state.write() {
                        Ok(g) => g,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                    guard.remove_member(session);
                }
                // Still clean up room_members for the old session.
                if let Some(members) = self.room_members.get_mut(&room) {
                    members.retain(|(sid, _, _)| *sid != session);
                    if members.is_empty() {
                        self.room_members.remove(&room);
                        // Mirror room_members lifecycle for SFU state.
                        self.forwarders.remove(&room);
                        self.room_states.remove(&room);
                        self.subscriptions.remove(&room);
                        // p3-11: tear down the speaker tick + scorer for
                        // the now-empty room. Dropping the TickHandle
                        // aborts the background task.
                        self.speaker_ticks.remove(&room);
                        self.speaker_scorers.remove(&room);
                        // vc-kol / p6-7 (vc-c6l): drop this room from the
                        // shared owner-pod beacon hub. Non-owner pods
                        // never registered; `unregister` is a no-op.
                        self.beacon_hub.unregister(&room);
                    }
                }
                return;
            }

            info!(
                "Grace period expired for user {} (session {}) in room {} — \
                 executing PARTICIPANT_LEFT",
                user_id, session, room
            );
            // Observer sessions bypass the grace period entirely (handled
            // directly in Disconnect), so this path is always non-observer.
            // The `if !pending.was_active { return; }` check above proves
            // `was_active == true` here, so pass `true` explicitly.
            self.leave_rooms(
                &session,
                Some(&room),
                Some(&user_id),
                Some(&display_name),
                false,
                true,
            );
        } else {
            info!(
                "Pending departure for user {} in room {} already cancelled (reconnected)",
                user_id, room
            );
        }
    }
}

/// Recover from an unexpected per-room dispatcher exit (vc-q0v).
///
/// The dispatcher posts [`RoomDispatcherExited`] when its NATS
/// subscription dies (initial subscribe failed or `sub.next()` returned
/// `None` mid-flight). Three cases:
///
/// 1. **Entry already gone** — normal teardown raced the abort; nothing
///    to do.
/// 2. **Entry present, receivers empty** — drain happened concurrently;
///    drop the entry.
/// 3. **Entry present, receivers non-empty** — respawn the dispatcher,
///    *reusing the same receivers map and forwarder*. Sessions stay
///    connected; they just experience the (NATS-bounded) gap that the
///    dispatcher was unavailable.
///
/// Without this handler, a single dispatcher failure would poison the
/// whole room: new joiners would insert into the orphaned receivers map
/// and silently receive zero media for the lifetime of the room. The
/// pre-vc-q0v per-session model degraded one session per failure; this
/// recovery preserves the same blast radius at the room level.
impl Handler<RoomDispatcherExited> for ChatServer {
    type Result = ();

    fn handle(
        &mut self,
        RoomDispatcherExited { room }: RoomDispatcherExited,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        let Some(existing) = self.room_dispatch.remove(&room) else {
            // Normal teardown already removed the entry — nothing to do.
            return;
        };
        // `existing.task` is the handle of the task that just sent this
        // message. It's already finished; calling abort() on a finished
        // task is a no-op, and dropping the handle detaches harmlessly.
        let receivers = existing.receivers;
        let has_receivers = {
            let g = match receivers.read() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            !g.is_empty()
        };
        if !has_receivers {
            info!(
                "Per-room dispatcher exited for room {} with no remaining receivers; \
                 entry removed",
                room
            );
            return;
        }
        // Respawn against the same receivers map and forwarder so live
        // sessions keep their mailbox identity and the room's SFU state
        // (members + capabilities) is preserved.
        let Some(forwarder) = self.forwarders.get(&room).cloned() else {
            warn!(
                "Per-room dispatcher exited for room {} but forwarder is gone; \
                 dropping entry (receivers will be cleaned up on next leave_rooms)",
                room
            );
            return;
        };
        // p3-11: the scorer should also still be live as long as the
        // forwarder is, since both share the room lifecycle. Fall back to
        // a fresh empty scorer if it has somehow been evicted — the
        // dispatcher must always have one to observe into.
        let scorer = self
            .speaker_scorers
            .entry(room.clone())
            .or_insert_with(|| Arc::new(TokioRwLock::new(SpeakerScorer::new())))
            .clone();
        // p4-4: the dispatcher needs the room's RoomState handle to record
        // per-receiver bandwidth estimates from DiagnosticsPacket ingest.
        // The forwarder above keeps room_state alive, so this lookup should
        // always succeed; fall back to materialising a fresh entry to keep
        // the respawn path infallible (matches the scorer fallback above).
        let room_state = self
            .room_states
            .entry(room.clone())
            .or_insert_with(|| Arc::new(RwLock::new(RoomState::new(room.clone()))))
            .clone();
        let subject = format!("room.{room}.*").replace(' ', "_");
        let sfu_mode = self.sfu_config.mode;
        warn!(
            "Respawning per-room dispatcher for room {} (subscription died with \
             {} live receivers still attached)",
            room,
            receivers
                .read()
                .map(|g| g.len())
                .unwrap_or_else(|p| p.into_inner().len()),
        );
        let task = spawn_room_dispatcher(
            self.nats_connection.clone(),
            room.clone(),
            subject,
            sfu_mode,
            forwarder,
            scorer,
            receivers.clone(),
            room_state,
            ctx.address(),
        );
        self.room_dispatch
            .insert(room, RoomDispatch { receivers, task });
    }
}

/// Apply the result of an async home-region lookup (bead vc-hc8 / p6-9).
///
/// Three cases:
///
/// 1. **In-region**: just populate the cache so subsequent joiners take
///    the synchronous fast path in `JoinRoom`. The session that triggered
///    the lookup remains admitted — no extra action.
///
/// 2. **Out-of-region, session still alive**: emit the REDIRECT packet on
///    the session's recipient, then synthesize a `Disconnect` to itself
///    so the normal leave path (PARTICIPANT_LEFT, pending-departure
///    grace window, SFU cleanup) runs identically to a client-initiated
///    disconnect. This is the v1 "accept ~250ms RTT penalty" window: the
///    first joiner for a foreign-homed room in this region pays one
///    admit-then-redirect round-trip; all subsequent joiners hit the
///    synchronous cache path.
///
/// 3. **Out-of-region, session already gone**: still populate the cache.
///    The session having disconnected in the meantime doesn't change the
///    home-region binding for the room; future joiners must still be
///    redirected. Dropping the cache update here would mean every
///    cross-region first-joiner re-paid the async lookup.
impl Handler<HomeRegionResolved> for ChatServer {
    type Result = ();

    fn handle(
        &mut self,
        HomeRegionResolved {
            room,
            home_region,
            session,
            user_id,
            display_name,
        }: HomeRegionResolved,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        // Idempotency: a second `HomeRegionResolved` for the same room can
        // arrive if two cache-miss joiners overlap (both spawn lookups
        // before either finishes). The second writer's value is identical
        // to the first (KV CAS makes the home region single-valued), so
        // overwriting is a no-op semantically.
        self.home_region_cache
            .insert(room.clone(), home_region.clone());

        let current_region = crate::sfu::affinity::current_region();
        let Some(target) = crate::sfu::affinity::compute_cross_region_redirect_target(
            &home_region,
            current_region,
            sfu_transport_kind(),
            crate::sfu::affinity::region_base_domain(),
        ) else {
            // Same region — nothing more to do; the originating JoinRoom
            // has already admitted the session locally.
            return;
        };

        // Out-of-region: emit REDIRECT to the originating session if it's
        // still alive. The session may have already disconnected (rare:
        // would require the client to drop within the KV-roundtrip window).
        // In that case we still kept the cache entry above so future
        // joiners in this region take the synchronous redirect path.
        let Some(recipient) = self.sessions.get(&session).cloned() else {
            info!(
                "p6-9 async redirect: session {} for user {} disappeared \
                 before lookup completed; cache for room {} now pinned to {}",
                session, user_id, room, home_region
            );
            return;
        };

        info!(
            "JoinRoom cross-region redirect (async): room {} homed in {} but \
             this pod is in {}; redirecting session {} (user {}) to {}",
            room, home_region, current_region, session, user_id, target,
        );
        let bytes = SessionManager::build_admission_redirect_packet(&target, "wrong_region");
        if let Err(e) = recipient.try_send(Message {
            msg: bytes::Bytes::from(bytes),
            session,
        }) {
            warn!(
                "Failed to deliver async ADMISSION_DECISION{{REDIRECT}} \
                 (wrong_region) to session {}: {}",
                session, e
            );
        }

        // Synthesize a Disconnect so the normal leave path runs. We address
        // it to ourselves rather than calling `leave_rooms` directly so the
        // Disconnect handler's session-state cleanup (sessions /
        // connection_states / suppress_join_broadcast removal) runs in one
        // place.
        //
        // vc-9g7: set `redirect: true` to BYPASS the pending-departure
        // grace window. The client will not reconnect to this pod — it's
        // being told to go to a different region — so the standard 2s
        // RECONNECT_GRACE_PERIOD deferral would just produce a ghost-
        // participant flicker for cross-region peers federated via NATS
        // (PARTICIPANT_JOINED → PARTICIPANT_LEFT pair). The Disconnect
        // handler's `if redirect` arm calls `leave_rooms` immediately.
        ctx.address().do_send(crate::messages::server::Disconnect {
            session,
            room,
            user_id,
            display_name,
            observer: false,
            redirect: true,
        });
    }
}

impl Handler<ClientMessage> for ChatServer {
    type Result = ();

    fn handle(&mut self, msg: ClientMessage, ctx: &mut Self::Context) -> Self::Result {
        let ClientMessage {
            session,
            room,
            msg,
            user: _,
        } = msg;
        let kind = msg.kind;
        trace!("got message in server room {room} session {session}");

        // Check connection state - only publish to NATS if Active
        let connection_state = self
            .connection_states
            .get(&session)
            .copied()
            .unwrap_or(ConnectionState::Testing);

        if connection_state != ConnectionState::Active {
            trace!(
                "Skipping NATS publish for session {} in Testing state",
                session
            );
            return; // Don't publish during Testing state
        }

        let nc = self.nats_connection.clone();
        let subject = format!("room.{room}.{session}");
        let subject = subject.replace(' ', "_");

        let packet_bytes =
            if let Ok(mut packet_wrapper) = PacketWrapper::parse_from_bytes(&msg.data) {
                if packet_wrapper.session_id == 0 {
                    packet_wrapper.session_id = session;
                }
                // p3-5: SUBSCRIPTION_UPDATE is a server-local control packet —
                // intercept it before NATS publish, apply it to the per-room
                // SubscriptionStore so the forwarder picks it up on the next
                // decide(), and return without broadcasting. Peers do not need
                // to see other peers' subscription state.
                if packet_wrapper.packet_type == PacketType::SUBSCRIPTION_UPDATE.into() {
                    self.apply_subscription_update(&room, session, &packet_wrapper.data);
                    return;
                }
                // p4-10: layer-aware KEYFRAME_REQUEST routing.
                //
                // A KFR triggers the named sender to blast a fresh ~1.5 MB
                // keyframe. If the requesting receiver is not currently
                // subscribed to any layer of that sender (per its cached
                // `LayerSelection`), the KFR is wasted work — and on a
                // bandwidth-constrained downlink it can wedge the receiver's
                // egress queue behind a keyframe burst it will never consume.
                //
                // The existing per-session KFR rate limit in
                // `session_logic.rs` remains in force; this is an additive
                // policy applied before the NATS publish so the named sender
                // is never woken at all for KFRs that wouldn't help.
                //
                // vc-jgj: branch directly on the `PacketKind` plumbed from
                // `SessionLogic::handle_inbound`. The inner `MediaPacket`
                // parse only runs when we already know this is a KFR, so
                // the AUDIO / VIDEO / SCREEN fan-out path never re-parses.
                if kind == PacketKind::KeyframeRequest {
                    if let Ok(media_packet) = MediaPacket::parse_from_bytes(&packet_wrapper.data) {
                        let members: &[(SessionId, String, String)] = self
                            .room_members
                            .get(&room)
                            .map(Vec::as_slice)
                            .unwrap_or(&[]);
                        // vc-wls: lock-free read of the cached selection.
                        // `last_selection_for` returns an
                        // `Arc<CachedSelection>` from a DashMap shard lock
                        // that contends only with writes to THIS receiver's
                        // entry. We then clone the inner `LayerSelection`
                        // (typically tiny) so we don't hand a clone-on-write
                        // Arc across the helper boundary.
                        let cached_selection = self.forwarders.get(&room).and_then(|fwd| {
                            fwd.layer_selector()
                                .last_selection_for(session)
                                .map(|cached| cached.selection.clone())
                        });
                        if should_drop_kfr_for_layer_selection(
                            &media_packet.user_id,
                            session,
                            members,
                            cached_selection.as_ref(),
                        ) {
                            crate::metrics::SFU_DROPPED_TOTAL
                                .with_label_values(&["kfr_unsubscribed"])
                                .inc();
                            debug!(
                                "Dropping KEYFRAME_REQUEST from session {} in room {} \
                                 (target {:?} not in current layer selection)",
                                session,
                                room,
                                std::str::from_utf8(&media_packet.user_id).unwrap_or("<bin>"),
                            );
                            return;
                        }
                    }
                }
                match packet_wrapper.write_to_bytes() {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        error!("Failed to serialize PacketWrapper with session_id: {}", e);
                        msg.data.to_vec()
                    }
                }
            } else {
                msg.data.to_vec()
            };

        let b = bytes::Bytes::from(packet_bytes);
        let fut = async move {
            match nc.publish(subject.clone(), b).await {
                Ok(_) => trace!("published message to {subject}"),
                Err(e) => error!("error publishing message to {subject}: {e}"),
            }
        };
        let fut = actix::fut::wrap_future::<_, Self>(fut);
        ctx.spawn(fut);
    }
}

impl Handler<JoinRoom> for ChatServer {
    type Result = MessageResult<JoinRoom>;

    fn handle(
        &mut self,
        JoinRoom {
            session,
            room,
            user_id,
            display_name,
            observer,
            capabilities,
        }: JoinRoom,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        // Validate user_id synchronously BEFORE spawning async task.
        // This ensures we return an error to the client if validation fails,
        // rather than returning Ok and silently failing in the spawned task.
        if user_id == SYSTEM_USER_ID {
            return MessageResult(Err("Cannot use reserved system user ID".into()));
        }

        if self.joined_sessions.contains(&session) {
            return MessageResult(Ok(()));
        }

        // --- Cross-region home-region pinning (bead vc-hc8 / p6-9) ---
        // Each region runs its own StatefulSet. Rooms have a "home region"
        // assigned by the first joiner via a NATS JetStream KV bucket
        // (`rooms-home-region`) under atomic create-if-absent semantics.
        // Out-of-region clients get redirected to the home region's load
        // balancer BEFORE the in-region pod-ordinal redirect below — a
        // wrong-region client must never reach the pod-ordinal check
        // because the pod ordinal it's about to be told to use lives in
        // the wrong cluster.
        //
        // Observers are EXEMPT for the same reason they're exempt from
        // the pod-ordinal redirect: they don't participate in SFU state,
        // and bouncing them across regions just to listen would degrade
        // the metrics / diagnostic surface without any consistency win.
        //
        // ORDER MATTERS — this runs BEFORE the p6-5 pod-ordinal redirect,
        // BEFORE the reconnection bookkeeping, BEFORE admission control.
        // Nothing has been mutated by this point, so an Err return (or
        // an async redirect-then-disconnect) requires no rollback.
        //
        // ASYNC HANDLING: the JoinRoom handler returns synchronously, but
        // the KV lookup is async. We use a synchronous in-actor cache as
        // the steady-state fast path; on cache miss the very first joiner
        // is admitted locally and the lookup runs in a spawned task that
        // posts `HomeRegionResolved` back to the actor. If the resolved
        // home turns out to be a foreign region, that handler emits the
        // REDIRECT packet and synthesizes a Disconnect, matching the
        // single-round-trip "accept ~250ms RTT penalty" v1 compromise
        // called out in the bead. Steady state (all subsequent joiners
        // for the room) is a synchronous cache hit with the redirect
        // decided up front — no admission of cross-region traffic.
        if !observer {
            let current_region = crate::sfu::affinity::current_region();
            if let Some(cached_home) = self.home_region_cache.get(&room).cloned() {
                if let Some(target) = crate::sfu::affinity::compute_cross_region_redirect_target(
                    &cached_home,
                    current_region,
                    sfu_transport_kind(),
                    crate::sfu::affinity::region_base_domain(),
                ) {
                    info!(
                        "JoinRoom cross-region redirect (cache hit): room {} \
                         homed in {} but this pod is in {}; redirecting \
                         session {} (user {}) to {}",
                        room, cached_home, current_region, session, user_id, target,
                    );
                    if let Some(recipient) = self.sessions.get(&session) {
                        let bytes = SessionManager::build_admission_redirect_packet(
                            &target,
                            "wrong_region",
                        );
                        if let Err(e) = recipient.try_send(Message {
                            msg: bytes::Bytes::from(bytes),
                            session,
                        }) {
                            warn!(
                                "Failed to deliver ADMISSION_DECISION{{REDIRECT}} \
                                 (wrong_region) to session {}: {}",
                                session, e
                            );
                        }
                    }
                    return MessageResult(Err(format!(
                        "Room {room} is homed in region {cached_home}; \
                         redirecting to {target}"
                    )));
                }
                // Cache hit, same region → fall through to p6-5 below.
            } else {
                // Cache miss: spawn the NATS-KV lookup and let the join
                // proceed locally for v1. The spawned task reports back
                // via `HomeRegionResolved`, which populates the cache and
                // (if the room is homed elsewhere) emits the REDIRECT
                // packet plus a synthesized Disconnect.
                //
                // This admits a tiny window where the very first joiner
                // for a foreign-homed room in this region pays a brief
                // admit-then-redirect instead of an up-front redirect.
                // The bead explicitly accepts this for v1. All subsequent
                // joiners in this region hit the cache path above.
                debug!(
                    "p6-9 cache-miss: spawning home-region lookup for room {} \
                     (session {}, user {}); session admitted locally pending \
                     lookup result.",
                    room, session, user_id
                );
                let kv = self.home_region_kv.clone();
                let room_for_task = room.clone();
                let user_for_task = user_id.clone();
                let display_for_task = display_name.clone();
                let region_for_task: &'static str = current_region;
                let chat_addr = ctx.address();
                tokio::spawn(async move {
                    let home = crate::sfu::affinity::home_region(
                        &room_for_task,
                        kv.as_ref(),
                        region_for_task,
                    )
                    .await;
                    // Fire-and-forget: if the actor is gone, dropping the
                    // future is the right thing.
                    let _ = chat_addr
                        .send(HomeRegionResolved {
                            room: room_for_task,
                            home_region: home,
                            session,
                            user_id: user_for_task,
                            display_name: display_for_task,
                        })
                        .await;
                });
            }
        }

        // --- Ownership redirect (bead vc-8oa / p6-5) ---
        // Wave 3 affinity migration: each room is jump-hashed to exactly
        // one pod ordinal in the StatefulSet. If a client connects to a
        // non-owner pod, we emit an ADMISSION_DECISION{REDIRECT} hint
        // pointing at the owner pod's headless DNS and decline the join.
        // The transport actor closes the connection on JoinRoom Err
        // (see SessionLogic::handle_join_room_result); the redirect packet
        // delivered through the recipient mpsc just before that close is
        // what the client uses to reconnect to the correct pod.
        //
        // Observers are EXEMPT — they don't participate in room ownership
        // (no media write path, no SFU room_state membership). Forcing an
        // observer to redirect just to listen makes the metrics/diagnostic
        // path more fragile without any consistency benefit.
        //
        // ORDER MATTERS — this MUST run BEFORE the reconnection bookkeeping
        // below. If we ran it after, a reconnecting user landing on the
        // wrong pod would lose their old `pending_departures` entry, have
        // their deferred PARTICIPANT_LEFT cancelled, and be removed from
        // `room_members` / `room_states` — and then be told to redirect.
        // Peers would never learn the user left, and the redirect would
        // not heal it because the leave event never fires. Doing the
        // ownership check synchronously up front means nothing has been
        // mutated yet, so there's nothing to roll back. The check also
        // runs BEFORE the soft/hard-cap admission accounting below so a
        // redirected client never increments the wrong pod's caps and
        // never receives a QUEUED/REJECTED packet it would discard anyway.
        //
        // vc-85p (p6-5) SPILLOVER OVERRIDE: before honouring the ownership
        // redirect, consult the owner-pod health beacon for this room. If
        // `is_spilled_over` is true — the owner is over the participant or
        // CPU threshold AND its beacon is fresh (< 15s) — we ADMIT THE
        // JOINER LOCALLY (spill) instead of redirecting. Admitting locally
        // means falling through to the same local-admit machinery below
        // (reconnection bookkeeping, admission caps, room_members /
        // room_states materialisation, per-room dispatcher) that any
        // normally-admitted local participant takes — we do NOT emit the
        // ADMISSION_DECISION{REDIRECT} packet. Spill-pod media federation
        // already works: every pod's dispatcher subscribes `room.{room}.*`,
        // so a locally-admitted listener receives senders' media via NATS.
        //
        // For under-threshold, unknown, or stale-beacon rooms,
        // `is_spilled_over` returns false and behaviour is UNCHANGED:
        // redirect to the owner exactly as before. Observers were already
        // exempt from the redirect and remain so.
        //
        // Idempotency: this decision lives only in JoinRoom. Once admitted
        // locally the session is in `room_members` / `room_states` and is
        // treated as an ordinary local member; no later state message
        // re-runs this branch, so a spilled joiner never bounces. A
        // reconnect re-evaluates the predicate, which is correct healing
        // behaviour (still spilled → stay; no longer spilled → redirect).
        if !observer {
            let replicas = crate::sfu::affinity::replicas_from_env();
            let self_ord = crate::sfu::affinity::self_ordinal_from_env();
            let spilled_over = self.spillover_store.is_spilled_over(&room);
            // Compute the redirect target once: needed both for the spill
            // observability log (when this pod is a non-owner) and for the
            // actual redirect on the non-spill path.
            let redirect_target = crate::sfu::affinity::compute_redirect_target(
                &room,
                self_ord,
                replicas,
                sfu_transport_kind(),
            );
            if spilled_over {
                // Log the spill admission UNCONDITIONALLY — the predicate
                // already passed, so the joiner is being admitted locally
                // regardless of whether this pod is the owner. Include the
                // would-be redirect target only when this pod is a non-owner
                // (target is Some); when this pod IS the owner the spill is a
                // normal local admission with no redirect to suppress.
                match &redirect_target {
                    Some(target) => info!(
                        "JoinRoom SPILL: admitting joiner locally for room={} \
                         (session {}, user {}, owner ordinal != self {:?}) \
                         (would-redirect-to={})",
                        room, session, user_id, self_ord, target,
                    ),
                    None => info!(
                        "JoinRoom SPILL: admitting joiner locally for room={} \
                         (session {}, user {}, this pod is the owner)",
                        room, session, user_id,
                    ),
                }
                // Fall through to the local-admit path below.
            } else if let Some(target) = redirect_target {
                info!(
                    "JoinRoom redirect: room {} owned by ordinal != self ({:?}); \
                     redirecting session {} (user {}) to {}",
                    room, self_ord, session, user_id, target,
                );
                if let Some(recipient) = self.sessions.get(&session) {
                    let bytes =
                        SessionManager::build_admission_redirect_packet(&target, "wrong_owner");
                    if let Err(e) = recipient.try_send(Message {
                        msg: bytes::Bytes::from(bytes),
                        session,
                    }) {
                        warn!(
                            "Failed to deliver ADMISSION_DECISION{{REDIRECT}} to \
                             session {}: {}",
                            session, e
                        );
                    }
                }
                // No rollback needed: this block runs before
                // `pending_departures.remove`, before `room_members` /
                // `room_states` cleanup, and before the
                // `suppress_join_broadcast` insert. Returning Err here
                // leaves all of that state untouched so the original
                // pod's deferred PARTICIPANT_LEFT still fires after the
                // grace period if the client doesn't successfully
                // reconnect on the correct pod.
                return MessageResult(Err(format!(
                    "Room {room} is owned by a different pod; redirecting to {target}"
                )));
            }
        }

        // --- Reconnection grace period: cancel pending departure ---
        // If the same user_id is reconnecting to the same room within
        // the grace window, suppress both PARTICIPANT_LEFT (already deferred)
        // and the PARTICIPANT_JOINED that would normally follow.
        let departure_key = (room.clone(), user_id.clone());
        let is_reconnection = if let Some(pending) = self.pending_departures.remove(&departure_key)
        {
            ctx.cancel_future(pending.spawn_handle);

            // Clean up stale room_members entry from the old session
            if let Some(members) = self.room_members.get_mut(&room) {
                members.retain(|(sid, _, _)| *sid != pending.old_session);
            }
            // Mirror the cleanup on the SFU member table — the old SID is
            // gone (subscription was already aborted in Disconnect) and a
            // new SID will be inserted below.
            if let Some(state) = self.room_states.get(&room) {
                let mut guard = match state.write() {
                    Ok(g) => g,
                    Err(poisoned) => poisoned.into_inner(),
                };
                guard.remove_member(pending.old_session);
            }

            info!(
                "Reconnection detected for user {} in room {} — cancelled pending \
                 PARTICIPANT_LEFT (old session {}, new session {})",
                user_id, room, pending.old_session, session
            );
            true
        } else {
            false
        };

        // Mark reconnection and observer sessions so ActivateConnection does not
        // broadcast PARTICIPANT_JOINED for them. Reconnection sessions never
        // "left" from peers' perspective; observers are never announced.
        if is_reconnection || observer {
            self.suppress_join_broadcast.insert(session);
        }

        // --- Admission control (bead vc-69e / p3-13) ---
        // Two-tier admission policy for non-observer joins:
        //   - count < WAITING_ROOM_THRESHOLD: admit silently (no packet emitted)
        //   - WAITING_ROOM_THRESHOLD <= count < hard_cap: admit + emit
        //     ADMISSION_DECISION{QUEUED} informational packet so the client
        //     can surface a "near capacity" hint to the user. The joiner IS
        //     still fully admitted to the room — wave-1 has no actual
        //     queueing mechanism (that lands in wave-3).
        //   - count >= hard_cap: reject. Emit ADMISSION_DECISION{REJECTED}
        //     to the session before declining the join, so the client can
        //     show a structured error instead of just a disconnect.
        //
        // Observers don't count: they bypass room_members tracking entirely.
        // Reconnections also pass: the stale row was just removed above, so
        // the count reflects the post-cleanup state.
        //
        // Without the hard cap, a scripted attacker with one valid JWT can
        // spawn thousands of sessions in a single room and OOM the pod (each
        // session = one bounded mpsc + QUIC connection state + N-1 broadcast
        // amplifier for PARTICIPANT_JOINED). The cap matches the SFU
        // refactor's webinar-shape design target (200 participants); see
        // PLAN.md §J / Open Risk #4.
        //
        // `pending_queued_packet` is set to Some(packet_bytes) when the soft
        // cap has been crossed; it is sent to the new joiner after the
        // session is fully registered in `room_members` so the client cannot
        // race the packet against its own JoinRoom Ok response (the packet
        // travels via the same recipient mpsc used for media fan-out).
        let mut pending_queued_packet: Option<Vec<u8>> = None;
        if !observer {
            let hard_cap = std::env::var(MAX_PARTICIPANTS_ENV)
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(MAX_PARTICIPANTS_PER_ROOM);
            // The soft cap is clamped to the hard cap so a misconfigured env
            // override (soft >= hard) collapses cleanly to a single-tier hard
            // reject instead of producing a negative overflow zone.
            let configured_soft = std::env::var(WAITING_ROOM_THRESHOLD_ENV)
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(WAITING_ROOM_THRESHOLD);
            let soft_cap = configured_soft.min(hard_cap);
            let current = self.room_members.get(&room).map(|m| m.len()).unwrap_or(0);
            if current >= hard_cap {
                warn!(
                    "JoinRoom rejected: room {} is at capacity ({}/{}) — \
                     user {} (session {}) denied",
                    room, current, hard_cap, user_id, session,
                );
                // Emit ADMISSION_DECISION{REJECTED} to the rejected session
                // before declining the join. Best-effort: a failure here
                // (e.g., session recipient already gone) does not change
                // the rejection outcome.
                if let Some(recipient) = self.sessions.get(&session) {
                    let bytes = SessionManager::build_admission_decision_packet(
                        AdmissionStatus::REJECTED,
                        // 1-based overflow position; for the rejected (N+1)st
                        // joiner this is (hard_cap - soft_cap + 1).
                        (current.saturating_sub(soft_cap).saturating_add(1)) as u32,
                        "room_full",
                        // Conservative client retry hint. Wave-3 will replace
                        // this with a server-computed value derived from
                        // recent churn.
                        30,
                    );
                    if let Err(e) = recipient.try_send(Message {
                        msg: bytes::Bytes::from(bytes),
                        session,
                    }) {
                        warn!(
                            "Failed to deliver ADMISSION_DECISION{{REJECTED}} to \
                             session {}: {}",
                            session, e
                        );
                    }
                }
                // Roll back the suppress_join_broadcast insertion we just did
                // for the reconnection case, so a later retry (after the room
                // drains) doesn't silently suppress the legitimate broadcast.
                if is_reconnection {
                    self.suppress_join_broadcast.remove(&session);
                }
                return MessageResult(Err(format!(
                    "Room {room} is at capacity ({hard_cap}); please try again later"
                )));
            }
            if current >= soft_cap {
                // 1-based offset into the soft-cap overflow zone:
                //   current == soft_cap     => position = 1
                //   current == soft_cap + 1 => position = 2
                //   ...
                // This matches the bead spec's "count - 194 = position" for
                // the default thresholds (WAITING_ROOM_THRESHOLD=195) and
                // generalises cleanly when the soft cap is reconfigured.
                let position = (current - soft_cap + 1) as u32;
                info!(
                    "JoinRoom soft-cap reached: room {} at {}/{} (hard {}) — \
                     admitting user {} (session {}) with QUEUED hint position={}",
                    room, current, soft_cap, hard_cap, user_id, session, position,
                );
                pending_queued_packet = Some(SessionManager::build_admission_decision_packet(
                    AdmissionStatus::QUEUED,
                    position,
                    "soft_cap_reached",
                    0,
                ));
            }
        }

        let room_clone = room.clone();
        let user_id_clone = user_id.clone();
        let display_name_clone = display_name.clone();
        let session_id = session;
        let nc = self.nats_connection.clone();

        let session_str = session.to_string();
        // vc-q0v: only the subject string is needed — the per-room demux
        // uses a plain `subscribe` rather than `queue_subscribe`. The queue
        // group was a per-session artifact of the pre-vc-q0v fan-out model.
        let (subject, _queue) = build_subject_and_queue(&room, &session_str);
        let session_recipient = match self.sessions.get(&session) {
            Some(addr) => addr.clone(),
            None => {
                return MessageResult(Err("Session not found".into()));
            }
        };

        // Collect existing non-observer room members for notifying the new joiner.
        // On reconnection, we still send the existing member list so the
        // reconnecting client knows who is in the room.
        let existing_members: Vec<(SessionId, String, String)> = if !observer {
            self.room_members.get(&room).cloned().unwrap_or_default()
        } else {
            Vec::new()
        };

        // True when the room had no non-observer participants before this join.
        // Used to gate the NATS MEETING_STARTED broadcast (the transport actors
        // already send MEETING_STARTED directly to every connecting client).
        let is_first_in_room = existing_members.is_empty() && !observer;

        // Track this session in room_members (only for non-observers)
        if !observer {
            self.room_members.entry(room.clone()).or_default().push((
                session,
                user_id.clone(),
                display_name.clone(),
            ));
        }

        // Lazily materialize per-room SFU state and register this session
        // in the member table (p2-6). `insert_member` overwrites prior
        // entries with matching `session_id`, mirroring reconnect semantics.
        let room_state = self
            .room_states
            .entry(room.clone())
            .or_insert_with(|| Arc::new(RwLock::new(RoomState::new(room.clone()))))
            .clone();
        let subscriptions = self
            .subscriptions
            .entry(room.clone())
            .or_insert_with(|| Arc::new(RwLock::new(SubscriptionStore::new())))
            .clone();
        // p3-11: materialise the per-room speaker scorer + tick on first
        // join. The tick owns the `watch::Sender<ActiveSpeakerSet>` it drives
        // on its 200ms cadence; we subscribe BEFORE calling `run()` so the
        // forwarder's receiver is wired up to the same channel the tick task
        // will publish to. The tick handle is retained in `speaker_ticks`;
        // dropping it on room drain aborts the background task.
        let scorer = self
            .speaker_scorers
            .entry(room.clone())
            .or_insert_with(|| Arc::new(TokioRwLock::new(SpeakerScorer::new())))
            .clone();
        // Atomically materialise the speaker tick + forwarder on first join.
        // Using `Entry::Vacant` here (rather than two `or_insert_with` calls
        // with a precomputed `speakers_rx`) keeps `speaker_ticks` and
        // `forwarders` impossible to drift: either both exist (occupied
        // branch reuses the cached forwarder) or both are inserted together.
        let forwarder = match self.forwarders.entry(room.clone()) {
            std::collections::hash_map::Entry::Occupied(occ) => occ.get().clone(),
            std::collections::hash_map::Entry::Vacant(vac) => {
                let publisher = Arc::new(NatsSpeakerPublisher::new(self.nats_connection.clone()));
                let tick = SpeakerTick::new(scorer.clone(), room.clone(), publisher);
                let speakers_rx = tick.subscribe();
                let handle = tick.run();
                self.speaker_ticks.insert(room.clone(), handle);
                // vc-kol / p6-7 (vc-c6l): register the room with the
                // shared owner-pod beacon hub — only when this pod is the
                // room's owner per the consistent-hash jump (p6-1).
                // Non-owner pods stay silent so the topic carries exactly
                // one beacon stream per room. The hub itself re-checks
                // ownership on each tick (defensive against runtime
                // replica scale changes).
                if crate::sfu::affinity::is_owner(&room) {
                    self.beacon_hub.register(room.clone(), room_state.clone());
                }
                // vc-wls: bare `Arc<LayerSelector>` — the selector now
                // owns its own interior locking (DashMap shards for the
                // hot read cache, a small Mutex for hysteresis state).
                let layer_selector = Arc::new(LayerSelector::new());
                let f = Arc::new(Forwarder::new(
                    room_state.clone(),
                    subscriptions.clone(),
                    speakers_rx,
                    layer_selector,
                ));
                vac.insert(f).clone()
            }
        };
        {
            // Poison-safe write: a panicked previous writer leaves the
            // table mutable. Capabilities default to 0 today; later phases
            // will refresh the entry from CONNECTION packets out of band.
            let mut guard = match room_state.write() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            guard.insert_member(session, capabilities);
        }
        // p4-7: membership change invalidates every cached layer selection
        // in the room — the new member's `AllowSet` resolution may now
        // include them as a candidate sender for existing receivers.
        {
            // vc-wls: lock-free invalidation — see LayerSelector
            // concurrency notes for the access pattern.
            forwarder.layer_selector().invalidate_all();
        }
        let sfu_mode = self.sfu_config.mode;

        // --- vc-q0v: per-room parse-once demux ----------------------------
        // Ensure the per-room dispatcher exists (the first joiner spawns it)
        // and register this session's recipient in the room's receiver map.
        // The dispatcher subscribes ONCE to `room.<room>.*`, parses each
        // inbound NATS message exactly once, and fans the parsed result out
        // to every entry in `receivers` via `egress_decide_from_parsed`.
        // This eliminates the N× parse cost of the pre-vc-q0v per-session
        // subscription model.
        //
        // The `subject` built above is the per-room wildcard subject; the
        // queue group was a per-session artifact that the new dispatcher
        // does not need (one `subscribe` per pod per room).
        let receivers_for_room = match self.room_dispatch.entry(room.clone()) {
            std::collections::hash_map::Entry::Occupied(occ) => occ.get().receivers.clone(),
            std::collections::hash_map::Entry::Vacant(vac) => {
                let receivers: Arc<RwLock<HashMap<SessionId, Recipient<Message>>>> =
                    Arc::new(RwLock::new(HashMap::new()));
                let task = spawn_room_dispatcher(
                    self.nats_connection.clone(),
                    room.clone(),
                    subject.clone(),
                    sfu_mode,
                    forwarder.clone(),
                    scorer.clone(),
                    receivers.clone(),
                    room_state.clone(),
                    ctx.address(),
                );
                let recvs = receivers.clone();
                vac.insert(RoomDispatch { receivers, task });
                recvs
            }
        };
        {
            let mut w = match receivers_for_room.write() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            // Insert (or replace) — reconnects share the same SessionId
            // semantics as the prior per-session model: the latest
            // recipient wins.
            //
            // vc-9eh ORDERING INVARIANT (Part B — DO NOT MOVE THIS BELOW THE
            // `tokio::spawn` POST-JOIN TASK): this `receivers.write().insert`
            // MUST happen synchronously here, BEFORE any `.await`/`spawn`
            // boundary in this handler. The dispatcher fans out by snapshotting
            // this exact `receivers` Arc per inbound message (see
            // `spawn_room_dispatcher`), so a joiner is delivery-eligible the
            // instant it is in the map — there is no separate "register with the
            // dispatcher" step. If a future refactor moves this insert after the
            // post-join `tokio::spawn` (or behind any await), a late joiner
            // could be admitted into the room AFTER the dispatcher snapshots,
            // reintroducing the insert-after-subscribe race that the vc-9eh
            // watchdog above is the recovery net for. Keep it here.
            w.insert(session, session_recipient.clone());
        }
        self.joined_sessions.insert(session);

        // Wave-1 soft-cap notification (bead vc-69e / p3-13). The joiner is
        // already fully tracked in room_members + room_dispatch above; this
        // packet is purely informational. Best-effort delivery — a full
        // recipient mpsc here does not roll back the admission.
        if let Some(bytes) = pending_queued_packet.take() {
            if let Err(e) = session_recipient.try_send(Message {
                msg: bytes::Bytes::from(bytes),
                session,
            }) {
                warn!(
                    "Failed to deliver ADMISSION_DECISION{{QUEUED}} to session {}: {}",
                    session, e
                );
            }
        }

        // Clone the recipient so we can send existing member info directly
        // to the new joiner from the one-shot post-join task below.
        let new_joiner_recipient = session_recipient.clone();

        tokio::spawn(async move {
            // start_session is called by the transport actors (ws_chat_session /
            // wt_chat_session) in their started() method, which blocks with
            // ctx.wait() before this JoinRoom handler runs. We do NOT call it
            // again here to avoid double-counting if SessionManager ever
            // acquires stateful tracking (room capacity, DB records, etc.).
            //
            // The reserved-user-ID check is performed synchronously at the top
            // of this handler, so we can proceed directly to NATS setup.

            let start_time_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);

            info!(
                "JoinRoom task running for user {} in room {} (session {})",
                user_id_clone, room_clone, session_id,
            );

            // SESSION_ASSIGNED is sent by ws_chat_session / wt_chat_session
            // in their started() method before this JoinRoom handler runs.

            // Only broadcast MEETING_STARTED via NATS for the first
            // participant. The transport actors already send it directly
            // to every connecting client, so subsequent joins would just
            // produce redundant events for existing participants.
            if is_first_in_room {
                send_meeting_info(&nc, &room_clone, start_time_ms, &user_id_clone).await;
            }

            // PARTICIPANT_JOINED broadcast is deferred until
            // ActivateConnection is received. This prevents ghost join
            // events from Testing connections during RTT election — only
            // the elected (activated) connection announces itself.
            //
            // Reconnection joins also skip the broadcast (the user never
            // "left" from peers' perspective), and observer joins are
            // never broadcast either.
            if is_reconnection {
                info!(
                    "Suppressing PARTICIPANT_JOINED for reconnecting user {} in room {} \
                     (deferred broadcast also skipped)",
                    user_id_clone, room_clone
                );
            } else if observer {
                info!(
                    "Skipping PARTICIPANT_JOINED for observer {} in room {}",
                    user_id_clone, room_clone
                );
            } else {
                info!(
                    "Deferring PARTICIPANT_JOINED for {} (display={}) in room {} \
                     until ActivateConnection (session {})",
                    user_id_clone, display_name_clone, room_clone, session_id
                );
            }

            // Send PARTICIPANT_JOINED for each existing member directly to the new joiner.
            // This ensures the new joiner learns about all participants already in the room.
            for (existing_sid, existing_uid, existing_display_name) in &existing_members {
                let existing_bytes = SessionManager::build_peer_joined_packet(
                    &room_clone,
                    existing_uid,
                    *existing_sid,
                    existing_display_name,
                );
                info!(
                    "Sending existing PARTICIPANT_JOINED for {} (display={}) to new joiner {}",
                    existing_uid, existing_display_name, user_id_clone
                );
                if let Err(e) = new_joiner_recipient.try_send(Message {
                    msg: bytes::Bytes::from(existing_bytes),
                    session: *existing_sid,
                }) {
                    warn!(
                        "Failed to send existing PARTICIPANT_JOINED for {} to new joiner {}: {}",
                        existing_uid, user_id_clone, e
                    );
                }
            }
        });

        MessageResult(Ok(()))
    }
}

// --- vc-9eh: per-room delivery watchdog tunables + decision ----------------
//
// These thresholds are justified against the vc-7wi responsiveness budget
// (first media <= 1.5s, usable audio <= 2.0s after a late join). See
// `spawn_room_dispatcher` for the full mechanism write-up.
//
//   * SILENCE_BASE = 750ms — the silence window for the FIRST resubscribe
//     after traffic. A healthy room with active publishers keeps the
//     dispatcher's `last_msg_at` fresh at ~30fps (audio alone is ~50pps), so
//     750ms of TOTAL silence is far outside any normal inter-packet gap. For a
//     genuinely-broken subscription with active publishers, the FIRST trip
//     fires at 750ms and the in-place resubscribe restores traffic — so first
//     media is well inside the 1.5s budget.
//   * TICK = 250ms — worst-case detection latency is the current silence
//     window + one tick; the in-place resubscribe is sub-millisecond
//     (single-digit ms to a remote NATS), so for the first trip the total is
//     ~= 1000ms, comfortably inside the 1.5s first-media budget.
//   * GRACE = 750ms minimum uptime AFTER each (re)subscribe before the
//     watchdog may fire again. A fresh subscription must be given at least one
//     silence-window to receive before we judge it dead.
//   * BACKOFF (the real anti-thrash, persisted across resubscribes): the
//     silence window ESCALATES on each *consecutive* trip that fails to
//     restore traffic — 750ms, 1.5s, 3s, 6s, 12s, 24s, capped at 30s — and
//     RESETS to SILENCE_BASE the instant any real message advances
//     `last_msg_at`. This is load-bearing because the `has_publishers` gate is
//     currently inert in production (`is_observer` is never set true, so
//     `senders()` == all members) and the only periodic traffic on the
//     `room.{room}.*` wildcard — the 5s health beacon on `.system` — is far
//     longer than the 750ms base window. Gating alone therefore cannot tell a
//     silently-broken subscription from a legitimately-quiet room, so without
//     the backoff a quiet populated room would resubscribe at a fixed ~1s
//     cadence forever. With it, a broken room recovers fast (first trip at
//     750ms) while a quiet room decays to one resubscribe per ~30s.
pub const WATCHDOG_TICK: std::time::Duration = std::time::Duration::from_millis(250);
pub const WATCHDOG_SILENCE: std::time::Duration = std::time::Duration::from_millis(750);
pub const WATCHDOG_GRACE: std::time::Duration = std::time::Duration::from_millis(750);
/// vc-9eh: ceiling on the escalating silence window. A persistently-quiet
/// populated room (everyone muted/camera-off) decays to ~one resubscribe per
/// 30s — enough to eventually heal a truly-wedged subscription without WARN-log
/// spam or resubscribe churn.
pub const WATCHDOG_SILENCE_CAP: std::time::Duration = std::time::Duration::from_secs(30);

/// vc-9eh: compute the silence window for the current consecutive-trip count.
///
/// `trips` is the number of consecutive watchdog resubscribes that have NOT
/// been followed by real traffic (it resets to 0 the moment `last_msg_at`
/// advances). The window doubles each trip from [`WATCHDOG_SILENCE`], capped at
/// [`WATCHDOG_SILENCE_CAP`]:
///
/// | trips | window |
/// |-------|--------|
/// | 0     | 750ms  |
/// | 1     | 1.5s   |
/// | 2     | 3s     |
/// | 3     | 6s     |
/// | 4     | 12s    |
/// | 5     | 24s    |
/// | >=6   | 30s (cap) |
///
/// `trips == 0` (the first trip after traffic) yields the base window, so a
/// genuinely-broken subscription with active publishers still resubscribes at
/// 750ms — keeping first-media within the 1.5s budget. The escalation only
/// bites on REPEATED no-traffic trips, which is exactly the legitimately-quiet
/// room the bead said must not thrash.
pub fn watchdog_silence_window(trips: u32) -> std::time::Duration {
    // Saturating shift so a large `trips` cannot overflow; clamp to the cap.
    let scaled = WATCHDOG_SILENCE
        .checked_mul(1u32.checked_shl(trips.min(20)).unwrap_or(u32::MAX))
        .unwrap_or(WATCHDOG_SILENCE_CAP);
    scaled.min(WATCHDOG_SILENCE_CAP)
}

/// vc-9eh: pure liveness-watchdog decision, factored out of the dispatcher
/// `select!` so the gating logic is unit-testable without provoking real
/// async-nats slow-consumer backpressure (which is impractical to trigger
/// deterministically in-process).
///
/// Returns `true` IFF the per-room dispatcher should force a clean resubscribe.
/// The gate is strict on purpose:
///
///   * `uptime >= WATCHDOG_GRACE` — never judge a freshly (re)subscribed
///     subscription dead before it has had a silence-window to receive.
///   * `silence >= silence_window` — only act on genuinely stalled delivery,
///     where `silence_window` is the (escalating) window from
///     [`watchdog_silence_window`].
///   * `has_receivers` — nobody to serve ⇒ nothing to recover (a respawn would
///     be pointless; the normal drain path aborts the task anyway).
///   * `has_publishers` — no member at all ⇒ nothing to recover. (NOTE: this is
///     a coarse gate today — `is_observer` is inert in production so this is
///     effectively `member_count > 0`; the escalating backoff, not this gate,
///     is what prevents a quiet-but-populated room from thrashing.)
///
/// All four must hold. This adds ZERO per-join work: it reads only the room's
/// own `receivers` map + `RoomState`, and runs on exactly ONE timer per ROOM.
pub fn watchdog_should_resubscribe(
    uptime: std::time::Duration,
    silence: std::time::Duration,
    silence_window: std::time::Duration,
    has_receivers: bool,
    has_publishers: bool,
) -> bool {
    uptime >= WATCHDOG_GRACE && silence >= silence_window && has_receivers && has_publishers
}

/// Spawn the per-room demux subscription task (vc-q0v).
///
/// One task per room. Subscribes once to `room.<room>.*`, parses each
/// inbound NATS message exactly once via [`parse_and_inspect`], and fans
/// the parsed result out to every receiver in `receivers` by calling
/// [`egress_decide_from_parsed`]. The pre-vc-q0v model ran one
/// `queue_subscribe` per session and re-parsed each wrapper N times for an
/// N-participant room; this consolidation eliminates the (N-1) redundant
/// parses per published packet.
///
/// The task exits on four conditions:
///   1. **Normal drain** — [`ChatServer::drop_room_receiver`] aborts the
///      `JoinHandle` once the receivers map empties.
///   2. **Initial subscribe failed** — `nc.subscribe` returned `Err`.
///   3. **Subscription closed mid-flight** — `sub.next()` returned `None`
///      (NATS server closed the subscription, lame-duck shutdown, etc.).
///   4. **vc-9eh liveness watchdog** — the subscription went *silent without
///      closing* (the slow-consumer black hole: async-nats drops the message
///      and fires a connection-global `Event::SlowConsumer` but keeps the
///      `Subscriber` stream open, so `sub.next()` never yields `None`). When no
///      inbound message has arrived for `WATCHDOG_SILENCE` WHILE the room still
///      has receivers AND active publishers, the watchdog notifies the actor to
///      force a clean resubscribe.
///
/// In cases (2), (3) and (4) the task sends [`RoomDispatcherExited`] back to
/// the actor so the entry is cleaned up (or the dispatcher respawned if
/// receivers are still present). Without this signal the room would be
/// silently dead — receivers in the map but no parser feeding them. In
/// case (1) the abort drops the message channel before the send can race,
/// which is fine: the handler checks whether the entry is still present
/// and exits if not.
#[allow(clippy::too_many_arguments)]
fn spawn_room_dispatcher(
    nc: async_nats::client::Client,
    room: String,
    subject: String,
    sfu_mode: SfuMode,
    forwarder: Arc<Forwarder>,
    scorer: Arc<TokioRwLock<SpeakerScorer>>,
    receivers: Arc<RwLock<HashMap<SessionId, Recipient<Message>>>>,
    // p4-4: per-room SFU state for the bandwidth-estimate ingest path. We
    // hold an `Arc<RwLock<_>>` so the dispatcher can take a short write
    // lock when a client sends a DiagnosticsPacket; the JoinRoom handler
    // owns the same Arc so member inserts/removes remain visible here.
    room_state: Arc<RwLock<RoomState>>,
    chat_server: actix::Addr<ChatServer>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut sub = match nc.subscribe(subject.clone()).await {
            Ok(s) => s,
            Err(e) => {
                error!(
                    "Per-room demux failed to subscribe to {} (room {}): {} — \
                     notifying actor for cleanup/respawn",
                    subject, room, e
                );
                // try_send is fine here: if the actor mailbox is full or
                // the actor is gone, recovery on the next JoinRoom for
                // this room will spawn a fresh dispatcher (the entry is
                // keyed on room, not on this task instance).
                let _ = chat_server.try_send(RoomDispatcherExited { room });
                return;
            }
        };
        info!(
            "Per-room demux subscribed to {} (room {}, mode {:?})",
            subject, room, sfu_mode
        );

        // --- vc-9eh: per-room delivery watchdog -------------------------------
        //
        // ROOT CAUSE (Bug A): under a sustained publisher storm the async-nats
        // wildcard subscription's bounded channel can fill faster than this
        // loop drains it. When that happens async-nats does NOT close the
        // subscription and does NOT block — it *silently drops* the message and
        // fires a connection-global `Event::SlowConsumer(sid)` (see
        // async-nats lib.rs ~L732). The `Subscriber` stream stays open, so
        // `sub.next()` never returns `None`; the existing `None`-exit / respawn
        // path therefore never fires and the subscription becomes a quiet black
        // hole. Every receiver in `receivers` — including any cohort that joins
        // afterward — then stops being served, with no liveness signal at all.
        //
        // We detect this with a liveness watchdog instead of trying to route the
        // connection-global `SlowConsumer` event back to a specific room (the
        // `Subscriber.sid` is private and the event channel is per-`Client`, so
        // there is no per-room hook). A subscription that has gone quiet — for
        // ANY reason (slow-consumer drops, a wedged re-attach across reconnect,
        // a server-side hiccup) — manifests uniformly as `last_msg_at` going
        // stale. On prolonged silence WHILE the room still has receivers AND
        // active publishers, we resubscribe IN PLACE: drop the old `Subscriber`,
        // re-`subscribe` on the SAME task against the SAME `receivers` Arc +
        // forwarder, and continue the loop. Live sessions (including the late
        // cohort) resume the instant the fresh subscription attaches, with no
        // client reconnect and no actor mailbox round-trip.
        //
        // Why IN-PLACE (not posting `RoomDispatcherExited`): the escalating
        // backoff state (`consecutive_silent_trips`) must PERSIST across
        // resubscribes, or it is defeated — a respawn via the actor would reset
        // it to base every time. Keeping the resubscribe local also avoids a
        // resubscribe herd through the actor mailbox on a cluster-wide
        // slow-consumer event. The `None`-exit → `RoomDispatcherExited` path is
        // kept as-is for the genuinely-closed-subscription case.
        //
        // Thresholds + the escalating-window + the gating predicate live in
        // `watchdog_silence_window` / `watchdog_should_resubscribe` above
        // (factored out so they are unit-testable without provoking real
        // backpressure).
        let mut subscribe_at = std::time::Instant::now();
        let mut last_msg_at = subscribe_at;
        // vc-9eh: consecutive watchdog resubscribes NOT followed by traffic.
        // Resets to 0 the moment a real message advances `last_msg_at`. Drives
        // the escalating silence window so a legitimately-quiet populated room
        // decays to ~one resubscribe per WATCHDOG_SILENCE_CAP instead of
        // thrashing at a fixed cadence.
        let mut consecutive_silent_trips: u32 = 0;
        let mut watchdog = tokio::time::interval(WATCHDOG_TICK);
        // The first tick fires immediately; skip it so the very first watchdog
        // evaluation happens one full TICK in (and is anyway gated by GRACE).
        watchdog.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // vc-9eh perf: de-align per-room watchdog timers so a cluster-wide
        // slow-consumer event does not trip every room within the same 250ms
        // tick (a synchronized resubscribe wave against the already-stressed
        // connection). Derive a deterministic 0..TICK phase from the room name
        // so each room's tick lands at a different sub-tick offset; no `rand`
        // dependency and stable across the room's lifetime.
        {
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            room.hash(&mut h);
            let jitter_ms = h.finish() % (WATCHDOG_TICK.as_millis() as u64);
            if jitter_ms > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(jitter_ms)).await;
            }
        }

        loop {
            let msg = tokio::select! {
                biased;
                maybe_msg = sub.next() => match maybe_msg {
                    Some(msg) => {
                        // vc-9eh: subscription is alive — refresh liveness and
                        // reset the backoff so the NEXT stall starts at the base
                        // 750ms window again.
                        last_msg_at = std::time::Instant::now();
                        consecutive_silent_trips = 0;
                        msg
                    }
                    // `None` => subscription closed (existing behavior). Break
                    // out of the loop to the abnormal-exit / respawn notify.
                    None => break,
                },
                _ = watchdog.tick() => {
                    // vc-9eh: liveness check. ZERO per-join work — exactly ONE
                    // timer per ROOM, O(1) resubscribe per room. We read only
                    // data already in scope (the `receivers` map + `room_state`)
                    // and defer the decision to the unit-tested predicate.
                    let now = std::time::Instant::now();
                    let uptime = now.duration_since(subscribe_at);
                    let silence = now.duration_since(last_msg_at);
                    let window = watchdog_silence_window(consecutive_silent_trips);
                    // Cheap pre-gate: avoid taking the locks at all until we are
                    // actually past grace AND past the (escalating) silence
                    // window. The common steady-state path (recent delivery)
                    // bails here without touching state.
                    if uptime < WATCHDOG_GRACE || silence < window {
                        continue;
                    }
                    let (has_receivers, receiver_count) = {
                        let g = match receivers.read() {
                            Ok(g) => g,
                            Err(poisoned) => poisoned.into_inner(),
                        };
                        (!g.is_empty(), g.len())
                    };
                    let has_publishers = {
                        let g = match room_state.read() {
                            Ok(g) => g,
                            Err(poisoned) => poisoned.into_inner(),
                        };
                        g.has_senders()
                    };
                    if !watchdog_should_resubscribe(
                        uptime,
                        silence,
                        window,
                        has_receivers,
                        has_publishers,
                    ) {
                        continue;
                    }
                    // Escalate the backoff: this trip counts toward the next
                    // window. It resets to 0 on the `Some(msg)` arm above the
                    // moment real traffic resumes.
                    consecutive_silent_trips = consecutive_silent_trips.saturating_add(1);
                    warn!(
                        "Per-room demux for room {} saw no inbound message for \
                         {}ms (window {}ms, consecutive silent trip #{}) while {} \
                         receiver(s) and active publishers remain — the \
                         subscription has gone silent (likely slow-consumer \
                         backpressure); resubscribing in place",
                        room,
                        silence.as_millis(),
                        window.as_millis(),
                        consecutive_silent_trips,
                        receiver_count,
                    );
                    // Resubscribe IN PLACE. Drop the old (silent) `Subscriber`
                    // and attach a fresh one on the SAME task. On success the
                    // grace clock restarts so the new subscription gets a full
                    // window to receive before it can be judged dead again.
                    match nc.subscribe(subject.clone()).await {
                        Ok(fresh) => {
                            let now = std::time::Instant::now();
                            sub = fresh;
                            subscribe_at = now;
                            // CRITICAL (anti-thrash): restart the SILENCE clock at
                            // the resubscribe too, not just the grace clock. The
                            // next trip is then measured FROM this resubscribe, so
                            // a persistently-quiet room re-trips only after the
                            // ESCALATED `watchdog_silence_window(trips)` elapses —
                            // decaying the cadence toward the 30s cap. If we left
                            // `last_msg_at` as-is, `silence` would grow
                            // monotonically from the last REAL message and, once it
                            // permanently exceeds the cap, the `silence >= window`
                            // gate would always pass — flooring the cadence at
                            // ~GRACE (≈1s) and spamming WARN forever. The escalation
                            // is keyed on `consecutive_silent_trips` (NOT reset
                            // here), and the `Some(msg)` arm resets BOTH the moment
                            // any real traffic resumes — so genuine-broken recovery
                            // stays fast.
                            last_msg_at = now;
                            info!(
                                "Per-room demux resubscribed in place to {} \
                                 (room {})",
                                subject, room
                            );
                            continue;
                        }
                        Err(e) => {
                            // Could not re-establish locally — fall back to the
                            // actor respawn path (it reuses the same receivers
                            // Arc + forwarder) and exit this task.
                            warn!(
                                "Per-room demux failed to resubscribe to {} \
                                 (room {}): {} — handing off to actor respawn",
                                subject, room, e
                            );
                            let _ = chat_server
                                .try_send(RoomDispatcherExited { room: room.clone() });
                            return;
                        }
                    }
                }
            };

            // Parse ONCE per inbound message — the whole point of vc-q0v.
            // `msg.payload` is `bytes::Bytes`; deref to `&[u8]` for the
            // parser. The decision call below takes the `&Bytes` so it
            // can `clone()` a refcount bump per forwarded receiver.
            let parsed = parse_and_inspect(&msg.payload[..]);
            let subject_str: &str = msg.subject.as_ref();

            // p3-11: feed the per-room SpeakerScorer for every inbound
            // AUDIO MediaPacket whose RoutingHeader carries an
            // `audio_level`. The SpeakerTick reads the scorer on its
            // 200ms cadence to maintain the ActiveSpeakerSet snapshot
            // that the forwarder consumes (via watch::Receiver) and that
            // is broadcast to clients on `room.{room}.system`.
            //
            // Gated on SFU mode: in legacy mode the scorer is never
            // consulted (the forwarder is bypassed), so the write is pure
            // waste — skip it entirely on the legacy hot path.
            if sfu_mode == SfuMode::Sfu {
                if let Some(p) = parsed.as_ref() {
                    if let Some(rh) = p.routing_header() {
                        let is_audio = p
                            .media_packet
                            .as_ref()
                            .map(|mp| mp.media_type == MediaType::AUDIO.into())
                            .unwrap_or(false);
                        if is_audio {
                            let sender_sid = p.wrapper.session_id;
                            let level = rh.audio_level;
                            let hint = rh.is_speaking;
                            // Short critical section: `observe()` is sync.
                            // The competing acquirer is `tick_once` taking
                            // a read for a snapshot copy; neither holds
                            // the lock across other awaits.
                            scorer.write().await.observe(sender_sid, level, hint);
                        }
                    }
                }
            }

            // p4-4: DiagnosticsPacket ingest — record the sender's most
            // recent receiver-downlink bandwidth estimate on its
            // MemberEntry so the LayerSelector (p4-5) can budget per-
            // receiver layer selection. We do NOT consume the packet:
            // diagnostics continue through the existing fan-out so peers
            // who today forward/inspect diagnostics keep seeing them.
            //
            // Gated on SfuMode::Sfu (mirrors the scorer gate above):
            // legacy mode bypasses the forwarder, so storing the estimate
            // would be pure waste.
            //
            // Cost: one extra `DiagnosticsPacket::parse_from_bytes` per
            // inbound DIAGNOSTICS message (the client emits per peer × per
            // media-type on its `report_interval_ms` cadence, ~2Hz by
            // default). Negligible relative to the MEDIA hot path.
            if sfu_mode == SfuMode::Sfu {
                if let Some(p) = parsed.as_ref() {
                    if p.wrapper.packet_type == PacketType::DIAGNOSTICS.into() {
                        match DiagnosticsPacket::parse_from_bytes(&p.wrapper.data) {
                            Ok(diag) => {
                                if let Some(est) = diag.bandwidth_estimate.as_ref() {
                                    let sender_sid = p.wrapper.session_id;
                                    debug!(
                                        "received bandwidth estimate from {}: \
                                         {}kbps rtt={}ms loss={}",
                                        sender_sid,
                                        est.estimated_downlink_kbps,
                                        est.rtt_ms,
                                        est.estimated_loss_rate,
                                    );
                                    // Tight critical section: only the
                                    // `update_bandwidth_estimate` call holds the
                                    // write lock; no awaits within. Poison-safe
                                    // pattern matches the rest of this file.
                                    let should_invalidate = {
                                        let mut guard = match room_state.write() {
                                            Ok(g) => g,
                                            Err(poisoned) => poisoned.into_inner(),
                                        };
                                        guard.update_bandwidth_estimate(sender_sid, est)
                                    };
                                    // p4-7: invalidate the cached layer
                                    // selection for this receiver so the
                                    // next `decide()` call recomputes
                                    // against the fresh bandwidth budget.
                                    // The DiagnosticsPacket's sender is
                                    // the receiver whose downlink changed
                                    // — they're reporting their OWN
                                    // bandwidth back to the SFU.
                                    // vc-wls: lock-free per-key invalidate.
                                    //
                                    // vc-17e: skip both the invalidate AND
                                    // the underlying `bandwidth_estimate`
                                    // overwrite when the new value is
                                    // within noise. The forwarder's cache-
                                    // validity check is exact equality on
                                    // `bandwidth_kbps` (forwarder.rs), so
                                    // a chatty client that we let through
                                    // here would still miss the cache and
                                    // force a recompute every tick — the
                                    // suppression has to be paired in
                                    // `update_bandwidth_estimate` to be
                                    // load-bearing.
                                    if should_invalidate {
                                        forwarder
                                            .layer_selector()
                                            .invalidate_for_receiver(sender_sid);
                                    }
                                }
                            }
                            Err(e) => {
                                trace!(
                                    "DiagnosticsPacket parse failed for session {} in room {}: {}",
                                    p.wrapper.session_id,
                                    room,
                                    e
                                );
                            }
                        }
                    }
                }
            }

            // Snapshot the recipients under the read lock, then drop it
            // before fan-out. Recipient<_> is Clone (Arc-backed inside
            // actix), so per-message cloning is cheap; this keeps the
            // join/leave write path from being blocked for the full
            // fan-out (which calls try_send N times).
            let snapshot: Vec<(SessionId, Recipient<Message>)> = {
                let guard = match receivers.read() {
                    Ok(g) => g,
                    Err(poisoned) => poisoned.into_inner(),
                };
                guard.iter().map(|(sid, r)| (*sid, r.clone())).collect()
            };

            for (rsid, recipient) in snapshot {
                if let Some(bytes) = egress_decide_from_parsed(
                    sfu_mode,
                    &forwarder,
                    rsid,
                    &room,
                    subject_str,
                    &msg.payload,
                    parsed.as_ref(),
                ) {
                    if let Err(e) = recipient.try_send(Message {
                        msg: bytes,
                        session: rsid,
                    }) {
                        warn!(
                            "Dropping inbound message for session {}: {} \
                             (mailbox full — subscription continues)",
                            rsid, e
                        );
                    }
                }
            }
        }
        // `sub.next()` returned None (the `None` arm `break`s the loop) — the
        // subscription is closed. This is
        // unexpected during normal operation (async-nats is supposed to
        // transparently re-attach subscriptions across reconnects). Surface
        // loudly and ask the actor to either respawn (receivers still
        // present) or drop the entry (room already drained). A normal
        // drain reaches this point via `.abort()` before the next `.await`,
        // so this log specifically marks abnormal exits — though `.abort()`
        // can race past the await, in which case the actor sees a stale
        // RoomDispatcherExited and a no-op cleanup, which is harmless.
        warn!(
            "Per-room demux subscription closed unexpectedly for room {} — \
             notifying actor for cleanup/respawn",
            room
        );
        let _ = chat_server.try_send(RoomDispatcherExited { room });
    })
}

async fn send_meeting_info(
    nc: &async_nats::client::Client,
    room: &str,
    start_time_ms: u64,
    creator_id: &str,
) {
    let packet_bytes =
        SessionManager::build_meeting_started_packet(room, start_time_ms, creator_id);

    let subject = format!("room.{}.system", room.replace(' ', "_"));
    match nc.publish(subject.clone(), packet_bytes.into()).await {
        Ok(_) => info!("Sent meeting start time {} to {}", start_time_ms, subject),
        Err(e) => error!("Failed to send meeting info to room {}: {}", room, e),
    }
}

/// Parse-and-decide helper for tests.
///
/// Production fan-out runs through [`spawn_room_dispatcher`] +
/// [`egress_decide_from_parsed`], which parses each inbound NATS message
/// exactly once per room (vc-q0v). This wrapper parses on every call and
/// is retained for test consumers — `sfu::tests::forwarder_parity_tests`
/// (legacy-vs-SFU golden-trace parity), `sfu::tests::parse_once_tests`
/// (the parse-per-receiver reference oracle), and the `#[cfg(test)]`
/// `handle_msg` helper. None of these need to amortize parsing across N
/// receivers, so they exercise the per-receiver decision path directly.
///
/// Semantics preserved bit-for-bit from the pre-extraction closure:
///
/// 1. **Self-skip with CONGESTION carve-out.** If `subject` is the receiver's
///    own publish subject, the message is dropped *unless* the parsed wrapper
///    is a `CONGESTION` packet (server-published back-off signal that all
///    peers must still receive).
/// 2. **Legacy mode** delivers every non-self-skipped payload byte-for-byte.
/// 3. **SFU mode** runs the forwarder's per-receiver `decide`. The
///    server-published CONGESTION class is carve-out-broadcast (see the
///    CRITICAL comment inline) and parse failures fall through to a tolerant
///    "forward as-is" — matching legacy so we never black-hole unparseable
///    payloads.
#[cfg(test)]
pub(crate) fn egress_decide_bytes(
    sfu_mode: SfuMode,
    forwarder: &Forwarder,
    receiver_session: SessionId,
    room: &str,
    subject: &str,
    payload: &bytes::Bytes,
) -> Option<bytes::Bytes> {
    let parsed = parse_and_inspect(&payload[..]);
    egress_decide_from_parsed(
        sfu_mode,
        forwarder,
        receiver_session,
        room,
        subject,
        payload,
        parsed.as_ref(),
    )
}

/// Per-receiver egress decision, given a *pre-parsed* wrapper.
///
/// The per-room demux task (see [`spawn_room_dispatcher`]) calls
/// [`parse_and_inspect`] once per inbound NATS message and then invokes this
/// function once per receiver. This avoids the O(N) parse fan-out that the
/// pre-vc-q0v per-session subscription model incurred (~6k extra parses/s
/// per active 30 fps sender per pod with a 200-participant room).
///
/// Semantics are identical to [`egress_decide_bytes`]; the only difference
/// is who owns the parse. `parsed` is `None` when the wrapper failed to
/// parse — preserving the pre-existing tolerant "forward as-is" behavior on
/// the SFU path so we never black-hole encrypted-or-unparseable payloads.
pub(crate) fn egress_decide_from_parsed(
    sfu_mode: SfuMode,
    forwarder: &Forwarder,
    receiver_session: SessionId,
    room: &str,
    subject: &str,
    payload: &bytes::Bytes,
    parsed: Option<&ParsedPacket>,
) -> Option<bytes::Bytes> {
    // p6-7 / vc-kol follow-up: HEALTH_BEACON is server-internal. It is
    // published on `room.{room}.system` by the owner pod for the spill
    // controller (p6-8) to consume, and is never relevant to any client.
    // Without this drop, the dispatcher's wildcard subscription
    // (`room.{room}.*`) would fan the ~70 B beacon out to every connected
    // receiver every 5 s — ~8 KB/min/client of pure overhead on mobile.
    //
    // Mode-independent and earliest-possible: applied before self-skip
    // so we never echo a beacon, applied in both SFU and Legacy modes
    // because the beacon has no place on either client path. The client-
    // side handler (`videocall-client/src/client/video_call_client.rs`)
    // also ignores it as defense-in-depth, but the drop MUST happen here
    // on the server so we don't burn the bytes on the wire.
    if let Some(p) = parsed {
        if p.wrapper.packet_type == PacketType::HEALTH_BEACON.into() {
            return None;
        }
    }

    let self_subject = format!("room.{room}.{receiver_session}").replace(' ', "_");
    if subject == self_subject.as_str() {
        // Self-skip prevents echo of our own broadcasts. However,
        // CONGESTION signals published on our subject by a congested
        // receiver must still be delivered — they are not echo.
        let is_congestion = parsed
            .map(|p| p.wrapper.packet_type == PacketType::CONGESTION.into())
            .unwrap_or(false);
        if !is_congestion {
            return None;
        }
    }

    match sfu_mode {
        SfuMode::Sfu => {
            // CRITICAL carve-out — preserve legacy broadcast semantics
            // for CONGESTION. Without this, the forwarder's per-receiver
            // filter could drop CONGESTION packets that all peers MUST
            // receive so they can back off. The full priority-class
            // carve-out (HEARTBEAT, RTT, SESSION_ASSIGNED, MEETING_*,
            // etc.) lands in P5 — see PLAN.md Phase 5 priority-queue
            // table.
            let is_congestion = parsed
                .map(|p| p.wrapper.packet_type == PacketType::CONGESTION.into())
                .unwrap_or(false);

            if is_congestion {
                Some(payload.clone())
            } else if let Some(p) = parsed {
                match forwarder.decide(receiver_session, &p.wrapper, p.media_packet.as_ref()) {
                    // Reuse the original on-wire NATS payload — no per-receiver
                    // re-serialization of an identical PacketWrapper. `Bytes::clone`
                    // is a refcount bump, so every receiver shares one allocation.
                    ForwardDecision::Forward => Some(payload.clone()),
                    ForwardDecision::Drop => {
                        // p2-7 will increment a dropped-packet counter here.
                        None
                    }
                }
            } else {
                // Parse failure — match the pre-existing tolerant
                // behavior: forward bytes as-is so we don't black-hole
                // encrypted-or-unparseable payloads on the SFU path.
                // Note: the mode-independent self-echo skip above
                // already dropped any self-addressed unparseable
                // payloads before we got here, so SFU and legacy
                // diverge only on *non-self-addressed* unparseable
                // payloads — which don't occur in practice (only
                // senders mint these, and they hit the self-skip).
                Some(payload.clone())
            }
        }
        SfuMode::Legacy => Some(payload.clone()),
    }
}

/// Per-receiver subscription handler used by the unit test that exercises
/// the SFU CONGESTION bypass branch (see
/// `test_sfu_mode_congestion_bypasses_forwarder`). Production fan-out runs
/// through [`spawn_room_dispatcher`] and [`egress_decide_from_parsed`]
/// instead, which parses each inbound NATS message exactly once per room
/// rather than once per receiver (vc-q0v).
#[cfg(test)]
fn handle_msg(
    session_recipient: Recipient<Message>,
    room: String,
    session: SessionId,
    sfu_mode: SfuMode,
    forwarder: Arc<Forwarder>,
) -> impl Fn(async_nats::Message) -> Result<(), std::io::Error> {
    move |msg| {
        let subject_str: &str = msg.subject.as_ref();
        if let Some(bytes) = egress_decide_bytes(
            sfu_mode,
            &forwarder,
            session,
            &room,
            subject_str,
            &msg.payload,
        ) {
            if let Err(e) = session_recipient.try_send(Message {
                msg: bytes,
                session,
            }) {
                warn!(
                    "Dropping inbound message for session {}: {} (mailbox full — subscription continues)",
                    session, e
                );
            }
        }

        Ok(())
    }
}

// ==========================================================================
// Test-only query: snapshot a room's SFU member table.
// ==========================================================================
// Returns `None` if the room has no SFU state. Otherwise returns a vector of
// `(session_id, capabilities)` pairs sorted by session_id for determinism.
#[cfg(test)]
#[derive(actix::Message)]
#[rtype(result = "Option<Vec<(SessionId, u32)>>")]
struct SnapshotRoomMembers {
    room: String,
}

#[cfg(test)]
impl Handler<SnapshotRoomMembers> for ChatServer {
    type Result = Option<Vec<(SessionId, u32)>>;

    fn handle(
        &mut self,
        SnapshotRoomMembers { room }: SnapshotRoomMembers,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let state = self.room_states.get(&room)?;
        let guard = match state.read() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        let mut entries: Vec<(SessionId, u32)> = guard
            .members
            .values()
            .map(|m| (m.session_id, m.capabilities))
            .collect();
        entries.sort_by_key(|(sid, _)| *sid);
        Some(entries)
    }
}

// ==========================================================================
// Test-only command: seed the spillover store for a room (vc-85p / p6-5).
// ==========================================================================
// Records a synthetic owner-pod health snapshot so the JoinRoom spill
// decision branch can be exercised deterministically without standing up a
// real owner pod publishing beacons. `last_seen` is set to `Instant::now()`
// inside the handler, so a seeded over-threshold snapshot is always fresh
// for the duration of a test.
#[cfg(test)]
#[derive(actix::Message)]
#[rtype(result = "()")]
struct SeedSpilloverState {
    room: String,
    owner_count: u32,
    owner_cpu: f32,
}

#[cfg(test)]
impl Handler<SeedSpilloverState> for ChatServer {
    type Result = ();

    fn handle(
        &mut self,
        SeedSpilloverState {
            room,
            owner_count,
            owner_cpu,
        }: SeedSpilloverState,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        // The ingest task stores subject-derived (underscore-normalized)
        // keys; mirror that here so the JoinRoom lookup (which normalizes
        // its raw room id the same way) hits the seeded entry.
        let key = room.replace(' ', "_");
        self.spillover_store.record(
            &key,
            crate::sfu::spillover::RoomSpilloverState {
                owner_count,
                owner_cpu,
                last_seen: std::time::Instant::now(),
            },
        );
    }
}

// ==========================================================================
// Test-only query: snapshot a (room, user) pending-departure entry.
// ==========================================================================
// Returns `Some(old_session)` if a deferred PARTICIPANT_LEFT is pending for
// the (room, user) key, `None` otherwise. Used by the p6-5 follow-up
// reconnection-into-redirect test to assert the redirect path runs BEFORE
// the reconnection bookkeeping (i.e. doesn't drain the pending_departures
// entry on its way to declining the join).
#[cfg(test)]
#[derive(actix::Message)]
#[rtype(result = "Option<SessionId>")]
struct SnapshotPendingDeparture {
    room: String,
    user_id: String,
}

#[cfg(test)]
impl Handler<SnapshotPendingDeparture> for ChatServer {
    type Result = Option<SessionId>;

    fn handle(
        &mut self,
        SnapshotPendingDeparture { room, user_id }: SnapshotPendingDeparture,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        self.pending_departures
            .get(&(room, user_id))
            .map(|p| p.old_session)
    }
}

// ==========================================================================
// Test-only query: snapshot the legacy room_members table for a room.
// ==========================================================================
// Mirrors SnapshotRoomMembers but returns the (room_members) tuple rather
// than the SFU member table — used to assert that the early redirect path
// does NOT touch the user-visible membership list before declining a join.
#[cfg(test)]
#[derive(actix::Message)]
#[rtype(result = "Option<Vec<(SessionId, String, String)>>")]
struct SnapshotRoomMembersList {
    room: String,
}

#[cfg(test)]
impl Handler<SnapshotRoomMembersList> for ChatServer {
    type Result = Option<Vec<(SessionId, String, String)>>;

    fn handle(
        &mut self,
        SnapshotRoomMembersList { room }: SnapshotRoomMembersList,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        self.room_members.get(&room).cloned()
    }
}

// ==========================================================================
// Unit Tests for ChatServer
// ==========================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use actix::Actor;
    use serial_test::serial;

    /// Test helper: create a database pool for integration tests.
    /// Kept for future JWT flow testing (create meeting -> get JWT -> connect via WS/WT).
    #[allow(dead_code)]
    async fn get_test_pool() -> sqlx::PgPool {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set for tests");
        sqlx::PgPool::connect(&database_url)
            .await
            .expect("Failed to connect to test database")
    }

    // ==========================================================================
    // TEST: JoinRoom rejects reserved system user ID synchronously
    // ==========================================================================
    // This test verifies the fix for the race condition where JoinRoom would
    // spawn an async task and immediately return Ok(()), even if validation
    // would fail inside the task. Now validation happens synchronously.
    #[actix_rt::test]
    #[serial]
    async fn test_join_room_rejects_system_user_id_synchronously() {
        let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());
        let nats_client = async_nats::connect(&nats_url)
            .await
            .expect("Failed to connect to NATS");

        // Start the ChatServer actor
        let chat_server = ChatServer::new(nats_client).await.start();

        // Create a mock session recipient
        // We need a real actor to receive messages, so we use a simple dummy
        struct DummySession;
        impl Actor for DummySession {
            type Context = actix::Context<Self>;
        }
        impl Handler<Message> for DummySession {
            type Result = ();
            fn handle(&mut self, _msg: Message, _ctx: &mut Self::Context) {}
        }

        let dummy = DummySession.start();
        let session_id = 1001u64;

        // Register the session first
        chat_server
            .send(Connect {
                id: session_id,
                addr: dummy.recipient(),
            })
            .await
            .expect("Connect should succeed");

        // Attempt to join with the reserved system user ID
        // This should return an error SYNCHRONOUSLY (not Ok then fail async)
        let result = chat_server
            .send(JoinRoom {
                session: session_id,
                room: "test-room".to_string(),
                user_id: SYSTEM_USER_ID.to_string(),
                display_name: SYSTEM_USER_ID.to_string(),
                observer: false,
                capabilities: 0,
            })
            .await
            .expect("Message delivery should succeed");

        // The key assertion: JoinRoom should return Err immediately
        assert!(
            result.is_err(),
            "JoinRoom with system user ID should return Err, not Ok"
        );

        let error_msg = result.unwrap_err();
        assert!(
            error_msg.contains("reserved system user ID"),
            "Error should mention reserved system user ID, got: {error_msg}"
        );
    }

    // ==========================================================================
    // TEST: JoinRoom succeeds with valid user_id
    // ==========================================================================
    #[actix_rt::test]
    #[serial]
    async fn test_join_room_succeeds_with_valid_user() {
        let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());
        let nats_client = async_nats::connect(&nats_url)
            .await
            .expect("Failed to connect to NATS");

        let chat_server = ChatServer::new(nats_client).await.start();

        struct DummySession;
        impl Actor for DummySession {
            type Context = actix::Context<Self>;
        }
        impl Handler<Message> for DummySession {
            type Result = ();
            fn handle(&mut self, _msg: Message, _ctx: &mut Self::Context) {}
        }

        let dummy = DummySession.start();
        let session_id = 1002u64;

        // Register the session
        chat_server
            .send(Connect {
                id: session_id,
                addr: dummy.recipient(),
            })
            .await
            .expect("Connect should succeed");

        // Join with a valid user_id - should succeed
        let result = chat_server
            .send(JoinRoom {
                session: session_id,
                room: "test-room-valid".to_string(),
                user_id: "valid-user@example.com".to_string(),
                display_name: "valid-user@example.com".to_string(),
                observer: false,
                capabilities: 0,
            })
            .await
            .expect("Message delivery should succeed");

        assert!(
            result.is_ok(),
            "JoinRoom with valid user should return Ok, got: {result:?}"
        );
    }

    // ==========================================================================
    // TEST: JoinRoom fails if session not registered
    // ==========================================================================
    #[actix_rt::test]
    #[serial]
    async fn test_join_room_fails_without_session() {
        let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());
        let nats_client = async_nats::connect(&nats_url)
            .await
            .expect("Failed to connect to NATS");

        let chat_server = ChatServer::new(nats_client).await.start();

        // Try to join WITHOUT registering the session first
        let result = chat_server
            .send(JoinRoom {
                session: 9999u64,
                room: "test-room".to_string(),
                user_id: "valid-user@example.com".to_string(),
                display_name: "valid-user@example.com".to_string(),
                observer: false,
                capabilities: 0,
            })
            .await
            .expect("Message delivery should succeed");

        assert!(
            result.is_err(),
            "JoinRoom without registered session should return Err"
        );
        assert!(
            result.unwrap_err().contains("Session not found"),
            "Error should mention session not found"
        );
    }

    // ==========================================================================
    // TEST: Duplicate join with same session returns Ok
    // ==========================================================================
    // Verifies that a second JoinRoom for the same session_id returns Ok
    // immediately because the session is already tracked in joined_sessions.
    #[actix_rt::test]
    #[serial]
    async fn test_duplicate_join_same_session_returns_ok() {
        let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());
        let nats_client = async_nats::connect(&nats_url)
            .await
            .expect("Failed to connect to NATS");

        let chat_server = ChatServer::new(nats_client).await.start();

        struct DummySession;
        impl Actor for DummySession {
            type Context = actix::Context<Self>;
        }
        impl Handler<Message> for DummySession {
            type Result = ();
            fn handle(&mut self, _msg: Message, _ctx: &mut Self::Context) {}
        }

        let dummy = DummySession.start();
        let session_id = 1003u64;

        // Register the session
        chat_server
            .send(Connect {
                id: session_id,
                addr: dummy.recipient(),
            })
            .await
            .expect("Connect should succeed");

        // First join attempt - should succeed (returns Ok immediately,
        // spawns async task which will also succeed with valid user)
        let result1 = chat_server
            .send(JoinRoom {
                session: session_id,
                room: "test-room-cleanup".to_string(),
                user_id: "valid-user@example.com".to_string(),
                display_name: "valid-user@example.com".to_string(),
                observer: false,
                capabilities: 0,
            })
            .await
            .expect("Message delivery should succeed");

        assert!(result1.is_ok(), "First join should succeed");

        // Second join attempt with same session - should return Ok
        // immediately because session is already in joined_sessions
        let result2 = chat_server
            .send(JoinRoom {
                session: session_id,
                room: "test-room-cleanup".to_string(),
                user_id: "valid-user@example.com".to_string(),
                display_name: "valid-user@example.com".to_string(),
                observer: false,
                capabilities: 0,
            })
            .await
            .expect("Message delivery should succeed");

        assert!(
            result2.is_ok(),
            "Second join with same session should return Ok (already active)"
        );
    }

    // ==========================================================================
    // TEST: Two clients with same user_id get unique session_id values
    // ==========================================================================
    #[actix_rt::test]
    #[serial]
    async fn test_same_user_id_unique_session_ids() {
        use crate::actors::session_logic::SessionLogic;
        use crate::server_diagnostics::{TrackerMessage, TrackerSender};
        use crate::session_manager::SessionManager;
        use tokio::sync::mpsc;

        let _pool = get_test_pool().await;
        let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());
        let nats_client = async_nats::connect(&nats_url)
            .await
            .expect("Failed to connect to NATS");

        let chat_server = ChatServer::new(nats_client.clone()).await.start();

        let (tx, _rx) = mpsc::unbounded_channel::<TrackerMessage>();
        let tracker_sender: TrackerSender = tx;
        let session_manager = SessionManager::new();

        // Create two sessions with the same user_id
        let user_id = "same-user@example.com".to_string();
        let room = "test-room-unique".to_string();

        let session1 = SessionLogic::new(
            chat_server.clone(),
            room.clone(),
            user_id.clone(),
            user_id.clone(), // display_name fallback
            nats_client.clone(),
            tracker_sender.clone(),
            session_manager.clone(),
            false,
        );

        let session2 = SessionLogic::new(
            chat_server.clone(),
            room.clone(),
            user_id.clone(),
            user_id.clone(), // display_name fallback
            nats_client.clone(),
            tracker_sender.clone(),
            session_manager.clone(),
            false,
        );

        // Verify they have different session IDs
        assert_ne!(
            session1.id, session2.id,
            "Two sessions with same user_id should have different session_id values"
        );
        assert!(session1.id != 0, "Session ID should not be zero");
        assert!(session2.id != 0, "Session ID should not be zero");
    }

    // ==========================================================================
    // TEST: ConnectionState transitions - Testing does not publish to NATS
    // ==========================================================================
    #[actix_rt::test]
    #[serial]
    async fn test_connection_state_testing_does_not_publish() {
        use crate::messages::server::Packet;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        use tokio::time::{sleep, Duration};

        let _pool = get_test_pool().await;
        let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());
        let nats_client = async_nats::connect(&nats_url)
            .await
            .expect("Failed to connect to NATS");

        let chat_server = ChatServer::new(nats_client.clone()).await.start();

        struct DummySession;
        impl Actor for DummySession {
            type Context = actix::Context<Self>;
        }
        impl Handler<Message> for DummySession {
            type Result = ();
            fn handle(&mut self, _msg: Message, _ctx: &mut Self::Context) {}
        }

        let dummy = DummySession.start();
        let session_id = 1004u64;
        let room = "test-room-state".to_string();

        // Register session - starts in Testing state
        chat_server
            .send(Connect {
                id: session_id,
                addr: dummy.recipient(),
            })
            .await
            .expect("Connect should succeed");

        let subject = format!("room.{room}.{session_id}").replace(' ', "_");
        let published = Arc::new(AtomicBool::new(false));
        let published_clone = published.clone();
        let mut sub = nats_client
            .subscribe(subject.clone())
            .await
            .expect("Failed to subscribe");

        tokio::spawn(async move {
            if let Ok(Some(_msg)) =
                tokio::time::timeout(Duration::from_millis(500), sub.next()).await
            {
                published_clone.store(true, Ordering::Relaxed);
            }
        });

        // Send message while in Testing state - should NOT publish
        chat_server
            .send(ClientMessage {
                session: session_id,
                room: room.clone(),
                msg: Packet {
                    data: Arc::new(b"test data".to_vec()),
                    kind: PacketKind::Data,
                },
                user: "test@example.com".to_string(),
            })
            .await
            .expect("Message delivery should succeed");

        // Wait a bit to ensure no publish happened
        sleep(Duration::from_millis(600)).await;

        assert!(
            !published.load(Ordering::Relaxed),
            "Message should NOT be published while in Testing state"
        );
    }

    // ==========================================================================
    // TEST: ConnectionState transitions - Active publishes to NATS
    // ==========================================================================
    #[actix_rt::test]
    #[serial]
    async fn test_connection_state_active_publishes() {
        use crate::messages::server::Packet;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        use tokio::time::{sleep, Duration};

        let _pool = get_test_pool().await;
        let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());
        let nats_client = async_nats::connect(&nats_url)
            .await
            .expect("Failed to connect to NATS");

        let chat_server = ChatServer::new(nats_client.clone()).await.start();

        struct DummySession;
        impl Actor for DummySession {
            type Context = actix::Context<Self>;
        }
        impl Handler<Message> for DummySession {
            type Result = ();
            fn handle(&mut self, _msg: Message, _ctx: &mut Self::Context) {}
        }

        let dummy = DummySession.start();
        let session_id = 1005u64;
        let room = "test-room-active".to_string();

        // Register session - starts in Testing state
        chat_server
            .send(Connect {
                id: session_id,
                addr: dummy.recipient(),
            })
            .await
            .expect("Connect should succeed");

        // Activate the connection
        chat_server
            .send(ActivateConnection {
                session: session_id,
            })
            .await
            .expect("ActivateConnection should succeed");

        let subject = format!("room.{room}.{session_id}").replace(' ', "_");
        let published = Arc::new(AtomicBool::new(false));
        let published_clone = published.clone();
        let mut sub = nats_client
            .subscribe(subject.clone())
            .await
            .expect("Failed to subscribe");

        tokio::spawn(async move {
            if let Ok(Some(_msg)) =
                tokio::time::timeout(Duration::from_millis(500), sub.next()).await
            {
                published_clone.store(true, Ordering::Relaxed);
            }
        });

        // Send message while in Active state - should publish
        chat_server
            .send(ClientMessage {
                session: session_id,
                room: room.clone(),
                msg: Packet {
                    data: Arc::new(b"test data".to_vec()),
                    kind: PacketKind::Data,
                },
                user: "test@example.com".to_string(),
            })
            .await
            .expect("Message delivery should succeed");

        // Wait for publish
        sleep(Duration::from_millis(600)).await;

        assert!(
            published.load(Ordering::Relaxed),
            "Message should be published while in Active state"
        );
    }

    // ==========================================================================
    // TEST: ActivateConnection handler is idempotent
    // ==========================================================================
    #[actix_rt::test]
    #[serial]
    async fn test_activate_connection_idempotent() {
        let _pool = get_test_pool().await;
        let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());
        let nats_client = async_nats::connect(&nats_url)
            .await
            .expect("Failed to connect to NATS");

        let chat_server = ChatServer::new(nats_client).await.start();

        struct DummySession;
        impl Actor for DummySession {
            type Context = actix::Context<Self>;
        }
        impl Handler<Message> for DummySession {
            type Result = ();
            fn handle(&mut self, _msg: Message, _ctx: &mut Self::Context) {}
        }

        let dummy = DummySession.start();
        let session_id = 1006u64;

        // Register session - starts in Testing state
        chat_server
            .send(Connect {
                id: session_id,
                addr: dummy.recipient(),
            })
            .await
            .expect("Connect should succeed");

        // First activation - should transition Testing -> Active
        chat_server
            .send(ActivateConnection {
                session: session_id,
            })
            .await
            .expect("ActivateConnection should succeed");

        // Verify state is Active
        let state1 = chat_server
            .send(GetConnectionState {
                session: session_id,
            })
            .await
            .expect("GetConnectionState should succeed")
            .expect("GetConnectionState should return Ok");
        assert_eq!(
            state1,
            ConnectionState::Active,
            "State should be Active after first activation"
        );

        // Second activation - should remain Active (idempotent)
        chat_server
            .send(ActivateConnection {
                session: session_id,
            })
            .await
            .expect("ActivateConnection should succeed");

        // Verify state is still Active
        let state2 = chat_server
            .send(GetConnectionState {
                session: session_id,
            })
            .await
            .expect("GetConnectionState should succeed")
            .expect("GetConnectionState should return Ok");
        assert_eq!(
            state2,
            ConnectionState::Active,
            "State should remain Active after second activation (idempotent)"
        );
    }

    // ==========================================================================
    // TEST: JoinRoom broadcasts MEETING_STARTED via NATS (no session_id)
    // ==========================================================================
    #[actix_rt::test]
    #[serial]
    async fn test_join_room_broadcasts_meeting_started() {
        use std::sync::{Arc, Mutex};
        use tokio::time::{sleep, Duration};
        use videocall_types::protos::packet_wrapper::packet_wrapper::PacketType;

        let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());
        let nats_client = async_nats::connect(&nats_url)
            .await
            .expect("Failed to connect to NATS");

        let chat_server = ChatServer::new(nats_client).await.start();

        let received: Arc<Mutex<Vec<bytes::Bytes>>> = Arc::new(Mutex::new(Vec::new()));

        struct CapturingSession {
            received: Arc<Mutex<Vec<bytes::Bytes>>>,
        }
        impl Actor for CapturingSession {
            type Context = actix::Context<Self>;
        }
        impl Handler<Message> for CapturingSession {
            type Result = ();
            fn handle(&mut self, msg: Message, _ctx: &mut Self::Context) {
                self.received.lock().unwrap().push(msg.msg);
            }
        }

        let capturing = CapturingSession {
            received: received.clone(),
        }
        .start();
        let session_id = 1007u64;

        chat_server
            .send(Connect {
                id: session_id,
                addr: capturing.recipient(),
            })
            .await
            .expect("Connect should succeed");

        let result = chat_server
            .send(JoinRoom {
                session: session_id,
                room: "test-room-broadcast".to_string(),
                user_id: "alice@example.com".to_string(),
                display_name: "alice@example.com".to_string(),
                observer: false,
                capabilities: 0,
            })
            .await
            .expect("Message delivery should succeed");

        assert!(result.is_ok(), "JoinRoom should succeed");

        // Wait for the spawned async task to complete and NATS subscription to deliver
        sleep(Duration::from_millis(500)).await;

        let msgs = received.lock().unwrap();
        // The session should NOT receive SESSION_ASSIGNED from ChatServer
        // (that's the transport layer's job). It may receive MEETING_STARTED
        // via NATS if the subscription was set up in time.
        for msg_bytes in msgs.iter() {
            if let Ok(wrapper) = <PacketWrapper as ProtobufMessage>::parse_from_bytes(msg_bytes) {
                assert_ne!(
                    wrapper.packet_type,
                    PacketType::SESSION_ASSIGNED.into(),
                    "ChatServer JoinRoom should NOT send SESSION_ASSIGNED directly"
                );
                if wrapper.packet_type == PacketType::MEETING.into() {
                    assert_eq!(
                        wrapper.session_id, 0,
                        "MEETING_STARTED must not carry session_id"
                    );
                }
            }
        }
    }

    // ==========================================================================
    // TEST: Observer JoinRoom does NOT publish PARTICIPANT_JOINED
    // ==========================================================================
    // When an observer (waiting room user) joins a room, the server should NOT
    // publish a PARTICIPANT_JOINED event to NATS. Only real participants trigger
    // this notification.
    #[actix_rt::test]
    #[serial]
    async fn test_observer_join_does_not_publish_participant_joined() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        use tokio::time::{sleep, Duration};

        let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());
        let nats_client = async_nats::connect(&nats_url)
            .await
            .expect("Failed to connect to NATS");

        let chat_server = ChatServer::new(nats_client.clone()).await.start();

        struct DummySession;
        impl Actor for DummySession {
            type Context = actix::Context<Self>;
        }
        impl Handler<Message> for DummySession {
            type Result = ();
            fn handle(&mut self, _msg: Message, _ctx: &mut Self::Context) {}
        }

        let dummy = DummySession.start();
        let session_id = 2001u64;
        let room = "test-room-observer-join";

        // Subscribe to the system subject for this room BEFORE join
        let system_subject = format!("room.{}.system", room.replace(' ', "_"));
        let participant_joined_received = Arc::new(AtomicBool::new(false));
        let flag = participant_joined_received.clone();
        let mut sub = nats_client
            .subscribe(system_subject)
            .await
            .expect("Failed to subscribe to system subject");

        tokio::spawn(async move {
            use videocall_types::protos::meeting_packet::meeting_packet::MeetingEventType;
            use videocall_types::protos::meeting_packet::MeetingPacket;

            while let Ok(Some(msg)) =
                tokio::time::timeout(Duration::from_millis(1500), sub.next()).await
            {
                if let Ok(wrapper) =
                    <PacketWrapper as ProtobufMessage>::parse_from_bytes(&msg.payload)
                {
                    if let Ok(inner) = MeetingPacket::parse_from_bytes(&wrapper.data) {
                        if inner.event_type == MeetingEventType::PARTICIPANT_JOINED.into() {
                            flag.store(true, Ordering::Relaxed);
                        }
                    }
                }
            }
        });

        // Register session
        chat_server
            .send(Connect {
                id: session_id,
                addr: dummy.recipient(),
            })
            .await
            .expect("Connect should succeed");

        // Join as observer - should NOT publish PARTICIPANT_JOINED
        let result = chat_server
            .send(JoinRoom {
                session: session_id,
                room: room.to_string(),
                user_id: "observer-user@example.com".to_string(),
                display_name: "observer-user@example.com".to_string(),
                observer: true,
                capabilities: 0,
            })
            .await
            .expect("Message delivery should succeed");

        assert!(result.is_ok(), "Observer JoinRoom should succeed");

        // Wait long enough for any NATS publish to arrive
        sleep(Duration::from_millis(1000)).await;

        assert!(
            !participant_joined_received.load(Ordering::Relaxed),
            "Observer join should NOT publish PARTICIPANT_JOINED to NATS"
        );
    }

    // ==========================================================================
    // TEST: Non-observer JoinRoom + ActivateConnection publishes PARTICIPANT_JOINED
    // ==========================================================================
    // When a real participant joins a room and their connection is activated,
    // the server should publish a PARTICIPANT_JOINED event to NATS so other
    // peers are notified. The broadcast is deferred from JoinRoom to
    // ActivateConnection to avoid ghost join events during RTT election.
    #[actix_rt::test]
    #[serial]
    async fn test_non_observer_join_publishes_participant_joined() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        use tokio::time::{sleep, Duration};

        let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());
        let nats_client = async_nats::connect(&nats_url)
            .await
            .expect("Failed to connect to NATS");

        let chat_server = ChatServer::new(nats_client.clone()).await.start();

        struct DummySession;
        impl Actor for DummySession {
            type Context = actix::Context<Self>;
        }
        impl Handler<Message> for DummySession {
            type Result = ();
            fn handle(&mut self, _msg: Message, _ctx: &mut Self::Context) {}
        }

        let dummy = DummySession.start();
        let session_id = 2002u64;
        let room = "test-room-non-observer-join";

        // Subscribe to the system subject for this room BEFORE join
        let system_subject = format!("room.{}.system", room.replace(' ', "_"));
        let participant_joined_received = Arc::new(AtomicBool::new(false));
        let flag = participant_joined_received.clone();
        let mut sub = nats_client
            .subscribe(system_subject)
            .await
            .expect("Failed to subscribe to system subject");

        tokio::spawn(async move {
            use videocall_types::protos::meeting_packet::meeting_packet::MeetingEventType;
            use videocall_types::protos::meeting_packet::MeetingPacket;

            while let Ok(Some(msg)) =
                tokio::time::timeout(Duration::from_millis(1500), sub.next()).await
            {
                if let Ok(wrapper) =
                    <PacketWrapper as ProtobufMessage>::parse_from_bytes(&msg.payload)
                {
                    if let Ok(inner) = MeetingPacket::parse_from_bytes(&wrapper.data) {
                        if inner.event_type == MeetingEventType::PARTICIPANT_JOINED.into() {
                            flag.store(true, Ordering::Relaxed);
                        }
                    }
                }
            }
        });

        // Register session
        chat_server
            .send(Connect {
                id: session_id,
                addr: dummy.recipient(),
            })
            .await
            .expect("Connect should succeed");

        // Join as non-observer
        let result = chat_server
            .send(JoinRoom {
                session: session_id,
                room: room.to_string(),
                user_id: "real-user@example.com".to_string(),
                display_name: "real-user@example.com".to_string(),
                observer: false,
                capabilities: 0,
            })
            .await
            .expect("Message delivery should succeed");

        assert!(result.is_ok(), "Non-observer JoinRoom should succeed");

        // Activate the connection — this triggers the deferred PARTICIPANT_JOINED
        chat_server
            .send(ActivateConnection {
                session: session_id,
            })
            .await
            .expect("ActivateConnection should succeed");

        // Wait for the async task to publish PARTICIPANT_JOINED
        sleep(Duration::from_millis(1000)).await;

        assert!(
            participant_joined_received.load(Ordering::Relaxed),
            "Non-observer join + activate SHOULD publish PARTICIPANT_JOINED to NATS"
        );
    }

    // ==========================================================================
    // TEST: JoinRoom without ActivateConnection does NOT publish PARTICIPANT_JOINED
    // ==========================================================================
    // When a connection joins but is never activated (e.g., the losing connection
    // during RTT election), PARTICIPANT_JOINED should NOT be broadcast.
    #[actix_rt::test]
    #[serial]
    async fn test_join_without_activate_does_not_publish_participant_joined() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        use tokio::time::{sleep, Duration};

        let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());
        let nats_client = async_nats::connect(&nats_url)
            .await
            .expect("Failed to connect to NATS");

        let chat_server = ChatServer::new(nats_client.clone()).await.start();

        struct DummySession;
        impl Actor for DummySession {
            type Context = actix::Context<Self>;
        }
        impl Handler<Message> for DummySession {
            type Result = ();
            fn handle(&mut self, _msg: Message, _ctx: &mut Self::Context) {}
        }

        let dummy = DummySession.start();
        let session_id = 3001u64;
        let room = "test-room-no-activate";

        // Subscribe to the system subject for this room BEFORE join
        let system_subject = format!("room.{}.system", room.replace(' ', "_"));
        let participant_joined_received = Arc::new(AtomicBool::new(false));
        let flag = participant_joined_received.clone();
        let mut sub = nats_client
            .subscribe(system_subject)
            .await
            .expect("Failed to subscribe to system subject");

        tokio::spawn(async move {
            use videocall_types::protos::meeting_packet::meeting_packet::MeetingEventType;
            use videocall_types::protos::meeting_packet::MeetingPacket;

            while let Ok(Some(msg)) =
                tokio::time::timeout(Duration::from_millis(1500), sub.next()).await
            {
                if let Ok(wrapper) =
                    <PacketWrapper as ProtobufMessage>::parse_from_bytes(&msg.payload)
                {
                    if let Ok(inner) = MeetingPacket::parse_from_bytes(&wrapper.data) {
                        if inner.event_type == MeetingEventType::PARTICIPANT_JOINED.into() {
                            flag.store(true, Ordering::Relaxed);
                        }
                    }
                }
            }
        });

        // Register session
        chat_server
            .send(Connect {
                id: session_id,
                addr: dummy.recipient(),
            })
            .await
            .expect("Connect should succeed");

        // Join as non-observer but do NOT activate
        let result = chat_server
            .send(JoinRoom {
                session: session_id,
                room: room.to_string(),
                user_id: "testing-user@example.com".to_string(),
                display_name: "testing-user@example.com".to_string(),
                observer: false,
                capabilities: 0,
            })
            .await
            .expect("Message delivery should succeed");

        assert!(result.is_ok(), "JoinRoom should succeed");

        // Wait — no ActivateConnection sent
        sleep(Duration::from_millis(1500)).await;

        assert!(
            !participant_joined_received.load(Ordering::Relaxed),
            "JoinRoom without ActivateConnection should NOT publish PARTICIPANT_JOINED"
        );
    }

    // ==========================================================================
    // TEST: Testing session disconnect does NOT publish PARTICIPANT_LEFT
    // ==========================================================================
    // When a Testing session disconnects (e.g., the losing connection during
    // RTT election), PARTICIPANT_LEFT should NOT be broadcast because
    // PARTICIPANT_JOINED was never broadcast for it.
    #[actix_rt::test]
    #[serial]
    async fn test_testing_session_disconnect_does_not_publish_participant_left() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        use tokio::time::{sleep, Duration};

        let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());
        let nats_client = async_nats::connect(&nats_url)
            .await
            .expect("Failed to connect to NATS");

        let chat_server = ChatServer::new(nats_client.clone()).await.start();

        struct DummySession;
        impl Actor for DummySession {
            type Context = actix::Context<Self>;
        }
        impl Handler<Message> for DummySession {
            type Result = ();
            fn handle(&mut self, _msg: Message, _ctx: &mut Self::Context) {}
        }

        let dummy = DummySession.start();
        let session_id = 3002u64;
        let room = "test-room-testing-dc";

        // Register and join (Testing state, never activated)
        chat_server
            .send(Connect {
                id: session_id,
                addr: dummy.recipient(),
            })
            .await
            .expect("Connect should succeed");

        let result = chat_server
            .send(JoinRoom {
                session: session_id,
                room: room.to_string(),
                user_id: "testing-dc@example.com".to_string(),
                display_name: "testing-dc@example.com".to_string(),
                observer: false,
                capabilities: 0,
            })
            .await
            .expect("Message delivery should succeed");
        assert!(result.is_ok(), "JoinRoom should succeed");

        // Wait for session setup
        sleep(Duration::from_millis(300)).await;

        // Subscribe to system subject to watch for PARTICIPANT_LEFT
        let system_subject = format!("room.{}.system", room.replace(' ', "_"));
        let participant_left_received = Arc::new(AtomicBool::new(false));
        let flag = participant_left_received.clone();
        let mut sub = nats_client
            .subscribe(system_subject)
            .await
            .expect("Failed to subscribe to system subject");

        tokio::spawn(async move {
            use videocall_types::protos::meeting_packet::meeting_packet::MeetingEventType;
            use videocall_types::protos::meeting_packet::MeetingPacket;

            while let Ok(Some(msg)) = tokio::time::timeout(Duration::from_secs(6), sub.next()).await
            {
                if let Ok(wrapper) =
                    <PacketWrapper as ProtobufMessage>::parse_from_bytes(&msg.payload)
                {
                    if let Ok(inner) = MeetingPacket::parse_from_bytes(&wrapper.data) {
                        if inner.event_type == MeetingEventType::PARTICIPANT_LEFT.into() {
                            flag.store(true, Ordering::Relaxed);
                        }
                    }
                }
            }
        });

        // Disconnect while still in Testing state (never activated)
        chat_server
            .send(Disconnect {
                session: session_id,
                room: room.to_string(),
                user_id: "testing-dc@example.com".to_string(),
                display_name: "testing-dc@example.com".to_string(),
                observer: false,
                redirect: false,
            })
            .await
            .expect("Disconnect should succeed");

        // Wait for grace period to expire plus buffer
        sleep(Duration::from_secs(4)).await;

        assert!(
            !participant_left_received.load(Ordering::Relaxed),
            "Testing session disconnect should NOT publish PARTICIPANT_LEFT \
             (was_active=false prevents ghost leave event)"
        );
    }

    // ==========================================================================
    // TEST: Observer Disconnect does NOT publish PARTICIPANT_LEFT
    // ==========================================================================
    // When an observer session disconnects (e.g., waiting room user admitted),
    // the server should NOT publish a PARTICIPANT_LEFT event. The user was never
    // a real participant in the meeting.
    #[actix_rt::test]
    #[serial]
    async fn test_observer_disconnect_does_not_publish_participant_left() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        use tokio::time::{sleep, Duration};

        let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());
        let nats_client = async_nats::connect(&nats_url)
            .await
            .expect("Failed to connect to NATS");

        let chat_server = ChatServer::new(nats_client.clone()).await.start();

        struct DummySession;
        impl Actor for DummySession {
            type Context = actix::Context<Self>;
        }
        impl Handler<Message> for DummySession {
            type Result = ();
            fn handle(&mut self, _msg: Message, _ctx: &mut Self::Context) {}
        }

        let dummy = DummySession.start();
        let session_id = 2003u64;
        let room = "test-room-observer-disconnect";

        // Register and join as observer first
        chat_server
            .send(Connect {
                id: session_id,
                addr: dummy.recipient(),
            })
            .await
            .expect("Connect should succeed");

        let result = chat_server
            .send(JoinRoom {
                session: session_id,
                room: room.to_string(),
                user_id: "observer-dc@example.com".to_string(),
                display_name: "observer-dc@example.com".to_string(),
                observer: true,
                capabilities: 0,
            })
            .await
            .expect("Message delivery should succeed");
        assert!(result.is_ok(), "Observer JoinRoom should succeed");

        // Wait for session to be fully set up
        sleep(Duration::from_millis(300)).await;

        // Now subscribe to system subject to watch for PARTICIPANT_LEFT
        let system_subject = format!("room.{}.system", room.replace(' ', "_"));
        let participant_left_received = Arc::new(AtomicBool::new(false));
        let flag = participant_left_received.clone();
        let mut sub = nats_client
            .subscribe(system_subject)
            .await
            .expect("Failed to subscribe to system subject");

        tokio::spawn(async move {
            use videocall_types::protos::meeting_packet::meeting_packet::MeetingEventType;
            use videocall_types::protos::meeting_packet::MeetingPacket;

            while let Ok(Some(msg)) =
                tokio::time::timeout(Duration::from_millis(1500), sub.next()).await
            {
                if let Ok(wrapper) =
                    <PacketWrapper as ProtobufMessage>::parse_from_bytes(&msg.payload)
                {
                    if let Ok(inner) = MeetingPacket::parse_from_bytes(&wrapper.data) {
                        if inner.event_type == MeetingEventType::PARTICIPANT_LEFT.into() {
                            flag.store(true, Ordering::Relaxed);
                        }
                    }
                }
            }
        });

        // Disconnect as observer - should NOT publish PARTICIPANT_LEFT
        chat_server
            .send(Disconnect {
                session: session_id,
                room: room.to_string(),
                user_id: "observer-dc@example.com".to_string(),
                display_name: "observer-dc@example.com".to_string(),
                observer: true,
                redirect: false,
            })
            .await
            .expect("Disconnect should succeed");

        // Wait long enough for any NATS publish to arrive
        sleep(Duration::from_millis(1000)).await;

        assert!(
            !participant_left_received.load(Ordering::Relaxed),
            "Observer disconnect should NOT publish PARTICIPANT_LEFT to NATS"
        );
    }

    // ==========================================================================
    // TEST: Non-observer Disconnect publishes PARTICIPANT_LEFT after grace period
    // ==========================================================================
    // When a real participant disconnects, the server defers the PARTICIPANT_LEFT
    // broadcast by RECONNECT_GRACE_PERIOD. If no reconnection occurs, the event
    // is published after the grace period expires. This test uses
    // ExecutePendingDeparture directly to avoid waiting for the full grace period.
    #[actix_rt::test]
    #[serial]
    async fn test_non_observer_disconnect_publishes_event() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        use tokio::time::{sleep, Duration};

        let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());
        let nats_client = async_nats::connect(&nats_url)
            .await
            .expect("Failed to connect to NATS");

        let chat_server = ChatServer::new(nats_client.clone()).await.start();

        struct DummySession;
        impl Actor for DummySession {
            type Context = actix::Context<Self>;
        }
        impl Handler<Message> for DummySession {
            type Result = ();
            fn handle(&mut self, _msg: Message, _ctx: &mut Self::Context) {}
        }

        let dummy = DummySession.start();
        let session_id = 2004u64;
        let room = "test-room-non-observer-disconnect";

        // Register and join as real participant
        chat_server
            .send(Connect {
                id: session_id,
                addr: dummy.recipient(),
            })
            .await
            .expect("Connect should succeed");

        let result = chat_server
            .send(JoinRoom {
                session: session_id,
                room: room.to_string(),
                user_id: "real-dc@example.com".to_string(),
                display_name: "real-dc@example.com".to_string(),
                observer: false,
                capabilities: 0,
            })
            .await
            .expect("Message delivery should succeed");
        assert!(result.is_ok(), "Non-observer JoinRoom should succeed");

        // Activate the connection so the state is Active before disconnect
        chat_server
            .send(ActivateConnection {
                session: session_id,
            })
            .await
            .expect("ActivateConnection should succeed");

        // Wait for session setup
        sleep(Duration::from_millis(300)).await;

        // Subscribe to system subject to watch for any meeting events
        let system_subject = format!("room.{}.system", room.replace(' ', "_"));
        let meeting_event_received = Arc::new(AtomicBool::new(false));
        let flag = meeting_event_received.clone();
        let mut sub = nats_client
            .subscribe(system_subject)
            .await
            .expect("Failed to subscribe to system subject");

        // Use a longer timeout to accommodate the reconnect grace period.
        // The NATS subscriber waits up to 6s (grace period is 2s + buffer).
        tokio::spawn(async move {
            use videocall_types::protos::meeting_packet::meeting_packet::MeetingEventType;
            use videocall_types::protos::meeting_packet::MeetingPacket;

            while let Ok(Some(msg)) = tokio::time::timeout(Duration::from_secs(6), sub.next()).await
            {
                if let Ok(wrapper) =
                    <PacketWrapper as ProtobufMessage>::parse_from_bytes(&msg.payload)
                {
                    if let Ok(inner) = MeetingPacket::parse_from_bytes(&wrapper.data) {
                        // Accept any meeting lifecycle event (PARTICIPANT_LEFT or MEETING_ENDED)
                        // depending on how end_session categorizes this session
                        if inner.event_type == MeetingEventType::PARTICIPANT_LEFT.into()
                            || inner.event_type == MeetingEventType::MEETING_ENDED.into()
                        {
                            flag.store(true, Ordering::Relaxed);
                        }
                    }
                }
            }
        });

        // Disconnect as non-observer — the departure is deferred by
        // RECONNECT_GRACE_PERIOD (2s). The PARTICIPANT_LEFT event will
        // not be published until the grace period expires.
        chat_server
            .send(Disconnect {
                session: session_id,
                room: room.to_string(),
                user_id: "real-dc@example.com".to_string(),
                display_name: "real-dc@example.com".to_string(),
                observer: false,
                redirect: false,
            })
            .await
            .expect("Disconnect should succeed");

        // Wait for the grace period to expire plus some buffer.
        // RECONNECT_GRACE_PERIOD is 2s, we wait 4s to give the deferred
        // execution and NATS publish time to complete.
        sleep(Duration::from_secs(4)).await;

        // The non-observer path should have attempted to publish via the full
        // end_session flow after the grace period expired.
        assert!(
            meeting_event_received.load(Ordering::Relaxed),
            "Non-observer disconnect should publish a meeting event after grace period \
             (PARTICIPANT_LEFT or MEETING_ENDED)"
        );
    }

    // ==========================================================================
    // TEST: Observer JoinRoom succeeds and session is tracked
    // ==========================================================================
    // Verify that observer sessions are accepted and registered just like normal
    // sessions - the only difference is in event publishing behavior.
    #[actix_rt::test]
    #[serial]
    async fn test_observer_join_room_succeeds() {
        let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());
        let nats_client = async_nats::connect(&nats_url)
            .await
            .expect("Failed to connect to NATS");

        let chat_server = ChatServer::new(nats_client).await.start();

        struct DummySession;
        impl Actor for DummySession {
            type Context = actix::Context<Self>;
        }
        impl Handler<Message> for DummySession {
            type Result = ();
            fn handle(&mut self, _msg: Message, _ctx: &mut Self::Context) {}
        }

        let dummy = DummySession.start();
        let session_id = 2005u64;

        chat_server
            .send(Connect {
                id: session_id,
                addr: dummy.recipient(),
            })
            .await
            .expect("Connect should succeed");

        // Join as observer - should succeed (same as non-observer)
        let result = chat_server
            .send(JoinRoom {
                session: session_id,
                room: "test-room-observer-ok".to_string(),
                user_id: "observer@example.com".to_string(),
                display_name: "observer@example.com".to_string(),
                observer: true,
                capabilities: 0,
            })
            .await
            .expect("Message delivery should succeed");

        assert!(
            result.is_ok(),
            "Observer JoinRoom should succeed, got: {result:?}"
        );

        // Joining again with same session should return Ok (already in joined_sessions)
        let result2 = chat_server
            .send(JoinRoom {
                session: session_id,
                room: "test-room-observer-ok".to_string(),
                user_id: "observer@example.com".to_string(),
                display_name: "observer@example.com".to_string(),
                observer: true,
                capabilities: 0,
            })
            .await
            .expect("Message delivery should succeed");

        assert!(
            result2.is_ok(),
            "Second observer JoinRoom should return Ok (already active)"
        );
    }

    // Helper message to get connection state for testing
    #[derive(ActixMessage)]
    #[rtype(result = "Result<ConnectionState, ()>")]
    struct GetConnectionState {
        session: SessionId,
    }

    impl Handler<GetConnectionState> for ChatServer {
        type Result = Result<ConnectionState, ()>;

        fn handle(&mut self, msg: GetConnectionState, _ctx: &mut Self::Context) -> Self::Result {
            Ok(self
                .connection_states
                .get(&msg.session)
                .copied()
                .unwrap_or(ConnectionState::Testing))
        }
    }

    // ==========================================================================
    // TEST: Hard admission cap rejects joins past the limit (S-P0-3)
    // ==========================================================================
    // Locks in the room-capacity check added per sfu-update/GAP-ANALYSIS.md
    // S-P0-3. Without this cap, a scripted attacker with one valid JWT can
    // OOM a pod by spawning thousands of sessions in a single room.
    //
    // Uses MAX_PARTICIPANTS_ENV to shrink the cap so the test runs quickly.
    // The env var is set/unset via the `serial` attribute on each test to
    // avoid races with other tests reading it.
    #[actix_rt::test]
    #[serial]
    async fn test_join_room_rejects_past_capacity() {
        let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());
        let nats_client = async_nats::connect(&nats_url)
            .await
            .expect("Failed to connect to NATS");

        let chat_server = ChatServer::new(nats_client).await.start();

        struct DummySession;
        impl Actor for DummySession {
            type Context = actix::Context<Self>;
        }
        impl Handler<Message> for DummySession {
            type Result = ();
            fn handle(&mut self, _msg: Message, _ctx: &mut Self::Context) {}
        }

        // Shrink the cap to 3 so the test joins 4 sessions instead of 201.
        // The handler reads MAX_PARTICIPANTS_ENV at request-time, so setting
        // it before any JoinRoom is sent is sufficient.
        std::env::set_var(MAX_PARTICIPANTS_ENV, "3");

        let room = "cap-test-room";

        // Helper: register a dummy session under the given id.
        async fn register(chat_server: &actix::Addr<ChatServer>, id: u64) {
            let dummy = DummySession.start();
            chat_server
                .send(Connect {
                    id,
                    addr: dummy.recipient(),
                })
                .await
                .expect("Connect should succeed");
        }

        // The first three joins succeed; the fourth is rejected at capacity.
        for (i, id) in [4001u64, 4002, 4003].iter().enumerate() {
            register(&chat_server, *id).await;
            let result = chat_server
                .send(JoinRoom {
                    session: *id,
                    room: room.to_string(),
                    user_id: format!("user{i}@example.com"),
                    display_name: format!("user{i}"),
                    observer: false,
                    capabilities: 0,
                })
                .await
                .expect("Message delivery should succeed");
            assert!(
                result.is_ok(),
                "Join #{} (session {id}) should succeed, got: {result:?}",
                i + 1,
            );
        }

        register(&chat_server, 4004u64).await;
        let result = chat_server
            .send(JoinRoom {
                session: 4004u64,
                room: room.to_string(),
                user_id: "user4@example.com".to_string(),
                display_name: "user4".to_string(),
                observer: false,
                capabilities: 0,
            })
            .await
            .expect("Message delivery should succeed");

        assert!(
            result.is_err(),
            "Join past cap should return Err, got: {result:?}"
        );
        let err = result.unwrap_err();
        assert!(
            err.contains("capacity"),
            "Error should mention capacity, got: {err}"
        );

        // Observers are not subject to the cap. Register and join a 5th
        // session as observer — should succeed even though room_members
        // is already at the (non-observer) cap.
        register(&chat_server, 4005u64).await;
        let observer_result = chat_server
            .send(JoinRoom {
                session: 4005u64,
                room: room.to_string(),
                user_id: "observer@example.com".to_string(),
                display_name: "observer".to_string(),
                observer: true,
                capabilities: 0,
            })
            .await
            .expect("Message delivery should succeed");
        assert!(
            observer_result.is_ok(),
            "Observer join should bypass the cap, got: {observer_result:?}"
        );

        std::env::remove_var(MAX_PARTICIPANTS_ENV);
    }

    // ==========================================================================
    // TEST (p2-5): CONGESTION packets bypass Forwarder::decide in SFU mode
    // ==========================================================================
    // Verifies the CRITICAL carve-out in `handle_msg`: in SfuMode::Sfu, a
    // CONGESTION packet must always be forwarded as-is regardless of what the
    // forwarder would decide. The forwarder is built on top of an empty
    // RoomState (p2-5 does not call insert_member — that's p2-6).
    //
    // To prove the bypass actually runs, we set `pw.session_id ==
    // receiver_sid`. If the CONGESTION bypass branch were removed, the code
    // would fall through to `Forwarder::decide`, whose self-skip filter
    // (keyed on `packet_wrapper.session_id == receiver_sid`) would return
    // `Drop` and no message would be captured — failing the assertion.
    // Equal SIDs therefore make the test sensitive to the very branch it is
    // intended to lock in.
    //
    // Exercised by constructing a synthetic async_nats::Message and invoking
    // the `handle_msg` closure directly — no NATS round-trip required.
    #[actix_rt::test]
    #[serial]
    async fn test_sfu_mode_congestion_bypasses_forwarder() {
        use crate::sfu::forwarder::Forwarder;
        use crate::sfu::room_state::RoomState;
        use crate::sfu::SfuMode;
        use std::sync::{Arc, Mutex, RwLock};
        use tokio::time::{sleep, Duration};

        // Capturing receiver — records every Message it gets.
        struct CapturingSession {
            captured: Arc<Mutex<Vec<bytes::Bytes>>>,
        }
        impl Actor for CapturingSession {
            type Context = actix::Context<Self>;
        }
        impl Handler<Message> for CapturingSession {
            type Result = ();
            fn handle(&mut self, msg: Message, _ctx: &mut Self::Context) {
                self.captured.lock().unwrap().push(msg.msg);
            }
        }

        let captured: Arc<Mutex<Vec<bytes::Bytes>>> = Arc::new(Mutex::new(Vec::new()));
        let receiver = CapturingSession {
            captured: captured.clone(),
        }
        .start();
        let recipient = receiver.recipient();

        let room = "p2-5-test-room".to_string();
        // sender_sid == receiver_sid is deliberate: if the CONGESTION bypass
        // were removed, Forwarder::decide would self-skip-drop this packet
        // (its only filter today keys on packet_wrapper.session_id ==
        // receiver_sid), so the assertion below would fail. Equal SIDs make
        // this test sensitive to the bypass branch actually running.
        let receiver_sid: SessionId = 7001;
        let sender_sid: SessionId = 7001;

        // Build the SFU side: empty RoomState + Forwarder over it.
        let room_state = Arc::new(RwLock::new(RoomState::new(room.clone())));
        let forwarder = Arc::new(Forwarder::with_room_only(room_state));

        let handler = handle_msg(
            recipient,
            room.clone(),
            receiver_sid,
            SfuMode::Sfu,
            forwarder,
        );

        // Build a CONGESTION PacketWrapper from the SAME session as the
        // receiver (see SID comment above). The top-of-handle_msg self-echo
        // skip carves out CONGESTION explicitly, so it reaches the SFU
        // branch; the SFU branch's bypass is then the only thing standing
        // between this packet and the assertion.
        let mut pw = PacketWrapper::new();
        pw.packet_type = PacketType::CONGESTION.into();
        pw.session_id = sender_sid;
        pw.user_id = b"test-user@example.com".to_vec();
        pw.data = b"congestion-payload".to_vec();
        let payload_bytes = pw.write_to_bytes().expect("serialize CONGESTION wrapper");
        let payload_len = payload_bytes.len();

        // Publish on the receiver's OWN subject. With sender_sid ==
        // receiver_sid, the top-level self-echo skip would drop this if
        // CONGESTION weren't carved out, and the SFU branch's bypass is the
        // only path that delivers it without consulting the forwarder.
        let subject_str = format!("room.{room}.{receiver_sid}").replace(' ', "_");
        let msg = async_nats::Message {
            subject: subject_str.into(),
            reply: None,
            payload: bytes::Bytes::from(payload_bytes),
            headers: None,
            status: None,
            description: None,
            length: payload_len,
        };

        handler(msg).expect("handle_msg should succeed");

        // The CapturingSession runs on the same actix runtime; give it one
        // scheduler tick to drain its mailbox.
        sleep(Duration::from_millis(50)).await;

        let got = captured.lock().unwrap().clone();
        assert_eq!(
            got.len(),
            1,
            "CONGESTION packet must be forwarded (bypass), got {} messages",
            got.len()
        );
        // The CONGESTION bypass forwards the original payload verbatim.
        let received = PacketWrapper::parse_from_bytes(&got[0])
            .expect("captured payload must be a valid PacketWrapper");
        assert_eq!(
            received.packet_type,
            PacketType::CONGESTION.into(),
            "forwarded packet must remain CONGESTION"
        );
        assert_eq!(
            received.session_id, sender_sid,
            "forwarded packet must preserve the sender SID"
        );
    }

    // ==========================================================================
    // TEST (p2-6): JoinRoom/Leave drive RoomState.members + capabilities
    // ==========================================================================
    // Two sessions join the same room with distinct capabilities; the SFU
    // member table must reflect both entries. One session leaves; the table
    // must shrink to one entry while preserving the other's capabilities.
    #[actix_rt::test]
    #[serial]
    async fn test_join_leave_drives_room_state_members() {
        use crate::sfu::room_state::{CAP_SFU_ROUTING_HEADER, CAP_SUBSCRIPTION, CAP_SVC};

        let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());
        let nats_client = async_nats::connect(&nats_url)
            .await
            .expect("Failed to connect to NATS");

        let chat_server = ChatServer::new(nats_client).await.start();

        struct DummySession;
        impl Actor for DummySession {
            type Context = actix::Context<Self>;
        }
        impl Handler<Message> for DummySession {
            type Result = ();
            fn handle(&mut self, _msg: Message, _ctx: &mut Self::Context) {}
        }

        let dummy_a = DummySession.start();
        let dummy_b = DummySession.start();
        let sid_a: SessionId = 30001;
        let sid_b: SessionId = 30002;
        let caps_a = CAP_SFU_ROUTING_HEADER | CAP_SVC;
        let caps_b = CAP_SUBSCRIPTION;
        let room = "test-room-p2-6".to_string();

        for (sid, addr) in [(sid_a, dummy_a.recipient()), (sid_b, dummy_b.recipient())] {
            chat_server
                .send(Connect { id: sid, addr })
                .await
                .expect("Connect should succeed");
        }

        for (sid, caps, user) in [
            (sid_a, caps_a, "alice@example.com"),
            (sid_b, caps_b, "bob@example.com"),
        ] {
            let res = chat_server
                .send(JoinRoom {
                    session: sid,
                    room: room.clone(),
                    user_id: user.to_string(),
                    display_name: user.to_string(),
                    observer: false,
                    capabilities: caps,
                })
                .await
                .expect("Message delivery should succeed");
            assert!(res.is_ok(), "JoinRoom should succeed for {user}: {res:?}");
        }

        let snapshot = chat_server
            .send(SnapshotRoomMembers { room: room.clone() })
            .await
            .expect("Snapshot delivery should succeed")
            .expect("Room should exist after JoinRoom");
        assert_eq!(
            snapshot,
            vec![(sid_a, caps_a), (sid_b, caps_b)],
            "Both sessions must appear with their declared capabilities"
        );

        chat_server
            .send(Leave {
                session: sid_a,
                room: room.clone(),
                user_id: "alice@example.com".to_string(),
            })
            .await
            .expect("Leave delivery should succeed");

        let snapshot = chat_server
            .send(SnapshotRoomMembers { room: room.clone() })
            .await
            .expect("Snapshot delivery should succeed")
            .expect("Room should still exist after one leave");
        assert_eq!(
            snapshot,
            vec![(sid_b, caps_b)],
            "After Leave(sid_a), only sid_b remains with its capabilities"
        );
    }

    // ==========================================================================
    // TEST (vc-69e / p3-13): admission control — below soft cap admits silently
    // ==========================================================================
    // count < WAITING_ROOM_THRESHOLD: the join is admitted and the new joiner
    // receives NO ADMISSION_DECISION packet (the common path is zero-overhead).
    #[actix_rt::test]
    #[serial]
    async fn test_admission_below_soft_cap_admits_silently() {
        use std::sync::{Arc, Mutex};
        use tokio::time::{sleep, Duration};
        use videocall_types::protos::admission_decision_packet::AdmissionDecision;

        // Default thresholds: 195 soft / 200 hard. Make sure nothing in this
        // process has overridden them via env.
        std::env::remove_var(MAX_PARTICIPANTS_ENV);
        std::env::remove_var(WAITING_ROOM_THRESHOLD_ENV);

        let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());
        let nats_client = async_nats::connect(&nats_url)
            .await
            .expect("Failed to connect to NATS");

        let chat_server = ChatServer::new(nats_client).await.start();

        let received: Arc<Mutex<Vec<bytes::Bytes>>> = Arc::new(Mutex::new(Vec::new()));

        struct CapturingSession {
            received: Arc<Mutex<Vec<bytes::Bytes>>>,
        }
        impl Actor for CapturingSession {
            type Context = actix::Context<Self>;
        }
        impl Handler<Message> for CapturingSession {
            type Result = ();
            fn handle(&mut self, msg: Message, _ctx: &mut Self::Context) {
                self.received.lock().unwrap().push(msg.msg);
            }
        }

        // Pre-populate the room with 100 placeholder members via repeated
        // JoinRoom calls. Below the 195 soft cap, the new (101st) joiner
        // should not receive any ADMISSION_DECISION packet.
        let room = "test-admission-below-soft-cap";
        let dummy = {
            struct DummySession;
            impl Actor for DummySession {
                type Context = actix::Context<Self>;
            }
            impl Handler<Message> for DummySession {
                type Result = ();
                fn handle(&mut self, _msg: Message, _ctx: &mut Self::Context) {}
            }
            DummySession.start()
        };
        for i in 0..100u64 {
            let sid = 50_000 + i;
            chat_server
                .send(Connect {
                    id: sid,
                    addr: dummy.clone().recipient(),
                })
                .await
                .unwrap();
            chat_server
                .send(JoinRoom {
                    session: sid,
                    room: room.to_string(),
                    user_id: format!("filler-{i}@example.com"),
                    display_name: format!("filler-{i}"),
                    observer: false,
                    capabilities: 0,
                })
                .await
                .unwrap()
                .expect("filler join should succeed");
        }

        // Now have the capturing session attempt to join — count=100 before
        // join, well below the 195 soft cap.
        let capturing = CapturingSession {
            received: received.clone(),
        }
        .start();
        let observer_sid = 51_000u64;
        chat_server
            .send(Connect {
                id: observer_sid,
                addr: capturing.recipient(),
            })
            .await
            .unwrap();

        let result = chat_server
            .send(JoinRoom {
                session: observer_sid,
                room: room.to_string(),
                user_id: "alice@example.com".to_string(),
                display_name: "alice".to_string(),
                observer: false,
                capabilities: 0,
            })
            .await
            .unwrap();
        assert!(result.is_ok(), "join below soft cap must succeed");

        sleep(Duration::from_millis(150)).await;

        let msgs = received.lock().unwrap().clone();
        for msg_bytes in msgs.iter() {
            if let Ok(wrapper) = <PacketWrapper as ProtobufMessage>::parse_from_bytes(msg_bytes) {
                assert_ne!(
                    wrapper.packet_type,
                    PacketType::ADMISSION_DECISION.into(),
                    "no ADMISSION_DECISION packet must be sent below the soft cap"
                );
                // Defensive parse — confirms the payload is also empty / no decision.
                let _ = AdmissionDecision::parse_from_bytes(&wrapper.data);
            }
        }
    }

    // ==========================================================================
    // TEST (vc-69e / p3-13): admission control — soft cap emits QUEUED hint
    // ==========================================================================
    // count >= WAITING_ROOM_THRESHOLD && count < hard cap: the join is still
    // admitted (no behavioural change) but the new joiner receives an
    // ADMISSION_DECISION{QUEUED} packet with a 1-based overflow position.
    //
    // Uses env overrides to shrink the thresholds for fast testing
    // (soft_cap=3, hard_cap=5) — equivalent to the production thresholds:
    //   count=3 -> position=1
    //   count=4 -> position=2
    //
    // Documents the position convention chosen: position = current - soft + 1.
    // For the production defaults (soft=195) this matches the bead spec's
    // "count - 194 = position" formula exactly: at count=195, position=1.
    #[actix_rt::test]
    #[serial]
    async fn test_admission_at_soft_cap_emits_queued_packet() {
        use std::sync::{Arc, Mutex};
        use tokio::time::{sleep, Duration};
        use videocall_types::protos::admission_decision_packet::admission_decision::Status as AdmStatus;
        use videocall_types::protos::admission_decision_packet::AdmissionDecision;

        std::env::set_var(MAX_PARTICIPANTS_ENV, "5");
        std::env::set_var(WAITING_ROOM_THRESHOLD_ENV, "3");

        let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());
        let nats_client = async_nats::connect(&nats_url)
            .await
            .expect("Failed to connect to NATS");

        let chat_server = ChatServer::new(nats_client).await.start();

        let room = "test-admission-soft-cap";

        // Fill 3 filler sessions (count=3, exactly at soft cap).
        let filler = {
            struct DummySession;
            impl Actor for DummySession {
                type Context = actix::Context<Self>;
            }
            impl Handler<Message> for DummySession {
                type Result = ();
                fn handle(&mut self, _msg: Message, _ctx: &mut Self::Context) {}
            }
            DummySession.start()
        };
        for i in 0..3u64 {
            let sid = 52_000 + i;
            chat_server
                .send(Connect {
                    id: sid,
                    addr: filler.clone().recipient(),
                })
                .await
                .unwrap();
            chat_server
                .send(JoinRoom {
                    session: sid,
                    room: room.to_string(),
                    user_id: format!("filler-{i}@example.com"),
                    display_name: format!("filler-{i}"),
                    observer: false,
                    capabilities: 0,
                })
                .await
                .unwrap()
                .expect("filler join should succeed");
        }

        let received: Arc<Mutex<Vec<bytes::Bytes>>> = Arc::new(Mutex::new(Vec::new()));

        struct CapturingSession {
            received: Arc<Mutex<Vec<bytes::Bytes>>>,
        }
        impl Actor for CapturingSession {
            type Context = actix::Context<Self>;
        }
        impl Handler<Message> for CapturingSession {
            type Result = ();
            fn handle(&mut self, msg: Message, _ctx: &mut Self::Context) {
                self.received.lock().unwrap().push(msg.msg);
            }
        }

        let capturing = CapturingSession {
            received: received.clone(),
        }
        .start();
        let soft_sid = 52_100u64;
        chat_server
            .send(Connect {
                id: soft_sid,
                addr: capturing.recipient(),
            })
            .await
            .unwrap();

        // count=3 before this join, in [soft_cap=3, hard_cap=5) — must be
        // admitted and QUEUED notification must be delivered.
        let result = chat_server
            .send(JoinRoom {
                session: soft_sid,
                room: room.to_string(),
                user_id: "queued-user@example.com".to_string(),
                display_name: "queued-user".to_string(),
                observer: false,
                capabilities: 0,
            })
            .await
            .unwrap();
        assert!(result.is_ok(), "join at soft cap must succeed (admitted)");

        sleep(Duration::from_millis(150)).await;

        let msgs = received.lock().unwrap().clone();
        let mut found = None;
        for msg_bytes in msgs.iter() {
            if let Ok(wrapper) = <PacketWrapper as ProtobufMessage>::parse_from_bytes(msg_bytes) {
                if wrapper.packet_type == PacketType::ADMISSION_DECISION.into() {
                    let inner = AdmissionDecision::parse_from_bytes(&wrapper.data)
                        .expect("AdmissionDecision must decode");
                    found = Some(inner);
                    break;
                }
            }
        }

        let inner = found.expect("ADMISSION_DECISION{QUEUED} must be delivered to soft-cap joiner");
        assert_eq!(
            inner.status.enum_value_or_default(),
            AdmStatus::QUEUED,
            "status must be QUEUED for soft-cap admit"
        );
        assert_eq!(
            inner.position, 1,
            "position convention: current=soft_cap (3) -> position=1"
        );
        assert_eq!(inner.reason, "soft_cap_reached");
        assert_eq!(
            inner.retry_after_secs, 0,
            "QUEUED packets do not set retry hints"
        );

        std::env::remove_var(MAX_PARTICIPANTS_ENV);
        std::env::remove_var(WAITING_ROOM_THRESHOLD_ENV);
    }

    // ==========================================================================
    // TEST (vc-69e / p3-13): admission control — hard cap rejects with packet
    // ==========================================================================
    // count >= hard cap: join is rejected. Server emits ADMISSION_DECISION
    // {REJECTED, reason="room_full"} to the would-be joiner BEFORE returning
    // an Err, and does NOT add the session to room_members.
    #[actix_rt::test]
    #[serial]
    async fn test_admission_at_hard_cap_rejects_with_packet() {
        use std::sync::{Arc, Mutex};
        use tokio::time::{sleep, Duration};
        use videocall_types::protos::admission_decision_packet::admission_decision::Status as AdmStatus;
        use videocall_types::protos::admission_decision_packet::AdmissionDecision;

        std::env::set_var(MAX_PARTICIPANTS_ENV, "5");
        std::env::set_var(WAITING_ROOM_THRESHOLD_ENV, "3");

        let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());
        let nats_client = async_nats::connect(&nats_url)
            .await
            .expect("Failed to connect to NATS");

        let chat_server = ChatServer::new(nats_client).await.start();

        let room = "test-admission-hard-cap";

        // Fill 5 filler sessions (count = hard_cap).
        let filler = {
            struct DummySession;
            impl Actor for DummySession {
                type Context = actix::Context<Self>;
            }
            impl Handler<Message> for DummySession {
                type Result = ();
                fn handle(&mut self, _msg: Message, _ctx: &mut Self::Context) {}
            }
            DummySession.start()
        };
        for i in 0..5u64 {
            let sid = 53_000 + i;
            chat_server
                .send(Connect {
                    id: sid,
                    addr: filler.clone().recipient(),
                })
                .await
                .unwrap();
            chat_server
                .send(JoinRoom {
                    session: sid,
                    room: room.to_string(),
                    user_id: format!("filler-{i}@example.com"),
                    display_name: format!("filler-{i}"),
                    observer: false,
                    capabilities: 0,
                })
                .await
                .unwrap()
                .expect("filler join should succeed");
        }

        let received: Arc<Mutex<Vec<bytes::Bytes>>> = Arc::new(Mutex::new(Vec::new()));

        struct CapturingSession {
            received: Arc<Mutex<Vec<bytes::Bytes>>>,
        }
        impl Actor for CapturingSession {
            type Context = actix::Context<Self>;
        }
        impl Handler<Message> for CapturingSession {
            type Result = ();
            fn handle(&mut self, msg: Message, _ctx: &mut Self::Context) {
                self.received.lock().unwrap().push(msg.msg);
            }
        }

        let capturing = CapturingSession {
            received: received.clone(),
        }
        .start();
        let rejected_sid = 53_100u64;
        chat_server
            .send(Connect {
                id: rejected_sid,
                addr: capturing.recipient(),
            })
            .await
            .unwrap();

        // count=5 before this join, == hard_cap=5 — must be rejected.
        let result = chat_server
            .send(JoinRoom {
                session: rejected_sid,
                room: room.to_string(),
                user_id: "rejected-user@example.com".to_string(),
                display_name: "rejected-user".to_string(),
                observer: false,
                capabilities: 0,
            })
            .await
            .unwrap();
        assert!(result.is_err(), "join at hard cap must be rejected");
        assert!(
            result.unwrap_err().contains("at capacity"),
            "error message should mention capacity"
        );

        sleep(Duration::from_millis(150)).await;

        let msgs = received.lock().unwrap().clone();
        let mut found = None;
        for msg_bytes in msgs.iter() {
            if let Ok(wrapper) = <PacketWrapper as ProtobufMessage>::parse_from_bytes(msg_bytes) {
                if wrapper.packet_type == PacketType::ADMISSION_DECISION.into() {
                    let inner = AdmissionDecision::parse_from_bytes(&wrapper.data)
                        .expect("AdmissionDecision must decode");
                    found = Some(inner);
                    break;
                }
            }
        }

        let inner =
            found.expect("ADMISSION_DECISION{REJECTED} must be delivered to rejected joiner");
        assert_eq!(
            inner.status.enum_value_or_default(),
            AdmStatus::REJECTED,
            "status must be REJECTED for hard-cap reject"
        );
        assert_eq!(inner.reason, "room_full");
        assert!(
            inner.retry_after_secs > 0,
            "REJECTED packets must include a retry_after_secs hint"
        );

        // Verify the rejected session is NOT in room_members.
        let snapshot = chat_server
            .send(SnapshotRoomMembers {
                room: room.to_string(),
            })
            .await
            .unwrap()
            .expect("room must still exist");
        assert_eq!(snapshot.len(), 5, "room_members must still hold exactly 5");
        assert!(
            !snapshot.iter().any(|(sid, _)| *sid == rejected_sid),
            "rejected session must NOT appear in room_members"
        );

        std::env::remove_var(MAX_PARTICIPANTS_ENV);
        std::env::remove_var(WAITING_ROOM_THRESHOLD_ENV);
    }

    // ==========================================================================
    // INTEGRATION (vc-69e / p3-13): 200 sequential joins succeed; 201st rejected
    // ==========================================================================
    // Uses the production hard cap (200) via env, with the soft cap shrunk to
    // 199 so that we don't have to deliver/capture 5 QUEUED packets per join
    // for participants 195..199. The acceptance criterion is the boundary:
    // the 200th join succeeds; the 201st is rejected and is not added.
    //
    // Marked #[ignore] by default because spinning up 201 sessions in-process
    // is slow (multi-second) and stresses NATS subscribers. Run explicitly via
    //   cargo test -p videocall-api -- --ignored test_admission_200_sequential
    // or in nightly CI.
    #[actix_rt::test]
    #[serial]
    #[ignore]
    async fn test_admission_200_sequential_joins_201st_rejected() {
        std::env::set_var(MAX_PARTICIPANTS_ENV, "200");
        std::env::set_var(WAITING_ROOM_THRESHOLD_ENV, "199");

        let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());
        let nats_client = async_nats::connect(&nats_url)
            .await
            .expect("Failed to connect to NATS");

        let chat_server = ChatServer::new(nats_client).await.start();

        let room = "test-admission-200-seq";

        let filler = {
            struct DummySession;
            impl Actor for DummySession {
                type Context = actix::Context<Self>;
            }
            impl Handler<Message> for DummySession {
                type Result = ();
                fn handle(&mut self, _msg: Message, _ctx: &mut Self::Context) {}
            }
            DummySession.start()
        };

        for i in 0..200u64 {
            let sid = 60_000 + i;
            chat_server
                .send(Connect {
                    id: sid,
                    addr: filler.clone().recipient(),
                })
                .await
                .unwrap();
            let res = chat_server
                .send(JoinRoom {
                    session: sid,
                    room: room.to_string(),
                    user_id: format!("user-{i}@example.com"),
                    display_name: format!("user-{i}"),
                    observer: false,
                    capabilities: 0,
                })
                .await
                .unwrap();
            assert!(
                res.is_ok(),
                "join #{i} must succeed (still within hard cap)"
            );
        }

        // 201st join: count=200 == hard cap -> reject.
        let extra_sid = 60_999u64;
        chat_server
            .send(Connect {
                id: extra_sid,
                addr: filler.clone().recipient(),
            })
            .await
            .unwrap();
        let result = chat_server
            .send(JoinRoom {
                session: extra_sid,
                room: room.to_string(),
                user_id: "overflow@example.com".to_string(),
                display_name: "overflow".to_string(),
                observer: false,
                capabilities: 0,
            })
            .await
            .unwrap();
        assert!(result.is_err(), "201st join must be rejected");

        let snapshot = chat_server
            .send(SnapshotRoomMembers {
                room: room.to_string(),
            })
            .await
            .unwrap()
            .expect("room must exist");
        assert_eq!(
            snapshot.len(),
            200,
            "room_members must still hold exactly 200"
        );
        assert!(
            !snapshot.iter().any(|(sid, _)| *sid == extra_sid),
            "the 201st session must NOT appear in room_members"
        );

        std::env::remove_var(MAX_PARTICIPANTS_ENV);
        std::env::remove_var(WAITING_ROOM_THRESHOLD_ENV);
    }

    // ==========================================================================
    // TEST (vc-8oa / p6-5): pod ownership redirect on JoinRoom
    // ==========================================================================
    // When a client joins a room whose jump-hash owner is a different pod
    // ordinal than this pod's own ordinal, the server MUST:
    //   1. emit ADMISSION_DECISION{REDIRECT, redirect_to=<owner DNS>}
    //   2. return MessageResult(Err(_)) so the transport closes the conn
    //   3. NOT add the session to room_members (no admission accounting)
    //
    // Env vars POD_NAME, STATEFULSET_REPLICAS, SFU_TRANSPORT_KIND are
    // process-global; the existing admission tests in this module already
    // gate on `#[serial]` for the same reason. We follow that pattern.
    #[actix_rt::test]
    #[serial]
    async fn test_join_room_redirects_on_pod_ownership_mismatch() {
        use std::sync::{Arc, Mutex};
        use tokio::time::{sleep, Duration};
        use videocall_types::protos::admission_decision_packet::admission_decision::Status as AdmStatus;
        use videocall_types::protos::admission_decision_packet::AdmissionDecision;

        // Configure ourselves as pod 0 in a 3-replica StatefulSet.
        std::env::set_var("POD_NAME", "rustlemania-webtransport-0");
        std::env::set_var("STATEFULSET_REPLICAS", "3");
        std::env::set_var("SFU_TRANSPORT_KIND", "webtransport");
        // Clear any admission-cap leakage from prior tests.
        std::env::remove_var(MAX_PARTICIPANTS_ENV);
        std::env::remove_var(WAITING_ROOM_THRESHOLD_ENV);

        // Pick a room whose jump_hash lands on a NON-zero ordinal so the
        // redirect path actually fires. Loop a few candidates; with 3
        // replicas the expected miss rate is ~2/3 so we find one quickly.
        let replicas = 3u32;
        let (room, owner_ord) = (0..200)
            .find_map(|i| {
                let r = format!("redirect-room-{i}");
                let o = crate::sfu::affinity::jump_hash(&r, replicas);
                (o != 0).then_some((r, o))
            })
            .expect("must find a room hashing to a non-zero ordinal among 200 candidates");

        let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());
        let nats_client = async_nats::connect(&nats_url)
            .await
            .expect("Failed to connect to NATS");
        let chat_server = ChatServer::new(nats_client).await.start();

        // Capturing recipient so we can decode the REDIRECT packet.
        let received: Arc<Mutex<Vec<bytes::Bytes>>> = Arc::new(Mutex::new(Vec::new()));
        struct CapturingSession {
            received: Arc<Mutex<Vec<bytes::Bytes>>>,
        }
        impl Actor for CapturingSession {
            type Context = actix::Context<Self>;
        }
        impl Handler<Message> for CapturingSession {
            type Result = ();
            fn handle(&mut self, msg: Message, _ctx: &mut Self::Context) {
                self.received.lock().unwrap().push(msg.msg);
            }
        }
        let capturing = CapturingSession {
            received: received.clone(),
        }
        .start();

        let sid: SessionId = 71_000;
        chat_server
            .send(Connect {
                id: sid,
                addr: capturing.recipient(),
            })
            .await
            .unwrap();

        let result = chat_server
            .send(JoinRoom {
                session: sid,
                room: room.clone(),
                user_id: "wrong-pod-user@example.com".to_string(),
                display_name: "wrong-pod-user".to_string(),
                observer: false,
                capabilities: 0,
            })
            .await
            .unwrap();
        assert!(result.is_err(), "join on non-owner pod must be declined");
        assert!(
            result.unwrap_err().contains("different pod"),
            "error message should mention pod ownership"
        );

        // Let the recipient mpsc drain.
        sleep(Duration::from_millis(150)).await;

        let msgs = received.lock().unwrap().clone();
        let mut found = None;
        for msg_bytes in msgs.iter() {
            if let Ok(wrapper) = <PacketWrapper as ProtobufMessage>::parse_from_bytes(msg_bytes) {
                if wrapper.packet_type == PacketType::ADMISSION_DECISION.into() {
                    let inner = AdmissionDecision::parse_from_bytes(&wrapper.data)
                        .expect("AdmissionDecision must decode");
                    found = Some(inner);
                    break;
                }
            }
        }
        let inner =
            found.expect("ADMISSION_DECISION{REDIRECT} must be delivered to redirected joiner");
        assert_eq!(
            inner.status.enum_value_or_default(),
            AdmStatus::REDIRECT,
            "status must be REDIRECT for ownership mismatch"
        );
        assert_eq!(inner.reason, "wrong_owner");
        let expected_dns =
            format!("rustlemania-webtransport-{owner_ord}.webtransport-headless.svc.cluster.local");
        assert_eq!(
            inner.redirect_to, expected_dns,
            "redirect_to must point at the owner pod's headless DNS"
        );

        // The redirected session must NOT have been admitted: the room
        // either doesn't exist (no prior joiners) or doesn't contain `sid`.
        let snapshot = chat_server
            .send(SnapshotRoomMembers { room: room.clone() })
            .await
            .unwrap();
        if let Some(members) = snapshot {
            assert!(
                !members.iter().any(|(s, _)| *s == sid),
                "redirected session must NOT appear in room_members"
            );
        }

        std::env::remove_var("POD_NAME");
        std::env::remove_var("STATEFULSET_REPLICAS");
        std::env::remove_var("SFU_TRANSPORT_KIND");
    }

    // ==========================================================================
    // TEST (vc-85p / p6-5): spillover ADMITS LOCALLY instead of redirecting
    // ==========================================================================
    // Same setup as test_join_room_redirects_on_pod_ownership_mismatch — a
    // room jump-hashed to a non-owner ordinal, so the redirect path would
    // normally fire — but with a FRESH, OVER-THRESHOLD owner-pod beacon
    // seeded into the spillover store. The non-owner pod MUST then:
    //   1. NOT emit ADMISSION_DECISION{REDIRECT}
    //   2. return MessageResult(Ok(_)) (the join succeeds locally)
    //   3. add the session to room_members (normal local admission)
    //
    // This is the inverse assertion of the redirect test and locks in the
    // admit-vs-redirect branch added in vc-85p.
    #[actix_rt::test]
    #[serial]
    async fn test_join_room_spills_locally_when_owner_over_threshold() {
        use std::sync::{Arc, Mutex};
        use tokio::time::{sleep, Duration};

        std::env::set_var("POD_NAME", "rustlemania-webtransport-0");
        std::env::set_var("STATEFULSET_REPLICAS", "3");
        std::env::set_var("SFU_TRANSPORT_KIND", "webtransport");
        std::env::remove_var(MAX_PARTICIPANTS_ENV);
        std::env::remove_var(WAITING_ROOM_THRESHOLD_ENV);

        // Room owned by a NON-zero ordinal (this pod is 0) so the redirect
        // path WOULD fire absent the spill override.
        let replicas = 3u32;
        let (room, _owner_ord) = (0..200)
            .find_map(|i| {
                let r = format!("spill-room-{i}");
                let o = crate::sfu::affinity::jump_hash(&r, replicas);
                (o != 0).then_some((r, o))
            })
            .expect("must find a room hashing to a non-zero ordinal among 200 candidates");

        let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());
        let nats_client = async_nats::connect(&nats_url)
            .await
            .expect("Failed to connect to NATS");
        let chat_server = ChatServer::new(nats_client).await.start();

        // Seed a fresh, over-threshold owner beacon so is_spilled_over()
        // returns true for this room.
        chat_server
            .send(SeedSpilloverState {
                room: room.clone(),
                owner_count: 200, // > SPILLOVER_PARTICIPANT_THRESHOLD (180)
                owner_cpu: 0.10,
            })
            .await
            .unwrap();

        let received: Arc<Mutex<Vec<bytes::Bytes>>> = Arc::new(Mutex::new(Vec::new()));
        struct CapturingSession {
            received: Arc<Mutex<Vec<bytes::Bytes>>>,
        }
        impl Actor for CapturingSession {
            type Context = actix::Context<Self>;
        }
        impl Handler<Message> for CapturingSession {
            type Result = ();
            fn handle(&mut self, msg: Message, _ctx: &mut Self::Context) {
                self.received.lock().unwrap().push(msg.msg);
            }
        }
        let capturing = CapturingSession {
            received: received.clone(),
        }
        .start();

        let sid: SessionId = 72_000;
        chat_server
            .send(Connect {
                id: sid,
                addr: capturing.recipient(),
            })
            .await
            .unwrap();

        let result = chat_server
            .send(JoinRoom {
                session: sid,
                room: room.clone(),
                user_id: "spill-user@example.com".to_string(),
                display_name: "spill-user".to_string(),
                observer: false,
                capabilities: 0,
            })
            .await
            .unwrap();
        assert!(
            result.is_ok(),
            "join on non-owner pod with over-threshold owner beacon must be \
             admitted LOCALLY (spill), not declined: {result:?}"
        );

        // Drain the recipient mpsc and assert NO REDIRECT was delivered.
        sleep(Duration::from_millis(150)).await;
        let msgs = received.lock().unwrap().clone();
        for msg_bytes in msgs.iter() {
            if let Ok(wrapper) = <PacketWrapper as ProtobufMessage>::parse_from_bytes(msg_bytes) {
                assert_ne!(
                    wrapper.packet_type,
                    PacketType::ADMISSION_DECISION.into(),
                    "a spilled (locally-admitted) joiner must NOT receive an \
                     ADMISSION_DECISION{{REDIRECT}} packet"
                );
            }
        }

        // The spilled session MUST be admitted to the local room_members.
        let snapshot = chat_server
            .send(SnapshotRoomMembers { room: room.clone() })
            .await
            .unwrap()
            .expect("room must exist after local admission");
        assert!(
            snapshot.iter().any(|(s, _)| *s == sid),
            "spilled session must appear in local room_members"
        );

        std::env::remove_var("POD_NAME");
        std::env::remove_var("STATEFULSET_REPLICAS");
        std::env::remove_var("SFU_TRANSPORT_KIND");
    }

    // ==========================================================================
    // TEST (vc-8oa / p6-5 follow-up): redirect runs BEFORE reconnection bookkeeping
    // ==========================================================================
    // Locks in the must-fix from the p6-5 review: the ownership redirect
    // path MUST run synchronously up front, before `pending_departures`,
    // `room_members`, or `room_states` are touched. If it ran after, a
    // reconnecting user landing on the wrong pod would:
    //   1. have their old `pending_departures` entry drained,
    //   2. have their deferred PARTICIPANT_LEFT timer cancelled,
    //   3. be removed from `room_members` and `room_states` (old session),
    //   4. be redirected.
    // Peers would never see the leave event, and the user would silently
    // disappear from the room until they reconnected to the correct pod.
    //
    // This test simulates the reconnection-into-redirect scenario by
    // staging a `pending_departures` entry via the Disconnect handler on
    // pod-0, then issuing a fresh JoinRoom for the same (room, user) where
    // the room is jump-hash-owned by pod 1 in a 3-replica StatefulSet.
    // The Err-with-redirect must NOT mutate any reconnection state.
    #[actix_rt::test]
    #[serial]
    async fn test_redirect_does_not_leak_reconnection_state() {
        use std::sync::{Arc, Mutex};
        use tokio::time::{sleep, Duration};

        // Pod-0 in a 3-replica cluster.
        std::env::set_var("POD_NAME", "rustlemania-webtransport-0");
        std::env::set_var("STATEFULSET_REPLICAS", "3");
        std::env::set_var("SFU_TRANSPORT_KIND", "webtransport");
        std::env::remove_var(MAX_PARTICIPANTS_ENV);
        std::env::remove_var(WAITING_ROOM_THRESHOLD_ENV);

        // Find a room whose jump-hash owner is NOT pod 0 (so the redirect
        // path actually fires for this pod).
        let replicas = 3u32;
        let (room, owner_ord) = (0..200)
            .find_map(|i| {
                let r = format!("reconnect-redirect-room-{i}");
                let o = crate::sfu::affinity::jump_hash(&r, replicas);
                (o != 0).then_some((r, o))
            })
            .expect("must find a room hashing to a non-zero ordinal among 200 candidates");

        let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());
        let nats_client = async_nats::connect(&nats_url)
            .await
            .expect("Failed to connect to NATS");
        let chat_server = ChatServer::new(nats_client).await.start();

        // Stage a pending_departures entry for (room, user) the same way
        // a real disconnect would: we cannot use the JoinRoom→Disconnect
        // sequence because JoinRoom itself would redirect on this pod for
        // this room (the whole point of the test). Instead, we drive the
        // Disconnect handler directly with a dummy session that we DO
        // pre-register (so Disconnect's cleanup paths are well-defined).
        //
        // The Disconnect handler requires the session to have been
        // ConnectionState::Active to defer (otherwise it bypasses the
        // grace period). We achieve that by joining with replicas=1 first
        // (no redirect), activating, then flipping the env to the 3-pod
        // configuration for the JoinRoom-under-test.
        std::env::set_var("STATEFULSET_REPLICAS", "1");

        struct DummySession;
        impl Actor for DummySession {
            type Context = actix::Context<Self>;
        }
        impl Handler<Message> for DummySession {
            type Result = ();
            fn handle(&mut self, _msg: Message, _ctx: &mut Self::Context) {}
        }

        let dummy = DummySession.start();
        let old_sid: SessionId = 72_000;
        let user_id = "reconnect-user@example.com";

        chat_server
            .send(Connect {
                id: old_sid,
                addr: dummy.recipient(),
            })
            .await
            .unwrap();
        chat_server
            .send(JoinRoom {
                session: old_sid,
                room: room.clone(),
                user_id: user_id.to_string(),
                display_name: user_id.to_string(),
                observer: false,
                capabilities: 0,
            })
            .await
            .unwrap()
            .expect("initial join with replicas=1 must succeed (pod 0 owns everything)");
        chat_server
            .send(ActivateConnection { session: old_sid })
            .await
            .unwrap();
        sleep(Duration::from_millis(100)).await;

        // Disconnect — defers PARTICIPANT_LEFT by RECONNECT_GRACE_PERIOD.
        // vc-9g7: `redirect: false` here is correct — this test simulates a
        // real client-initiated disconnect (not a cross-region async
        // redirect). The point of the test is that a *subsequent* client
        // reconnect onto a non-owner pod (a separate redirect path, p6-5)
        // must not drain the pending_departures entry staged below; the
        // deferred-leave behavior must be exercised exactly as in
        // production for that scenario.
        chat_server
            .send(Disconnect {
                session: old_sid,
                room: room.clone(),
                user_id: user_id.to_string(),
                display_name: user_id.to_string(),
                observer: false,
                redirect: false,
            })
            .await
            .unwrap();

        // Verify the pending_departures entry was staged.
        let staged = chat_server
            .send(SnapshotPendingDeparture {
                room: room.clone(),
                user_id: user_id.to_string(),
            })
            .await
            .unwrap();
        assert_eq!(
            staged,
            Some(old_sid),
            "pre-condition: Disconnect must stage a pending_departures entry"
        );
        // And room_members still contains the old session (cleanup is
        // deferred to either reconnection or grace-period expiry).
        let members_before = chat_server
            .send(SnapshotRoomMembersList { room: room.clone() })
            .await
            .unwrap()
            .expect("room_members entry should exist after Disconnect (deferred cleanup)");
        assert!(
            members_before.iter().any(|(s, _, _)| *s == old_sid),
            "pre-condition: room_members must still contain old session before reconnect"
        );

        // NOW switch to the 3-replica configuration so the room is owned
        // by a different pod, and attempt to "reconnect" with a new SID.
        // The redirect path must fire WITHOUT draining pending_departures
        // / room_members / room_states.
        std::env::set_var("STATEFULSET_REPLICAS", "3");

        let received: Arc<Mutex<Vec<bytes::Bytes>>> = Arc::new(Mutex::new(Vec::new()));
        struct CapturingSession {
            received: Arc<Mutex<Vec<bytes::Bytes>>>,
        }
        impl Actor for CapturingSession {
            type Context = actix::Context<Self>;
        }
        impl Handler<Message> for CapturingSession {
            type Result = ();
            fn handle(&mut self, msg: Message, _ctx: &mut Self::Context) {
                self.received.lock().unwrap().push(msg.msg);
            }
        }
        let capturing = CapturingSession {
            received: received.clone(),
        }
        .start();
        let new_sid: SessionId = 72_001;
        chat_server
            .send(Connect {
                id: new_sid,
                addr: capturing.recipient(),
            })
            .await
            .unwrap();

        let result = chat_server
            .send(JoinRoom {
                session: new_sid,
                room: room.clone(),
                user_id: user_id.to_string(),
                display_name: user_id.to_string(),
                observer: false,
                capabilities: 0,
            })
            .await
            .unwrap();
        assert!(
            result.is_err(),
            "reconnect onto non-owner pod must be redirected (Err)"
        );

        // CORE ASSERTION 1: pending_departures entry was NOT drained.
        // If the redirect block ran after the reconnection bookkeeping,
        // `pending_departures.remove(&key)` would have fired and this
        // would be `None`, leaking the deferred PARTICIPANT_LEFT.
        let after = chat_server
            .send(SnapshotPendingDeparture {
                room: room.clone(),
                user_id: user_id.to_string(),
            })
            .await
            .unwrap();
        assert_eq!(
            after,
            Some(old_sid),
            "pending_departures entry MUST survive a redirect — the deferred \
             PARTICIPANT_LEFT will still fire after the grace period if the \
             client doesn't successfully reconnect on the correct pod"
        );

        // CORE ASSERTION 2: room_members entry for the old session is
        // intact. If the reconnection bookkeeping had run, the old SID
        // would have been retained out of `room_members`.
        let members_after = chat_server
            .send(SnapshotRoomMembersList { room: room.clone() })
            .await
            .unwrap()
            .expect("room_members entry must still exist after redirect");
        assert!(
            members_after.iter().any(|(s, _, _)| *s == old_sid),
            "room_members MUST still contain old session — redirect must not \
             prematurely evict the disconnected session's row"
        );

        // CORE ASSERTION 3: the new session was NOT added to room_members.
        assert!(
            !members_after.iter().any(|(s, _, _)| *s == new_sid),
            "redirected session MUST NOT appear in room_members"
        );

        // SANITY: the REDIRECT packet was delivered.
        sleep(Duration::from_millis(150)).await;
        use videocall_types::protos::admission_decision_packet::admission_decision::Status as AdmStatus;
        use videocall_types::protos::admission_decision_packet::AdmissionDecision;
        let msgs = received.lock().unwrap().clone();
        let mut found = None;
        for msg_bytes in msgs.iter() {
            if let Ok(wrapper) = <PacketWrapper as ProtobufMessage>::parse_from_bytes(msg_bytes) {
                if wrapper.packet_type == PacketType::ADMISSION_DECISION.into() {
                    let inner = AdmissionDecision::parse_from_bytes(&wrapper.data)
                        .expect("AdmissionDecision must decode");
                    found = Some(inner);
                    break;
                }
            }
        }
        let inner =
            found.expect("ADMISSION_DECISION{REDIRECT} must be delivered to redirected session");
        assert_eq!(inner.status.enum_value_or_default(), AdmStatus::REDIRECT);
        let expected_dns =
            format!("rustlemania-webtransport-{owner_ord}.webtransport-headless.svc.cluster.local");
        assert_eq!(inner.redirect_to, expected_dns);

        std::env::remove_var("POD_NAME");
        std::env::remove_var("STATEFULSET_REPLICAS");
        std::env::remove_var("SFU_TRANSPORT_KIND");
    }

    // ==========================================================================
    // TEST: vc-9g7 — cross-region async redirect bypasses RECONNECT_GRACE_PERIOD
    // ==========================================================================
    // When the JoinRoom async cache-miss path admits a non-observer joiner and
    // the spawned KV lookup resolves to a foreign home region,
    // `Handler<HomeRegionResolved>` sends a synthetic Disconnect to itself with
    // `redirect: true`. The new `if redirect` arm in `Handler<Disconnect>` must
    // call `leave_rooms` IMMEDIATELY (no 2s deferral) so cross-region peers
    // federated via NATS do not observe a JOINED → LEFT ghost-participant pair.
    //
    // We drive `HomeRegionResolved` directly with `home_region != current_region`
    // to exercise the redirect branch without needing a multi-region NATS-KV
    // setup. After the synthetic Disconnect flows through:
    //
    //   1. `pending_departures` MUST NOT contain an entry for the session
    //      (the redirect arm must NOT take the deferred path).
    //   2. `room_members` MUST NOT contain the session (immediate leave_rooms
    //      cleanup ran).
    //   3. PARTICIPANT_LEFT must either fire immediately (within ~200ms) or be
    //      suppressed entirely (if leave_rooms gates on the session having
    //      reached Active). It MUST NOT fire 2s later.
    #[actix_rt::test]
    #[serial]
    async fn test_cross_region_redirect_bypasses_grace_period() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::{Arc, Mutex};
        use tokio::time::{sleep, Duration, Instant};

        // Single-pod, default region ("local"). The HomeRegionResolved we
        // synthesize will name "us-east" so the cross-region branch fires.
        std::env::remove_var("POD_NAME");
        std::env::remove_var("STATEFULSET_REPLICAS");
        std::env::set_var("SFU_TRANSPORT_KIND", "webtransport");
        std::env::remove_var(MAX_PARTICIPANTS_ENV);
        std::env::remove_var(WAITING_ROOM_THRESHOLD_ENV);

        let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());
        let nats_client = async_nats::connect(&nats_url)
            .await
            .expect("Failed to connect to NATS");
        let chat_server = ChatServer::new(nats_client.clone()).await.start();

        // Capturing session — records all Message bytes for later inspection.
        let received: Arc<Mutex<Vec<bytes::Bytes>>> = Arc::new(Mutex::new(Vec::new()));
        struct CapturingSession {
            received: Arc<Mutex<Vec<bytes::Bytes>>>,
        }
        impl Actor for CapturingSession {
            type Context = actix::Context<Self>;
        }
        impl Handler<Message> for CapturingSession {
            type Result = ();
            fn handle(&mut self, msg: Message, _ctx: &mut Self::Context) {
                self.received.lock().unwrap().push(msg.msg);
            }
        }
        let capturing = CapturingSession {
            received: received.clone(),
        }
        .start();

        let session_id: SessionId = 91_000;
        let user_id = "redirect-bypass@example.com";
        let room = "vc-9g7-redirect-bypass-room";

        chat_server
            .send(Connect {
                id: session_id,
                addr: capturing.recipient(),
            })
            .await
            .expect("Connect should succeed");

        chat_server
            .send(JoinRoom {
                session: session_id,
                room: room.to_string(),
                user_id: user_id.to_string(),
                display_name: user_id.to_string(),
                observer: false,
                capabilities: 0,
            })
            .await
            .expect("Message delivery should succeed")
            .expect("JoinRoom must succeed in single-region default config");

        // Activate so the session reaches the state where PARTICIPANT_LEFT
        // *would* fire on a normal disconnect — this is the worst case for
        // the bug we are fixing (where the deferred path would produce a
        // 2s ghost window).
        chat_server
            .send(ActivateConnection {
                session: session_id,
            })
            .await
            .expect("ActivateConnection should succeed");

        // Watch the system subject for PARTICIPANT_LEFT and record the time
        // it arrived (if at all).
        use videocall_types::protos::meeting_packet::meeting_packet::MeetingEventType;
        use videocall_types::protos::meeting_packet::MeetingPacket;
        let system_subject = format!("room.{}.system", room.replace(' ', "_"));
        let left_arrived_at: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));
        let left_clone = left_arrived_at.clone();
        let observed_any_left = Arc::new(AtomicBool::new(false));
        let observed_clone = observed_any_left.clone();
        let mut sub = nats_client
            .subscribe(system_subject)
            .await
            .expect("Failed to subscribe to system subject");
        tokio::spawn(async move {
            // Watch for ~3s — long enough to detect a deferred (2s) leave
            // if the bug were still present, plus margin.
            while let Ok(Some(msg)) =
                tokio::time::timeout(Duration::from_millis(3000), sub.next()).await
            {
                if let Ok(wrapper) =
                    <PacketWrapper as ProtobufMessage>::parse_from_bytes(&msg.payload)
                {
                    if let Ok(inner) = MeetingPacket::parse_from_bytes(&wrapper.data) {
                        if inner.event_type == MeetingEventType::PARTICIPANT_LEFT.into() {
                            observed_clone.store(true, Ordering::Relaxed);
                            *left_clone.lock().unwrap() = Some(Instant::now());
                            return;
                        }
                    }
                }
            }
        });

        // Let the JOINED publish settle.
        sleep(Duration::from_millis(200)).await;

        // Drive the redirect path directly. The HomeRegionResolved handler
        // will (a) emit the ADMISSION_DECISION{REDIRECT} packet to the
        // capturing session, and (b) synthesize a Disconnect with
        // redirect=true to itself.
        let synth_start = Instant::now();
        chat_server
            .send(HomeRegionResolved {
                room: room.to_string(),
                home_region: "us-east".to_string(),
                session: session_id,
                user_id: user_id.to_string(),
                display_name: user_id.to_string(),
            })
            .await
            .expect("HomeRegionResolved delivery should succeed");

        // Give actix a moment to run the synthesized Disconnect.
        sleep(Duration::from_millis(150)).await;

        // CORE ASSERTION 1: pending_departures must be empty for this room+user.
        // If the redirect arm fell through to the deferred path, this would be
        // Some(session_id).
        let staged = chat_server
            .send(SnapshotPendingDeparture {
                room: room.to_string(),
                user_id: user_id.to_string(),
            })
            .await
            .unwrap();
        assert_eq!(
            staged, None,
            "vc-9g7: redirect Disconnect MUST NOT stage a pending_departures \
             entry — the deferred-leave grace window must be bypassed"
        );

        // CORE ASSERTION 2: room_members must not contain this session anymore.
        // leave_rooms ran synchronously, so the row should be gone (or the
        // entire room_members entry for this room may be gone if it was the
        // last member).
        let members_after = chat_server
            .send(SnapshotRoomMembersList {
                room: room.to_string(),
            })
            .await
            .unwrap();
        if let Some(members) = members_after {
            assert!(
                !members.iter().any(|(s, _, _)| *s == session_id),
                "vc-9g7: redirect Disconnect MUST evict the session from \
                 room_members immediately, not after the grace period"
            );
        }

        // CORE ASSERTION 3: if PARTICIPANT_LEFT fired at all, it fired
        // immediately — NOT after RECONNECT_GRACE_PERIOD (2s).
        // Wait for the watcher window to elapse so a deferred event would
        // have a chance to be observed if the bug were present.
        sleep(Duration::from_millis(2500)).await;
        if observed_any_left.load(Ordering::Relaxed) {
            let when = left_arrived_at
                .lock()
                .unwrap()
                .expect("flag set implies timestamp set");
            let delay = when.duration_since(synth_start);
            assert!(
                delay < Duration::from_millis(300),
                "vc-9g7: PARTICIPANT_LEFT after redirect must fire promptly \
                 (got {:?} after HomeRegionResolved); the synchronous \
                 leave_rooms path should complete well under 300ms — a longer \
                 delay indicates a regression toward the deferred-leave \
                 timing the redirect path is supposed to bypass",
                delay
            );
        }
        // If observed_any_left == false, that's also a passing outcome: the
        // session's PARTICIPANT_JOINED was suppressed because no peer had
        // subscribed yet, and `leave_rooms` correctly elided the LEFT
        // broadcast. Either way no JOINED→LEFT ghost pair was emitted with
        // a 2s gap.

        // SANITY: the ADMISSION_DECISION{REDIRECT} packet was delivered to
        // the originating session.
        use videocall_types::protos::admission_decision_packet::admission_decision::Status as AdmStatus;
        use videocall_types::protos::admission_decision_packet::AdmissionDecision;
        let msgs = received.lock().unwrap().clone();
        let mut found = None;
        for msg_bytes in msgs.iter() {
            if let Ok(wrapper) = <PacketWrapper as ProtobufMessage>::parse_from_bytes(msg_bytes) {
                if wrapper.packet_type == PacketType::ADMISSION_DECISION.into() {
                    let inner = AdmissionDecision::parse_from_bytes(&wrapper.data)
                        .expect("AdmissionDecision must decode");
                    found = Some(inner);
                    break;
                }
            }
        }
        let inner = found.expect(
            "ADMISSION_DECISION{REDIRECT} (wrong_region) must be delivered to redirected session",
        );
        assert_eq!(inner.status.enum_value_or_default(), AdmStatus::REDIRECT);

        std::env::remove_var("SFU_TRANSPORT_KIND");
    }

    // ==========================================================================
    // TEST: vc-9g7 follow-up — cross-region redirect with never-activated session
    // ==========================================================================
    // Sibling of `test_cross_region_redirect_bypasses_grace_period`, exercising
    // the REALISTIC post-cache-miss timing: the KV roundtrip resolves BEFORE
    // the client's CONNECTION packet reaches `ActivateConnection`, so the
    // joiner never reaches the `Active` state and no PARTICIPANT_JOINED was
    // ever published.
    //
    // Before the leave_rooms refactor, the in-band `connection_states` lookup
    // inside `leave_rooms` saw `None` (the Disconnect handler already removed
    // the entry), the gate became a no-op, and PARTICIPANT_LEFT fired for a
    // participant nobody ever saw join. After the refactor, the caller passes
    // its captured `was_active=false` and the broadcast is suppressed.
    //
    // Assertions:
    //   1. `pending_departures` is empty (grace bypassed — same as the sibling).
    //   2. `room_members` no longer contains the session (immediate eviction).
    //   3. NO PARTICIPANT_LEFT is published at all — wait at least 300ms after
    //      the synthesized Disconnect, then assert nothing was observed. This
    //      is the assertion the previous bug would have failed.
    #[actix_rt::test]
    #[serial]
    async fn test_cross_region_redirect_when_session_never_activated() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::{Arc, Mutex};
        use tokio::time::{sleep, Duration};

        std::env::remove_var("POD_NAME");
        std::env::remove_var("STATEFULSET_REPLICAS");
        std::env::set_var("SFU_TRANSPORT_KIND", "webtransport");
        std::env::remove_var(MAX_PARTICIPANTS_ENV);
        std::env::remove_var(WAITING_ROOM_THRESHOLD_ENV);

        let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());
        let nats_client = async_nats::connect(&nats_url)
            .await
            .expect("Failed to connect to NATS");
        let chat_server = ChatServer::new(nats_client.clone()).await.start();

        let received: Arc<Mutex<Vec<bytes::Bytes>>> = Arc::new(Mutex::new(Vec::new()));
        struct CapturingSession {
            received: Arc<Mutex<Vec<bytes::Bytes>>>,
        }
        impl Actor for CapturingSession {
            type Context = actix::Context<Self>;
        }
        impl Handler<Message> for CapturingSession {
            type Result = ();
            fn handle(&mut self, msg: Message, _ctx: &mut Self::Context) {
                self.received.lock().unwrap().push(msg.msg);
            }
        }
        let capturing = CapturingSession {
            received: received.clone(),
        }
        .start();

        let session_id: SessionId = 91_001;
        let user_id = "redirect-never-active@example.com";
        let room = "vc-9g7-redirect-never-active-room";

        chat_server
            .send(Connect {
                id: session_id,
                addr: capturing.recipient(),
            })
            .await
            .expect("Connect should succeed");

        chat_server
            .send(JoinRoom {
                session: session_id,
                room: room.to_string(),
                user_id: user_id.to_string(),
                display_name: user_id.to_string(),
                observer: false,
                capabilities: 0,
            })
            .await
            .expect("Message delivery should succeed")
            .expect("JoinRoom must succeed in single-region default config");

        // INTENTIONALLY DO NOT call ActivateConnection — this is the
        // realistic timing where the KV-roundtrip resolves first.

        // Watch the system subject for ANY PARTICIPANT_LEFT — we expect none.
        use videocall_types::protos::meeting_packet::meeting_packet::MeetingEventType;
        use videocall_types::protos::meeting_packet::MeetingPacket;
        let system_subject = format!("room.{}.system", room.replace(' ', "_"));
        let observed_any_left = Arc::new(AtomicBool::new(false));
        let observed_clone = observed_any_left.clone();
        let mut sub = nats_client
            .subscribe(system_subject)
            .await
            .expect("Failed to subscribe to system subject");
        tokio::spawn(async move {
            while let Ok(Some(msg)) =
                tokio::time::timeout(Duration::from_millis(3000), sub.next()).await
            {
                if let Ok(wrapper) =
                    <PacketWrapper as ProtobufMessage>::parse_from_bytes(&msg.payload)
                {
                    if let Ok(inner) = MeetingPacket::parse_from_bytes(&wrapper.data) {
                        if inner.event_type == MeetingEventType::PARTICIPANT_LEFT.into() {
                            observed_clone.store(true, Ordering::Relaxed);
                            return;
                        }
                    }
                }
            }
        });

        // Let the subscription settle.
        sleep(Duration::from_millis(200)).await;

        // Drive the redirect path directly while the session is still in
        // Testing state (never activated).
        chat_server
            .send(HomeRegionResolved {
                room: room.to_string(),
                home_region: "us-east".to_string(),
                session: session_id,
                user_id: user_id.to_string(),
                display_name: user_id.to_string(),
            })
            .await
            .expect("HomeRegionResolved delivery should succeed");

        // Give actix a moment to run the synthesized Disconnect.
        sleep(Duration::from_millis(150)).await;

        // ASSERTION 1: pending_departures must be empty (grace bypassed).
        let staged = chat_server
            .send(SnapshotPendingDeparture {
                room: room.to_string(),
                user_id: user_id.to_string(),
            })
            .await
            .unwrap();
        assert_eq!(
            staged, None,
            "vc-9g7: redirect Disconnect MUST NOT stage a pending_departures \
             entry even for never-activated sessions"
        );

        // ASSERTION 2: room_members must not contain this session anymore.
        let members_after = chat_server
            .send(SnapshotRoomMembersList {
                room: room.to_string(),
            })
            .await
            .unwrap();
        if let Some(members) = members_after {
            assert!(
                !members.iter().any(|(s, _, _)| *s == session_id),
                "vc-9g7: redirect Disconnect MUST evict the session from \
                 room_members immediately, even when never activated"
            );
        }

        // ASSERTION 3: NO PARTICIPANT_LEFT must be published. Wait long
        // enough that a synchronous publish would have shown up; also longer
        // than the (bypassed) 2s grace window so a regression to the
        // deferred path would also be caught.
        sleep(Duration::from_millis(2500)).await;
        assert!(
            !observed_any_left.load(Ordering::Relaxed),
            "vc-9g7: no PARTICIPANT_LEFT must be published when redirecting \
             a session that never reached Active — the joiner was never \
             visible to peers, so emitting LEFT creates a ghost-departed \
             participant peers never saw join"
        );

        // SANITY: ADMISSION_DECISION{REDIRECT} was still delivered to the
        // originating session.
        use videocall_types::protos::admission_decision_packet::admission_decision::Status as AdmStatus;
        use videocall_types::protos::admission_decision_packet::AdmissionDecision;
        let msgs = received.lock().unwrap().clone();
        let mut found = None;
        for msg_bytes in msgs.iter() {
            if let Ok(wrapper) = <PacketWrapper as ProtobufMessage>::parse_from_bytes(msg_bytes) {
                if wrapper.packet_type == PacketType::ADMISSION_DECISION.into() {
                    let inner = AdmissionDecision::parse_from_bytes(&wrapper.data)
                        .expect("AdmissionDecision must decode");
                    found = Some(inner);
                    break;
                }
            }
        }
        let inner = found.expect(
            "ADMISSION_DECISION{REDIRECT} must be delivered to redirected \
             session even when it never activated",
        );
        assert_eq!(inner.status.enum_value_or_default(), AdmStatus::REDIRECT);

        std::env::remove_var("SFU_TRANSPORT_KIND");
    }

    // ==========================================================================
    // TEST (vc-9z6): concurrent T=0 joiners on a non-owner pod must NOT loop
    // ==========================================================================
    // Reproduction for vc-9z6: at T=0 on shard A (the room's non-owner pod),
    // many concurrent JoinRoom messages arrive at once (e.g. the 200-bot
    // harness sending 100 listener bots + senders against the same room).
    //
    // The bead's failure signature is bots exhausting MAX_REDIRECT_HOPS=5
    // without converging. For that to happen, each bot's reconnect attempt
    // must keep being told to redirect — i.e. the server is emitting a
    // REDIRECT packet for sessions it should NOT be redirecting, OR the
    // REDIRECT target it emits is wrong for some sessions.
    //
    // This test exercises the chat_server side in isolation: spawn N
    // concurrent JoinRoom messages from N distinct sessions, all targeting
    // a room whose jump-hash owner is a DIFFERENT pod ordinal than `self`.
    // Required behaviour:
    //
    //   1. Every JoinRoom must return Err(... "different pod" ...).
    //   2. Every session must receive exactly ONE ADMISSION_DECISION{REDIRECT}
    //      packet on its recipient.
    //   3. Every redirect target must point at the same owner pod's headless
    //      DNS — N concurrent joiners must NOT split between conflicting
    //      targets (which would happen if jump_hash or the env reads were
    //      non-deterministic under contention).
    //   4. No session must end up in `joined_sessions` or in the SFU member
    //      table for the room. The redirect path is non-admitting.
    //
    // A passing assertion set here is necessary-but-not-sufficient to fix
    // vc-9z6 — but a failure here at concurrent T=0 join time would be a
    // proximate explanation for the redirect-loop signature, since a bot
    // that arrives at a pod and is told to redirect a second time would
    // increment its hop counter against the MAX_REDIRECT_HOPS=5 cap.
    #[actix_rt::test]
    #[serial]
    async fn test_concurrent_t0_joiners_redirect_consistently_vc_9z6() {
        use std::sync::{Arc, Mutex};
        use tokio::time::{sleep, Duration};
        use videocall_types::protos::admission_decision_packet::admission_decision::Status as AdmStatus;
        use videocall_types::protos::admission_decision_packet::AdmissionDecision;

        // Pod 0 in a 3-replica StatefulSet, identical to
        // test_join_room_redirects_on_pod_ownership_mismatch so the redirect
        // target shape is comparable.
        std::env::set_var("POD_NAME", "rustlemania-webtransport-0");
        std::env::set_var("STATEFULSET_REPLICAS", "3");
        std::env::set_var("SFU_TRANSPORT_KIND", "webtransport");
        std::env::remove_var(MAX_PARTICIPANTS_ENV);
        std::env::remove_var(WAITING_ROOM_THRESHOLD_ENV);

        // Pick a room whose jump-hash owner is NOT pod 0.
        let replicas = 3u32;
        let (room, owner_ord) = (0..200)
            .find_map(|i| {
                let r = format!("concurrent-t0-room-{i}");
                let o = crate::sfu::affinity::jump_hash(&r, replicas);
                (o != 0).then_some((r, o))
            })
            .expect("must find a room hashing to a non-zero ordinal among 200 candidates");

        let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats:4222".to_string());
        let nats_client = async_nats::connect(&nats_url)
            .await
            .expect("Failed to connect to NATS");
        let chat_server = ChatServer::new(nats_client).await.start();

        // One capturing recipient per session so we can assert that EVERY
        // session received the REDIRECT (rather than asserting on a shared
        // counter that masks per-session delivery failures).
        struct CapturingSession {
            received: Arc<Mutex<Vec<bytes::Bytes>>>,
        }
        impl Actor for CapturingSession {
            type Context = actix::Context<Self>;
        }
        impl Handler<Message> for CapturingSession {
            type Result = ();
            fn handle(&mut self, msg: Message, _ctx: &mut Self::Context) {
                self.received.lock().unwrap().push(msg.msg);
            }
        }

        const N: u64 = 100;
        let mut per_session_received: Vec<Arc<Mutex<Vec<bytes::Bytes>>>> =
            Vec::with_capacity(N as usize);
        let mut sids: Vec<SessionId> = Vec::with_capacity(N as usize);

        // Phase 1: register all N sessions synchronously so Connect lands
        // BEFORE any JoinRoom is processed. This matches the real flow
        // (WtChatSession.started does Connect.send.await before JoinRoom.send).
        for i in 0..N {
            let received: Arc<Mutex<Vec<bytes::Bytes>>> = Arc::new(Mutex::new(Vec::new()));
            let capturing = CapturingSession {
                received: received.clone(),
            }
            .start();
            let sid: SessionId = 80_000 + i;
            chat_server
                .send(Connect {
                    id: sid,
                    addr: capturing.recipient(),
                })
                .await
                .unwrap();
            per_session_received.push(received);
            sids.push(sid);
        }

        // Phase 2: fan out N JoinRoom messages CONCURRENTLY. Use a tokio
        // join_all so the actor's mailbox sees them all at once — the
        // single-threaded actor will still serialise execution, but the
        // queueing models the T=0 burst scenario from the bead.
        let join_futs: Vec<_> = sids
            .iter()
            .enumerate()
            .map(|(i, sid)| {
                let cs = chat_server.clone();
                let room = room.clone();
                let user = format!("listener-{i}@example.com");
                async move {
                    cs.send(JoinRoom {
                        session: *sid,
                        room,
                        user_id: user.clone(),
                        display_name: user,
                        observer: false,
                        capabilities: 0,
                    })
                    .await
                    .unwrap()
                }
            })
            .collect();
        let results = futures::future::join_all(join_futs).await;

        // ASSERTION 1: every JoinRoom returned Err with the pod-ownership
        // redirect message. Any Ok here would mean the joiner was admitted
        // onto the wrong pod, which is the proximate cause of bots running
        // out of the bot harness's per-pod accounting.
        for (i, r) in results.iter().enumerate() {
            assert!(
                r.is_err(),
                "session #{i} ({}) must be redirected (Err), got Ok",
                sids[i]
            );
            let err = r.as_ref().unwrap_err();
            assert!(
                err.contains("different pod"),
                "session #{i}: error must be a pod-ownership redirect, got: {err}"
            );
        }

        // Let the actor mailbox flush all REDIRECT Message deliveries.
        sleep(Duration::from_millis(300)).await;

        // ASSERTION 2 + 3: every session got exactly ONE ADMISSION_DECISION
        // {REDIRECT} packet, and every redirect target points at the same
        // owner pod DNS. If concurrent joiners produced inconsistent
        // redirect targets, the bot's reconnect logic would split between
        // pods and increment hop counters non-monotonically — the bead's
        // "loop redirect" signature.
        let expected_dns =
            format!("rustlemania-webtransport-{owner_ord}.webtransport-headless.svc.cluster.local");
        for (i, recv) in per_session_received.iter().enumerate() {
            let msgs = recv.lock().unwrap().clone();
            let mut redirects: Vec<AdmissionDecision> = Vec::new();
            for bytes in msgs.iter() {
                if let Ok(wrapper) = <PacketWrapper as ProtobufMessage>::parse_from_bytes(bytes) {
                    if wrapper.packet_type == PacketType::ADMISSION_DECISION.into() {
                        let dec = AdmissionDecision::parse_from_bytes(&wrapper.data)
                            .expect("AdmissionDecision must decode");
                        if dec.status.enum_value_or_default() == AdmStatus::REDIRECT {
                            redirects.push(dec);
                        }
                    }
                }
            }
            assert_eq!(
                redirects.len(),
                1,
                "session #{i} ({}) must receive exactly ONE REDIRECT \
                 packet at T=0; got {} (a duplicate would push the bot \
                 closer to MAX_REDIRECT_HOPS without it actually following \
                 conflicting targets)",
                sids[i],
                redirects.len()
            );
            let dec = &redirects[0];
            assert_eq!(
                dec.redirect_to, expected_dns,
                "session #{i} ({}) redirect_to MUST point at the same owner \
                 pod under concurrent joiners — any divergence here would \
                 split the bot fleet across pods on reconnect",
                sids[i]
            );
            assert_eq!(dec.reason, "wrong_owner");
        }

        // ASSERTION 4: NO session was admitted to the room. Both the
        // user-visible room_members list and the SFU member table must be
        // empty for the room. If any session is admitted on a non-owner
        // pod, the bot harness's per-pod participant-count gate breaks.
        let sfu_members = chat_server
            .send(SnapshotRoomMembers { room: room.clone() })
            .await
            .unwrap();
        assert!(
            sfu_members.map(|m| m.is_empty()).unwrap_or(true),
            "SFU member table for redirected room must be empty"
        );
        let user_members = chat_server
            .send(SnapshotRoomMembersList { room: room.clone() })
            .await
            .unwrap();
        assert!(
            user_members.map(|m| m.is_empty()).unwrap_or(true),
            "room_members for redirected room must be empty"
        );

        std::env::remove_var("POD_NAME");
        std::env::remove_var("STATEFULSET_REPLICAS");
        std::env::remove_var("SFU_TRANSPORT_KIND");
    }
}
