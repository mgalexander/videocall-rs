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
