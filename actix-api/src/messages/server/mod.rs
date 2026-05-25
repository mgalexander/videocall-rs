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

use std::sync::Arc;

use crate::actors::packet_handler::PacketKind;
use crate::actors::session_logic::{RoomId, SessionId};

use super::session::Message;
use actix::{Message as ActixMessage, Recipient};

#[derive(ActixMessage)]
#[rtype(result = "()")]
pub struct ClientMessage {
    pub session: SessionId,
    pub user: String,
    pub room: RoomId,
    pub msg: Packet,
}

/// Why a [`JoinRoom`] was declined (the `Err` arm of its result).
///
/// vc-n9o: the transport actors map this onto `sfu_session_teardown_total`'s
/// `reason` label so the teardown counter lines up with the
/// `sfu_join_decision_total` decision counter — in particular so a `Reject`
/// teardown is NOT mislabeled `redirect` (which would inflate the teardown
/// side and mask a real redirect-vs-teardown gap), and so the `redirect`
/// teardown bucket covers exactly the redirect decisions on BOTH transports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinDeclineKind {
    /// Pod-ordinal or cross-region `ADMISSION_DECISION{REDIRECT}`: the client
    /// must reconnect to another pod/region. This is the bucket the vc-n9o
    /// teardown-reliability fix protects.
    Redirect,
    /// Hard-cap `ADMISSION_DECISION{REJECTED}`: the room is full.
    Reject,
    /// Validation / internal precondition failure (reserved user id, missing
    /// session record, etc.).
    Error,
}

/// Typed `JoinRoom` decline carrying both a machine-readable [`JoinDeclineKind`]
/// (for metrics / teardown labeling) and a human-readable message (for logs
/// and the client-facing error). Replaces the previous bare `String` error so
/// the transport actors can label teardowns without fragile string matching
/// (vc-n9o).
#[derive(Debug, Clone)]
pub struct JoinRoomError {
    pub kind: JoinDeclineKind,
    pub message: String,
}

impl JoinRoomError {
    pub fn redirect(message: String) -> Self {
        Self {
            kind: JoinDeclineKind::Redirect,
            message,
        }
    }
    pub fn reject(message: String) -> Self {
        Self {
            kind: JoinDeclineKind::Reject,
            message,
        }
    }
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            kind: JoinDeclineKind::Error,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for JoinRoomError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for JoinRoomError {}

/// `?`-coercion from `JoinRoomError` into `String`.
///
/// USED (do not remove): the `register_and_join` helpers in the SFU
/// integration tests propagate a `Result<(), JoinRoomError>` with `?` inside
/// functions that return `Result<(), String>`, e.g.
/// `tests/sfu_p5_burst_test.rs:438`, `tests/sfu_integration.rs:259`,
/// `tests/sfu_p4_throttle_test.rs:260`, `tests/sfu_12client_demo.rs:218`.
/// The `?` operator's implicit `From` conversion is what drives this impl —
/// there is no explicit `.into()`/`String::from` call site, so it can look
/// unused at a glance, but removing it fails to compile all four test crates.
/// The `kind` discriminant is dropped here; only the transport actors consume
/// it (via `TeardownReason::from_join_decline`).
impl From<JoinRoomError> for String {
    fn from(e: JoinRoomError) -> Self {
        e.message
    }
}

#[derive(ActixMessage)]
#[rtype(result = "Result<(), JoinRoomError>")]
pub struct JoinRoom {
    pub session: SessionId,
    pub room: RoomId,
    pub user_id: String,
    /// Participant's chosen display name (from JWT claims).
    /// Falls back to `user_id` when no display name is available.
    pub display_name: String,
    /// When true, this is an observer session (waiting room) and should NOT
    /// trigger PARTICIPANT_JOINED notifications.
    pub observer: bool,
    /// SFU capability bitmask declared by the client's `CONNECTION` packet.
    /// Defaults to `0` for legacy clients or when the JoinRoom is built
    /// before any CONNECTION packet has been observed (the common case
    /// today; CONNECTION currently arrives after JoinRoom).
    pub capabilities: u32,
}

#[derive(ActixMessage)]
#[rtype(result = "()")]
pub struct Connect {
    pub id: SessionId,
    pub addr: Recipient<Message>,
}

#[derive(ActixMessage)]
#[rtype(result = "()")]
pub struct Packet {
    pub data: Arc<Vec<u8>>,
    /// Classification computed once upstream by `classify_and_inspect`.
    pub kind: PacketKind,
}

#[derive(ActixMessage)]
#[rtype(result = "()")]
pub struct Disconnect {
    pub session: SessionId,
    pub room: RoomId,
    pub user_id: String,
    /// Participant's chosen display name (from JWT claims).
    /// Falls back to `user_id` when no display name is available.
    pub display_name: String,
    /// When true, the disconnecting session is an observer (waiting room)
    /// and should NOT trigger PARTICIPANT_LEFT notifications.
    pub observer: bool,
    /// When true, this Disconnect is a server-synthesized leave (e.g.,
    /// cross-region redirect) where the client will not reconnect to this
    /// pod, so the grace-period deferral MUST be skipped to avoid a
    /// ghost-participant window for cross-region peers.
    pub redirect: bool,
}

#[derive(ActixMessage)]
#[rtype(result = "()")]
pub struct Leave {
    pub session: SessionId,
    pub room: RoomId,
    pub user_id: String,
}

#[derive(ActixMessage)]
#[rtype(result = "()")]
pub struct ActivateConnection {
    pub session: SessionId,
}
