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

//! WebTransport Actor Bridge
//!
//! Bridges the gap between WebTransport (quinn async I/O) and Actix actors.
//!
//! Quinn uses pure tokio async, while actors use Actix's LocalSet runtime.
//! This bridge spawns I/O tasks that communicate with the actor via messages
//! and channels.
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────┐
//! │                    WebTransportBridge                                │
//! ├─────────────────────────────────────────────────────────────────────┤
//! │  ┌──────────────────┐              ┌──────────────────┐             │
//! │  │ UniStream Reader │              │ Datagram Reader  │             │
//! │  │ session.accept_  │              │ session.read_    │             │
//! │  │ uni().await      │              │ datagram().await │             │
//! │  └────────┬─────────┘              └────────┬─────────┘             │
//! │           │                                 │                       │
//! │           │ WtInbound(UniStream)            │ WtInbound(Datagram)   │
//! │           └────────────┬────────────────────┘                       │
//! │                        ▼                                            │
//! │           ┌────────────────────────┐                                │
//! │           │      Actor (external)  │                                │
//! │           └────────────┬───────────┘                                │
//! │                        │ outbound channel                           │
//! │                        ▼                                            │
//! │           ┌────────────────────────┐                                │
//! │           │      Writer Task       │                                │
//! │           │  UniStream / Datagram  │                                │
//! │           └────────────────────────┘                                │
//! └─────────────────────────────────────────────────────────────────────┘
//! ```

use crate::actors::packet_handler::DATAGRAM_MAX_SIZE;
use crate::actors::transports::wt_chat_session::{WtInbound, WtInboundSource};
use crate::constants::MAX_FRAME_SIZE;
use crate::sfu::priority_queue::PriorityReceiver;
use actix::Addr;
use bytes::Bytes;
use tokio::task::JoinSet;
use tracing::{error, info, warn};
use web_transport_quinn::Session;

/// Callback for tracking packets sent to clients (used in tests)
pub type PacketSentCallback = Box<dyn Fn() + Send + Sync>;

/// vc-s9e: grace period the bridge writer task holds its `Session` clone
/// after its outbound channel closes (every `PrioritySender` clone has been
/// dropped, which in practice means the `WtChatSession` actor stopped).
///
/// `web_transport_quinn::SendStream::finish` only sets the local FIN flag;
/// the underlying QUIC stream's FIN frame and any buffered bytes are
/// delivered later by quinn's I/O driver. Without this grace period, the
/// writer's `Session` clone drops at task exit, `bridge.shutdown()` then
/// aborts the reader tasks (dropping their clones too), and the connection
/// refcount hits zero — quinn's `implicit_close` discards anything not yet
/// flushed. That race is what lost the JoinRoom-Err
/// `ADMISSION_DECISION{REDIRECT}` packet that vc-883 took care to keep
/// alive across the actor stop, leading to "redirects_followed counter
/// never increments despite real redirects" (bot-spec §6.1).
///
/// 250ms is a generous one-RTT-class budget for any sane production network
/// and is well under the 2s `RECONNECT_GRACE_PERIOD` elsewhere in this
/// crate. Only paid on server-initiated teardown (writer recv→None); on
/// client-initiated disconnect the readers end first and `bridge.shutdown()`
/// aborts the writer before it reaches the sleep.
pub(crate) const WRITER_DRAIN_GRACE: std::time::Duration = std::time::Duration::from_millis(250);

/// Bridge between WebTransport session and an Actix actor.
///
/// Spawns I/O tasks that:
/// - Read from WebTransport streams/datagrams → send `WtInbound` to actor
/// - Receive from outbound channel → write to WebTransport streams/datagrams
pub struct WebTransportBridge {
    join_set: JoinSet<()>,
}

impl WebTransportBridge {
    /// Create a new bridge and start I/O tasks.
    ///
    /// # Arguments
    /// * `session` - The WebTransport session (quinn)
    /// * `actor_addr` - Address of the actor to receive inbound messages
    /// * `outbound_rx` - Channel receiver for outbound messages from actor
    #[allow(dead_code)] // Useful API even if currently only new_with_callback is used
    pub fn new<A>(session: Session, actor_addr: Addr<A>, outbound_rx: PriorityReceiver) -> Self
    where
        A: actix::Actor<Context = actix::Context<A>> + actix::Handler<WtInbound>,
    {
        Self::new_with_callback(session, actor_addr, outbound_rx, None)
    }

    /// Create a new bridge with optional callback for packet tracking.
    pub fn new_with_callback<A>(
        session: Session,
        actor_addr: Addr<A>,
        outbound_rx: PriorityReceiver,
        on_packet_sent: Option<PacketSentCallback>,
    ) -> Self
    where
        A: actix::Actor<Context = actix::Context<A>> + actix::Handler<WtInbound>,
    {
        let mut join_set = JoinSet::new();

        Self::spawn_unistream_reader(&mut join_set, session.clone(), actor_addr.clone());
        Self::spawn_datagram_reader(&mut join_set, session.clone(), actor_addr);
        Self::spawn_writer(&mut join_set, session, outbound_rx, on_packet_sent);

        Self { join_set }
    }

    /// Wait for any I/O task to complete (indicates session end).
    pub async fn wait_for_disconnect(&mut self) {
        self.join_set.join_next().await;
    }

    /// Shutdown all I/O tasks.
    pub async fn shutdown(mut self) {
        self.join_set.shutdown().await;
    }

    /// Spawn UniStream reader task.
    fn spawn_unistream_reader<A>(join_set: &mut JoinSet<()>, session: Session, actor_addr: Addr<A>)
    where
        A: actix::Actor<Context = actix::Context<A>> + actix::Handler<WtInbound>,
    {
        join_set.spawn(async move {
            while let Ok(mut uni_stream) = session.accept_uni().await {
                let actor_addr = actor_addr.clone();
                tokio::spawn(async move {
                    match uni_stream.read_to_end(MAX_FRAME_SIZE).await {
                        Ok(buf) => {
                            let buf_len = buf.len();
                            if let Err(e) = actor_addr.try_send(WtInbound {
                                data: Bytes::from(buf),
                                source: WtInboundSource::UniStream,
                            }) {
                                warn!("Dropped UniStream frame ({} bytes): {}", buf_len, e);
                            }
                        }
                        Err(e) => {
                            error!(
                                "UniStream read failed (limit {} bytes): {}",
                                MAX_FRAME_SIZE, e
                            );
                        }
                    }
                });
            }
            info!("WebTransport UniStream reader ended");
        });
    }

    /// Spawn Datagram reader task.
    fn spawn_datagram_reader<A>(join_set: &mut JoinSet<()>, session: Session, actor_addr: Addr<A>)
    where
        A: actix::Actor<Context = actix::Context<A>> + actix::Handler<WtInbound>,
    {
        join_set.spawn(async move {
            while let Ok(buf) = session.read_datagram().await {
                let _ = actor_addr.try_send(WtInbound {
                    data: buf,
                    source: WtInboundSource::Datagram,
                });
            }
            info!("WebTransport Datagram reader ended");
        });
    }

    /// Spawn Writer task.
    ///
    /// Drains the per-session [`PriorityReceiver`] (p5-4) and writes each
    /// packet to the QUIC session. Transport (UniStream vs Datagram) is
    /// recovered cheaply by inspecting the first two bytes of the
    /// `PacketWrapper`: a MEDIA wrapper begins with the field-1 varint tag
    /// `0x08` followed by `MEDIA=3`, so [`is_media_packet`] is a constant-
    /// time check that lets us preserve the legacy `send_auto` policy
    /// (non-MEDIA small packets via Datagram, MEDIA or large via UniStream)
    /// without a full protobuf parse on the hot egress path.
    fn spawn_writer(
        join_set: &mut JoinSet<()>,
        session: Session,
        mut outbound_rx: PriorityReceiver,
        on_packet_sent: Option<PacketSentCallback>,
    ) {
        join_set.spawn(async move {
            while let Some(data) = outbound_rx.recv().await {
                if is_datagram_eligible(&data) {
                    if let Err(e) = session.send_datagram(data) {
                        error!("Error sending datagram: {}", e);
                        // Don't break on datagram errors - they're unreliable
                    } else if let Some(ref callback) = on_packet_sent {
                        callback();
                    }
                } else {
                    match session.open_uni().await {
                        Ok(mut stream) => {
                            if let Err(e) = stream.write_all(&data).await {
                                error!("Error writing to UniStream: {}", e);
                                break;
                            }
                            if let Err(e) = stream.finish() {
                                error!("Error finishing UniStream: {}", e);
                            }
                            if let Some(ref callback) = on_packet_sent {
                                callback();
                            }
                        }
                        Err(e) => {
                            error!("Error opening UniStream: {}", e);
                            break;
                        }
                    }
                }
            }

            Self::writer_drain_grace().await;
            // `session` is still alive here — the grace inside
            // `writer_drain_grace` gave quinn's I/O driver time to flush
            // pending stream data before the clone drops at task exit.
            drop(session);
            info!("WebTransport Writer ended");
        });
    }

    /// vc-s9e: dedicated async helper invoked by [`Self::spawn_writer`]
    /// after its recv loop exits. Holds the `Session` clone for
    /// [`WRITER_DRAIN_GRACE`] so the trailing UniStream FIN + payload
    /// (notably the `ADMISSION_DECISION{REDIRECT}` packet emitted by the
    /// JoinRoom-Err path — see chat_server.rs ~1540 and the vc-883
    /// regression test in `wt_chat_session::tests`) reaches the wire
    /// before refcount→0 triggers quinn's implicit `Connection::close`,
    /// which discards anything not yet flushed.
    ///
    /// Exposed as a separate fn (rather than inlined into
    /// [`Self::spawn_writer`]) so the vc-s9e regression test in this
    /// module can confirm it actually sleeps without requiring a real
    /// `Session`. If this is ever inlined back, the regression-guard
    /// test below has to be replaced with an integration test that runs
    /// the full bridge against a quinn endpoint pair.
    async fn writer_drain_grace() {
        tokio::time::sleep(WRITER_DRAIN_GRACE).await;
    }
}

/// True iff `bytes` is a serialized `PacketWrapper` whose `packet_type`
/// is `MEDIA`.
///
/// `PacketWrapper.packet_type` is field 1 (varint, tag byte `0x08`) and
/// `MEDIA = 3` in `packet_wrapper.proto`. The rust-protobuf encoder emits
/// fields in declaration order, so a MEDIA-wrapped packet starts with the
/// two-byte prefix `[0x08, 0x03]`. This avoids a full protobuf parse — and
/// the `data: bytes` field's allocation/copy — on every outbound packet.
fn is_media_packet(bytes: &[u8]) -> bool {
    matches!(bytes, [0x08, 0x03, ..])
}

/// True iff `bytes` should be sent via Datagram (unreliable, low-overhead)
/// rather than a unidirectional stream.
///
/// Matches the legacy `send_auto` policy: non-MEDIA packets that fit
/// within the datagram MTU go via Datagram; MEDIA or oversized packets
/// go via UniStream.
fn is_datagram_eligible(bytes: &[u8]) -> bool {
    !is_media_packet(bytes) && bytes.len() <= DATAGRAM_MAX_SIZE
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sfu::priority_queue::{Class, PriorityReceiver, PrioritySender, SendOutcome};
    use bytes::Bytes;
    use protobuf::Message as ProtobufMessage;
    use videocall_types::protos::packet_wrapper::packet_wrapper::PacketType;
    use videocall_types::protos::packet_wrapper::PacketWrapper;

    /// Acceptance test for the p5-4 swap (bead vc-37u).
    ///
    /// Feeds a burst of `P4Enhancement` packets alongside a single
    /// `P0Control` packet into the production-wiring `PrioritySender` and
    /// asserts the consumer-side `PriorityReceiver::recv` surfaces the
    /// `P0Control` packet before the tail of the P4 burst — proving strict
    /// priority is in effect at the swap site (transport replacement of
    /// the legacy `mpsc::channel::<WtOutbound>(256)`).
    #[tokio::test]
    async fn p0_control_preempts_p4_burst_tail() {
        let (sender, channels) = PrioritySender::new();
        let mut receiver = PriorityReceiver::new(channels);

        // Producer: a burst of P4Enhancement, with a single P0Control
        // injected after the first few P4 packets so we observe preemption
        // mid-burst rather than ahead of it.
        let burst_len: usize = 32;
        for i in 0..burst_len {
            let outcome = sender.send(
                Class::P4Enhancement,
                Bytes::from(format!("p4-{i}").into_bytes()),
            );
            assert_eq!(outcome, SendOutcome::Sent, "P4 fill #{i} should succeed");
        }
        let outcome = sender.send(Class::P0Control, Bytes::from_static(b"p0-ctl"));
        assert_eq!(outcome, SendOutcome::Sent);

        // The very next packet drained must be the P0Control packet (strict
        // priority), even though the consumer has not yet read a single
        // P4 packet — the producer side is synchronous, so the P0 packet
        // is already enqueued by the time the consumer's first recv() runs.
        let first = receiver.recv().await.expect("receiver should yield P0");
        assert_eq!(
            &first[..],
            b"p0-ctl",
            "P0Control must preempt the P4 burst — got {:?}",
            std::str::from_utf8(&first).ok()
        );

        // The remaining P4 packets drain in FIFO order. We explicitly drain
        // all of them so the assertion "P0 reaches the receiver before any
        // of the burst's tail" is verified end-to-end (no P4 packet was
        // surfaced before P0).
        for i in 0..burst_len {
            let next = receiver.recv().await.expect("P4 backlog should drain");
            assert_eq!(&next[..], format!("p4-{i}").as_bytes());
        }
    }

    #[test]
    fn is_media_packet_detects_media_wrapper() {
        let mut w = PacketWrapper::new();
        w.packet_type = PacketType::MEDIA.into();
        w.data = b"payload".to_vec();
        let bytes = w.write_to_bytes().expect("encode MEDIA wrapper");
        assert!(
            is_media_packet(&bytes),
            "MEDIA wrapper must satisfy the fast-path media check"
        );
    }

    #[test]
    fn is_media_packet_rejects_non_media_wrappers() {
        for pt in [
            PacketType::CONGESTION,
            PacketType::SESSION_ASSIGNED,
            PacketType::MEETING,
            PacketType::SPEAKER_UPDATE,
            PacketType::SUBSCRIPTION_UPDATE,
            PacketType::LAYER_HINT,
            PacketType::ADMISSION_DECISION,
            PacketType::CAPABILITY_ANNOUNCE,
        ] {
            let mut w = PacketWrapper::new();
            w.packet_type = pt.into();
            let bytes = w.write_to_bytes().expect("encode wrapper");
            assert!(
                !is_media_packet(&bytes),
                "non-MEDIA wrapper ({pt:?}) must NOT satisfy the media check"
            );
        }
    }

    #[test]
    fn is_datagram_eligible_matches_legacy_policy() {
        // Non-MEDIA, small → Datagram (matches legacy send_auto for
        // is_media=false, size <= DATAGRAM_MAX_SIZE).
        let mut ctrl = PacketWrapper::new();
        ctrl.packet_type = PacketType::CONGESTION.into();
        let ctrl_bytes = ctrl.write_to_bytes().expect("encode CONGESTION");
        assert!(is_datagram_eligible(&ctrl_bytes));

        // MEDIA, small → UniStream (legacy: MEDIA always UniStream).
        let mut media = PacketWrapper::new();
        media.packet_type = PacketType::MEDIA.into();
        media.data = vec![0u8; 64];
        let media_bytes = media.write_to_bytes().expect("encode MEDIA");
        assert!(!is_datagram_eligible(&media_bytes));

        // Non-MEDIA, oversized → UniStream (legacy: !is_media but len >
        // DATAGRAM_MAX_SIZE falls to UniStream branch).
        let mut big = PacketWrapper::new();
        big.packet_type = PacketType::DIAGNOSTICS.into();
        big.data = vec![0u8; DATAGRAM_MAX_SIZE + 1];
        let big_bytes = big.write_to_bytes().expect("encode oversized");
        assert!(!is_datagram_eligible(&big_bytes));
    }

    // =====================================================================
    // vc-s9e regression tests for the writer's QUIC-flush grace period.
    //
    // The writer task's `Session` clone is what keeps quinn's I/O driver
    // alive long enough to actually transmit the trailing UniStream FIN +
    // payload after `stream.finish()` returns. Without the grace, every
    // server-initiated teardown (notably the JoinRoom-Err
    // `ADMISSION_DECISION{REDIRECT}` path tied off by vc-883) races the
    // implicit `Connection::close` that fires when the last clone drops,
    // and the bot's `start_inbound_consumer` sees `accept_uni` error
    // without ever observing the REDIRECT bytes. That's the
    // "redirects_followed counter never increments" symptom from
    // bot-spec §6.1.
    //
    // We can't exercise the real writer in-process without standing up a
    // full quinn endpoint pair, so the test below shapes the exact
    // observable invariant (the writer task does not drop its session-
    // proxy reference for at least WRITER_DRAIN_GRACE after every
    // PrioritySender clone has been dropped) via a stand-in that mirrors
    // the writer's structure: a tokio task that drains a `PriorityReceiver`
    // to None and then sleeps for `WRITER_DRAIN_GRACE` before letting its
    // owned reference drop. The reference is an `Arc<()>` cloned out of
    // the test scope so we can observe drop timing via `Arc::strong_count`.
    // =====================================================================
    // =====================================================================

    /// vc-s9e: documents the grace-period invariant. If this value is
    /// changed materially (e.g. dropped to 0 or above 2s) the writer's
    /// teardown semantics shift in ways that affect every server-initiated
    /// disconnect — re-read the constant's doc comment and bot-spec §6.1
    /// before tuning.
    #[test]
    fn writer_drain_grace_is_non_zero_and_bounded() {
        assert!(
            WRITER_DRAIN_GRACE >= std::time::Duration::from_millis(50),
            "WRITER_DRAIN_GRACE must be >= 50ms — anything shorter loses \
             the trailing FIN under realistic localhost wall-clock jitter, \
             reverting the vc-s9e fix. Current value: {WRITER_DRAIN_GRACE:?}"
        );
        assert!(
            WRITER_DRAIN_GRACE <= std::time::Duration::from_secs(2),
            "WRITER_DRAIN_GRACE must be <= 2s — longer values delay actor \
             teardown past the disconnect grace periods elsewhere in this \
             crate and start interacting with reconnection bookkeeping. \
             Current value: {WRITER_DRAIN_GRACE:?}"
        );
    }

    /// vc-s9e: the writer task must NOT drop its `Session` reference the
    /// instant `outbound_rx.recv()` returns None. It has to hold the
    /// reference for `WRITER_DRAIN_GRACE` so quinn's I/O driver can
    /// transmit the trailing UniStream FIN + payload from the last
    /// `stream.finish()` call. This test directly exercises
    /// [`WebTransportBridge::writer_drain_grace`] — the helper invoked
    /// by `spawn_writer` after its drain loop exits — by composing it
    /// with a proxy `Arc<()>` whose strong count stands in for the real
    /// `Session` refcount. Without the grace sleep the proxy drops the
    /// instant recv→None, with the grace sleep the proxy is held for at
    /// least `WRITER_DRAIN_GRACE/2`.
    ///
    /// If [`WebTransportBridge::writer_drain_grace`] is removed or
    /// inlined-and-deleted from `spawn_writer`, this test stops
    /// compiling, which is the right signal — the comment on
    /// `writer_drain_grace` calls that out explicitly.
    #[tokio::test(start_paused = false)]
    async fn writer_task_holds_session_clone_through_drain_grace_vc_s9e() {
        let (sender, channels) = PrioritySender::new();
        let mut receiver = PriorityReceiver::new(channels);

        // Stand-in for the writer's `session: Session` field — quinn's
        // `Session` is internally `Arc<...>`, so refcount semantics are
        // what matter here. We hold one clone outside the task to observe
        // the inside-task drop timing.
        let session_proxy = std::sync::Arc::new(());
        let writer_proxy = session_proxy.clone();
        assert_eq!(std::sync::Arc::strong_count(&session_proxy), 2);

        // Pre-populate one P0 packet so the writer drains at least once
        // before the channel closes (mirrors the real REDIRECT-then-stop
        // sequence: the actor pushes a final packet, then the actor stops
        // and `outbound_tx` drops → recv returns None on the *next* call).
        let outcome = sender.send(Class::P0Control, Bytes::from_static(b"redirect-stand-in"));
        assert_eq!(outcome, SendOutcome::Sent);

        let writer_task = tokio::spawn(async move {
            // Drain phase: identical shape to the production loop, minus
            // the I/O.
            while let Some(_data) = receiver.recv().await {
                // Real writer would call `session.open_uni().await` +
                // `write_all` + `finish` here. We omit because the
                // grace-window invariant is independent of payload.
            }
            // Exact same call as `spawn_writer`: this is the helper the
            // production writer uses. If it's removed or stops sleeping
            // for WRITER_DRAIN_GRACE, this test catches it.
            WebTransportBridge::writer_drain_grace().await;
            drop(writer_proxy);
        });

        // Drop the sender — the writer's recv will return None on its
        // next iteration. Record this moment so we can assert the writer
        // task holds the proxy for at least most of WRITER_DRAIN_GRACE
        // after this point.
        drop(sender);
        let dropped_at = std::time::Instant::now();

        // Sample at the midpoint of the grace window: the writer must
        // still be holding its clone (strong_count == 2). If the grace
        // sleep is removed, the writer drops its clone within a few
        // microseconds of recv returning None and strong_count == 1.
        let sample_at = WRITER_DRAIN_GRACE / 2;
        tokio::time::sleep(sample_at).await;
        let elapsed = dropped_at.elapsed();
        assert!(
            elapsed < WRITER_DRAIN_GRACE,
            "test scheduling failed — slept past grace window"
        );
        assert_eq!(
            std::sync::Arc::strong_count(&session_proxy),
            2,
            "writer task dropped its session proxy {:?} after sender drop \
             (grace window is {:?}) — the WRITER_DRAIN_GRACE sleep in \
             `spawn_writer` was removed or shortened, reverting the \
             vc-s9e fix: the trailing UniStream FIN + payload (e.g. the \
             vc-883 ADMISSION_DECISION{{REDIRECT}} packet) will race the \
             implicit Connection::close and the bot's `redirects_followed` \
             counter will stop incrementing.",
            elapsed,
            WRITER_DRAIN_GRACE,
        );

        // Let the writer finish so we don't leak the task.
        writer_task.await.expect("writer task did not panic");
        assert_eq!(
            std::sync::Arc::strong_count(&session_proxy),
            1,
            "writer task must eventually drop its session proxy"
        );
    }
}
