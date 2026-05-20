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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
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
    /// Reader tasks (UniStream + Datagram). They drive `wait_for_disconnect`
    /// ONLY when they exit because of a QUIC-level error (the client closed /
    /// the link dropped). When they exit cooperatively because the actor
    /// cleared [`AcceptInboundFlag`] on a redirect teardown, they park instead
    /// of returning, so they do NOT prematurely trip `wait_for_disconnect` and
    /// abort the writer mid-REDIRECT-flush (preserving vc-883 / vc-s9e). They
    /// are reaped by `shutdown()` once the writer has finished.
    readers: JoinSet<()>,
    /// Writer task. On a server-initiated teardown (redirect, error), the
    /// writer is the authoritative end-of-session signal: it ends only after
    /// `outbound_tx` has dropped (actor fully stopped, REDIRECT already
    /// pulled) AND its [`WRITER_DRAIN_GRACE`] flush window elapsed.
    writer: tokio::task::JoinHandle<()>,
}

/// vc-n9o: shared "keep accepting inbound" flag between the bridge reader
/// tasks and the [`WtChatSession`](crate::actors::transports::wt_chat_session::WtChatSession)
/// actor.
///
/// # The mailbox-starvation bug this breaks
///
/// On a JoinRoom-Err redirect (an `ADMISSION_DECISION{REDIRECT}` on a
/// non-owner pod, see chat_server.rs `JoinRoom`), the actor must drain the
/// pre-queued REDIRECT `Message` from its mailbox (vc-883) and then stop so
/// `outbound_tx` drops, the writer's `recv()` returns `None`, and
/// `wait_for_disconnect` returns — closing the QUIC session and letting the
/// client follow the redirect.
///
/// But `ctx.notify(StopSession)` enqueues the stop on the actor *items* list,
/// which actix only processes once the *mailbox* is fully drained. Under
/// sustained 30fps inbound, the unistream/datagram readers keep `try_send`ing
/// `WtInbound` into the mailbox, so it is never empty, so `StopSession` is
/// starved and the actor never stops — the redirected sender hangs on the
/// non-owner pod and never publishes to NATS (the multi-pod 0-decode root
/// cause).
///
/// Setting this flag to `false` makes both readers stop forwarding inbound
/// frames immediately, so the mailbox drains, `StopSession` runs, and the
/// teardown chain completes — WITHOUT touching the outbound (writer) path, so
/// the queued REDIRECT still reaches the wire first (vc-883) over its reliable
/// UniStream (vc-xnp) within the writer's flush grace (vc-s9e).
pub type AcceptInboundFlag = Arc<AtomicBool>;

impl WebTransportBridge {
    /// Create a new bridge and start I/O tasks.
    ///
    /// # Arguments
    /// * `session` - The WebTransport session (quinn)
    /// * `actor_addr` - Address of the actor to receive inbound messages
    /// * `outbound_rx` - Channel receiver for outbound messages from actor
    #[allow(dead_code)] // Useful API even if currently only new_with_callback is used
    pub fn new<A>(
        session: Session,
        actor_addr: Addr<A>,
        outbound_rx: PriorityReceiver,
        accept_inbound: AcceptInboundFlag,
    ) -> Self
    where
        A: actix::Actor<Context = actix::Context<A>> + actix::Handler<WtInbound>,
    {
        Self::new_with_callback(session, actor_addr, outbound_rx, accept_inbound, None)
    }

    /// Create a new bridge with optional callback for packet tracking.
    ///
    /// `accept_inbound` is the shared [`AcceptInboundFlag`] (vc-n9o): while
    /// `true` the readers forward client frames as `WtInbound`; once the
    /// actor clears it on a redirect teardown the readers stop forwarding so
    /// the actor mailbox can drain and `StopSession` can run.
    pub fn new_with_callback<A>(
        session: Session,
        actor_addr: Addr<A>,
        outbound_rx: PriorityReceiver,
        accept_inbound: AcceptInboundFlag,
        on_packet_sent: Option<PacketSentCallback>,
    ) -> Self
    where
        A: actix::Actor<Context = actix::Context<A>> + actix::Handler<WtInbound>,
    {
        let mut readers = JoinSet::new();

        Self::spawn_unistream_reader(
            &mut readers,
            session.clone(),
            actor_addr.clone(),
            accept_inbound.clone(),
        );
        Self::spawn_datagram_reader(&mut readers, session.clone(), actor_addr, accept_inbound);
        let writer = Self::spawn_writer(session, outbound_rx, on_packet_sent);

        Self { readers, writer }
    }

    /// Wait for the session to end.
    ///
    /// Returns when EITHER:
    /// * a reader task exits because of a QUIC-level error (client-initiated
    ///   disconnect / link drop), OR
    /// * the writer task exits (server-initiated teardown: the actor stopped,
    ///   `outbound_tx` dropped, and the [`WRITER_DRAIN_GRACE`] flush window
    ///   elapsed — so the trailing REDIRECT is already on the wire).
    ///
    /// vc-n9o: a reader that exits *cooperatively* (because the actor cleared
    /// [`AcceptInboundFlag`] on a redirect teardown) does NOT return from its
    /// task — it parks — so it can never trip this function and abort the
    /// writer before the REDIRECT has flushed. On the redirect path the writer
    /// branch is what fires, after the REDIRECT is safely transmitted.
    pub async fn wait_for_disconnect(&mut self) {
        tokio::select! {
            _ = self.readers.join_next() => {}
            _ = &mut self.writer => {}
        }
    }

    /// Shutdown all I/O tasks (readers + writer).
    pub async fn shutdown(mut self) {
        self.readers.shutdown().await;
        self.writer.abort();
        let _ = self.writer.await;
    }

    /// Spawn UniStream reader task.
    ///
    /// vc-n9o: the loop stops forwarding `WtInbound` once `accept_inbound` is
    /// cleared. The flag is checked at the top of the loop and again right
    /// after each `accept_uni()` returns, plus inside the per-stream read task
    /// before `try_send`, so a stream accepted just before the flag flipped
    /// cannot slip a frame into the mailbox the actor is trying to drain to a
    /// stop.
    ///
    /// IMPORTANT: a reader currently parked *inside* `accept_uni().await` is
    /// NOT released by clearing the flag — there is no cancellation signal on
    /// the await, so the check only fires once `accept_uni` next returns (a new
    /// stream arrives) or the await ends with a QUIC error. For an actively
    /// sending client (the bug's scenario) `accept_uni` returns ~30×/s, so the
    /// flag is observed promptly. For a client that goes silent exactly at the
    /// redirect, the parked reader is instead reaped by `shutdown()` once the
    /// writer-driven teardown completes; and because a silent client stops
    /// feeding the mailbox, `StopSession` is no longer starved anyway. The
    /// actor-side `REDIRECT_TEARDOWN_DEADLINE` backstop bounds the worst case.
    fn spawn_unistream_reader<A>(
        join_set: &mut JoinSet<()>,
        session: Session,
        actor_addr: Addr<A>,
        accept_inbound: AcceptInboundFlag,
    ) where
        A: actix::Actor<Context = actix::Context<A>> + actix::Handler<WtInbound>,
    {
        join_set.spawn(async move {
            // Cooperative-stop bookkeeping: if we exit the accept loop because
            // the actor cleared `accept_inbound` (redirect teardown), we must
            // NOT return — returning would trip `wait_for_disconnect` and let
            // `shutdown()` abort the writer mid-REDIRECT-flush. Instead we park
            // (`std::future::pending`) until `shutdown()` aborts us, after the
            // writer has finished. A QUIC-error exit, by contrast, DOES return
            // so it can drive `wait_for_disconnect` (client-initiated close).
            let cooperative_stop = loop {
                // Top-of-loop check catches a flag already cleared before we
                // re-enter `accept_uni`.
                if !accept_inbound.load(Ordering::Acquire) {
                    break true;
                }
                let mut uni_stream = match session.accept_uni().await {
                    Ok(s) => s,
                    Err(_) => break false, // QUIC-level error: client closed.
                };
                // Re-check after the (possibly long) accept await: the actor
                // may have cleared the flag while we were parked.
                if !accept_inbound.load(Ordering::Acquire) {
                    break true;
                }
                let actor_addr = actor_addr.clone();
                let accept_inbound = accept_inbound.clone();
                tokio::spawn(async move {
                    match uni_stream.read_to_end(MAX_FRAME_SIZE).await {
                        Ok(buf) => {
                            // Final gate: do not enqueue into a mailbox the
                            // actor is draining toward StopSession (vc-n9o).
                            if !accept_inbound.load(Ordering::Acquire) {
                                return;
                            }
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
            };
            if cooperative_stop {
                // Redirect teardown: park until `shutdown()` aborts us, so we
                // never trip `wait_for_disconnect` ahead of the writer's
                // REDIRECT flush (vc-883 / vc-s9e).
                info!("WebTransport UniStream reader parking on cooperative stop");
                std::future::pending::<()>().await;
            }
            info!("WebTransport UniStream reader ended");
        });
    }

    /// Spawn Datagram reader task.
    ///
    /// vc-n9o: same flag discipline as the UniStream reader. A reader parked
    /// inside `read_datagram().await` is not released by clearing the flag; it
    /// observes the flag once `read_datagram` next returns, or is reaped by
    /// `shutdown()`. See `spawn_unistream_reader` for the full rationale.
    fn spawn_datagram_reader<A>(
        join_set: &mut JoinSet<()>,
        session: Session,
        actor_addr: Addr<A>,
        accept_inbound: AcceptInboundFlag,
    ) where
        A: actix::Actor<Context = actix::Context<A>> + actix::Handler<WtInbound>,
    {
        join_set.spawn(async move {
            let cooperative_stop = loop {
                if !accept_inbound.load(Ordering::Acquire) {
                    break true;
                }
                let buf = match session.read_datagram().await {
                    Ok(b) => b,
                    Err(_) => break false, // QUIC-level error: client closed.
                };
                if !accept_inbound.load(Ordering::Acquire) {
                    break true;
                }
                let _ = actor_addr.try_send(WtInbound {
                    data: buf,
                    source: WtInboundSource::Datagram,
                });
            };
            if cooperative_stop {
                // See the UniStream reader: park rather than return so we do
                // not trip `wait_for_disconnect` before the writer flushes the
                // REDIRECT (vc-n9o / vc-883 / vc-s9e).
                info!("WebTransport Datagram reader parking on cooperative stop");
                std::future::pending::<()>().await;
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
        session: Session,
        mut outbound_rx: PriorityReceiver,
        on_packet_sent: Option<PacketSentCallback>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
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
        })
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

/// True iff `bytes` is a serialized `PacketWrapper` whose `packet_type`
/// is `ADMISSION_DECISION` — the redirect-critical control packet.
///
/// `PacketWrapper.packet_type` is field 1 (varint, tag byte `0x08`) and
/// `ADMISSION_DECISION = 13` in `packet_wrapper.proto`, so an
/// ADMISSION_DECISION-wrapped packet starts with the two-byte prefix
/// `[0x08, 0x0d]`. Like [`is_media_packet`] this is a constant-time check
/// that avoids a full protobuf parse on the egress path.
///
/// Datagrams are lossy (unreliable QUIC), but a redirect MUST be reliably
/// delivered: if the `ADMISSION_DECISION{REDIRECT}` packet is dropped, a
/// non-owner-pod listener never follows the redirect and silently fails to
/// receive media (vc-xnp). The bot reader only consumes uni-streams
/// (`bot/src/webtransport_client.rs`), so this packet MUST go via UniStream.
fn is_redirect_critical(bytes: &[u8]) -> bool {
    matches!(bytes, [0x08, 0x0d, ..])
}

/// True iff `bytes` should be sent via Datagram (unreliable, low-overhead)
/// rather than a unidirectional stream.
///
/// Matches the legacy `send_auto` policy: non-MEDIA packets that fit
/// within the datagram MTU go via Datagram; MEDIA or oversized packets
/// go via UniStream.
///
/// Exception (vc-xnp): redirect-critical control packets
/// (`ADMISSION_DECISION`, see [`is_redirect_critical`]) are forced onto a
/// reliable UniStream even though they are small non-MEDIA packets. The
/// exclusion is scoped narrowly to that packet type — ordinary small
/// control packets (CONGESTION, etc.) remain datagram-eligible.
fn is_datagram_eligible(bytes: &[u8]) -> bool {
    !is_media_packet(bytes) && !is_redirect_critical(bytes) && bytes.len() <= DATAGRAM_MAX_SIZE
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

    /// vc-xnp: small non-MEDIA control packets other than the
    /// redirect-critical `ADMISSION_DECISION` must remain datagram-eligible.
    /// The exclusion added for redirect must NOT regress these.
    #[test]
    fn small_control_packets_remain_datagram_eligible() {
        for pt in [
            PacketType::CONGESTION,
            PacketType::SESSION_ASSIGNED,
            PacketType::MEETING,
            PacketType::SPEAKER_UPDATE,
            PacketType::SUBSCRIPTION_UPDATE,
            PacketType::LAYER_HINT,
            PacketType::CAPABILITY_ANNOUNCE,
        ] {
            let mut w = PacketWrapper::new();
            w.packet_type = pt.into();
            let bytes = w.write_to_bytes().expect("encode control wrapper");
            assert!(
                is_datagram_eligible(&bytes),
                "small non-MEDIA control packet ({pt:?}) must stay datagram-eligible"
            );
        }
    }

    /// vc-xnp regression test: a real serialized `ADMISSION_DECISION`
    /// wrapper (the redirect-critical control packet) MUST NOT be
    /// datagram-eligible — it has to ride a reliable UniStream so the bot
    /// reader (uni-stream only) actually receives the redirect.
    ///
    /// We encode via `write_to_bytes()` rather than hardcoding bytes to
    /// prove the real wire encoding hits the exclusion. If this fails, the
    /// redirect would be sent as a lossy datagram and `redirect_chain`
    /// would stay 0 (the original vc-xnp bug).
    #[test]
    fn admission_decision_is_not_datagram_eligible() {
        let mut redirect = PacketWrapper::new();
        redirect.packet_type = PacketType::ADMISSION_DECISION.into();
        // Mirror a realistic small redirect payload (a host string).
        redirect.data =
            b"rustlemania-webtransport-0.webtransport-headless.svc.cluster.local".to_vec();
        let bytes = redirect
            .write_to_bytes()
            .expect("encode ADMISSION_DECISION wrapper");

        // Sanity: the payload is well within the datagram MTU, so the ONLY
        // reason it must be UniStream-routed is the redirect-critical
        // exclusion — not size.
        assert!(
            bytes.len() <= DATAGRAM_MAX_SIZE,
            "test fixture must be small enough to otherwise be datagram-eligible"
        );
        assert!(
            is_redirect_critical(&bytes),
            "real ADMISSION_DECISION encoding must match the fast-path detector"
        );
        assert!(
            !is_datagram_eligible(&bytes),
            "ADMISSION_DECISION (redirect) MUST be reliably delivered via UniStream, \
             not a lossy datagram"
        );
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
