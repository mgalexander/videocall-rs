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
            info!("WebTransport Writer ended");
        });
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
}
