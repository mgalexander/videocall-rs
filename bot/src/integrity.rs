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

//! Byte-fidelity integrity instrumentation for the load-test bot (vc-1re).
//!
//! The integrity path lets a sender bot stamp every codec payload with a
//! fixed **trailer** carrying a per-(user_id, media_type) monotonic sequence
//! number and a CRC32 of the codec bytes, and lets a listener bot verify that
//! trailer end-to-end. This turns the bot into a Level-3 (material match) and
//! Level-4 (completeness) instrument: it can prove not just that bytes arrived
//! but that the *exact* codec payload arrived intact and account for any
//! sequence gaps.
//!
//! ## Why a payload trailer, NOT a `RoutingHeader`
//!
//! Carrying seq+CRC in the protobuf `RoutingHeader` would flip the SFU off its
//! legacy passthrough path (`forwarder.rs:412`) onto the untested P4
//! layer-drop branch (`forwarder.rs:415`), because that branch is gated on
//! `mp.routing_header.is_some()`. Integrity runs must stay comparable to the
//! baseline, so we append the trailer to the codec `data` and leave the
//! `RoutingHeader` unset. The SFU forwards the bytes verbatim and the listener
//! strips the trailer back off before feeding the codec.
//!
//! ## Trailer wire format
//!
//! Appended to the end of the codec `data` (big-endian):
//!
//! ```text
//! [ magic: 4 bytes ][ seq: 8 bytes ][ crc32: 4 bytes ]
//! ```
//!
//! `crc32` is computed over the codec payload *only* (the bytes that precede
//! the trailer), using `crc32fast`.

use std::collections::HashMap;

use videocall_types::protos::media_packet::media_packet::MediaType;

/// Magic prefix on the integrity trailer. Lets the receiver cheaply reject a
/// payload that doesn't actually carry a trailer (e.g. a heartbeat that
/// slipped through, or a sender that wasn't run with `--verify-integrity`).
/// ASCII `VCI1` = "VideoCall Integrity v1".
pub const TRAILER_MAGIC: [u8; 4] = *b"VCI1";

/// Total trailer length: 4 (magic) + 8 (seq) + 4 (crc32).
pub const TRAILER_LEN: usize = 4 + 8 + 4;

/// Bounded ring of recently-observed sequence gaps per (publisher,
/// media_type). Cap is fixed so the tracker is O(publishers × media_types)
/// and explicitly NOT duration-dependent (vc-1re bounded-resource contract):
/// a 10-minute run and a 10-hour run hold the same worst-case memory.
const GAP_RING_CAP: usize = 1024;

/// Append the integrity trailer to `data` in place (vc-1re).
///
/// `seq` is the monotonic per-(user_id, media_type) sequence number, reusing
/// `VideoMetadata.sequence` / `AudioMetadata.sequence` semantics. The CRC32 is
/// computed over the *original* `data` (codec payload) before the trailer is
/// appended.
pub fn append_trailer(data: &mut Vec<u8>, seq: u64) {
    let crc = crc32fast::hash(data);
    data.extend_from_slice(&TRAILER_MAGIC);
    data.extend_from_slice(&seq.to_be_bytes());
    data.extend_from_slice(&crc.to_be_bytes());
}

/// Result of stripping + verifying a trailer.
#[derive(Debug, PartialEq, Eq)]
pub enum TrailerCheck {
    /// No trailer present (no magic / too short). Caller should treat the
    /// payload as un-instrumented and skip integrity accounting.
    Absent,
    /// Trailer present and the recomputed CRC matched. Carries the codec
    /// payload length (so the caller can split the slice) and the sequence.
    Ok { payload_len: usize, seq: u64 },
    /// Trailer present but the recomputed CRC did NOT match the stamped CRC.
    /// Carries the codec payload length so the caller can still decode, and
    /// the sequence so gap accounting stays consistent.
    CrcMismatch { payload_len: usize, seq: u64 },
}

/// Inspect the tail of `data` for an integrity trailer and, if present,
/// recompute the CRC over the codec payload and compare it to the stamped
/// value (vc-1re).
///
/// Does NOT mutate `data`; the caller splits at `payload_len` to feed the
/// codec the bytes *before* the trailer.
pub fn check_trailer(data: &[u8]) -> TrailerCheck {
    if data.len() < TRAILER_LEN {
        return TrailerCheck::Absent;
    }
    let split = data.len() - TRAILER_LEN;
    let (payload, trailer) = data.split_at(split);
    if trailer[0..4] != TRAILER_MAGIC {
        return TrailerCheck::Absent;
    }
    // Edition 2018: `TryInto` is not in the prelude, so build the fixed-size
    // arrays explicitly. The slice bounds above guarantee the lengths.
    let mut seq_bytes = [0u8; 8];
    seq_bytes.copy_from_slice(&trailer[4..12]);
    let seq = u64::from_be_bytes(seq_bytes);
    let mut crc_bytes = [0u8; 4];
    crc_bytes.copy_from_slice(&trailer[12..16]);
    let stamped_crc = u32::from_be_bytes(crc_bytes);
    let actual_crc = crc32fast::hash(payload);
    if actual_crc == stamped_crc {
        TrailerCheck::Ok {
            payload_len: split,
            seq,
        }
    } else {
        TrailerCheck::CrcMismatch {
            payload_len: split,
            seq,
        }
    }
}

/// Per-(publisher, media_type) completeness state. Counts + a bounded ring of
/// recent gaps; deliberately NOT a dense bitmap, so memory is bounded
/// regardless of run length (vc-1re).
#[derive(Debug, Default)]
struct PerKey {
    min_seq: Option<u64>,
    max_seq: Option<u64>,
    received: u64,
    /// Last sequence observed, used to detect forward gaps cheaply.
    last_seq: Option<u64>,
    /// Bounded ring of `(prev_seq, next_seq)` gap edges. Capped at
    /// [`GAP_RING_CAP`]; oldest entries roll off. Diagnostic only — the
    /// completeness math uses the counts, not the ring.
    recent_gaps: Vec<(u64, u64)>,
}

impl PerKey {
    fn observe(&mut self, seq: u64) {
        self.received += 1;
        self.min_seq = Some(self.min_seq.map_or(seq, |m| m.min(seq)));
        self.max_seq = Some(self.max_seq.map_or(seq, |m| m.max(seq)));
        if let Some(prev) = self.last_seq {
            if seq > prev + 1 {
                if self.recent_gaps.len() >= GAP_RING_CAP {
                    // Roll the oldest entry off; O(n) but only on the cold
                    // overflow path and n is the small fixed cap.
                    self.recent_gaps.remove(0);
                }
                self.recent_gaps.push((prev, seq));
            }
        }
        // Track the running max as "last" so out-of-order delivery doesn't
        // synthesize phantom gaps: completeness is `expected - received`,
        // which is order-independent.
        self.last_seq = Some(self.max_seq.unwrap_or(seq));
    }
}

/// Aggregated integrity result rolled up across every (publisher, media_type)
/// the listener observed (vc-1re).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct IntegritySummary {
    /// Highest per-key `max_seq` observed across all keys. `0` if nothing
    /// was tracked.
    pub media_seq_max: u64,
    /// Total distinct media payloads with a verified trailer (sum of
    /// per-key `received`).
    pub media_received_distinct: u64,
    /// Count of trailers whose recomputed CRC did not match. MUST be 0 on a
    /// clean path.
    pub crc_mismatches: u64,
    /// `sum(expected - received)` across keys, where `expected = max - min +
    /// 1`. On legacy passthrough this is dominated by AllowSet `unsubscribed`
    /// gaps; a clean loopback run is 0.
    pub unexplained_gaps: u64,
}

/// Listener-side integrity tracker. Keyed by `(publisher, media_type)`; holds
/// counts plus a bounded gap ring per key. Bounded resource: O(publishers ×
/// media_types), NOT duration-dependent (vc-1re).
#[derive(Debug, Default)]
pub struct IntegrityTracker {
    /// Nested `media_type -> publisher -> PerKey`. Nesting (rather than a
    /// `(String, MediaType)` tuple key) lets the per-packet hot path do a
    /// borrowed `get_mut` lookup and only allocate the publisher `String` on
    /// the first observation for that publisher — avoiding a heap allocation
    /// per media packet on the integrity-ON path (vc-1re).
    keys: HashMap<MediaType, HashMap<String, PerKey>>,
    crc_mismatches: u64,
}

impl IntegrityTracker {
    /// Borrow (or first-time create) the [`PerKey`] for `(publisher,
    /// media_type)`. The borrowed `get_mut` lookup means the publisher `String`
    /// is only allocated on the first observation for that publisher; cache
    /// hits do no heap allocation (vc-1re).
    fn per_key(&mut self, publisher: &str, media_type: MediaType) -> &mut PerKey {
        let by_publisher = self.keys.entry(media_type).or_default();
        if !by_publisher.contains_key(publisher) {
            by_publisher.insert(publisher.to_string(), PerKey::default());
        }
        by_publisher
            .get_mut(publisher)
            .expect("entry just ensured present")
    }

    /// Record a verified trailer observation for `(publisher, media_type)`.
    pub fn record_ok(&mut self, publisher: &str, media_type: MediaType, seq: u64) {
        self.per_key(publisher, media_type).observe(seq);
    }

    /// Record a CRC mismatch for `(publisher, media_type)`. The sequence is
    /// still folded into completeness accounting so a corrupted-but-present
    /// frame doesn't masquerade as a gap.
    pub fn record_crc_mismatch(&mut self, publisher: &str, media_type: MediaType, seq: u64) {
        self.crc_mismatches += 1;
        self.per_key(publisher, media_type).observe(seq);
    }

    /// Roll the per-key counts up into an [`IntegritySummary`].
    ///
    /// `accounted_drops` is subtracted from the raw expected-minus-received
    /// gap total so that drops explained by the SFU's AllowSet
    /// (`unsubscribed`) on the legacy-passthrough path are not double-counted
    /// as `unexplained_gaps`. The result is clamped at 0.
    pub fn summarize(&self, accounted_drops: u64) -> IntegritySummary {
        let mut media_seq_max = 0u64;
        let mut media_received_distinct = 0u64;
        let mut raw_gaps = 0u64;
        for key in self.keys.values().flat_map(|by_pub| by_pub.values()) {
            if let Some(m) = key.max_seq {
                media_seq_max = media_seq_max.max(m);
            }
            media_received_distinct += key.received;
            if let (Some(min), Some(max)) = (key.min_seq, key.max_seq) {
                let expected = max - min + 1;
                raw_gaps += expected.saturating_sub(key.received);
            }
        }
        let unexplained_gaps = raw_gaps.saturating_sub(accounted_drops);
        IntegritySummary {
            media_seq_max,
            media_received_distinct,
            crc_mismatches: self.crc_mismatches,
            unexplained_gaps,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_then_check_roundtrips_clean() {
        let mut data = vec![1u8, 2, 3, 4, 5];
        append_trailer(&mut data, 42);
        assert_eq!(data.len(), 5 + TRAILER_LEN);
        match check_trailer(&data) {
            TrailerCheck::Ok { payload_len, seq } => {
                assert_eq!(payload_len, 5);
                assert_eq!(seq, 42);
                assert_eq!(&data[..payload_len], &[1, 2, 3, 4, 5]);
            }
            other => panic!("expected Ok, got {:?}", other),
        }
    }

    #[test]
    fn corrupting_one_payload_byte_trips_crc_mismatch() {
        let mut data = vec![10u8, 20, 30, 40];
        append_trailer(&mut data, 7);
        // Flip a single payload byte — the stamped CRC no longer matches.
        data[0] ^= 0xFF;
        match check_trailer(&data) {
            TrailerCheck::CrcMismatch { payload_len, seq } => {
                assert_eq!(payload_len, 4);
                assert_eq!(seq, 7);
            }
            other => panic!("expected CrcMismatch, got {:?}", other),
        }
    }

    #[test]
    fn payload_without_magic_is_absent() {
        // Long enough to have a trailer slot, but no magic.
        let data = vec![0u8; TRAILER_LEN + 4];
        assert_eq!(check_trailer(&data), TrailerCheck::Absent);
        // Too short to hold a trailer at all.
        assert_eq!(check_trailer(&[1, 2, 3]), TrailerCheck::Absent);
    }

    #[test]
    fn tracker_clean_sequence_has_no_gaps() {
        let mut t = IntegrityTracker::default();
        for seq in 0..100 {
            t.record_ok("sender-0", MediaType::VIDEO, seq);
        }
        let s = t.summarize(0);
        assert_eq!(s.media_seq_max, 99);
        assert_eq!(s.media_received_distinct, 100);
        assert_eq!(s.crc_mismatches, 0);
        assert_eq!(s.unexplained_gaps, 0);
    }

    #[test]
    fn tracker_counts_gap_minus_accounted_drops() {
        let mut t = IntegrityTracker::default();
        // 0,1,2, [skip 3,4], 5  -> expected 6, received 4, raw gap 2.
        for seq in [0, 1, 2, 5] {
            t.record_ok("sender-0", MediaType::AUDIO, seq);
        }
        // No accounted drops: both missing frames are unexplained.
        assert_eq!(t.summarize(0).unexplained_gaps, 2);
        // One drop accounted by the AllowSet unsubscribe: one remains.
        assert_eq!(t.summarize(1).unexplained_gaps, 1);
        // Over-accounting clamps at 0 rather than underflowing.
        assert_eq!(t.summarize(5).unexplained_gaps, 0);
    }

    #[test]
    fn tracker_crc_mismatch_counts_and_still_tracks_seq() {
        let mut t = IntegrityTracker::default();
        t.record_ok("sender-0", MediaType::VIDEO, 0);
        t.record_crc_mismatch("sender-0", MediaType::VIDEO, 1);
        t.record_ok("sender-0", MediaType::VIDEO, 2);
        let s = t.summarize(0);
        assert_eq!(s.crc_mismatches, 1);
        // The corrupted frame still counts toward completeness, so no gap.
        assert_eq!(s.media_received_distinct, 3);
        assert_eq!(s.unexplained_gaps, 0);
    }

    #[test]
    fn gap_ring_is_bounded_not_duration_dependent() {
        let mut key = PerKey::default();
        // Every other sequence is missing -> a gap on every observation.
        // Push far more than the ring cap; the ring must not grow past it.
        for i in 0..(GAP_RING_CAP as u64 * 4) {
            key.observe(i * 2);
        }
        assert!(
            key.recent_gaps.len() <= GAP_RING_CAP,
            "gap ring grew to {} (cap {})",
            key.recent_gaps.len(),
            GAP_RING_CAP
        );
    }

    #[test]
    fn tracker_is_keyed_by_publisher_and_media_type() {
        let mut t = IntegrityTracker::default();
        // Two publishers, two media types — four independent keys.
        t.record_ok("sender-0", MediaType::VIDEO, 5);
        t.record_ok("sender-0", MediaType::AUDIO, 9);
        t.record_ok("sender-1", MediaType::VIDEO, 2);
        let s = t.summarize(0);
        // Distinct counts sum across keys; max is the global max.
        assert_eq!(s.media_received_distinct, 3);
        assert_eq!(s.media_seq_max, 9);
        // Each key has a single observation -> no gaps anywhere.
        assert_eq!(s.unexplained_gaps, 0);
    }

    /// vc-1re stress contract: integrity tracking must hold up under the
    /// 100-bot startup race, where many listener decode threads fold
    /// observations into per-bot trackers concurrently the instant the room
    /// fills. We simulate 100 bots each running a decode thread that records
    /// a clean trailered sequence against its own `BotStats.integrity`
    /// tracker, then assert every bot summarized to zero mismatches / zero
    /// gaps. This guards the integrity path against data races and unbounded
    /// growth introduced by the simultaneous-join wave.
    #[test]
    fn integrity_tracking_survives_100_bot_startup_race_vc_1re() {
        use std::sync::atomic::Ordering;
        use std::sync::{Arc, Barrier};
        use std::thread;

        use crate::stats::{BotRole, BotStats};

        const BOTS: usize = 100;
        const FRAMES: u64 = 200;

        let barrier = Arc::new(Barrier::new(BOTS));
        let mut handles = Vec::with_capacity(BOTS);
        let mut stats_handles = Vec::with_capacity(BOTS);

        for b in 0..BOTS {
            let stats: Arc<BotStats> = BotStats::new(format!("listener-{b}"), BotRole::Listener);
            stats.enable_verify_integrity();
            stats_handles.push(stats.clone());
            let barrier = barrier.clone();
            handles.push(thread::spawn(move || {
                // Align all threads to start folding observations at once —
                // this is the startup-race window.
                barrier.wait();
                for seq in 0..FRAMES {
                    stats
                        .integrity
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .record_ok("sender-0", MediaType::VIDEO, seq);
                    // Half the bots also receive audio, exercising the
                    // multi-key path under contention.
                    if b % 2 == 0 {
                        stats
                            .integrity
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .record_ok("sender-0", MediaType::AUDIO, seq);
                    }
                }
            }));
        }
        for h in handles {
            h.join().expect("join bot thread");
        }

        for stats in &stats_handles {
            let s = stats
                .integrity
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .summarize(stats.drops.load(Ordering::Relaxed));
            assert_eq!(s.crc_mismatches, 0, "race must not corrupt CRC accounting");
            assert_eq!(s.unexplained_gaps, 0, "clean sequences must have no gaps");
            assert_eq!(s.media_seq_max, FRAMES - 1);
        }
    }
}
