# DEFECT 2 — mid-stream-joining listeners receive video but decode 0 frames — root cause + fix spec — 2026-05-20

Read-only investigation against `experimental-sfu` @ `8938518`.
Evidence source: `sfu-update/audits/200bot-monitor/spillover-decode/`
(`run.log`, `s.log`, `l11.log`, `l13.log`, `DECODE-VERIFY-FINDINGS.md`).

---

## TL;DR (decisive)

This is a **BOT-HARNESS defect, not an SFU shipping bug.** The SFU forwards
keyframes correctly and routes KEYFRAME_REQUESTs correctly. The decode-0 signature
is fully explained by two compounding flaws in the **bot sender**:

1. **(PRIMARY) The bot sender drops keyframes indiscriminately under channel
   backpressure.** The producer→writer channel is a 100-slot bounded mpsc shared by
   audio + video, drained via `try_send` with **no keyframe priority**
   (`bot/src/video_producer.rs:201-227`). In the measured run the sender dropped
   **263,527 of ~280,357 packets (94%)** to `tx_drops_channel_full`
   (`s.log`: `tx_packets_enqueued=16830`, `tx_drops_channel_full=263527`). A
   periodic keyframe (one per 150 frames) has the same ~6% survival odds as any
   P-frame, and a dropped keyframe poisons the entire GOP for every mid-stream
   joiner until the *next surviving* keyframe — which, at a 94% drop rate, may never
   land inside a given joiner's receive window.

2. **(SECONDARY) The bot sender cannot honor a KEYFRAME_REQUEST at all.** Listeners
   correctly emit KFRs (`l11.log`: `kfr=59` on the busiest listener; `kfr=2` on most),
   the SFU correctly routes them, but the **bot sender has no mechanism to force a
   keyframe**: its encoder always calls `vpx_codec_encode(..., flags=0, ...)`
   (`bot/src/video_encoder.rs:166`) — `VPX_EFLAG_FORCE_KF` is never set — and the
   sender's `WebTransportClient` is built **without** `.with_decode(true)`
   (only listeners get it, `bot/src/orchestrate.rs:680`), so the sender's inbound
   consumer drains KFR streams and discards them. There is also no channel from the
   inbound path to the `VideoProducer` thread. So the only keyframe source is the
   encoder's periodic auto-keyframe (every 150 frames), which flaw #1 then drops.

The decoder behavior is correct and expected: a VP9 P-frame fed without a prior
keyframe returns non-`VPX_CODEC_OK` and is routed to `on_error` →
`record_decode_error` (`videocall-codecs/src/decoder/native.rs:161-179`,
`bot/src/webtransport_client.rs:890-900`). Every received frame errors because the
decoder never receives an initializing keyframe. This is libvpx working as designed,
not a bug.

Single-pod replicas=1 T=0 joiners decoded 276k frames because they joined *with*
frame 0 (the encoder's first frame is always a keyframe) and the channel was not yet
backpressured, so the initializing keyframe was delivered before the GOP started.

---

## Code evidence, ranked

### Cause (a) — sender drops keyframes under backpressure (PRIMARY)

`bot/src/video_producer.rs:201-227` — the only send path. `try_send` on a bounded
channel; on `Full` it bumps `tx_drops_channel_full` and silently drops the frame:

```
match packet_sender.try_send(packet_data) {
    Ok(())                       => record_tx_packet_enqueued(),
    Err(TrySendError::Full(_))   => record_tx_drop_channel_full(),  // <- keyframe or P-frame, no distinction
    Err(TrySendError::Closed(_)) => return Ok(()),
}
```

- The channel is created at `bot/src/orchestrate.rs:552`:
  `mpsc::channel::<Vec<u8>>(100)` and is **shared** by audio + video producers
  (both receive `packet_tx.clone()`, `:559` and `:574`).
- `frame.key` is known at this point (`bot/src/video_producer.rs:174`,
  `frame_type: if frame.key { "key" } else { "delta" }`) but is **not consulted** for
  drop priority.
- Measured drop rate (`s.log`): `tx_packets_enqueued=16830`,
  `tx_drops_channel_full=263527` → 94% of all media dropped. A keyframe occurs
  once per 150 encoded frames (see encoder config below); at 94% loss the
  expected number of *consecutive* keyframe losses is high, and the busiest
  listener (`l11.log`: `recv_vid=1785`, ~59s of video spanning ~11 keyframe
  intervals) still decoded **0** — proving keyframes were not merely
  mistimed but were being dropped on the sender.

Encoder keyframe cadence (confirms keyframes *should* exist every 150 frames, and
that the config matches the known-good CLI so the config itself is not the bug):
`bot/src/video_encoder.rs:101-103`
```
cfg.kf_max_dist = 150;
cfg.kf_min_dist = 150;
cfg.kf_mode     = vpx_kf_mode::VPX_KF_AUTO;
```
identical to `videocall-cli/src/video_encoder.rs:100-102`.

### Cause (c) — KFR is emitted and routed, but the sender cannot act on it (SECONDARY)

The listener side is correct and fully wired:
- Gap detector arms a KFR after `KEYFRAME_REQUEST_GAP_ARM_MS=1000` and debounces at
  `KEYFRAME_REQUEST_MIN_INTERVAL_MS=500`
  (`bot/src/webtransport_client.rs:1009-1018`).
- KFR is built with the **target publisher** in the inner `MediaPacket.user_id`
  and pushed to the feedback writer (`bot/src/webtransport_client.rs:1212-1248`).
- This matches the production client wire format and the SFU's routing expectation.

The SFU routes it correctly:
- `classify_and_inspect` tags it `PacketKind::KeyframeRequest`
  (`actix-api/src/actors/packet_handler.rs:110-112`).
- `chat_server` applies the layer-aware drop filter then publishes to NATS so it
  reaches the named publisher (`actix-api/src/actors/chat_server.rs:1372-1410`).
  Because the bot sets **no** `LayerSelection` (no SubscriptionUpdate, no
  RoutingHeader), `should_drop_kfr_for_layer_selection` hits the
  "`selection.is_none()` → forward" rule
  (`actix-api/src/actors/packet_handler.rs:285-290`) — the KFR is **not** dropped.
- Per-session rate limiter (`KeyframeRequestLimiter`,
  `actix-api/src/actors/packet_handler.rs:340-367`) is generous and not the gate here.

The bot sender drops it on the floor:
- Sender is built without decode: `bot/src/orchestrate.rs:540-543` (`run_sender`)
  has no `.with_decode(true)`; only `run_listener` does (`:680`). The sender's
  `start_inbound_consumer` therefore parses nothing and just drains
  (`bot/src/webtransport_client.rs:469-492`, `decoders=None`).
- The encoder is never asked to force a keyframe: `encode()` always passes
  `flags=0` (`bot/src/video_encoder.rs:166-175`). There is no `force_keyframe`
  API and no channel from the inbound path into the `VideoProducer` thread
  (`bot/src/video_producer.rs:46-76` takes only a `packet_sender`).

So KFRs are a no-op against bot senders. `kfr=59` on a listener that still decoded 0
is the proof.

### Cause (b) — SFU does NOT proactively request/synthesize a keyframe for new joiners

Ruled IN as a *latent SFU gap*, but NOT the cause of this run's 0-decode:

- The SFU does not generate a keyframe request when a receiver joins/subscribes
  mid-stream. There is no join-time KFR injection in the JoinRoom /
  SubscriptionUpdate path (`actix-api/src/actors/chat_server.rs` JoinRoom handler
  begins `:1434`; no KFR emission). The SFU relies entirely on the **receiver** to
  notice the gap and emit a KFR — which the real browser client does
  (`peer_decode_manager.rs`), and which the bot listener also does. So the
  mid-stream-join recovery contract is "receiver-driven KFR → publisher re-keys."
- That contract is sound for **real** publishers (the browser client honors KFRs by
  forcing an encoder keyframe). It breaks here only because the **bot publisher**
  cannot honor a KFR (cause c) and also drops its periodic keyframes (cause a).

### RoutingHeader / SVC angle — ruled OUT

The bot sets no `RoutingHeader` (`bot/src/video_producer.rs:160-184`, comment at
`:161-164`), so the forwarder's layer-drop / reference-aware stages are skipped
entirely (`actix-api/src/sfu/forwarder.rs:522-625` only run when
`mp.routing_header.as_ref()` is `Some`). The SFU is on the legacy passthrough path
for bot media; it is **not** dropping the base/keyframe layer. The 15,916 received
video packets with `crc_mismatches=0` confirm the SFU delivered byte-faithful media.
The keyframe loss is upstream of the SFU, on the sender.

---

## Decision: is this a real SFU shipping bug?

**No — for the bot run, this is a test-harness artifact (causes a + c).** A real
browser publisher (a) prioritizes keyframes / does not run a 100-slot shared
try_send drop loop, and (b) honors KEYFRAME_REQUESTs by forcing an encoder keyframe.
So a real mid-meeting joiner WOULD recover within the receiver-KFR round-trip.

**One latent SFU hardening item (cause b)** is worth filing but is lower priority:
the SFU could proactively emit a KFR to the relevant publisher(s) when a receiver
joins or first subscribes, shrinking time-to-first-frame and removing the dependency
on the receiver's ~1s gap-arm timer. This is an optimization, not a correctness fix,
and only helps once publishers honor KFRs.

---

## Fix spec

### BOT-FIX-1 (PRIMARY) — keyframe-aware backpressure in the video producer
File: `bot/src/video_producer.rs:201-227`.

Make the producer never silently drop a keyframe under channel-full:
- On `frame.key == true`, do **not** use `try_send`. Use a bounded blocking send
  with a short deadline (e.g. `send_timeout` if the channel were tokio on this
  thread; since the producer runs on a `std::thread`, use
  `blocking_send`/a small spin with `try_send` retry budget, or a dedicated
  higher-priority path). The keyframe MUST win against P-frame backlog.
- Optionally drain/skip pending delta frames ahead of a keyframe so the keyframe
  is not stuck behind a P-frame backlog it invalidates anyway.
- Keep `try_send`-drop semantics for delta frames (P-frames are expendable).
- Acceptance metric: with the sender CPU-pegged, `keyframes_dropped` → 0 while
  `tx_drops_channel_full` (delta) may remain high.

Secondary mitigation (cheap, do alongside): give video and audio **separate**
channels, or raise the video channel bound, so audio backpressure and video
backpressure don't share one 100-slot queue (`bot/src/orchestrate.rs:552`).

### BOT-FIX-2 (SECONDARY) — bot sender honors KEYFRAME_REQUEST
Files: `bot/src/webtransport_client.rs`, `bot/src/video_producer.rs`,
`bot/src/video_encoder.rs`, `bot/src/orchestrate.rs`.

- Add a `force_keyframe` capability to the encoder: thread `VPX_EFLAG_FORCE_KF`
  into the next `vpx_codec_encode` flags (`bot/src/video_encoder.rs:166`).
- Add an inbound control channel from the sender's decode/inspect path to the
  `VideoProducer` thread (e.g. an `Arc<AtomicBool> force_kf` the producer checks
  each iteration and clears after setting the flag).
- Enable lightweight KFR inspection on the **sender's** inbound consumer: either
  build `run_sender`'s client with a KFR-only inspect mode, or add a parse in the
  sender path that recognizes `MediaType::KEYFRAME_REQUEST` targeting this sender's
  `user_id` and sets the `force_kf` flag. Reuse the existing wire-format parse from
  `bot/src/webtransport_client.rs:1212-1248` (inverse direction).
- Acceptance: a KFR delivered to a bot sender produces a keyframe within one frame
  interval (~33ms) of receipt.

### SFU-FIX-1 (OPTIONAL, latent hardening — cause b)
File: `actix-api/src/actors/chat_server.rs` JoinRoom / SubscriptionUpdate paths.

- On a receiver join (or first SubscriptionUpdate that adds a sender), emit a
  KEYFRAME_REQUEST to each newly-subscribed publisher so the new receiver gets a
  fast initializing keyframe instead of waiting ~1s for its own gap timer.
- Must respect the existing `KeyframeRequestLimiter` and the layer-selection drop
  filter so a join storm cannot trigger a KFR storm (Change Impact: 200-participant
  webinar — join waves are O(n); coalesce per (room, publisher) within a window).
- This only helps once publishers honor KFRs; for real browser publishers it
  reduces time-to-first-frame for mid-meeting joiners.

---

## Acceptance criteria (DEFECT 2 closed)

1. In a multi-bot run with senders streaming before listeners join, a
   mid-stream-joining listener reaches `video_frames_decoded > 0` within a bounded
   time of join (target: first keyframe ≤ 2s after join under CPU-pegged senders).
2. `decode_errors` for that listener stops monotonically tracking `media_received_video`
   (i.e. errors plateau once a keyframe initializes the decoder).
3. With BOT-FIX-1 alone (no SFU change), a joiner recovers on the next periodic
   keyframe (≤5s) reliably even at high sender drop rates.
4. With BOT-FIX-2, an emitted KFR (`keyframe_requests_sent > 0`) measurably reduces
   time-to-first-decoded-frame versus periodic-only.

---

## Bead breakdown

| Bead | Scope | Priority | Depends on | Notes |
|---|---|---|---|---|
| BOT-FIX-1 | bot/video_producer + orchestrate channel split | **P1** | none | Closes DEFECT 2 on its own; keyframe-priority send |
| BOT-FIX-2 | bot/video_encoder force-KF + sender inbound KFR inspect + producer control channel | P2 | BOT-FIX-1 | Makes bot senders honor KFRs; required for realistic harness fidelity |
| SFU-FIX-1 | actix-api join-time KFR injection (coalesced) | P3 (optional) | BOT-FIX-2 (to be observable) | Latent hardening; reduces TTFF for real publishers; must not create join-wave KFR storms |

Bot fixes are independent of the multi-pod redirect/teardown blocker documented in
`MULTIPOD-ROOTCAUSE.md` (vc-s9e) — they are orthogonal harness-fidelity defects on
the data plane and can land in parallel.
</content>
</invoke>
