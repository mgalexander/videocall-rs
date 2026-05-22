# DEFECT 3 — Cross-pod data-plane gap: spill-admitted listeners decode zero A/V — 2026-05-21

Read-only investigation. Branch `experimental-sfu`, tip near `494656b` (vc-el0
namespace fix landed; vc-72a + vc-85p + vc-8wd present). Evidence:
`sfu-update/audits/200bot-monitor/spillover-decode/` (`run.log`, `s.log`,
`l1..l15.log` from the 06:27–06:35 run).

---

## TL;DR (decisive)

After the vc-el0 redirect-namespace fix, senders + most listeners now correctly
land on the owner pod and decode. The residual defect is on the SPILL pods: a
listener spill-admitted on a non-owner pod **receives the cross-pod MEDIA
wrappers but decodes ZERO audio and ZERO video**, while non-A/V MEDIA
(HEARTBEAT/RTT, the `_ => true` arm) passes through fine. The media is being
**dropped at `Forwarder::decide`'s AllowSet filter** on the spill pod — NOT lost
in NATS federation, NOT undelivered to the receiver's outbound. The federated
bytes arrive at the spill pod's dispatcher and are fanned to the receiver; the
per-receiver `decide` then drops the AUDIO/VIDEO MediaPackets.

The decisive, code-proven root cause for **VIDEO** is the receive-all fallback's
cap test in `forwarder.rs:487`, which is measured against `allow.video.len()` —
the LOCAL membership-bound video set. On a spill pod the local members are the
*other spill listeners* (dozens of them), so `allow.video.len() >>
MAX_VISIBLE_VIDEO (6)` and the `allow.video.len() < MAX_VISIBLE_VIDEO` guard is
FALSE → every cross-pod publisher's video is dropped as `unsubscribed`.

For **AUDIO** the forwarder source as written *should* admit (the audio fallback
at `forwarder.rs:444` is an unconditional `|| recv_all_audio`, and bots resolve
to `receive_mode == (true, true)`), yet the live data shows `rx_audio = 0` on the
fully-populated spill pod and a non-zero `rx_audio` only on the lightly-loaded
spill pod. That gap is what the SFU_TRACE_ROOM-gated run below must capture
live before committing to the fix — see §2 and §4. The most likely mechanism is
the same population-dependent AllowSet behavior interacting with the audio tier
(audio mirrors video membership), surfacing only once the spill pod's local
member count is large; the trace will pin `reason=unsubscribed` vs `forward` for
a known sender→spill-listener pair.

---

## Part 1 — The evidence (ground truth from the run)

Aggregated per landing pod across `l1..l15.log` (parsed from the JSON
summaries). Owner pod = `10.42.1.88` (where all 10 senders landed — `s.log`
`joined_pod = [::ffff:10.42.1.88]:443`, `redirects_followed: 0..4`).

| joined_pod | bots | rx_audio | rx_video | rx_other (MEDIA) | audio_decoded | video_decoded |
|---|---|---|---|---|---|---|
| `10.42.1.88` (OWNER) | 1,385 | >0 | >0 | >0 | 32,296 | **0** |
| `10.42.2.83` (spill) | 87 | **0** | **0** | 28,939 | **0** | **0** |
| `10.42.0.79` (spill) | 28 | small | **0** | small | 264 | **0** |

Per-pod listener-log breakdown (the `totals` block of one summary each):

- Owner (`l2.log`): `media_received_audio=6788, media_received_video=2218,
  media_received_other=5875, redirects_followed=100, audio_frames_decoded=6788`.
  Audio AND video MEDIA both flow; every audio packet decodes.
- Spill (`l14.log`, the `10.42.2.83` cohort): `media_received_audio=0,
  media_received_video=0, media_received_other=28939, control_packets=53,
  packets_received=28992, bytes_received=1,777,803, redirects_followed=28 (of
  100), decode_errors=0`.

Decisive reads from the spill summary:

1. **`packets_received=28992` / `bytes_received=1.7 MB`** — substantial traffic
   is delivered to the spill listener's outbound. So the *delivery from
   dispatcher → receiver's outbound* is NOT the gap (refutes the "never
   delivered" half of investigate-item #4).
2. **`media_received_other=28939` but `media_received_audio=0` /
   `media_received_video=0`** — the spill listener receives cross-pod MEDIA
   wrappers whose inner `media_type` is neither AUDIO nor VIDEO/SCREEN (the
   `_ => true` pass-through arm in `decide`), but ZERO AUDIO/VIDEO MediaPackets.
3. **`decode_errors=0`** — nothing was received-but-unparseable; the A/V simply
   never arrived.

The bot classifies inbound at `bot/src/webtransport_client.rs:788-805`: non-MEDIA
wrappers → `control_packets_received`; MEDIA wrappers split by inner `media_type`
into video / audio / **other**. So "other" is exactly the set of MEDIA packets
that clear `decide`'s `_ => true` arm but are not A/V. The senders publish
`media_type: AUDIO` / `media_type: VIDEO` with **no RoutingHeader**
(`bot/src/audio_producer.rs:180-192`, `bot/src/video_producer.rs:162-174`), so
their A/V packets DO hit the AllowSet-filtered branch (`needs_filter == true`,
`forwarder.rs:377-381`) — and on the spill pod they are dropped.

This is the same gap the ramp predicted ("media federates TO the spill pod via
NATS but may not be forwarded to listeners admitted THERE"; suspiciously low
~60m SFU CPU) in
`spillover-ramp/SPILLOVER-RAMP-FINDINGS.md`.

---

## Part 2 — Decisive root cause (file:line)

### 2a. Federation + delivery to the spill pod are CORRECT (ruled out)

- The per-room dispatcher on EVERY pod subscribes to the room-scoped wildcard
  `room.{room}.*` with a **plain** `nc.subscribe` (not `queue_subscribe`), so
  NATS delivers a copy to every pod's subscriber:
  `actix-api/src/actors/chat_server.rs:2388` (subscribe),
  `:2372-2386` (`spawn_room_dispatcher` signature),
  subject built at `:1955` via `build_subject_and_queue`
  (`actix-api/src/models/mod.rs:44-49`, `_queue` discarded at the call site).
- Spill-admit falls through to the SAME local-admit machinery as any normal
  join: `room_states` + `insert_member` (`chat_server.rs:1989-2054`), dispatcher
  spawn (`:2078-2098`), and the synchronous `receivers.write().insert`
  (`:2099-2121`). So the spill listener is in the dispatcher's `receivers` map
  and is fanned every inbound message (`:2722-2751`, `egress_decide_from_parsed`
  at `:2844-2926`).
- All SFU pods in a region share one NATS cluster (`nats-us-east:4222`,
  `helm/global/us-east/{webtransport,websocket}/values.yaml`). No queue-group
  load-balancing across pods.

The 28,939 "other" MEDIA packets on the spill listener PROVE the bytes traverse
NATS → dispatcher → `decide` → receiver outbound. The gap is purely the per-
receiver `decide` verdict for AUDIO/VIDEO.

### 2b. VIDEO — code-proven drop at the receive-all cap (the decisive bug)

`Forwarder::decide`, VIDEO/SCREEN branch:

```
actix-api/src/sfu/forwarder.rs:448-496
MediaType::VIDEO | MediaType::SCREEN => {
    if allow.video.contains_key(&sender_sid) { true }       // membership-bound: cross-pod sender ABSENT
    else if recv_all_video {                                // bot path: true
        ...
        if allow.video.len() < MAX_VISIBLE_VIDEO as usize { // <-- THE BUG
            non_member_video_admit = true; true
        } else { false }                                    // <-- cross-pod video DROPPED here
    } else { false }
}
```

`allow` is the membership-bound AllowSet from `SubscriptionStore::resolve_inner`
(`subscription.rs:324-443`). For a no-update bot the legacy-default branch
(`subscription.rs:330-342`) populates `allow.video` with **every other local
member** (minus self). On the owner pod the "other members" are the 10 senders →
`allow.video.len() == 10`; combined with the cap that already over-admits, video
behaves differently there. On a SPILL pod the local members are the dozens of
*other spill listeners* (87 on `10.42.2.83`), so `allow.video.len()` ≈ 86 ≫
`MAX_VISIBLE_VIDEO (6)`. The guard `allow.video.len() < 6` is therefore FALSE,
the `else { false }` branch runs, and **every cross-pod publisher's video is
dropped as `unsubscribed`** (`forwarder.rs:499-511`).

`MAX_VISIBLE_VIDEO = 6` is defined at `subscription.rs:40`.

This is the decisive, deterministic cross-pod video drop: the receive-all
fallback's capacity test is sized against the LOCAL-member video set, which on a
spill pod is dominated by fellow *listeners* (who publish nothing), structurally
guaranteeing the cap is exceeded and cross-pod *publishers* are excluded.

Note: video also decoded 0 on the OWNER pod (1,385 listeners, 0 video frames).
That is a separate, compounding video defect (see the sibling
`DEFECT2-VIDEO-KEYFRAME.md`) — bots run `enable_video=false` on the producer but
the senders DO emit VIDEO MediaPackets; with no RoutingHeader and no bandwidth
estimate the layer path is legacy pass-through, so the owner-pod 0-video is its
own issue. DEFECT3 is specifically the **spill-pod** A/V gap. Keep them
separate.

### 2c. AUDIO — observed drop; confirm-live before fixing

Per the forwarder source, the AUDIO branch is unconditional fallback:

```
actix-api/src/sfu/forwarder.rs:444
MediaType::AUDIO => allow.audio.contains(&sender_sid) || recv_all_audio,
```

and `receive_mode` returns `(true, true)` for a receiver that never sent a
`SubscriptionUpdate` (`subscription.rs:472-478`). Bots never emit a
`SubscriptionUpdate` (no reference anywhere in `bot/src/*.rs`), so they take the
`None => (true, true)` arm. As written, this admits cross-pod audio
unconditionally. **Yet the live data shows `rx_audio = 0` on the populated spill
pod** (`l14.log`) and only a trickle on the lightly-loaded one (`l9.log`/`l...`,
264 frames over 28 bots).

Two candidate reconciliations, to be discriminated by the live trace in §4:

- **(C1) Audio is in fact dropped at `forwarder.rs:444`** — implying
  `recv_all_audio` resolved `false` for these spill receivers (e.g. an
  unexpected `per_receiver` entry, a population/ordering interaction in
  `receive_mode`, or a build/topology assumption that does not hold on the spill
  pod). If so, `sfu_dropped_total{reason=unsubscribed}` rises on the spill pod
  for the senders' AUDIO packets — same signal as the video drop.
- **(C2) Audio clears `decide` but the spill listeners are starved upstream**
  for A/V specifically — e.g. a vc-9eh slow-consumer silence window that drops
  the high-rate A/V while the low-rate "other" survives (less likely given
  `bytes_received` is high and steady, but must be excluded).

The fix in §3 addresses the membership-bound cap that is *proven* for video and
*audio-tier-mirroring*; the trace confirms whether audio needs the same
treatment or has an independent cause. **Do not skip the trace** — it is the
arbiter between C1 (forwarder fix) and C2 (dispatcher/subscription fix).

---

## Part 3 — Fix spec

Goal: a spill-admitted receiver on a non-owner pod must have cross-pod publishers
(present in NATS, absent from this pod's `RoomState.members`) admitted to its
AllowSet exactly as if they were local members, subject to the per-receiver
visible-video cap measured against ACTUAL distinct admitted *publishers* — not
against the count of local *members* (which on a spill pod are mostly fellow
listeners).

### Option A (preferred) — register cross-pod publishers as forwardable senders

Maintain a per-room **remote-publisher set** on each pod, populated from the
dispatcher's federated-media ingress: when the dispatcher parses an inbound MEDIA
packet whose `wrapper.session_id` is NOT a local `RoomState` member, record that
sid (with a short TTL / liveness stamp) as a known remote publisher for the room.
Then make the AllowSet resolver and the forwarder cap treat remote publishers as
admissible senders:

- Resolver (`subscription.rs:resolve_inner`): when building the legacy-default
  and receive-all tiers, draw candidates from `local_members ∪ remote_publishers`
  (still minus self), so cross-pod publishers land in `allow.audio` /
  `allow.video` directly. This makes the membership-bound path correct rather
  than relying on the `decide` fallback.
- Cap correctness: size the visible-video cap against **distinct admitted
  publishers** (members + remote publishers that actually send video), so a
  spill pod full of non-publishing listeners does not exhaust the cap with
  zero-video members. Keep `MAX_VISIBLE_VIDEO` as the visible ceiling.

### Option B (smaller, forwarder-local) — fix the cap denominator in `decide`

If a full resolver change is too large for one bead, the minimal correctness fix
is to stop measuring the receive-all video cap against `allow.video.len()` (local
members, mostly non-publishers on a spill pod). Instead measure it against a
per-receiver count of distinct *non-member publishers already admitted this
generation* (bounded, reset on speaker/membership generation change), or against
the count of senders the receiver is actually being shown. This removes the
structural "spill pod full of listeners → cap always exceeded → all cross-pod
video dropped" failure at `forwarder.rs:487` while preserving the
`MAX_VISIBLE_VIDEO` ceiling. Audio needs no cap; if the trace shows C1, ensure
`recv_all_audio` is honored on the spill pod (it already is in source — confirm
the deployed binary matches).

### O(n) fan-out risk at 200+ (call out explicitly)

- The remote-publisher set must be **bounded** (it is naturally bounded by the
  number of distinct senders ≈ 10 in a webinar, not by listener count) and
  TTL-reaped, or a churny room leaks entries. Reap on the same `prune_session`
  path the forwarder already uses (`forwarder.rs:658-667`).
- Do NOT widen `allow.video`/`allow.audio` to include every remote sid
  unconditionally — keep the `MAX_VISIBLE_VIDEO` ceiling so a 200-participant
  room with many publishers cannot blow per-receiver fan-out. The cap is the
  knob; the bug is only in WHAT it counts.
- The resolver runs once per (receiver, generation) and is cached
  (`resolve_cached`, `subscription.rs:265-320`); adding remote publishers to the
  candidate set does not change the per-packet hot path cost. The dispatcher's
  per-message "is this a new remote publisher?" check is O(1) (a `DashMap`
  contains/insert keyed by sid), gated to MEDIA packets, and only writes on the
  first packet from a new sid.
- PRESERVE the vc-72a `receive_mode` fallback as a belt-and-suspenders admit for
  the brief window before a remote publisher is registered.

---

## Part 4 — vc-8wd instrumentation: the exact counter/trace that confirms it

The build already has the instrumentation needed; PRESERVE it. Confirming
counters and trace points (cite by file:line):

- **`sfu_dropped_total{reason=unsubscribed}`** — incremented at
  `forwarder.rs:500-502` (the `if !allowed` block). This is THE confirming
  counter: on the spill pod it will rise for the senders' AUDIO and/or VIDEO
  packets in lock-step with the spill listeners' `media_received_audio/video`
  staying 0. The drop-reason constant is `sfu_drop_reason::UNSUBSCRIBED`
  (`forwarder.rs:32, 501`).
- **`sfu_forward_total`** (`forwarder.rs:635`) and
  **`sfu_forwarded_total{packet_type}`** (`forwarder.rs:631-633`) — on the spill
  pod these will show forwards for non-MEDIA / `media`-typed-but-`_=>true` MEDIA
  (the 28,939 "other") while AUDIO/VIDEO are absent from the forwarded stream.
- **`sfu_allowset_size`** (`subscription.rs:291`) — the resolved `allow.video`
  size histogram. On a spill pod this will pile up at the LOCAL-member count
  (e.g. ~86), demonstrating the cap denominator problem of §2b: a large AllowSet
  that nonetheless excludes the cross-pod publishers.
- **Per-decision trace** at the forward/drop site:
  `trace_forward_decision` (`forwarder.rs:872-891`), called on every drop/forward
  in `decide` (`forwarder.rs:361-366, 503-508, 568-573, 613-618, 636`). Emits
  `target: "sfu_trace"` with `room`, `sender`, `decision`, `reason`, gated by
  `trace::tracing_enabled()` + `trace::traced_room()` (resolved once under the
  room lock at `forwarder.rs:325-352`) and 1-in-N sampled
  (`trace::should_sample_forward()`).
- **AllowSet-resolved trace** at `subscription.rs:297-309` (gated on
  `traced_session`), emitting `audio_len` / `video_len` — shows the resolved
  sizes for the spill receiver.
- **Join decision trace** at `chat_server.rs:1699-1723` (`decision=admit_local,
  reason=spilled_over`) confirms the receiver was spill-admitted on the non-owner
  pod.

### Precise SFU_TRACE_ROOM-gated run shape (validate before AND after the fix)

1. Pick the trace room and a known spill listener session. The trace gate is
   armed via the `SFU_TRACE_ROOM` env (and optional `SFU_TRACE_SESSION`) consumed
   by `crate::sfu::trace` (`tracing_enabled` / `traced_room` / `traced_session`).
   Set on the SFU pods for the run: `SFU_TRACE_ROOM=<room>` (e.g. the dvrun room),
   leave tracing OFF on all other rooms (default).
2. Run the multi-pod decode-verify exactly as
   `spillover-decode/decode-verify.sh` (replicas≥3, prod limits, 10 senders +
   ≥1,500 listeners so spill engages, fixed 360s, `--verify-integrity`).
3. On the SPILL pod(s) (non-owner, e.g. `10.42.2.83`), scrape:
   - `sfu_dropped_total{reason="unsubscribed"}` delta over the run — must be
     LARGE pre-fix (≈ the senders' A/V packet rate × spill listeners), ≈0
     post-fix for sender→spill-listener A/V.
   - `sfu_forwarded_total{packet_type="media"}` delta — near-0 pre-fix, rising
     to the A/V rate post-fix.
   - `sfu_allowset_size` histogram — piled at the local-member count pre-fix.
   - `sfu_trace` log lines filtered to `room=<room>` and the senders' sids:
     `decision=drop reason=unsubscribed` pre-fix → `decision=forward reason=ok`
     post-fix for the same (spill-listener, sender) pairs.
4. Cross-check against the bot summaries: pre-fix spill listeners
   `media_received_audio=0 / media_received_video=0`; post-fix > 0 with
   `audio_frames_decoded` ≈ `media_received_audio` and `crc_mismatches=0`.

This trace is the arbiter between §2c C1 (audio dropped at `decide` →
`unsubscribed` rises) and C2 (audio starved upstream → `unsubscribed` does NOT
rise for audio but forwarded count is also low). Capture it BEFORE writing the
fix so the fix targets the confirmed mechanism.

---

## Part 5 — Acceptance criteria

1. replicas ≥ 3, prod limits, ≥1,500 listeners so spill engages on ≥2 non-owner
   pods.
2. A spill-admitted listener on an adjacent (non-owner) pod decodes the senders'
   AUDIO with `crc_mismatches = 0` and `audio_frames_decoded ≈ media_received_audio`.
3. The same listener decodes the senders' VIDEO with `crc_mismatches = 0`
   (subject to DEFECT2 being resolved in parallel — DEFECT3 acceptance is that
   cross-pod video reaches `decide` as `forward`, i.e. `media_received_video > 0`
   on the spill pod; end-to-end video decode also requires DEFECT2).
4. On the spill pod, `sfu_dropped_total{reason="unsubscribed"}` ≈ 0 for the
   senders' packets over the run (down from the pre-fix large value).
5. `sfu_allowset_size` and the per-receiver fan-out remain bounded by
   `MAX_VISIBLE_VIDEO` — no O(n) blow-up at 200+ (verify CPU/mem do not regress
   vs the ramp's ~60m/pod baseline; a correct fix should INCREASE forwarding CPU
   modestly because media now actually flows, but per-receiver fan-out stays
   capped).
6. vc-8wd instrumentation preserved and still off-by-default.

---

## Part 6 — Bead breakdown (SFU)

All beads are SFU/backend (`backend-rust-streaming`); review with `code-reviewer`
+ `performance-reviewer`; no UI/e2e surface (server-internal forwarding).

| Bead | Title | Priority | Depends on |
|---|---|---|---|
| **vc-d3a** | SFU_TRACE_ROOM-gated decode-verify on the spill pod to capture the live forward/drop verdict for a sender→spill-listener pair; record `sfu_dropped_total{reason=unsubscribed}`, `sfu_forwarded_total{media}`, `sfu_allowset_size`, and `sfu_trace` lines. Arbitrate §2c C1 vs C2. | P0 (must precede the fix) | none |
| **vc-d3b** | Fix the receive-all video cap denominator: stop sizing the cap against `allow.video.len()` (local members, mostly non-publishing listeners on a spill pod) at `forwarder.rs:487`; size against distinct admitted *publishers* / shown senders, preserving `MAX_VISIBLE_VIDEO`. Add a regression test with a spill-shaped membership (1 cross-pod publisher + N local non-publishing listeners, N≫6) asserting the publisher's video forwards. | P0 | vc-d3a |
| **vc-d3c** | Register cross-pod publishers as forwardable senders (Option A): per-room remote-publisher set populated from the dispatcher MEDIA ingress for non-member sids, consumed by `resolve_inner` so cross-pod publishers enter `allow.audio`/`allow.video` directly; TTL-reaped via the existing `prune_session` path. Bound the set; keep `MAX_VISIBLE_VIDEO`. | P0 | vc-d3a (and folds in vc-d3b's cap fix) |
| **vc-d3d** | If vc-d3a shows C1 (audio dropped at `decide`): ensure `recv_all_audio` is honored on the spill pod and the deployed binary matches source (`forwarder.rs:444`, `subscription.rs:472-478`); add a spill-shaped audio regression test. If C2: file a separate dispatcher/slow-consumer bead instead. | P0 | vc-d3a |
| **vc-d3e** | Post-fix decode-verify acceptance run (Part 5) on the same harness; assert spill-admitted listeners decode A/V with `crc_mismatches=0` and `unsubscribed`≈0 on the spill pod; confirm fan-out stays capped at 200+. PRESERVE vc-8wd. | P1 | vc-d3b, vc-d3c, vc-d3d |

Sequencing: vc-d3a first (oracle), then vc-d3c (which subsumes vc-d3b's cap fix
if Option A is taken; otherwise ship vc-d3b standalone), vc-d3d gated on the
trace verdict, vc-d3e to validate. Do NOT land any fix without the vc-d3a trace
proving the mechanism — the forwarder source as written should already admit
audio, so the fix must target the *confirmed* deployed behavior, not the
source's apparent behavior.
