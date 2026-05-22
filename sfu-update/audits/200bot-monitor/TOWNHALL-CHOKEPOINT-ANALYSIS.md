# Town-Hall Chokepoint Analysis — Does Mixdown Solve the Real >500 Wall, or Move It?

Read-only architectural analysis. Branch `experimental-sfu`. SFU single OWNER pod (room pinned
to one pod; cross-pod spillover deferred/broken). No code changes, no cluster commands.

Inputs reviewed: ADR-0009b (`sfu-update/adr/0009-hybrid-presenter-scaling-townhall.md`, Part B,
§4 Revision, both security reviews, performance review), root cause
(`sfu-update/audits/200bot-monitor/PRESENTER-SCALING-ROOTCAUSE.md`).

---

## TL;DR — Direct answers

1. **Most-contended chokepoint in the town-hall plan: VIDEO egress bandwidth on the single owner
   pod's NIC.** Once audio is mixed to `1×R`, the dominant remaining per-receiver byte stream is
   per-presenter E2EE video at up to `MAX_VISIBLE_VIDEO=6 × ~400 kbps`. That term is **untouched
   by the mixer** and binds first as R grows.

2. **It binds at R ≈ 520 receivers** (1 Gbps usable NIC) **/ R ≈ 1040** (2 Gbps), at the
   *visible-video saturation* shape (6 active video tiles per receiver). Town-hall's enable
   threshold is **500** — i.e. the video-egress wall lands **essentially exactly where town-hall
   turns on**, and at a *lower* R than where audio-mixed CPU or the mixer itself would ever bind.

3. **Probability the mixer/mix-path is the binding bottleneck: ~10–15%.** Probability the binding
   constraint is **video egress bandwidth: ~55%**; **single-owner-pod ceiling (aggregate, CPU+NIC
   on one pod, no spillover): ~25%**; everything else (mixer CPU, crypto, scorer, ingest)
   collectively **~5–10%**. **The town-hall plan optimizes a term (audio egress) that the perf
   review already showed is NOT the first wall once Part A lands.** Mixdown is real headroom on the
   audio axis, but it **moves the bottleneck onto video egress / the single-pod ceiling**, which the
   design does not address.

4. **Mixdown moves the bottleneck. To actually scale 500→thousands the design MUST also address:
   (i) video at scale** — an SFU-side SVC layer-drop / visible-video budget that bounds video
   egress as R grows (not just per-receiver MAX_VISIBLE=6), and/or video simulcast tier capping; and
   **(ii) the single-owner-pod ceiling** — cross-pod spillover for the egress fan-out (the deferred
   `CROSS-POD-DATA-PLANE-FUTURE` work). Without one of these, town-hall lets a room *enter*
   town-hall mode and immediately hit the video NIC wall on the same pod.

5. **Bead-plan impact: YES, re-prioritize.** Track 2's long pole today is **B6 (crypto)**. But the
   crypto perfects a path (audio mixdown) that does not unblock scaling past ~520 receivers because
   video egress binds first. **A video-egress-bounding bead (call it B13: SFU video layer-drop /
   per-pod egress budget) should be P0-of-scaling and gate the "town-hall scales past 500" claim
   ahead of B7–B12.** Otherwise Track 2 ships a fully-audited crypto path that demonstrably does not
   move the scaling ceiling.

---

## 1. Cost model — each candidate constraint as f(R, P, K)

### Constants grounded in source
- Audio: Opus **20 ms frames → 50 packets/s/presenter** (`microphone_encoder.rs:568` `encoder_frame_size: Some(20)`; sequence increments per frame `:393`). Audio bitrate ~32 kbps/stream (ADR-0009 §Context; matches the deferral doc).
- Video: capped per receiver at **`MAX_VISIBLE_VIDEO = 6`** (`subscription.rs:41`, enforced `forwarder.rs:499`). Audio is **uncapped** (`forwarder.rs:456`). Per-tile video ~**400 kbps** (typical simulcast/SVC mid-tier; the ADR/capacity-model use 400 kbps).
- Mix: **K = `MAX_SPEAKERS` = 4** (`speaker.rs:159`, `SFU_TOWNHALL_MIX_K` default). Non-speakers contribute silence and are excluded → mix is **independent of P** (ADR §4.2.1).
- Scorer tick: **200 ms** (`speaker.rs:162` `TICK_INTERVAL`).
- Pod: 4 CPU / 4 Gi; effective fan-out cores **≈ 0.6–0.75·C ≈ 2.5–3** (perf review §C). NIC: assume **1–2 Gbps usable egress** (perf review §C).

### (a) Mixer fan-IN + DSP CPU — f(K), independent of P and R

The mixer (ADR §4.2.2) is a **single per-room task on the fan-out pool**: per 20 ms frame it
GCM-decrypts the **top-K** speakers (NOT all P — §4.2.1 "Non-speaking presenters contribute silence
and are excluded from the mix … makes the mix independent of P"; the scorer's top-K set drives it,
`speaker.rs:434/509`), Opus-decodes K streams, sums/normalizes PCM, Opus-encodes **1** stream,
GCM-encrypts 1.

```
mixer_cpu ≈ (1/0.02 s) × [ K·(gcm_dec + opus_dec) + mix + opus_enc + gcm_enc ]
          = 50/s × [ 4·(decode) + 1·encode + crypto ]
```
At K=4: ~50 Opus decodes/s + 50 encodes/s + ~250 GCM ops/s for ONE room. Opus decode ≈ tens of µs;
encode ≈ low hundreds of µs/frame. **Total ≈ a few % of one core, constant in R and P.**

**Is it a single-thread choke (like the old dispatcher)?** It is a *single task*, but its load is
**O(K)=O(1)**, not O(P×R). It does **not** grow with the room. It is a single point of *failure*
(mitigated by forward-all fallback, §4.2) but **not** a single point of *contention*. The decode is
**top-K-only**, not decode-all-to-rank — ranking uses the cleartext `RoutingHeader.audio_level`
EWMA (`speaker.rs:94 observe`, ADR-0002), never decoded audio, so the mixer never decodes P streams.

- **Verdict: tiny, flat, ~1 core-% per room. Does not bind at any R or P relevant here.**

### (b) VIDEO egress bandwidth — f(R), the headline term after audio is gone

Video stays per-presenter, E2EE, capped per receiver at `MAX_VISIBLE_VIDEO=6`. So:
```
video_egress_bps ≈ min(P, 6) × 400 kbps × R        (per-receiver cap = 6 tiles)
```
At town-hall scale P ≫ 6, so the per-receiver term saturates at **6 × 400 kbps = 2.4 Mbps/receiver**.

| R | video egress (6 tiles × 400 kbps × R) | vs 1 Gbps | vs 2 Gbps |
|---|---|---|---|
| 500 | **1.20 Gbps** | **over** | under |
| 1000 | **2.40 Gbps** | over | **over** |
| 2000 | **4.80 Gbps** | over | over |

**NIC-bound R for video alone:** 1 Gbps ⇒ **R ≈ 520**; 2 Gbps ⇒ **R ≈ 1040**.

Compare the perf review's E2EE-base ceiling (§C): NIC-bound at **R ≈ 1560** at P=20 — but that
number is **audio-dominated** (20 × 32 kbps = 640 kbps/receiver dominated the per-receiver budget).
**Once audio is mixed away (1 × 48 kbps ≈ negligible), the per-receiver budget is dominated by the
6-tile video term (2.4 Mbps), which is ~3.75× larger than the 640 kbps audio it replaced.** So
mixing audio does **not** raise the NIC ceiling to ~21000 (the CPU crossover) — it lowers the
*per-receiver* bytes from (640 kbps audio + 2.4 Mbps video) to (~48 kbps + 2.4 Mbps video), i.e.
from ~3.04 Mbps to ~2.45 Mbps. **The NIC ceiling moves from R≈1560 (mixed shape would be ~ 3.04→2.45
Mbps ⇒ R≈408→520 at 1 Gbps for the heavy-video shape).**

Crucial nuance — **the audio-dominated R≈1560 figure assumed P=20 presenters all sending audio but
video capped at 6.** At P=20: per-receiver = 20×32 (audio) + 6×400 (video) = 640 + 2400 = 3.04 Mbps
⇒ 1 Gbps ⇒ **R ≈ 329**, not 1560. (The 1560 figure in the perf review used audio-only 640 kbps and
appears to omit the 6-tile video term — see §4 note.) **Either way, video is the larger per-receiver
term at town-hall scale, and removing audio leaves a 2.4 Mbps/receiver video floor that caps R at
~520 (1 Gbps) / ~1040 (2 Gbps) regardless of how perfectly audio is mixed.**

- **Verdict: this is the binding term. It binds at R ≈ 520 (1 Gbps) — right at the 500 threshold.**

### (c) Single-owner-pod INGEST — f(P)

All P presenters ingest+parse at the one owner pod (Track-1 ingest sharding K is intra-pod). Per §3.3
+ perf review §D, K parallel NATS consumers each drain P/K presenters; parse-once per message. Ingest
rate = P × (50 audio + ~30 video) pps. At P=50: ~4000 pkt/s. Parse is cheap (`parse_and_inspect`
once, vc-q0v). With K-way sharding and batched scorer feed (perf F.4), ingest is **spread across
min(K,C) cores** and is not the binder until P is very large (thousands of presenters). Town-hall
has P in the tens.

- **Verdict: does not bind at P=20/50. Would bind only at P in the high hundreds+.**

### (d) Per-frame CRYPTO on the owner pod — f(K)

GCM-decrypt of **top-K** audio (not P — see (a)) + GCM-encrypt of 1 mixed stream, per 20 ms frame.
= same K=4 path as (a). ~250 GCM ops/s for one room. AES-GCM is AES-NI accelerated (low µs).
**Negligible**, and **independent of P and R**. (Note: this is *less* crypto than E2EE-base, where
the SFU did zero crypto but the bytes were larger; here the SFU does a little crypto on a tiny
constant set.)

- **Verdict: negligible. Does not bind.**

### (e) Active-speaker scorer — f(P)

Two costs: (1) per-inbound-audio `scorer.write().await` (`chat_server.rs:3218`) — flagged by perf
review B.3 as a serialization point that scales with P; mitigated to **K writes/tick** by the
required batched-feed fix (perf F.4). (2) the 200 ms `tick_once` (`speaker.rs:417`) which sorts/caps
O(P) candidates — at P=50, 5 ticks/s × O(50) work = trivial.

With the batched-feed fix, scorer cost is **O(P) every 200 ms off the hot path** — a few µs. Without
the fix, K-way ingest reintroduces write contention scaling with P, but town-hall P (tens) keeps it
tolerable.

- **Verdict: not a binder at P=20/50 *provided* the perf F.4 batched-scorer fix lands. Watch it as P→hundreds.**

### (f) Single-owner-pod CEILING (aggregate) — the structural wall

The room is pinned to ONE owner pod (cross-pod spillover deferred/broken per
`CROSS-POD-DATA-PLANE-FUTURE.md` and project memory). So **every** term above —
ingest, mix, crypto, scorer, AND both audio+video egress — must fit in ONE pod's CPU and ONE pod's
NIC. The aggregate egress bandwidth (b) is the first sub-term of this ceiling to bind, but the
ceiling itself is the *reason* (b) cannot be escaped by adding pods: you cannot split a town-hall
room's receivers across pods today.

```
single_pod_ceiling = min( pod_NIC_egress, 0.7·C cores of CPU, pod_mem )
   binding sub-term at town-hall scale = pod_NIC_egress (driven by video, term b)
```

- **Verdict: this is the *structural* wall that makes (b) un-escapable. (b) is the proximate binder;
  (f) is why no amount of audio mixing helps past one pod.**

---

## 2. Ranked binding order as R grows past 500 (P=20–50, single owner pod, 1 Gbps NIC)

| Rank | Constraint | f(R,P,K) | Binds at |
|---|---|---|---|
| **1** | **(b) VIDEO egress bandwidth** | `min(P,6)·400 kbps·R` | **R ≈ 520** (1 Gbps) / 1040 (2 Gbps) |
| 2 | **(f) Single-pod aggregate ceiling** | `video_egress + audio_egress + cpu` on 1 pod | same R, no escape w/o spillover |
| 3 | egress CPU (audio decides + video decide) | `P·R / (0.7·C)` | R ≈ 6000–20000 (far past NIC) |
| 4 | (c) ingest | `P·(50+30)` pps / min(K,C) | P in high hundreds |
| 5 | (e) scorer write | K writes/tick (w/ F.4) | P in hundreds |
| 6 | (a) mixer DSP CPU | `50·[K·dec+enc+crypto]` | never (flat O(K)) |
| 7 | (d) per-frame crypto | `~250 GCM ops/s/room` | never (flat O(K)) |

**The mixer (a) and its crypto (d) — the entire thing the town-hall plan builds and the security
reviews exhaustively audited — are ranked LAST. They never bind.** The audio-egress term they
eliminate was real, but the perf review already established it sits *behind* the NIC, and video
egress is the larger per-receiver term once audio is mixed.

---

## 3. The single most-contended chokepoint

**VIDEO egress bandwidth on the single owner pod's NIC**, binding at **R ≈ 520 receivers** (1 Gbps)
at the visible-video-saturated shape (6 active tiles), independent of how large P grows.

- Cite: video cap `MAX_VISIBLE_VIDEO=6` (`subscription.rs:41`, `forwarder.rs:499`); video stays
  per-presenter E2EE and is **explicitly untouched by the mixer** (ADR §4.2.4 "Video is untouched");
  audio uncapped path that mixdown collapses (`forwarder.rs:456`); single-pod pinning
  (`CROSS-POD-DATA-PLANE-FUTURE` deferral).
- The mixer lives on the same fan-out pool as the egress shards (ADR §4.2.2), on the same owner pod,
  behind the same NIC. So mixdown frees CPU and audio bytes but cannot free the NIC of video bytes.

**Why this is "most contended" and not the mixer:** the mixer is a single task but O(K)=O(1) load
and never grows. The NIC is a hard physical ceiling that **every receiver's video stream shares**,
and it grows linearly in R. At R=520 it is full; the mixer is at ~1% of a core.

---

## 4. Probability estimate — is the plan solving the binding constraint?

Rough confidence that each is the *actual first* bottleneck to scaling past 500 (single owner pod):

| Constraint | P(binds first) | Rationale |
|---|---|---|
| **(b) Video egress bandwidth** | **~55%** | Largest per-receiver term after audio mix; binds at R≈520, right at threshold. Sensitivity: depends on usable NIC (1 vs 2 Gbps) and per-tile bitrate (200–600 kbps), but it's first across that whole range. |
| **(f) Single-pod ceiling (aggregate)** | **~25%** | (b) is its proximate sub-term; if NIC is generous (2.5 Gbps+) the *next* binder is still on-pod (CPU at R≈6k, or memory). Either way you can't add pods. |
| **(a)+(d) Mixer / mix-path CPU+crypto** | **~10–15%** | Only binds if K is raised far above 4, or mixer-per-room becomes mixer-per-many-rooms-on-one-pod (many concurrent town-halls), or Opus encode is far slower than estimated. Low. |
| (c)+(e) ingest / scorer | **~5%** | Only at P in the hundreds; town-hall P is tens. |

**So: the probability the mixer/mix-path is the binding bottleneck is ~10–15%.** The town-hall plan
is **optimizing a non-binding term for the >500 single-pod regime it targets.** Audio egress *was* a
real cost in the uncapped `P×R` shape, and mixdown is a legitimate, large reduction on that axis —
but the perf review already showed audio sits *behind* the NIC, and after the mix the NIC is filled
by **video**, which mixdown does nothing about.

**Note on the perf review's R≈1560 figure:** that number (perf §C, §C-bullet) is derived from
audio-only 640 kbps/receiver (20×32 kbps) and does not add the 6-tile video term. Adding video
(6×400=2.4 Mbps) makes the *real* E2EE-base per-receiver budget ≈3.04 Mbps ⇒ R≈329 at 1 Gbps. This
matters: it means even **before** town-hall, the NIC binds well under 500 at the heavy-video shape,
and removing audio (3.04→2.45 Mbps) only nudges R from ~329 to ~408. **The dominant per-receiver
byte term is video in both modes.** This should be reconciled with the perf review.

---

## 5. Does mixdown move the bottleneck? — Yes. What the design MUST also address.

**Stated plainly: town-hall audio mixdown moves the bottleneck from audio-egress onto VIDEO egress
and the single-owner-pod NIC ceiling.** A room can now *enter* town-hall at 500 and immediately be
NIC-bound on video on the same pod. The audio collapse is necessary-but-not-sufficient.

To actually scale 500→thousands, the design must add at least one of:

1. **SFU-side VIDEO layer-drop / per-pod egress budget (highest leverage, lowest cost).** The
   forwarder already has VP9 SVC layer machinery (`forwarder.rs:534`+ enhancement-layer drop,
   `should_drop_non_member_for_layer_budget`). Extend it so that **at scale the SFU drops video
   spatial/temporal layers** (e.g. force all receivers to base-layer-only, or 1–2 tiles instead of
   6, or lower the per-tile bitrate target) as R rises — bounding `video_egress_bps` independent of
   R growth on a single pod. This is the video analogue of audio mixdown: bound the *per-receiver*
   byte cost. It does NOT require decrypting video (layer-drop reads cleartext `RoutingHeader`
   layer ids per ADR-0001), so **video stays E2EE.** This is the missing piece that makes town-hall
   actually fit on one pod.

2. **Cross-pod spillover for egress fan-out (structural, larger).** Lift the single-owner-pod pin
   for receiver fan-out so a town-hall room's R receivers split across pods (each pod gets the mix +
   the video tiles and fans out to its share). This is the deferred `CROSS-POD-DATA-PLANE-FUTURE`
   work and is the only way to scale R into the thousands without per-receiver video reduction.

3. **(Weaker) Video tiling/mixing.** Server-side compositing of N video tiles into one mosaic stream
   would collapse video egress like audio mixdown — but it **breaks video E2EE** (the design's
   headline invariant: "video stays E2EE in both modes") and is far heavier than audio mix. **Not
   recommended** — option 1 (layer-drop) achieves the bound while keeping video E2EE.

**Recommendation:** option 1 (SFU video layer-drop budget at scale) is the minimum the design must
add to make the town-hall claim ("fit 500+ on bounded infra") true, and it preserves the video-E2EE
invariant the whole security review is built on. Option 2 is required to go past ~1000–1500 on a
single pod's NIC.

---

## 6. Does this change the B4–B12 bead-plan priorities? — Yes.

The current Track-2 plan makes **B6 (crypto: media-scoped key + GCM + OAEP)** the long pole, with
two MUST-FIX nonce items. The security work is correct and necessary *if* audio is decrypted at all.
**But B6→B7→B8 perfect a path that does not move the scaling ceiling**, because video egress (term b)
binds at R≈520 — at or below the 500 threshold where town-hall even turns on.

Recommended re-prioritization:

- **Add B13 (NEW, P0-of-scaling): SFU video egress budget / layer-drop at scale.** Extend the
  existing SVC layer-drop (`forwarder.rs:534`+) to bound per-receiver video bytes as R grows; keep
  video E2EE. This is what makes "town-hall scales past 500" true. **It should gate the scaling
  claim ahead of B7–B12** and can ship independently of the crypto work (it touches no keys).
- **Re-sequence the scaling validation:** the bot-harness T2 ("cross 500 live") currently asserts
  `sfu_audio_egress_streams → ~1` (the audio collapse). **Add a video-egress-bandwidth assertion**:
  measure aggregate pod egress Gbps at R=500/750/1000 and assert it stays under the NIC budget *with
  video layer-drop active*. Without this, T2 will pass the audio metric while the room is NIC-bound
  on video — a green test over a non-scaling system.
- **Keep B1–B3 (Track 1) as-is** — they are the real, validated late-joiner fix and are
  E2EE-preserving; unaffected.
- **B6/B7 crypto remains correct but lower-priority for *scaling*:** it gates the *security* of
  audio mixdown, not the *scaling* of the room. Audio mixdown is still worth doing (it removes the
  audio-egress term and frees ~640 kbps/receiver, nudging the NIC R-ceiling up ~25%), but it is a
  secondary lever behind video-egress bounding.

**Bottom line for the Track-2 prototype decision:** Track 2 as designed will produce a correct,
fully-audited audio-mixdown path that **does not, by itself, let a room scale past ~520 receivers on
one pod**, because video egress (untouched by the design) binds there. **Before prototyping Track 2
as the scaling solution, add the video-egress bound (B13) or the cross-pod spillover (option 2) to
the design — otherwise Track 2 optimizes a non-binding term and the >500 wall stays put.** This is a
**scope change to the design**, not just a re-ordering: the ADR currently has no answer for video
egress at scale beyond the per-receiver MAX_VISIBLE=6 cap, which is exactly the term that binds.

---

## Key citations
- Audio uncapped fan-out (the term mixdown collapses): `actix-api/src/sfu/forwarder.rs:456`.
- Video per-receiver cap `MAX_VISIBLE_VIDEO=6` (the term that binds after mix): `actix-api/src/sfu/subscription.rs:41`, enforced `forwarder.rs:499`.
- Video stays per-presenter E2EE, untouched by mixer: ADR §4.2.4; SVC layer-drop machinery to extend: `forwarder.rs:534`+.
- Mix is top-K only (K=`MAX_SPEAKERS=4`), independent of P: ADR §4.2.1; `speaker.rs:159`, `:434`, `:509`.
- Mixer on the fan-out pool / same pod / same NIC: ADR §4.2.2.
- Opus 20 ms frames → 50 pps/presenter: `videocall-client/src/encode/microphone_encoder.rs:568`, seq `:393`.
- Scorer write on ingest hot path (perf F.4 batching required): `chat_server.rs:3218`; tick 200 ms `speaker.rs:162`, `tick_once` `:417`.
- Effective fan-out cores ≈0.6–0.75·C; NIC named as E2EE-base ceiling: perf review §C, §F.5.
- Single-owner-pod pinning / deferred cross-pod data plane: project memory `project_sfu_v1_validated.md`, `CROSS-POD-DATA-PLANE-FUTURE.md`.
