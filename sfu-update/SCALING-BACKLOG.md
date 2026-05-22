# SFU Scaling Backlog (post-v1, deliberate track) — 2026-05-22

v1 is BANKED: the late-joiner blocker is resolved at the 200-participant / ≤10-presenter
target (slow-join → 97.5% audio, 90% video, crc=0, E2EE intact). Full fix stack landed on
`experimental-sfu`: Track 1 (vc-c609/9u8e/kcpg multi-thread sharded fan-out + ingest) + the
late-joiner fixes (vc-zexm cache-off-hot-path = the v1 fix; vc-vyg9 drain-decouple; vc-j4kz
remote-pub offload; vc-nys3 drop-slope recovery = rooms self-heal, no permanent-dark).

Everything below is the DELIBERATE >v1 scaling track — schedule intentionally, NOT via
single-shot soak-grinding. Root analyses: `audits/200bot-monitor/{PRESENTER-SCALING,
LATE-JOINER-INTEGRATION,FORWARDING-STALL,TOWNHALL-CHOKEPOINT}-ROOTCAUSE.md`.

## The binding constraints, in the order they appear as a room grows
| Range | Binding constraint | Lever |
|---|---|---|
| ≤~366 recv/pod (steady) | single-task inbound DRAIN = 1 core | **lj-9 (K>1 ingest shards)** |
| slow-join transient <366 | drain transient cliff | lj-8 pipeline, lj-9 (landed: lj-6 recovery, lj-7 offload) |
| ~329-520 recv/pod | **VIDEO egress bandwidth (NIC)** — dominant per-receiver term | **B13 video egress budget** |
| >~1000-1500/room | single-owner-pod ceiling | **multi-pod spillover** |
| >500 + many presenters | audio egress P×R (only after video bounded) | town-hall mixdown (B5-B12) |

## Backlog (ordered)
1. **lj-8 (vc-8rh2, P1, SFU-only):** pipeline the fan-out barrier off the per-message
   critical path. Small; adds drain margin. Cheapest next step.
2. **lj-9 (vc-rcpp, P1, SFU+client+bot):** K>1 ingest shards — parallelize the inbound
   drain across cores (the ONLY lever past the ~366 single-core drain ceiling). Needs
   coordinated sharded-publish `room.{room}.{shard}.{session}` on the client AND the bot
   harness; dual-subscribe migration so K=1==today. Breaks 20p/400 reach past ~200.
3. **B13 (vc-stee, P0-of-scaling, SFU):** per-receiver VIDEO egress budget / VP9 SVC
   layer-drop. The real >500 wall (video = ~2.4 Mbps/receiver, untouched by audio work;
   NIC-binds ~520 @ 1Gbps). Reads cleartext RoutingHeader → video STAYS E2EE. Ships
   independently. ALSO: the bot harness must assert video-egress Gbps.
4. **multi-pod spillover (CROSS-POD-DATA-PLANE-FUTURE.md):** one room across pods, for
   >~1000-1500. The structural scale-out; currently deferred/broken (13-commit arc didn't
   converge). Required for true thousands-per-room.
5. **town-hall audio mixdown (ADR-0009 B5-B12, security-CLEARED to prototype):** dynamic
   at 500; collapses audio egress P×R→1×R. ONLY relevant after B13 (video binds first) and
   only >500. Binding crypto conditions in the ADR: B6 NF-1 (HKDF per-(epoch,sender) subkey)
   + NF-2 (epoch-scoped monotonic counter), B5 Ed25519 switch auth, RSA→OAEP sub-bead +
   legacy parity test, GCM-tag/RED framing. web-security-auditor re-audits at B5/B6/B7.
6. **lj-3 (vc-133g, P1):** verify it isn't subsumed by lj-6 (vc-nys3 drop-slope dispatcher
   recovery) before building. **lj-4 (vc-3hxy, P2):** SFU_ROOM_SIZE gauge off per-decide.

## Validation note
Use a FRESH SFU pod per soak (back-to-back soaks degrade state). Bot-harness must collect
listener summaries AFTER pods complete, and (post-B13) assert video-egress Gbps not just the
audio collapse. gastown repeatedly wedges these commits off experimental-sfu → cherry-pick
from the bare repo (back up scratch to /tmp first per the clone-wipe hazard).
