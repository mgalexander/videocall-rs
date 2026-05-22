# Spillover ramp — 500-step, multi-pod — 2026-05-20 16:04–16:23 CDT

Built from `1dc5802` (vc-xnp redirect-uni-stream + vc-85p admit-to-spill).
SFU patched to prod-class limits (3500m/6000Mi), replicas=3. 10 persistent
senders + 500 listeners/batch (5 pods × 100), one room, `--verify-integrity`.

## Result: 6,000 listeners, 0 errors, load evenly distributed across 3 pods

| Listeners | pod-0 | pod-1 | pod-2 |
|---|---|---|---|
| 500   | 7m/24Mi   | 7m/23Mi   | 5m/15Mi |
| 1,500 | 22m/68Mi  | 16m/52Mi  | 16m/46Mi |
| 3,000 | 32m/111Mi | 33m/105Mi | 35m/107Mi |
| 4,500 | 49m/175Mi | 45m/158Mi | 41m/149Mi |
| **6,000** | **62m/229Mi** | **56m/208Mi** | **56m/201Mi** |

Ramp hit its 6,000 cap with NO errors (no restart/OOM/panic/Pending).

## PROVEN
- **Redirect delivery works (vc-xnp).** 300 "owned by a different pod" + 600
  ADMISSION_DECISION log lines = early/below-threshold joiners are redirected to
  the owner — and the system did NOT collapse (v10r2 collapsed here). The
  uni-stream fix made redirects actually deliver.
- **Spillover admits locally + distributes (vc-85p).** Past the 180 cap, pods
  admit their own joiners instead of redirecting → the 3 pods carry near-equal
  load all the way to 6,000 (62/56/56m). If spillover were off, all load would
  pile on the owner.
- **Media federates across pods** (NATS `room.{room}.*` messages arrive on all
  pods).
- **Huge resource headroom**: at 6,000, each pod is ~60m CPU / ~220Mi vs the
  3500m/6000Mi limits (~2% CPU, ~4% mem). The ceiling was NOT reached.

## NOT PROVEN (important caveat)
- **Per-listener media DECODE at scale was NOT verified.** The ramp checks only
  for crashes; listener pods were torn down without emitting decode/crc
  summaries. So "6,000 healthy" means connected + load-distributed + no SFU
  errors — NOT "6,000 verified decoding with crc_mismatches=0".
- **Suspiciously low SFU CPU.** ~60m/pod for ~2,000 listeners each is implausibly
  low for active media fan-out: in v10r1 a single pod used **436m for just 300
  listeners** that verifiably decoded 276k video + 937k audio frames. If
  spill-admitted listeners were truly receiving media at volume, CPU should be
  far HIGHER, not lower. This suggests a possible **spill-pod → local-listener
  forwarding/subscription gap**: media federates TO the spill pod (NATS) but may
  not be forwarded to the listeners admitted THERE. (Analogous to the
  late-listener class of bug, but on spill pods.)

## Recommended next step
A **decode-verification run**: fixed-duration multi-pod test (e.g. 3,000
listeners across 3 pods, 5 min, pods complete and emit summaries) asserting
spill-admitted listeners actually decode media with crc_mismatches=0. If they
don't, file a spill-pod-forwarding bead. The connection/distribution scaling is
proven; media delivery to spill-admitted listeners is the open question.
