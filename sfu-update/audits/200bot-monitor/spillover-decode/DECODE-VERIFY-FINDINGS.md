# Decode-verification run — multi-pod data plane is BROKEN — 2026-05-20 16:28–16:36 CDT

Built from `1dc5802` (vc-xnp + vc-85p). SFU replicas=3, prod limits (3500m/6000Mi).
10 senders + 1,500 listeners (15 pods × 100), one room, fixed 360s so pods emit
summaries. `--verify-integrity` on.

## Result: 0 media decoded, across all 3 pods

| joined_pod (SFU pod) | bots | video decoded | audio decoded | crc_mismatch |
|---|---|---|---|---|
| 10.42.1.46 | 600 | **0** | **0** | 0 |
| 10.42.0.42 | 500 | **0** | **0** | 0 |
| 10.42.2.45 | 400 | **0** | **0** | 0 |
| **TOTAL** | **1,500** | **0** | **0** | 0 |

Total packets received across 1,500 listeners: **3,127** (~2/bot = control/RTT noise).
crc_mismatches=0 only because nothing was received to verify.

Senders DID produce media: `tx_packets_enqueued=2,048` (tx_drops=276,315, CPU-pegged).
So media existed — it just never reached listeners.

## Conclusion: the spillover "6,000 healthy" was connection-only
The spillover RAMP proved the **control plane** (connect, distribute load across 3
pods, no crash). This run proves the **data plane is broken** in multi-pod mode:
listeners connect and spread across pods but **decode nothing**. The low SFU CPU
(~60m/pod) from the ramp is now explained — the pods weren't forwarding media.

This is consistent across every multi-pod run:
- replicas=1 (v10r1): **WORKS** — 300 listeners decoded 276k video + 937k audio, crc=0.
- replicas=2 (v9r2/v10r2): 0 decode.
- replicas=3 (this run): 0 decode.

**Single-pod works; any multi-pod config delivers no media.**

## Suspected root cause (for the next investigation)
- **1,210 "owned by a different pod" rejections** (pod-1: 610, pod-2: 600) for 1,500
  listeners — most joiners are still being REDIRECTED, not spill-admitted locally,
  despite the room being far past the 180 cap. So vc-85p's admit-to-spill may not be
  engaging as expected.
- Even listeners co-located with the senders' pod decoded 0 — pointing at a
  **subscription/AllowSet failure** in the multi-pod path: spill-admitted (or
  redirected) listeners are connected but never get the senders into their AllowSet,
  so the forwarder drops everything. Likely the admit-to-spill path skips the
  receive-all/AllowSet initialization the normal single-pod join performs, and/or
  cross-pod publisher registration (vc-72a) isn't applied on spill/redirect pods.

## Next step
Investigate the multi-pod media-delivery path (why federated/cross-pod sender media
never reaches listeners; why spill-admit isn't engaging) and file a fix bead. This
is THE blocker for distributed capacity. Single-pod remains the only proven-working
deployment.
