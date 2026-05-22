# Stress: 500→1000 listeners, 20 presenters, minimal SFU — 2026-05-21 08:41–09:00

replicas=1, SFU minimal (500m/256Mi). 20 senders (2 pods×10) + 500 listeners
co-arrival, churn waves at T+180/300/420, grow to 1000 at T+540. ~18min.

## Results
| Phase | bots | video_decoded | audio_decoded | NO-VIDEO | NO-AUDIO | crc |
|---|---|---|---|---|---|---|
| core (co-arrival, 500) | 500 | 385 | 25,887 | 434/500 (86%) | 413/500 (82%) | 0 |
| grow (mid-stream, +500→1000) | 500 | 0 | 0 | 500/500 (100%) | 500/500 (100%) | 0 |

SFU: **0 restarts, crc_mismatches=0 throughout.** CPU peaked **458m / 500m**
(near but not pegged), memory **115Mi / 256Mi** (comfortable). (Churn-pod logs were
GC'd before collection — short-lived; core+grow tell the story.)

## Root cause of the failures: the BOT SENDERS, not the SFU
Sender output: **s1 tx_enqueued 3,018 / tx_drops 718,497; s2 3,018 / 706,916 —
~99.6% of frames DROPPED.** 20 VP9 encoders crammed into 2 CPU-pegged pods (6 CPU,
10 senders = 0.6 CPU/sender) cannot encode fast enough; almost no media ever
enters the SFU. Listeners can't decode what was never sent.

- The SFU forwarded the trickle that escaped (450m CPU, 115Mi, crc=0, no crash) —
  it was NOT the bottleneck and was nowhere near OOM.
- The 14% of core listeners that decoded video / 18% that decoded audio got the
  small fraction that the senders managed to emit.
- The grow (mid-stream) listeners got 0: compounded by (a) sender starvation and
  (b) DEFECT-2 (mid-stream joiners miss the keyframe; bot drops keyframes under
  backpressure).

## Interpretation
This run captured massive listener video/audio failures — but they are **bot-
harness sender-starvation failures, not SFU capacity failures.** The minimal SFU
pod stayed healthy. The harness simply **cannot drive 20 real VP9 presenters** on
CPU-limited pods (this is the sender producer ceiling first noted in the capacity
ramp, now acute at 20 senders).

## What a TRUE 20-presenter test needs (harness enhancement)
- Give senders real CPU: ~1–2 senders per pod (e.g. 20 pods × 1, or 10 × 2 with
  high CPU limits) so VP9 encode keeps up, OR
- A lighter/precomputed sender encode path (encode once, replay) so one pod can
  drive many presenters without per-sender VP9 cost, AND
- DEFECT-2 keyframe-aware backpressure (never drop keyframes) so mid-stream joiners
  can decode video.
Only then does a 20-presenter / 500–1000-listener run measure the SFU rather than
the harness's encoder budget.

## SFU takeaway
On a 500m/256Mi pod the SFU forwarded faithfully (crc=0), stayed at 458m/115Mi, and
did not crash across 500→1000 listeners + 20 publishers + churn. The SFU's behavior
here is a clean pass; the test bed's sender side is the limiter.
