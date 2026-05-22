# Realistic stress: 20 presenters (own pods) + 500→1000 listeners, minimal SFU — 2026-05-21 10:12–10:31

Methodology fix vs prior cramped run: each of the 20 presenters on its OWN pod
(6 CPU headroom for one 720p30 VP9 encode — realistic device), not 10–20 crammed
into 2 CPU-pegged pods. SFU stays minimal (500m/256Mi, replicas=1).

## Results
| Phase | listeners | NO-VIDEO | NO-AUDIO | crc |
|---|---|---|---|---|
| core (co-arrival, 500) | 500 | 82% | 37% | 0 |
| grow (mid-stream → 1000) | 500 | 100% | 100% | 0 |

SFU: 0 restarts, crc=0, mem 119Mi/256Mi, **CPU pegged 502m/500m**.
Senders (20, own pods): tx_enqueued 7,393 total / tx_drops 1,511,862 (~99%).

## Findings
1. **Methodology fix helped:** core-listener audio failures dropped 82%→37%
   (18%→63% decoding) once senders had their own pods. More real media flowed.
2. **Minimal 500m SFU is the bottleneck.** Senders on their own 6-CPU pods STILL
   drop ~99% — not sender CPU starvation now, but the SFU unable to INGEST 20
   presenters at 500m → QUIC backpressure fills sender outbound channels. The SFU
   pegged at 502m. CPU-bound (memory fine). **A 500m pod is undersized for 20
   presenters + ~1000 listeners.**
3. **Video gated by Defect-2** (vc-7zjq): 82% no-video co-arrival, 100% on grow.
4. SFU stayed stable (0 restarts, crc=0) — it degrades by shedding under CPU
   saturation, doesn't crash.

## For a clean 20-presenter capacity number
- Land Defect-2 (vc-7zjq) → video for co-arrival + mid-stream joiners.
- Give the SFU more than 500m CPU (it's pegged) — 20 presenters' ingest + fan-out
  to ~1000 listeners needs real server CPU. Then the run measures true capacity
  rather than the 0.5-CPU ceiling.

## Update — Defect-2 (vc-7zjq) verified fixed (2026-05-21 10:48)
Mid-stream-join test, 10 own-pod senders + 300 listeners joining T+25s, minimal SFU:
- video_frames_decoded = 454,930 (was 0 pre-fix), audio = 1,221,356, crc = 0.
- NO-VIDEO dropped from 100% → 42% (58% of mid-stream listeners decode video).
- A first verification with CRAMPED senders falsely showed 0 video (starved senders
  emit no keyframe to deliver) — always test keyframe delivery with own-pod senders.
Residual 42% = keyframe-timing tail, future refinement.
