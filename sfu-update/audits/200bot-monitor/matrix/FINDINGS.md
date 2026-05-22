# Scaling-pattern matrix (10 tests, 10-400 users, 4/10/20 presenters) — 2026-05-22 20:22-21:25

Full real-decode + CRC per listener; SFU 4CPU/4Gi single pod. Results in matrix-results.csv.

## Universal results
- **crc_mismatches = 0 in ALL 10 tests** — byte-perfect delivery across every
  pattern/presenter/churn combo.
- **0 SFU restarts in ALL 10** — stable through bulk join, mass depart, rejoin
  cycles, slow trickle, stepped ramp. CPU <=942m, mem ~360Mi (well under 4CPU/4Gi).

## Decode pattern (listeners with video / audio, of summarized)
| test | pattern | pres | users | video% | audio% |
|---|---|---|---|---|---|
| t1 | large-join  | 4  | 100 | 99%  | 99%  |
| t2 | large-join  | 10 | 400 | 82%  | 90%  |
| t3 | large-join  | 20 | 400 | 67%  | 92%  |
| t4 | join-depart | 10 | 300 | 31%* | 36%* |
| t5 | join-depart | 20 | 400 | 77%  | 90%  |
| t6 | slow-join   | 4  | 200 | 97%  | 98%  |
| t7 | slow-join   | 10 | 400 | 12%  | 12%  |
| t8 | join-rejoin | 10 | 200 | 42%  | 48%  |
| t9 | join-rejoin | 20 | 300 | 16%  | 72%  |
| t10| step-ramp   | 10 | 400 | 2%   | 2%   |
(* t4 counts include departed/short-lived pods)

## Findings
1. **Co-arrival / burst joins decode well** (t1 99%, t2/t3 ~67-92%, t6 97%). The
   opening burst registers everyone before the SFU is saturated serving them.
2. **Gradual / late joiners degrade — and AUDIO drops too, not just video**
   (t7 slow-to-400 = 12%, t10 stepped-to-400 = 2%). Audio has no keyframe
   dependency, so audio≈0 means late joiners are NOT being forwarded at all — the
   same late-joiner DELIVERY limit found at 10k (ChatServer actor / dispatcher
   throughput for NEW joiners), surfacing earlier under gradual-join patterns and
   higher presenter counts.
3. **Video-only tail in the otherwise-good tests** (t3 67% video vs 92% audio;
   t9 16% video vs 72% audio) = the separate Defect-2 keyframe limit for
   mid-stream joiners (audio fine, video misses the keyframe).
4. **More presenters compounds it** — 20 presenters (t3) lowers video% vs 10 (t2);
   the active-speaker / MAX_VISIBLE_VIDEO=6 + keyframe contention bites harder.

## Net
- Byte-fidelity (crc=0) and SFU stability are SOLID across all patterns at v1 scale.
- A meeting where most users are present at start (typical webinar) works well.
- A meeting that GROWS gradually or churns heavily to 400 with many presenters
  loses late joiners — same root as the 10k delivery ceiling (de-serialize join/
  fan-out: the E-fixes + vc-ypx3) plus Defect-2 keyframe for video.
