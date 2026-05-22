# 4-CPU SFU soak (post panic-fix) — 2026-05-21 12:01–12:42

Single SFU pod 4CPU/4Gi (replicas=1) + liveness probe (vco-kct). 20 presenters
each on own pod. Listeners 500 → +500/5min to 4000. After vc-nidq (JoinHandle
panic fix). 40-min cap.

## Result: SURVIVED full 40 min, ceiling not reached
| Listeners | SFU CPU | restarts |
|---|---|---|
| 500   | ~730m | 0 |
| 1000  | ~825m | 0 |
| 2000  | ~825m | 0 |
| 3000  | ~899m | 0 |
| 4000  | ~946–977m | 0 |

- Ran full 2400s, peak 4000 listeners + 20 presenters, stable.
- **0 JoinHandle panics** (was 115 in 100s pre-fix). 0 restarts. ready throughout.
- Peak CPU **977m / 4000m (24%)**; mem ~392Mi / 4Gi. ~76% CPU headroom at 4000.

## Significance
- vc-nidq (writer JoinHandle re-await guard) fully resolved the panic→zombie that
  flatlined TX at ~100s in the prior run.
- The earlier "minimal SFU saturates at ~1000" was the panic/zombie + the 500m cap,
  NOT real saturation. At 4 CPU the SFU handles 4000 listeners + 20 presenters with
  large headroom; the capacity ceiling for this shape is well above 4000.

## Gap (fix for next time)
- Listener decode NOT captured: the soak collected `kubectl logs --tail=400` at the
  40-min mark while listener pods were still running (summaries print at pod-exit
  ~2min later). SFU-side health is the headline; data-plane decode+crc already
  proven in single-pod-verify (video+audio crc=0) and mid-stream-verify (454,930
  video crc=0). TODO: soak should collect summaries after pod completion.
