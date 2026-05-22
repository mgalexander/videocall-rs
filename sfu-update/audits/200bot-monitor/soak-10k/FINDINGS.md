# 10k egress soak — 2026-05-21 14:40–15:21 (4CPU SFU, 20 presenters, lite base +1000/step to 10k)

## SFU resource: stable, no crash, FLAT cpu
| listeners | peak cpu_m | %4cpu | mem_Mi |
|---|---|---|---|
| 1000 | 624 | 15 | 217 |
| 2000 | 903 | 22 | 250 |
| 3000 | 963 | 24 | 335 |
| 4000 | 965 | 24 | 407 |
| 5000 | 959 | 23 | 489 |
| 6000 | 993 | 24 | 559 |
| 7000 | 965 | 24 | 624 |
| 8000 | 933 | 23 | 684 |
| 9000 | 712 | 17 | 746 |
| 10000| 672 | 16 | 796 |
0 restarts, 0 panics, ready throughout. (vc-nidq panic fix held at 10k.)

## CRITICAL CAVEAT — delivery did NOT scale to 10k
Decode probes (100 full-decode listeners joining at each step):
- @1000 (joined T=0, co-arrival): video=1297 audio=6059 crc=0  -> got media.
- @2000..10000 (mid-stream joiners): video=0 audio=0  -> got NOTHING.

So only the co-arrival probe decoded; every mid-stream probe from 2000+ received
zero media (not even audio). CORROBORATED by the FLAT ~960m CPU: true fan-out to
10k would scale CPU with listeners; constant CPU => bounded work, NOT 10k-worth of
forwarding. The SFU is resource-stable but its DELIVERY plateaus ~1-2k listeners;
mid-stream joiners beyond that get nothing.

## Interpretation
- Resource ceiling NOT reached (4 CPU, 796Mi at 10k nominal — both low).
- The real limit is a DELIVERY/subscription scaling issue for mid-stream joiners at
  large room size (mid-stream join WORKED at 300 in single-pod-verify; 0 at 2000+).
  Candidates: per-room receiver/subscription scale cap, active-speaker/AllowSet
  forwarded-set limit, or CPU-pegged senders producing too little to fan out.
- GAP: lightweight BASE listeners' receive/CRC data was not collected (soak10k.sh
  has no post-completion base summary collection) — only probes + metrics. Add base
  collection to confirm whether the base also stops receiving past ~1-2k.

## Next
Investigate why mid-stream joiners get 0 media beyond ~1-2k listeners (it is NOT
resource exhaustion). Distinct from the panic (vc-nidq) and spillover bugs.
