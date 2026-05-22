# SFU: per-receiver VIDEO egress budget / layer-drop at scale — the actual >500 scaling wall

Source: `sfu-update/audits/200bot-monitor/TOWNHALL-CHOKEPOINT-ANALYSIS.md`. The
town-hall audio mixdown optimizes a NON-BINDING term: once audio egress collapses to
1×R, the binding chokepoint is VIDEO egress bandwidth on the single owner pod's NIC
(~55% probability it's THE bottleneck), binding at R≈520 receivers @ 1 Gbps. Video
stays per-presenter E2EE (6 tiles × 400kbps = 2.4 Mbps/receiver, untouched by mixdown).
The mixer/crypto ranks LAST (~10-15%). This bead is what actually makes a room scale
past ~500; the town-hall crypto (B5-B12) gates audio SECURITY, not room SCALING.

## Scope
- Extend the existing VP9 SVC per-receiver layer-drop (`forwarder.rs:534`+,
  `layer_selector.rs`) with an AGGREGATE per-pod egress budget: as receiver count /
  NIC utilization rises, drop video temporal/spatial layers per receiver to bound
  total video egress under the NIC ceiling. Reads cleartext `RoutingHeader` layer ids
  → VIDEO STAYS E2EE (no decryption).
- Greedy: guarantee base layer (T0/S0) for every visible tile; shed enhancement
  layers first under budget pressure; fairness across receivers.
- Surface: `sfu_video_egress_bps`, `sfu_layer_drops_total{reason=egress_budget}`,
  and a NIC-utilization-aware signal. Tie to the vc-m7k6 saturation metric family.

## Acceptance
- A 20p × (R growing past 500) soak on a bandwidth-constrained pod: total video
  egress stays under the NIC budget; base-layer video preserved for all visible
  tiles (crc=0 on what's delivered); enhancement layers shed gracefully, not
  black-holed. Per-receiver video bytes bounded as R grows.
- Reconcile the E2EE-base ceiling number with the perf review (video-inclusive
  ~329-520/pod @ 1Gbps, not audio-only 1560).
## Priority: P0-of-scaling — gates the "scales past 500" claim AHEAD of town-hall
##   crypto (B5-B12). Ships independently (touches NO keys, video stays E2EE).
## Also: T2 bot-harness MUST assert video-egress Gbps (today only asserts audio
##   collapse → would pass green over a video-NIC-bound system).
## backend-rust-streaming + performance-reviewer sign-off. Lint: fmt + clippy -D.
