# SFU: tunable join-milestone markers (SFU_JOIN_MILESTONES) for room-growth observability

Source: `sfu-update/audits/200bot-monitor/DELIVERY-SCALING-ROOTCAUSE.md` (Deliverable 2).
Do EARLY — it confirms the delivery-scaling root cause in the wild AND validates the
fan-out fix.

## Ask
A configurable marker that logs ONCE each time a room's participant count crosses an
"interesting clip": 10, 50, 100, 250, 500, 1000, 2000, 4000, 8000, ... (tunable).

## Design
- Config param `SFU_JOIN_MILESTONES` (comma list), parsed once in
  `SfuConfig::from_env` (`actix-api/src/sfu/config.rs:43-57`). Default a sane list
  (e.g. `10,50,100,250,500,1000,2000,4000,8000`) or off; document in help/README.
- Hook the crossing check right AFTER the receiver insert in JoinRoom
  (`actix-api/src/actors/chat_server.rs:2122`). O(1), only fires at a crossing — NOT
  per join.
- Emit ONE structured `sfu_trace`/`tracing` event `sfu_join_milestone` carrying the
  state that explains the delivery plateau:
  - `room`, `member_count`,
  - **`receiver_set`** = the size the per-room dispatcher actually fans out to
    (`room_dispatch[room].receivers.len()`) — this DIVERGES from member_count when
    delivery breaks,
  - new joiner's AllowSet audio/video sizes,
  - current `sfu_forward_total` / `sfu_dropped_total` (so a flatlining forward rate
    at a milestone is visible).
- New gauges next to `SFU_ROOM_SIZE` (`actix-api/src/metrics.rs:347`):
  `sfu_room_members{room}`, `sfu_room_receiver_set{room}`.
- Reuse vc-8wd tracing infra; cheap when no milestone crossed.

## Acceptance
- `SFU_JOIN_MILESTONES=...` set: crossing each listed count logs exactly one
  `sfu_join_milestone` with member_count, receiver_set, allowset sizes, forward/drop.
- In a pre-fix soak the marker shows `receiver_set` climbing with `member_count`
  while `sfu_forward_total` rate flatlines past ~1-2k (reproduces the root cause).
- Gauges appear on /metrics. Off/empty list = no overhead.
## Priority: P1, do FIRST. SFU. Lint: fmt + clippy -D warnings clean.
