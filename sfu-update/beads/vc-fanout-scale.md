# SFU: per-room fan-out is single-task O(N) — silent black-hole past ~1-2k receivers

Source: `DELIVERY-SCALING-ROOTCAUSE.md` (Rank-1 root cause + Fixes A/B/C/D).
THE per-room scaling ceiling: a single SFU pod stops delivering media to ANY
listener once a room exceeds ~1-2k receivers.

## Root cause (code-proven)
One dispatcher task per room (`spawn_room_dispatcher`, `chat_server.rs:2372`) does
serial single-core fan-out: `sub.next()` (bounded 16*1024 NATS channel,
`nats_connect.rs:170`) → parse → iterate the ENTIRE receiver snapshot calling
`forwarder.decide` + `try_send` per receiver (`chat_server.rs:2800-2835`). At
~1-2k receivers × inbound pps this exceeds one core; the 16K channel fills;
async-nats SILENTLY DROPS inbound + fires connection-global SlowConsumer
(`chat_server.rs:2411-2420`); `sub.next()` never returns None so the vc-9eh
watchdog can't help. Result: flat ~1-core CPU and a room-wide black hole (new and
existing receivers get 0). Confirmed: worked at 300, breaks ~300-2k, hard-0 (not
low) at 10k, CPU flat ~960m.

## Fix
- **A (P0): shard fan-out across K worker tasks/cores.** Parse once, then dispatch
  the parsed packet to K workers each owning a receiver shard, so fan-out scales
  with cores instead of capping at one. Preserve ordering guarantees per receiver.
- **B (P0, ships with A): bounded intake stage** so inbound is ALWAYS drained;
  overflow becomes explicit priority-class shedding (drop P4 first, never silently
  black-hole the whole room). Surface drops via `sfu_dropped_total{reason}`.
- **C (P1): PrioritySender egress + larger receiver mailbox** to absorb post-
  resubscribe bursts (`chat_server.rs:2824-2832`).
- **D (P2): remove the per-`decide` `SFU_ROOM_SIZE` gauge write** that takes the
  room lock on every packet×receiver (`forwarder.rs:344-346`) — move to a periodic
  updater.
Applies to BOTH WebTransport and WebSocket fan-out paths.

## Acceptance
- replicas=1, 4 CPU: a mid-stream listener joining a room of 2k/5k/10k receives +
  decodes media (crc=0); SFU CPU SCALES with receiver count (not flat at 1 core);
  no silent inbound drops (SlowConsumer not hit, or shed explicitly + counted).
- Validate with the SFU_JOIN_MILESTONES markers: receiver_set tracks member_count
  and sfu_forward_total rate scales past 2k (was flat).
- No regression at small scale (200-participant webinar unaffected); WS path tested.
## Priority: P0 (the room-scale ceiling). SFU. Run code-reviewer + performance-reviewer.
## Lint: fmt + clippy -D warnings clean.
