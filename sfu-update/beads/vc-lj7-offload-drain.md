# SFU: offload per-message work off the single inbound-drain task (raise the saturation cliff)
Source: `FORWARDING-STALL-ROOTCAUSE.md` (P0 bead 2). THE throughput fix for the 20p/400
forwarding stall. The single room dispatcher task (`chat_server.rs:3712` loop) does, PER
INBOUND MESSAGE before pulling the next: inline `room_state.write()` for remote-publisher
registration (`:4033-4087`, load-bearing — presenters are on other pods so EVERY packet is
remote), the lj-2 greedy-drain RE-PARSE of every drained message (`:4428`), scorer
write.await (`:3633`), diagnostics (`:4109`). Per-message service time × inbound rate > 1
core → async-nats 16KiB buffer (`nats_connect.rs:196`) tail-drops → SlowConsumer → starved
trickle → CPU dies to ~18m at ~250 receivers.
## Fix: move remote-publisher registration OFF the drain task (register async / batched /
##   off-hot-path); eliminate the greedy-drain re-parse (carry the parsed form, don't reparse);
##   move scorer.write off the per-message await. Goal: minimize per-message drain service time
##   so the cliff moves well above 400 receivers @ 20 presenters.
## Acceptance: 20p/400 slow-join — CPU does NOT collapse to ~18m; reach climbs toward co-arrival
##   (~366+); SFU_DISPATCHER_INBOUND_DROPPED_TOTAL stays ~0 across waves. P0. code+perf review.
