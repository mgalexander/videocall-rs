# SFU: pipeline the fan-out (remove the strict per-message barrier)
Source: `FORWARDING-STALL-ROOTCAUSE.md` (P1 bead 3). The fan-out barrier `for h in handles {
h.await }` (`chat_server.rs:4325`/`:4207`) blocks the drain task on the slowest shard per
message, ADDING spawn+join latency to per-message service time. Pipeline so the drain can pull
the next message while shards finish (bounded in-flight), removing the barrier from the critical
path. P1 (after lj-7). Acceptance: per-message drain latency drops; no ordering regression.
