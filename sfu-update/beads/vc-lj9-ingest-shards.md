# SFU: enable K>1 ingest shards for large rooms (parallelize the inbound DRAIN itself)
Source: `FORWARDING-STALL-ROOTCAUSE.md` (P1 bead 4). B3 (vc-kcpg) built subject-sharded ingest
but defaults K=1 (`config.rs:108`) so the drain is still single-task — the ONLY lever that
parallelizes the inbound drain. Enable K>1 (`SFU_INGEST_SHARDS`) for large rooms: requires the
PUBLISH side (client + BOT harness) to publish on sharded subjects `room.{room}.{shard}.{session}`
(dual-subscribe migration so K=1==today). Bot change needed to actually exercise/validate K>1.
## P1 (after lj-7). Acceptance: with K=4 + sharded publish, 20p/400 inbound drain parallelizes
##   across 4 tasks; cliff raised ~4x; bot harness publishes sharded. SFU + bot.
