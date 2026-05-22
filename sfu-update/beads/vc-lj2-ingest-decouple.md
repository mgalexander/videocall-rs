# SFU: decouple inbound drain from per-message fan-out recompute — never silently black-hole

Source: LATE-JOINER-INTEGRATION-ROOTCAUSE.md (lj-2). Defense-in-depth for lj-1: even
with the cache fixed, the per-message fan-out barrier (`chat_server.rs:3873-3892`,
shard tasks awaited before `sub.next()`) couples inbound drain to egress work, so any
recompute/egress spike throttles ingest → silent async-nats drop (`nats_connect.rs`).

## Fix
- Flow-control / decouple: drain the bounded NATS subscription independently of the
  fan-out barrier so a transient egress/recompute spike can't stall ingest; on
  genuine overflow, shed by priority class explicitly (P4 first) and COUNT it
  (`SFU_DISPATCHER_INBOUND_DROPPED_TOTAL`) — never silent.
## Acceptance: induced egress spike does not cause silent inbound loss; overflow is
##   explicit + counted; late-joiner waves don't drop. P0, ships with/after lj-1.
