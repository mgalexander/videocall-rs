# SFU: AllowSet cache invalidation thundering-herd — ONE join busts all R caches → O(R²) recompute storm chokes ingest (THE late-joiner root cause)

Source: `sfu-update/audits/200bot-monitor/LATE-JOINER-INTEGRATION-ROOTCAUSE.md`. THE
mechanism behind late-joiner failure (slow-join 10p/200=49%, 20p/400=188 vs
co-arrival 366 at same CPU; crc=0; silent).

## Root cause (code-proven)
- Every join bumps a GLOBAL `members_generation` (`room_state.rs:462-463`/`:349`).
- The per-receiver AllowSet cache is keyed on that global generation
  (`subscription.rs:289-296`) → one join invalidates ALL R receivers' entries.
- Each later media packet misses → `resolve_inner` rebuilds iterating ALL members
  (`subscription.rs:365-388`) = O(R²) per join wave, ON THE MEDIA HOT PATH (inside
  the fan-out barrier via `forwarder.rs:435` resolve_cached).
- → throttles inbound drain → async-nats silently drops at the 16Ki sub
  (`nats_connect.rs:196`) → late joiners (and everyone, during the storm) lose media.
- Co-arrival: generation churns once at T=0 then stable → lock-free Arc::clone fast
  path. Slow-join: a fresh storm every wave. More presenters multiply it.

## Fix
- Incremental, RECEIVE-ALL-AWARE cache invalidation: a join must NOT bust all R
  entries on the media hot path. For receive-all receivers (the webinar default,
  `subscription.rs:554-560`) a join doesn't change their effective AllowSet at all →
  no invalidation needed. For explicit-subscription receivers, invalidate only the
  affected entries, not globally. Decouple cache rebuild from the media decide path
  (rebuild off-hot-path / on subscription change, not per-join-generation).

## Acceptance
- slow-join 10p/200 and 20p/400 reach parity with CO-ARRIVAL (late joiners decode
  audio+video, crc=0); no O(R²) recompute on the media path per join; no inbound
  drops (`SFU_DISPATCHER_INBOUND_DROPPED_TOTAL`==0) across join waves.
- No regression to subscription correctness (pin/slot/explicit receivers still
  resolve correctly).
## Priority: P0 (v1-blocking late-joiner root cause). code+perf review. fmt+clippy.
