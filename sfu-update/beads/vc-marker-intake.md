# SFU: milestone markers must key off INTAKE counters, not room_members (so they fire when registration is the bottleneck)

Source: `DELIVERY-SCALING-ROOTCAUSE.md` REFINEMENT Q4. Follow-up to vc-xow8: the
markers keyed off `room_members` fired ZERO times because registration itself was
the bottleneck — the very failure mode we needed to SEE.

## Fix
- Add `sfu_join_attempts_total` (increment at JoinRoom handler ENTRY,
  `chat_server.rs:1619`) and `sfu_sessions_connected_total` (in `Connect`, `:834`)
  — increment regardless of registration success. Add a `sfu_join_inflight` gauge.
- Drive the milestone crossing off the INTAKE counter (connected/attempts), not
  `room_members`, so milestones fire at 1000/2000/... even when joins aren't
  registering.
- Emit `connected, join_attempts, members, receiver_set` TOGETHER in the
  `sfu_join_milestone` payload so ONE marker distinguishes the two failure modes:
  registration plateau ⇒ `connected ≫ members ≈ receiver_set`; fan-out plateau ⇒
  `members ≈ receiver_set` climbing while `sfu_forward_total` flatlines.
- Keep the existing `sfu_room_members`/`sfu_room_receiver_set` gauges.

## Acceptance
- In a 3,000-listener soak BEFORE Fix E, markers fire at 1000/2000/... showing
  `connected≫members` (registration plateau visible).
- After Fix E, markers show connected≈members≈receiver_set climbing.
## Priority: P1 (do alongside Fix E so the soak shows the plateau). SFU.
## Lint: fmt + clippy -D warnings clean.
