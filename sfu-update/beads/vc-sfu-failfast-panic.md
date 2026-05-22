# SFU: fail-fast on critical-task panic (don't zombie) + forwarding-aware /healthz

Source: `DEFECT-JOINHANDLE-PANIC.md` (fail-fast section). Robustness fix so a
forwarding-task panic RECOVERS (k8s restart) instead of leaving a zombie.

## Problem
When the bridge/forwarding tasks panicked (see vc-bridge-joinhandle-race), the
process stayed alive (NATS PINGs, ready=true, 0 restarts) and never recovered —
forwarding dead, TX flat, indefinitely. `/healthz` is a static `Ok`
(`actix-api/src/bin/webtransport_server.rs:32-34`) served by an independent task,
so it reports healthy even when forwarding is dead.

## Fix (SFU side)
1. Install a panic hook (or `panic = "abort"`) in `webtransport_server.rs::main`
   so a panic on a critical task crashes the PROCESS → k8s restarts it → recovery.
   (Pair with the k8s liveness/readiness probes added in the helm bead.)
2. Make `/healthz` forwarding-aware: fail (5xx) if the SFU is not forwarding
   (reuse the vc-9eh dispatcher liveness signal / a forwarding-progress heartbeat),
   so a stuck-but-alive process is detected.

## Acceptance
- Inject a panic in a forwarding task → the process exits (non-zero) rather than
  zombie-ing; under k8s it restarts and resumes forwarding.
- `/healthz` returns non-200 when forwarding is dead.
## Priority: P1. Pairs with vco helm liveness-probe bead.
## Lint: cargo fmt + clippy -D warnings on actix-api clean.
