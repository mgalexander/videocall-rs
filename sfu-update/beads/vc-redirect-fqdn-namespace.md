# SFU: ADMISSION_DECISION redirect FQDN omits the pod namespace — spillover/redirect traffic can't reach the adjacent pod

Source: `sfu-update/audits/200bot-monitor/DEFECT1-REDIRECT-BOUNCE.md` (decisive
root cause). THE blocker that prevents redirected/spillover clients from landing
on the correct pod in multi-pod deployments.

## Root cause
`actix-api/src/sfu/affinity.rs:525-527` (`compute_redirect_target`) formats the
redirect host as:
```
rustlemania-{tk}-{owner}.{tk}-headless.svc.cluster.local
```
i.e. `rustlemania-webtransport-0.webtransport-headless.svc.cluster.local` — with
**no namespace label**. The correct StatefulSet per-pod FQDN is:
```
<pod>.<headless-svc>.<namespace>.svc.cluster.local
```
The namespace-less name does not resolve to the target pod; the client's reconnect
round-robins back onto a random (often non-owner) pod. Measured: 1,100 listeners
each got exactly ONE redirect (`hop 1/5`, no hop-2), mis-landed on a non-owner pod,
and decoded 0. The bot side (`bot/src/orchestrate.rs:453` `compute_redirect_url`)
is correct — it only swaps the host and preserves scheme/port/path; it cannot
re-insert a namespace the server never sent.

The doc comment (`affinity.rs:497`) and unit tests (`affinity.rs:686`, `:693`) bake
in the same broken template, so tests pass while the production name is unresolvable.

Secondary (prod-only): the template hardcodes the `rustlemania-{tk}` prefix, but
`helm/global/us-east/webtransport/values.yaml:3-4` sets
`fullnameOverride: webtransport-us-east`. Masked locally because the local
StatefulSet is literally named `rustlemania-webtransport`. Make the workload-name
prefix configurable (env) so prod overlays produce a matching FQDN.

## Fix spec
1. Build the redirect FQDN WITH the namespace:
   `{workload}-{owner}.{workload}-headless.{namespace}.svc.cluster.local`.
2. Resolve `namespace` robustly so it works locally AND in prod without depending
   on a helm change landing first:
   - prefer `POD_NAMESPACE` env (set by the helm bead via downward API), else
   - read `/var/run/secrets/kubernetes.io/serviceaccount/namespace` (present in
     every pod, contains the real namespace — gives "default" locally), else
   - fall back to `"default"`.
3. Make the workload-name prefix come from config/env (the existing fullname/
   release name the chart already provides) instead of the hardcoded
   `rustlemania-{tk}` literal, so `fullnameOverride` overlays resolve correctly.
4. Fix the doc comment + unit tests to assert the FQDN CONTAINS a namespace label
   (the tests must fail on the old namespace-less form).

## INSTRUMENTATION GUARDRAIL (explicit overseer requirement)
This fix lives in the redirect/affinity path. If the change touches any
join / subscribe / forward / spillover code that carries the vc-8wd
instrumentation, **PRESERVE and (where natural) extend it** — do NOT remove or
weaken `sfu_join_decision_total`, `sfu_session_teardown_total`,
`sfu_spillover_owner_count`, `sfu_dropped_total{reason}`, the `SFU_TRACE_ROOM`
gated trace, or the always-on counters. We need that observability intact to
investigate the cross-pod data-plane issue (the follow-up after this lands). If
the redirect-decision site is touched, ensure the join-decision counter still
records admit_local / redirect / reject with reason. Adding a counter/trace point
for "redirect FQDN emitted" (target + namespace) would help validate this fix in
the wild.

## Acceptance
- replicas≥3: a client that lands on a non-owner pod and is redirected reaches the
  intended OWNER pod (FQDN resolves correctly), OR is spill-admitted locally — and
  in either case lands on a resolvable address; "owned by a different pod"
  rejections drop to ~1 per listener (no second redirect from a bad name).
- The emitted redirect FQDN contains the namespace label; verified by a unit test
  that fails on the namespace-less form, and observed in a replicas≥3 run.
- vc-8wd instrumentation still present and functioning (counters on the metrics
  endpoint; `SFU_TRACE_ROOM` still works).
- (Per-pod media DECODE for spill-admitted listeners is validated in the SEPARATE
  cross-pod data-plane follow-up, deferred until after this lands.)

## Priority: P0 — blocks moving spillover traffic to an adjacent pod.

## Lint
`cargo fmt` + `cargo clippy -- -D warnings` on `actix-api` clean.
