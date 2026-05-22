# helm: add liveness + readiness probes to SFU StatefulSets (none exist → zombies invisible)

Source: `DEFECT-JOINHANDLE-PANIC.md`. The SFU StatefulSet has NO livenessProbe/
readinessProbe, so a forwarding-dead-but-alive pod is never restarted by k8s.

## Fix
Add `livenessProbe` + `readinessProbe` (httpGet `/healthz` on the health port) to
`helm/rustlemania-webtransport/templates/statefulset.yaml` and the websocket
equivalent. Tune thresholds for a real-time SFU (avoid flapping under brief load
spikes; a few consecutive failures before restart). Pairs with the SFU
forwarding-aware `/healthz` (vc-sfu-failfast-panic) so liveness reflects actual
forwarding health.

## Acceptance
- SFU pods have liveness + readiness probes; a pod whose `/healthz` fails is
  restarted by k8s within a bounded window.
- No flapping under normal load.
## Owning rig: videocall_ops. Priority: P1.
