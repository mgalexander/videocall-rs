# helm: wire POD_NAMESPACE (downward API) into SFU StatefulSets + ensure per-pod headless DNS

Source: `sfu-update/audits/200bot-monitor/DEFECT1-REDIRECT-BOUNCE.md`. Coordinated
helm half of the redirect-FQDN-namespace fix (SFU code half = vc bead
`vc-redirect-fqdn-namespace`). Ops-rig work (helm/).

## Why
The SFU builds its ADMISSION_DECISION redirect target as a per-pod StatefulSet
FQDN. To emit the correct namespace in that FQDN, the SFU should read
`POD_NAMESPACE` from its environment (the SFU code falls back to the mounted
serviceaccount namespace file + "default" if unset, so this bead is robustness/
explicitness, not a hard prerequisite for local). Provide it via the K8s downward
API, and verify the headless Service actually supports per-pod DNS.

## Fix spec
1. In `helm/rustlemania-webtransport/templates/statefulset.yaml` AND
   `helm/rustlemania-websocket/templates/statefulset.yaml`, add an env var:
   ```yaml
   - name: POD_NAMESPACE
     valueFrom:
       fieldRef:
         fieldPath: metadata.namespace
   ```
   (Confirm `POD_NAME` is similarly wired; add if missing — used for ordinal.)
2. Verify per-pod DNS prerequisites in
   `helm/rustlemania-webtransport/templates/headless-service.yaml` (+ websocket):
   the Service is headless (`clusterIP: None`), the StatefulSet's `serviceName`
   points at the headless Service (required for `<pod>.<svc>.<ns>` DNS), and set
   `publishNotReadyAddresses: true` if redirects must reach a pod before it's
   marked Ready.
3. Reconcile the workload-name prefix with `fullnameOverride` used by
   `helm/global/us-east/webtransport/values.yaml` and singapore, so the name the
   SFU builds matches the actual pod/service names in prod overlays.

## Acceptance
- A pod started from these charts has `POD_NAMESPACE` (and `POD_NAME`) in its env.
- `<pod>.<headless-svc>.<namespace>.svc.cluster.local` resolves to exactly that pod
  from another pod in the namespace (verify in the local k3d stack at replicas≥2).
- Prod overlays (us-east/singapore) produce a redirect FQDN that matches the actual
  workload names.

## Owning rig: videocall_ops (helm/). Coordinate with vc-redirect-fqdn-namespace.
## Priority: P1.
