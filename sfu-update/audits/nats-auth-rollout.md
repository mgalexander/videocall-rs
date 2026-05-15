# NATS Auth + TLS Rollout Playbook

**Companion to** [`nats-acl-audit.md`](./nats-acl-audit.md). The audit identified S-P0-4 (NATS unauthenticated bus) as a blocker for P1 closing. This playbook is the deployment sequence that closes it without dropping any pod.

**Pre-flight verification (already done on `experimental-sfu`):**

The four cells of the (client-creds, server-auth) matrix were tested end-to-end against a local NATS sandbox using the actix-api `nats_connect` helper:

| Cell | Client | Server | Observed | Why it matters |
| --- | --- | --- | --- | --- |
| A | no creds | auth | Authorization Violation | Cell A failing is the rollback-gone-wrong shape |
| B | creds | auth | success | Target post-Phase-C state |
| **C** | **creds** | **no-auth** | **success** | **Critical: proves Phase B is safe before Phase C** |
| D | no creds | no-auth | success | Today's baseline |

Reproduce locally with:
```bash
bash sfu-update/audits/nats-sandbox-up.sh
cargo test -p videocall-api --test nats_auth_integration -- --ignored --test-threads=1
bash sfu-update/audits/nats-sandbox-up.sh down   # when done
```

All four cells passed (2026-05-15). The integration test is `actix-api/tests/nats_auth_integration.rs`; run it again before each phase rollout if you've touched `nats_connect.rs`.

**Code changes have already landed** on `experimental-sfu`:
- `actix-api/src/nats_connect.rs` — env-driven helper, reads `NATS_USER`, `NATS_PASSWORD`, `NATS_TLS`, `NATS_TLS_CA`. Falls back to no-auth + plaintext when env unset (back-compat).
- Four production NATS call sites refactored (`bin/{webtransport,websocket,metrics_server,metrics_server_snapshot}.rs`) to use the helper. Two deprecated test-side call sites also routed through for consistency.
- `helm/rustlemania-{webtransport,websocket}/values.yaml` — `NATS_USER`/`NATS_PASSWORD` env from `nats-credentials` Secret (optional; safe with Secret missing), `NATS_TLS=false` placeholder.
- `helm/global/{us-east,singapore}/nats/values.yaml` — annotated with the rollout sequence and a commented-out `users:` block to copy into when flipping.

**Pre-flight assumption.** The cluster runs the existing nats Helm chart (a wrapper on the upstream `nats` chart). Auth field names below match that chart's convention. If your chart version differs, adjust the YAML keys but the sequence remains the same.

---

## Phase A — create the credential (does nothing on its own)

Runnable script: [`nats-auth-phase-a-create-secret.sh`](./nats-auth-phase-a-create-secret.sh).

```bash
# 1. Generate the password (just once; copy to password manager).
openssl rand -base64 32 | tr -d '=+/' | head -c 32 ; echo

# 2. Apply to BOTH cluster contexts with the SAME credential value
#    (single NATS supercluster trust domain):
KUBECTX=do-us-east-cluster NATS_USER=sfu-cluster NATS_PASSWORD='<paste>' \
    bash sfu-update/audits/nats-auth-phase-a-create-secret.sh
KUBECTX=do-singapore-cluster NATS_USER=sfu-cluster NATS_PASSWORD='<paste>' \
    bash sfu-update/audits/nats-auth-phase-a-create-secret.sh
```

Use `DRY_RUN=1` to preview the manifest (password is redacted in preview).

At this point: nothing changes. The actix-api pods don't know the Secret exists (they haven't been redeployed). NATS still has `auth.enabled: false`.

---

## Phase B — redeploy the SFU pods to pick up the env (still permissive)

Runnable script: [`nats-auth-phase-b-redeploy-sfu.sh`](./nats-auth-phase-b-redeploy-sfu.sh).

```bash
KUBECTX=do-us-east-cluster bash sfu-update/audits/nats-auth-phase-b-redeploy-sfu.sh
KUBECTX=do-singapore-cluster bash sfu-update/audits/nats-auth-phase-b-redeploy-sfu.sh
```

The script also rolls out the new chart values (with the optional `nats-credentials` secretKeyRef + `NATS_TLS` env), waits for the deploy rollout, and tails the pod logs for the `auth=on/off` line so you see the connect posture immediately.

Cell C of the pre-flight matrix proves this phase is safe with the NATS server still permissive.

After this rollout: every actix-api pod attempts to authenticate with the credentials. NATS is still in no-auth mode, so it **accepts** any client (with or without credentials). The pods log `auth=on tls=off` from `nats_connect`.

**Validation:** in each pod, `kubectl logs <pod> | grep "connecting to NATS"` should show `auth=on`. If it shows `auth=off`, the Secret didn't reach the pod (typo in name? wrong namespace?).

---

## Phase C — enable auth on the NATS server

Runnable script: [`nats-auth-phase-c-enable-nats-auth.sh`](./nats-auth-phase-c-enable-nats-auth.sh).

```bash
# BOTH regions, within a few minutes of each other:
KUBECTX=do-us-east-cluster NATS_USER=sfu-cluster NATS_PASSWORD='<paste>' \
    bash sfu-update/audits/nats-auth-phase-c-enable-nats-auth.sh
KUBECTX=do-singapore-cluster NATS_USER=sfu-cluster NATS_PASSWORD='<paste>' \
    bash sfu-update/audits/nats-auth-phase-c-enable-nats-auth.sh
```

The script uses `--set` and `--set-string` to inject the auth block at apply time, keeping the password out of `values.yaml`. Field path is `nats.nats.auth.users[0]` per the nats chart v1.x convention; if your chart version differs, verify with `helm show values nats/nats` and override `CHART_PATH` or edit the script's `AUTH_SETS` array.

Both regions **must** be flipped together (within a few minutes of each other). The cross-region gateway is a NATS-to-NATS supercluster connection; if one side is authenticated and the other isn't, the gateway misbehaves.

**Validation:** any pod or sidecar trying to connect to NATS without credentials now gets a connect error. Existing actix-api pods (from Phase B) keep working because their env has credentials.

---

## Phase D — verify auth is actually enforced

Runnable script: [`nats-auth-phase-d-validate.sh`](./nats-auth-phase-d-validate.sh).

```bash
KUBECTX=do-us-east-cluster NATS_USER=sfu-cluster NATS_PASSWORD='<paste>' \
    bash sfu-update/audits/nats-auth-phase-d-validate.sh
KUBECTX=do-singapore-cluster NATS_USER=sfu-cluster NATS_PASSWORD='<paste>' \
    bash sfu-update/audits/nats-auth-phase-d-validate.sh
```

The script runs two probes in each cluster using `kubectl run --rm` against `natsio/nats-box`: (a) connect without creds, expects refusal; (b) connect with creds, expects success. Exits non-zero with a clear `FAIL: expected ...` message if either probe disagrees.

If (a) succeeds (unauthenticated connection accepted), Phase C didn't take effect — re-check the chart key names match your nats chart's conventions and re-run Phase C.

---

## Phase E (optional, defense in depth) — TLS on the client port

This is heavier and not strictly required to close S-P0-4; the audit ranked it as "Phase: lift cross-region exposure (before P6 closes)." If you take it on now, the steps are:

1. Create a NATS server cert via cert-manager: `Certificate` resource pointing at a `ClusterIssuer`, output `nats-tls` Secret with `tls.crt`/`tls.key`.
2. Update `helm/global/{us-east,singapore}/nats/values.yaml`: add `nats.nats.tls.enabled: true` (chart-specific key; check your chart docs) referencing the secret.
3. Mount the CA bundle into the actix-api pods:
   ```yaml
   - name: NATS_TLS_CA
     value: /etc/nats-ca/ca.crt
   # plus a volumeMount onto the cert-manager-managed CA Secret.
   ```
4. Set `NATS_TLS: "true"` in `helm/rustlemania-{webtransport,websocket}/values.yaml`.
5. Redeploy actix-api pods, then nats. Validate with the `nats-box` probe using a `tls://` URL.

If you defer this phase, document so in `sfu-update/audits/nats-acl-audit.md` §"Recommended remediation order" — flag the residual exposure (pod-to-nats plaintext on the internal Kubernetes pod network).

---

## Phase F — subject ACLs (post-P1)

Define per-credential subject ACLs from `nats-acl-audit.md` §F-5. Not blocking P1; needed before any third-party workload runs on the same cluster.

---

## Rollback

If anything wedges in Phase C (auth enabled, pods can't connect):

```bash
# Revert just the nats Helm release; pods keep their (now no-op) auth env:
helm --kube-context us-east   rollback nats
helm --kube-context singapore rollback nats
```

The actix-api pods continue working because they were already attempting auth in Phase B and the no-auth NATS accepts anyone. The damage window is limited to the seconds between auth being enabled and the rollback completing.

If pods can't even start after Phase B (env-related crash), `kubectl rollout undo deploy/rustlemania-webtransport` restores the previous Deployment which didn't reference `nats-credentials`.

---

## Hooks to monitor

- `nats_connect` logs at INFO: `auth=on/off tls=on/off`. Aggregate across pods; spot any pod that says `auth=off` after the rollout.
- NATS server logs: `[Authorization Violation]` rate. Should be 0 once Phase D probe is cleaned up; anything non-zero indicates a misconfigured workload.
- Pod CrashLoopBackOff during Phase C: usually means `auth.users[].password` doesn't match the Secret's `password` field. Compare via `kubectl get secret nats-credentials -o jsonpath='{.data.password}' | base64 -d`.

---

## Where this fits with P1

P1 lands the `RoutingHeader` proto fields including `audio_level` and `is_speaking`. With NATS auth enabled (Phases A–D complete), those fields are only visible to credentialed subscribers — which currently means the SFU pods and (when added) the metrics pipeline. Without auth, those fields would be exposed to any in-cluster workload that can resolve `nats:4222`.

**Hard recommendation:** Phases A–D complete BEFORE merging P1 into `experimental-sfu`. The code-level pieces of this rollout have already landed; the remaining work is operational (Secret creation, two helm upgrades, one validation step).
