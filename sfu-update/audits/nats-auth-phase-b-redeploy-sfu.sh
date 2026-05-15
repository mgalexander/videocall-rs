#!/usr/bin/env bash
# Phase B — redeploy SFU pods so they pick up nats-credentials env.
#
# Per sfu-update/audits/nats-auth-rollout.md §"Phase B". After this phase
# the actix-api pods authenticate with their credentials but NATS still
# accepts everyone. Confirmed safe end-to-end by Cell C of
# tests/nats_auth_integration.rs.
#
# Run AFTER Phase A on the same cluster context. The chart values.yaml
# already references the optional Secret via secretKeyRef + optional:
# true, so this rollout reads existing chart contents — no helm flags
# needed beyond the standard upgrade.
#
# Usage:
#   KUBECTX=do-us-east-cluster bash sfu-update/audits/nats-auth-phase-b-redeploy-sfu.sh
#   KUBECTX=do-singapore-cluster bash sfu-update/audits/nats-auth-phase-b-redeploy-sfu.sh
#
# To preview the diff without applying:
#   KUBECTX=... DRY_RUN=1 bash sfu-update/audits/nats-auth-phase-b-redeploy-sfu.sh
set -euo pipefail

: "${KUBECTX:?set KUBECTX to the kubectl context name}"
: "${NAMESPACE:=default}"
REPO_ROOT="${REPO_ROOT:-/mnt/llms/videocall}"

CHARTS=(
  "rustlemania-webtransport:$REPO_ROOT/helm/rustlemania-webtransport"
  "rustlemania-websocket:$REPO_ROOT/helm/rustlemania-websocket"
)

for entry in "${CHARTS[@]}"; do
  RELEASE="${entry%%:*}"
  CHART_PATH="${entry##*:}"
  echo "=== $RELEASE @ $KUBECTX/$NAMESPACE ==="
  if [[ -n "${DRY_RUN:-}" ]]; then
    helm --kube-context "$KUBECTX" -n "$NAMESPACE" diff upgrade "$RELEASE" "$CHART_PATH" \
        2>&1 || true   # `helm diff` plugin may not be installed; non-fatal
    echo "(dry-run, no apply)"
  else
    helm --kube-context "$KUBECTX" -n "$NAMESPACE" upgrade --install "$RELEASE" "$CHART_PATH"
    kubectl --kube-context "$KUBECTX" -n "$NAMESPACE" rollout status deploy/"$RELEASE" --timeout=180s
  fi
done

if [[ -z "${DRY_RUN:-}" ]]; then
  echo
  echo "=== verifying pods log auth=on ==="
  for RELEASE in rustlemania-webtransport rustlemania-websocket; do
    POD=$(kubectl --context "$KUBECTX" -n "$NAMESPACE" get pods \
        -l app.kubernetes.io/name="$RELEASE" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
    if [[ -n "$POD" ]]; then
      echo "$RELEASE / $POD :"
      kubectl --context "$KUBECTX" -n "$NAMESPACE" logs "$POD" --tail=200 2>/dev/null \
          | grep -E "connecting to NATS|auth=" | head -3 || \
          echo "  (no 'connecting to NATS' line yet; pod may still be starting)"
    fi
  done
fi

echo
echo "next: bash sfu-update/audits/nats-auth-phase-c-enable-nats-auth.sh \\"
echo "          KUBECTX=$KUBECTX"
