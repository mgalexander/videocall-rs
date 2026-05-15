#!/usr/bin/env bash
# Phase C — flip auth.enabled=true on the NATS server chart.
#
# Per sfu-update/audits/nats-auth-rollout.md §"Phase C". This is the
# moment unauthenticated clients start getting refused. The SFU pods
# from Phase B authenticate successfully and keep running; every other
# unprivileged in-cluster workload that was talking to NATS stops.
#
# **BOTH REGIONS MUST BE FLIPPED WITHIN A FEW MINUTES OF EACH OTHER**
# because the cross-region gateway is a NATS-to-NATS supercluster link.
# A one-sided flip breaks the gateway in unpredictable ways.
#
# This script uses --set to inject the auth block without editing
# values.yaml in-tree. That keeps the secret value out of the repo.
# The chart-specific field path is determined by the nats Helm chart
# version; verify with `helm show values nats/nats` before applying.
#
# Usage:
#   KUBECTX=do-us-east-cluster NATS_USER=sfu-cluster \
#       NATS_PASSWORD='<paste from password manager>' \
#       bash sfu-update/audits/nats-auth-phase-c-enable-nats-auth.sh
#   # immediately followed by:
#   KUBECTX=do-singapore-cluster NATS_USER=sfu-cluster \
#       NATS_PASSWORD='<same paste>' \
#       bash sfu-update/audits/nats-auth-phase-c-enable-nats-auth.sh
#
# Or DRY_RUN=1 to render the manifest without applying.
set -euo pipefail

: "${KUBECTX:?set KUBECTX (e.g. do-us-east-cluster)}"
: "${NATS_USER:?set NATS_USER (same as Phase A)}"
: "${NATS_PASSWORD:?set NATS_PASSWORD (same as Phase A)}"
: "${NAMESPACE:=default}"
: "${RELEASE:=nats}"
REPO_ROOT="${REPO_ROOT:-/mnt/llms/videocall}"

# Pick the chart for the cluster's region.
case "$KUBECTX" in
  *us-east*) CHART_PATH="$REPO_ROOT/helm/global/us-east/nats" ;;
  *singapore*) CHART_PATH="$REPO_ROOT/helm/global/singapore/nats" ;;
  *)
    echo "Cannot infer region from KUBECTX=$KUBECTX (expected substring 'us-east' or 'singapore')" >&2
    echo "Override with CHART_PATH=/path/to/chart" >&2
    : "${CHART_PATH:?}"
    ;;
esac

echo "=== flipping nats auth on $KUBECTX using $CHART_PATH ==="

# Field path for the nats helm chart (verify with `helm show values nats/nats`).
# As of nats chart v1.x the basic-auth users list lives at .nats.nats.auth.users.
AUTH_SETS=(
  --set "nats.nats.auth.enabled=true"
  --set-string "nats.nats.auth.users[0].user=$NATS_USER"
  --set-string "nats.nats.auth.users[0].password=$NATS_PASSWORD"
)

if [[ -n "${DRY_RUN:-}" ]]; then
  helm --kube-context "$KUBECTX" -n "$NAMESPACE" template "$RELEASE" "$CHART_PATH" \
      "${AUTH_SETS[@]}" \
      | grep -E -A 2 "user|password" \
      | sed 's/password: .*/password: <REDACTED>/' | head -20
  echo "(dry-run, no apply)"
  exit 0
fi

helm --kube-context "$KUBECTX" -n "$NAMESPACE" upgrade --install "$RELEASE" "$CHART_PATH" \
    "${AUTH_SETS[@]}"
kubectl --context "$KUBECTX" -n "$NAMESPACE" rollout status statefulset/"$RELEASE" --timeout=300s

echo
echo "=== verifying SFU pods still healthy ==="
for r in rustlemania-webtransport rustlemania-websocket; do
  kubectl --context "$KUBECTX" -n "$NAMESPACE" get pods -l app.kubernetes.io/name="$r" \
      -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.phase}{"\n"}{end}'
done

echo
echo "next: bash sfu-update/audits/nats-auth-phase-d-validate.sh \\"
echo "          KUBECTX=$KUBECTX"
