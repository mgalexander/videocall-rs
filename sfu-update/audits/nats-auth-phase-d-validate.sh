#!/usr/bin/env bash
# Phase D — validate that NATS auth is actually enforced.
#
# Per sfu-update/audits/nats-auth-rollout.md §"Phase D". Two probes:
#   1. Connect WITHOUT creds — must be refused.
#   2. Connect WITH creds — must succeed.
#
# Run after Phase C completes on BOTH regions. Repeat per cluster.
#
# Usage:
#   KUBECTX=do-us-east-cluster NATS_USER=sfu-cluster \
#       NATS_PASSWORD='<paste>' \
#       bash sfu-update/audits/nats-auth-phase-d-validate.sh
set -euo pipefail

: "${KUBECTX:?set KUBECTX}"
: "${NATS_USER:?}"
: "${NATS_PASSWORD:?}"
: "${NAMESPACE:=default}"
: "${NATS_HOST:=nats:4222}"

POD_BASE="nats-probe-$(date +%s)"

run_probe() {
    local NAME="$1" URL="$2" EXPECT="$3"
    echo "=== probe: $NAME (expect: $EXPECT) ==="
    # `kubectl run --rm` returns nats CLI exit code through the pod.
    set +e
    kubectl --context "$KUBECTX" -n "$NAMESPACE" run "$NAME" \
        --image=natsio/nats-box:latest --restart=Never --rm -i --quiet \
        --timeout=15s -- \
        nats --server="$URL" sub --count=0 'sfu-probe.>' 2>&1 \
        | head -3
    local rc=$?
    set -e
    case "$EXPECT" in
      refused) [[ $rc -ne 0 ]] || { echo "FAIL: expected refusal but got success" >&2; return 1; } ;;
      success) [[ $rc -eq 0 ]] || { echo "FAIL: expected success but got failure ($rc)" >&2; return 1; } ;;
    esac
}

run_probe "${POD_BASE}-nocreds" "nats://$NATS_HOST" refused
run_probe "${POD_BASE}-creds" "nats://$NATS_USER:$NATS_PASSWORD@$NATS_HOST" success

echo
echo "✓ NATS auth enforced on $KUBECTX"
