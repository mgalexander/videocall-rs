#!/usr/bin/env bash
# Phase A — create the nats-credentials Secret on a kubectl context.
#
# Per sfu-update/audits/nats-auth-rollout.md §"Phase A". Run ONCE per
# K8s cluster context (us-east AND singapore). Same credential value in
# both regions because the NATS supercluster is a single trust domain.
#
# This phase is a no-op for running pods — neither NATS nor the SFU pods
# pick up the secret automatically. Phase B (helm upgrade) does that.
#
# Generate the password once, save it in a password manager, then pass
# it via stdin to BOTH region invocations so they match exactly.
#
# Usage:
#   # 1. Generate the password (just this once; copy to password manager)
#   openssl rand -base64 32 | tr -d '=+/' | head -c 32 ; echo
#
#   # 2. Apply to each cluster:
#   KUBECTX=do-us-east-cluster NATS_USER=sfu-cluster NATS_PASSWORD='<paste>' \
#       bash sfu-update/audits/nats-auth-phase-a-create-secret.sh
#   KUBECTX=do-singapore-cluster NATS_USER=sfu-cluster NATS_PASSWORD='<paste>' \
#       bash sfu-update/audits/nats-auth-phase-a-create-secret.sh
#
# Or with --dry-run to preview the manifest:
#   KUBECTX=... NATS_USER=... NATS_PASSWORD=... DRY_RUN=1 \
#       bash sfu-update/audits/nats-auth-phase-a-create-secret.sh
set -euo pipefail

: "${KUBECTX:?set KUBECTX to the kubectl context name (e.g. do-us-east-cluster)}"
: "${NATS_USER:?set NATS_USER (recommended: sfu-cluster)}"
: "${NATS_PASSWORD:?set NATS_PASSWORD (32+ chars; generated, not memorable)}"
: "${NAMESPACE:=default}"
SECRET_NAME="${SECRET_NAME:-nats-credentials}"

# Build the secret manifest. Pass through stdin so the password never
# appears in process listings or shell history.
MANIFEST=$(
  kubectl --context "$KUBECTX" -n "$NAMESPACE" create secret generic \
      "$SECRET_NAME" \
      --from-literal=user="$NATS_USER" \
      --from-literal=password="$NATS_PASSWORD" \
      --dry-run=client -o yaml
)

if [[ -n "${DRY_RUN:-}" ]]; then
    echo "=== dry-run: would apply the following Secret to $KUBECTX ==="
    # Redact the password value in the preview.
    echo "$MANIFEST" | sed 's/^  password: .*/  password: <REDACTED>/'
    exit 0
fi

echo "=== applying $SECRET_NAME to $KUBECTX/$NAMESPACE ==="
echo "$MANIFEST" | kubectl --context "$KUBECTX" -n "$NAMESPACE" apply -f -

echo
echo "=== verifying ==="
kubectl --context "$KUBECTX" -n "$NAMESPACE" get secret "$SECRET_NAME" \
    -o jsonpath='{.metadata.name}: {range .data}{@}{"\n"}{end}' | head -5
echo
echo "next: bash sfu-update/audits/nats-auth-phase-b-redeploy-sfu.sh \\"
echo "          KUBECTX=$KUBECTX"
