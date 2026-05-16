#!/usr/bin/env bash
# nats-sandbox-up.sh — bring up the local NATS dev sandbox used by the
# auth integration matrix in actix-api/tests/nats_auth_integration.rs.
#
# Thin wrapper around `docker compose -f docker/docker-compose.nats-dev.yaml`.
# The actual service definitions live in that compose file (see
# docker/README.nats-dev.md for end-to-end usage).
#
# Usage:
#   bash sfu-update/audits/nats-sandbox-up.sh         # start (up -d)
#   bash sfu-update/audits/nats-sandbox-up.sh down    # stop + remove
#   bash sfu-update/audits/nats-sandbox-up.sh status  # ps
#
# After `up`, run the matrix from the repo root:
#   cargo test -p videocall-api --test nats_auth_integration -- \
#       --ignored --test-threads=1
#
# Pre-2026-05-15 versions of this script ran `docker run` directly against
# the host docker daemon with /tmp-based config files. That violated the
# "no host-bare docker fixtures" rule from the operating model (see
# sfu-update/PLAN.md "Isolation rules"). The compose form keeps the dev
# loop inside the project's docker/ convention.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
COMPOSE_FILE="$REPO_ROOT/docker/docker-compose.nats-dev.yaml"

if [[ ! -f "$COMPOSE_FILE" ]]; then
    echo "compose file not found: $COMPOSE_FILE" >&2
    exit 2
fi

CMD="${1:-up}"
case "$CMD" in
  up|start)
    docker compose -f "$COMPOSE_FILE" up -d
    docker compose -f "$COMPOSE_FILE" ps
    ;;
  down|stop)
    docker compose -f "$COMPOSE_FILE" down
    ;;
  status|ps)
    docker compose -f "$COMPOSE_FILE" ps
    ;;
  *)
    echo "usage: $0 [up|down|status]" >&2
    exit 2
    ;;
esac
