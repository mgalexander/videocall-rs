#!/usr/bin/env bash
# nats-sandbox-up.sh — bring up two local NATS containers for matrix
# testing of the actix-api auth code path. Verified working 2026-05-15;
# matches the env defaults in tests/nats_auth_integration.rs.
#
# Usage:
#   bash sfu-update/audits/nats-sandbox-up.sh         # start
#   bash sfu-update/audits/nats-sandbox-up.sh down    # stop+remove
#
# Then run the auth matrix from /mnt/llms/videocall:
#   cargo test -p videocall-api --test nats_auth_integration -- \
#       --ignored --test-threads=1
#
# Or probe by hand with nats-box:
#   docker run --rm --network host natsio/nats-box:latest \
#       nats sub --server=nats://sfu-cluster:testpass123@127.0.0.1:24222 \
#       --count=1 'test.>'
set -euo pipefail

CMD="${1:-up}"
CONF_DIR="${CONF_DIR:-/tmp/sfu-nats-sandbox}"

case "$CMD" in
  up)
    mkdir -p "$CONF_DIR"
    cat > "$CONF_DIR/auth.conf" <<'EOF'
listen: 0.0.0.0:4222
http: 0.0.0.0:8222
authorization {
  user: sfu-cluster
  password: testpass123
}
EOF
    cat > "$CONF_DIR/noauth.conf" <<'EOF'
listen: 0.0.0.0:4222
http: 0.0.0.0:8222
EOF
    docker rm -f sfu-nats-auth sfu-nats-noauth >/dev/null 2>&1 || true
    docker run -d --name sfu-nats-auth -p 24222:4222 \
        -v "$CONF_DIR/auth.conf:/etc/nats/nats.conf:ro" \
        nats:2.10 -c /etc/nats/nats.conf >/dev/null
    docker run -d --name sfu-nats-noauth -p 24223:4222 \
        -v "$CONF_DIR/noauth.conf:/etc/nats/nats.conf:ro" \
        nats:2.10 -c /etc/nats/nats.conf >/dev/null
    docker ps --filter "name=sfu-nats" --format "{{.Names}}\t{{.Ports}}"
    ;;
  down)
    docker rm -f sfu-nats-auth sfu-nats-noauth >/dev/null 2>&1 || true
    rm -rf "$CONF_DIR"
    echo "sandbox down"
    ;;
  *)
    echo "usage: $0 [up|down]" >&2
    exit 2
    ;;
esac
