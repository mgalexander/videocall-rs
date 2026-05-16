# docker-compose.nats-dev.yaml — NATS dev sandbox

A pair of local NATS servers used to verify the `actix-api/src/nats_connect.rs` auth helper end-to-end. Sibling of `docker-compose.integration.yaml` and `docker-compose.e2e.yaml`.

| Service | Auth | Host port | Container port |
| --- | --- | --- | --- |
| `sfu-nats-auth` | basic auth, `sfu-cluster` / `testpass123` | 24222 | 4222 |
| `sfu-nats-noauth` | none | 24223 | 4222 |

## When you'd use this

- After touching `actix-api/src/nats_connect.rs` or any of its call sites.
- Before signing off on a phase that adds new NATS-traversing packets (P1, P3).
- Re-verifying the auth-rollout safety properties before the operator runs `sfu-update/audits/nats-auth-phase-{a..d}-*.sh` against a real cluster.

## Run the auth integration matrix

```bash
# 1. Bring the sandbox up.
docker compose -f docker/docker-compose.nats-dev.yaml up -d

# 2. Run the four-cell matrix.
cargo test -p videocall-api --test nats_auth_integration -- \
    --ignored --test-threads=1

# 3. Tear down when done.
docker compose -f docker/docker-compose.nats-dev.yaml down
```

Or use the wrapper:

```bash
bash sfu-update/audits/nats-sandbox-up.sh up
cargo test -p videocall-api --test nats_auth_integration -- --ignored --test-threads=1
bash sfu-update/audits/nats-sandbox-up.sh down
```

## What "passing" means

All four cells of `(client-creds × server-auth)` must hold:

| Cell | Client creds in env | Server posture | Expected helper behaviour |
| --- | --- | --- | --- |
| A | none | auth | error (Authorization Violation) |
| B | set | auth | success |
| C | set | no-auth | success — creds are ignored by no-auth NATS |
| D | none | no-auth | success — today's production baseline |

Cell C is the **load-bearing one** for the auth rollout: it proves a code release with `NATS_USER`/`NATS_PASSWORD` set is safe against a still-permissive NATS server (i.e., the "deploy SFU pods before flipping NATS auth" ordering is safe). If Cell C ever regresses, the rollout sequence in `sfu-update/audits/nats-auth-rollout.md` needs to change.

## Probe by hand (without cargo)

```bash
# Refused — Cell A:
docker run --rm --network host natsio/nats-box:latest \
    nats sub --server=nats://127.0.0.1:24222 --count=1 'test.>'

# Accepted — Cell B:
docker run --rm --network host natsio/nats-box:latest \
    nats sub --server=nats://sfu-cluster:testpass123@127.0.0.1:24222 --count=1 'test.>'
```

## Credentials are dev-only

`sfu-cluster` / `testpass123` are deliberately weak. Don't reuse them anywhere that's reachable from outside this host. Real cluster credentials live in K8s Secrets (`nats-credentials`), provisioned per `sfu-update/audits/nats-auth-rollout.md` Phase A.

## Connecting from other compose stacks

If you need a stack from `docker-compose.integration.yaml` to talk to the auth sandbox, link them via an external network:

```yaml
# in integration.yaml or your test stack
services:
  rust-tests:
    environment:
      - NATS_TEST_AUTH_URL=nats://sfu-cluster:testpass123@sfu-nats-auth:4222
      - NATS_TEST_NOAUTH_URL=nats://sfu-nats-noauth:4222
networks:
  default:
    name: docker_default
    external: true
```

(The compose project name `docker` is used by default; verify with `docker network ls`.)
