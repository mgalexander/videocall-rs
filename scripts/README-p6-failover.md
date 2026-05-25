# SFU pod-kill failover test (bead vc-607 / p6-11)

`scripts/sfu_p6_failover_test.sh` is an end-to-end test scaffold that
verifies the SFU cluster recovers from an owner-pod kill within 15 seconds.

## What it tests

1. Six bots (1 sender + 5 listeners) join room `failover-test` via the
   load-balancer URL.
2. After 3 seconds of steady-state media flow, the script deletes the
   owner pod (`rustlemania-webtransport-0` by default).
3. The listener bots detect the disconnect, reconnect on a 500ms cadence,
   and resume receiving the sender's media when the pod comes back up
   (or when the LB routes them to the surviving replica that issues an
   `ADMISSION_DECISION{REDIRECT}` once `pod-0` is back).
4. The script asserts `max_downtime_ms < 15000` across all listeners.

The bot side of this scaffold (reconnect loop, redirect parsing, downtime
bookkeeping) lives in `bot/src/failover.rs`,
`bot/src/webtransport_client.rs`, and `bot/src/stats.rs`. The
`--failover-test` CLI flag opts in; the existing `--orchestrate` mode
(the 200-bot harness from p6-10) is unchanged.

## Prerequisites

- `kubectl`, `jq`, and (unless `BOT_BIN` is set) `cargo`.
- A k3d cluster with the `rustlemania-webtransport` StatefulSet scaled to
  at least 2 replicas:

  ```bash
  kubectl scale sts rustlemania-webtransport --replicas=2
  kubectl rollout status sts/rustlemania-webtransport
  ```

- A reachable WebTransport URL. For the local k3d bringup
  (`helm/local/up.sh`) the URL is typically:

  ```bash
  export SERVER_URL=https://transport.videocall.local:30443
  ```

  Adjust to your local stack's NodePort or LoadBalancer entrypoint. The
  script does not assume a specific port.

## Running

```bash
SERVER_URL=https://transport.videocall.local:30443 \
    scripts/sfu_p6_failover_test.sh
```

Override anything via env vars (full list in the script header):

```bash
SERVER_URL=https://transport.videocall.local:30443 \
DURATION_S=45 \
LISTENERS=10 \
MAX_DOWNTIME_MS=10000 \
    scripts/sfu_p6_failover_test.sh
```

To skip the cargo build (e.g. in CI where the binary is already built):

```bash
SERVER_URL=... BOT_BIN=./target/release/bot scripts/sfu_p6_failover_test.sh
```

## Reading the output

On a successful run the script prints, to stderr:

```
[ts] Summary:
[ts]   listeners_with_gap   = 5
[ts]   listeners_recovered  = 5
[ts]   max_downtime_ms      = 6820
[ts]   threshold            = 15000 ms
[ts]
[ts] Per-listener downtime breakdown:
  listener-0  connected=true  packets=842  downtime_ms=6210  ...
  listener-1  connected=true  packets=839  downtime_ms=6820  ...
  ...
[ts] PASS: max_downtime_ms=6820 < 15000
```

The raw JSON summary is written to a temp file (path logged at the top
of the run). Inspect it with `jq` for richer detail — every listener
snapshot includes `packets_received`, `bytes_received`, `disconnect_at_ms`,
`reconnect_at_ms`, and `downtime_ms`.

## Exit codes

| Code | Meaning                                                            |
|------|--------------------------------------------------------------------|
| 0    | Pass: max downtime under threshold and every listener recovered.   |
| 1    | Preflight failure (kubectl missing, STS not found, replicas < 2).  |
| 2    | Bot process crashed before emitting JSON.                          |
| 3    | Bot emitted no/invalid JSON.                                       |
| 4    | Assertion failure: too-long downtime or a listener never recovered.|

## Limitations

- Designed for local k3d. The `ADMISSION_DECISION{REDIRECT}` packet
  contains a cluster-internal headless DNS name; bots running outside
  the cluster log it for diagnostics but reconnect via the LB URL
  rather than chasing the redirect target directly. The k3d LB routes
  reconnects to whatever pod is up, which is sufficient to measure
  recovery.
- The test is not a `cargo test`. Shelling out to `kubectl` from a
  `#[test]` is awkward and easy to misuse; an explicit shell script
  makes the orchestration honest about what it is.
