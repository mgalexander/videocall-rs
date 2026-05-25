# Test Matrix — SFU Refactor

> **Status:** Filled out under bead `vc-c4e.10`.
>
> **Source of truth:** [`PLAN.md` — Verification](./PLAN.md#verification).
>
> Rows are (codec, browser, shape) combinations. Cells mark whether a given
> verification gate (routing-header read, subscription update, layer dropping,
> active speaker, affinity redirect) is covered by an automated test, a manual
> run, out of scope for v1, or a known gap.

## Dimensions

- **Codec.** The on-the-wire video codec emitted by `camera_encoder.rs` via
  WebCodecs `scalabilityMode`.
  - `VP9-L1T3` — 1 spatial × 3 temporal layers. The initial v1 default (per
    PLAN.md Phase 4). Single spatial layer means layer dropping degenerates to
    a temporal-only / hold-or-drop heuristic.
  - `VP9-L3T3_KEY` — 3 spatial × 3 temporal, keyframe-aligned. Deferred but the
    capacity-model target. Exercises the full layer-selector two-pass.
- **Browser / Client.** The receiver's runtime. Sender-side is symmetric on
  Chromium; Safari send-path is verified only where the browser ships
  WebCodecs+WebTransport (Safari 18.2+).
  - `Chromium M111+` — primary target; full VP9-SVC encode + decode.
  - `Safari 18.2+` — WebTransport ships in 18.2 (per PLAN.md Open Risk #8);
    VP9-SVC dropped-layer rendering is the open verification question.
- **Shape.** The meeting topology, drawn from PLAN.md's locked decisions and
  capacity model.
  - `1:1` — two peers, symmetric. No speaker rotation, no affinity work.
  - `small-group` — ≤6 peers, symmetric video. The default conference-ish
    shape inside the webinar v1 envelope.
  - `webinar (~200)` — 10 senders, ~190 listeners. The capacity-model target.
  - `pathological (one slow receiver)` — any of the above shapes with a single
    receiver throttled (see `network_throttle.py`). The motivating case for
    layer dropping and class-aware drop policy.

## Matrix

| Codec        | Browser        | Shape                        | Routing-header read | Subscription update | Layer dropping | Active speaker | Affinity redirect | Notes |
|--------------|----------------|------------------------------|---------------------|---------------------|----------------|----------------|-------------------|-------|
| VP9-L1T3     | Chromium M111+ | 1:1                          | ✅                  | ✅                  | ⏭             | ⏭             | ⏭                | single-layer; layer dropping N/A; speaker/affinity not meaningful with one peer |
| VP9-L1T3     | Chromium M111+ | small-group (≤6)             | ✅                  | ✅                  | 🧪             | ✅             | ✅                | single-layer fallback is a hold/drop heuristic; manual coverage |
| VP9-L1T3     | Chromium M111+ | webinar (~200)               | ✅                  | ✅                  | 🧪             | ✅             | ✅                | covered by 200-bot load test; layer-drop limited to temporal axis |
| VP9-L1T3     | Chromium M111+ | pathological (slow receiver) | ✅                  | ✅                  | 🧪             | ✅             | ✅                | temporal-only drop heuristic; manual `network_throttle.py` run |
| VP9-L1T3     | Safari 18.2+   | 1:1                          | ✅                  | ✅                  | ⏭             | ⏭             | ⏭                | Safari send-path 18.2+ only; single-layer |
| VP9-L1T3     | Safari 18.2+   | small-group (≤6)             | ✅                  | ✅                  | 🧪             | ✅             | ✅                | Safari decode parity verified manually |
| VP9-L1T3     | Safari 18.2+   | webinar (~200)               | ✅                  | ✅                  | 🧪             | ✅             | ✅                | listener-only Safari clients folded into 200-bot test |
| VP9-L1T3     | Safari 18.2+   | pathological (slow receiver) | ✅                  | ✅                  | 🧪             | ✅             | ✅                | manual; pairs with Chromium throttled-receiver run |
| VP9-L3T3_KEY | Chromium M111+ | 1:1                          | ✅                  | ✅                  | ✅             | ⏭             | ⏭                | full SVC selector exercised even at N=2 |
| VP9-L3T3_KEY | Chromium M111+ | small-group (≤6)             | ✅                  | ✅                  | ✅             | ✅             | ✅                | layer-selector two-pass unit + integration covers this row |
| VP9-L3T3_KEY | Chromium M111+ | webinar (~200)               | ✅                  | ✅                  | ✅             | ✅             | ✅                | release-gate 200-bot load test row |
| VP9-L3T3_KEY | Chromium M111+ | pathological (slow receiver) | ✅                  | ✅                  | ✅             | ✅             | ✅                | the motivating scenario; covered by `network_throttle.py` integration |
| VP9-L3T3_KEY | Safari 18.2+   | 1:1                          | ✅                  | ✅                  | ❌             | ⏭             | ⏭                | deferred — Safari VP9-SVC dropped-layer rendering TBD (Open Risk #8) |
| VP9-L3T3_KEY | Safari 18.2+   | small-group (≤6)             | ✅                  | ✅                  | ❌             | ✅             | ✅                | deferred — Safari SVC support TBD; tracked as follow-up |
| VP9-L3T3_KEY | Safari 18.2+   | webinar (~200)               | ✅                  | ✅                  | ❌             | ✅             | ✅                | deferred — Safari SVC support TBD; webinar coverage gated on Safari fix |
| VP9-L3T3_KEY | Safari 18.2+   | pathological (slow receiver) | ✅                  | ✅                  | ❌             | ✅             | ✅                | deferred — Safari SVC support TBD; covered by sfu-speaker-rotation.spec.ts for the speaker column |

## Legend

- ✅ — Automated coverage (CI)
- 🧪 — Manual coverage (documented runbook)
- ⏭ — Out of scope for v1
- ❌ — Known gap; tracked as a follow-up bead

## CI Gates (P6 close gate)

Wired under bead `vc-8qc`. Implemented by:

- `.github/workflows/load-test.yaml` (the two GH Actions jobs).
- `helm/local/load-test.sh` (in-cluster k8s Job runner + log capture).
- `helm/local/manifests/load-test-job.yaml` (the bot Job manifest).
- `scripts/eval-load-test.py` (threshold evaluator).
- `Makefile` targets `ci-load-test` (merge) and `ci-load-test-release` (release).

### Merge gate

- **Triggers.** `pull_request` against `main` and `experimental-sfu`, plus
  `workflow_dispatch`.
- **Job name.** `merge-gate` in `.github/workflows/load-test.yaml`.
- **Shape.** 5 senders + 45 listeners = 50 bots, 300 s steady state, 1 SFU
  replica.
- **Thresholds.** `max_loss_pct = 0.5`, `require_all_connected = true`. The
  loss budget is 5× the production 0.1% target because local k3d on a CI
  runner has more variance than production hardware (shared kernel, no
  dedicated NIC, single-node UDP loopback). The "every bot connected"
  check is the fail-fast — if 5/50 bots can't even dial the SFU, no loss
  number from the survivors is meaningful.
- **Time budget.** Workflow `timeout-minutes: 25`; conceptual target is a
  ~5 min steady-state run, but `up.sh` rebuilds the SFU and meeting-api
  images locally on the runner, which dominates wall-clock. See "CI
  runner risk" below.

### Release gate

- **Triggers.** Nightly schedule (`cron: '17 7 * * *'` UTC) and
  `workflow_dispatch`. Not run on `pull_request`.
- **Job name.** `release-gate`.
- **Shape.** 10 senders + 190 listeners = 200 bots, 300 s, 2 SFU replicas
  (exercises the cross-pod NATS room hub paths from p6-9 and the SFU
  health-beacon hub from vc-c6l).
- **Thresholds.** `max_loss_pct = 0.1` (production target), every bot
  connected, AND zero SFU pod `restartCount` deltas during the run. A
  release-gate run that survives only because a crashed SFU pod was
  recovered by the kubelet is a regression — the bot might not see the
  blip if recovery is fast, but we don't want to ship that.

### Loss metric definition

`scripts/eval-load-test.py` computes

```
loss_pct = listener_totals.drops
           / (listener_totals.packets_received + listener_totals.drops)
           * 100.0
```

over the orchestrator's `listener_totals` aggregate (listeners only). The
bot's `drops` counter is "failed-to-drain inbound unistreams" (see
`bot/src/stats.rs::BotStats::record_drop`); every accepted-but-unreadable
stream increments it. That is the available proxy for receive-side packet
loss but it does **not** distinguish audio frames from video frames and
does **not** separate transport-level errors from media-level drops.

Known limitation; tracked as follow-up bead title: **"bot: add
codec-aware loss tracking (audio vs video)"**.

### Override mechanism

Both Make targets honor env-var overrides for local tuning. The flags map
1:1 to `helm/local/load-test.sh`:

| Variable          | Default (merge) | Default (release) | Purpose                  |
|-------------------|-----------------|-------------------|--------------------------|
| `SENDERS`         | 5               | —                 | publishing bot count     |
| `LISTENERS`       | 45              | —                 | subscriber bot count     |
| `DURATION`        | 300             | —                 | steady-state seconds     |
| `MAX_LOSS_PCT`    | 0.5             | —                 | merge-gate loss budget   |
| `REPLICAS`        | 1               | —                 | SFU pod replicas         |
| `RELEASE_SENDERS` | —               | 10                | release-gate senders     |
| `RELEASE_LISTENERS` | —             | 190               | release-gate listeners   |
| `RELEASE_DURATION` | —              | 300               | release-gate duration    |
| `RELEASE_MAX_LOSS_PCT` | —          | 0.1               | release-gate loss budget |
| `RELEASE_REPLICAS` | —              | 2                 | release-gate SFU pods    |

Example 60-second smoke during dev (smaller bot count, more permissive
loss budget, no release-gate footprint):

```bash
make ci-load-test SENDERS=2 LISTENERS=8 DURATION=60 MAX_LOSS_PCT=2.0 REPLICAS=1
```

### CI runner risk

The 50-bot merge gate runs on `ubuntu-latest`, which is a 2-vCPU / 7 GiB
RAM / 14 GiB disk runner. `helm/local/up.sh` does a full `cargo build
--release` for both `Dockerfile.meeting-api` and `Dockerfile.actix` plus
the new `bot/Dockerfile` on first invocation — that dominates wall-clock
and lives at the edge of the runner's RAM budget.

If the merge gate consistently flakes (OOM kills during the build phase,
or post-startup CPU starvation inflating bot drop counters), the options
are:

1. **Reduce bot count** to 20 listeners and tighten the loss budget back
   up — keeps the gate cheap at the cost of weaker coverage.
2. **Move to a larger runner** (e.g. `ubuntu-latest-4-cores` if the org
   has access, or a self-hosted runner) — preserves the 50-bot target.
3. **Split into a build job + test job** so the SFU/bot image build can
   parallelise and cache across PRs — the cleanest long-term fix.

Flagged for the team to monitor over the first ~10 PR runs after this
lands. Do **not** silently lower the thresholds; treat flakes as a
signal to escalate to option 2 or 3.

## Open Questions

1. **Safari 18.2 VP9-L3T3_KEY decode.** Does Safari 18.2's VP9 decoder accept
   dropped-layer (`L3T3_KEY` minus top spatial) bitstreams without artifacting?
   If not, all four `VP9-L3T3_KEY × Safari` ❌ cells stay ❌ for v1 and we ship
   Safari clients on `VP9-L1T3` only.
2. **Pathological "slow" threshold.** What downlink bandwidth (or RTT/loss
   combination) declares a receiver "slow" enough to exercise the pathological
   row? Currently the integration test pins 500 kbps per PLAN.md Phase 4 exit
   criteria, but production triage needs a documented threshold.
3. **Firefox row before v1 GA?** Firefox has no WebTransport in stable as of
   v1. Do we add a `Firefox (WebSocket fallback)` browser dimension, or defer
   until WebTransport ships there?
4. **iOS Safari parity.** The matrix lists "Safari 18.2+" generically; do we
   need to split desktop Safari from iOS Safari for the pathological row,
   where mobile downlink variability is the realistic case?
5. **`bot/` headless client coverage.** The 200-bot load test uses the Rust
   `bot/` client, which does not exercise either browser's codec path. Is a
   per-browser webinar run required as a release gate, or is browser parity
   sufficiently covered by the small-group row?
