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
