# Test Matrix — SFU Refactor

> **Status:** Skeleton. To be filled out under bead `vc-c4e.10`.
>
> **Source of truth:** [`PLAN.md` — Verification](./PLAN.md#verification).
>
> Rows are (codec, browser, shape) combinations. Cells should mark whether a
> given verification (correctness, capacity, fallback, layer dropping) is
> covered by an automated test, a manual run, or is out of scope for v1.

## Dimensions

- **Codec:** VP8, VP9 (single-layer), VP9-SVC (L1T3 / L2T3), Opus, Opus+RED
- **Browser / Client:** Chrome (desktop), Chrome (Android), Firefox (desktop),
  Safari (macOS), Safari (iOS), `bot/` headless WebTransport client
- **Shape:** Webinar (200 receivers, 10 senders), Conference (≤30 symmetric),
  1:1, Solo (1 sender, no receivers)

## Matrix

| Codec | Browser | Shape | Routing-header read | Subscription update | Layer dropping | Active speaker | Affinity redirect | Notes |
|-------|---------|-------|---------------------|---------------------|----------------|----------------|-------------------|-------|
| _TBD_ | _TBD_   | _TBD_ | _TBD_               | _TBD_               | _TBD_          | _TBD_          | _TBD_             | _TBD_ |

## Legend

- ✅ — Automated coverage (CI)
- 🧪 — Manual coverage (documented runbook)
- ⏭ — Out of scope for v1
- ❌ — Known gap; tracked as a follow-up bead

## Open Questions

_TBD — which combinations are blocked by codec/browser support (e.g., VP9-SVC
on Safari), and which are deferred to post-v1._
