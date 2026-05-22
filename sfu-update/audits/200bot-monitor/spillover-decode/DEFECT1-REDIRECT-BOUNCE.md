# DEFECT 1 — Redirect target DNS is malformed (missing namespace) → redirect never lands on the owner pod

Read-only root-cause investigation. Branch `experimental-sfu`, tip `8938518`.
Evidence: decode-verify retest at `sfu-update/audits/200bot-monitor/spillover-decode/`
(replicas=3, prod limits, 1500 listeners + 10 senders, one room).

---

## 1. Decisive root cause: **A** (the redirect DNS name is wrong). It is NOT a true bounce, and B (spillover) is a red herring for this defect.

### What the logs actually show (not a bounce)

Aggregated across all 15 listener logs (`l*.log`):

```
joined_pod distribution (final landing):
  400  [::ffff:10.42.0.68]:443   <- pod-0 (OWNER) — these 400 decoded
  600  [::ffff:10.42.1.75]:443   <- pod-1 (non-owner) — decoded 0
  500  [::ffff:10.42.2.71]:443   <- pod-2 (non-owner) — decoded 0

REDIRECT packets RECEIVED by bots:  1100
"following ... hop" log lines:      1100   (i.e. hop 1/5 only)
hop 2/5 .. hop 5/5 lines:              0
"exhausted redirect budget":           0
"could not parse redirect target":     0
```

Every stuck listener received **exactly ONE** redirect, followed it **once**, and
then **stayed put on a non-owner pod** — there is **no repeated per-listener
bounce**. The "2,105 owned-by-different-pod rejections" are not one listener
bouncing many times; they are the initial round-robin landings (≈1,100 listeners
that first hit a non-owner) plus the post-redirect landings that the SFU also
rejected. The "bounce" framing in the brief is therefore incorrect — it is a
**single misdirected hop**.

### The smoking gun (one listener's full lifecycle, `l1.log`)

```
03:14:10.955  Connecting client l1_listener-0 to https://webtransport-headless.default.svc.cluster.local/
03:14:15.073  l1_listener-0 received ADMISSION_DECISION REDIRECT to rustlemania-webtransport-0.webtransport-headless.svc.cluster.local
03:14:15.073  l1_listener-0 following ... to https://rustlemania-webtransport-0.webtransport-headless.svc.cluster.local/ (hop 1/5)
03:14:15.073  Connecting client l1_listener-0 to https://rustlemania-webtransport-0.webtransport-headless.svc.cluster.local/
   <no further redirect, no further connect — session stays open, media_received = 0>
```

Compare the two host names:

| | Host |
|---|---|
| Original `--server-url` (correct, resolvable) | `webtransport-headless.**default**.svc.cluster.local` |
| Redirect target (the SFU emits this) | `rustlemania-webtransport-0.webtransport-headless.svc.cluster.local` |

The redirect host **drops the `.default` namespace label**. The correct
StatefulSet per-pod FQDN is:

```
<pod>.<headless-svc>.<namespace>.svc.cluster.local
= rustlemania-webtransport-0.webtransport-headless.default.svc.cluster.local
```

The name the SFU emits — `rustlemania-webtransport-0.webtransport-headless.svc.cluster.local` —
is **not** a valid Kubernetes pod-DNS record. Because the bot connects with that
literal name and it is not a real per-pod A/AAAA record, the resolver falls back
(NXDOMAIN on the literal → search-domain expansion / partial match onto the
headless Service round-robin), so the QUIC session establishes against an
**arbitrary** headless endpoint — frequently a non-owner pod again. That is why
1,100 of 1,500 listeners landed (and stayed) on pods 1/2 instead of pod-0.

### Where the bug lives in code

`actix-api/src/sfu/affinity.rs:525-527` — `compute_redirect_target`:

```rust
Some(format!(
    "rustlemania-{transport_kind}-{owner}.{transport_kind}-headless.svc.cluster.local"
))
```

There is **no namespace segment** between `{transport_kind}-headless` and
`svc.cluster.local`. The doc comment (affinity.rs:497) and the unit tests
(affinity.rs:686, :693) all bake in the same namespace-less template, so the
tests pass while the production DNS name is unresolvable.

The bot side is correct and is not the cause: `bot/src/orchestrate.rs:453`
(`compute_redirect_url`) only swaps the host via `Url::set_host` and faithfully
preserves scheme/port/path. It cannot re-insert a namespace the server never
sent. The original URL did carry `.default`; the redirect host replaces the
*entire* host string, so the namespace is lost because the **server omitted it**.

### Why there is no hop 2 (and why this masquerades as "spillover not engaging")

On the post-redirect (2nd) connect the listener lands on a non-owner pod again,
but the SFU does **not** issue a second redirect. By T≈5s the owner pod-0's health
beacon (`actix-api/src/sfu/health_beacon.rs:389-395`, count = pod-0's ~410 local
members > 180) has propagated to pods 1/2, so on the 2nd `JoinRoom` the spillover
override **fires** and admits the listener locally:

- `actix-api/src/actors/chat_server.rs:1638` — `is_spilled_over(&room)` now returns `true`
- `chat_server.rs:1666-1711` — the SPILL branch admits locally, suppresses the redirect

So spillover-admit **is** wired (vc-85p) and **does** engage — that is precisely
why the listener stops getting redirected after hop 1. The defect is not "spillover
fails to engage"; it is that **the redirect that should have reached the owner
never does** (wrong DNS), and the spill-admit that catches the listener on a
non-owner pod cannot deliver media (the separate multi-pod data-plane defect noted
in `DECODE-VERIFY-FINDINGS.md`: spill-admitted listeners never receive senders'
media → AllowSet/cross-pod subscription gap). The result of THIS defect is the
400-vs-1100 split: only the listeners that happened to round-robin onto pod-0
decoded.

### Causal order

A (wrong DNS) is the **independent** root cause of the 1,100 mis-landings. B
(spillover) is *functioning* and is what stops the loop at one hop — but it lands
the listener on a pod that can't serve media. The earlier hypothesis that "the
owner count oscillates because of a bounce" is **disproved**: there is no bounce,
the beacon count is stable (~410 on pod-0), and `is_spilled_over` returns `true`
on pods 1/2 as designed.

> Net: **A causes the mis-landing. A and the separate data-plane defect together
> cause the 0-decode. B is correct and is not part of DEFECT 1.**

---

## 2. Fix spec

### Primary fix (SFU code — required, fixes local AND prod)

Insert the pod namespace into the redirect FQDN in `compute_redirect_target`
(`actix-api/src/sfu/affinity.rs:508-528`). The K8s per-pod record requires the
namespace label:

```
rustlemania-{tk}-{owner}.{tk}-headless.<namespace>.svc.cluster.local
```

Recommended implementation:
- Wire `POD_NAMESPACE` into the SFU env from the downward API
  (`fieldRef: metadata.namespace`) in **both** StatefulSet templates
  (`helm/rustlemania-webtransport/templates/statefulset.yaml` and
  `helm/rustlemania-websocket/templates/statefulset.yaml`, alongside the existing
  `POD_NAME`/`STATEFULSET_REPLICAS` at statefulset.yaml:40-45).
- Read it in affinity.rs (default `"default"` for local/dev parity, mirroring the
  `self_ordinal_from_env` fallback) and splice it into the template.
- Update the doc comment (affinity.rs:497) and unit tests (affinity.rs:686, :693)
  to assert the namespace label is present (the tests currently *lock in the bug*).

This is **SFU code (actix-api)** plus a small **helm/ops** env addition.

### Secondary fix (prod-only latent break — must fix before any non-`default`
release-name / region deploy)

The redirect template hardcodes the StatefulSet prefix `rustlemania-{transport_kind}`,
but the StatefulSet is named via `rustlemania.fullname` (helm `_helpers.tpl:13-24`),
which is `fullnameOverride` when set. In `helm/global/us-east/webtransport/values.yaml:3-4`
the override is `webtransport-us-east`, so pods are `webtransport-us-east-{ord}` and
the headless service stays `webtransport-headless`. The redirect name
`rustlemania-webtransport-0.…` would point at a non-existent StatefulSet there.

In the local k3d test the StatefulSet *happens* to be named `rustlemania-webtransport`
(see `decode-verify.sh:34`: `kubectl scale statefulset/rustlemania-webtransport`), so
this second mismatch is masked locally and only the namespace omission bit us. Fix by
sourcing the StatefulSet name from env too (e.g. a `STATEFULSET_NAME` /
`POD_NAME`-prefix-derived value) rather than hardcoding `rustlemania-{tk}`, so the
emitted FQDN tracks the actual deployment name. Otherwise the namespace fix alone
will pass the local retest but still break us-east/singapore.

> Note: `publishNotReadyAddresses` is **not** required for the steady-state path
> (pods are Ready before bots connect) but is advisable on the headless service so
> per-pod DNS resolves during rollouts/scale events — recommend adding it
> defensively.

---

## 3. Acceptance criteria

With replicas ≥ 3, prod limits, 10 senders + 1,500 listeners, one room:

1. **No mis-landing.** Listeners redirected off a non-owner reconnect to a name
   that resolves to **exactly the owner pod** (pod-0 / `10.42.0.68`). `joined_pod`
   for redirected listeners == the owner pod IP, OR the listener is spill-admitted
   locally on a non-owner pod *that can deliver media* (depends on the separate
   data-plane fix).
2. **Rejections ≈ 1 per listener.** "owned by a different pod" total ≈ the number
   of listeners that first round-robin onto a non-owner (~1,000), each redirected
   **once** and then landing on the owner — not 2,105.
3. **Decode crc=0 on all pods.** Every listener that ends on the owner (or on a
   working spill pod) decodes media with `crc_mismatches=0` and
   `media_received_distinct > 0`. This requires DEFECT 1 (this fix) **plus** the
   multi-pod data-plane defect to be resolved for the spill-admit path; if spill is
   intended to serve media on non-owner pods, those pods must subscribe senders.
4. **No hop-2+ and no budget exhaustion** remain true — but now because the single
   hop reaches the owner, not because spill-admit absorbs a mis-landing.

A focused intermediate check: assert the SFU emits
`rustlemania-webtransport-0.webtransport-headless.default.svc.cluster.local`
(namespace present) and that an in-cluster `getent hosts` / `nslookup` of that name
returns **only** pod-0's IP.

---

## 4. Bead breakdown

| Bead | Scope | Priority | Depends on |
|---|---|---|---|
| **D1-sfu-1**: Add namespace label to `compute_redirect_target` FQDN; read `POD_NAMESPACE` (default `"default"`); fix doc + tests (affinity.rs:497, :508-528, :686, :693). | SFU (actix-api) | **P0** | — |
| **D1-helm-1**: Wire `POD_NAMESPACE` (downward API `metadata.namespace`) into both StatefulSet templates (webtransport + websocket statefulset.yaml). | helm/ops | **P0** | blocks D1-sfu-1 verification |
| **D1-sfu-2**: Source StatefulSet name from env (don't hardcode `rustlemania-{tk}`) so the redirect host tracks `fullnameOverride` (us-east = `webtransport-us-east`). | SFU (actix-api) | **P1** (prod-only; not exercised by local retest) | D1-sfu-1 |
| **D1-helm-2**: Wire StatefulSet name into env (`STATEFULSET_NAME` from a Helm value or label); optionally add `publishNotReadyAddresses: true` to headless services. | helm/ops | **P1** | pairs with D1-sfu-2 |
| **D1-bot-0**: No bot change required — `compute_redirect_url` (orchestrate.rs:453) is correct. Optionally add a one-line guard/log if the redirect host lacks `.svc.cluster.local`/namespace, to fail loud next time. | bot | **P3** (optional) | — |

**Out of scope for DEFECT 1 (separate defect, already filed in
`DECODE-VERIFY-FINDINGS.md`):** spill-admitted listeners on non-owner pods receive
**0** media (AllowSet / cross-pod sender-subscription gap). DEFECT 1's fix routes
listeners to the owner where media already works for the 400 that arrived there;
full multi-pod decode on spill pods needs that data-plane bead too. The two are
independent — fixing the DNS alone will move most listeners onto the owner and
restore decode for them, but any listener that legitimately spills (owner truly
full) will still decode 0 until the data-plane bead lands.

---

## Appendix — files cited

- `actix-api/src/sfu/affinity.rs:497, :508-528, :686, :693` — the malformed FQDN template (root cause).
- `actix-api/src/actors/chat_server.rs:1635-1765` — JoinRoom ownership-redirect + vc-85p spillover override (working as designed).
- `actix-api/src/sfu/spillover.rs:147-194` — `is_spilled_over` predicate + store (working as designed).
- `actix-api/src/sfu/health_beacon.rs:341-396` — owner-pod beacon publisher (publishes ~410 for pod-0).
- `actix-api/src/webtransport/bridge.rs:116-132, :434-464` — reliable ADMISSION_DECISION delivery (vc-xnp); confirms the first redirect is delivered, not lost.
- `bot/src/orchestrate.rs:453-458` — `compute_redirect_url` (host-only swap; correct).
- `bot/src/orchestrate.rs:654-733` — listener reconnect-on-REDIRECT loop (max 5 hops; only 1 hop used).
- `helm/rustlemania-webtransport/templates/{headless-service,statefulset,_helpers.tpl}.yaml` — DNS/serviceName/fullname config.
- `helm/global/us-east/webtransport/values.yaml:3-4` — `fullnameOverride: webtransport-us-east` (secondary prod break).
- `sfu-update/audits/200bot-monitor/spillover-decode/{run.log, l*.log, decode-verify.sh, shard-pod.tmpl.yaml, DECODE-VERIFY-FINDINGS.md}` — evidence.
