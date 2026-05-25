# ADR-0005: Room-Affinity Routing (hybrid)

- **Status:** Accepted
- **Date:** 2026-05-17
- **Deciders:** overseer (malexander)
- **Related:** [ADR-0001](0001-routing-header-out-of-encryption.md) (the unencrypted `PacketWrapper` envelope that carries `ADMISSION_DECISION`), [ADR-0002](0002-active-speaker-detection.md) §8 (cross-region authority: only the owner pod runs the scorer), [ADR-0003](0003-hybrid-subscription-model.md) (room state held by the owner pod), [ADR-0004](0004-outbound-priority-queue.md) (per-pod outbound capacity envelope that motivates spillover), [`PLAN.md` Phase 6](../PLAN.md#phase-6--room-affinity-routing--capacity-validation-35-days), [`PLAN.md` New Wire Surface](../PLAN.md#new-wire-surface-consolidated), [`PLAN.md` Convoy P6](../PLAN.md#convoy-p6--room-affinity--capacity-validation), [`PLAN.md` Open Risk #2](../PLAN.md#open-risks-escalate-before-each-phase) (cross-region cost), [`PLAN.md` Open Risk #7](../PLAN.md#open-risks-escalate-before-each-phase) (cross-region speaker consistency), [`capacity-model.md` §3](../capacity-model.md#3-per-pod-outbound-binding-constraint), [`capacity-model.md` §4a](../capacity-model.md#4a-multi-pod-fanout-v1-default), [`capacity-model.md` §5](../capacity-model.md#5-nats-bandwidth), [`packet-diagrams.md`](../packet-diagrams.md), [`rfc-2-sfu-architecture.md`](../../rfc/rfc-2-sfu-architecture.md), bead `vc-c4e.7`.

## Context

The SFU forwards only within a single pod. For a meeting to fan out across
pods (egress is the binding constraint — see [`capacity-model.md`](../capacity-model.md))
all participants of a room must land on the same pod, or pods must relay to
each other. v1 chooses the former: **room affinity**.

Approaches considered:

1. **Pure consistent hashing** on `room_id` — deterministic, but cross-region
   clients pay a fixed RTT penalty (~250 ms) and capacity is room-size-bound.
2. **Pure dynamic placement** — best load balance, complex coordination, hard
   to keep stable across rolling deploys.
3. **Hybrid** (proposed): each region's StatefulSet hashes locally on
   `room_id` to pick a home pod; the first joiner's region sets the room's
   home region; out-of-region joiners are redirected via
   `ADMISSION_DECISION` (`PacketType = 13`).

This ADR captures the hashing scheme, the home-region election, the redirect
protocol, and the failover behavior when the home pod dies mid-meeting.

## Decision

**Within each region, `jump_hash(room_id_hash, replicas)` selects a single owner pod from the WebTransport/WebSocket StatefulSet; the first joiner's region becomes the room's home region; out-of-region or wrong-pod arrivals receive an `ADMISSION_DECISION` redirect on the unencrypted `PacketWrapper` envelope and reconnect to the named pod; spillover under load is gated by NATS health beacons on `room.{room}.system`; pod death is handled by StatefulSet restart with re-election of room state on the same ordinal.**

Concretely:

1. **Hashing scheme — Lamping–Veach jump consistent hash.** Owner pod selection within a region is

   ```rust
   // actix-api/src/sfu/affinity.rs (p6-1)
   pub fn owner_ordinal(room_id: &str, replicas: u32) -> u32 {
       let key = xxhash_rust::xxh64::xxh64(room_id.as_bytes(), 0);
       jump_hash(key, replicas)
   }
   ```

   `jump_hash` is the algorithm from Lamping & Veach 2014 ("A Fast, Minimal Memory, Consistent Hash Algorithm", [arXiv:1406.2294](https://arxiv.org/abs/1406.2294)): O(ln N) time, O(1) memory, *zero allocation*, and — its load-bearing property here — when `replicas` changes from `N` to `N+1`, exactly `k/(N+1)` keys move and no others. This is the minimal-disruption property we want during rolling deploys: adding a pod reshuffles only the fraction of rooms that *must* move to balance, not 2× that fraction (Ketama) and not all of them (modulo hashing). The input hash is xxhash64 of the room id (xxhash is already in the workspace dependency closure, is non-cryptographic, and produces a uniform 64-bit key — we don't need cryptographic resistance because the input `room_id` is not adversarial in this code path). The output `pod_ordinal` is in `[0, replicas)` and is interpreted as the StatefulSet ordinal of the owner pod.

   Why not Ketama / rendezvous hashing: see Rejected alternative A. Why a hash at all rather than dynamic placement: see Rejected alternative B.

2. **Pod identity from the K8s downward API.** Each pod knows two facts about itself, both injected at startup via the [downward API](https://kubernetes.io/docs/concepts/workloads/pods/downward-api/):

   ```yaml
   # helm/rustlemania-webtransport/templates/statefulset.yaml (p6-2, p6-4)
   env:
     - name: POD_NAME
       valueFrom: { fieldRef: { fieldPath: metadata.name } }   # e.g. "webtransport-2"
     - name: STATEFULSET_REPLICAS
       value: "{{ .Values.replicaCount }}"                      # rendered at helm template time
     - name: REGION
       value: "{{ .Values.region }}"                            # e.g. "us-east1"
   ```

   `POD_NAME` is parsed for its trailing ordinal (`webtransport-2` → `2`). `STATEFULSET_REPLICAS` is the current replica count rendered at Helm template time; it is re-rendered on every `helm upgrade`, so a scale-up restarts pods with the new value and the affinity function returns new owners deterministically. The Deployment → StatefulSet migration ([p6-2](../PLAN.md#convoy-p6--room-affinity--capacity-validation), [p6-3](../PLAN.md#convoy-p6--room-affinity--capacity-validation)) is what makes the ordinal stable across pod restarts — the load-bearing difference between Deployment (random pod name, identity dies with the pod) and StatefulSet (`{name}-{N}`, identity outlives the pod). It also gives us stable per-pod DNS: `webtransport-{N}.webtransport-headless.svc.cluster.local:443` resolves to the same pod across restarts, which is exactly what the redirect target needs to name.

3. **Owner mismatch redirect — `ADMISSION_DECISION = 13`.** When a client connects to pod `P`, the pod authenticates the connection, parses the room id from the join, computes `expected = owner_ordinal(room_id, replicas)`, and if `expected != self.ordinal` it emits a single `ADMISSION_DECISION` on the `PacketWrapper` envelope and closes the transport. The new packet type is reserved in [`PLAN.md` New Wire Surface](../PLAN.md#new-wire-surface-consolidated):

   ```protobuf
   // protobuf/types/admission.proto (p6-5)
   enum AdmissionReason {
     ADMISSION_REASON_UNSPECIFIED = 0;
     NOT_OWNER        = 1;   // hash mismatch, redirect to owner in same region
     REGION_REDIRECT  = 2;   // home region differs from connect region
     SPILLED          = 3;   // owner is over soft cap; redirected to a spill pod
   }
   message AdmissionDecision {
     AdmissionReason reason   = 1;
     string redirect_to       = 2;   // FQDN+port, e.g. "webtransport-2.webtransport-headless.svc:443"
     string home_region       = 3;   // optional; populated on REGION_REDIRECT
     uint32 ttl_redirects     = 4;   // server hint: client should not chase beyond this many hops
   }
   ```

   `ADMISSION_DECISION` rides the unencrypted `PacketWrapper` envelope per [ADR-0001](0001-routing-header-out-of-encryption.md) — there is no media payload, only routing metadata, and a client that doesn't yet know the E2EE group key must still be able to parse it. The server emits it *immediately* after auth + room-name parse, before any media subscription state is allocated, so the wrong-pod path is cheap.

4. **Client redirect handling — bounded retry in `ConnectionManager`.** The client's RTT election (`videocall-client/src/connection/connection_manager.rs:182-219`) is **unchanged**: clients still elect the lowest-RTT pod from the pool on cold join. Affinity redirects happen *after* RTT election. On receipt of `ADMISSION_DECISION` (p6-6), the manager:

   - Reads `redirect_to`, bypasses the RTT pool for this single attempt, and reconnects directly to the named FQDN+port.
   - Tracks a `redirect_hops` counter on the current join attempt; aborts with a surfaced error after `>= 2` hops on the same room id within a 30 s window. This breaks loops if hash state is briefly inconsistent across pods during a rolling deploy (e.g., one pod sees `replicas=3`, the next sees `replicas=4`).
   - Persists `home_region` from a `REGION_REDIRECT` for the lifetime of this room id on this client, so the next join of the same room skips the cross-region first hop (see §7 below).
   - On `SPILLED`, treats the redirect target as authoritative for this connection only; does *not* cache it as "the owner for this room" because spill targets are dynamic.

5. **Home region election — first-joiner-wins, with deterministic tiebreaker.** A room has exactly one home region. The first joiner's pod claims it. Mechanism (no central coordinator, no etcd):

   - On the first join the owner pod sees for a room id, before admitting the client, the owner publishes `RoomBirth { home_region, owner_pod, room_id, birth_ts }` on `room.{room_id}.system` (the existing room-wide control subject used for `SpeakerUpdate` per [ADR-0002](0002-active-speaker-detection.md) §6 and for the health beacons in §6 below).
   - Other-region owner pods that subsequently receive a first joiner for the same room id check their NATS-local cache (populated by the same `room.{room_id}.system` subscription) for an existing `RoomBirth`. If present, they emit `ADMISSION_DECISION { reason: REGION_REDIRECT, redirect_to: "webtransport-{owner_pod}.webtransport-headless.{home_region}.svc:443", home_region }` and close.
   - The race window is the cross-region NATS RTT: ~5–20 ms within a region (negligible relative to the time between human-driven joins of the same room) and up to ~250 ms cross-region (relevant only when two participants in two different regions try to create the same room within that window). When two `RoomBirth` messages collide, the tiebreaker is **lexicographically smallest region name wins**; the loser's owner publishes a corrective `RoomBirth { home_region = winner }` and redirects its joiner to the winner. The tiebreaker is deterministic, so both losers and winners converge without further messages.

   Why NATS-as-truth and not a central registry: see Rejected alternative C. Why this is acceptable given the race window: in a 200-participant webinar the first joiner predates the second by seconds-to-minutes in practice, not milliseconds, and the cost of a single corrective redirect on the rare colliding-first-joiner case is one extra reconnect, not a data-plane disturbance.

6. **Spillover protocol — NATS health beacons, soft-cap-only.** The owner pod publishes a health beacon every 5 s on `room.{room}.system`:

   ```protobuf
   message RoomHealthBeacon {
     uint32 pod_ordinal       = 1;
     uint32 participant_count = 2;
     uint32 cpu_load_pct      = 3;     // 0..100
     bool   marked_spilled    = 4;     // true iff over soft cap
     uint64 beacon_ts_ms      = 5;
   }
   ```

   Thresholds (locked in [`PLAN.md` Phase 6](../PLAN.md#phase-6--room-affinity-routing--capacity-validation-35-days)): `participant_count > 180` **or** `cpu_load_pct > 80` flips `marked_spilled = true` in the next beacon. Other pods *in the same region* that have a new joiner inbound for this room observe `marked_spilled` and accept the joiner locally rather than redirecting via `ADMISSION_DECISION { SPILLED, ... }` to themselves; they then federate over NATS exactly as they already do — spill pods subscribe to `room.{room}.*` and the forwarder fans out to local receivers transparently.

   Soft-cap exit: spillover *only* relaxes admission for **new joiners**. Existing connections to the owner stay put — we do not migrate live sessions. Once the owner's participant count drops back under 180 and CPU drops back under 80 for **two consecutive beacons** (10 s), `marked_spilled` clears and subsequent joiners route to the owner normally. The two-beacon damping is what prevents flapping at the threshold.

   Capacity sizing per [`capacity-model.md` §4a](../capacity-model.md#4a-multi-pod-fanout-v1-default): the webinar shape's 1.76 Gbps egress needs `ceil(1.76 / 0.8) ≈ 3` pods @ 800 Mbps headroom. The 180-participant soft cap is the per-pod budget that arrives at that pod count when a 200-participant room saturates.

   Why beacons over NATS and not a central coordinator: pods already share a NATS plane, beacons are tens of bytes at 0.2 Hz per room (negligible in [`capacity-model.md` §5](../capacity-model.md#5-nats-bandwidth)), and the system has no other shared-state dependency we'd want to take on. See Rejected alternative C.

7. **Cross-region redirect and `region_hint`.** Out-of-region joiners receive `ADMISSION_DECISION { reason: REGION_REDIRECT, redirect_to: "webtransport-{owner_ordinal}.webtransport-headless.{home_region}.svc:443", home_region: "us-east1" }` and reconnect. To avoid paying the RTT-election → cross-region first-hop penalty on every subsequent join of the same room, the client may pass `?region_hint=us-east1` (or set a session-scoped `X-Region-Hint` header on WebSocket) on the next connect; the RTT election then biases toward pods in that region first. This is an optimisation, not a correctness mechanism: a wrong hint just falls through to the normal RTT pool, and the server-side affinity check is unconditional.

   Per [`PLAN.md` Open Risk #2](../PLAN.md#open-risks-escalate-before-each-phase): cross-region bandwidth at 30% remote mix runs ~$200/hr per active webinar. v1 *pins* rooms to home region — out-of-region participants pay the latency penalty, the SFU never splits a single room's media plane across regions. Splitting rooms across regions with media relay is Rejected alternative E.

8. **Failover behavior — StatefulSet restart, room state reset.** Owner pod dies → K8s restarts the StatefulSet pod with the **same ordinal** (the load-bearing property of StatefulSet vs. Deployment from §2). Existing client connections to the owner see the WebTransport/WebSocket close and reconnect via the existing `connection-closed` handlers (`videocall-client/src/connection/connection_manager.rs`). The reconnect lands on the same FQDN, which now resolves to the freshly-started pod.

   The new pod has empty room state. The first joiner re-elects the home region the same way as §5 — but because the pre-existing `RoomBirth` was an in-memory artifact of the dead pod, there is briefly no `RoomBirth` on `room.{room}.system` until the new pod publishes one. Spill pods that were already serving the room continue to do so (their state survives because *they* didn't die), and the new owner's first beacon will re-anchor the room.

   User-visible effect: **brief media stall, no rejoin UX**, layer-selection and speaker-detection state reset to defaults on the new owner. Receivers see a ~5–15 s downtime ([`PLAN.md` Verification §7](../PLAN.md#verification)), then video resumes with the speaker set rebuilding from EWMA cold-start ([ADR-0002](0002-active-speaker-detection.md) §1). This is the v1 contract; zero-downtime failover via room-state replication is Rejected alternative D.

9. **Capability flag interaction.** `ADMISSION_DECISION` is in the unencrypted `PacketWrapper` envelope ([ADR-0001](0001-routing-header-out-of-encryption.md)) and is parseable by any client that speaks the current `PacketWrapper` proto, regardless of whether the client advertises `SFU_ROUTING_HEADER` capability. Legacy clients that don't *handle* `ADMISSION_DECISION` simply see the connection close and reconnect through the RTT pool — statistically likely to land on the same wrong pod again. A small fraction of legacy-client joins will therefore need 2–3 reconnect cycles before hitting the owner, especially in deployments with `replicas >= 4`. Documented degradation; not a correctness bug.

   A future capability bit `SFU_AFFINITY_REDIRECT` could let the server skip emission for clients that won't act on it (close-only), but it's deferred for v1 — the cost of sending a `ADMISSION_DECISION` to a legacy client is one small packet, not worth the bit.

10. **Observability.** The following Prometheus metrics are added in `actix-api/src/metrics.rs` (mirroring the style of [`PLAN.md` Open Risk #5](../PLAN.md#open-risks-escalate-before-each-phase)):

    - `sfu_admission_redirect_total{reason}` — counter, labels `NOT_OWNER | REGION_REDIRECT | SPILLED`. A redirect storm during a rolling deploy is the headline regression signal.
    - `sfu_room_owner_pod{room_id}` — gauge, value is the pod ordinal. Cardinality is bounded by active rooms (typically O(10²)); acceptable.
    - `sfu_pod_participant_count` — gauge, per-pod.
    - `sfu_pod_cpu_load_pct` — gauge, per-pod (read from `/proc/stat` or `cgroup` accounting).
    - `sfu_spillover_active_rooms` — gauge, count of rooms where this pod is currently `marked_spilled`.
    - `sfu_jump_hash_compute_us` — histogram. The function is single-digit microseconds even on cold cache; the histogram exists so a regression in the dependency (xxhash, integer math) shows up before it matters in production.

11. **Implementation locus.** Each numbered decision above maps to a [PLAN.md Convoy P6](../PLAN.md#convoy-p6--room-affinity--capacity-validation) bead. See the Implementation section for the keyed checklist.

## Consequences

**Pro:**

- **Deterministic routing.** Given the same `room_id` and `replicas`, every pod, every client, every operator computes the same owner. Debugging "which pod is this room on?" is a single `jump_hash` call away.
- **NATS unchanged at the data plane.** All affinity decisions live in the connect path and on `room.{room}.system`; the per-packet hot path (`session_logic.rs`) doesn't change. The capacity arithmetic in [`capacity-model.md` §3](../capacity-model.md#3-per-pod-outbound-binding-constraint) is preserved verbatim.
- **Preserves E2EE.** `ADMISSION_DECISION` carries only routing metadata in the clear, exactly as the per-packet `RoutingHeader` does per [ADR-0001](0001-routing-header-out-of-encryption.md). No payload access, no membership in the E2EE group.
- **StatefulSet gives stable hostnames.** `webtransport-{N}.webtransport-headless.svc.cluster.local` is the same pod after a restart. Redirects can name a pod by FQDN without coordinating through service discovery.
- **No central coordinator.** No etcd, no consul, no Postgres-as-room-registry. NATS is the only shared dependency, and we already depend on it.
- **Jump hash is cheap.** O(ln N) integer math, zero allocation, single-digit microseconds — fast enough to call on every connect without caching, simple enough to audit.
- **Minimal-disruption rolling deploys.** A `replicas` change from `N` to `N+1` reshuffles exactly `k/(N+1)` rooms. At `N=3` that's ~33% of rooms moving — still painful, but the best property a consistent hash can give us.
- **Sized for the binding constraint.** The 180-participant soft cap is the per-pod budget that hits ~800 Mbps egress, which is the headroom the capacity model is built around ([`capacity-model.md` §4a](../capacity-model.md#4a-multi-pod-fanout-v1-default)). The threshold isn't arbitrary; it's the binding constraint expressed as a count.

**Con:**

- **Rolling deploys reshuffle rooms.** A `replicas` change from `N` to `N+1` moves ~`k/(N+1)` rooms to new owners. Affected clients see one extra `ADMISSION_DECISION` round trip. At `N=3 → 4` that's ~25% of rooms each paying one extra hop — acceptable but documented. There is no graceful "drain old owner" path in v1.
- **Home-region race needs a tiebreaker.** When two cross-region clients race to be the first joiner, the lexicographic tiebreaker resolves the conflict but can briefly bounce one of them through a corrective `REGION_REDIRECT`. Visible to the loser only, only on the genuine first-joiner-collision case, and one extra reconnect.
- **Legacy clients without redirect handling take 2–3 reconnects.** They close, retry, statistically hit the same wrong pod, close, retry, eventually land on the owner. Not a correctness bug; cost is a measurable but small uptick in `sfu_admission_redirect_total{reason="NOT_OWNER"}` for legacy-mix rooms.
- **Pod death = 5–15 s receiver downtime.** Worse than zero-downtime active-active. Acceptable for the webinar shape per [`PLAN.md` Verification §7](../PLAN.md#verification) but a known regression from the legacy NATS-fanout topology, which had no single-pod dependency.
- **StatefulSets are harder to operate than Deployments.** No rolling-restart-with-bigger-pool trick; `helm upgrade` rolls one ordinal at a time and a stuck pod stalls the rollout. Pod ordinal recycling is the property we want, but it means operational muscle memory built around Deployments has to be retrained.
- **Cross-region pinning leaves money on the table.** Out-of-region participants pay ~250 ms RTT for the entire meeting. Splitting media across regions would lower that but at the cost noted in [`PLAN.md` Open Risk #2](../PLAN.md#open-risks-escalate-before-each-phase) (~$200/hr at 30% remote mix). v1 keeps the money; revisits at scale.
- **Spillover relaxes admission *only* for new joiners.** Existing connections to the owner stay put even as the room spills. We do not migrate live sessions — that would be Rejected alternative D wearing a different hat — but it means that once the owner is hot, it stays hot until a meaningful number of receivers leave.

**Mitigations / things this ADR explicitly does NOT do:**

- Does **not** split a single room across regions. v1 pins. See Rejected alternative E.
- Does **not** stand up a central room registry (consul/etcd/Postgres). NATS is the source of truth, with all the eventual-consistency caveats explicit in §5–§6. See Rejected alternative C.
- Does **not** build a DHT, gossip protocol, or Raft for room state. We already have NATS; we don't pay for a second coordination layer.
- Does **not** implement pod-to-pod relay or room-state replication on owner death. The room state is genuinely lost on pod death and rebuilt from scratch as receivers reconnect. See Rejected alternative D.
- Does **not** migrate live sessions during spillover. Spillover affects only new joiners' admission decisions; established connections stay on their pod.
- Does **not** add an `SFU_AFFINITY_REDIRECT` capability bit in v1. Legacy clients eat the extra reconnects; a capability bit would be a future ADR if the legacy-client mix turns out to dominate the redirect metric.
- Does **not** terminate WebTransport at an L7 ingress. See Rejected alternative F.

## Implementation

- [ ] `actix-api/src/sfu/affinity.rs` — `jump_hash` (Lamping–Veach), `owner_ordinal(room_id, replicas) -> u32`, unit tests over a range of `replicas` to verify minimal-disruption property (`p6-1`).
- [ ] `helm/rustlemania-webtransport/templates/statefulset.yaml` — migrate WebTransport Deployment → StatefulSet; headless service; stable per-ordinal DNS (`p6-2`).
- [ ] `helm/rustlemania-websocket/templates/statefulset.yaml` — migrate WebSocket Deployment → StatefulSet to match (`p6-3`).
- [ ] `actix-api/src/bin/webtransport_server.rs`, `actix-api/src/bin/websocket_server.rs` — read `POD_NAME`, parse ordinal, read `STATEFULSET_REPLICAS`, read `REGION` from the K8s downward API at boot (`p6-4`).
- [ ] `protobuf/types/admission.proto` + `actix-api/src/actors/chat_server.rs` (around `JoinRoom` at `chat_server.rs:560-765`) — emit `ADMISSION_DECISION` on owner mismatch; `PacketType = 13` per [`PLAN.md` New Wire Surface](../PLAN.md#new-wire-surface-consolidated) (`p6-5`).
- [ ] `videocall-client/src/connection/connection_manager.rs` — handle inbound `ADMISSION_DECISION`, bounded redirect retry, persist `home_region` for the room id, `?region_hint` URL param plumbing (`p6-6`, `p6-9`).
- [ ] `actix-api/src/sfu/affinity.rs` — owner-pod periodic `RoomHealthBeacon` emission every 5 s on `room.{room}.system`; `RoomBirth` emission on first-joiner admission (`p6-7`).
- [ ] `actix-api/src/sfu/affinity.rs` + `actix-api/src/actors/chat_server.rs` — spillover acceptance: pods in the same region accept new joiners when the owner's last beacon has `marked_spilled = true`; two-beacon damping on exit (`p6-8`).
- [ ] `actix-api/src/sfu/affinity.rs` — cross-region election: `RoomBirth` race + lexicographic tiebreaker; `REGION_REDIRECT` emission (`p6-9`).
- [ ] `actix-api/src/metrics.rs` — `sfu_admission_redirect_total{reason}`, `sfu_room_owner_pod`, `sfu_pod_participant_count`, `sfu_pod_cpu_load_pct`, `sfu_spillover_active_rooms`, `sfu_jump_hash_compute_us` (per [`PLAN.md` Open Risk #5](../PLAN.md#open-risks-escalate-before-each-phase)).
- [ ] `bot/` — extend headless WebTransport client to drive a 200-bot load test: `--room R --senders 10 --listeners 190 --duration 300s`; assertions on per-pod participant count and redirect-storm metrics (`p6-10`).
- [ ] `bot/` + `e2e/` — pod-kill failover test; assert `<15 s` receiver downtime after `kubectl delete pod webtransport-{owner}` (`p6-11`).
- [ ] `sfu-update/capacity-model.md` — update §4a and §5 with measured per-pod egress and NATS fanout from the 200-bot run (`p6-12`).
- [ ] CI: nightly 200-bot run as release gate; 50-bot 5-minute smoke as merge gate (`p6-13`).

Phase 6 closes the v1 SFU work. Beads `p6-2` and `p6-3` (StatefulSet migrations) block `p6-4` (downward-API wiring); `p6-1` blocks `p6-5` and `p6-7`; `p6-5` blocks `p6-6`; `p6-7` blocks `p6-8`; `p6-5`/`p6-6`/`p6-8` all block `p6-10`; `p6-10` is the convoy close gate for `p6-11`/`p6-12`/`p6-13`. See the [Convoy P6 DAG](../PLAN.md#convoy-p6--room-affinity--capacity-validation).

## Rejected alternatives

**Alternative A — Ketama or rendezvous hashing instead of jump hash.** Both are consistent-hash algorithms with similar minimal-disruption properties. **Rejected** because (a) Ketama allocates a ring of `N × virtual_nodes` entries (typical `virtual_nodes = 160`, so `O(N · 160)` memory for a 3-pod cluster is tiny but non-zero, and the ring must be re-sorted on every `replicas` change), (b) rendezvous hashing is `O(N)` per lookup vs. jump's `O(ln N)`, and (c) jump hash has provably optimal key movement (`k/N` keys move on a `+1` replica change, the theoretical minimum). For `replicas ∈ [1, 16]` the practical difference is negligible, but jump hash is the simplest of the three to audit (single page of integer math, no ring data structure) and the original Google paper's proofs are tighter. We're not arguing against Ketama for caches with thousands of nodes; we're arguing for jump in a single-StatefulSet pod count we expect to stay under 20.

**Alternative B — Pure dynamic placement (LRU pod, no hash).** Each pod tracks load; clients query a coordinator for "least-loaded pod for room X" or are assigned by a layer-7 ingress with load-aware policy. **Rejected** for three reasons. First, [`capacity-model.md` §4a](../capacity-model.md#4a-multi-pod-fanout-v1-default) explicitly assumes per-room pinning to size pod count (`ceil(1.76 / 0.8) ≈ 3`); dynamic placement would scatter receivers across pods and force NATS fanout into the binding-constraint position. Second, clients lose the ability to reason about reconnects ("which pod will I land on?") which complicates the existing RTT election. Third, dynamic placement requires a coordinator (Alternative C's problem) or layer-7 load awareness (Alternative F's problem). The combination of "deterministic hash + spillover beacons" is dynamic-enough for v1 and stays inside the constraints the capacity model already validated.

**Alternative C — Central room registry (consul / etcd / Postgres).** Persist the `(room_id → owner_pod, home_region)` mapping in a CP store; pods read on every connect. **Rejected** because the operational surface is enormous relative to the value: we'd add a new dependency, a new failure mode (registry partition or outage takes the SFU offline for all new joins), a new latency on the connect path (read-from-registry on every join, vs. zero-network `jump_hash`), and a new consistency model to reason about (read-after-write semantics during pod restart). NATS-as-eventual-consistency for `RoomBirth` and `RoomHealthBeacon` carries known races (§5, §6) but they're bounded, deterministic, and contained to the connect path. At the room scales v1 targets (O(10²) concurrent rooms), the eventual-consistency window is not user-visible.

**Alternative D — Pod-to-pod relay / room-state replication on owner death.** Replicate room state (participant list, speaker EWMA, layer state, subscription tables) to a hot standby pod via Raft or NATS-JetStream so that owner death triggers an immediate failover without receiver downtime. **Rejected** because we'd be building a small Raft for room state — including conflict resolution for the EWMA scorer, the AllowSet ([ADR-0003](0003-hybrid-subscription-model.md)), and the priority queue ([ADR-0004](0004-outbound-priority-queue.md)) — and the operational cost is well past the v1 product shape. Receivers already reconnect on any connection close (existing client behaviour, unchanged); reconnect is the failover path, and the 5–15 s downtime is acceptable for webinar shape. The decision can be revisited if real-world usage shows pod death is frequent enough to matter, but at v1 our pod-death rate is rare-restart-during-rolling-deploy, not unexpected-crash.

**Alternative E — Cross-region room split with media relay between regions.** Split a single room across regions, with one pod per region owning the local participants and inter-region relay over NATS or a dedicated channel. **Rejected for v1** because [`PLAN.md` Open Risk #2](../PLAN.md#open-risks-escalate-before-each-phase) calls out the cost: at a 30% remote-participant mix, cross-region bandwidth runs ~$200/hr per active webinar — order of magnitude larger than the within-region egress cost. v1 pins; out-of-region clients pay the latency, not the operator. If product demand shifts (genuinely-global rooms, customers willing to pay for low cross-region latency), this becomes a follow-up ADR after v1 ships.

**Alternative F — Layer-7 ingress hashing (nginx hash on cookie/header).** Push affinity into the ingress layer: nginx (or HAProxy, or an Envoy filter) hashes the `room_id` from a cookie or header and pins the connection to a backend pod. **Rejected** because (a) ingress doesn't see `room_id` until after the WebTransport CONNECT body is parsed, and ingresses don't typically peek inside WebTransport streams, (b) WebTransport goes pod-direct via Cloudflare/whatever-edge in the current deployment — it doesn't route through nginx as L7 — so this would require terminating WebTransport at the edge, which we are not doing in v1, and (c) even for the WebSocket path where the room id *could* be in a query parameter, layer-7 hashing puts a critical SFU correctness invariant into an operationally-fragile component (a misconfigured nginx hash bucket becomes "rooms randomly land on wrong pods"). Server-side affinity in Rust is exactly as fast as layer-7 hashing and lives in code we already own and test.

## Status

**Accepted** 2026-05-17. The hashing scheme (Lamping–Veach jump hash on `xxhash64(room_id)`), spillover thresholds (180 participants / 80% CPU, two-beacon exit damping), beacon cadence (5 s), redirect mechanism (`ADMISSION_DECISION` = `PacketType 13`), home-region election (NATS `RoomBirth` with lexicographic tiebreaker), and failover model (StatefulSet restart with room-state reset) are the v1 defaults. The soft-cap thresholds are tunable via `SfuConfig` without an ADR change. Algorithmic changes (different hash family, room-state replication on death, cross-region room split, layer-7 hashing) require a new ADR. Supersedes nothing. Superseded by: none.
