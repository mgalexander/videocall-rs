# NATS ACL Audit (S-P0-4)

**Scope.** Audit the current NATS deployment + connection model used by the videocall-rs SFU plan, and answer: *if a single workload inside the Kubernetes cluster is compromised, what's the blast radius via NATS?*

**Reviewer.** Single-pass, 2026-05-15.

**Verdict.** NATS is currently configured with **no authentication and no subject ACLs** in both US-East and Singapore deployments. Inside-cluster compromise is a meeting-content disclosure event. The cross-region gateway uses NodePort + no auth, which is meeting-content disclosure for anyone who can reach the K8s worker IPs on port 30722 (firewall scope unknown to this audit). For the SFU refactor, this is not a regression — pre-refactor and post-refactor have the same exposure — but the refactor is a natural moment to fix it, because the new packet types introduce additional sensitive content (`audio_level`, speaker rankings) on the same bus.

---

## 1. Findings

### F-1. NATS server has authentication disabled (both regions). Severity: HIGH

> **Source:** `helm/global/us-east/nats/values.yaml:19-20`, `helm/global/singapore/nats/values.yaml:19-20`:
>
> ```yaml
> auth:
>   enabled: false
> ```

There is no user/password, no TLS client cert, no NKey, no JWT-based auth on the NATS bus. Any client that can reach the server socket on port 4222 can connect and subscribe to any subject.

### F-2. NATS client connects with TLS disabled and no credentials. Severity: HIGH

> **Source:** `actix-api/src/bin/webtransport_server.rs:47-52`:
>
> ```rust
> let nats_client = async_nats::ConnectOptions::new()
>     .require_tls(false)
>     .ping_interval(std::time::Duration::from_secs(10))
>     .connect(&nats_url)
>     .await
> ```

Plaintext on the wire (within the cluster's pod-to-pod network), no client identity asserted. Same pattern in `chat_server.rs:845, 908, 960, 997`, `webtransport/mod.rs:423`, `bin/websocket_server.rs:275`, `bin/metrics_server.rs:661`, `bin/metrics_server_snapshot.rs:397`, and `ws_chat_session.rs:329`.

### F-3. NATS service is `ClusterIP` (intra-cluster only). Severity: MEDIUM (partial mitigation)

> **Source:** `helm/global/us-east/nats/values.yaml:39-48`:
>
> ```yaml
> service:
>   name: nats-us-east
>   type: ClusterIP
> ```

`ClusterIP` is not externally exposed via K8s. Combined with F-1/F-2, this means the threat model is **intra-cluster adversary** — a compromised pod (any pod, not just videocall-rs pods), a malicious sidecar, a CI runner sharing the cluster, a leaked kubeconfig with `exec` rights — can connect to NATS and tap every room. External attackers cannot reach NATS directly.

The current SFU is a brownfield deployment; this finding is **not** introduced by the refactor. It is the existing baseline and the refactor inherits it.

### F-4. Cross-region gateway is NodePort with no auth. Severity: HIGH

> **Source:** `helm/global/us-east/nats/values.yaml:28-36`:
>
> ```yaml
> gateway:
>   enabled: true
>   port: 7222
>   name: "us-east-1"
>   gateways:
>     - name: "singapore"
>       urls:
>         - "nats://10.110.0.2:30722"
> ```
>
> Singapore's mirror config points back at `10.100.0.2:30722`.

NodePort `30722` is open on every K8s worker node in each region. The IPs in the config are RFC1918 (DigitalOcean private network), which is good — but anything that can route to those /16s can speak the NATS gateway protocol. The gateway has no auth (`nats://` not `tls://`, no creds in the URL). A peer who can route to either side's `:30722` can join the NATS supercluster as a "neighbor" and consume the full subject space across both regions.

**Open question for the user:** are DigitalOcean droplets' private networks isolated to a single account/team, or shared across the region's customers? If shared, this is internet-exposed without auth. If single-account, it's same exposure as ClusterIP — i.e., F-3 applies cross-region.

### F-5. No subject ACLs. Severity: MEDIUM (because F-1 supersedes it)

NATS supports per-credential subject ACLs (publish allowlist, subscribe allowlist). The current config has none. Even if F-1 is fixed by enabling auth, the bus is still flat — a credential with rights at all has rights everywhere.

The SFU plan's NATS subjects look like:
- `room.{room_id}.{session_id}` — per-sender media + control fanout
- `room.{room_id}.system` — meeting events (PARTICIPANT_*, MEETING_STARTED, in P3 also SPEAKER_UPDATE)

A reasonable ACL model:
- **SFU pod credential** (used by webtransport-server + websocket-server): publish on `room.>`, subscribe on `room.>`.
- **Metrics pod credential**: subscribe on `metrics.>` (or wherever metrics are published) only; no `room.*` rights.
- **meeting-api credential**: publish on `room.*.system` only (for admission decisions); no per-session subjects.
- **External clients**: no NATS credentials. Clients reach the SFU via WebTransport/WebSocket, not NATS.

### F-6. JetStream disabled (no persistence). Severity: INFORMATIONAL

> **Source:** `helm/global/us-east/nats/values.yaml:10-18`. `jetstream.enabled: false`; `memStore.enabled: true; maxSize: 1Gi`.

NATS is in-memory only. Messages are not persisted to disk. This means a leaked credential cannot exfiltrate historical meetings — only meetings in flight when the credential is active. Mild upside.

### F-7. `natsbox` is enabled in both regions. Severity: LOW

> **Source:** `values.yaml:4-5`. `natsbox.enabled: true`.

`natsbox` is the NATS debug helper container — a `nats-box` image with the `nats` CLI. It has the same credentials as the server (which are none, per F-1). If `natsbox` is reachable via `kubectl exec`, anyone with cluster `exec` rights gets a one-step on-ramp to subscribe to every room. **Disable in production.**

### F-8. NATS cluster mode is disabled. Severity: INFORMATIONAL

> **Source:** `cluster.enabled: false` with `replicas: 3` (US-East) and `replicas: 2` (Singapore).

Despite `replicas` being set, `cluster.enabled: false`. This means each region runs a **single** NATS server, not a clustered one. If that single server dies, the whole region's meeting plane loses coordination until pod restart. Not a security issue, but operationally fragile. Worth flagging in P6 (room-affinity routing) which assumes cross-region coordination works.

### F-9. No NATS-side rate limiting. Severity: LOW

A misbehaving SFU pod can flood NATS with messages. No per-credential connection limits or publish rate limits configured. With F-3 + F-4, internal abuse only — not external.

---

## 2. Recommended remediation order

Because F-1/F-2 are the root cause and everything else stacks on top:

### Phase: lift admission ACLs (recommended **before P1 closes**)

1. **Enable basic auth.** Generate a single shared NATS user/password, store in K8s Secret. Update both region values.yaml: `auth.enabled: true`, add `users:` block. Update the `actix-api` `ConnectOptions::new()` to use `with_user_and_password(...)` reading from env. **Impact:** any non-credentialed connection now refused.
2. **Enable TLS on the client port.** Add a server cert (cert-manager already in `helm/cert-manager/`); flip `require_tls(false)` → `require_tls(true)` and provide CA cert. **Impact:** plaintext on the pod-to-pod hop is closed; eavesdropping requires CA-trusted MITM.

### Phase: lift cross-region exposure (recommended **before P6 closes**)

3. **Switch the cross-region gateway from NodePort to a LoadBalancer with private IP** (DigitalOcean LB internal), or a VPC peering tunnel. NodePort exposes the gateway on every worker's public-facing route table; LB internal restricts it to one private endpoint.
4. **Add gateway-level auth** (NATS supports per-gateway tokens). Without this, even a private LB is still passworded by nothing.

### Phase: subject ACLs (recommended **before public launch**)

5. Define the credential→subject ACL matrix in §1.F-5. Roll one credential per workload class (sfu, metrics, meeting-api). Rotate as part of standard secret rotation (S-P3-1 in GAP-ANALYSIS.md).

### Phase: ops hygiene (anytime)

6. Disable `natsbox` in production values overlay.
7. Confirm DigitalOcean firewall rules block `:30722` from outside the videocall-rs VPC tag (this audit could not verify without DO console access).

---

## 3. Specifically for the SFU refactor (P0–P6)

| When | What | Why |
| --- | --- | --- |
| Before P1 closes | Enable NATS basic auth + TLS (steps 1+2 above) | P1 adds `RoutingHeader` to the wire. `audio_level` + `is_speaking` leak speaking patterns even with E2EE on payloads. Disclosure of these to a tapping credential is a meaningful regression vs. pre-refactor. |
| During P3 | Document `room.{room}.system` is now also carrying `SpeakerUpdate` | If F-5 is not done yet, the speaker rotation pattern leaks the same speaking-pattern info as F-1 but in aggregated form. |
| During P6 | Audit the spillover path (S-P2-4): cross-pod federation via NATS assumes the bus is trusted. Without F-5, a compromised spill pod can publish forged `SpeakerUpdate` to alter every receiver's view. | The plan already calls this out at S-P2-2 (signed SpeakerUpdate). The NATS ACL audit reinforces why signing is needed *even after* ACLs land. |

---

## 4. Bottom line

NATS is the **single most concentrated trust point** in the current architecture, and it currently has zero defense in depth. The SFU refactor doesn't make this worse, but it adds new sensitive fields (`audio_level`, `is_speaking`) that ride the same bus. **Step 1 (enable NATS basic auth) should land as part of S0 — it's a one-day operations change with a strict-net improvement to the threat model.**
