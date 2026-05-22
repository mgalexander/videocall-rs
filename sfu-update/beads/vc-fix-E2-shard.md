# SFU: shard ChatServer per-room + non-blocking JoinRoom (E2) — registration is still single-thread rate-limited (~1 join/s)

Source: `DELIVERY-SCALING-ROOTCAUSE.md` Fix E2. The incremental fixes proved the
limit is the single ChatServer actor's serial JoinRoom handling — registration
progressed but never crossed:

## Evidence (registration of ~3,030 connected, 12-min soak)
- baseline (16-slot mailbox): ~360
- E1 (mailbox 8192, vc-knqr): ~527
- E3 (per-packet publish off-actor, vc-ud6o): ~948
Each helped, but registration is still ~1 join/sec and caps just under 1,000, so
vc-xow8 milestones never fire (need >=1000) and mid-stream probes stay 0 at 2k/3k.
The remaining serialization: `JoinRoom` is a bounded awaited `.send()`
(`wt_chat_session.rs:456`, `.wait(ctx)` :540) into the single ChatServer, whose
heavy handler (`chat_server.rs:1860-2232`) runs serially on one thread.

## Fix (the structural de-serialization)
- **Shard `ChatServer` per-room by jump-hash** (mirror `affinity::is_owner`
  `chat_server.rs:2030`) — N actors across N cores so JoinRoom handling and room
  state parallelize instead of funneling through one actor/thread. AND/OR
- **Make `JoinRoom` non-blocking** (`do_send`, return result via the existing
  recipient channel) so joins don't serialize on a bounded awaited `.send()`.
- Keep E1 (8192 mailbox) + E3 (off-actor publish).
Applies to BOTH WebTransport and WebSocket.

## Acceptance
- replicas=1, 4 CPU, 3,000+ lightweight listeners: registration reaches ~the
  connected count, rate >> 1/s; vc-xow8 milestones FIRE at 1000/2000/3000;
  connected ≈ members ≈ receiver_set.
- Then vc-ypx3 (fan-out shard) becomes the measurable next limit en route to 10k.
- No 200-participant regression; WS path; code+perf review (touches the core
  per-pod actor + room ownership).
## Priority: P0 — the registration rate ceiling (incremental fixes insufficient).
## Lint: cargo fmt + clippy -D warnings clean.
