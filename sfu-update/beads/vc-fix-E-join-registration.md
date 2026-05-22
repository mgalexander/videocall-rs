# SFU: ChatServer single actor + DEFAULT 16-slot mailbox caps registration at ~hundreds — connected listeners never register (the real scaling ceiling)

Source: `DELIVERY-SCALING-ROOTCAUSE.md` REFINEMENT (2026-05-21). Confirmed by the
vc-xow8 markers firing ZERO times at 3,000 connected listeners (room_members never
crossed 1000; ~360 distinct sessions registered). This is HIT FIRST, before the
dispatcher fan-out limit (vc-ypx3), and masks it.

## Root cause (code-proven)
One `ChatServer` actor per pod, started with the DEFAULT actix `Context`
(`chat_server.rs:827-829`; `bin/webtransport_server.rs:169`, `bin/websocket_server.rs:334`)
— `Actor::started` is NOT overridden, so the mailbox is the actix DEFAULT (16). The
only `set_mailbox_capacity(4096)` is a unit-test actor (`wt_chat_session.rs:838`),
not prod. Everything funnels through that one 16-slot mailbox on one thread:
- `JoinRoom` = bounded awaited `.send()` (`wt_chat_session.rs:456`, `.wait(ctx)` :540)
  — blocks until dequeued; the handler is heavy (`chat_server.rs:1860-2232`).
- `ClientMessage` (every inbound packet: RTT/heartbeat/diag/media) = `do_send` into
  the SAME mailbox (`wt_chat_session.rs:446` → `chat_server.rs:1479`).
- plus Connect/ActivateConnection/Disconnect/RoomDispatcherExited.
Under ~1000 joins/step + the packet flood, JoinRoom `.send()`s stall behind the full
16-slot mailbox; only a few hundred drain. `room_members` (written only in the
serialized handler at `:1979`) never reaches 1000. `Connect` (`:834`) does NOT add
membership or the dispatcher `receivers` entry (that's in the JoinRoom handler at
`:2120`), so unregistered sessions are invisible to BOTH the marker and the
dispatcher — connected but never registered.

## Fix (de-serialize registration)
- **E1 (do FIRST, cheap, likely most of the win):** override `ChatServer::started`
  to `set_mailbox_capacity(>=8192)` so the JoinRoom flood doesn't head-of-line
  stall. Verify whether E1 alone lets registration reach the connected count.
- **E2 (structural, if E1 insufficient):** make `JoinRoom` `do_send` (return result
  via the existing recipient channel) and/or shard `ChatServer` per-room by
  jump-hash (mirror `affinity::is_owner` :2030) so registration parallelizes.
- **E3 (relieve competing load):** move the per-packet NATS publish off the actor so
  the mailbox carries only lifecycle messages, not the media flood.
Applies to BOTH WebTransport and WebSocket ChatServer paths.

## Acceptance
- replicas=1, 4 CPU, 3,000+ lightweight listeners: `room_members` reaches the
  connected count (the vc-xow8 milestones FIRE at 1000/2000/... ), i.e.
  connected ≈ members ≈ receiver_set; no large connected≫members gap.
- A mid-stream listener joining a 2k/3k room is registered + (with the senders
  producing) receives media.
- No regression at 200-participant scale; WS path covered.
## Priority: P0 — the registration ceiling; do BEFORE vc-ypx3 (fan-out). code+perf review.
## Lint: cargo fmt + clippy -D warnings clean.
