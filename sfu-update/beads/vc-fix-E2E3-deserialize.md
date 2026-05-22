# SFU: de-serialize ChatServer — move per-packet NATS publish OFF the actor (E3) + non-blocking/sharded JoinRoom (E2)

Source: `DELIVERY-SCALING-ROOTCAUSE.md` Fix E2/E3. Follow-up to vc-knqr (E1 mailbox
8192), which proved INSUFFICIENT empirically:

## Evidence E1 alone didn't fix it
With `SFU_CHATSERVER_MAILBOX_CAPACITY=8192` (vc-knqr), a 3,000-listener soak still
registered only **~527 of ~3,030** connected (up from ~360 with the 16-slot
default — a partial gain), markers STILL fired 0× (room_members never crossed
1000), probes still 0 at 2k/3k. ~0.7 joins/sec registered. So the limit is actor
THROUGHPUT, not queue depth: the single `ChatServer` thread is monopolized by the
per-packet `ClientMessage`→NATS-publish flood (`wt_chat_session.rs:446` →
`chat_server.rs:1479`) from 20 senders, starving the JoinRoom handler.

## Fix (the structural de-serialization)
- **E3 (do FIRST — likely the dominant win): move the per-packet NATS publish OFF
  the ChatServer actor.** Inbound `ClientMessage` should publish to
  `room.{room}.{session}` from the transport/session task (or a dedicated publish
  task pool), NOT via `do_send` into the single ChatServer mailbox. This removes
  the media flood from the actor thread so it can process JoinRoom/lifecycle.
- **E2 (structural): non-blocking + parallel registration.** Make `JoinRoom`
  `do_send` (return the result via the existing recipient channel instead of a
  bounded awaited `.send()`), and/or shard `ChatServer` per-room by jump-hash
  (mirror `affinity::is_owner` at `chat_server.rs:2030`) so registration isn't a
  single-thread serialization point.
Keep E1 (8192 mailbox) in place. Applies to BOTH WebTransport and WebSocket.

## Acceptance
- replicas=1, 4 CPU, 3,000+ lightweight listeners: room_members reaches ~the
  connected count (vc-xow8 milestones FIRE at 1000/2000/...), registration rate
  >> 0.7/s; connected≈members≈receiver_set.
- After this, the NEXT bottleneck (per-room dispatcher fan-out, vc-ypx3) can be
  measured/fixed and the SFU can be pushed toward the true 10k ceiling.
- No regression at 200-participant scale; WS path covered; code+perf review.
## Priority: P0 — the actor-throughput registration ceiling (E1 was insufficient).
## Lint: cargo fmt + clippy -D warnings clean.
