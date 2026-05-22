# SFU: spill-admitted listeners get no cross-pod media — receive-all video cap counts local members, not publishers

Source: `sfu-update/audits/200bot-monitor/DEFECT3-CROSSPOD-DATAPLANE.md`. The
remaining multi-pod blocker after the redirect-namespace fix: listeners admitted
to a SPILL pod (not the owner, not where senders publish) decode 0, while
owner-pod listeners decode fine.

## Confirmed (live data, replicas=3, after namespace fix)
- Owner pod (senders local): 1,385 listeners decoded audio, crc=0. Intra-pod OK.
- Spill pods: 87 + 28 listeners, **0 audio / 0 video decoded** despite receiving
  28,939 MEDIA packets (`media_received_other`) — federation DELIVERS the bytes,
  the forwarder DROPS them per-receiver.

## Root cause
### Video — code-proven (`actix-api/src/sfu/forwarder.rs:487`)
The receive-all video fallback admits a publisher only while
`allow.video.len() < MAX_VISIBLE_VIDEO (6)`. `allow.video` is membership-bound
(local members minus self). On a populated spill pod the local members are dozens
of fellow LISTENERS (~86), so `allow.video.len() ≈ 86 ≫ 6` → guard FALSE → every
cross-pod publisher's video is dropped as `unsubscribed` (`forwarder.rs:499-511`).
The cap denominator counts non-publishing listeners against the visible-video
budget. On the owner pod the senders are local members so they're already in
`allow.video`; on a spill pod they're remote and the fallback that should admit
them is blocked.

### Audio — contradiction to resolve via trace FIRST
Bots send no SubscriptionUpdate → `receive_mode()=(true,true)`
(`subscription.rs:472-478`) → audio branch `allow.audio.contains || recv_all_audio`
(`forwarder.rs:444`) SHOULD admit unconditionally. Yet spill-pod audio = 0.
Resolve before changing audio logic: run the vc-8wd trace (below) to see whether
audio is dropped at `decide` (`sfu_dropped_total{reason}` rises) or never reaches
the receiver from the dispatcher.

## Confirm with vc-8wd instrumentation (overseer-run, before AND after fix)
- Set `SFU_TRACE_ROOM=<room>` on the SFU, run `spillover-decode/decode-verify.sh`
  at replicas≥3, scrape the SPILL pod's `/metrics`:
  - Expect (pre-fix) `sfu_dropped_total{reason="unsubscribed"}` rising for the
    senders' sids on the spill pod (`forwarder.rs:500-502`).
  - `sfu_allowset_size` (`subscription.rs:291`) piled at the local-member count.
  - `trace_forward_decision` (`forwarder.rs:872-891`) shows drop+reason per sender sid.
- Post-fix: `unsubscribed` drops ~0 for sender packets; `sfu_forwarded_total` rises.
PRESERVE all vc-8wd instrumentation; do not weaken it.

## Fix spec (preferred: register cross-pod publishers)
1. Treat non-member sids seen as MEDIA publishers on the dispatcher ingress
   (`chat_server.rs:~2388` / the receive path) as forwardable senders, so
   `resolve_inner` (`subscription.rs:330`) places them in `allow.*` for local
   receivers. Bound the remote-publisher set + TTL-reap via `prune_session`
   (avoid unbounded growth).
2. Size the visible-video cap against distinct admitted VIDEO PUBLISHERS, not
   local member count — so `MAX_VISIBLE_VIDEO` limits actual video sources, not
   listeners. (This is the core video fix.)
3. Resolve the audio drop per the trace verdict (likely the same membership-bound
   path; the fix in #1 should also cover audio).
4. O(n) safety: keep the `MAX_VISIBLE_VIDEO` ceiling; the remote-publisher set is
   bounded by sender count (≤ ~10 for webinar), not receiver count. No per-receiver
   O(members) work added on the hot path.

## Acceptance
- replicas≥3: a spill-admitted listener on an adjacent pod decodes the senders'
  AUDIO (and video, once DEFECT2 keyframe is also addressed) with crc_mismatches=0.
- Spill pod `sfu_dropped_total{reason="unsubscribed"}` ~0 for sender packets;
  `sfu_forwarded_total` rises on the spill pod.
- Owner-pod behavior unchanged (no regression for intra-pod forwarding).
- vc-8wd instrumentation intact.

## Priority: P0 — last blocker for true distributed (spill-to-adjacent-pod) capacity.

## Lint
`cargo fmt` + `cargo clippy -- -D warnings` on `actix-api` clean.
