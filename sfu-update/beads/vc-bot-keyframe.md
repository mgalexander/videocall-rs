# bot: keyframe-aware backpressure + honor KEYFRAME_REQUEST so mid-stream joiners decode video

Source: `sfu-update/audits/200bot-monitor/DEFECT2-VIDEO-KEYFRAME.md`. Bot-harness
defect (NOT an SFU bug): a listener joining mid-stream decodes 0 video (audio
fine) because it never receives a decodable keyframe. Confirmed across multiple
multi-pod + single-pod runs; co-arrival joiners decode video, mid-stream joiners
get `decode_errors` on every frame.

Independent of the realistic-sender stress test and the cross-pod work — pure
`bot/` change.

## Root cause (two compounding sender-side flaws)
### Primary — keyframes dropped under backpressure
The producer→writer path is a single 100-slot bounded mpsc shared by audio+video,
drained via `try_send` with NO keyframe priority:
- channel: `bot/src/orchestrate.rs:~552`
- producer try_send: `bot/src/video_producer.rs:~201-227`
- periodic keyframe cadence (1 per 150 frames): `bot/src/video_encoder.rs:~101-103`
Under load the channel drops a large fraction of packets; a periodic keyframe has
the same survival odds as any P-frame, and a single dropped keyframe poisons the
GOP for every mid-stream joiner until the next one (also likely dropped).

### Secondary — sender can't honor a KEYFRAME_REQUEST
Listeners DO emit KFRs and the SFU DOES route them, but the bot sender:
- never sets `VPX_EFLAG_FORCE_KF` — always encodes with `flags=0`
  (`bot/src/video_encoder.rs:~166`), and
- is built without `.with_decode(true)` (`orchestrate.rs:~680` is listener-only),
  so it never reads inbound packets and discards KFRs. No channel from the inbound
  path into the `VideoProducer` thread.

## Fix spec
1. **Keyframe-aware backpressure (primary, closes the defect on its own):** never
   drop a keyframe. Options: give video its own channel separate from audio, and/or
   on a full channel evict a P-frame before a keyframe / block briefly for a
   keyframe. A keyframe must always make it to the writer.
2. **Honor KFR (secondary):** sender reads inbound packets, detects
   KEYFRAME_REQUEST, and signals the `VideoProducer` (a control channel) to force a
   keyframe on the next encode (`VPX_EFLAG_FORCE_KF`).
3. Keep the periodic keyframe cadence as a fallback.

## Acceptance
- A listener joining mid-stream (after the sender is established) decodes video
  within a bounded time (e.g. first decodable keyframe ≤ a few seconds of join),
  with `crc_mismatches=0`.
- The single-pod decode-verify (mid-stream join shape, e.g. senders start, 20s
  later listeners join) shows `video_frames_decoded > 0` for the late joiners
  (today it's 0).
- No regression for co-arrival joiners (still decode video).
- Unit/integration: under simulated channel backpressure, keyframes are NOT dropped;
  a KFR forces a keyframe on the next frame.

## Priority: P1 (bot-harness; unblocks faithful video at scale + mid-stream/churn).

## Lint
`cargo fmt` + `cargo clippy -- -D warnings` on `bot` clean.
