# bot: --listener-no-decode lightweight mode (receive+CRC-verify, skip codec decode) for high-scale egress tests

## Why
Capacity-testing the SFU to ~10k listeners is EGRESS-bound on the SFU but
DECODE-bound on the test host: a 100-listener real-decode pod costs ~2.5 CPU, so
10k = ~250 cores > the 192-core k3d host. To push the SFU's forwarding ceiling we
need lightweight listeners that still exercise the full receive path + byte-
fidelity check, but skip the expensive VP9/Opus decode. Decode itself is verified
separately by small "probe" cohorts (decode ON) at each load level.

## Scope
Add a flag (e.g. `--listener-no-decode`, default OFF) to orchestrate mode. When
set, listener bots:
- still connect, subscribe (receive-all), `accept_uni`, `read_to_end`,
  `record_packet` (media-vs-control split), and follow redirects, AND
- still strip + verify the trailer CRC (`crc_mismatches`) and track
  sequence/`unexplained_gaps` — byte-fidelity does NOT require codec decode, AND
- do NOT build/drive the `DecoderPool` (skip the `.with_decode(true)` at
  `bot/src/orchestrate.rs:688` and the VP9/Opus decode in `decode_packet`), so a
  100-listener pod costs ~0.1–0.3 CPU instead of ~2.5.
`video_frames_decoded`/`audio_frames_decoded` are 0/absent in this mode by design
(documented); `crc_mismatches`, media-vs-control counts, packets_received remain
meaningful.

## Acceptance
- `--listener-no-decode`: a 100-listener pod uses <0.5 CPU under load (vs ~2.5 with
  decode); `crc_mismatches` and media-received counts still populate; no DecoderPool
  allocated.
- Default (flag off) unchanged: full decode as today.
- `bot --help` + `bot/README.md` document the flag (what + when: high-scale egress
  load where decode is sampled via separate probe cohorts).
## Priority: P1 (unblocks the 10k SFU egress soak). bot-only.
## Lint: cargo fmt + clippy -D warnings on bot clean.
