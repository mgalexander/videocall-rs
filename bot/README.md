# Videocall Synthetic Client Bot

Enhanced bot that streams synthetic audio and video to videocall-rs for load testing and scale validation.

## Features

- **WebTransport Client**: Connects using WebTransport instead of WebSocket
- **Audio Streaming**: Loops WAV files and encodes to Opus (50fps, 20ms packets)
- **Video Streaming**: Cycles through JPEG images and sends mock video packets (30fps)
- **Per-Client Configuration**: Individual audio/video settings per client
- **Linear Ramp-up**: Configurable delay between client starts
- **Multi-Client Support**: Run multiple synthetic clients per container

## Configuration

### Option 1: YAML Configuration File (Recommended)

Create a `config.yaml` file:

```yaml
ramp_up_delay_ms: 1000
server_url: "https://webtransport-us-east.webtransport.video"
clients:
  - user_id: "bot001"
    meeting_id: "test-room"
    enable_audio: true
    enable_video: false
  - user_id: "bot002"
    meeting_id: "test-room"
    enable_audio: false
    enable_video: true
```

Then run:
```bash
BOT_CONFIG_PATH=config.yaml cargo run
```

### Option 2: Environment Variables (Backwards Compatible)

```bash
N_CLIENTS=3
SERVER_URL="https://webtransport-us-east.webtransport.video"
ROOM="test-room"
CLIENT_0_ENABLE_AUDIO=true
CLIENT_0_ENABLE_VIDEO=false
CLIENT_1_ENABLE_AUDIO=false  
CLIENT_1_ENABLE_VIDEO=true
CLIENT_2_ENABLE_AUDIO=true
CLIENT_2_ENABLE_VIDEO=true
cargo run
```

## Assets

The bot includes pre-loaded media assets:

- **Audio**: `BundyBests2.wav` - Looped and encoded to Opus
- **Video**: `output_120.jpg` to `output_124.jpg` - Cycled as mock video frames

## Usage Examples

### Test Audio-Only Clients
```bash
BOT_CONFIG_PATH=config.yaml RUST_LOG=info cargo run
```

### Quick 10-Client Test
```bash
N_CLIENTS=10 ROOM="load-test" cargo run
```

### Docker Build
```bash
docker build -t videocall-synthetic-client .
```

### Sharded Multi-Driver Load Test

To run multiple `--orchestrate` (or `--failover-test`) drivers against the same
room without colliding user IDs, give each driver a unique `--user-id-prefix`
and/or `--index-offset`. The bot composes user IDs as
`{prefix}sender-{i + offset}` / `{prefix}listener-{j + offset}`.

```bash
# Driver A: senders/listeners 0..99
cargo run -- --orchestrate --room load --senders 50 --listeners 50 \
  --duration 60 --server-url https://... --index-offset 0

# Driver B: senders/listeners 100..199 (same room, no collisions)
cargo run -- --orchestrate --room load --senders 50 --listeners 50 \
  --duration 60 --server-url https://... --index-offset 100
```

A `--user-id-prefix=us-east-` would yield `us-east-sender-100`, etc.

### Byte-Fidelity Integrity Verification (`--verify-integrity`)

`--verify-integrity` makes senders append a `[magic][seq][crc32]` trailer to
each codec payload and listeners strip + verify it, populating the
`crc_mismatches`, `media_seq_max`, `media_received_distinct`, and
`unexplained_gaps` counters in the summary JSON. Use it when you need to prove
not just that media arrived but that the exact codec bytes arrived intact
(Level-3 material match) and account for sequence gaps (Level-4 completeness);
leave it off for ordinary capacity runs, since it keeps traffic byte-for-byte
identical to baseline only when disabled.

```bash
cargo run -- --orchestrate --room load --senders 5 --listeners 45 \
  --duration 60 --server-url https://... --verify-integrity
```

### High-Scale Egress Soak Mode (`--listener-no-decode`)

`--listener-no-decode` (default OFF) makes listener bots skip the expensive
VP9/Opus codec decode while still doing **everything else** on the receive
path: connect, subscribe, `accept_uni`, `read_to_end`, the media-vs-control
packet split, integrity trailer CRC strip+verify (with `--verify-integrity`),
redirect following, and per-publisher sequence-gap tracking.

Use it for high-scale (10k) SFU egress load/soak tests. A 100-listener pod
drops from ~2.5 CPU to ~0.1-0.3 CPU because the dominant cost — the libvpx VP9
decoder threads and per-frame `.decode()` calls — is removed, so a single pod
can host many more synthetic egress receivers. Decode correctness is verified
separately by small probe cohorts run **with the flag off**.

What still populates in the summary JSON: `packets_received`, `bytes_received`,
`media_packets_received` / `control_packets_received`, `crc_mismatches`,
`media_seq_max`, `media_received_distinct`, `unexplained_gaps`. By design,
`video_frames_decoded` / `audio_frames_decoded` stay 0 (the codec never runs).

Only the `--orchestrate` listener path honours this flag; it is a no-op in
`--failover-test` (those listeners already run without decode) and in
single-bot mode, and senders are never affected. With the flag off, behaviour
is byte-for-byte identical to the previous full-decode default.

```bash
cargo run -- --orchestrate --room load --senders 5 --listeners 95 \
  --duration 300 --server-url https://... --listener-no-decode
```

## Media Protocol

- **Audio**: 50fps (20ms Opus packets) following neteq_player.rs pattern
- **Video**: 30fps (~33ms packets) with mock VP9 data
- **Transport**: WebTransport unidirectional streams
- **Format**: videocall-types protobuf MediaPacket

## TODO

- [ ] Add proper VP9 encoding (currently sends mock data)
- [ ] Add Helm chart for Kubernetes deployment  
- [ ] Add metrics collection and Prometheus export
- [ ] Add graceful shutdown handling
- [ ] Add configurable test duration

## Development

Compile and check:
```bash
cargo check
cargo clippy
cargo test
```

Run with debug logging:
```bash
RUST_LOG=debug cargo run
```