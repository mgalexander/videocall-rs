# SFU: move SFU_ROOM_SIZE gauge write off the per-decide hot path
Source: LATE-JOINER-INTEGRATION-ROOTCAUSE.md (lj-4). `forwarder.rs:357` writes the
room-size gauge per decide (per packet×receiver). Move to a periodic updater.
## P2. Acceptance: no per-decide gauge write; gauge still accurate within ~1s.
