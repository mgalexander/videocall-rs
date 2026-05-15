# sfu-update Operations Log

Chronological log of bootstrap and convoy-execution events. Append-only.

Format: `YYYY-MM-DD HH:MM TZ  ACTOR  EVENT  DETAIL`

---

## 2026-05-15 Bootstrap

- baseline disk: `/dev/mapper/data-root 908G / 596G used / 266G avail / 70% used` — well under 80% soft alert and 85% halt threshold.
- baseline docker: `Images 107.7GB (73.43GB reclaimable), Build cache 4.77GB, Local volumes 1.738GB`.
- B0 verified `gastown-sandbox` running (up ~2 days hosting `lps-`, `imap-` rigs). Existing bind mounts: labs-pim-server, labs-imap-server, /gt → /mnt/llms/gas-town/town.
- B1 added `/mnt/llms/videocall:/mnt/llms/videocall` bind mount to `/mnt/llms/gas-town/docker-compose.override.yml`; `docker compose up -d gastown` recreated the container cleanly.
- Verified: `docker exec gastown-sandbox ls /mnt/llms/videocall` returns repo tree; `gt --version` returns `gt version dev`; `bd` found at `/usr/local/bin/bd`.
- Created branch `experimental-sfu` from `c01a773 (helm: fix webtransport version URL to use LB service (#827))`.
