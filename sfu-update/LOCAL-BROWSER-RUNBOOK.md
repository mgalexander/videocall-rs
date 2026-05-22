# Local Browser Runbook — real browser → k3d SFU (experimental-sfu)

Connect real browsers to the locally-running k3d SFU. Topology mirrors the proven e2e
setup (all-`localhost`, same-site) but points at the **k3d cluster** via host
port-forwards. WebSocket transport (WebTransport/QUIC UDP isn't reachable through k3d's
serverlb; not needed for calling). No certs, no `/etc/hosts`, no cross-site cookie issues.

```
browser → http://localhost:3001  (Dioxus UI, docker container docker-dioxus-ui-1)
          ├── http://localhost:8081  (meeting-api,   port-forward → k3d)
          └── ws://localhost:8090     (websocket SFU, port-forward → k3d rustlemania-websocket)
```

## Status: VERIFIED RUNNING (2026-05-22)
- UI built + serving on `http://localhost:3001` (HTTP 200, wasm artifact loads).
- Auth chain proven end-to-end: `POST /api/v1/meetings/demo/join` returns 401 without a
  cookie and `{"success":true,"status":"admitted","room_token":...}` with the Alice cookie.

## What's already running (set up by the overseer)
1. **meeting-api CORS** allows `http://localhost:3001` (`CORS_ALLOWED_ORIGIN`).
2. **Port-forwards** (host → k3d): `meeting-api 8081:8081`, `rustlemania-websocket 8090:8080`.
3. **Dioxus UI** container `docker-dioxus-ui-1` on `:3001` (config: api `localhost:8081`,
   ws `localhost:8090`, OAuth off, WebTransport off), via
   `docker/docker-compose.{e2e,local-ui}.yaml`.

## Auth — inject a session cookie (OAuth off; same mechanism as e2e/helpers/auth.ts)
The meeting-api requires a valid `session` JWT (HS256, secret `dev-jwt-secret-replace-me`,
`iss: videocall-meeting-backend`). Two 30-day tokens pre-generated. Inject via DevTools:

- DevTools → **Application → Cookies → `http://localhost:3001`** → add cookie:
  **Name** `session`, **Value** = token below, **Domain** `localhost`, **Path** `/`,
  **Secure** off, **SameSite** `Lax`. (Cookies aren't port-specific → rides to `:8081`.)

**Alice** (`sub: alice@videocall.local`):
```
eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhbGljZUB2aWRlb2NhbGwubG9jYWwiLCJuYW1lIjoiQWxpY2UiLCJleHAiOjE3ODIwNjkyMTEsImlhdCI6MTc3OTQ3NzIxMSwiaXNzIjoidmlkZW9jYWxsLW1lZXRpbmctYmFja2VuZCJ9.7luwMdD2uDuX4NtL0DUv_hl7hMMpPgSV36MuKdxL5TY
```
**Bob** (`sub: bob@videocall.local`):
```
eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJib2JAdmlkZW9jYWxsLmxvY2FsIiwibmFtZSI6IkJvYiIsImV4cCI6MTc4MjA2OTIxMSwiaWF0IjoxNzc5NDc3MjExLCJpc3MiOiJ2aWRlb2NhbGwtbWVldGluZy1iYWNrZW5kIn0.wrWgfKcrss9XsJs9APAX3_psnfk6zqjfUSZcv9iuPLk
```

## Join a meeting (two participants)
1. **Window 1 (Alice):** open `http://localhost:3001`, inject the **Alice** cookie, reload.
   Join a room (e.g. `demo`). Grant camera/mic.
2. **Window 2 (Bob):** a **separate browser profile or incognito window** (cookies are
   per-profile), open `http://localhost:3001`, inject **Bob**, reload, join the same `demo`.
3. They see/hear each other — media flows through the **k3d SFU** over WebSocket.

> Distinct profiles/incognito for distinct participants; same-profile tabs share identity.

## Restart / operate
- **UI:** `docker compose -f docker/docker-compose.e2e.yaml -f docker/docker-compose.local-ui.yaml up -d --no-deps dioxus-ui` · logs `docker logs -f docker-dioxus-ui-1` · stop `... stop dioxus-ui`.
- **Port-forwards** (if dropped — they don't survive a reboot/disconnect):
  - `kubectl --context k3d-videocall-local -n default port-forward svc/meeting-api 8081:8081`
  - `kubectl --context k3d-videocall-local -n default port-forward svc/rustlemania-websocket 8090:8080`
- **Health:** `curl localhost:8081` (404=ok) · `curl localhost:8090` (404=ok) · `curl localhost:3001` (200=ok).
- **Auth smoke:** `curl -X POST localhost:8081/api/v1/meetings/demo/join -H "Cookie: session=<Alice>" -d '{}'` → `success:true`.

## Notes / limits
- **WebSocket only.** WebTransport (QUIC/UDP) needs a k3d UDP port mapping (cluster recreate);
  not required for calling. To enable later: recreate k3d with `443:30777/udp` and set
  `WEBTRANSPORT_ENABLED=true` + `WEBTRANSPORT_HOST` in `docker/docker-compose.local-ui.yaml`.
- **New tokens:** HS256-sign `{sub,name,exp,iat,iss:"videocall-meeting-backend"}` with
  `dev-jwt-secret-replace-me` (the k3d `jwt-secret`, from `helm/local/.env`).
- Drives the **experimental-sfu** SFU (Track 1 + late-joiner fixes), replicas=1 — fine for
  small browser sessions; >v1 scaling is the separate backlog (`SCALING-BACKLOG.md`).
