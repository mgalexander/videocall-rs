# syntax=docker/dockerfile:1
#
# Multi-stage Dockerfile for the videocall `bot` crate.
#
# The bot is a Rust binary that drives load tests against the SFU. In the
# local k3d stack it runs as a k8s Job (helm/local/manifests/load-test-job.yaml)
# pulling its parameters from the `load-test-config` ConfigMap, then prints a
# single JSON summary on stdout.
#
# Build context: REPO ROOT (because the bot crate depends on `../videocall-types`
# via a path = "../videocall-types" workspace reference). The image is intended
# for the k3d local registry tag `localhost:5000/videocall-bot:dev` and is
# side-loaded into the cluster via `k3d image import`.
#
# Build dependencies mirror Dockerfile.videocall-cli: libvpx (for the
# env-libvpx-sys VP9 encoder), libssl/pkg-config (TLS), build-essential (cc),
# clang/libclang (bindgen for env-libvpx-sys), cmake/nasm (vendored deps).
#
# Runtime image stays small: debian:bookworm-slim + libvpx + ca-certificates.
# The bot baked-in assets (BundyBests2.wav, output_120..124.jpg) ship under
# /app/ so the default CLI flags --audio-path /app/BundyBests2.wav and
# --image-dir /app resolve.

FROM rust:1.89-slim-bookworm AS build

ARG GIT_SHA=unknown
ARG GIT_BRANCH=unknown
ARG BUILD_TIMESTAMP=unknown

RUN apt-get --yes update && apt-get --yes install --no-install-recommends \
        libssl-dev pkg-config libvpx-dev build-essential libclang-dev \
        clang cmake nasm ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

# Build only the bot crate. The workspace path dep on videocall-types is
# already in scope because we copied the whole tree.
RUN cargo build --release --package bot

# ----- runtime ----------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

ARG GIT_SHA=unknown
ARG GIT_BRANCH=unknown
LABEL org.opencontainers.image.revision=${GIT_SHA}
LABEL org.opencontainers.image.ref.name=${GIT_BRANCH}
LABEL org.opencontainers.image.title="videocall-bot"
LABEL org.opencontainers.image.description="Load-test bot for the videocall SFU"

RUN apt-get update && apt-get install -y --no-install-recommends \
        libvpx7 ca-certificates \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Non-root runtime user. Matches the security guidance in CLAUDE.md and
# limits blast radius if a bot container escapes the Job sandbox.
RUN groupadd --system --gid 10001 bot && \
    useradd --system --uid 10001 --gid 10001 --home-dir /app --shell /usr/sbin/nologin bot

WORKDIR /app

# Bake in the load-test media assets so the k8s Job doesn't have to mount
# anything. Paths line up with the bot CLI defaults the Job manifest passes.
COPY --from=build /app/bot/BundyBests2.wav /app/BundyBests2.wav
COPY --from=build /app/bot/output_120.jpg /app/output_120.jpg
COPY --from=build /app/bot/output_121.jpg /app/output_121.jpg
COPY --from=build /app/bot/output_122.jpg /app/output_122.jpg
COPY --from=build /app/bot/output_123.jpg /app/output_123.jpg
COPY --from=build /app/bot/output_124.jpg /app/output_124.jpg
COPY --from=build /app/target/release/bot /usr/local/bin/bot

RUN chown -R bot:bot /app
USER bot

ENV RUST_LOG=info,quinn=warn

ENTRYPOINT ["/usr/local/bin/bot"]
CMD ["--help"]
