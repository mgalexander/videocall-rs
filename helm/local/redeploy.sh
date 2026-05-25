#!/usr/bin/env bash
#
# helm/local/redeploy.sh — fast inner-loop image + helm refresh against the
# already-running local k3d cluster.
#
# Scope: a developer changed Rust source. Without bringing the cluster down,
# this script:
#   1. Rebuilds container images for the requested components (incremental
#      docker layer-cached `cargo build` inside the dev image).
#   2. Pushes them to the k3d local registry (localhost:5000).
#   3. `k3d image import`s them onto every k3s node (fast warm path —
#      kubelets never have to talk to the registry).
#   4. `helm upgrade`s the affected releases with a per-run image tag so
#      pods actually restart (see "Tag strategy" below).
#   5. Waits for each rollout to complete.
#   6. Tails pod logs for ~5s to surface immediate startup errors.
#
# CLI:
#   ./helm/local/redeploy.sh [component...]
#     component: meeting-api | websocket | webtransport | all
#     Default:   all  (= meeting-api + websocket + webtransport)
#
# Tag strategy:
#   values-local.yaml pins each chart to `image.tag: dev` with
#   `pullPolicy: IfNotPresent`. Rebuilding under that same tag does NOT
#   trigger a pod restart — helm sees no change and kubelet has a cached
#   image. We therefore mint a unique per-run tag of the form
#       dev-<short-sha>-<unix-timestamp>
#   and pass it via `--set image.tag=...`. helm sees a real diff, rolls
#   the deployment, and the kubelet pulls the freshly-imported image.
#
#   Side-effect: the local registry container accumulates `dev-*` tags
#   over time. That's fine — `down.sh` deletes the registry container
#   on full teardown. Pruning is out of scope for this inner-loop tool.
#
# Not a replacement for up.sh:
#   redeploy.sh assumes the cluster is already running AND that each
#   target helm release has already been installed by up.sh. If the
#   cluster is unreachable or a release is missing, redeploy.sh errors
#   out with a hint rather than falling back to `helm install` (which
#   would skip dependency phases like Secrets, NATS, and postgres).

set -euo pipefail

# ----- Configurable knobs (env-overridable) -----------------------------------
CLUSTER_NAME="${CLUSTER_NAME:-videocall-local}"
REGISTRY_NAME="${REGISTRY_NAME:-videocall-local-registry}"
REGISTRY_PORT="${REGISTRY_PORT:-5000}"
KUBECONTEXT="${KUBECONTEXT:-k3d-${CLUSTER_NAME}}"

# How long (seconds) to wait for each Deployment to finish rolling out.
ROLLOUT_TIMEOUT="${ROLLOUT_TIMEOUT:-180}"

# How long (seconds) to tail logs from a freshly-rolled pod before moving on.
LOG_TAIL_SECONDS="${LOG_TAIL_SECONDS:-5}"

# Resolve the helm/ directory and repo root relative to this script so the
# script can be invoked from any cwd. REPO_ROOT is the docker build context
# for the meeting-api and SFU images (their Dockerfiles live at the repo root
# and `COPY . /app` the whole tree). Matches up.sh.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HELM_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${HELM_DIR}/.." && pwd)"

LOG_PREFIX="[redeploy.sh]"

log() {
    echo "${LOG_PREFIX} $*"
}

err() {
    echo "${LOG_PREFIX} ERROR: $*" >&2
}

# ----- Preflight: required binaries -------------------------------------------
require_bin() {
    local bin="$1"
    local hint="$2"
    if ! command -v "${bin}" >/dev/null 2>&1; then
        err "'${bin}' not found on PATH."
        err "${hint}"
        exit 1
    fi
}

require_bin docker \
    "Install Docker Desktop / Docker Engine for your platform: https://docs.docker.com/get-docker/"

require_bin kubectl \
    "Install kubectl: https://kubernetes.io/docs/tasks/tools/  (macOS: brew install kubectl)"

require_bin k3d \
    "Install k3d. macOS: brew install k3d. Or: curl -s https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | bash"

require_bin helm \
    "Install helm: https://helm.sh/docs/intro/install/  (macOS: brew install helm)"

# ----- Parse + validate component list ----------------------------------------
VALID_COMPONENTS=(meeting-api websocket webtransport)

usage() {
    cat >&2 <<EOF
${LOG_PREFIX} usage: $(basename "$0") [component...]
${LOG_PREFIX}   component: meeting-api | websocket | webtransport | all
${LOG_PREFIX}   Default:   all
${LOG_PREFIX}
${LOG_PREFIX} Examples:
${LOG_PREFIX}   $(basename "$0")
${LOG_PREFIX}   $(basename "$0") meeting-api
${LOG_PREFIX}   $(basename "$0") websocket webtransport
EOF
}

is_valid_component() {
    local needle="$1"
    local c
    for c in "${VALID_COMPONENTS[@]}"; do
        if [ "${c}" = "${needle}" ]; then
            return 0
        fi
    done
    return 1
}

# Default to "all" when no args supplied.
if [ "$#" -eq 0 ]; then
    set -- all
fi

# Expand "all" anywhere in the arg list and dedupe.
SELECTED=()
seen_meeting_api=0
seen_websocket=0
seen_webtransport=0

add_selection() {
    case "$1" in
        meeting-api)
            if [ "${seen_meeting_api}" -eq 0 ]; then
                SELECTED+=(meeting-api)
                seen_meeting_api=1
            fi
            ;;
        websocket)
            if [ "${seen_websocket}" -eq 0 ]; then
                SELECTED+=(websocket)
                seen_websocket=1
            fi
            ;;
        webtransport)
            if [ "${seen_webtransport}" -eq 0 ]; then
                SELECTED+=(webtransport)
                seen_webtransport=1
            fi
            ;;
    esac
}

for arg in "$@"; do
    case "${arg}" in
        -h|--help)
            usage
            exit 0
            ;;
        all)
            add_selection meeting-api
            add_selection websocket
            add_selection webtransport
            ;;
        *)
            if is_valid_component "${arg}"; then
                add_selection "${arg}"
            else
                err "unknown component: '${arg}'"
                err "valid components: ${VALID_COMPONENTS[*]} | all"
                usage
                exit 1
            fi
            ;;
    esac
done

log "selected components: ${SELECTED[*]}"

# ----- Cluster reachability check ---------------------------------------------
# If the cluster is paused or torn down, fail fast with a clear hint rather
# than waiting for the first kubectl/helm call to hang.
if ! kubectl --context "${KUBECONTEXT}" get nodes >/dev/null 2>&1; then
    err "cannot reach cluster via context '${KUBECONTEXT}'."
    err "hint: bring it up with ./helm/local/up.sh  (or wake it with ./helm/local/resume.sh)"
    exit 1
fi

# ----- Verify each target helm release already exists -------------------------
# redeploy.sh is an inner-loop refresh, not an installer. If a release is
# missing it means up.sh hasn't run (or didn't complete its install phase),
# so we error out rather than silently `helm install` and skip the platform
# dependencies (Secrets / NATS / postgres / cert-manager Certificate).
release_exists() {
    local name="$1"
    helm --kube-context "${KUBECONTEXT}" status "${name}" --namespace default \
        >/dev/null 2>&1
}

require_release() {
    local component="$1" release="$2"
    if ! release_exists "${release}"; then
        err "helm release '${release}' (component '${component}') not found in namespace 'default'"
        err "hint: run ./helm/local/up.sh first — redeploy.sh only refreshes existing releases"
        exit 1
    fi
}

for component in "${SELECTED[@]}"; do
    case "${component}" in
        meeting-api)    require_release meeting-api    meeting-api ;;
        websocket)      require_release websocket      rustlemania-websocket ;;
        webtransport)   require_release webtransport   rustlemania-webtransport ;;
    esac
done

# ----- Build a per-run image tag ----------------------------------------------
# values-local.yaml pins `image.pullPolicy: IfNotPresent` and `image.tag: dev`.
# Re-pushing under the same tag won't cause helm to roll the pod, so we mint a
# unique per-run tag and pass it via --set on each helm upgrade.
GIT_SHA="$(git -C "${REPO_ROOT}" rev-parse --short HEAD 2>/dev/null || echo unknown)"
GIT_BRANCH="$(git -C "${REPO_ROOT}" rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
BUILD_TIMESTAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
TAG="dev-${GIT_SHA}-$(date +%s)"
log "image tag for this run: ${TAG}"

LOCAL_REGISTRY="localhost:${REGISTRY_PORT}"
MEETING_API_IMAGE="${LOCAL_REGISTRY}/videocall-meeting-api:${TAG}"
MEDIA_SERVER_IMAGE="${LOCAL_REGISTRY}/videocall-media-server:${TAG}"

# ----- Build / push / import helper (mirrors up.sh's build_push_import) -------
build_push_import() {
    # build_push_import <image-tag> <dockerfile-relpath>
    local tag="$1" dockerfile="$2"
    log "building ${tag} from ${dockerfile} (context: ${REPO_ROOT})"
    docker build \
        --tag "${tag}" \
        --file "${REPO_ROOT}/${dockerfile}" \
        --build-arg "GIT_SHA=${GIT_SHA}" \
        --build-arg "GIT_BRANCH=${GIT_BRANCH}" \
        --build-arg "BUILD_TIMESTAMP=${BUILD_TIMESTAMP}" \
        "${REPO_ROOT}"
    log "pushing ${tag} to local registry"
    docker push "${tag}"
    log "k3d image import ${tag} -> cluster '${CLUSTER_NAME}'"
    k3d image import "${tag}" --cluster "${CLUSTER_NAME}"
}

# ----- Build only the images we actually need ---------------------------------
# meeting-api and media-server are independent. websocket + webtransport SHARE
# the media-server image, so we build it exactly once even when both are in
# the selection.
need_meeting_api_build=0
need_media_server_build=0
for component in "${SELECTED[@]}"; do
    case "${component}" in
        meeting-api)                       need_meeting_api_build=1 ;;
        websocket|webtransport)            need_media_server_build=1 ;;
    esac
done

if [ "${need_meeting_api_build}" -eq 1 ]; then
    build_push_import "${MEETING_API_IMAGE}" "Dockerfile.meeting-api"
fi

if [ "${need_media_server_build}" -eq 1 ]; then
    build_push_import "${MEDIA_SERVER_IMAGE}" "Dockerfile.actix"
fi

# ----- helm upgrade + rollout + log-tail per component ------------------------
# Background-tail PID tracked so the trap can clean up if we're interrupted
# mid-tail (Ctrl-C, kill, etc.). Empty string means "no tail in flight".
TAIL_PID=""

cleanup() {
    if [ -n "${TAIL_PID}" ] && kill -0 "${TAIL_PID}" 2>/dev/null; then
        kill "${TAIL_PID}" 2>/dev/null || true
        wait "${TAIL_PID}" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

upgrade_and_tail() {
    # upgrade_and_tail <release> <chart-dir> <kind> <workload> <values-file>
    # <kind> is the kubectl resource kind (deployment or statefulset) that the
    # chart renders; rustlemania-{websocket,webtransport} render StatefulSets,
    # meeting-api renders a Deployment.
    local release="$1" chart="$2" kind="$3" workload="$4" values_file="$5"

    log "helm upgrade ${release} (chart: ${chart}, tag: ${TAG})"
    # We deliberately re-render from values-local.yaml (no --reuse-values) so
    # local overlay edits between up.sh and redeploy.sh land too. The only
    # per-run override is image.tag.
    helm --kube-context "${KUBECONTEXT}" upgrade --install "${release}" \
        "${chart}" \
        --namespace default \
        --values "${values_file}" \
        --set "image.tag=${TAG}"

    log "waiting up to ${ROLLOUT_TIMEOUT}s for ${kind}/${workload} to roll out"
    kubectl --context "${KUBECONTEXT}" -n default rollout status \
        "${kind}/${workload}" --timeout="${ROLLOUT_TIMEOUT}s"

    log "tailing ${kind}/${workload} logs for ${LOG_TAIL_SECONDS}s (prefix: [${workload} logs])"
    # Use a subshell so we can prefix every log line for readable multi-
    # component runs. `kubectl logs -f` on a Deployment/StatefulSet streams
    # from all matching pods; --tail=50 caps the initial backlog. Stderr is
    # folded in via 2>&1 so panics show up under the same prefix.
    #
    # awk + fflush (not `sed -u`) for the line-prefix: BSD sed on macOS
    # rejects -u and we ship to macOS dev boxes as the primary platform.
    (
        kubectl --context "${KUBECONTEXT}" -n default logs \
            "${kind}/${workload}" -f --tail=50 2>&1 \
            | awk -v p="[${workload} logs] " '{ print p $0; fflush(); }'
    ) &
    TAIL_PID=$!

    sleep "${LOG_TAIL_SECONDS}"

    if kill -0 "${TAIL_PID}" 2>/dev/null; then
        kill "${TAIL_PID}" 2>/dev/null || true
        wait "${TAIL_PID}" 2>/dev/null || true
    fi
    TAIL_PID=""
}

for component in "${SELECTED[@]}"; do
    case "${component}" in
        meeting-api)
            upgrade_and_tail \
                meeting-api \
                "${HELM_DIR}/meeting-api" \
                deployment \
                meeting-api \
                "${HELM_DIR}/meeting-api/values-local.yaml"
            ;;
        websocket)
            upgrade_and_tail \
                rustlemania-websocket \
                "${HELM_DIR}/rustlemania-websocket" \
                statefulset \
                rustlemania-websocket \
                "${HELM_DIR}/rustlemania-websocket/values-local.yaml"
            ;;
        webtransport)
            upgrade_and_tail \
                rustlemania-webtransport \
                "${HELM_DIR}/rustlemania-webtransport" \
                statefulset \
                rustlemania-webtransport \
                "${HELM_DIR}/rustlemania-webtransport/values-local.yaml"
            ;;
    esac
done

# ----- Emit machine-readable handles for downstream scripts -------------------
# Same two lines up.sh / resume.sh emit, so anything that already greps them
# keeps working.
log "redeploy complete"
echo "KUBECONTEXT=${KUBECONTEXT}"
echo "REGISTRY=localhost:${REGISTRY_PORT}"
