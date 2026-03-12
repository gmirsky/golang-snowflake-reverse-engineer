#!/usr/bin/env bash
set -euo pipefail

# Check and update pinned Chainguard image digests in Dockerfile and Containerfile.
# Requires: docker with buildx support.
# Exit code contract:
# - 0 on success or already up-to-date
# - 1 when required tooling is missing or check mode finds drift
# - 2 for invalid arguments

# Resolve repository root relative to this script location so the script can be
# run from any current working directory.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DOCKERFILE="$ROOT_DIR/Dockerfile"
CONTAINERFILE="$ROOT_DIR/Containerfile"

# Images we track in both Dockerfile variants.
GO_IMAGE="cgr.dev/chainguard/go:latest-dev"
STATIC_IMAGE="cgr.dev/chainguard/static:latest"

# Default mode is update so running without arguments does useful work.
MODE="update"
if [[ "${1-}" == "--check" ]]; then
  MODE="check"
elif [[ "${1-}" == "--update" || -z "${1-}" ]]; then
  MODE="update"
else
  echo "Usage: $0 [--check|--update]"
  exit 2
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "error: docker is required"
  exit 1
fi

get_latest_digest() {
  local image="$1"
  # `imagetools inspect` prints the OCI index digest line as "Digest: ...".
  # We intentionally take the first match to stay deterministic.
  docker buildx imagetools inspect "$image" | awk '/^Digest:/ {print $2; exit}'
}

get_current_digest() {
  local file="$1"
  local image_prefix="$2"
  # Extract only the pinned digest currently referenced in the file.
  # Format expected: image@sha256:<hex>
  grep -Eo "${image_prefix}@sha256:[0-9a-f]+" "$file" | head -n1 | awk -F'@' '{print $2}'
}

update_file() {
  local file="$1"
  local go_digest="$2"
  local static_digest="$3"

  # Use perl for reliable in-place regex replacement on macOS and Linux.
  # .bak is removed immediately after a successful edit.
  perl -i.bak -pe "s#(cgr\.dev/chainguard/go:latest-dev@)sha256:[0-9a-f]+#\$1${go_digest}#g; s#(cgr\.dev/chainguard/static:latest@)sha256:[0-9a-f]+#\$1${static_digest}#g" "$file"
  rm -f "${file}.bak"
}

# Query latest remote digests once, then reuse throughout the script.
latest_go="$(get_latest_digest "$GO_IMAGE")"
latest_static="$(get_latest_digest "$STATIC_IMAGE")"

# Read all current digests from both files so we can report drift clearly.
current_go_docker="$(get_current_digest "$DOCKERFILE" "$GO_IMAGE")"
current_static_docker="$(get_current_digest "$DOCKERFILE" "$STATIC_IMAGE")"
current_go_container="$(get_current_digest "$CONTAINERFILE" "$GO_IMAGE")"
current_static_container="$(get_current_digest "$CONTAINERFILE" "$STATIC_IMAGE")"

# Human-readable summary helps reviewers understand exactly what changed.
echo "Latest:"
echo "  $GO_IMAGE@$latest_go"
echo "  $STATIC_IMAGE@$latest_static"
echo

echo "Current (Dockerfile):"
echo "  $GO_IMAGE@$current_go_docker"
echo "  $STATIC_IMAGE@$current_static_docker"
echo

echo "Current (Containerfile):"
echo "  $GO_IMAGE@$current_go_container"
echo "  $STATIC_IMAGE@$current_static_container"
echo

# Single drift flag keeps branching logic simple for check and update modes.
needs_update=0
if [[ "$current_go_docker" != "$latest_go" || "$current_static_docker" != "$latest_static" || "$current_go_container" != "$latest_go" || "$current_static_container" != "$latest_static" ]]; then
  needs_update=1
fi

# In check mode we report drift and fail intentionally for CI use.
if [[ "$MODE" == "check" ]]; then
  if [[ "$needs_update" -eq 1 ]]; then
    echo "status: out-of-date"
    exit 1
  fi
  echo "status: up-to-date"
  exit 0
fi

# Avoid rewriting files when there is no drift.
if [[ "$needs_update" -eq 0 ]]; then
  echo "status: already up-to-date (no changes made)"
  exit 0
fi

# Update both files so Docker and Podman build definitions remain aligned.
update_file "$DOCKERFILE" "$latest_go" "$latest_static"
update_file "$CONTAINERFILE" "$latest_go" "$latest_static"

echo "status: updated"
