#!/usr/bin/env bash
set -euo pipefail

# Check and update pinned Chainguard image digests in Dockerfile and Containerfile.
# Requires: docker with buildx support.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DOCKERFILE="$ROOT_DIR/Dockerfile"
CONTAINERFILE="$ROOT_DIR/Containerfile"

GO_IMAGE="cgr.dev/chainguard/go:latest-dev"
STATIC_IMAGE="cgr.dev/chainguard/static:latest"

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
  docker buildx imagetools inspect "$image" | awk '/^Digest:/ {print $2; exit}'
}

get_current_digest() {
  local file="$1"
  local image_prefix="$2"
  grep -Eo "${image_prefix}@sha256:[0-9a-f]+" "$file" | head -n1 | awk -F'@' '{print $2}'
}

update_file() {
  local file="$1"
  local go_digest="$2"
  local static_digest="$3"

  perl -i.bak -pe "s#(cgr\.dev/chainguard/go:latest-dev@)sha256:[0-9a-f]+#\$1${go_digest}#g; s#(cgr\.dev/chainguard/static:latest@)sha256:[0-9a-f]+#\$1${static_digest}#g" "$file"
  rm -f "${file}.bak"
}

latest_go="$(get_latest_digest "$GO_IMAGE")"
latest_static="$(get_latest_digest "$STATIC_IMAGE")"

current_go_docker="$(get_current_digest "$DOCKERFILE" "$GO_IMAGE")"
current_static_docker="$(get_current_digest "$DOCKERFILE" "$STATIC_IMAGE")"
current_go_container="$(get_current_digest "$CONTAINERFILE" "$GO_IMAGE")"
current_static_container="$(get_current_digest "$CONTAINERFILE" "$STATIC_IMAGE")"

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

needs_update=0
if [[ "$current_go_docker" != "$latest_go" || "$current_static_docker" != "$latest_static" || "$current_go_container" != "$latest_go" || "$current_static_container" != "$latest_static" ]]; then
  needs_update=1
fi

if [[ "$MODE" == "check" ]]; then
  if [[ "$needs_update" -eq 1 ]]; then
    echo "status: out-of-date"
    exit 1
  fi
  echo "status: up-to-date"
  exit 0
fi

if [[ "$needs_update" -eq 0 ]]; then
  echo "status: already up-to-date (no changes made)"
  exit 0
fi

update_file "$DOCKERFILE" "$latest_go" "$latest_static"
update_file "$CONTAINERFILE" "$latest_go" "$latest_static"

echo "status: updated"
