#!/usr/bin/env bats
# Tests for update_chainguard_images.sh using bats-core.
# Run with: bats scripts/update_chainguard_images.bats

setup() {
  WORKDIR="$(mktemp -d)"
  REPO_ROOT="$WORKDIR/repo"
  SCRIPT_DIR="$REPO_ROOT/scripts"
  BIN_DIR="$WORKDIR/bin"
  DOCKER_LOG="$WORKDIR/docker.log"

  mkdir -p "$SCRIPT_DIR" "$BIN_DIR"

  SCRIPT_UNDER_TEST="$SCRIPT_DIR/update_chainguard_images.sh"
  cp "$(cd "$(dirname "$BATS_TEST_FILENAME")" && pwd)/update_chainguard_images.sh" "$SCRIPT_UNDER_TEST"
  chmod +x "$SCRIPT_UNDER_TEST"

  cat > "$BIN_DIR/docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' "$*" >> "${DOCKER_LOG}"

args="$*"
if [[ "$args" == *"cgr.dev/chainguard/go:latest-dev"* ]]; then
  cat <<'OUT'
Name: cgr.dev/chainguard/go:latest-dev
Digest: sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
OUT
elif [[ "$args" == *"cgr.dev/chainguard/static:latest"* ]]; then
  cat <<'OUT'
Name: cgr.dev/chainguard/static:latest
Digest: sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
OUT
else
  echo "unknown image" >&2
  exit 1
fi
EOF
  chmod +x "$BIN_DIR/docker"

  BASE_PATH="$PATH"
}

teardown() {
  rm -rf "$WORKDIR"
}

write_files() {
  local go_digest="$1"
  local static_digest="$2"

  cat > "$REPO_ROOT/Dockerfile" <<EOF
FROM cgr.dev/chainguard/go:latest-dev@${go_digest} AS builder
FROM cgr.dev/chainguard/static:latest@${static_digest}
EOF

  cat > "$REPO_ROOT/Containerfile" <<EOF
FROM cgr.dev/chainguard/go:latest-dev@${go_digest} AS builder
FROM cgr.dev/chainguard/static:latest@${static_digest}
EOF
}

run_script() {
  run env PATH="$BIN_DIR:$BASE_PATH" DOCKER_LOG="$DOCKER_LOG" bash "$SCRIPT_UNDER_TEST" "$@"
}

@test "invalid argument exits 2 with usage" {
  write_files \
    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" \
    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

  run_script --bogus

  [ "$status" -eq 2 ]
  [[ "$output" == *"Usage:"* ]]
}

@test "check mode returns 0 when both files are up-to-date" {
  write_files \
    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" \
    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

  run_script --check

  [ "$status" -eq 0 ]
  [[ "$output" == *"status: up-to-date"* ]]
}

@test "check mode returns 1 when digests are outdated and does not edit files" {
  write_files \
    "sha256:1111111111111111111111111111111111111111111111111111111111111111" \
    "sha256:2222222222222222222222222222222222222222222222222222222222222222"

  dockerfile_before="$(cat "$REPO_ROOT/Dockerfile")"
  containerfile_before="$(cat "$REPO_ROOT/Containerfile")"

  run_script --check

  [ "$status" -eq 1 ]
  [[ "$output" == *"status: out-of-date"* ]]

  dockerfile_after="$(cat "$REPO_ROOT/Dockerfile")"
  containerfile_after="$(cat "$REPO_ROOT/Containerfile")"
  [ "$dockerfile_before" = "$dockerfile_after" ]
  [ "$containerfile_before" = "$containerfile_after" ]
}

@test "update mode rewrites both files to latest digests" {
  write_files \
    "sha256:1111111111111111111111111111111111111111111111111111111111111111" \
    "sha256:2222222222222222222222222222222222222222222222222222222222222222"

  run_script --update

  [ "$status" -eq 0 ]
  [[ "$output" == *"status: updated"* ]]

  run grep -n "cgr.dev/chainguard/go:latest-dev@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" "$REPO_ROOT/Dockerfile"
  [ "$status" -eq 0 ]
  run grep -n "cgr.dev/chainguard/static:latest@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" "$REPO_ROOT/Dockerfile"
  [ "$status" -eq 0 ]

  run grep -n "cgr.dev/chainguard/go:latest-dev@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" "$REPO_ROOT/Containerfile"
  [ "$status" -eq 0 ]
  run grep -n "cgr.dev/chainguard/static:latest@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" "$REPO_ROOT/Containerfile"
  [ "$status" -eq 0 ]
}

@test "default mode is update and reports already up-to-date when no changes are needed" {
  write_files \
    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" \
    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

  run_script

  [ "$status" -eq 0 ]
  [[ "$output" == *"status: already up-to-date (no changes made)"* ]]
}

@test "fails when docker is not available" {
  write_files \
    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" \
    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

  run env PATH="/usr/bin:/bin" bash "$SCRIPT_UNDER_TEST" --check

  [ "$status" -eq 1 ]
  [[ "$output" == *"error: docker is required"* ]]
}
