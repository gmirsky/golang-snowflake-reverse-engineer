#!/usr/bin/env bats
# Tests for update_github_actions.sh using bats-core.
# Run with: bats scripts/update_github_actions.bats

setup() {
  WORKDIR="$(mktemp -d)"
  REPO_ROOT="$WORKDIR/repo"
  SCRIPT_DIR="$REPO_ROOT/scripts"
  WORKFLOW_DIR="$REPO_ROOT/.github/workflows"
  BIN_DIR="$WORKDIR/bin"
  CURL_LOG="$WORKDIR/curl.log"

  mkdir -p "$SCRIPT_DIR" "$WORKFLOW_DIR" "$BIN_DIR"

  SCRIPT_UNDER_TEST="$SCRIPT_DIR/update_github_actions.sh"
  cp "$(cd "$(dirname "$BATS_TEST_FILENAME")" && pwd)/update_github_actions.sh" "$SCRIPT_UNDER_TEST"
  chmod +x "$SCRIPT_UNDER_TEST"

  # Mock curl so tests are deterministic and offline.
  cat > "$BIN_DIR/curl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

# Record all arguments to verify optional auth behavior.
printf '%s\n' "$*" >> "${CURL_LOG}"

args="$*"
if [[ "$args" == *"actions/checkout/releases/latest"* ]]; then
  cat <<'JSON'
{
  "tag_name": "v4.2.2"
}
JSON
elif [[ "$args" == *"actions/setup-go/releases/latest"* ]]; then
  cat <<'JSON'
{
  "tag_name": "v5.0.1"
}
JSON
elif [[ "$args" == *"bad/repo/releases/latest"* ]]; then
  # Missing tag_name to trigger parse failure path.
  cat <<'JSON'
{
  "name": "latest"
}
JSON
else
  # Default fallback for any other action repository.
  cat <<'JSON'
{
  "tag_name": "v1.0.0"
}
JSON
fi
EOF
  chmod +x "$BIN_DIR/curl"

  BASE_PATH="$PATH"
}

teardown() {
  rm -rf "$WORKDIR"
}

write_workflow() {
  local content="$1"
  printf '%s' "$content" > "$WORKFLOW_DIR/ci.yml"
}

run_script() {
  run env PATH="$BIN_DIR:$BASE_PATH" CURL_LOG="$CURL_LOG" bash "$SCRIPT_UNDER_TEST" "$@"
}

@test "invalid argument exits 2 with usage" {
  run_script --bogus
  [ "$status" -eq 2 ]
  [[ "$output" == *"Usage:"* ]]
}

@test "check mode returns 0 when all actions are up-to-date" {
  write_workflow 'name: CI
on: [push]
jobs:
  test:
    steps:
      - name: Checkout
        uses: actions/checkout@v4.2.2 # v4.2.2
      - name: Setup Go
        uses: actions/setup-go@v5.0.1
'

  run_script --check

  [ "$status" -eq 0 ]
  [[ "$output" == *"checked: 2 action reference(s)"* ]]
  [[ "$output" == *"status: up-to-date"* ]]
}

@test "check mode returns 1 when workflow has outdated references and does not edit file" {
  write_workflow 'jobs:
  test:
    steps:
      - name: Checkout
        uses: actions/checkout@v3
      - name: Setup Go
        uses: actions/setup-go@v4
'

  before="$(cat "$WORKFLOW_DIR/ci.yml")"

  run_script --check

  [ "$status" -eq 1 ]
  [[ "$output" == *"outdated: actions/checkout (v3 -> v4.2.2)"* ]]
  [[ "$output" == *"outdated: actions/setup-go (v4 -> v5.0.1)"* ]]
  [[ "$output" == *"status: out-of-date"* ]]

  after="$(cat "$WORKFLOW_DIR/ci.yml")"
  [ "$before" = "$after" ]
}

@test "update mode rewrites outdated refs to latest tags and appends normalized comment" {
  write_workflow 'jobs:
  test:
    steps:
      - name: Checkout
        uses: actions/checkout@v3
      - name: Setup Go
        uses: actions/setup-go@v4 # keep me
'

  run_script --update

  [ "$status" -eq 0 ]
  [[ "$output" == *"updated: 2 action reference(s)"* ]]

  run grep -n "uses: actions/checkout@v4.2.2 # v4.2.2" "$WORKFLOW_DIR/ci.yml"
  [ "$status" -eq 0 ]

  run grep -n "uses: actions/setup-go@v5.0.1 # v5.0.1" "$WORKFLOW_DIR/ci.yml"
  [ "$status" -eq 0 ]
}

@test "default mode is update and reports already up-to-date when no changes are needed" {
  write_workflow 'jobs:
  test:
    steps:
      - name: Checkout
        uses: actions/checkout@v4.2.2
'

  run_script

  [ "$status" -eq 0 ]
  [[ "$output" == *"checked: 1 action reference(s)"* ]]
  [[ "$output" == *"status: already up-to-date (no changes made)"* ]]
}

@test "fails when API payload does not include tag_name" {
  write_workflow 'jobs:
  test:
    steps:
      - name: Bad Repo
        uses: bad/repo@v1
'

  run_script --check

  [ "$status" -eq 1 ]
  [[ "$output" == *"error: could not determine latest release tag for bad/repo"* ]]
}

@test "passes Authorization header to curl when GITHUB_TOKEN is set" {
  write_workflow 'jobs:
  test:
    steps:
      - name: Checkout
        uses: actions/checkout@v3
'

  run env PATH="$BIN_DIR:$BASE_PATH" CURL_LOG="$CURL_LOG" GITHUB_TOKEN="token-123" \
    bash "$SCRIPT_UNDER_TEST" --check

  [ "$status" -eq 1 ]

  run grep -F "Authorization: Bearer token-123" "$CURL_LOG"
  [ "$status" -eq 0 ]
}
