#!/usr/bin/env bats
# Tests for check_markdown_quality.sh using bats-core.

setup() {
  WORKDIR="$(mktemp -d)"
  REPO_ROOT="$WORKDIR/repo"
  SCRIPT_DIR="$REPO_ROOT/scripts"
  BIN_DIR="$WORKDIR/bin"

  mkdir -p "$SCRIPT_DIR" "$BIN_DIR"

  SCRIPT_UNDER_TEST="$SCRIPT_DIR/check_markdown_quality.sh"
  cp "$(cd "$(dirname "$BATS_TEST_FILENAME")" && pwd)/check_markdown_quality.sh" "$SCRIPT_UNDER_TEST"
  chmod +x "$SCRIPT_UNDER_TEST"

  BASE_PATH="$PATH"
}

teardown() {
  rm -rf "$WORKDIR"
}

write_markdown_file() {
  local path="$1"
  local content="$2"

  mkdir -p "$(dirname "$REPO_ROOT/$path")"
  printf '%s' "$content" > "$REPO_ROOT/$path"
}

mock_checker_tools_success() {
  cat > "$BIN_DIR/markdownlint" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
exit 0
EOF

  cat > "$BIN_DIR/lychee" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
exit 0
EOF

  cat > "$BIN_DIR/write-good" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
exit 0
EOF

  chmod +x "$BIN_DIR/markdownlint" "$BIN_DIR/lychee" "$BIN_DIR/write-good"
}

run_script() {
  run env PATH="$BIN_DIR:$BASE_PATH" bash "$SCRIPT_UNDER_TEST"
}

@test "returns success when no markdown files exist" {
  mock_checker_tools_success

  run_script

  [ "$status" -eq 0 ]
  [[ "$output" == *"No markdown files found"* ]]
}

@test "fails when a required tool is missing" {
  write_markdown_file "README.md" '# Title
'

  cat > "$BIN_DIR/markdownlint" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
exit 0
EOF
  cat > "$BIN_DIR/lychee" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
exit 0
EOF
  chmod +x "$BIN_DIR/markdownlint" "$BIN_DIR/lychee"

  run env PATH="$BIN_DIR:/usr/bin:/bin:/usr/sbin:/sbin" "$BASH" "$SCRIPT_UNDER_TEST"

  [ "$status" -eq 1 ]
  [[ "$output" == *"error: required tool not found: write-good"* ]]
}

@test "runs all markdown checkers and reports pass" {
  write_markdown_file "README.md" '# Title
'

  cat > "$BIN_DIR/markdownlint" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "${LOG_FILE}"
exit 0
EOF
  cat > "$BIN_DIR/lychee" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'lychee %s\n' "$*" >> "${LOG_FILE}"
exit 0
EOF
  cat > "$BIN_DIR/write-good" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'write-good %s\n' "$*" >> "${LOG_FILE}"
exit 0
EOF
  chmod +x "$BIN_DIR/markdownlint" "$BIN_DIR/lychee" "$BIN_DIR/write-good"

  LOG_FILE="$WORKDIR/checkers.log"
  run env PATH="$BIN_DIR:$BASE_PATH" LOG_FILE="$LOG_FILE" bash "$SCRIPT_UNDER_TEST"

  [ "$status" -eq 0 ]
  [[ "$output" == *"Markdown quality check passed"* ]]

  run grep -q "lychee" "$LOG_FILE"
  [ "$status" -eq 0 ]

  run grep -q "write-good" "$LOG_FILE"
  [ "$status" -eq 0 ]
}

@test "fails when at least one checker fails" {
  write_markdown_file "README.md" '# Title
'

  cat > "$BIN_DIR/markdownlint" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
exit 0
EOF
  cat > "$BIN_DIR/lychee" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
exit 1
EOF
  cat > "$BIN_DIR/write-good" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
exit 0
EOF
  chmod +x "$BIN_DIR/markdownlint" "$BIN_DIR/lychee" "$BIN_DIR/write-good"

  run_script

  [ "$status" -eq 1 ]
  [[ "$output" == *"Markdown quality check failed"* ]]
}
