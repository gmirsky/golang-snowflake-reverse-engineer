#!/usr/bin/env bats
# Tests for rename_module_path.sh using bats-core.
# Run with: bats scripts/rename_module_path.bats

setup() {
  WORKDIR="$(mktemp -d)"
  REPO_ROOT="$WORKDIR/repo"
  SCRIPT_DIR="$REPO_ROOT/scripts"

  mkdir -p "$SCRIPT_DIR" "$REPO_ROOT/internal/app" "$REPO_ROOT/output" "$REPO_ROOT/logs"

  SCRIPT_UNDER_TEST="$SCRIPT_DIR/rename_module_path.sh"
  cp "$(cd "$(dirname "$BATS_TEST_FILENAME")" && pwd)/rename_module_path.sh" "$SCRIPT_UNDER_TEST"
  chmod +x "$SCRIPT_UNDER_TEST"

  cat > "$REPO_ROOT/go.mod" <<'EOF'
module github.com/old/repo

go 1.24
EOF

  cat > "$REPO_ROOT/internal/app/app.go" <<'EOF'
package app

import "github.com/old/repo/internal/reverseengineer"

func Use() string {
	return reverseengineer.Name()
}
EOF

  cat > "$REPO_ROOT/README.md" <<'EOF'
# Example

Module: github.com/old/repo
EOF

  # These directories are intentionally excluded by the script.
  cat > "$REPO_ROOT/output/ignored.txt" <<'EOF'
github.com/old/repo
EOF

  cat > "$REPO_ROOT/logs/ignored.txt" <<'EOF'
github.com/old/repo
EOF
}

teardown() {
  rm -rf "$WORKDIR"
}

run_script() {
  run bash "$SCRIPT_UNDER_TEST" "$@"
}

@test "help flag exits 0 and prints usage" {
  run_script --help

  [ "$status" -eq 0 ]
  [[ "$output" == *"Usage:"* ]]
  [[ "$output" == *"--dry-run"* ]]
}

@test "missing module argument exits 2" {
  run_script

  [ "$status" -eq 2 ]
  [[ "$output" == *"new module path is required"* ]]
}

@test "unknown option exits 2" {
  run_script --bogus github.com/new/repo

  [ "$status" -eq 2 ]
  [[ "$output" == *"unknown argument"* ]]
}

@test "module path with whitespace exits 2" {
  run_script "github.com/new repo"

  [ "$status" -eq 2 ]
  [[ "$output" == *"must not contain whitespace"* ]]
}

@test "module path with unsupported characters exits 2" {
  run_script "github.com/new/repo?bad"

  [ "$status" -eq 2 ]
  [[ "$output" == *"unsupported characters"* ]]
}

@test "returns 0 when module path is already set" {
  run_script github.com/old/repo

  [ "$status" -eq 0 ]
  [[ "$output" == *"module path already set to github.com/old/repo"* ]]
}

@test "dry-run lists candidate files and does not modify content" {
  before_go_mod="$(cat "$REPO_ROOT/go.mod")"
  before_app="$(cat "$REPO_ROOT/internal/app/app.go")"
  before_readme="$(cat "$REPO_ROOT/README.md")"

  run_script --dry-run github.com/new/repo

  [ "$status" -eq 0 ]
  [[ "$output" == *"dry run:"* ]]
  [[ "$output" == *"go.mod"* ]]
  [[ "$output" == *"internal/app/app.go"* ]]
  [[ "$output" == *"README.md"* ]]
  [[ "$output" != *"output/ignored.txt"* ]]
  [[ "$output" != *"logs/ignored.txt"* ]]

  after_go_mod="$(cat "$REPO_ROOT/go.mod")"
  after_app="$(cat "$REPO_ROOT/internal/app/app.go")"
  after_readme="$(cat "$REPO_ROOT/README.md")"

  [ "$before_go_mod" = "$after_go_mod" ]
  [ "$before_app" = "$after_app" ]
  [ "$before_readme" = "$after_readme" ]
}

@test "update mode replaces module path in tracked text files" {
  run_script github.com/new/repo

  [ "$status" -eq 0 ]
  [[ "$output" == *"updated"* ]]
  [[ "$output" == *"go.mod"* ]]

  run grep -n "module github.com/new/repo" "$REPO_ROOT/go.mod"
  [ "$status" -eq 0 ]

  run grep -n "github.com/new/repo/internal/reverseengineer" "$REPO_ROOT/internal/app/app.go"
  [ "$status" -eq 0 ]

  run grep -n "Module: github.com/new/repo" "$REPO_ROOT/README.md"
  [ "$status" -eq 0 ]
}

@test "excluded directories remain unchanged during update" {
  run_script github.com/new/repo

  [ "$status" -eq 0 ]

  run grep -n "github.com/old/repo" "$REPO_ROOT/output/ignored.txt"
  [ "$status" -eq 0 ]

  run grep -n "github.com/old/repo" "$REPO_ROOT/logs/ignored.txt"
  [ "$status" -eq 0 ]
}
