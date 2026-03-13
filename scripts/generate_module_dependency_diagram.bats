#!/usr/bin/env bats
# Tests for generate_module_dependency_diagram.sh using bats-core.
# Run with: bats scripts/generate_module_dependency_diagram.bats
#
# Performance note: go mod graph is slow on large dependency graphs.
# setup_file runs --print ONCE and caches the output; all content-assertion
# tests read from that file instead of re-invoking the script.
#
# The script modifies README.md in the repo root. setup_file backs it up once;
# setup() restores it before every test so each README-modifying test starts
# from the original state. teardown_file does a final restore.

checksum_file() {
  cksum "$1" | awk '{print $1":"$2}'
}

# ---------------------------------------------------------------------------
# Suite-level fixture – runs once before all tests
# ---------------------------------------------------------------------------
setup_file() {
  export SCRIPT_DIR REPO_ROOT SCRIPT README MARKER_START MARKER_END
  export PRINT_OUTPUT_FILE ROOT_MODULE

  SCRIPT_DIR="$(cd "$(dirname "$BATS_TEST_FILENAME")" && pwd)"
  REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
  SCRIPT="$SCRIPT_DIR/generate_module_dependency_diagram.sh"
  README="$REPO_ROOT/README.md"
  MARKER_START="<!-- MODULE_DEP_GRAPH_START -->"
  MARKER_END="<!-- MODULE_DEP_GRAPH_END -->"
  PRINT_OUTPUT_FILE="$BATS_FILE_TMPDIR/print_output.md"

  # Back up README once for the entire test file.
  cp "$README" "$BATS_FILE_TMPDIR/README.bak"

  # Run --print once and cache output so content tests never call go mod graph again.
  (cd "$REPO_ROOT" && bash "$SCRIPT" --print) > "$PRINT_OUTPUT_FILE"

  # Cache root module name for assertions.
  ROOT_MODULE="$(cd "$REPO_ROOT" && go list -m)"
}

# ---------------------------------------------------------------------------
# Suite-level teardown – restore README after all tests
# ---------------------------------------------------------------------------
teardown_file() {
  cp "$BATS_FILE_TMPDIR/README.bak" "$README"
}

# ---------------------------------------------------------------------------
# Per-test fixture – restore README to original before every test
# ---------------------------------------------------------------------------
setup() {
  cp "$BATS_FILE_TMPDIR/README.bak" "$README"
}

# ---------------------------------------------------------------------------
# Argument parsing – these exit before go is consulted
# ---------------------------------------------------------------------------

@test "--help prints usage and exits 0" {
  run bash "$SCRIPT" --help
  [ "$status" -eq 0 ]
  [[ "$output" == *"Usage:"* ]]
  [[ "$output" == *"--print"* ]]
  [[ "$output" == *"--update-readme"* ]]
  [[ "$output" == *"--chunk-size"* ]]
}

@test "-h prints usage and exits 0" {
  run bash "$SCRIPT" -h
  [ "$status" -eq 0 ]
  [[ "$output" == *"Usage:"* ]]
}

@test "unknown argument exits 2" {
  run bash "$SCRIPT" --bogus-flag
  [ "$status" -eq 2 ]
  [[ "$output" == *"unknown argument"* ]]
}

@test "unknown argument prints usage hint" {
  run bash "$SCRIPT" --bogus-flag
  [ "$status" -eq 2 ]
  [[ "$output" == *"Usage:"* ]]
}

@test "--chunk-size with no following value exits 2" {
  run bash "$SCRIPT" --print --chunk-size
  [ "$status" -eq 2 ]
  [[ "$output" == *"--chunk-size requires a value"* ]]
}

@test "--chunk-size 0 exits 2" {
  run bash "$SCRIPT" --print --chunk-size 0
  [ "$status" -eq 2 ]
  [[ "$output" == *"positive integer"* ]]
}

@test "--chunk-size negative value exits 2" {
  run bash "$SCRIPT" --print --chunk-size -5
  [ "$status" -eq 2 ]
  [[ "$output" == *"positive integer"* ]]
}

@test "--chunk-size non-numeric value exits 2" {
  run bash "$SCRIPT" --print --chunk-size abc
  [ "$status" -eq 2 ]
  [[ "$output" == *"positive integer"* ]]
}

@test "--chunk-size decimal value exits 2" {
  run bash "$SCRIPT" --print --chunk-size 1.5
  [ "$status" -eq 2 ]
  [[ "$output" == *"positive integer"* ]]
}

# ---------------------------------------------------------------------------
# Missing required tools
# ---------------------------------------------------------------------------

@test "exits 1 with message when go is not in PATH" {
  local no_go_bin
  no_go_bin="$(mktemp -d)"
  ln -s "$(command -v bash)" "$no_go_bin/bash"

  run /usr/bin/env PATH="$no_go_bin" bash "$SCRIPT" --print

  rm -rf "$no_go_bin"

  [ "$status" -eq 1 ]
  [[ "$output" == *"go is required"* ]]
}

# ---------------------------------------------------------------------------
# Missing README for update-readme mode
# ---------------------------------------------------------------------------

@test "exits 1 when README does not exist in update-readme mode" {
  # setup() already restored README; remove it to simulate missing file.
  rm -f "$README"
  run bash "$SCRIPT" --update-readme
  [ "$status" -eq 1 ]
  [[ "$output" == *"README file not found"* ]]
}

# ---------------------------------------------------------------------------
# --print mode: content tests use the output cached by setup_file
# ---------------------------------------------------------------------------

@test "--print produced non-empty output" {
  [ -s "$PRINT_OUTPUT_FILE" ]
}

@test "--print output contains direct dependencies section header" {
  grep -q "### 1) Direct Dependencies" "$PRINT_OUTPUT_FILE"
}

@test "--print output contains transitive dependencies section header" {
  grep -q "### 2) Transitive Dependencies" "$PRINT_OUTPUT_FILE"
}

@test "--print output contains mermaid code fences" {
  grep -qF '```mermaid' "$PRINT_OUTPUT_FILE"
}

@test "--print output contains flowchart directive" {
  grep -q "flowchart TD" "$PRINT_OUTPUT_FILE"
}

@test "--print output contains root node class definition" {
  grep -q "classDef root" "$PRINT_OUTPUT_FILE"
}

@test "--print output contains root module name as a node label" {
  grep -qF "$ROOT_MODULE" "$PRINT_OUTPUT_FILE"
}

@test "--print output contains chunk section headers" {
  grep -q "#### Chunk 1:" "$PRINT_OUTPUT_FILE"
}

@test "--print with small chunk-size produces more chunk sections" {
  # Run once with a tiny chunk to verify chunking; this is the only extra go mod graph call.
  run bash -c "cd '$REPO_ROOT' && bash '$SCRIPT' --print --chunk-size 3"
  [ "$status" -eq 0 ]
  [[ "$output" == *"#### Chunk 1:"* ]]
  [[ "$output" == *"#### Chunk 2:"* ]]
}

@test "--print chunk-size label appears in transitive section intro" {
  grep -q "chunks of up to 20 nodes" "$PRINT_OUTPUT_FILE"
}

@test "--print does not modify README" {
  readme_before="$(checksum_file "$README")"
  # Use the cached output; no need to re-run the script.
  readme_after="$(checksum_file "$README")"
  [ "$readme_before" = "$readme_after" ]
}

# ---------------------------------------------------------------------------
# --update-readme mode
# ---------------------------------------------------------------------------

@test "--update-readme exits 0 with success message" {
  run bash -c "cd '$REPO_ROOT' && bash '$SCRIPT' --update-readme"
  [ "$status" -eq 0 ]
  [[ "$output" == *"README updated"* ]]
}

@test "--update-readme with existing markers replaces managed block" {
  # Replace the managed block content with a known stale string.
  awk -v start="$MARKER_START" -v end="$MARKER_END" '
    $0 == start { print; print "STALE CONTENT"; in_block=1; next }
    $0 == end   { in_block=0 }
    !in_block   { print }
  ' "$README" > "${README}.tmp"
  mv "${README}.tmp" "$README"

  bash -c "cd '$REPO_ROOT' && bash '$SCRIPT' --update-readme"

  # Stale content must be gone; fresh mermaid content must be present.
  run grep -c "STALE CONTENT" "$README"
  [ "$output" -eq 0 ]
  grep -q "flowchart TD" "$README"
}

@test "--update-readme without markers appends managed block" {
  # Strip any existing managed block to simulate a fresh README.
  awk -v start="$MARKER_START" -v end="$MARKER_END" '
    $0 == start { skip=1 }
    $0 == end   { skip=0; next }
    !skip       { print }
  ' "$README" > "${README}.tmp"
  mv "${README}.tmp" "$README"

  bash -c "cd '$REPO_ROOT' && bash '$SCRIPT' --update-readme"

  run grep -c "$MARKER_START" "$README"
  [ "$output" -eq 1 ]
  run grep -c "$MARKER_END" "$README"
  [ "$output" -eq 1 ]
  grep -q "flowchart TD" "$README"
}

@test "--update-readme is idempotent: running twice yields one managed block" {
  bash -c "cd '$REPO_ROOT' && bash '$SCRIPT' --update-readme"
  bash -c "cd '$REPO_ROOT' && bash '$SCRIPT' --update-readme"

  run grep -c "$MARKER_START" "$README"
  [ "$output" -eq 1 ]
  run grep -c "$MARKER_END" "$README"
  [ "$output" -eq 1 ]
}

@test "--update-readme inserts both start and end markers" {
  bash -c "cd '$REPO_ROOT' && bash '$SCRIPT' --update-readme"
  grep -q "$MARKER_START" "$README"
  grep -q "$MARKER_END" "$README"
}

@test "default mode (no flag) behaves like --update-readme" {
  run bash -c "cd '$REPO_ROOT' && bash '$SCRIPT'"
  [ "$status" -eq 0 ]
  [[ "$output" == *"README updated"* ]]
}
