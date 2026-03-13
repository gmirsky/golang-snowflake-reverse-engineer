#!/usr/bin/env bats
# Tests for check_comment_style.sh using bats-core.
# Run with: bats scripts/check_comment_style.bats

setup() {
  # Create a temp directory that is cleaned up after every test.
  WORKDIR="$(mktemp -d)"
  SCRIPT="$(cd "$(dirname "$BATS_TEST_FILENAME")/.." && pwd)/scripts/check_comment_style.sh"
}

teardown() {
  rm -rf "$WORKDIR"
}

# ---------------------------------------------------------------------------
# Helper: write a .go file into WORKDIR and run the script from there.
# ---------------------------------------------------------------------------
run_check() {
  local name="$1"
  local content="$2"
  local filepath="$WORKDIR/${name}.go"
  printf '%s' "$content" > "$filepath"
  # Run the script from WORKDIR so it picks up only the test file.
  run bash -c "cd '$WORKDIR' && bash '$SCRIPT'"
}

# ---------------------------------------------------------------------------
# Passing cases
# ---------------------------------------------------------------------------

@test "exported func with correct prefix and Given/when/then passes" {
  run_check "valid_exported" \
'package foo

// Greet: greets a person.
// Given a name, when the function is called, then it returns a greeting.
func Greet(name string) string {
	return "Hello, " + name
}
'
  [ "$status" -eq 0 ]
  [[ "$output" == *"passed"* ]]
}

@test "unexported func without doc comment is ignored" {
  run_check "unexported_no_comment" \
'package foo

func helper() {}
'
  [ "$status" -eq 0 ]
}

@test "unexported func with incorrect comment is ignored" {
  run_check "unexported_wrong_comment" \
'package foo

// wrong prefix
func helper() {}
'
  [ "$status" -eq 0 ]
}

@test "multiple exported funcs all valid passes" {
  run_check "multi_valid" \
'package foo

// Foo: does foo.
// Given input, when called, then it returns foo.
func Foo() {}

// Bar: does bar.
// Given input, when called, then it returns bar.
func Bar() {}
'
  [ "$status" -eq 0 ]
}

@test "exported method on receiver with correct style passes" {
  run_check "method_valid" \
'package foo

type S struct{}

// DoThing: performs the thing.
// Given a receiver, when called, then it executes.
func (s S) DoThing() {}
'
  [ "$status" -eq 0 ]
}

@test "empty go file passes" {
  run_check "empty" \
'package foo
'
  [ "$status" -eq 0 ]
}

@test "file with only unexported funcs passes" {
  run_check "all_unexported" \
'package foo

func a() {}
func b() {}
func c() {}
'
  [ "$status" -eq 0 ]
}

@test "exported func with multi-line comment containing Given when then passes" {
  run_check "multiline_comment" \
'package foo

// Process: processes items.
// Given a list of items,
// when the list is non-empty,
// then each item is processed in order.
func Process() {}
'
  [ "$status" -eq 0 ]
}

# ---------------------------------------------------------------------------
# Failing cases – missing prefix
# ---------------------------------------------------------------------------

@test "exported func missing doc comment entirely fails" {
  run_check "no_comment" \
'package foo

func Export() {}
'
  [ "$status" -ne 0 ]
  [[ "$output" == *"missing style doc comment"* ]]
}

@test "exported func comment prefix does not start with func name fails" {
  run_check "wrong_prefix" \
'package foo

// DoSomethingElse: wrong name.
// Given x, when called, then y.
func Export() {}
'
  [ "$status" -ne 0 ]
  [[ "$output" == *"missing style doc comment for Export"* ]]
}

@test "exported func comment missing colon after name fails" {
  run_check "no_colon" \
'package foo

// Export does something.
// Given input, when called, then output.
func Export() {}
'
  [ "$status" -ne 0 ]
  [[ "$output" == *"missing style doc comment for Export"* ]]
}

# ---------------------------------------------------------------------------
# Failing cases – missing Given/when/then keywords
# ---------------------------------------------------------------------------

@test "exported func comment missing Given fails" {
  run_check "missing_given" \
'package foo

// Export: does export.
// when called, then it returns.
func Export() {}
'
  [ "$status" -ne 0 ]
  [[ "$output" == *"Given/when/then"* ]]
}

@test "exported func comment missing when fails" {
  run_check "missing_when" \
'package foo

// Export: does export.
// Given input, then it returns.
func Export() {}
'
  [ "$status" -ne 0 ]
  [[ "$output" == *"Given/when/then"* ]]
}

@test "exported func comment missing then fails" {
  run_check "missing_then" \
'package foo

// Export: does export.
// Given input, when called.
func Export() {}
'
  [ "$status" -ne 0 ]
  [[ "$output" == *"Given/when/then"* ]]
}

@test "exported func comment with uppercase When does not count as when" {
  run_check "uppercase_when" \
'package foo

// Export: does export.
// Given input, When called, then output.
func Export() {}
'
  # "when" is case-sensitive per the script; "When" should NOT satisfy the check.
  [ "$status" -ne 0 ]
}

@test "exported func comment with uppercase Then does not count as then" {
  run_check "uppercase_then" \
'package foo

// Export: does export.
// Given input, when called, Then output.
func Export() {}
'
  [ "$status" -ne 0 ]
}

# ---------------------------------------------------------------------------
# Failing cases – comment adjacency
# ---------------------------------------------------------------------------

@test "exported func with blank line between comment and func fails" {
  run_check "blank_line_gap" \
'package foo

// Export: describes export.
// Given input, when called, then output.

func Export() {}
'
  [ "$status" -ne 0 ]
  [[ "$output" == *"missing style doc comment for Export"* ]]
}

# ---------------------------------------------------------------------------
# Mixed valid/invalid – script reports all violations
# ---------------------------------------------------------------------------

@test "file with one valid and one invalid exported func fails overall" {
  run_check "mixed" \
'package foo

// Good: does good things.
// Given input, when called, then good things happen.
func Good() {}

func Bad() {}
'
  [ "$status" -ne 0 ]
  [[ "$output" == *"Bad"* ]]
  # Error message should NOT mention Good.
  [[ "$output" != *"Good"* ]]
}

@test "overall exit message says failed when violations exist" {
  run_check "fail_msg" \
'package foo

func Exported() {}
'
  [ "$status" -ne 0 ]
  [[ "$output" == *"Comment style check failed"* ]]
}

@test "overall exit message says passed when all clean" {
  run_check "pass_msg" \
'package foo

// Clean: clean function.
// Given nothing, when called, then it cleans.
func Clean() {}
'
  [ "$status" -eq 0 ]
  [[ "$output" == *"Comment style check passed"* ]]
}
