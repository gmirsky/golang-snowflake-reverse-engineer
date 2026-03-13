package main

import (
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"

	appconfig "github.com/gmirsky/golang-snowflake-reverse-engineer/internal/config"
)

// TestRunExitCode2OnUnknownFlag: Given an unknown CLI flag, when run executes,
// then exit code 2 is returned to indicate a flag parse error.
func TestRunExitCode2OnUnknownFlag(t *testing.T) {
	t.Parallel()

	if code := run([]string{"--unknown-flag-xyz"}); code != 2 {
		t.Fatalf("run() = %d, want 2 for unknown flag", code)
	}
}

// TestRunExitCode2OnMissingRequiredFlags: Given no flags, when run executes,
// then exit code 2 is returned because all required flags are missing.
func TestRunExitCode2OnMissingRequiredFlags(t *testing.T) {
	t.Parallel()

	if code := run([]string{}); code != 2 {
		t.Fatalf("run() = %d, want 2 when required flags are absent", code)
	}
}

// TestRunExitCode1OnRuntimeFailure: Given valid parsed config, when app
// execution returns an error, then run returns exit code 1.
func TestRunExitCode1OnRuntimeFailure(t *testing.T) {
	originalParseConfig := parseConfig
	originalRunApp := runApp
	t.Cleanup(func() {
		parseConfig = originalParseConfig
		runApp = originalRunApp
	})

	parseConfig = func([]string) (appconfig.Config, error) {
		return appconfig.Config{}, nil
	}
	runApp = func(appconfig.Config) error {
		return errors.New("runtime failure")
	}

	if code := run([]string{"--ignored"}); code != 1 {
		t.Fatalf("run() = %d, want 1 when app execution fails", code)
	}
}

// TestRunExitCode0OnSuccess: Given valid parsed config, when app execution
// succeeds, then run returns exit code 0.
func TestRunExitCode0OnSuccess(t *testing.T) {
	originalParseConfig := parseConfig
	originalRunApp := runApp
	t.Cleanup(func() {
		parseConfig = originalParseConfig
		runApp = originalRunApp
	})

	parseConfig = func([]string) (appconfig.Config, error) {
		return appconfig.Config{}, nil
	}
	runApp = func(appconfig.Config) error {
		return nil
	}

	if code := run([]string{"--ignored"}); code != 0 {
		t.Fatalf("run() = %d, want 0 when app execution succeeds", code)
	}
}

// TestMainPropagatesRunExitCode: Given main invokes run in a helper process,
// when run exits with a flag-parse failure, then main propagates exit code 2.
func TestMainPropagatesRunExitCode(t *testing.T) {
	t.Parallel()

	cmd := exec.Command(os.Args[0], "-test.run=TestMainProcessHelper")
	cmd.Env = append(os.Environ(),
		"GO_WANT_HELPER_PROCESS=1",
		"HELPER_ARGS=--unknown-flag-xyz",
	)

	err := cmd.Run()
	if err == nil {
		t.Fatal("expected helper process to exit non-zero")
	}

	exitErr, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("expected ExitError, got %T (%v)", err, err)
	}
	if exitErr.ExitCode() != 2 {
		t.Fatalf("main() exit code = %d, want 2", exitErr.ExitCode())
	}
}

// TestMainProcessHelper: Given the helper-process env var is set, when the test
// helper rewrites os.Args and calls main, then the process exits via main.
func TestMainProcessHelper(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}

	args := strings.Fields(os.Getenv("HELPER_ARGS"))
	os.Args = append([]string{"snowflake-reverse-engineer"}, args...)
	main()
}
