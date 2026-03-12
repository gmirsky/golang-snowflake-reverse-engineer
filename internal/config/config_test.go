package config

import (
	"strings"
	"testing"
)

// TestParseDefaults: Given only required flags, when Parse runs, then optional
// settings must resolve to stable safe defaults.
func TestParseDefaults(t *testing.T) {
	t.Parallel()

	// Arrange + Act: build the minimal valid CLI input and parse it.
	cfg, err := Parse([]string{
		"--user", "demo_user",
		"--account", "demo_account",
		"--warehouse", "demo_wh",
		"--database", "demo_db",
		"--output-dir", "./output",
		"--log-dir", "./logs",
		"--private-key", "./keys/demo.p8",
	})
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	// Assert: defaults should be stable because many downstream behaviors rely on them.
	if cfg.MaxConnections != defaultMaxConnections {
		t.Fatalf("expected default max connections %d, got %d", defaultMaxConnections, cfg.MaxConnections)
	}
	if cfg.RunTimestamp == "" {
		t.Fatal("expected run timestamp to be set")
	}
	if cfg.CompactPackages {
		t.Fatal("expected compact-packages default to be false")
	}
	if cfg.CompactPackagesMaxRuntimes != 0 {
		t.Fatalf("expected default compact-packages-max-runtimes 0, got %d", cfg.CompactPackagesMaxRuntimes)
	}
	if !cfg.CompactPackagesOmitTruncationCount {
		t.Fatal("expected compact-packages-omit-truncation-count default to be true")
	}
}

// TestParseCompactPackagesFlag: Given the compact flag, when Parse runs, then
// compact package mode should be enabled.
func TestParseCompactPackagesFlag(t *testing.T) {
	t.Parallel()

	// Arrange + Act: explicitly pass the flag users enable for compact package output.
	cfg, err := Parse([]string{
		"--user", "demo_user",
		"--account", "demo_account",
		"--warehouse", "demo_wh",
		"--database", "demo_db",
		"--output-dir", "./output",
		"--log-dir", "./logs",
		"--private-key", "./keys/demo.p8",
		"--compact-packages",
	})
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	// Assert: the parsed config should carry the explicit user intent.
	if !cfg.CompactPackages {
		t.Fatal("expected compact-packages to be enabled")
	}
}

// TestParseCompactPackagesMaxRuntimesFlag: Given a runtime cap flag, when
// Parse runs, then the integer value should be preserved exactly.
func TestParseCompactPackagesMaxRuntimesFlag(t *testing.T) {
	t.Parallel()

	// Arrange + Act: parse a command line that sets an explicit runtime cap.
	cfg, err := Parse([]string{
		"--user", "demo_user",
		"--account", "demo_account",
		"--warehouse", "demo_wh",
		"--database", "demo_db",
		"--output-dir", "./output",
		"--log-dir", "./logs",
		"--private-key", "./keys/demo.p8",
		"--compact-packages-max-runtimes", "5",
	})
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	// Assert: no coercion should occur; this value is used directly for truncation logic.
	if cfg.CompactPackagesMaxRuntimes != 5 {
		t.Fatalf("expected compact-packages-max-runtimes=5, got %d", cfg.CompactPackagesMaxRuntimes)
	}
}

// TestParseCompactPackagesOmitTruncationCountFlag: Given the omit-suffix flag,
// when Parse runs, then truncation-count suffix output should be disabled.
func TestParseCompactPackagesOmitTruncationCountFlag(t *testing.T) {
	t.Parallel()

	// Arrange + Act: parse the explicit suffix-suppression flag.
	cfg, err := Parse([]string{
		"--user", "demo_user",
		"--account", "demo_account",
		"--warehouse", "demo_wh",
		"--database", "demo_db",
		"--output-dir", "./output",
		"--log-dir", "./logs",
		"--private-key", "./keys/demo.p8",
		"--compact-packages-omit-truncation-count",
	})
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	// Assert: this switch should be true so compact output omits verbose suffix text.
	if !cfg.CompactPackagesOmitTruncationCount {
		t.Fatal("expected compact-packages-omit-truncation-count to be enabled")
	}
}

// TestValidateRejectsOutOfRangeConnections: Given an invalid connection count,
// when Validate runs, then it must reject the config.
func TestValidateRejectsOutOfRangeConnections(t *testing.T) {
	t.Parallel()

	// Arrange: construct config with an invalid MaxConnections value.
	cfg := Config{
		User:           "u",
		Account:        "a",
		Warehouse:      "w",
		Database:       "d",
		OutputDir:      "./output",
		LogDir:         "./logs",
		PrivateKeyPath: "./key.p8",
		MaxConnections: 10,
	}

	// Act: validate the config.
	err := cfg.Validate()

	// Assert: error should mention the exact field that failed validation.
	if err == nil || !strings.Contains(err.Error(), "max-connections") {
		t.Fatalf("expected max-connections validation error, got %v", err)
	}
}

// TestValidateRejectsNegativeCompactPackagesMaxRuntimes: Given a negative
// runtime cap, when Validate runs, then it must reject that setting.
func TestValidateRejectsNegativeCompactPackagesMaxRuntimes(t *testing.T) {
	t.Parallel()

	// Arrange: runtime cap cannot be negative, so use an invalid value.
	cfg := Config{
		User:                       "u",
		Account:                    "a",
		Warehouse:                  "w",
		Database:                   "d",
		OutputDir:                  "./output",
		LogDir:                     "./logs",
		PrivateKeyPath:             "./key.p8",
		MaxConnections:             3,
		CompactPackagesMaxRuntimes: -1,
	}

	// Act: validate the config.
	err := cfg.Validate()

	// Assert: validation should fail for this specific option.
	if err == nil || !strings.Contains(err.Error(), "compact-packages-max-runtimes") {
		t.Fatalf("expected compact-packages-max-runtimes validation error, got %v", err)
	}
}

// TestRedactedParametersMasksPassphrase: Given a passphrase, when
// RedactedParameters runs, then the output must mask the secret.
func TestRedactedParametersMasksPassphrase(t *testing.T) {
	t.Parallel()

	// Arrange: include a real-looking secret to prove masking always happens.
	cfg := Config{
		User:           "u",
		Account:        "a",
		Warehouse:      "w",
		Database:       "d",
		OutputDir:      "./output",
		LogDir:         "./logs",
		PrivateKeyPath: "./key.p8",
		MaxConnections: 3,
		Passphrase:     "secret",
		RunTimestamp:   "20260311T000000Z",
	}

	// Act: generate log-safe parameter values.
	params := cfg.RedactedParameters()

	// Assert: the secret should be replaced rather than exposed in logs.
	if params["passphrase"] != "***" {
		t.Fatalf("expected masked passphrase, got %q", params["passphrase"])
	}
}
