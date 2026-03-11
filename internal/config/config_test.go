package config

import (
	"strings"
	"testing"
)

func TestParseDefaults(t *testing.T) {
	t.Parallel()

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

func TestParseCompactPackagesFlag(t *testing.T) {
	t.Parallel()

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

	if !cfg.CompactPackages {
		t.Fatal("expected compact-packages to be enabled")
	}
}

func TestParseCompactPackagesMaxRuntimesFlag(t *testing.T) {
	t.Parallel()

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

	if cfg.CompactPackagesMaxRuntimes != 5 {
		t.Fatalf("expected compact-packages-max-runtimes=5, got %d", cfg.CompactPackagesMaxRuntimes)
	}
}

func TestParseCompactPackagesOmitTruncationCountFlag(t *testing.T) {
	t.Parallel()

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

	if !cfg.CompactPackagesOmitTruncationCount {
		t.Fatal("expected compact-packages-omit-truncation-count to be enabled")
	}
}

func TestValidateRejectsOutOfRangeConnections(t *testing.T) {
	t.Parallel()

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

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "max-connections") {
		t.Fatalf("expected max-connections validation error, got %v", err)
	}
}

func TestValidateRejectsNegativeCompactPackagesMaxRuntimes(t *testing.T) {
	t.Parallel()

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

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "compact-packages-max-runtimes") {
		t.Fatalf("expected compact-packages-max-runtimes validation error, got %v", err)
	}
}

func TestRedactedParametersMasksPassphrase(t *testing.T) {
	t.Parallel()

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

	params := cfg.RedactedParameters()
	if params["passphrase"] != "***" {
		t.Fatalf("expected masked passphrase, got %q", params["passphrase"])
	}
}
