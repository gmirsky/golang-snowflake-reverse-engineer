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
