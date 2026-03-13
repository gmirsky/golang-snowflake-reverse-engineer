//go:build integration

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// integrationArg reads a required environment variable, skipping the test if absent.
func integrationArg(t *testing.T, key string) string {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		t.Skipf("integration test skipped: %s not set", key)
	}
	return v
}

// integrationKeyPath resolves the private key path from the environment, falling
// back to the repo-local key at ../../keys/rsa_key.p8 if the variable is unset.
func integrationKeyPath(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("SNOWFLAKE_PRIVATE_KEY_PATH"); p != "" {
		return p
	}
	// Go test working directories are set to the package directory, so
	// ../../keys/ resolves to the repository root keys/ folder.
	p := filepath.Join("..", "..", "keys", "rsa_key.p8")
	if _, err := os.Stat(p); err != nil {
		t.Skip("integration test skipped: SNOWFLAKE_PRIVATE_KEY_PATH not set and keys/rsa_key.p8 not found")
	}
	return p
}

// TestIntegrationRunFullPipeline: Given valid Snowflake credentials, when run
// executes, then exit code 0 is returned and SQL output files are written.
func TestIntegrationRunFullPipeline(t *testing.T) {
	account := strings.ToUpper(integrationArg(t, "SNOWFLAKE_ACCOUNT"))
	user := strings.ToUpper(integrationArg(t, "SNOWFLAKE_USER"))
	warehouse := strings.ToUpper(integrationArg(t, "SNOWFLAKE_WAREHOUSE"))
	database := strings.ToUpper(integrationArg(t, "SNOWFLAKE_DATABASE"))
	keyPath := integrationKeyPath(t)

	outputDir := filepath.Join(t.TempDir(), "output")
	logDir := filepath.Join(t.TempDir(), "logs")

	code := run([]string{
		"--account", account,
		"--user", user,
		"--warehouse", warehouse,
		"--database", database,
		"--private-key", keyPath,
		"--output-dir", outputDir,
		"--log-dir", logDir,
	})

	if code != 0 {
		t.Fatalf("run() = %d, want 0", code)
	}

	entries, err := os.ReadDir(outputDir)
	if err != nil {
		t.Fatalf("ReadDir(%q) error = %v", outputDir, err)
	}
	if len(entries) == 0 {
		t.Fatal("expected at least one SQL output file to be written")
	}
	t.Logf("run() wrote %d SQL file(s) to %s", len(entries), outputDir)
}

// TestIntegrationRunExitCode1OnBadKey: Given an invalid private key path, when
// run executes, then exit code 1 is returned for a runtime connection failure.
func TestIntegrationRunExitCode1OnBadKey(t *testing.T) {
	account := integrationArg(t, "SNOWFLAKE_ACCOUNT")
	user := integrationArg(t, "SNOWFLAKE_USER")
	warehouse := integrationArg(t, "SNOWFLAKE_WAREHOUSE")
	database := integrationArg(t, "SNOWFLAKE_DATABASE")

	outputDir := filepath.Join(t.TempDir(), "output")
	logDir := filepath.Join(t.TempDir(), "logs")

	code := run([]string{
		"--account", account,
		"--user", user,
		"--warehouse", warehouse,
		"--database", database,
		"--private-key", "/nonexistent/path/rsa_key.p8",
		"--output-dir", outputDir,
		"--log-dir", logDir,
	})

	if code != 1 {
		t.Fatalf("run() = %d, want 1 for bad key path", code)
	}
}
