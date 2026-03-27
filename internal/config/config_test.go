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

// TestLogFileName: Given timestamp settings, when log filename generation
// runs, then names should include or omit timestamp as configured.
func TestLogFileName(t *testing.T) {
	t.Parallel()

	t.Run("timestamp disabled", func(t *testing.T) {
		cfg := Config{TimestampedOutput: false, RunTimestamp: "20260312T190000Z"}
		if got := cfg.LogFileName(); got != "snowflake-reverse-engineer.log" {
			t.Fatalf("LogFileName() = %q, want %q", got, "snowflake-reverse-engineer.log")
		}
	})

	t.Run("timestamp enabled", func(t *testing.T) {
		cfg := Config{TimestampedOutput: true, RunTimestamp: "20260312T190000Z"}
		if got := cfg.LogFileName(); got != "snowflake-reverse-engineer_20260312T190000Z.log" {
			t.Fatalf("LogFileName() = %q, want %q", got, "snowflake-reverse-engineer_20260312T190000Z.log")
		}
	})
}

// TestOutputFileName: Given a view name and timestamp setting, when output
// filename generation runs, then names should be lowercase with optional suffix.
func TestOutputFileName(t *testing.T) {
	t.Parallel()

	t.Run("lowercase without timestamp", func(t *testing.T) {
		cfg := Config{TimestampedOutput: false}
		if got := cfg.OutputFileName("TABLES"); got != "tables.sql" {
			t.Fatalf("OutputFileName() = %q, want %q", got, "tables.sql")
		}
	})

	t.Run("lowercase with timestamp", func(t *testing.T) {
		cfg := Config{TimestampedOutput: true, RunTimestamp: "20260312T190000Z"}
		if got := cfg.OutputFileName("TABLES"); got != "tables_20260312T190000Z.sql" {
			t.Fatalf("OutputFileName() = %q, want %q", got, "tables_20260312T190000Z.sql")
		}
	})
}

// TestWithTimestamp: Given base filename variations, when timestamp insertion
// runs, then suffix placement should be deterministic.
func TestWithTimestamp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		fileName  string
		enabled   bool
		timestamp string
		want      string
	}{
		{name: "disabled", fileName: "tables.sql", enabled: false, timestamp: "20260312T190000Z", want: "tables.sql"},
		{name: "enabled with extension", fileName: "tables.sql", enabled: true, timestamp: "20260312T190000Z", want: "tables_20260312T190000Z.sql"},
		{name: "enabled no extension", fileName: "tables", enabled: true, timestamp: "20260312T190000Z", want: "tables_20260312T190000Z"},
	}

	for _, testCase := range tests {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := withTimestamp(testCase.fileName, testCase.enabled, testCase.timestamp); got != testCase.want {
				t.Fatalf("withTimestamp(%q, %t, %q) = %q, want %q", testCase.fileName, testCase.enabled, testCase.timestamp, got, testCase.want)
			}
		})
	}
}

// TestParseVerboseFlag: Given the --verbose flag, when Parse runs, then
// verbose mode should be enabled in the returned config.
func TestParseVerboseFlag(t *testing.T) {
	t.Parallel()

	cfg, err := Parse([]string{
		"--user", "demo_user",
		"--account", "demo_account",
		"--warehouse", "demo_wh",
		"--database", "demo_db",
		"--output-dir", "./output",
		"--log-dir", "./logs",
		"--private-key", "./keys/demo.p8",
		"--verbose",
	})
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	if !cfg.Verbose {
		t.Fatal("expected verbose to be true when --verbose flag is passed")
	}
}

// TestParseTimestampedOutputFlag: Given the --timestamped-output flag, when
// Parse runs, then timestamped output should be enabled.
func TestParseTimestampedOutputFlag(t *testing.T) {
	t.Parallel()

	cfg, err := Parse([]string{
		"--user", "demo_user",
		"--account", "demo_account",
		"--warehouse", "demo_wh",
		"--database", "demo_db",
		"--output-dir", "./output",
		"--log-dir", "./logs",
		"--private-key", "./keys/demo.p8",
		"--timestamped-output",
	})
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	if !cfg.TimestampedOutput {
		t.Fatal("expected timestamped-output to be true when --timestamped-output flag is passed")
	}
}

// TestParsePassphraseFlag: Given the --passphrase flag, when Parse runs, then
// the passphrase value should be preserved exactly.
func TestParsePassphraseFlag(t *testing.T) {
	t.Parallel()

	passphrase := "s3cr3t"
	cfg, err := Parse([]string{
		"--user", "demo_user",
		"--account", "demo_account",
		"--warehouse", "demo_wh",
		"--database", "demo_db",
		"--output-dir", "./output",
		"--log-dir", "./logs",
		"--private-key", "./keys/demo.p8",
		"--passphrase", passphrase,
	})
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	if cfg.Passphrase != passphrase {
		t.Fatalf("expected passphrase %q, got %q", passphrase, cfg.Passphrase)
	}
}

// TestParseUppercasesIdentifiers: Given lowercase flag values, when Parse
// runs, then user, account, warehouse, and database should be uppercased.
func TestParseUppercasesIdentifiers(t *testing.T) {
	t.Parallel()

	cfg, err := Parse([]string{
		"--user", "myuser",
		"--account", "myaccount",
		"--warehouse", "mywh",
		"--database", "mydb",
		"--output-dir", "./output",
		"--log-dir", "./logs",
		"--private-key", "./keys/demo.p8",
	})
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	if cfg.User != "MYUSER" {
		t.Fatalf("expected User %q, got %q", "MYUSER", cfg.User)
	}
	if cfg.Account != "MYACCOUNT" {
		t.Fatalf("expected Account %q, got %q", "MYACCOUNT", cfg.Account)
	}
	if cfg.Warehouse != "MYWH" {
		t.Fatalf("expected Warehouse %q, got %q", "MYWH", cfg.Warehouse)
	}
	if cfg.Database != "MYDB" {
		t.Fatalf("expected Database %q, got %q", "MYDB", cfg.Database)
	}
}

// TestValidateRejectsTooFewConnections: Given MaxConnections below the minimum,
// when Validate runs, then it must reject the config.
func TestValidateRejectsTooFewConnections(t *testing.T) {
	t.Parallel()

	cfg := Config{
		User:           "u",
		Account:        "a",
		Warehouse:      "w",
		Database:       "d",
		OutputDir:      "./output",
		LogDir:         "./logs",
		PrivateKeyPath: "./key.p8",
		MaxConnections: 0,
	}

	err := cfg.Validate()

	if err == nil || !strings.Contains(err.Error(), "max-connections") {
		t.Fatalf("expected max-connections validation error for value 0, got %v", err)
	}
}

// TestValidateRejectsDotOutputDir: Given "." as the output directory, when
// Validate runs, then it must reject degenerate path to prevent accidental writes.
func TestValidateRejectsDotOutputDir(t *testing.T) {
	t.Parallel()

	cfg := Config{
		User:           "u",
		Account:        "a",
		Warehouse:      "w",
		Database:       "d",
		OutputDir:      ".",
		LogDir:         "./logs",
		PrivateKeyPath: "./key.p8",
		MaxConnections: 3,
	}

	err := cfg.Validate()

	if err == nil || !strings.Contains(err.Error(), "explicit paths") {
		t.Fatalf("expected explicit-paths validation error for OutputDir='.', got %v", err)
	}
}

// TestRedactedParametersNullPassphrase: Given an empty passphrase, when
// RedactedParameters runs, then the passphrase field should read "null".
func TestRedactedParametersNullPassphrase(t *testing.T) {
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
		Passphrase:     "",
	}

	params := cfg.RedactedParameters()

	if params["passphrase"] != "null" {
		t.Fatalf("expected passphrase to be \"null\" when empty, got %q", params["passphrase"])
	}
}
