//go:build integration

// Package snowflake integration tests exercise all Repository methods against
// a live Snowflake account. They are gated behind the "integration" build tag
// and require the following environment variables:
//
//   - SNOWFLAKE_ACCOUNT   – Snowflake account identifier (e.g. ORG-ACCOUNT_NAME)
//   - SNOWFLAKE_USER      – Snowflake user name
//   - SNOWFLAKE_WAREHOUSE – virtual warehouse to use
//   - SNOWFLAKE_DATABASE  – target database to query
//
// Optional:
//
//   - SNOWFLAKE_PRIVATE_KEY_PATH – path to RSA private key (defaults to ../../keys/rsa_key.p8)
//   - SNOWFLAKE_PASSPHRASE       – key passphrase (defaults to empty / unencrypted key)
//
// Run with: go test -tags integration ./internal/snowflake/...
package snowflake

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	appconfig "github.com/gmirsky/golang-snowflake-reverse-engineer/internal/config"
	"github.com/gmirsky/golang-snowflake-reverse-engineer/internal/reverseengineer"
)

// integrationCfg builds an appconfig.Config from environment variables.
// It calls t.Skip when any required variable is absent or the key file cannot
// be found, so integration tests are silently bypassed in environments without
// Snowflake access.
func integrationCfg(t *testing.T) appconfig.Config {
	t.Helper()
	requireEnv := func(key string) string {
		v := os.Getenv(key)
		if v == "" {
			t.Skipf("integration test skipped: %s not set", key)
		}
		return strings.ToUpper(v) // normalize identifiers consistently with config.Parse
	}

	keyPath := os.Getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
	if keyPath == "" {
		// Go tests run with cwd set to the package directory; ../../keys/ is the repo root.
		keyPath = filepath.Join("..", "..", "keys", "rsa_key.p8")
	}
	if _, err := os.Stat(keyPath); err != nil {
		t.Skipf("integration test skipped: private key not accessible at %s", keyPath)
	}

	return appconfig.Config{
		Account:        requireEnv("SNOWFLAKE_ACCOUNT"),
		User:           requireEnv("SNOWFLAKE_USER"),
		Warehouse:      requireEnv("SNOWFLAKE_WAREHOUSE"),
		Database:       requireEnv("SNOWFLAKE_DATABASE"),
		PrivateKeyPath: keyPath,
		Passphrase:     os.Getenv("SNOWFLAKE_PASSPHRASE"),
		MaxConnections: 1,
	}
}

// openRepo is a test helper that creates a Repository and registers cleanup.
func openRepo(t *testing.T, cfg appconfig.Config) *Repository {
	t.Helper()
	repo, err := NewRepository(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })
	return repo
}

// TestIntegrationNewRepository: Given valid credentials, when NewRepository
// runs, then a live connection pool is returned with no error.
func TestIntegrationNewRepository(t *testing.T) {
	cfg := integrationCfg(t)
	repo := openRepo(t, cfg)
	if repo == nil {
		t.Fatal("expected non-nil Repository")
	}
}

// TestIntegrationListViews: Given a connected repository, when ListViews runs,
// then a non-empty slice of INFORMATION_SCHEMA view names is returned.
func TestIntegrationListViews(t *testing.T) {
	cfg := integrationCfg(t)
	repo := openRepo(t, cfg)

	views, err := repo.ListViews(context.Background(), cfg.Database)
	if err != nil {
		t.Fatalf("ListViews() error = %v", err)
	}
	if len(views) == 0 {
		t.Fatal("expected at least one view in INFORMATION_SCHEMA")
	}
	t.Logf("ListViews() returned %d view(s)", len(views))
}

// TestIntegrationFetchViewRows: Given a connected repository, when FetchViewRows
// runs against INFORMATION_SCHEMA.SCHEMATA, then rows with uppercase keys are
// returned.
func TestIntegrationFetchViewRows(t *testing.T) {
	cfg := integrationCfg(t)
	repo := openRepo(t, cfg)

	rows, err := repo.FetchViewRows(context.Background(), cfg.Database, "SCHEMATA")
	if err != nil {
		t.Fatalf("FetchViewRows(SCHEMATA) error = %v", err)
	}
	if len(rows) == 0 {
		t.Fatal("expected at least one row in INFORMATION_SCHEMA.SCHEMATA")
	}

	// Verify every column key is uppercased as required by the interface contract.
	for _, row := range rows {
		for key := range row {
			if key != strings.ToUpper(key) {
				t.Errorf("row key %q is not uppercase", key)
			}
		}
	}
	t.Logf("FetchViewRows(SCHEMATA) returned %d row(s)", len(rows))
}

// TestIntegrationFetchDDL: Given a connected repository, when FetchDDL runs
// for the target database, then a non-empty DDL string containing CREATE is
// returned.
func TestIntegrationFetchDDL(t *testing.T) {
	cfg := integrationCfg(t)
	repo := openRepo(t, cfg)

	req := reverseengineer.DDLRequest{
		ObjectType:    "DATABASE",
		QualifiedName: `"` + cfg.Database + `"`,
	}

	ddl, err := repo.FetchDDL(context.Background(), req)
	if err != nil {
		t.Fatalf("FetchDDL(DATABASE, %q) error = %v", cfg.Database, err)
	}
	if !strings.Contains(strings.ToUpper(ddl), "CREATE") {
		t.Fatalf("expected DDL to contain CREATE, got: %s", ddl)
	}
	t.Logf("FetchDDL(DATABASE) returned %d chars", len(ddl))
}

// TestIntegrationListStorageIntegrations: Given a connected repository, when
// ListStorageIntegrations runs, then a slice is returned without error. The
// slice may be empty if no storage integrations exist on the account.
func TestIntegrationListStorageIntegrations(t *testing.T) {
	cfg := integrationCfg(t)
	repo := openRepo(t, cfg)

	names, err := repo.ListStorageIntegrations(context.Background())
	if err != nil {
		t.Fatalf("ListStorageIntegrations() error = %v", err)
	}
	t.Logf("ListStorageIntegrations() returned %d integration(s)", len(names))
}

// TestIntegrationDescStorageIntegration: Given at least one storage integration,
// when DescStorageIntegration runs, then property rows are returned without
// error. The test is skipped when no storage integrations exist.
func TestIntegrationDescStorageIntegration(t *testing.T) {
	cfg := integrationCfg(t)
	repo := openRepo(t, cfg)

	names, err := repo.ListStorageIntegrations(context.Background())
	if err != nil {
		t.Fatalf("ListStorageIntegrations() error = %v", err)
	}
	if len(names) == 0 {
		t.Skip("no storage integrations found; skipping DescStorageIntegration")
	}

	rows, err := repo.DescStorageIntegration(context.Background(), names[0])
	if err != nil {
		t.Fatalf("DescStorageIntegration(%q) error = %v", names[0], err)
	}
	if len(rows) == 0 {
		t.Fatalf("expected at least one property row from DESC STORAGE INTEGRATION %q", names[0])
	}
	t.Logf("DescStorageIntegration(%q) returned %d property row(s)", names[0], len(rows))
}
