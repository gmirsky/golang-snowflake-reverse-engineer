package reverseengineer

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	appconfig "github.com/gmirsky/golang-snowflake-reverse-engineer/internal/config"
)

type fakeRepo struct {
	views               []string
	rows                map[string][]Row
	ddls                map[string]string
	storageIntegrations []string
	storageIntegRows    map[string][]Row
}

func (f fakeRepo) ListViews(_ context.Context, _ string) ([]string, error) {
	return f.views, nil
}

func (f fakeRepo) FetchViewRows(_ context.Context, _ string, viewName string) ([]Row, error) {
	return f.rows[viewName], nil
}

func (f fakeRepo) FetchDDL(_ context.Context, request DDLRequest) (string, error) {
	return f.ddls[request.QualifiedName], nil
}

func (f fakeRepo) ListStorageIntegrations(_ context.Context) ([]string, error) {
	return f.storageIntegrations, nil
}

func (f fakeRepo) DescStorageIntegration(_ context.Context, name string) ([]Row, error) {
	if f.storageIntegRows == nil {
		return nil, nil
	}
	return f.storageIntegRows[name], nil
}

func TestServiceRunWritesFiles(t *testing.T) {
	t.Parallel()

	outputDir := t.TempDir()
	cfg := appconfig.Config{
		Database:       "DEMO_DB",
		OutputDir:      outputDir,
		LogDir:         outputDir,
		MaxConnections: 2,
	}

	repo := fakeRepo{
		views: []string{"TABLES", "EMPTY_VIEW"},
		rows: map[string][]Row{
			"TABLES": {
				{
					"TABLE_CATALOG": "DEMO_DB",
					"TABLE_SCHEMA":  "PUBLIC",
					"TABLE_NAME":    "CUSTOMERS",
					"TABLE_TYPE":    "BASE TABLE",
				},
			},
			"EMPTY_VIEW": {},
		},
		ddls: map[string]string{
			`"DEMO_DB"."PUBLIC"."CUSTOMERS"`: "create table customers(id number)",
		},
	}

	service := NewService(repo, log.New(io.Discard, "", 0), cfg)
	summary, err := service.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if summary.FilesWritten != 3 {
		t.Fatalf("expected 3 files, got %d", summary.FilesWritten)
	}

	tablesContent, err := os.ReadFile(filepath.Join(outputDir, "tables.sql"))
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if !strings.Contains(string(tablesContent), "create table customers") {
		t.Fatalf("expected generated DDL in output, got %s", string(tablesContent))
	}

	emptyContent, err := os.ReadFile(filepath.Join(outputDir, "empty_view.sql"))
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if !strings.Contains(string(emptyContent), "No data found") {
		t.Fatalf("expected no data comment, got %s", string(emptyContent))
	}
}

func TestServiceRunCompactsPackages(t *testing.T) {
	t.Parallel()

	outputDir := t.TempDir()
	cfg := appconfig.Config{
		Database:        "DEMO_DB",
		OutputDir:       outputDir,
		LogDir:          outputDir,
		MaxConnections:  2,
		CompactPackages: true,
	}

	repo := fakeRepo{
		views: []string{"PACKAGES"},
		rows: map[string][]Row{
			"PACKAGES": {
				{
					"PACKAGE_NAME":    "abi3audit",
					"VERSION":         "0.0.24",
					"LANGUAGE":        "python",
					"RUNTIME_VERSION": "3.10",
				},
				{
					"PACKAGE_NAME":    "abi3audit",
					"VERSION":         "0.0.24",
					"LANGUAGE":        "python",
					"RUNTIME_VERSION": "3.11",
				},
				{
					"PACKAGE_NAME": "abi3audit",
					"VERSION":      "0.0.24",
					"LANGUAGE":     "python",
				},
			},
		},
		ddls: map[string]string{},
	}

	service := NewService(repo, log.New(io.Discard, "", 0), cfg)
	summary, err := service.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if summary.StatementsGenerated != 1 {
		t.Fatalf("expected 1 compact package statement, got %d", summary.StatementsGenerated)
	}

	content, err := os.ReadFile(filepath.Join(outputDir, "packages.sql"))
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

	output := string(content)
	if !strings.Contains(output, "runtimes [\"3.10\", \"3.11\", \"default\"]") {
		t.Fatalf("expected grouped runtimes in output, got %s", output)
	}
}

func TestServiceRunCompactsPackagesWithRuntimeCap(t *testing.T) {
	t.Parallel()

	outputDir := t.TempDir()
	cfg := appconfig.Config{
		Database:                   "DEMO_DB",
		OutputDir:                  outputDir,
		LogDir:                     outputDir,
		MaxConnections:             2,
		CompactPackages:            true,
		CompactPackagesMaxRuntimes: 2,
	}

	repo := fakeRepo{
		views: []string{"PACKAGES"},
		rows: map[string][]Row{
			"PACKAGES": {
				{"PACKAGE_NAME": "abi3audit", "VERSION": "0.0.24", "LANGUAGE": "python", "RUNTIME_VERSION": "3.10"},
				{"PACKAGE_NAME": "abi3audit", "VERSION": "0.0.24", "LANGUAGE": "python", "RUNTIME_VERSION": "3.11"},
				{"PACKAGE_NAME": "abi3audit", "VERSION": "0.0.24", "LANGUAGE": "python", "RUNTIME_VERSION": "3.12"},
			},
		},
		ddls: map[string]string{},
	}

	service := NewService(repo, log.New(io.Discard, "", 0), cfg)
	_, err := service.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	content, err := os.ReadFile(filepath.Join(outputDir, "packages.sql"))
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

	output := string(content)
	if !strings.Contains(output, "runtimes [\"3.10\", \"3.11\"] (truncated, 1 more)") {
		t.Fatalf("expected capped runtime list in output, got %s", output)
	}
}

func TestServiceRunCompactsPackagesWithRuntimeCapOmitTruncationCount(t *testing.T) {
	t.Parallel()

	outputDir := t.TempDir()
	cfg := appconfig.Config{
		Database:                           "DEMO_DB",
		OutputDir:                          outputDir,
		LogDir:                             outputDir,
		MaxConnections:                     2,
		CompactPackages:                    true,
		CompactPackagesMaxRuntimes:         2,
		CompactPackagesOmitTruncationCount: true,
	}

	repo := fakeRepo{
		views: []string{"PACKAGES"},
		rows: map[string][]Row{
			"PACKAGES": {
				{"PACKAGE_NAME": "abi3audit", "VERSION": "0.0.24", "LANGUAGE": "python", "RUNTIME_VERSION": "3.10"},
				{"PACKAGE_NAME": "abi3audit", "VERSION": "0.0.24", "LANGUAGE": "python", "RUNTIME_VERSION": "3.11"},
				{"PACKAGE_NAME": "abi3audit", "VERSION": "0.0.24", "LANGUAGE": "python", "RUNTIME_VERSION": "3.12"},
			},
		},
		ddls: map[string]string{},
	}

	service := NewService(repo, log.New(io.Discard, "", 0), cfg)
	_, err := service.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	content, err := os.ReadFile(filepath.Join(outputDir, "packages.sql"))
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

	output := string(content)
	if !strings.Contains(output, "runtimes [\"3.10\", \"3.11\"]") {
		t.Fatalf("expected capped runtime list in output, got %s", output)
	}
	if strings.Contains(output, "truncated,") {
		t.Fatalf("expected no truncation suffix, got %s", output)
	}
}

func TestServiceRunProcessesStorageIntegrations(t *testing.T) {
	t.Parallel()

	outputDir := t.TempDir()
	cfg := appconfig.Config{
		Database:       "DEMO_DB",
		OutputDir:      outputDir,
		LogDir:         outputDir,
		MaxConnections: 2,
	}

	repo := fakeRepo{
		views:               []string{},
		rows:                map[string][]Row{},
		ddls:                map[string]string{},
		storageIntegrations: []string{"MY_S3_INTEGRATION"},
		storageIntegRows: map[string][]Row{
			"MY_S3_INTEGRATION": {
				{"PROPERTY": "ENABLED", "PROPERTY_VALUE": "true"},
				{"PROPERTY": "STORAGE_PROVIDER", "PROPERTY_VALUE": "S3"},
				{"PROPERTY": "STORAGE_AWS_ROLE_ARN", "PROPERTY_VALUE": "arn:aws:iam::123456789012:role/my-role"},
				{"PROPERTY": "STORAGE_ALLOWED_LOCATIONS", "PROPERTY_VALUE": "s3://mybucket/mypath/"},
				// Read-only properties must not appear in the output.
				{"PROPERTY": "STORAGE_AWS_IAM_USER_ARN", "PROPERTY_VALUE": "arn:aws:iam::000000000000:user/sf-user"},
				{"PROPERTY": "STORAGE_AWS_EXTERNAL_ID", "PROPERTY_VALUE": "MYACCOUNT_SFCRole=2_abc"},
			},
		},
	}

	service := NewService(repo, log.New(io.Discard, "", 0), cfg)
	summary, err := service.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if summary.StatementsGenerated != 1 {
		t.Fatalf("expected 1 storage integration statement, got %d", summary.StatementsGenerated)
	}
	if summary.FilesWritten != 1 {
		t.Fatalf("expected 1 file written, got %d", summary.FilesWritten)
	}

	content, err := os.ReadFile(filepath.Join(outputDir, "storage_integrations.sql"))
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	output := string(content)

	for _, want := range []string{
		"CREATE STORAGE INTEGRATION IF NOT EXISTS",
		`"MY_S3_INTEGRATION"`,
		"STORAGE_PROVIDER = 'S3'",
		"ENABLED = TRUE",
		"STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/my-role'",
		"STORAGE_ALLOWED_LOCATIONS = ('s3://mybucket/mypath/')",
	} {
		if !strings.Contains(output, want) {
			t.Errorf("expected storage_integrations.sql to contain %q, got:\n%s", want, output)
		}
	}

	for _, forbidden := range []string{"STORAGE_AWS_IAM_USER_ARN", "STORAGE_AWS_EXTERNAL_ID"} {
		if strings.Contains(output, forbidden) {
			t.Errorf("expected storage_integrations.sql to NOT contain read-only property %q", forbidden)
		}
	}
}

func TestServiceRunWritesBU91777StorageIntegrationToTimestampedOutputFile(t *testing.T) {
	t.Parallel()

	outputDir := t.TempDir()
	cfg := appconfig.Config{
		Database:          "DEMO_DB",
		OutputDir:         outputDir,
		LogDir:            outputDir,
		MaxConnections:    2,
		TimestampedOutput: true,
		RunTimestamp:      "20260312T000000Z",
	}

	repo := fakeRepo{
		views:               []string{},
		rows:                map[string][]Row{},
		ddls:                map[string]string{},
		storageIntegrations: []string{"INTG_CC_ALWAYS_ON_LANDING"},
		storageIntegRows: map[string][]Row{
			"INTG_CC_ALWAYS_ON_LANDING": {
				{"PROPERTY": "ENABLED", "PROPERTY_VALUE": "true"},
				{"PROPERTY": "STORAGE_PROVIDER", "PROPERTY_VALUE": "S3"},
				{"PROPERTY": "STORAGE_AWS_ROLE_ARN", "PROPERTY_VALUE": "arn:aws:iam::123456789012:role/bu91777-role"},
				{"PROPERTY": "STORAGE_ALLOWED_LOCATIONS", "PROPERTY_VALUE": "s3://cc-always-on-landing/"},
			},
		},
	}

	service := NewService(repo, log.New(io.Discard, "", 0), cfg)
	summary, err := service.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if summary.FilesWritten != 1 {
		t.Fatalf("expected 1 file written, got %d", summary.FilesWritten)
	}

	expectedPath := filepath.Join(outputDir, cfg.OutputFileName(storageIntegrationsViewName))
	content, err := os.ReadFile(expectedPath)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", expectedPath, err)
	}

	output := string(content)
	for _, want := range []string{
		`"INTG_CC_ALWAYS_ON_LANDING"`,
		"TYPE = EXTERNAL_STAGE",
		"STORAGE_PROVIDER = 'S3'",
		"STORAGE_ALLOWED_LOCATIONS = ('s3://cc-always-on-landing/')",
	} {
		if !strings.Contains(output, want) {
			t.Errorf("expected %s to contain %q, got:\n%s", expectedPath, want, output)
		}
	}
}

func TestServiceRunWritesStorageIntegrationsFileWhenEmpty(t *testing.T) {
	t.Parallel()

	outputDir := t.TempDir()
	cfg := appconfig.Config{
		Database:       "DEMO_DB",
		OutputDir:      outputDir,
		LogDir:         outputDir,
		MaxConnections: 2,
	}

	// No storage integrations returned.
	repo := fakeRepo{
		views:               []string{},
		rows:                map[string][]Row{},
		ddls:                map[string]string{},
		storageIntegrations: []string{},
	}

	service := NewService(repo, log.New(io.Discard, "", 0), cfg)
	summary, err := service.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if summary.FilesWritten != 1 {
		t.Fatalf("expected 1 file written, got %d", summary.FilesWritten)
	}

	content, err := os.ReadFile(filepath.Join(outputDir, "storage_integrations.sql"))
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if !strings.Contains(string(content), "No data found") {
		t.Fatalf("expected no-data comment in empty storage_integrations.sql, got %s", string(content))
	}
}
