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
	views []string
	rows  map[string][]Row
	ddls  map[string]string
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

	if summary.FilesWritten != 2 {
		t.Fatalf("expected 2 files, got %d", summary.FilesWritten)
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
