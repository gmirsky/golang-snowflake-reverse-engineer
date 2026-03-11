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
