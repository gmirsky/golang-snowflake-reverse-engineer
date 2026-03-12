package app

import (
	"bytes"
	"context"
	"errors"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	appconfig "github.com/gmirsky/golang-snowflake-reverse-engineer/internal/config"
	"github.com/gmirsky/golang-snowflake-reverse-engineer/internal/reverseengineer"
)

type testRepo struct{}

// ListViews: Given a fake repository, when view listing is requested, then
// it returns no rows and no error for deterministic tests.
func (testRepo) ListViews(context.Context, string) ([]string, error) { return nil, nil }

// FetchViewRows: Given a fake repository, when view rows are requested, then
// it returns no rows and no error for deterministic tests.
func (testRepo) FetchViewRows(context.Context, string, string) ([]reverseengineer.Row, error) {
	return nil, nil
}

// FetchDDL: Given a fake repository, when DDL is requested, then it returns
// an empty DDL string and no error for deterministic tests.
func (testRepo) FetchDDL(context.Context, reverseengineer.DDLRequest) (string, error) { return "", nil }

// ListStorageIntegrations: Given a fake repository, when storage integrations
// are requested, then it returns no rows and no error.
func (testRepo) ListStorageIntegrations(context.Context) ([]string, error)            { return nil, nil }

// DescStorageIntegration: Given a fake repository, when integration details
// are requested, then it returns no rows and no error.
func (testRepo) DescStorageIntegration(context.Context, string) ([]reverseengineer.Row, error) {
	return nil, nil
}

// Close: Given a fake repository, when close is requested, then it returns nil.
func (testRepo) Close() error { return nil }

type testService struct {
	summary reverseengineer.RunSummary
	err     error
}

// Run: Given a fake service, when Run is invoked, then it returns the preset
// summary and error for deterministic orchestration tests.
func (s testService) Run(context.Context) (reverseengineer.RunSummary, error) {
	return s.summary, s.err
}

// TestRunReturnsErrorWhenOutputDirIsAFile: Given an output path that already
// exists as a file, when Run executes, then it should fail before repository work.
func TestRunReturnsErrorWhenOutputDirIsAFile(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	outputFilePath := filepath.Join(tempDir, "not_a_directory")
	if err := os.WriteFile(outputFilePath, []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	cfg := appconfig.Config{
		OutputDir: outputFilePath,
		LogDir:    filepath.Join(tempDir, "logs"),
	}

	err := Run(cfg)
	if err == nil {
		t.Fatal("expected error when output-dir is a file")
	}
	if !strings.Contains(err.Error(), "create output directory") {
		t.Fatalf("expected output-dir creation error, got %v", err)
	}
}

// TestRunReturnsErrorWhenLogDirIsAFile: Given a log path that already exists
// as a file, when Run executes, then it should fail before opening log output.
func TestRunReturnsErrorWhenLogDirIsAFile(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	logFilePath := filepath.Join(tempDir, "not_a_directory")
	if err := os.WriteFile(logFilePath, []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	cfg := appconfig.Config{
		OutputDir: filepath.Join(tempDir, "output"),
		LogDir:    logFilePath,
	}

	err := Run(cfg)
	if err == nil {
		t.Fatal("expected error when log-dir is a file")
	}
	if !strings.Contains(err.Error(), "create log directory") {
		t.Fatalf("expected log-dir creation error, got %v", err)
	}
}

// TestLogParametersWritesSortedKeys: Given a config, when parameters are
// logged, then param keys should be emitted in deterministic sorted order.
func TestLogParametersWritesSortedKeys(t *testing.T) {
	t.Parallel()

	var buffer bytes.Buffer
	logger := log.New(&buffer, "", 0)
	cfg := appconfig.Config{
		User:           "USER",
		Account:        "ACCOUNT",
		Warehouse:      "WH",
		Database:       "DB",
		OutputDir:      "./out",
		LogDir:         "./logs",
		PrivateKeyPath: "./keys/rsa_key.p8",
		MaxConnections: 2,
	}

	logParameters(logger, cfg)

	lines := strings.Split(strings.TrimSpace(buffer.String()), "\n")
	if len(lines) == 0 {
		t.Fatal("expected at least one logged parameter")
	}

	keys := make([]string, 0, len(lines))
	for _, line := range lines {
		const prefix = "param "
		if !strings.HasPrefix(line, prefix) {
			t.Fatalf("unexpected log line format: %q", line)
		}
		payload := strings.TrimPrefix(line, prefix)
		key, _, found := strings.Cut(payload, "=")
		if !found {
			t.Fatalf("expected key/value log format, got %q", line)
		}
		keys = append(keys, key)
	}

	sortedKeys := append([]string(nil), keys...)
	sort.Strings(sortedKeys)
	if !reflect.DeepEqual(keys, sortedKeys) {
		t.Fatalf("expected sorted keys, got %v", keys)
	}
}

// TestRunReturnsRepositoryError: Given openRepository failure, when Run
// executes, then it should return the repository error.
func TestRunReturnsRepositoryError(t *testing.T) {
	tempDir := t.TempDir()

	originalOpenRepository := openRepository
	originalBuildService := buildService
	t.Cleanup(func() {
		openRepository = originalOpenRepository
		buildService = originalBuildService
	})

	openRepository = func(context.Context, appconfig.Config) (repositoryWithClose, error) {
		return nil, errors.New("repository unavailable")
	}
	buildService = func(reverseengineer.Repository, *log.Logger, appconfig.Config) runnableService {
		t.Fatal("buildService should not be called when openRepository fails")
		return nil
	}

	cfg := appconfig.Config{
		OutputDir: filepath.Join(tempDir, "output"),
		LogDir:    filepath.Join(tempDir, "logs"),
	}

	err := Run(cfg)
	if err == nil || !strings.Contains(err.Error(), "repository unavailable") {
		t.Fatalf("expected repository failure, got %v", err)
	}
}

// TestRunReturnsServiceError: Given service Run failure, when app Run executes,
// then it should return the service error.
func TestRunReturnsServiceError(t *testing.T) {
	tempDir := t.TempDir()

	originalOpenRepository := openRepository
	originalBuildService := buildService
	t.Cleanup(func() {
		openRepository = originalOpenRepository
		buildService = originalBuildService
	})

	openRepository = func(context.Context, appconfig.Config) (repositoryWithClose, error) {
		return testRepo{}, nil
	}
	buildService = func(reverseengineer.Repository, *log.Logger, appconfig.Config) runnableService {
		return testService{summary: reverseengineer.RunSummary{FilesWritten: 2}, err: errors.New("service failed")}
	}

	cfg := appconfig.Config{
		OutputDir: filepath.Join(tempDir, "output"),
		LogDir:    filepath.Join(tempDir, "logs"),
	}

	err := Run(cfg)
	if err == nil || !strings.Contains(err.Error(), "service failed") {
		t.Fatalf("expected service failure, got %v", err)
	}
}

// TestRunSuccessWithInjectedDependencies: Given successful repository and
// service fakes, when Run executes, then it should succeed without error.
func TestRunSuccessWithInjectedDependencies(t *testing.T) {
	tempDir := t.TempDir()

	originalOpenRepository := openRepository
	originalBuildService := buildService
	t.Cleanup(func() {
		openRepository = originalOpenRepository
		buildService = originalBuildService
	})

	openRepository = func(context.Context, appconfig.Config) (repositoryWithClose, error) {
		return testRepo{}, nil
	}
	buildService = func(reverseengineer.Repository, *log.Logger, appconfig.Config) runnableService {
		return testService{summary: reverseengineer.RunSummary{ViewsProcessed: 1, FilesWritten: 1}, err: nil}
	}

	cfg := appconfig.Config{
		OutputDir: filepath.Join(tempDir, "output"),
		LogDir:    filepath.Join(tempDir, "logs"),
	}

	if err := Run(cfg); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
}
