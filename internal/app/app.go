// Package app wires together configuration, Snowflake repository, and the
// reverse-engineering service, then drives a single run to completion.
package app

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"

	appconfig "github.com/gmirsky/golang-snowflake-reverse-engineer/internal/config"
	"github.com/gmirsky/golang-snowflake-reverse-engineer/internal/reverseengineer"
	"github.com/gmirsky/golang-snowflake-reverse-engineer/internal/snowflake"
)

type repositoryWithClose interface {
	reverseengineer.Repository
	Close() error
}

type runnableService interface {
	Run(ctx context.Context) (reverseengineer.RunSummary, error)
}

var openRepository = func(ctx context.Context, cfg appconfig.Config) (repositoryWithClose, error) {
	return snowflake.NewRepository(ctx, cfg)
}

var buildService = func(repo reverseengineer.Repository, logger *log.Logger, cfg appconfig.Config) runnableService {
	return reverseengineer.NewService(repo, logger, cfg)
}

// Run: Given a validated config, when orchestration starts, then required
// directories, logging, repository setup, and service execution are performed.
func Run(cfg appconfig.Config) error {
	// Ensure the output and log directories exist before anything tries to write.
	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}
	if err := os.MkdirAll(cfg.LogDir, 0o755); err != nil {
		return fmt.Errorf("create log directory: %w", err)
	}

	// Open (or create) the log file; truncate any prior run's content.
	logPath := filepath.Join(cfg.LogDir, cfg.LogFileName())
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open log file: %w", err)
	}
	defer logFile.Close()

	// Mirror all log output to stdout and the log file.
	logger := log.New(io.MultiWriter(os.Stdout, logFile), "", log.LstdFlags|log.LUTC)
	logger.Printf("log_file=%s", logPath)
	logParameters(logger, cfg)

	ctx := context.Background()
	// Open the Snowflake connection pool; close it when Run returns.
	repo, err := openRepository(ctx, cfg)
	if err != nil {
		return err
	}
	defer repo.Close()

	// The service encapsulates all reverse-engineering logic so Run remains thin.
	service := buildService(repo, logger, cfg)
	summary, runErr := service.Run(ctx)
	// Always log the summary even when Run returns an error.
	logger.Printf("summary views=%d rows=%d sql_statements=%d files=%d", summary.ViewsProcessed, summary.RowsProcessed, summary.StatementsGenerated, summary.FilesWritten)
	if runErr != nil {
		return runErr
	}

	return nil
}

// logParameters: Given redacted parameter data, when values are logged, then
// keys are emitted in alphabetical order for deterministic output.
func logParameters(logger *log.Logger, cfg appconfig.Config) {
	params := cfg.RedactedParameters()
	keys := make([]string, 0, len(params))
	for key := range params {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		logger.Printf("param %s=%s", key, params[key])
	}
}
