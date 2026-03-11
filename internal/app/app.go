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

func Run(cfg appconfig.Config) error {
	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}
	if err := os.MkdirAll(cfg.LogDir, 0o755); err != nil {
		return fmt.Errorf("create log directory: %w", err)
	}

	logPath := filepath.Join(cfg.LogDir, cfg.LogFileName())
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open log file: %w", err)
	}
	defer logFile.Close()

	logger := log.New(io.MultiWriter(os.Stdout, logFile), "", log.LstdFlags|log.LUTC)
	logger.Printf("log_file=%s", logPath)
	logParameters(logger, cfg)

	ctx := context.Background()
	repo, err := snowflake.NewRepository(ctx, cfg)
	if err != nil {
		return err
	}
	defer repo.Close()

	service := reverseengineer.NewService(repo, logger, cfg)
	summary, runErr := service.Run(ctx)
	logger.Printf("summary views=%d rows=%d sql_statements=%d files=%d", summary.ViewsProcessed, summary.RowsProcessed, summary.StatementsGenerated, summary.FilesWritten)
	if runErr != nil {
		return runErr
	}

	return nil
}

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
