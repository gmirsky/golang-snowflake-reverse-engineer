// Package reverseengineer orchestrates concurrent INFORMATION_SCHEMA view
// processing and serial storage-integration DDL reconstruction.
package reverseengineer

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	appconfig "github.com/gmirsky/golang-snowflake-reverse-engineer/internal/config"
)

// Service coordinates the full reverse-engineering pipeline for a single run.
type Service struct {
	repo   Repository
	logger *log.Logger
	cfg    appconfig.Config
}

// RunSummary records high-level metrics for a completed run.
type RunSummary struct {
	ViewsProcessed      int // number of INFORMATION_SCHEMA views processed (including storage integrations)
	RowsProcessed       int // total metadata rows (or integration names) seen
	StatementsGenerated int // total DDL/SQL statements written to files
	FilesWritten        int // number of .sql files successfully created
}

// viewResult carries the outcome of processing a single view (or the storage
// integrations step) back to the Run coordinator.
type viewResult struct {
	ViewName            string
	RowsProcessed       int
	StatementsGenerated int
	FilePath            string // empty when writing failed
	Err                 error
}

// NewService constructs a Service bound to the given repository, logger, and config.
func NewService(repo Repository, logger *log.Logger, cfg appconfig.Config) *Service {
	return &Service{repo: repo, logger: logger, cfg: cfg}
}

// Run executes the full pipeline: INFORMATION_SCHEMA views are processed
// concurrently up to MaxConnections; storage integrations follow serially.
// A non-nil error is returned only when one or more views/steps fail; the
// summary is always populated with the work that did succeed.
func (s *Service) Run(ctx context.Context) (RunSummary, error) {
	views, err := s.repo.ListViews(ctx, s.cfg.Database)
	if err != nil {
		return RunSummary{}, fmt.Errorf("list information_schema views: %w", err)
	}
	sort.Strings(views) // deterministic processing order

	summary := RunSummary{}
	var failures []string

	// Process INFORMATION_SCHEMA views concurrently.
	if workerCount := min(s.cfg.MaxConnections, len(views)); workerCount > 0 {
		jobs := make(chan string)
		results := make(chan viewResult)

		var wg sync.WaitGroup
		for range workerCount {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for viewName := range jobs {
					results <- s.processView(ctx, viewName)
				}
			}()
		}

		// Feed view names to workers, then close results once all workers drain.
		go func() {
			for _, viewName := range views {
				jobs <- viewName
			}
			close(jobs)
			wg.Wait()
			close(results)
		}()

		for result := range results {
			summary.ViewsProcessed++
			summary.RowsProcessed += result.RowsProcessed
			summary.StatementsGenerated += result.StatementsGenerated
			if result.FilePath != "" {
				summary.FilesWritten++
			}

			if result.Err != nil {
				failures = append(failures, fmt.Sprintf("%s: %v", result.ViewName, result.Err))
				s.logger.Printf("view=%s error=%v", result.ViewName, result.Err)
				continue
			}

			s.logger.Printf("view=%s rows=%d sql_statements=%d file=%s", result.ViewName, result.RowsProcessed, result.StatementsGenerated, result.FilePath)
		}
	}

	// Process storage integrations serially after all view workers finish.
	storageResult := s.processStorageIntegrations(ctx)
	summary.ViewsProcessed++
	summary.RowsProcessed += storageResult.RowsProcessed
	summary.StatementsGenerated += storageResult.StatementsGenerated
	if storageResult.FilePath != "" {
		summary.FilesWritten++
	}
	if storageResult.Err != nil {
		failures = append(failures, fmt.Sprintf("%s: %v", storageResult.ViewName, storageResult.Err))
		s.logger.Printf("view=%s error=%v", storageResult.ViewName, storageResult.Err)
	} else {
		s.logger.Printf("view=%s rows=%d sql_statements=%d file=%s", storageResult.ViewName, storageResult.RowsProcessed, storageResult.StatementsGenerated, storageResult.FilePath)
	}

	if len(failures) > 0 {
		return summary, fmt.Errorf("processing completed with %d failed views: %s", len(failures), strings.Join(failures, "; "))
	}

	return summary, nil
}

// processView fetches rows for one INFORMATION_SCHEMA view and writes the
// corresponding SQL output file. It is designed to run in a worker goroutine.
func (s *Service) processView(ctx context.Context, viewName string) viewResult {
	rows, err := s.repo.FetchViewRows(ctx, s.cfg.Database, viewName)
	if err != nil {
		return viewResult{ViewName: viewName, Err: err}
	}

	outputBlocks := make([]string, 0, len(rows))
	generatedStatements := 0
	if len(rows) == 0 {
		// Write a no-data sentinel so the file is never empty.
		outputBlocks = append(outputBlocks, RenderNoDataComment(viewName))
	} else {
		if s.cfg.CompactPackages && strings.EqualFold(viewName, "PACKAGES") {
			// Compact mode: collapse duplicate package rows into grouped comment lines.
			outputBlocks = renderCompactPackages(rows, s.cfg.CompactPackagesMaxRuntimes, s.cfg.CompactPackagesOmitTruncationCount)
			generatedStatements = len(outputBlocks)
		} else {
			for _, row := range rows {
				request, ok := InferDDLRequest(s.cfg.Database, viewName, row)
				if !ok {
					// Row shape not recognised; emit a comment so nothing is silently dropped.
					outputBlocks = append(outputBlocks, RenderFallbackComment(viewName, row, "unsupported row shape"))
					continue
				}

				if strings.TrimSpace(request.InlineSQL) != "" {
					// InlineSQL was computed by the inference layer; no GET_DDL call needed.
					outputBlocks = append(outputBlocks, EnsureTerminatedSQL(request.InlineSQL))
					generatedStatements++
					continue
				}

				ddl, ddlErr := s.repo.FetchDDL(ctx, request)
				if ddlErr != nil {
					outputBlocks = append(outputBlocks, RenderFallbackComment(viewName, row, ddlErr.Error()))
					continue
				}

				outputBlocks = append(outputBlocks, EnsureTerminatedSQL(ddl))
				generatedStatements++
			}
		}
	}

	filePath := filepath.Join(s.cfg.OutputDir, sanitizeFileName(s.cfg.OutputFileName(viewName)))
	content := strings.Join(outputBlocks, "\n\n")
	if !strings.HasSuffix(content, "\n") {
		content += "\n"
	}

	if err := os.WriteFile(filePath, []byte(content), 0o644); err != nil {
		return viewResult{ViewName: viewName, Err: fmt.Errorf("write view output: %w", err)}
	}

	if s.cfg.Verbose {
		s.logger.Printf("view=%s wrote_file=%s", viewName, filePath)
	}

	return viewResult{
		ViewName:            viewName,
		RowsProcessed:       len(rows),
		StatementsGenerated: generatedStatements,
		FilePath:            filePath,
	}
}

// storageIntegrationsViewName is the logical view name used for logging and
// output file naming of the storage integrations step.
const storageIntegrationsViewName = "STORAGE_INTEGRATIONS"

// processStorageIntegrations lists all storage-type integrations via
// SHOW INTEGRATIONS, then calls DESC STORAGE INTEGRATION for each one and
// builds CREATE STORAGE INTEGRATION DDL. Always writes storage_integrations.sql.
func (s *Service) processStorageIntegrations(ctx context.Context) viewResult {
	names, err := s.repo.ListStorageIntegrations(ctx)
	if err != nil {
		return viewResult{ViewName: storageIntegrationsViewName, Err: err}
	}

	outputBlocks := make([]string, 0, len(names))
	generatedStatements := 0

	if len(names) == 0 {
		// Write a no-data sentinel so the file is never empty.
		outputBlocks = append(outputBlocks, RenderNoDataComment(storageIntegrationsViewName))
	} else {
		for _, name := range names {
			descRows, descErr := s.repo.DescStorageIntegration(ctx, name)
			if descErr != nil {
				// Emit a comment so the failure is visible in the output file.
				outputBlocks = append(outputBlocks, fmt.Sprintf("/* Unable to describe storage integration %q: %v */", name, descErr))
				continue
			}

			ddl, ok := BuildStorageIntegrationDDL(name, descRows)
			if !ok {
				// STORAGE_PROVIDER missing; emit a comment rather than silently skipping.
				outputBlocks = append(outputBlocks, fmt.Sprintf("/* Unable to reconstruct DDL for storage integration %q: missing required properties */", name))
				continue
			}

			outputBlocks = append(outputBlocks, EnsureTerminatedSQL(ddl))
			generatedStatements++
		}
	}

	filePath := filepath.Join(s.cfg.OutputDir, sanitizeFileName(s.cfg.OutputFileName(storageIntegrationsViewName)))
	content := strings.Join(outputBlocks, "\n\n")
	if !strings.HasSuffix(content, "\n") {
		content += "\n"
	}

	if err := os.WriteFile(filePath, []byte(content), 0o644); err != nil {
		return viewResult{ViewName: storageIntegrationsViewName, Err: fmt.Errorf("write storage integrations output: %w", err)}
	}

	if s.cfg.Verbose {
		s.logger.Printf("view=%s wrote_file=%s", storageIntegrationsViewName, filePath)
	}

	return viewResult{
		ViewName:            storageIntegrationsViewName,
		RowsProcessed:       len(names),
		StatementsGenerated: generatedStatements,
		FilePath:            filePath,
	}
}

// sanitizeFileName replaces characters that are illegal or undesirable in file
// names (slashes, backslashes, spaces, colons) with underscores.
func sanitizeFileName(fileName string) string {
	replacer := strings.NewReplacer("/", "_", `\\`, "_", " ", "_", ":", "_")
	return replacer.Replace(fileName)
}

// min returns the smaller of a and b.
func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

// packageGroupKey is the composite key used to collapse PACKAGES rows that
// differ only in RUNTIME_VERSION into a single compact comment line.
type packageGroupKey struct {
	PackageName string
	Version     string
	Language    string
}

// renderCompactPackages groups PACKAGES rows by (PackageName, Version, Language),
// collects all distinct RUNTIME_VERSION values per group, and emits one comment
// line per group. maxRuntimes caps the runtime list (0 = unlimited).
func renderCompactPackages(rows []Row, maxRuntimes int, omitTruncationCount bool) []string {
	grouped := make(map[packageGroupKey]map[string]struct{})
	outputBlocks := make([]string, 0, len(rows))

	for _, row := range rows {
		packageName, okPackage := getString(row, "PACKAGE_NAME")
		version, okVersion := getString(row, "VERSION")
		language, okLanguage := getString(row, "LANGUAGE")
		if !okLanguage {
			language = "unknown"
		}
		if !okPackage || !okVersion {
			outputBlocks = append(outputBlocks, RenderFallbackComment("PACKAGES", row, "unsupported row shape"))
			continue
		}

		runtimeVersion, okRuntime := getString(row, "RUNTIME_VERSION")
		if !okRuntime {
			runtimeVersion = "default"
		}

		key := packageGroupKey{
			PackageName: packageName,
			Version:     version,
			Language:    language,
		}
		if _, exists := grouped[key]; !exists {
			grouped[key] = make(map[string]struct{})
		}
		grouped[key][runtimeVersion] = struct{}{}
	}

	keys := make([]packageGroupKey, 0, len(grouped))
	for key := range grouped {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i int, j int) bool {
		if keys[i].PackageName != keys[j].PackageName {
			return keys[i].PackageName < keys[j].PackageName
		}
		if keys[i].Version != keys[j].Version {
			return keys[i].Version < keys[j].Version
		}
		return keys[i].Language < keys[j].Language
	})

	for _, key := range keys {
		runtimeSet := grouped[key]
		runtimes := make([]string, 0, len(runtimeSet))
		for runtime := range runtimeSet {
			runtimes = append(runtimes, runtime)
		}
		sort.Strings(runtimes)

		truncatedCount := 0
		if maxRuntimes > 0 && len(runtimes) > maxRuntimes {
			truncatedCount = len(runtimes) - maxRuntimes
			runtimes = runtimes[:maxRuntimes]
		}

		quotedRuntimes := make([]string, 0, len(runtimes))
		for _, runtime := range runtimes {
			quotedRuntimes = append(quotedRuntimes, strconv.Quote(runtime))
		}

		statement := fmt.Sprintf(
			"-- Package %s version %s language %s runtimes [%s]",
			quoteIdentifier(key.PackageName),
			quoteLiteral(key.Version),
			quoteLiteral(key.Language),
			strings.Join(quotedRuntimes, ", "),
		)
		if truncatedCount > 0 && !omitTruncationCount {
			statement += fmt.Sprintf(" (truncated, %d more)", truncatedCount)
		}

		outputBlocks = append(outputBlocks, statement)
	}

	return outputBlocks
}
