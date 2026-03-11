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

type Service struct {
	repo   Repository
	logger *log.Logger
	cfg    appconfig.Config
}

type RunSummary struct {
	ViewsProcessed      int
	RowsProcessed       int
	StatementsGenerated int
	FilesWritten        int
}

type viewResult struct {
	ViewName            string
	RowsProcessed       int
	StatementsGenerated int
	FilePath            string
	Err                 error
}

func NewService(repo Repository, logger *log.Logger, cfg appconfig.Config) *Service {
	return &Service{repo: repo, logger: logger, cfg: cfg}
}

func (s *Service) Run(ctx context.Context) (RunSummary, error) {
	views, err := s.repo.ListViews(ctx, s.cfg.Database)
	if err != nil {
		return RunSummary{}, fmt.Errorf("list information_schema views: %w", err)
	}
	sort.Strings(views)

	jobs := make(chan string)
	results := make(chan viewResult)
	workerCount := min(s.cfg.MaxConnections, len(views))
	if workerCount == 0 {
		return RunSummary{}, nil
	}

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

	go func() {
		for _, viewName := range views {
			jobs <- viewName
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	summary := RunSummary{}
	var failures []string
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

	if len(failures) > 0 {
		return summary, fmt.Errorf("processing completed with %d failed views: %s", len(failures), strings.Join(failures, "; "))
	}

	return summary, nil
}

func (s *Service) processView(ctx context.Context, viewName string) viewResult {
	rows, err := s.repo.FetchViewRows(ctx, s.cfg.Database, viewName)
	if err != nil {
		return viewResult{ViewName: viewName, Err: err}
	}

	outputBlocks := make([]string, 0, len(rows))
	generatedStatements := 0
	if len(rows) == 0 {
		outputBlocks = append(outputBlocks, RenderNoDataComment(viewName))
	} else {
		if s.cfg.CompactPackages && strings.EqualFold(viewName, "PACKAGES") {
			outputBlocks = renderCompactPackages(rows, s.cfg.CompactPackagesMaxRuntimes, s.cfg.CompactPackagesOmitTruncationCount)
			generatedStatements = len(outputBlocks)
		} else {
			for _, row := range rows {
				request, ok := InferDDLRequest(s.cfg.Database, viewName, row)
				if !ok {
					outputBlocks = append(outputBlocks, RenderFallbackComment(viewName, row, "unsupported row shape"))
					continue
				}

				if strings.TrimSpace(request.InlineSQL) != "" {
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

func sanitizeFileName(fileName string) string {
	replacer := strings.NewReplacer("/", "_", `\\`, "_", " ", "_", ":", "_")
	return replacer.Replace(fileName)
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

type packageGroupKey struct {
	PackageName string
	Version     string
	Language    string
}

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
