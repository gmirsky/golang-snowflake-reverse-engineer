package reverseengineer

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	appconfig "github.com/gmirsky/golang-snowflake-reverse-engineer/internal/config"
)

// fakeRepo is a minimal in-memory implementation of Repository used to drive
// deterministic service tests without opening real Snowflake connections.
type fakeRepo struct {
	views                      []string
	rows                       map[string][]Row
	ddls                       map[string]string
	storageIntegrations        []string
	storageIntegRows           map[string][]Row
	listViewsErr               error
	fetchViewRowsErrByView     map[string]error
	fetchDDLErrByQualified     map[string]error
	listStorageIntegrationsErr error
	descStorageErrByName       map[string]error
}

// ListViews: Given a fake repository, when view discovery runs, then fixture
// view names are returned.
func (f fakeRepo) ListViews(_ context.Context, _ string) ([]string, error) {
	if f.listViewsErr != nil {
		return nil, f.listViewsErr
	}
	return f.views, nil
}

// FetchViewRows: Given a view name, when row retrieval runs, then fixture rows
// for that view are returned.
func (f fakeRepo) FetchViewRows(_ context.Context, _ string, viewName string) ([]Row, error) {
	if err, ok := f.fetchViewRowsErrByView[viewName]; ok {
		return nil, err
	}
	return f.rows[viewName], nil
}

// FetchDDL: Given an inferred request, when DDL retrieval runs, then fixture
// DDL text is returned by qualified object name.
func (f fakeRepo) FetchDDL(_ context.Context, request DDLRequest) (string, error) {
	if err, ok := f.fetchDDLErrByQualified[request.QualifiedName]; ok {
		return "", err
	}
	return f.ddls[request.QualifiedName], nil
}

// ListStorageIntegrations: Given fake integration data, when listing runs,
// then fixture integration names are returned.
func (f fakeRepo) ListStorageIntegrations(_ context.Context) ([]string, error) {
	if f.listStorageIntegrationsErr != nil {
		return nil, f.listStorageIntegrationsErr
	}
	return f.storageIntegrations, nil
}

// DescStorageIntegration: Given an integration name, when DESC retrieval runs,
// then configured fixture rows are returned.
func (f fakeRepo) DescStorageIntegration(_ context.Context, name string) ([]Row, error) {
	if err, ok := f.descStorageErrByName[name]; ok {
		return nil, err
	}
	if f.storageIntegRows == nil {
		return nil, nil
	}
	return f.storageIntegRows[name], nil
}

// TestServiceRunReturnsListViewsError: Given a repository list-views failure,
// when Service.Run executes, then it should fail before processing work.
func TestServiceRunReturnsListViewsError(t *testing.T) {
	t.Parallel()

	cfg := appconfig.Config{Database: "DEMO_DB", OutputDir: t.TempDir(), LogDir: t.TempDir(), MaxConnections: 2}
	repo := fakeRepo{listViewsErr: errors.New("list failed")}

	service := NewService(repo, log.New(io.Discard, "", 0), cfg)
	_, err := service.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "list information_schema views") {
		t.Fatalf("expected list views error, got %v", err)
	}
}

// TestServiceRunAggregatesViewFailure: Given one view fetch error, when
// Service.Run executes, then other steps still complete and failure is returned.
func TestServiceRunAggregatesViewFailure(t *testing.T) {
	t.Parallel()

	outputDir := t.TempDir()
	cfg := appconfig.Config{Database: "DEMO_DB", OutputDir: outputDir, LogDir: outputDir, MaxConnections: 1}
	repo := fakeRepo{
		views:               []string{"BROKEN_VIEW"},
		rows:                map[string][]Row{},
		ddls:                map[string]string{},
		storageIntegrations: []string{},
		fetchViewRowsErrByView: map[string]error{
			"BROKEN_VIEW": errors.New("view fetch failed"),
		},
	}

	service := NewService(repo, log.New(io.Discard, "", 0), cfg)
	summary, err := service.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "BROKEN_VIEW") {
		t.Fatalf("expected aggregated view error, got %v", err)
	}
	if summary.ViewsProcessed != 2 {
		t.Fatalf("expected 2 processed views including storage step, got %d", summary.ViewsProcessed)
	}
	if summary.FilesWritten != 1 {
		t.Fatalf("expected storage output file to still be written, got %d", summary.FilesWritten)
	}
}

// TestServiceRunWritesFallbackWhenFetchDDLFails: Given inferable rows with
// DDL fetch failures, when Service.Run executes, then fallback comments are written.
func TestServiceRunWritesFallbackWhenFetchDDLFails(t *testing.T) {
	t.Parallel()

	outputDir := t.TempDir()
	cfg := appconfig.Config{Database: "DEMO_DB", OutputDir: outputDir, LogDir: outputDir, MaxConnections: 1}
	repo := fakeRepo{
		views: []string{"TABLES"},
		rows: map[string][]Row{
			"TABLES": {
				{"TABLE_CATALOG": "DEMO_DB", "TABLE_SCHEMA": "PUBLIC", "TABLE_NAME": "CUSTOMERS", "TABLE_TYPE": "BASE TABLE"},
			},
		},
		ddls:                map[string]string{},
		storageIntegrations: []string{},
		fetchDDLErrByQualified: map[string]error{
			`"DEMO_DB"."PUBLIC"."CUSTOMERS"`: errors.New("ddl fetch failed"),
		},
	}

	service := NewService(repo, log.New(io.Discard, "", 0), cfg)
	summary, err := service.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if summary.StatementsGenerated != 0 {
		t.Fatalf("expected zero generated statements when DDL fetch fails, got %d", summary.StatementsGenerated)
	}

	content, readErr := os.ReadFile(filepath.Join(outputDir, "tables.sql"))
	if readErr != nil {
		t.Fatalf("ReadFile() error = %v", readErr)
	}
	if !strings.Contains(string(content), "Unable to generate DDL") {
		t.Fatalf("expected fallback comment in tables.sql, got %s", string(content))
	}
}

// TestServiceRunStorageListError: Given storage integration listing fails,
// when Service.Run executes, then the error should be returned and no file written.
func TestServiceRunStorageListError(t *testing.T) {
	t.Parallel()

	outputDir := t.TempDir()
	cfg := appconfig.Config{Database: "DEMO_DB", OutputDir: outputDir, LogDir: outputDir, MaxConnections: 1}
	repo := fakeRepo{
		views:                      []string{},
		rows:                       map[string][]Row{},
		ddls:                       map[string]string{},
		listStorageIntegrationsErr: errors.New("show integrations failed"),
	}

	service := NewService(repo, log.New(io.Discard, "", 0), cfg)
	summary, err := service.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "STORAGE_INTEGRATIONS") {
		t.Fatalf("expected storage integration error, got %v", err)
	}
	if summary.FilesWritten != 0 {
		t.Fatalf("expected no files written, got %d", summary.FilesWritten)
	}
}

// TestServiceRunStorageDescAndDDLReconstructionFallbacks: Given describe
// failures and incomplete properties, when Service.Run executes, then both fallback comments are emitted.
func TestServiceRunStorageDescAndDDLReconstructionFallbacks(t *testing.T) {
	t.Parallel()

	outputDir := t.TempDir()
	cfg := appconfig.Config{Database: "DEMO_DB", OutputDir: outputDir, LogDir: outputDir, MaxConnections: 1}
	repo := fakeRepo{
		views:               []string{},
		rows:                map[string][]Row{},
		ddls:                map[string]string{},
		storageIntegrations: []string{"BROKEN_DESC", "MISSING_PROVIDER"},
		descStorageErrByName: map[string]error{
			"BROKEN_DESC": errors.New("desc failed"),
		},
		storageIntegRows: map[string][]Row{
			"MISSING_PROVIDER": {
				{"PROPERTY": "ENABLED", "PROPERTY_VALUE": "true"},
			},
		},
	}

	service := NewService(repo, log.New(io.Discard, "", 0), cfg)
	summary, err := service.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if summary.StatementsGenerated != 0 {
		t.Fatalf("expected no generated storage statements, got %d", summary.StatementsGenerated)
	}

	content, readErr := os.ReadFile(filepath.Join(outputDir, "storage_integrations.sql"))
	if readErr != nil {
		t.Fatalf("ReadFile() error = %v", readErr)
	}
	output := string(content)
	if !strings.Contains(output, `Unable to describe storage integration "BROKEN_DESC"`) {
		t.Fatalf("expected desc fallback comment, got %s", output)
	}
	if !strings.Contains(output, `Unable to reconstruct DDL for storage integration "MISSING_PROVIDER"`) {
		t.Fatalf("expected reconstruction fallback comment, got %s", output)
	}
}

// TestServiceRunWritesFiles: Given mixed view data, when Service.Run executes,
// then it should write DDL files and no-data sentinels deterministically.
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

// TestServiceRunCompactsPackages: Given duplicate package rows, when compact
// mode runs, then runtimes should collapse into one summarized line.
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
					"RUNTIME_VERSION": "3.12",
				},
				{
					"PACKAGE_NAME":    "abi3audit",
					"VERSION":         "0.0.24",
					"LANGUAGE":        "python",
					"RUNTIME_VERSION": "3.13",
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
	if !strings.Contains(output, "runtimes [\"3.12\", \"3.13\", \"default\"]") {
		t.Fatalf("expected grouped runtimes in output, got %s", output)
	}
}

// TestServiceRunCompactsPackagesWithRuntimeCap: Given many runtime values,
// when capped compact mode runs, then truncation should be advertised.
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
				{"PACKAGE_NAME": "abi3audit", "VERSION": "0.0.24", "LANGUAGE": "python", "RUNTIME_VERSION": "3.12"},
				{"PACKAGE_NAME": "abi3audit", "VERSION": "0.0.24", "LANGUAGE": "python", "RUNTIME_VERSION": "3.13"},
				{"PACKAGE_NAME": "abi3audit", "VERSION": "0.0.24", "LANGUAGE": "python", "RUNTIME_VERSION": "3.14"},
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
	if !strings.Contains(output, "runtimes [\"3.12\", \"3.13\"] (truncated, 1 more)") {
		t.Fatalf("expected capped runtime list in output, got %s", output)
	}
}

// TestServiceRunCompactsPackagesWithRuntimeCapOmitTruncationCount: Given
// truncation-count suppression, when capped compact mode runs, then suffix text should be omitted.
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
				{"PACKAGE_NAME": "abi3audit", "VERSION": "0.0.24", "LANGUAGE": "python", "RUNTIME_VERSION": "3.12"},
				{"PACKAGE_NAME": "abi3audit", "VERSION": "0.0.24", "LANGUAGE": "python", "RUNTIME_VERSION": "3.13"},
				{"PACKAGE_NAME": "abi3audit", "VERSION": "0.0.24", "LANGUAGE": "python", "RUNTIME_VERSION": "3.14"},
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
	if !strings.Contains(output, "runtimes [\"3.12\", \"3.13\"]") {
		t.Fatalf("expected capped runtime list in output, got %s", output)
	}
	if strings.Contains(output, "truncated,") {
		t.Fatalf("expected no truncation suffix, got %s", output)
	}
}

// TestServiceRunProcessesStorageIntegrations: Given storage integration DESC
// rows, when Service.Run executes, then reconstructed DDL should exclude read-only fields.
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

// TestServiceRunWritesBU91777StorageIntegrationToTimestampedOutputFile: Given
// BU91777 integration data and timestamped output, when Service.Run executes, then the expected timestamped file should contain correct DDL.
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

// TestServiceRunWritesStorageIntegrationsFileWhenEmpty: Given no storage
// integrations, when Service.Run executes, then a deterministic no-data file should be written.
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

// TestRenderCompactPackagesFallbackOnMissingFields: Given a PACKAGES row that
// lacks PACKAGE_NAME or VERSION, when compact rendering runs directly, then
// the fallback comment path should be activated for the incomplete row.
func TestRenderCompactPackagesFallbackOnMissingFields(t *testing.T) {
	t.Parallel()

	rows := []Row{
		{"LANGUAGE": "python"}, // missing PACKAGE_NAME and VERSION → fallback
		{"PACKAGE_NAME": "abc", "VERSION": "1.0", "LANGUAGE": "python"},
	}

	output := renderCompactPackages(rows, 0, false)
	if len(output) < 2 {
		t.Fatalf("expected at least 2 output blocks, got %d", len(output))
	}
	if !strings.HasPrefix(output[0], "/* Unable to generate DDL") {
		t.Fatalf("expected fallback comment for first row, got %q", output[0])
	}
}

// TestServiceRunFallbackCommentOnUnrecognisedRow: Given a view row that
// InferDDLRequest cannot recognise, when processView runs, then a fallback
// comment should be written to the output file instead of silently skipping.
func TestServiceRunFallbackCommentOnUnrecognisedRow(t *testing.T) {
	t.Parallel()

	outputDir := t.TempDir()
	cfg := appconfig.Config{
		Database:       "DEMO_DB",
		OutputDir:      outputDir,
		LogDir:         outputDir,
		MaxConnections: 1,
	}

	repo := fakeRepo{
		views: []string{"UNKNOWN_VIEW"},
		rows: map[string][]Row{
			// Row has no keys that InferDDLRequest can map to a DDL object.
			"UNKNOWN_VIEW": {
				{"SOME_UNRECOGNIZED_KEY": "value"},
			},
		},
		ddls: map[string]string{},
	}

	service := NewService(repo, log.New(io.Discard, "", 0), cfg)
	summary, err := service.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	// One file should have been written (the view output + storage integrations file).
	if summary.FilesWritten < 1 {
		t.Fatalf("expected at least 1 file written, got %d", summary.FilesWritten)
	}

	content, err := os.ReadFile(filepath.Join(outputDir, "unknown_view.sql"))
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if !strings.Contains(string(content), "Unable to generate DDL") {
		t.Fatalf("expected fallback comment in output, got %s", string(content))
	}
}

// TestSanitizeFileName: Given file names containing path-unsafe characters,
// when sanitizeFileName runs, then each unsafe char should become an underscore.
func TestSanitizeFileName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   string
		want string
	}{
		{in: "my/file.sql", want: "my_file.sql"},
		{in: "a b:c\\\\d", want: "a_b_c_d"},
		{in: "clean.sql", want: "clean.sql"},
		{in: "a/b/c", want: "a_b_c"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			got := sanitizeFileName(tc.in)
			if got != tc.want {
				t.Fatalf("sanitizeFileName(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestRenderCompactPackagesMissingLanguage: Given a PACKAGES row without the
// LANGUAGE field, when compact rendering runs, then the language field in the
// emitted line should default to "unknown".
func TestRenderCompactPackagesMissingLanguage(t *testing.T) {
	t.Parallel()

	rows := []Row{
		{"PACKAGE_NAME": "mylib", "VERSION": "1.0"},
	}

	output := renderCompactPackages(rows, 0, false)
	if len(output) != 1 {
		t.Fatalf("expected 1 output block, got %d", len(output))
	}
	if !strings.Contains(output[0], "language 'unknown'") {
		t.Fatalf("expected language 'unknown' in compact output, got %q", output[0])
	}
}

// TestServiceRunVerboseLogsWriteFile: Given verbose mode enabled, when a view
// is processed, then the logger should receive a wrote_file= log line.
func TestServiceRunVerboseLogsWriteFile(t *testing.T) {
	t.Parallel()

	outputDir := t.TempDir()
	var buf strings.Builder
	logger := log.New(&buf, "", 0)

	cfg := appconfig.Config{
		Database:       "DEMO_DB",
		OutputDir:      outputDir,
		LogDir:         outputDir,
		MaxConnections: 1,
		Verbose:        true,
	}

	repo := fakeRepo{
		views:               []string{"TABLES"},
		rows:                map[string][]Row{"TABLES": {}},
		ddls:                map[string]string{},
		storageIntegrations: []string{},
	}

	service := NewService(repo, logger, cfg)
	_, err := service.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	logOutput := buf.String()
	if !strings.Contains(logOutput, "wrote_file=") {
		t.Fatalf("expected wrote_file= in verbose log output, got:\n%s", logOutput)
	}
}
