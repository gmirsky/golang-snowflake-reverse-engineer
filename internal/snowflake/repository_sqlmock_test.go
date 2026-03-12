package snowflake

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/gmirsky/golang-snowflake-reverse-engineer/internal/reverseengineer"
)

func newMockRepo(t *testing.T) (*Repository, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return &Repository{db: db}, mock
}

func TestRepositoryCloseNilReceiver(t *testing.T) {
	t.Parallel()

	var repo *Repository
	if err := repo.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestRepositoryCloseNilDB(t *testing.T) {
	t.Parallel()

	repo := &Repository{db: nil}
	if err := repo.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestRepositoryListViewsReturnsRows(t *testing.T) {
	t.Parallel()

	repo, mock := newMockRepo(t)
	query := "SELECT table_name FROM \"DEMO_DB\".\"INFORMATION_SCHEMA\".\"VIEWS\" WHERE table_schema = 'INFORMATION_SCHEMA' ORDER BY table_name"
	rows := sqlmock.NewRows([]string{"table_name"}).AddRow("A_VIEW").AddRow("B_VIEW")
	mock.ExpectQuery(regexp.QuoteMeta(query)).WillReturnRows(rows)

	views, err := repo.ListViews(context.Background(), "DEMO_DB")
	if err != nil {
		t.Fatalf("ListViews() error = %v", err)
	}
	if len(views) != 2 || views[0] != "A_VIEW" || views[1] != "B_VIEW" {
		t.Fatalf("unexpected views: %#v", views)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestRepositoryListViewsQueryError(t *testing.T) {
	t.Parallel()

	repo, mock := newMockRepo(t)
	query := "SELECT table_name FROM \"DEMO_DB\".\"INFORMATION_SCHEMA\".\"VIEWS\" WHERE table_schema = 'INFORMATION_SCHEMA' ORDER BY table_name"
	mock.ExpectQuery(regexp.QuoteMeta(query)).WillReturnError(errors.New("boom"))

	_, err := repo.ListViews(context.Background(), "DEMO_DB")
	if err == nil || !strings.Contains(err.Error(), "query information_schema.views") {
		t.Fatalf("expected wrapped query error, got %v", err)
	}
}

func TestRepositoryListViewsScanError(t *testing.T) {
	t.Parallel()

	repo, mock := newMockRepo(t)
	query := "SELECT table_name FROM \"DEMO_DB\".\"INFORMATION_SCHEMA\".\"VIEWS\" WHERE table_schema = 'INFORMATION_SCHEMA' ORDER BY table_name"
	rows := sqlmock.NewRows([]string{"table_name"}).AddRow(nil)
	mock.ExpectQuery(regexp.QuoteMeta(query)).WillReturnRows(rows)

	_, err := repo.ListViews(context.Background(), "DEMO_DB")
	if err == nil || !strings.Contains(err.Error(), "scan view name") {
		t.Fatalf("expected wrapped scan error, got %v", err)
	}
}

func TestRepositoryListViewsIterationError(t *testing.T) {
	t.Parallel()

	repo, mock := newMockRepo(t)
	query := "SELECT table_name FROM \"DEMO_DB\".\"INFORMATION_SCHEMA\".\"VIEWS\" WHERE table_schema = 'INFORMATION_SCHEMA' ORDER BY table_name"
	rows := sqlmock.NewRows([]string{"table_name"}).AddRow("A_VIEW").RowError(0, errors.New("row failure"))
	mock.ExpectQuery(regexp.QuoteMeta(query)).WillReturnRows(rows)

	_, err := repo.ListViews(context.Background(), "DEMO_DB")
	if err == nil || !strings.Contains(err.Error(), "iterate view names") {
		t.Fatalf("expected wrapped iteration error, got %v", err)
	}
}

func TestRepositoryFetchViewRowsNormalizesValuesAndKeys(t *testing.T) {
	t.Parallel()

	repo, mock := newMockRepo(t)
	query := "SELECT * FROM \"DEMO_DB\".\"INFORMATION_SCHEMA\".\"SCHEMATA\""
	when := time.Date(2026, time.March, 12, 12, 0, 0, 0, time.FixedZone("UTC-5", -5*60*60))
	rows := sqlmock.NewRows([]string{"name", "created_at", "blob_value"}).
		AddRow("PUBLIC", when, []byte("abc"))
	mock.ExpectQuery(regexp.QuoteMeta(query)).WillReturnRows(rows)

	got, err := repo.FetchViewRows(context.Background(), "DEMO_DB", "SCHEMATA")
	if err != nil {
		t.Fatalf("FetchViewRows() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 row, got %d", len(got))
	}
	if got[0]["NAME"] != "PUBLIC" {
		t.Fatalf("unexpected NAME value: %#v", got[0]["NAME"])
	}
	if got[0]["BLOB_VALUE"] != "abc" {
		t.Fatalf("unexpected BLOB_VALUE: %#v", got[0]["BLOB_VALUE"])
	}
	wantTime := when.UTC().Format(time.RFC3339Nano)
	if got[0]["CREATED_AT"] != wantTime {
		t.Fatalf("unexpected CREATED_AT: got %v want %v", got[0]["CREATED_AT"], wantTime)
	}
}

func TestRepositoryFetchViewRowsQueryError(t *testing.T) {
	t.Parallel()

	repo, mock := newMockRepo(t)
	query := "SELECT * FROM \"DEMO_DB\".\"INFORMATION_SCHEMA\".\"SCHEMATA\""
	mock.ExpectQuery(regexp.QuoteMeta(query)).WillReturnError(errors.New("boom"))

	_, err := repo.FetchViewRows(context.Background(), "DEMO_DB", "SCHEMATA")
	if err == nil || !strings.Contains(err.Error(), "query SCHEMATA") {
		t.Fatalf("expected wrapped query error, got %v", err)
	}
}

func TestRepositoryFetchViewRowsIterationError(t *testing.T) {
	t.Parallel()

	repo, mock := newMockRepo(t)
	query := "SELECT * FROM \"DEMO_DB\".\"INFORMATION_SCHEMA\".\"SCHEMATA\""
	rows := sqlmock.NewRows([]string{"name"}).AddRow("PUBLIC").RowError(0, errors.New("row failure"))
	mock.ExpectQuery(regexp.QuoteMeta(query)).WillReturnRows(rows)

	_, err := repo.FetchViewRows(context.Background(), "DEMO_DB", "SCHEMATA")
	if err == nil || !strings.Contains(err.Error(), "iterate rows") {
		t.Fatalf("expected wrapped iteration error, got %v", err)
	}
}

func TestRepositoryFetchDDL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		rowValue  any
		queryErr  error
		wantError string
	}{
		{name: "valid", rowValue: "CREATE DATABASE DEMO_DB", wantError: ""},
		{name: "query error", queryErr: errors.New("boom"), wantError: "fetch DATABASE ddl"},
		{name: "null ddl", rowValue: nil, wantError: "empty DDL returned"},
		{name: "blank ddl", rowValue: "   ", wantError: "empty DDL returned"},
	}

	for _, testCase := range tests {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			repo, mock := newMockRepo(t)
			request := reverseengineer.DDLRequest{ObjectType: "DATABASE", QualifiedName: "\"DEMO_DB\""}
			query := regexp.QuoteMeta("SELECT GET_DDL(?, ?)")

			exp := mock.ExpectQuery(query).WithArgs(request.ObjectType, request.QualifiedName)
			if testCase.queryErr != nil {
				exp.WillReturnError(testCase.queryErr)
			} else {
				rows := sqlmock.NewRows([]string{"GET_DDL"}).AddRow(testCase.rowValue)
				exp.WillReturnRows(rows)
			}

			ddl, err := repo.FetchDDL(context.Background(), request)
			if testCase.wantError == "" {
				if err != nil {
					t.Fatalf("FetchDDL() error = %v", err)
				}
				if !strings.Contains(ddl, "CREATE") {
					t.Fatalf("unexpected ddl: %q", ddl)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), testCase.wantError) {
				t.Fatalf("expected error containing %q, got %v", testCase.wantError, err)
			}
		})
	}
}

func TestRepositoryListStorageIntegrationsFiltersStorage(t *testing.T) {
	t.Parallel()

	repo, mock := newMockRepo(t)
	rows := sqlmock.NewRows([]string{"NAME", "TYPE", "CATEGORY"}).
		AddRow("S1", "EXTERNAL_STAGE", "STORAGE").
		AddRow("S2", "STORAGE", nil).
		AddRow("N1", "API_AUTHENTICATION", "API").
		AddRow("  ", "STORAGE", nil)
	mock.ExpectQuery(regexp.QuoteMeta("SHOW INTEGRATIONS")).WillReturnRows(rows)

	names, err := repo.ListStorageIntegrations(context.Background())
	if err != nil {
		t.Fatalf("ListStorageIntegrations() error = %v", err)
	}
	if len(names) != 2 || names[0] != "S1" || names[1] != "S2" {
		t.Fatalf("unexpected integration names: %#v", names)
	}
}

func TestRepositoryListStorageIntegrationsQueryError(t *testing.T) {
	t.Parallel()

	repo, mock := newMockRepo(t)
	mock.ExpectQuery(regexp.QuoteMeta("SHOW INTEGRATIONS")).WillReturnError(errors.New("boom"))

	_, err := repo.ListStorageIntegrations(context.Background())
	if err == nil || !strings.Contains(err.Error(), "show integrations") {
		t.Fatalf("expected wrapped query error, got %v", err)
	}
}

func TestRepositoryListStorageIntegrationsScanError(t *testing.T) {
	t.Parallel()

	repo, mock := newMockRepo(t)
	rows := sqlmock.NewRows([]string{"NAME", "TYPE", "CATEGORY"}).
		AddRow("S1", "EXTERNAL_STAGE", "STORAGE").
		RowError(0, errors.New("row failure"))
	mock.ExpectQuery(regexp.QuoteMeta("SHOW INTEGRATIONS")).WillReturnRows(rows)

	_, err := repo.ListStorageIntegrations(context.Background())
	if err == nil || !strings.Contains(err.Error(), "scan show integrations") {
		t.Fatalf("expected wrapped scanRows error, got %v", err)
	}
}

func TestRepositoryDescStorageIntegration(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		repo, mock := newMockRepo(t)
		query := "DESC STORAGE INTEGRATION \"MY_INTG\""
		rows := sqlmock.NewRows([]string{"property", "property_value"}).AddRow("ENABLED", "true")
		mock.ExpectQuery(regexp.QuoteMeta(query)).WillReturnRows(rows)

		result, err := repo.DescStorageIntegration(context.Background(), "MY_INTG")
		if err != nil {
			t.Fatalf("DescStorageIntegration() error = %v", err)
		}
		if len(result) != 1 || result[0]["PROPERTY"] != "ENABLED" {
			t.Fatalf("unexpected result: %#v", result)
		}
	})

	t.Run("query error", func(t *testing.T) {
		repo, mock := newMockRepo(t)
		query := "DESC STORAGE INTEGRATION \"MY_INTG\""
		mock.ExpectQuery(regexp.QuoteMeta(query)).WillReturnError(errors.New("boom"))

		_, err := repo.DescStorageIntegration(context.Background(), "MY_INTG")
		if err == nil || !strings.Contains(err.Error(), "desc storage integration") {
			t.Fatalf("expected wrapped query error, got %v", err)
		}
	})
}
