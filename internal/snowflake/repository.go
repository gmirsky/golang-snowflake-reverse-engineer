// Package snowflake provides a Snowflake-backed Repository implementation and
// key-pair authentication helpers.
package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	appconfig "github.com/gmirsky/golang-snowflake-reverse-engineer/internal/config"
	"github.com/gmirsky/golang-snowflake-reverse-engineer/internal/reverseengineer"
	sf "github.com/snowflakedb/gosnowflake/v2"
)

// applicationName is reported to Snowflake via the driver config so queries
// show a recognizable source in the query history.
const applicationName = "golang-snowflake-reverse-engineer"

// Repository implements reverseengineer.Repository against a live Snowflake
// database using the gosnowflake driver.
type Repository struct {
	db *sql.DB
}

// NewRepository: Given config and credentials, when initialization runs, then
// a verified Snowflake connection pool is returned.
func NewRepository(ctx context.Context, cfg appconfig.Config) (*Repository, error) {
	privateKey, err := LoadPrivateKey(cfg.PrivateKeyPath, cfg.Passphrase)
	if err != nil {
		return nil, err
	}

	driverConfig := sf.Config{
		Account:        cfg.Account,
		User:           cfg.User,
		Database:       cfg.Database,
		Warehouse:      cfg.Warehouse,
		Authenticator:  sf.AuthTypeJwt, // JWT key-pair authentication
		PrivateKey:     privateKey,
		Application:    applicationName,
		LoginTimeout:   30 * time.Second,
		RequestTimeout: 60 * time.Second,
	}

	connector := sf.NewConnector(sf.SnowflakeDriver{}, driverConfig)
	db := sql.OpenDB(connector)
	// Keep idle connections equal to MaxConnections to avoid reconnect overhead.
	db.SetMaxOpenConns(cfg.MaxConnections)
	db.SetMaxIdleConns(cfg.MaxConnections)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping snowflake: %w", err)
	}

	return &Repository{db: db}, nil
}

// Close: Given a repository instance, when shutdown runs, then database
// connections are released safely even for nil receivers.
func (r *Repository) Close() error {
	if r == nil || r.db == nil {
		return nil
	}
	return r.db.Close()
}

// ListViews: Given a database name, when querying INFORMATION_SCHEMA.VIEWS,
// then all view names are returned in sorted query order.
func (r *Repository) ListViews(ctx context.Context, database string) ([]string, error) {
	query := fmt.Sprintf("SELECT table_name FROM %s WHERE table_schema = 'INFORMATION_SCHEMA' ORDER BY table_name", qualifiedView(database, "VIEWS"))
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query information_schema.views: %w", err)
	}
	defer rows.Close()

	var views []string
	for rows.Next() {
		var viewName string
		if err := rows.Scan(&viewName); err != nil {
			return nil, fmt.Errorf("scan view name: %w", err)
		}
		views = append(views, viewName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate view names: %w", err)
	}

	return views, nil
}

// FetchViewRows: Given a view name, when SELECT * executes, then rows are
// returned as uppercase-key maps for case-insensitive lookups.
func (r *Repository) FetchViewRows(ctx context.Context, database string, viewName string) ([]reverseengineer.Row, error) {
	query := fmt.Sprintf("SELECT * FROM %s", qualifiedView(database, viewName))
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query %s: %w", viewName, err)
	}
	defer rows.Close()

	return scanRows(rows)
}

// FetchDDL: Given a DDL request, when GET_DDL executes, then non-empty DDL is
// returned or an error is raised for null/empty responses.
func (r *Repository) FetchDDL(ctx context.Context, request reverseengineer.DDLRequest) (string, error) {
	var ddl sql.NullString
	err := r.db.QueryRowContext(ctx, "SELECT GET_DDL(?, ?)", request.ObjectType, request.QualifiedName).Scan(&ddl)
	if err != nil {
		return "", fmt.Errorf("fetch %s ddl for %s: %w", request.ObjectType, request.QualifiedName, err)
	}
	if !ddl.Valid || strings.TrimSpace(ddl.String) == "" {
		return "", fmt.Errorf("empty DDL returned for %s", request.QualifiedName)
	}
	return ddl.String, nil
}

// ListStorageIntegrations: Given SHOW INTEGRATIONS results, when filtering
// runs, then only storage integration names are returned.
func (r *Repository) ListStorageIntegrations(ctx context.Context) ([]string, error) {
	rows, err := r.db.QueryContext(ctx, "SHOW INTEGRATIONS")
	if err != nil {
		return nil, fmt.Errorf("show integrations: %w", err)
	}
	defer rows.Close()

	allRows, err := scanRows(rows)
	if err != nil {
		return nil, fmt.Errorf("scan show integrations: %w", err)
	}

	var names []string
	for _, row := range allRows {
		if !isStorageIntegrationRow(row) {
			continue
		}
		rawName, ok := row["NAME"]
		if !ok {
			continue
		}
		name := strings.TrimSpace(fmt.Sprint(rawName))
		if name != "" {
			names = append(names, name)
		}
	}

	return names, nil
}

func isStorageIntegrationRow(row reverseengineer.Row) bool {
	// Preferred signal in modern Snowflake output.
	if strings.EqualFold(strings.TrimSpace(fmt.Sprint(row["CATEGORY"])), "STORAGE") {
		return true
	}

	// Backward-compatible fallback in case SHOW INTEGRATIONS is projected
	// without CATEGORY and older logic depended on TYPE=STORAGE.
	return strings.EqualFold(strings.TrimSpace(fmt.Sprint(row["TYPE"])), "STORAGE")
}

// DescStorageIntegration: Given an integration name, when DESC STORAGE
// INTEGRATION runs, then normalized property rows are returned.
func (r *Repository) DescStorageIntegration(ctx context.Context, name string) ([]reverseengineer.Row, error) {
	query := fmt.Sprintf("DESC STORAGE INTEGRATION %s", quoteQualifiedName(name))
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("desc storage integration %s: %w", name, err)
	}
	defer rows.Close()

	return scanRows(rows)
}

// qualifiedView: Given a database and view name, when qualification runs,
// then a fully quoted INFORMATION_SCHEMA path is returned.
func qualifiedView(database string, viewName string) string {
	return quoteQualifiedName(database, "INFORMATION_SCHEMA", viewName)
}

// quoteQualifiedName: Given identifier parts, when quoting runs, then a
// dot-separated, SQL-safe qualified name is returned.
func quoteQualifiedName(parts ...string) string {
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		quoted = append(quoted, `"`+strings.ReplaceAll(strings.TrimSpace(part), `"`, `""`)+`"`)
	}
	return strings.Join(quoted, ".")
}

// scanRows: Given sql.Rows, when scanning runs, then all rows are normalized
// into uppercase-key maps with driver-safe value conversions.
func scanRows(rows *sql.Rows) ([]reverseengineer.Row, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("fetch columns: %w", err)
	}

	results := make([]reverseengineer.Row, 0)
	for rows.Next() {
		values := make([]any, len(columns))
		destinations := make([]any, len(columns))
		for index := range values {
			// sql.Rows.Scan writes into pointers, so build a pointer slice first.
			destinations[index] = &values[index]
		}

		if err := rows.Scan(destinations...); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}

		row := make(reverseengineer.Row, len(columns))
		for index, column := range columns {
			// Uppercase all column names for case-insensitive key lookups.
			row[strings.ToUpper(column)] = normalizeValue(values[index])
		}
		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rows: %w", err)
	}

	return results, nil
}

// normalizeValue: Given a raw driver value, when normalization runs, then
// bytes/time values are converted to stable string formats for serialization.
func normalizeValue(value any) any {
	switch typed := value.(type) {
	case nil:
		return nil
	case []byte:
		return string(typed)
	case time.Time:
		return typed.UTC().Format(time.RFC3339Nano)
	default:
		return typed
	}
}
