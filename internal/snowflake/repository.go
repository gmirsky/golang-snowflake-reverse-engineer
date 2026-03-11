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

const applicationName = "golang-snowflake-reverse-engineer"

type Repository struct {
	db *sql.DB
}

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
		Authenticator:  sf.AuthTypeJwt,
		PrivateKey:     privateKey,
		Application:    applicationName,
		LoginTimeout:   30 * time.Second,
		RequestTimeout: 60 * time.Second,
	}

	connector := sf.NewConnector(sf.SnowflakeDriver{}, driverConfig)
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(cfg.MaxConnections)
	db.SetMaxIdleConns(cfg.MaxConnections)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping snowflake: %w", err)
	}

	return &Repository{db: db}, nil
}

func (r *Repository) Close() error {
	if r == nil || r.db == nil {
		return nil
	}
	return r.db.Close()
}

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

func (r *Repository) FetchViewRows(ctx context.Context, database string, viewName string) ([]reverseengineer.Row, error) {
	query := fmt.Sprintf("SELECT * FROM %s", qualifiedView(database, viewName))
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query %s: %w", viewName, err)
	}
	defer rows.Close()

	return scanRows(rows)
}

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

// ListStorageIntegrations executes SHOW INTEGRATIONS and returns the names of
// all integrations whose type is STORAGE.
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
		rawType, ok := row["TYPE"]
		if !ok {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(fmt.Sprint(rawType)), "STORAGE") {
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

// DescStorageIntegration executes DESC STORAGE INTEGRATION <name> and returns
// the result rows. Each row contains PROPERTY, PROPERTY_VALUE, PROPERTY_TYPE,
// PROPERTY_DEFAULT, and PARENT_INTEGRATION columns (uppercased by scanRows).
func (r *Repository) DescStorageIntegration(ctx context.Context, name string) ([]reverseengineer.Row, error) {
	query := fmt.Sprintf("DESC STORAGE INTEGRATION %s", quoteQualifiedName(name))
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("desc storage integration %s: %w", name, err)
	}
	defer rows.Close()

	return scanRows(rows)
}

func qualifiedView(database string, viewName string) string {
	return quoteQualifiedName(database, "INFORMATION_SCHEMA", viewName)
}

func quoteQualifiedName(parts ...string) string {
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		quoted = append(quoted, `"`+strings.ReplaceAll(strings.TrimSpace(part), `"`, `""`)+`"`)
	}
	return strings.Join(quoted, ".")
}

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
			destinations[index] = &values[index]
		}

		if err := rows.Scan(destinations...); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}

		row := make(reverseengineer.Row, len(columns))
		for index, column := range columns {
			row[strings.ToUpper(column)] = normalizeValue(values[index])
		}
		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rows: %w", err)
	}

	return results, nil
}

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
