// Package reverseengineer contains the domain logic for reading Snowflake
// metadata and reconstructing CREATE / GRANT SQL statements from it.
package reverseengineer

import "context"

// Row is a single result row returned by a Snowflake query, keyed by
// uppercase column name.
type Row map[string]any

// DDLRequest describes a single object whose DDL should be fetched or
// constructed. Either QualifiedName+ObjectType (for GET_DDL calls) or
// InlineSQL (for directly computed statements) must be set.
type DDLRequest struct {
	ObjectType    string // Snowflake object type passed to GET_DDL (e.g. "TABLE")
	QualifiedName string // fully-qualified double-quoted identifier
	InlineSQL     string // pre-computed SQL; skips the GET_DDL call when non-empty
	ViewName      string // source INFORMATION_SCHEMA view name (for diagnostics)
	Row           Row    // original metadata row (for fallback comments)
}

// Repository abstracts all Snowflake I/O so that the service layer can be
// tested with a fake implementation without a live connection.
type Repository interface {
	// ListViews: Given a database, when discovery runs, then source
	// INFORMATION_SCHEMA view names are returned.
	ListViews(ctx context.Context, database string) ([]string, error)
	// FetchViewRows: Given one view, when retrieval runs, then all rows are
	// returned as normalized maps.
	FetchViewRows(ctx context.Context, database string, viewName string) ([]Row, error)
	// FetchDDL: Given an inferred request, when GET_DDL runs, then object DDL is
	// returned for resolvable rows.
	FetchDDL(ctx context.Context, request DDLRequest) (string, error)
	ListStorageIntegrations(ctx context.Context) ([]string, error)          // SHOW INTEGRATIONS filtered to storage integrations
	DescStorageIntegration(ctx context.Context, name string) ([]Row, error) // DESC STORAGE INTEGRATION <name>
}
