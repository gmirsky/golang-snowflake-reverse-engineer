package reverseengineer

import "context"

type Row map[string]any

type DDLRequest struct {
	ObjectType    string
	QualifiedName string
	InlineSQL     string
	ViewName      string
	Row           Row
}

type Repository interface {
	ListViews(ctx context.Context, database string) ([]string, error)
	FetchViewRows(ctx context.Context, database string, viewName string) ([]Row, error)
	FetchDDL(ctx context.Context, request DDLRequest) (string, error)
}
