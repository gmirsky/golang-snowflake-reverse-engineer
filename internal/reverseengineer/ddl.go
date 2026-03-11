package reverseengineer

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

func InferDDLRequest(database string, viewName string, row Row) (DDLRequest, bool) {
	if request, ok := inferRoutineLikeObject("PROCEDURE", row, "PROCEDURE"); ok {
		request.ViewName = viewName
		request.Row = row
		return request, true
	}
	if request, ok := inferRoutineLikeObject("FUNCTION", row, "FUNCTION"); ok {
		request.ViewName = viewName
		request.Row = row
		return request, true
	}

	for _, candidate := range []struct {
		prefix     string
		objectType string
	}{
		{prefix: "TASK", objectType: "TASK"},
		{prefix: "STAGE", objectType: "STAGE"},
		{prefix: "PIPE", objectType: "PIPE"},
		{prefix: "STREAM", objectType: "STREAM"},
		{prefix: "SEQUENCE", objectType: "SEQUENCE"},
		{prefix: "FILE_FORMAT", objectType: "FILE FORMAT"},
		{prefix: "DYNAMIC_TABLE", objectType: "DYNAMIC TABLE"},
	} {
		if request, ok := inferQualifiedObject(candidate.prefix, candidate.objectType, row); ok {
			request.ViewName = viewName
			request.Row = row
			return request, true
		}
	}

	if tableCatalog, ok := getString(row, "TABLE_CATALOG"); ok {
		tableSchema, okSchema := getString(row, "TABLE_SCHEMA")
		tableName, okName := getString(row, "TABLE_NAME")
		if okSchema && okName {
			objectType := "TABLE"
			if tableType, ok := getString(row, "TABLE_TYPE"); ok {
				upper := strings.ToUpper(tableType)
				if strings.Contains(upper, "VIEW") {
					objectType = "VIEW"
				}
			}
			if strings.EqualFold(viewName, "VIEWS") {
				objectType = "VIEW"
			}
			if strings.EqualFold(viewName, "MATERIALIZED_VIEWS") {
				objectType = "VIEW"
			}
			return DDLRequest{
				ObjectType:    objectType,
				QualifiedName: quoteQualifiedName(tableCatalog, tableSchema, tableName),
				ViewName:      viewName,
				Row:           row,
			}, true
		}
	}

	if catalog, ok := getString(row, "CATALOG_NAME"); ok {
		if schema, ok := getString(row, "SCHEMA_NAME"); ok {
			return DDLRequest{
				ObjectType:    "SCHEMA",
				QualifiedName: quoteQualifiedName(catalog, schema),
				ViewName:      viewName,
				Row:           row,
			}, true
		}
		return DDLRequest{
			ObjectType:    "DATABASE",
			QualifiedName: quoteQualifiedName(catalog),
			ViewName:      viewName,
			Row:           row,
		}, true
	}

	if database != "" {
		if schema, ok := getString(row, "SCHEMA_NAME"); ok {
			return DDLRequest{
				ObjectType:    "SCHEMA",
				QualifiedName: quoteQualifiedName(database, schema),
				ViewName:      viewName,
				Row:           row,
			}, true
		}
	}

	return DDLRequest{}, false
}

func RenderNoDataComment(viewName string) string {
	return fmt.Sprintf("/* No data found in the view %s */\n", viewName)
}

func RenderFallbackComment(viewName string, row Row, reason string) string {
	encoded, err := json.MarshalIndent(sortedRow(row), "", "  ")
	if err != nil {
		encoded = []byte(`{"error":"failed to serialize row metadata"}`)
	}

	return fmt.Sprintf("/* Unable to generate DDL for view %s: %s\n%s\n*/", viewName, reason, string(encoded))
}

func EnsureTerminatedSQL(sqlText string) string {
	trimmed := strings.TrimSpace(sqlText)
	if trimmed == "" {
		return trimmed
	}
	if strings.HasSuffix(trimmed, ";") {
		return trimmed
	}
	return trimmed + ";"
}

func inferQualifiedObject(prefix string, objectType string, row Row) (DDLRequest, bool) {
	catalog, okCatalog := getString(row, prefix+"_CATALOG")
	schema, okSchema := getString(row, prefix+"_SCHEMA")
	name, okName := getString(row, prefix+"_NAME")
	if !okCatalog || !okSchema || !okName {
		return DDLRequest{}, false
	}

	return DDLRequest{
		ObjectType:    objectType,
		QualifiedName: quoteQualifiedName(catalog, schema, name),
	}, true
}

func inferRoutineLikeObject(prefix string, row Row, objectType string) (DDLRequest, bool) {
	catalog, okCatalog := getString(row, prefix+"_CATALOG")
	schema, okSchema := getString(row, prefix+"_SCHEMA")
	name, okName := getString(row, prefix+"_NAME")
	if !okCatalog || !okSchema || !okName {
		return DDLRequest{}, false
	}

	qualified := quoteQualifiedName(catalog, schema, name)
	if signature, ok := getString(row, "ARGUMENT_SIGNATURE"); ok {
		qualified += normalizeSignature(signature)
	}

	return DDLRequest{
		ObjectType:    objectType,
		QualifiedName: qualified,
	}, true
}

func normalizeSignature(signature string) string {
	trimmed := strings.TrimSpace(signature)
	if trimmed == "" {
		return "()"
	}
	if strings.HasPrefix(trimmed, "(") {
		return trimmed
	}
	return "(" + trimmed + ")"
}

func quoteQualifiedName(parts ...string) string {
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		quoted = append(quoted, `"`+strings.ReplaceAll(trimmed, `"`, `""`)+`"`)
	}
	return strings.Join(quoted, ".")
}

func getString(row Row, key string) (string, bool) {
	value, ok := row[key]
	if !ok || value == nil {
		return "", false
	}

	switch typed := value.(type) {
	case string:
		if strings.TrimSpace(typed) == "" {
			return "", false
		}
		return typed, true
	default:
		text := fmt.Sprint(typed)
		if strings.TrimSpace(text) == "" || text == "<nil>" {
			return "", false
		}
		return text, true
	}
}

func sortedRow(row Row) map[string]any {
	keys := make([]string, 0, len(row))
	for key := range row {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	result := make(map[string]any, len(row))
	for _, key := range keys {
		result[key] = row[key]
	}
	return result
}
