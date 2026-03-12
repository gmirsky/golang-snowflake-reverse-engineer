// Package reverseengineer provides DDL inference and rendering helpers used
// to convert INFORMATION_SCHEMA rows into executable Snowflake SQL statements.
package reverseengineer

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// InferDDLRequest: Given one INFORMATION_SCHEMA row, when inference runs, then
// either a fetchable/inline DDL request is returned or the row is marked unsupported.
func InferDDLRequest(database string, viewName string, row Row) (DDLRequest, bool) {
	// Handle views that map to inline SQL comments/GRANTs before GET_DDL paths.
	if request, ok := inferPrivilegeAndRoleView(viewName, row); ok {
		request.ViewName = viewName
		request.Row = row
		return request, true
	}

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
		// Many INFORMATION_SCHEMA views follow the same PREFIX_CATALOG/SCHEMA/NAME layout,
		// so this loop handles those patterns without repeating similar code blocks.
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
			// TABLE_TYPE and viewName together disambiguate TABLE vs VIEW output paths.
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

// RenderNoDataComment: Given a view name, when no rows exist, then a no-data
// SQL comment is returned for deterministic output.
func RenderNoDataComment(viewName string) string {
	return fmt.Sprintf("/* No data found in the view %s */\n", viewName)
}

// RenderFallbackComment: Given inference failure context, when fallback runs,
// then JSON metadata is embedded in a SQL comment instead of being discarded.
func RenderFallbackComment(viewName string, row Row, reason string) string {
	encoded, err := json.MarshalIndent(sortedRow(row), "", "  ")
	if err != nil {
		encoded = []byte(`{"error":"failed to serialize row metadata"}`)
	}

	return fmt.Sprintf("/* Unable to generate DDL for view %s: %s\n%s\n*/", viewName, reason, string(encoded))
}

// EnsureTerminatedSQL: Given SQL text, when termination is checked, then one
// trailing semicolon is ensured for executable output.
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

// inferQualifiedObject: Given prefix-shaped row fields, when all parts exist,
// then a qualified DDL request is produced.
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

// inferRoutineLikeObject: Given routine metadata, when inference runs, then
// a qualified routine name with optional signature is produced.
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

func inferPrivilegeAndRoleView(viewName string, row Row) (DDLRequest, bool) {
	switch strings.ToUpper(strings.TrimSpace(viewName)) {
	case "APPLICABLE_ROLES", "ENABLED_ROLES":
		roleName, ok := getString(row, "ROLE_NAME")
		if !ok {
			return DDLRequest{}, false
		}
		if strings.EqualFold(viewName, "APPLICABLE_ROLES") {
			if grantee, ok := getString(row, "GRANTEE"); ok {
				return DDLRequest{
					InlineSQL: fmt.Sprintf("-- Applicable role %s is granted to %s", quoteIdentifier(roleName), quoteIdentifier(grantee)),
				}, true
			}
		}
		return DDLRequest{
			InlineSQL: fmt.Sprintf("-- Enabled role in current session: %s", quoteIdentifier(roleName)),
		}, true
	case "SHARES":
		shareName, ok := getString(row, "NAME")
		if !ok {
			return DDLRequest{}, false
		}
		statement := fmt.Sprintf("CREATE SHARE IF NOT EXISTS %s", quoteIdentifier(shareName))
		if comment, ok := getString(row, "COMMENT"); ok {
			trimmedComment := strings.TrimSpace(comment)
			if trimmedComment != "" {
				statement += fmt.Sprintf(" COMMENT = %s", quoteLiteral(trimmedComment))
			}
		}
		return DDLRequest{
			InlineSQL: statement,
		}, true
	case "LISTINGS":
		listingName, okListing := getString(row, "NAME")
		if shareName, ok := getString(row, "SHARE"); ok {
			return DDLRequest{
				InlineSQL: fmt.Sprintf("-- Listing %s publishes share %s", quoteIdentifier(listingName), quoteIdentifier(shareName)),
			}, true
		}
		if okListing {
			return DDLRequest{
				InlineSQL: fmt.Sprintf("-- Listing %s has no share reference in INFORMATION_SCHEMA.LISTINGS", quoteIdentifier(listingName)),
			}, true
		}
		return DDLRequest{}, false
	case "OBJECT_PRIVILEGES":
		statement, ok := inferObjectPrivilegeStatement(row)
		if !ok {
			return DDLRequest{}, false
		}
		return DDLRequest{InlineSQL: statement}, true
	case "DATABASES":
		statement, ok := inferDatabaseStatement(row)
		if !ok {
			return DDLRequest{}, false
		}
		return DDLRequest{InlineSQL: statement}, true
	case "ELEMENT_TYPES":
		statement, ok := inferElementTypesStatement(row)
		if !ok {
			return DDLRequest{}, false
		}
		return DDLRequest{InlineSQL: statement}, true
	case "PACKAGES":
		statement, ok := inferPackageStatement(row)
		if !ok {
			return DDLRequest{}, false
		}
		return DDLRequest{InlineSQL: statement}, true
	case "REPLICATION_GROUPS":
		statement, ok := inferReplicationGroupStatement(row)
		if !ok {
			return DDLRequest{}, false
		}
		return DDLRequest{InlineSQL: statement}, true
	default:
		return DDLRequest{}, false
	}
}

func inferDatabaseStatement(row Row) (string, bool) {
	name, ok := getString(row, "DATABASE_NAME")
	if !ok {
		return "", false
	}

	databaseType, _ := getString(row, "TYPE")
	isTransient, _ := getString(row, "IS_TRANSIENT")
	comment, _ := getString(row, "COMMENT")

	upperType := strings.ToUpper(strings.TrimSpace(databaseType))
	if upperType == "" || upperType == "STANDARD" || strings.EqualFold(isTransient, "YES") {
		prefix := "CREATE DATABASE IF NOT EXISTS"
		if strings.EqualFold(isTransient, "YES") {
			prefix = "CREATE TRANSIENT DATABASE IF NOT EXISTS"
		}
		statement := fmt.Sprintf("%s %s", prefix, quoteIdentifier(name))
		if strings.TrimSpace(comment) != "" {
			statement += fmt.Sprintf(" COMMENT = %s", quoteLiteral(comment))
		}
		return statement, true
	}

	return fmt.Sprintf("-- Database %s is type %s and requires specialized provisioning", quoteIdentifier(name), quoteLiteral(databaseType)), true
}

func inferElementTypesStatement(row Row) (string, bool) {
	objectType, okObjectType := getString(row, "OBJECT_TYPE")
	objectCatalog, okCatalog := getString(row, "OBJECT_CATALOG")
	objectSchema, okSchema := getString(row, "OBJECT_SCHEMA")
	objectName, okName := getString(row, "OBJECT_NAME")
	dataType, okDataType := getString(row, "DATA_TYPE")
	identifier, _ := getString(row, "COLLECTION_TYPE_IDENTIFIER")

	if !okObjectType || !okCatalog || !okSchema || !okName || !okDataType {
		return "", false
	}

	qualified := quoteQualifiedName(objectCatalog, objectSchema, objectName)
	if strings.TrimSpace(identifier) == "" {
		return fmt.Sprintf("-- Element type %s on %s %s", quoteLiteral(dataType), normalizeObjectType(objectType), qualified), true
	}

	return fmt.Sprintf("-- Element type %s on %s %s (collection %s)", quoteLiteral(dataType), normalizeObjectType(objectType), qualified, quoteLiteral(identifier)), true
}

func inferPackageStatement(row Row) (string, bool) {
	packageName, okName := getString(row, "PACKAGE_NAME")
	version, okVersion := getString(row, "VERSION")
	if !okName || !okVersion {
		return "", false
	}

	language, _ := getString(row, "LANGUAGE")
	runtimeVersion, _ := getString(row, "RUNTIME_VERSION")
	if strings.TrimSpace(runtimeVersion) == "" {
		runtimeVersion = "default"
	}

	return fmt.Sprintf("-- Package %s version %s language %s runtime %s", quoteIdentifier(packageName), quoteLiteral(version), quoteLiteral(language), quoteLiteral(runtimeVersion)), true
}

func inferReplicationGroupStatement(row Row) (string, bool) {
	name, ok := getString(row, "NAME")
	if !ok {
		return "", false
	}

	groupType, _ := getString(row, "TYPE")
	isPrimary, _ := getString(row, "IS_PRIMARY")
	region, _ := getString(row, "SNOWFLAKE_REGION")
	account, _ := getString(row, "ACCOUNT_NAME")

	return fmt.Sprintf("-- Replication group %s type %s account %s region %s is_primary %s", quoteIdentifier(name), quoteLiteral(groupType), quoteLiteral(account), quoteLiteral(region), quoteLiteral(isPrimary)), true
}

func inferObjectPrivilegeStatement(row Row) (string, bool) {
	privilege, ok := getString(row, "PRIVILEGE_TYPE")
	if !ok {
		return "", false
	}

	grantedTo, okGrantedTo := getString(row, "GRANTED_TO")
	grantee, okGrantee := getString(row, "GRANTEE")
	if !okGrantedTo || !okGrantee {
		return "", false
	}

	objectType, okObjectType := getString(row, "OBJECT_TYPE")
	if !okObjectType {
		return "", false
	}

	name, okName := inferObjectNameFromPrivilegeRow(row)
	if !okName {
		return "", false
	}

	return fmt.Sprintf(
		"GRANT %s ON %s %s TO %s %s",
		normalizeObjectToken(privilege),
		normalizeObjectType(objectType),
		name,
		normalizePrincipalType(grantedTo),
		quoteIdentifier(grantee),
	), true
}

func inferObjectNameFromPrivilegeRow(row Row) (string, bool) {
	objectName, ok := getString(row, "OBJECT_NAME")
	if !ok {
		return "", false
	}

	catalog, okCatalog := getString(row, "OBJECT_CATALOG")
	schema, okSchema := getString(row, "OBJECT_SCHEMA")

	if okCatalog && okSchema {
		return quoteQualifiedName(catalog, schema, objectName), true
	}
	if okCatalog {
		return quoteQualifiedName(catalog, objectName), true
	}
	return quoteQualifiedName(objectName), true
}

// normalizeObjectType converts a raw type string to uppercase SQL token form,
// defaulting to "OBJECT" when the string is blank.
func normalizeObjectType(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "OBJECT"
	}
	return normalizeObjectToken(trimmed)
}

// normalizePrincipalType converts a grantee-type string to uppercase SQL form,
// defaulting to "ROLE" when blank.
func normalizePrincipalType(value string) string {
	principal := normalizeObjectToken(value)
	if principal == "" {
		return "ROLE"
	}
	return principal
}

// normalizeObjectToken upper-cases a token and collapses underscores/extra
// whitespace to single spaces (e.g. "file_format" → "FILE FORMAT").
func normalizeObjectToken(value string) string {
	cleaned := strings.TrimSpace(value)
	if cleaned == "" {
		return ""
	}
	cleaned = strings.ReplaceAll(cleaned, "_", " ")
	cleaned = strings.Join(strings.Fields(cleaned), " ")
	return strings.ToUpper(cleaned)
}

// quoteIdentifier wraps value in double-quotes, escaping any embedded
// double-quotes per SQL standard doubling rules.
func quoteIdentifier(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return `""`
	}
	return `"` + strings.ReplaceAll(trimmed, `"`, `""`) + `"`
}

// quoteLiteral wraps value in single-quotes, escaping embedded single-quotes
// by doubling them per SQL standard rules.
func quoteLiteral(value string) string {
	return `'` + strings.ReplaceAll(value, `'`, `''`) + `'`
}

// normalizeSignature ensures a routine argument signature is wrapped in
// parentheses, returning "()" when the signature is empty.
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

// quoteQualifiedName joins one or more identifier parts into a
// fully-qualified double-quoted name (e.g. "DB"."SCHEMA"."TABLE").
// Empty parts are silently skipped.
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

// getString retrieves a string value from a Row by key. It returns ("", false)
// for missing keys, nil values, and blank strings.
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
		// Non-string columns (e.g. numbers) are converted via fmt.Sprint.
		text := fmt.Sprint(typed)
		if strings.TrimSpace(text) == "" || text == "<nil>" {
			return "", false
		}
		return text, true
	}
}

// sortedRow returns a copy of row with keys in alphabetical order, used to
// produce stable JSON output in fallback comments.
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
