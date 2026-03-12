package reverseengineer

import (
	"strings"
	"testing"
)

// TestInferDDLRequestForTable: Given a TABLES row, when InferDDLRequest runs,
// then it should return a TABLE request with the expected qualified name.
func TestInferDDLRequestForTable(t *testing.T) {
	t.Parallel()

	request, ok := InferDDLRequest("DEMO_DB", "TABLES", Row{
		"TABLE_CATALOG": "DEMO_DB",
		"TABLE_SCHEMA":  "PUBLIC",
		"TABLE_NAME":    "CUSTOMERS",
		"TABLE_TYPE":    "BASE TABLE",
	})
	if !ok {
		t.Fatal("expected request to be inferred")
	}
	if request.ObjectType != "TABLE" {
		t.Fatalf("expected TABLE, got %s", request.ObjectType)
	}
	if request.QualifiedName != `"DEMO_DB"."PUBLIC"."CUSTOMERS"` {
		t.Fatalf("unexpected qualified name %s", request.QualifiedName)
	}
}

// TestInferDDLRequestForProcedure: Given a PROCEDURES row, when inference
// runs, then the argument signature should be preserved in the name.
func TestInferDDLRequestForProcedure(t *testing.T) {
	t.Parallel()

	request, ok := InferDDLRequest("DEMO_DB", "PROCEDURES", Row{
		"PROCEDURE_CATALOG":  "DEMO_DB",
		"PROCEDURE_SCHEMA":   "PUBLIC",
		"PROCEDURE_NAME":     "SYNC_DATA",
		"ARGUMENT_SIGNATURE": "(VARCHAR)",
	})
	if !ok {
		t.Fatal("expected request to be inferred")
	}
	if request.ObjectType != "PROCEDURE" {
		t.Fatalf("expected PROCEDURE, got %s", request.ObjectType)
	}
	if request.QualifiedName != `"DEMO_DB"."PUBLIC"."SYNC_DATA"(VARCHAR)` {
		t.Fatalf("unexpected qualified name %s", request.QualifiedName)
	}
}

// TestRenderFallbackComment: Given an unsupported row, when fallback rendering
// runs, then output should be a deterministic SQL comment.
func TestRenderFallbackComment(t *testing.T) {
	t.Parallel()

	comment := RenderFallbackComment("UNKNOWN_VIEW", Row{"A": "B"}, "unsupported object")
	if comment == "" {
		t.Fatal("expected non-empty fallback comment")
	}
	if comment[:2] != "/*" {
		t.Fatalf("expected SQL comment, got %q", comment)
	}
}

// TestInferDDLRequestForApplicableRoles: Given APPLICABLE_ROLES data, when
// inference runs, then role relationships should be emitted as inline SQL.
func TestInferDDLRequestForApplicableRoles(t *testing.T) {
	t.Parallel()

	request, ok := InferDDLRequest("DEMO_DB", "APPLICABLE_ROLES", Row{
		"ROLE_NAME": "SYSADMIN",
		"GRANTEE":   "ACCOUNTADMIN",
	})
	if !ok {
		t.Fatal("expected request to be inferred")
	}
	if request.InlineSQL == "" {
		t.Fatal("expected inline SQL")
	}
	expected := `-- Applicable role "SYSADMIN" is granted to "ACCOUNTADMIN"`
	if request.InlineSQL != expected {
		t.Fatalf("unexpected inline SQL %q", request.InlineSQL)
	}
}

// TestInferDDLRequestForEnabledRoles: Given ENABLED_ROLES data, when
// inference runs, then the enabled role should be emitted as inline SQL.
func TestInferDDLRequestForEnabledRoles(t *testing.T) {
	t.Parallel()

	request, ok := InferDDLRequest("DEMO_DB", "ENABLED_ROLES", Row{
		"ROLE_NAME": "ACCOUNTADMIN",
	})
	if !ok {
		t.Fatal("expected request to be inferred")
	}
	if request.InlineSQL == "" {
		t.Fatal("expected inline SQL")
	}
	expected := `-- Enabled role in current session: "ACCOUNTADMIN"`
	if request.InlineSQL != expected {
		t.Fatalf("unexpected inline SQL %q", request.InlineSQL)
	}
}

// TestInferDDLRequestForObjectPrivileges: Given OBJECT_PRIVILEGES data, when
// inference runs, then a normalized GRANT statement should be returned.
func TestInferDDLRequestForObjectPrivileges(t *testing.T) {
	t.Parallel()

	request, ok := InferDDLRequest("DEMO_DB", "OBJECT_PRIVILEGES", Row{
		"PRIVILEGE_TYPE": "SELECT",
		"OBJECT_TYPE":    "TABLE",
		"OBJECT_CATALOG": "DEMO_DB",
		"OBJECT_SCHEMA":  "PUBLIC",
		"OBJECT_NAME":    "CUSTOMERS",
		"GRANTED_TO":     "ROLE",
		"GRANTEE":        "ANALYST",
	})
	if !ok {
		t.Fatal("expected request to be inferred")
	}
	if request.InlineSQL == "" {
		t.Fatal("expected inline SQL for object privileges")
	}
	expected := `GRANT SELECT ON TABLE "DEMO_DB"."PUBLIC"."CUSTOMERS" TO ROLE "ANALYST"`
	if request.InlineSQL != expected {
		t.Fatalf("unexpected inline SQL %q", request.InlineSQL)
	}
}

// TestInferDDLRequestForShares: Given SHARES data, when inference runs, then
// output should be CREATE SHARE SQL with propagated comment text.
func TestInferDDLRequestForShares(t *testing.T) {
	t.Parallel()

	request, ok := InferDDLRequest("DEMO_DB", "SHARES", Row{
		"NAME":    "OUTBOUND_SHARE",
		"COMMENT": "test share",
	})
	if !ok {
		t.Fatal("expected request to be inferred")
	}
	if request.InlineSQL == "" {
		t.Fatal("expected inline SQL")
	}
	expected := `CREATE SHARE IF NOT EXISTS "OUTBOUND_SHARE" COMMENT = 'test share'`
	if request.InlineSQL != expected {
		t.Fatalf("unexpected inline SQL %q", request.InlineSQL)
	}
}

// TestInferDDLRequestForListings: Given LISTINGS data, when inference runs,
// then output should describe listing-to-share linkage.
func TestInferDDLRequestForListings(t *testing.T) {
	t.Parallel()

	request, ok := InferDDLRequest("DEMO_DB", "LISTINGS", Row{
		"NAME":  "MY_LISTING",
		"SHARE": "OUTBOUND_SHARE",
	})
	if !ok {
		t.Fatal("expected request to be inferred")
	}
	if request.InlineSQL == "" {
		t.Fatal("expected inline SQL")
	}
	expected := `-- Listing "MY_LISTING" publishes share "OUTBOUND_SHARE"`
	if request.InlineSQL != expected {
		t.Fatalf("unexpected inline SQL %q", request.InlineSQL)
	}
}

// TestInferDDLRequestForDatabases: Given DATABASES data, when inference runs,
// then standard databases should map to CREATE DATABASE SQL.
func TestInferDDLRequestForDatabases(t *testing.T) {
	t.Parallel()

	request, ok := InferDDLRequest("DEMO_DB", "DATABASES", Row{
		"DATABASE_NAME": "DEV_CC",
		"TYPE":          "STANDARD",
		"IS_TRANSIENT":  "NO",
		"COMMENT":       "development db",
	})
	if !ok {
		t.Fatal("expected request to be inferred")
	}
	expected := `CREATE DATABASE IF NOT EXISTS "DEV_CC" COMMENT = 'development db'`
	if request.InlineSQL != expected {
		t.Fatalf("unexpected inline SQL %q", request.InlineSQL)
	}
}

// TestInferDDLRequestForElementTypes: Given ELEMENT_TYPES data, when
// inference runs, then output should preserve metadata as descriptive SQL.
func TestInferDDLRequestForElementTypes(t *testing.T) {
	t.Parallel()

	request, ok := InferDDLRequest("DEMO_DB", "ELEMENT_TYPES", Row{
		"OBJECT_TYPE":                "TABLE",
		"OBJECT_CATALOG":             "DEMO_DB",
		"OBJECT_SCHEMA":              "INFORMATION_SCHEMA",
		"OBJECT_NAME":                "SEMANTIC_TABLES",
		"DATA_TYPE":                  "TEXT",
		"COLLECTION_TYPE_IDENTIFIER": "$8:e",
	})
	if !ok {
		t.Fatal("expected request to be inferred")
	}
	expected := `-- Element type 'TEXT' on TABLE "DEMO_DB"."INFORMATION_SCHEMA"."SEMANTIC_TABLES" (collection '$8:e')`
	if request.InlineSQL != expected {
		t.Fatalf("unexpected inline SQL %q", request.InlineSQL)
	}
}

// TestInferDDLRequestForPackages: Given PACKAGES data, when inference runs,
// then package metadata should be emitted as inline comments.
func TestInferDDLRequestForPackages(t *testing.T) {
	t.Parallel()

	request, ok := InferDDLRequest("DEMO_DB", "PACKAGES", Row{
		"PACKAGE_NAME":    "abi3audit",
		"VERSION":         "0.0.24",
		"LANGUAGE":        "python",
		"RUNTIME_VERSION": "3.12",
	})
	if !ok {
		t.Fatal("expected request to be inferred")
	}
	expected := `-- Package "abi3audit" version '0.0.24' language 'python' runtime '3.12'`
	if request.InlineSQL != expected {
		t.Fatalf("unexpected inline SQL %q", request.InlineSQL)
	}
}

// TestInferDDLRequestForReplicationGroups: Given REPLICATION_GROUPS data,
// when inference runs, then output should be an informational SQL comment.
func TestInferDDLRequestForReplicationGroups(t *testing.T) {
	t.Parallel()

	request, ok := InferDDLRequest("DEMO_DB", "REPLICATION_GROUPS", Row{
		"NAME":             "RG_ONE",
		"TYPE":             "REPLICATION",
		"ACCOUNT_NAME":     "MRI_SIMMONS_CONSUMER_CANVAS_AZURE",
		"SNOWFLAKE_REGION": "AZURE_CENTRALUS",
		"IS_PRIMARY":       true,
	})
	if !ok {
		t.Fatal("expected request to be inferred")
	}
	expected := `-- Replication group "RG_ONE" type 'REPLICATION' account 'MRI_SIMMONS_CONSUMER_CANVAS_AZURE' region 'AZURE_CENTRALUS' is_primary 'true'`
	if request.InlineSQL != expected {
		t.Fatalf("unexpected inline SQL %q", request.InlineSQL)
	}
}

// TestInferDDLRequestUnsupportedRow: Given a row shape with no recognized
// keys, when inference runs, then no DDL request should be produced.
func TestInferDDLRequestUnsupportedRow(t *testing.T) {
	t.Parallel()

	_, ok := InferDDLRequest("DEMO_DB", "UNKNOWN_VIEW", Row{"X": "Y"})
	if ok {
		t.Fatal("expected inference to fail for unsupported view/row")
	}
}

// TestInferDDLRequestForObjectPrivilegesRequiresObjectName: Given incomplete
// privilege metadata, when inference runs, then it should reject the row.
func TestInferDDLRequestForObjectPrivilegesRequiresObjectName(t *testing.T) {
	t.Parallel()

	_, ok := InferDDLRequest("DEMO_DB", "OBJECT_PRIVILEGES", Row{
		"PRIVILEGE_TYPE": "SELECT",
		"OBJECT_TYPE":    "TABLE",
		"GRANTED_TO":     "ROLE",
		"GRANTEE":        "ANALYST",
		// OBJECT_NAME intentionally absent.
	})
	if ok {
		t.Fatal("expected inference to fail when OBJECT_NAME is missing")
	}
}

// TestInferObjectNameFromPrivilegeRow: Given different privilege row layouts,
// when object-name inference runs, then qualification should degrade safely.
func TestInferObjectNameFromPrivilegeRow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		row  Row
		want string
		ok   bool
	}{
		{
			name: "catalog and schema present",
			row:  Row{"OBJECT_CATALOG": "DEMO_DB", "OBJECT_SCHEMA": "PUBLIC", "OBJECT_NAME": "T1"},
			want: `"DEMO_DB"."PUBLIC"."T1"`,
			ok:   true,
		},
		{
			name: "catalog only",
			row:  Row{"OBJECT_CATALOG": "DEMO_DB", "OBJECT_NAME": "T1"},
			want: `"DEMO_DB"."T1"`,
			ok:   true,
		},
		{
			name: "object only",
			row:  Row{"OBJECT_NAME": "T1"},
			want: `"T1"`,
			ok:   true,
		},
		{
			name: "missing object name",
			row:  Row{"OBJECT_CATALOG": "DEMO_DB"},
			want: "",
			ok:   false,
		},
	}

	for _, testCase := range tests {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			got, ok := inferObjectNameFromPrivilegeRow(testCase.row)
			if ok != testCase.ok {
				t.Fatalf("ok = %t, want %t", ok, testCase.ok)
			}
			if got != testCase.want {
				t.Fatalf("name = %q, want %q", got, testCase.want)
			}
		})
	}
}

// TestEnsureTerminatedSQL: Given SQL with and without terminators, when
// normalization runs, then one trailing semicolon should be ensured.
func TestEnsureTerminatedSQL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "blank", in: "   ", want: ""},
		{name: "already terminated", in: "SELECT 1;", want: "SELECT 1;"},
		{name: "adds terminator", in: "SELECT 1", want: "SELECT 1;"},
		{name: "trims and keeps single terminator", in: "  SELECT 1;  ", want: "SELECT 1;"},
	}

	for _, testCase := range tests {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			got := EnsureTerminatedSQL(testCase.in)
			if got != testCase.want {
				t.Fatalf("EnsureTerminatedSQL(%q) = %q, want %q", testCase.in, got, testCase.want)
			}
		})
	}
}

// TestNormalizeSignature: Given different signature inputs, when normalization
// runs, then argument signatures should always be parenthesized.
func TestNormalizeSignature(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "empty", in: "", want: "()"},
		{name: "whitespace", in: "   ", want: "()"},
		{name: "already wrapped", in: "(VARCHAR)", want: "(VARCHAR)"},
		{name: "needs wrapping", in: "VARCHAR", want: "(VARCHAR)"},
	}

	for _, testCase := range tests {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			got := normalizeSignature(testCase.in)
			if got != testCase.want {
				t.Fatalf("normalizeSignature(%q) = %q, want %q", testCase.in, got, testCase.want)
			}
		})
	}
}

// TestInferDDLRequestForCatalogDatabaseFallback: Given CATALOG_NAME-only rows,
// when inference runs, then DATABASE-level requests should be produced.
func TestInferDDLRequestForCatalogDatabaseFallback(t *testing.T) {
	t.Parallel()

	request, ok := InferDDLRequest("DEMO_DB", "SOME_VIEW", Row{
		"CATALOG_NAME": "ANOTHER_DB",
	})
	if !ok {
		t.Fatal("expected request to be inferred")
	}
	if request.ObjectType != "DATABASE" {
		t.Fatalf("expected DATABASE object type, got %s", request.ObjectType)
	}
	if request.QualifiedName != `"ANOTHER_DB"` {
		t.Fatalf("unexpected qualified name %q", request.QualifiedName)
	}
}

// TestInferDDLRequestForDatabaseScopedSchemaFallback: Given a database input
// and row with SCHEMA_NAME only, when inference runs, then schema request should be inferred.
func TestInferDDLRequestForDatabaseScopedSchemaFallback(t *testing.T) {
	t.Parallel()

	request, ok := InferDDLRequest("DEMO_DB", "SCHEMATA", Row{
		"SCHEMA_NAME": "PUBLIC",
	})
	if !ok {
		t.Fatal("expected request to be inferred")
	}
	if request.ObjectType != "SCHEMA" {
		t.Fatalf("expected SCHEMA object type, got %s", request.ObjectType)
	}
	if request.QualifiedName != `"DEMO_DB"."PUBLIC"` {
		t.Fatalf("unexpected qualified name %q", request.QualifiedName)
	}
}

// TestInferDDLRequestForListingsWithoutShare: Given LISTINGS rows without
// a share reference, when inference runs, then informational fallback SQL should be emitted.
func TestInferDDLRequestForListingsWithoutShare(t *testing.T) {
	t.Parallel()

	request, ok := InferDDLRequest("DEMO_DB", "LISTINGS", Row{
		"NAME": "ORPHAN_LISTING",
	})
	if !ok {
		t.Fatal("expected request to be inferred")
	}
	expected := `-- Listing "ORPHAN_LISTING" has no share reference in INFORMATION_SCHEMA.LISTINGS`
	if request.InlineSQL != expected {
		t.Fatalf("unexpected inline SQL %q", request.InlineSQL)
	}
}

// TestInferDDLRequestForSharesMissingName: Given SHARES rows missing NAME,
// when inference runs, then the row should be rejected.
func TestInferDDLRequestForSharesMissingName(t *testing.T) {
	t.Parallel()

	_, ok := InferDDLRequest("DEMO_DB", "SHARES", Row{
		"COMMENT": "missing share name",
	})
	if ok {
		t.Fatal("expected inference to fail when share name is missing")
	}
}

// TestInferDDLRequestForApplicableRolesWithoutGrantee: Given APPLICABLE_ROLES
// row with ROLE_NAME only, when inference runs, then the grantee-less fallback
// "Enabled role" comment should be emitted.
func TestInferDDLRequestForApplicableRolesWithoutGrantee(t *testing.T) {
	t.Parallel()

	request, ok := InferDDLRequest("DEMO_DB", "APPLICABLE_ROLES", Row{
		"ROLE_NAME": "SYSADMIN",
		// GRANTEE deliberately absent.
	})
	if !ok {
		t.Fatal("expected request to be inferred")
	}
	expected := `-- Enabled role in current session: "SYSADMIN"`
	if request.InlineSQL != expected {
		t.Fatalf("unexpected inline SQL %q", request.InlineSQL)
	}
}

// TestInferDDLRequestForApplicableRolesMissingRoleName: Given an
// APPLICABLE_ROLES row with no ROLE_NAME, when inference runs, then the row
// should be rejected since the role identity is unknown.
func TestInferDDLRequestForApplicableRolesMissingRoleName(t *testing.T) {
	t.Parallel()

	_, ok := InferDDLRequest("DEMO_DB", "APPLICABLE_ROLES", Row{
		"GRANTEE": "ACCOUNTADMIN",
		// ROLE_NAME deliberately absent.
	})
	if ok {
		t.Fatal("expected inference to fail when ROLE_NAME is missing")
	}
}

// TestInferDDLRequestForListingsMissingBothFields: Given a LISTINGS row with
// neither NAME nor SHARE, when inference runs, then the row should be rejected.
func TestInferDDLRequestForListingsMissingBothFields(t *testing.T) {
	t.Parallel()

	_, ok := InferDDLRequest("", "LISTINGS", Row{
		"OTHER": "value",
	})
	if ok {
		t.Fatal("expected inference to fail when both NAME and SHARE are missing")
	}
}

// TestInferDDLRequestForTransientDatabase: Given a DATABASES row with
// IS_TRANSIENT = "YES", when inference runs, then CREATE TRANSIENT DATABASE
// SQL should be produced.
func TestInferDDLRequestForTransientDatabase(t *testing.T) {
	t.Parallel()

	request, ok := InferDDLRequest("DEMO_DB", "DATABASES", Row{
		"DATABASE_NAME": "TEMP_DB",
		"IS_TRANSIENT":  "YES",
	})
	if !ok {
		t.Fatal("expected request to be inferred")
	}
	expected := `CREATE TRANSIENT DATABASE IF NOT EXISTS "TEMP_DB"`
	if request.InlineSQL != expected {
		t.Fatalf("unexpected inline SQL %q", request.InlineSQL)
	}
}

// TestInferDDLRequestForNonStandardDatabase: Given a DATABASES row with a
// non-standard TYPE, when inference runs, then a descriptive comment should be
// emitted rather than a CREATE statement.
func TestInferDDLRequestForNonStandardDatabase(t *testing.T) {
	t.Parallel()

	request, ok := InferDDLRequest("DEMO_DB", "DATABASES", Row{
		"DATABASE_NAME": "SHARED_DB",
		"TYPE":          "IMPORTED DATABASE",
	})
	if !ok {
		t.Fatal("expected request to be inferred")
	}
	if !strings.Contains(request.InlineSQL, "SHARED_DB") {
		t.Fatalf("expected database name in inline SQL, got %q", request.InlineSQL)
	}
	if !strings.Contains(request.InlineSQL, "IMPORTED DATABASE") {
		t.Fatalf("expected database type in inline SQL, got %q", request.InlineSQL)
	}
}

// TestInferPackageStatementMissingVersion: Given a PACKAGES row without
// VERSION, when inference runs, then the row should be rejected.
func TestInferPackageStatementMissingVersion(t *testing.T) {
	t.Parallel()

	_, ok := InferDDLRequest("", "PACKAGES", Row{
		"PACKAGE_NAME": "mypackage",
		// VERSION deliberately absent.
	})
	if ok {
		t.Fatal("expected inference to fail when VERSION is missing")
	}
}

// TestInferObjectPrivilegeStatementMissingFields: Given OBJECT_PRIVILEGES rows
// each missing one required field, when inference runs, then each should be
// rejected rather than producing partial SQL.
func TestInferObjectPrivilegeStatementMissingFields(t *testing.T) {
	t.Parallel()

	base := Row{
		"PRIVILEGE_TYPE": "SELECT",
		"OBJECT_TYPE":    "TABLE",
		"OBJECT_CATALOG": "DEMO_DB",
		"OBJECT_SCHEMA":  "PUBLIC",
		"OBJECT_NAME":    "CUSTOMERS",
		"GRANTED_TO":     "ROLE",
		"GRANTEE":        "ANALYST",
	}
	tests := []struct {
		name   string
		remove string
	}{
		{"missing privilege type", "PRIVILEGE_TYPE"},
		{"missing grantee", "GRANTEE"},
		{"missing granted_to", "GRANTED_TO"},
		{"missing object type", "OBJECT_TYPE"},
	}

	for _, testCase := range tests {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			row := make(Row, len(base))
			for k, v := range base {
				row[k] = v
			}
			delete(row, testCase.remove)

			_, ok := InferDDLRequest("DEMO_DB", "OBJECT_PRIVILEGES", row)
			if ok {
				t.Fatalf("expected inference to fail when %s is missing", testCase.remove)
			}
		})
	}
}

// TestGetStringWithNonStringValue: Given a row containing a non-string numeric
// value, when getString runs, then it should convert via fmt.Sprint.
func TestGetStringWithNonStringValue(t *testing.T) {
	t.Parallel()

	row := Row{"COUNT": 42}
	val, ok := getString(row, "COUNT")
	if !ok {
		t.Fatal("expected getString to return true for non-string value")
	}
	if val != "42" {
		t.Fatalf("expected \"42\", got %q", val)
	}
}

// TestNormalizeObjectTypeBlank: Given a blank string, when normalizeObjectType
// runs, then it should default to "OBJECT".
func TestNormalizeObjectTypeBlank(t *testing.T) {
	t.Parallel()

	got := normalizeObjectType("")
	if got != "OBJECT" {
		t.Fatalf("expected OBJECT for blank input, got %q", got)
	}
}

// TestNormalizePrincipalTypeBlank: Given a blank string, when
// normalizePrincipalType runs, then it should default to "ROLE".
func TestNormalizePrincipalTypeBlank(t *testing.T) {
	t.Parallel()

	got := normalizePrincipalType("")
	if got != "ROLE" {
		t.Fatalf("expected ROLE for blank input, got %q", got)
	}
}

// TestQuoteIdentifierBlank: Given a blank or empty string, when
// quoteIdentifier runs, then it should return an empty double-quoted token.
func TestQuoteIdentifierBlank(t *testing.T) {
	t.Parallel()

	got := quoteIdentifier("")
	if got != `""` {
		t.Fatalf("expected empty quoted identifier, got %q", got)
	}
}

// TestInferQualifiedObjectMissingName: Given a prefix-shaped row where the
// NAME field is absent, when inferQualifiedObject runs, then it should reject
// the row.
func TestInferQualifiedObjectMissingName(t *testing.T) {
	t.Parallel()

	_, ok := inferQualifiedObject("TASK", "TASK", Row{
		"TASK_CATALOG": "DB",
		"TASK_SCHEMA":  "SCH",
		// TASK_NAME deliberately absent.
	})
	if ok {
		t.Fatal("expected inferQualifiedObject to fail with missing NAME")
	}
}

// TestInferElementTypesStatementMissingDataType: Given an ELEMENT_TYPES row
// missing DATA_TYPE, when inference runs, then the row should be rejected.
func TestInferElementTypesStatementMissingDataType(t *testing.T) {
	t.Parallel()

	_, ok := InferDDLRequest("DEMO_DB", "ELEMENT_TYPES", Row{
		"OBJECT_TYPE":    "TABLE",
		"OBJECT_CATALOG": "DEMO_DB",
		"OBJECT_SCHEMA":  "PUBLIC",
		"OBJECT_NAME":    "MY_TABLE",
		// DATA_TYPE deliberately absent.
	})
	if ok {
		t.Fatal("expected inference to fail when DATA_TYPE is missing")
	}
}

// TestInferElementTypesStatementNoCollectionIdentifier: Given an ELEMENT_TYPES
// row without COLLECTION_TYPE_IDENTIFIER, when inference runs, then the simple
// form (without collection qualifier) should be emitted.
func TestInferElementTypesStatementNoCollectionIdentifier(t *testing.T) {
	t.Parallel()

	request, ok := InferDDLRequest("DEMO_DB", "ELEMENT_TYPES", Row{
		"OBJECT_TYPE":    "TABLE",
		"OBJECT_CATALOG": "DEMO_DB",
		"OBJECT_SCHEMA":  "PUBLIC",
		"OBJECT_NAME":    "MY_TABLE",
		"DATA_TYPE":      "TEXT",
		// COLLECTION_TYPE_IDENTIFIER deliberately absent.
	})
	if !ok {
		t.Fatal("expected request to be inferred")
	}
	expected := `-- Element type 'TEXT' on TABLE "DEMO_DB"."PUBLIC"."MY_TABLE"`
	if request.InlineSQL != expected {
		t.Fatalf("unexpected inline SQL %q", request.InlineSQL)
	}
}

// TestInferDDLRequestForTask: Given a TASKS row with complete TASK_CATALOG /
// TASK_SCHEMA / TASK_NAME fields, when inference runs via the prefix loop,
// then a TASK DDL request should be produced (covers inferQualifiedObject
// success path).
func TestInferDDLRequestForTask(t *testing.T) {
	t.Parallel()

	request, ok := InferDDLRequest("DEMO_DB", "TASKS", Row{
		"TASK_CATALOG": "DEMO_DB",
		"TASK_SCHEMA":  "PUBLIC",
		"TASK_NAME":    "MY_TASK",
	})
	if !ok {
		t.Fatal("expected request to be inferred for TASK row")
	}
	if request.ObjectType != "TASK" {
		t.Fatalf("expected TASK, got %s", request.ObjectType)
	}
	if request.QualifiedName != `"DEMO_DB"."PUBLIC"."MY_TASK"` {
		t.Fatalf("unexpected qualified name %q", request.QualifiedName)
	}
}

// TestInferDDLRequestForStage: Given a STAGES row with complete fields, when
// inference runs, then a STAGE DDL request should be produced.
func TestInferDDLRequestForStage(t *testing.T) {
	t.Parallel()

	request, ok := InferDDLRequest("DEMO_DB", "STAGES", Row{
		"STAGE_CATALOG": "DEMO_DB",
		"STAGE_SCHEMA":  "PUBLIC",
		"STAGE_NAME":    "MY_STAGE",
	})
	if !ok {
		t.Fatal("expected request to be inferred for STAGE row")
	}
	if request.ObjectType != "STAGE" {
		t.Fatalf("expected STAGE, got %s", request.ObjectType)
	}
}

// TestQuoteQualifiedNameSkipsEmptyParts: Given a mix of non-empty and empty
// parts, when quoteQualifiedName runs, then empty parts should be silently
// skipped without producing consecutive dots.
func TestQuoteQualifiedNameSkipsEmptyParts(t *testing.T) {
	t.Parallel()

	got := quoteQualifiedName("DB", "", "TABLE")
	want := `"DB"."TABLE"`
	if got != want {
		t.Fatalf("quoteQualifiedName skipping empty: got %q, want %q", got, want)
	}
}

// TestGetStringForNilValue: Given a row that explicitly maps a key to nil,
// when getString runs, then it should return ("", false) for the nil value.
func TestGetStringForNilValue(t *testing.T) {
	t.Parallel()

	row := Row{"KEY": nil}
	_, ok := getString(row, "KEY")
	if ok {
		t.Fatal("expected getString to return false for nil value")
	}
}

// TestInferPackageStatementDefaultRuntime: Given a PACKAGES row without
// RUNTIME_VERSION, when inference runs, then the missing runtime should
// default to "default" in the emitted comment.
func TestInferPackageStatementDefaultRuntime(t *testing.T) {
	t.Parallel()

	request, ok := InferDDLRequest("", "PACKAGES", Row{
		"PACKAGE_NAME": "mylib",
		"VERSION":      "2.0",
		"LANGUAGE":     "python",
		// RUNTIME_VERSION deliberately absent → should default to "default".
	})
	if !ok {
		t.Fatal("expected request to be inferred")
	}
	if !strings.Contains(request.InlineSQL, "runtime 'default'") {
		t.Fatalf("expected default runtime in output, got %q", request.InlineSQL)
	}
}

// TestInferReplicationGroupMissingName: Given a REPLICATION_GROUPS row with no
// NAME field, when inference runs, then the row should be rejected.
func TestInferReplicationGroupMissingName(t *testing.T) {
	t.Parallel()

	_, ok := InferDDLRequest("", "REPLICATION_GROUPS", Row{
		"TYPE": "REPLICATION",
	})
	if ok {
		t.Fatal("expected inference to fail when NAME is missing")
	}
}

// TestInferDDLRequestForViewsViewName: Given a row with TABLE_CATALOG /
// TABLE_SCHEMA / TABLE_NAME and viewName "VIEWS", when inference runs, then
// ObjectType should be "VIEW" (covers the EqualFold("VIEWS") branch).
func TestInferDDLRequestForViewsViewName(t *testing.T) {
	t.Parallel()

	request, ok := InferDDLRequest("DEMO_DB", "VIEWS", Row{
		"TABLE_CATALOG": "DEMO_DB",
		"TABLE_SCHEMA":  "PUBLIC",
		"TABLE_NAME":    "V_CUSTOMERS",
	})
	if !ok {
		t.Fatal("expected request to be inferred")
	}
	if request.ObjectType != "VIEW" {
		t.Fatalf("expected VIEW, got %s", request.ObjectType)
	}
}

// TestInferDDLRequestForTableTypeView: Given a TABLES row where TABLE_TYPE
// contains "VIEW", when inference runs, then ObjectType should be "VIEW".
func TestInferDDLRequestForTableTypeView(t *testing.T) {
	t.Parallel()

	request, ok := InferDDLRequest("DEMO_DB", "TABLES", Row{
		"TABLE_CATALOG": "DEMO_DB",
		"TABLE_SCHEMA":  "PUBLIC",
		"TABLE_NAME":    "V_SALES",
		"TABLE_TYPE":    "VIEW",
	})
	if !ok {
		t.Fatal("expected request to be inferred")
	}
	if request.ObjectType != "VIEW" {
		t.Fatalf("expected VIEW for TABLE_TYPE=VIEW, got %s", request.ObjectType)
	}
}
