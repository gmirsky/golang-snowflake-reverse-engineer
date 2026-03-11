package reverseengineer

import "testing"

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
