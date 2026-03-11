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
