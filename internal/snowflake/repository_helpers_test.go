package snowflake

import (
	"testing"
	"time"
)

// TestQualifiedView: Given database/view values, when qualification runs,
// then the INFORMATION_SCHEMA path should be fully quoted.
func TestQualifiedView(t *testing.T) {
	t.Parallel()

	got := qualifiedView("DEMO_DB", "VIEWS")
	want := `"DEMO_DB"."INFORMATION_SCHEMA"."VIEWS"`
	if got != want {
		t.Fatalf("qualifiedView() = %q, want %q", got, want)
	}
}

// TestQuoteQualifiedNameEscapesAndTrims: Given mixed identifier parts, when
// quoting runs, then spaces are trimmed and internal quotes are escaped.
func TestQuoteQualifiedNameEscapesAndTrims(t *testing.T) {
	t.Parallel()

	got := quoteQualifiedName(` DEMO_DB `, `MY"SCHEMA`, `MY TABLE`)
	want := `"DEMO_DB"."MY""SCHEMA"."MY TABLE"`
	if got != want {
		t.Fatalf("quoteQualifiedName() = %q, want %q", got, want)
	}
}

// TestNormalizeValue: Given driver values, when normalization runs, then
// []byte and time.Time are converted to stable string forms.
func TestNormalizeValue(t *testing.T) {
	t.Parallel()

	when := time.Date(2026, time.March, 12, 18, 0, 1, 234, time.FixedZone("UTC-5", -5*60*60))
	tests := []struct {
		name  string
		input any
		want  any
	}{
		{name: "nil", input: nil, want: nil},
		{name: "bytes", input: []byte("abc"), want: "abc"},
		{name: "time", input: when, want: when.UTC().Format(time.RFC3339Nano)},
		{name: "passthrough", input: 42, want: 42},
	}

	for _, testCase := range tests {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			got := normalizeValue(testCase.input)
			if got != testCase.want {
				t.Fatalf("normalizeValue(%v) = %v, want %v", testCase.input, got, testCase.want)
			}
		})
	}
}
