package snowflake

import (
	"testing"

	"github.com/gmirsky/golang-snowflake-reverse-engineer/internal/reverseengineer"
)

// TestIsStorageIntegrationRow_UsesCategoryStorage: Given CATEGORY=STORAGE,
// when row classification runs, then it should be treated as storage.
func TestIsStorageIntegrationRow_UsesCategoryStorage(t *testing.T) {
	t.Parallel()

	row := reverseengineer.Row{
		"NAME":     "INTG_CC_ALWAYS_ON_LANDING",
		"TYPE":     "EXTERNAL_STAGE",
		"CATEGORY": "STORAGE",
	}

	if !isStorageIntegrationRow(row) {
		t.Fatal("expected CATEGORY=STORAGE row to be treated as a storage integration")
	}
}

// TestIsStorageIntegrationRow_FallsBackToTypeStorage: Given legacy TYPE data,
// when row classification runs, then TYPE=STORAGE should still be accepted.
func TestIsStorageIntegrationRow_FallsBackToTypeStorage(t *testing.T) {
	t.Parallel()

	row := reverseengineer.Row{
		"NAME": "LEGACY_STORAGE_INTEGRATION",
		"TYPE": "STORAGE",
	}

	if !isStorageIntegrationRow(row) {
		t.Fatal("expected TYPE=STORAGE row to be treated as a storage integration when CATEGORY is absent")
	}
}

// TestIsStorageIntegrationRow_RejectsNonStorageIntegration: Given non-storage
// integration data, when row classification runs, then it should be rejected.
func TestIsStorageIntegrationRow_RejectsNonStorageIntegration(t *testing.T) {
	t.Parallel()

	row := reverseengineer.Row{
		"NAME":     "API_INTEGRATION",
		"TYPE":     "API_AUTHENTICATION",
		"CATEGORY": "API",
	}

	if isStorageIntegrationRow(row) {
		t.Fatal("expected non-storage integration row to be rejected")
	}
}
