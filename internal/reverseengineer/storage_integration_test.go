package reverseengineer

import (
	"strings"
	"testing"
)

// TestBuildStorageIntegrationDDL_S3: Given S3 DESC rows, when DDL building
// runs, then expected fields should be emitted and read-only ones omitted.
func TestBuildStorageIntegrationDDL_S3(t *testing.T) {
	t.Parallel()

	rows := []Row{
		{"PROPERTY": "ENABLED", "PROPERTY_VALUE": "true"},
		{"PROPERTY": "STORAGE_PROVIDER", "PROPERTY_VALUE": "S3"},
		{"PROPERTY": "STORAGE_AWS_ROLE_ARN", "PROPERTY_VALUE": "arn:aws:iam::123456789012:role/my-snowflake-role"},
		{"PROPERTY": "STORAGE_ALLOWED_LOCATIONS", "PROPERTY_VALUE": "s3://mybucket/mypath/,s3://other/data/"},
		{"PROPERTY": "STORAGE_BLOCKED_LOCATIONS", "PROPERTY_VALUE": "s3://mybucket/blocked/"},
		{"PROPERTY": "COMMENT", "PROPERTY_VALUE": "S3 integration"},
		// Read-only properties that must NOT appear in the output.
		{"PROPERTY": "STORAGE_AWS_IAM_USER_ARN", "PROPERTY_VALUE": "arn:aws:iam::000000000000:user/snowflake-user"},
		{"PROPERTY": "STORAGE_AWS_EXTERNAL_ID", "PROPERTY_VALUE": "MYACCOUNT_SFCRole=2_abc"},
	}

	ddl, ok := BuildStorageIntegrationDDL("MY_S3_INTEGRATION", rows)
	if !ok {
		t.Fatal("expected DDL to be built successfully")
	}

	for _, want := range []string{
		"CREATE STORAGE INTEGRATION IF NOT EXISTS",
		`"MY_S3_INTEGRATION"`,
		"TYPE = EXTERNAL_STAGE",
		"STORAGE_PROVIDER = 'S3'",
		"ENABLED = TRUE",
		"STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/my-snowflake-role'",
		"STORAGE_ALLOWED_LOCATIONS = ('s3://mybucket/mypath/', 's3://other/data/')",
		"STORAGE_BLOCKED_LOCATIONS = ('s3://mybucket/blocked/')",
		"COMMENT = 'S3 integration'",
	} {
		if !strings.Contains(ddl, want) {
			t.Errorf("expected DDL to contain %q, got:\n%s", want, ddl)
		}
	}

	for _, forbidden := range []string{
		"STORAGE_AWS_IAM_USER_ARN",
		"STORAGE_AWS_EXTERNAL_ID",
	} {
		if strings.Contains(ddl, forbidden) {
			t.Errorf("expected DDL to NOT contain read-only property %q, got:\n%s", forbidden, ddl)
		}
	}
}

// TestBuildStorageIntegrationDDL_S3_Disabled: Given ENABLED=false, when DDL
// building runs, then the statement should preserve the disabled state.
func TestBuildStorageIntegrationDDL_S3_Disabled(t *testing.T) {
	t.Parallel()

	rows := []Row{
		{"PROPERTY": "ENABLED", "PROPERTY_VALUE": "false"},
		{"PROPERTY": "STORAGE_PROVIDER", "PROPERTY_VALUE": "S3"},
		{"PROPERTY": "STORAGE_AWS_ROLE_ARN", "PROPERTY_VALUE": "arn:aws:iam::123456789012:role/my-role"},
		{"PROPERTY": "STORAGE_ALLOWED_LOCATIONS", "PROPERTY_VALUE": "s3://bucket/"},
	}

	ddl, ok := BuildStorageIntegrationDDL("DISABLED_INTEGRATION", rows)
	if !ok {
		t.Fatal("expected DDL to be built successfully")
	}
	if !strings.Contains(ddl, "ENABLED = FALSE") {
		t.Errorf("expected ENABLED = FALSE in DDL, got:\n%s", ddl)
	}
}

// TestBuildStorageIntegrationDDL_Azure: Given Azure DESC rows, when DDL
// building runs, then Azure fields should be emitted and generated ones omitted.
func TestBuildStorageIntegrationDDL_Azure(t *testing.T) {
	t.Parallel()

	rows := []Row{
		{"PROPERTY": "ENABLED", "PROPERTY_VALUE": "true"},
		{"PROPERTY": "STORAGE_PROVIDER", "PROPERTY_VALUE": "AZURE"},
		{"PROPERTY": "AZURE_TENANT_ID", "PROPERTY_VALUE": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"},
		{"PROPERTY": "STORAGE_ALLOWED_LOCATIONS", "PROPERTY_VALUE": "azure://myaccount.blob.core.windows.net/mycontainer/path/"},
		// Read-only properties that must NOT appear.
		{"PROPERTY": "AZURE_CONSENT_URL", "PROPERTY_VALUE": "https://login.microsoftonline.com/..."},
		{"PROPERTY": "AZURE_MULTI_TENANT_APP_NAME", "PROPERTY_VALUE": "snowflake_abc123"},
	}

	ddl, ok := BuildStorageIntegrationDDL("MY_AZURE_INTEGRATION", rows)
	if !ok {
		t.Fatal("expected DDL to be built successfully")
	}

	for _, want := range []string{
		"STORAGE_PROVIDER = 'AZURE'",
		"AZURE_TENANT_ID = 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'",
		"azure://myaccount.blob.core.windows.net/mycontainer/path/",
	} {
		if !strings.Contains(ddl, want) {
			t.Errorf("expected DDL to contain %q, got:\n%s", want, ddl)
		}
	}

	for _, forbidden := range []string{"AZURE_CONSENT_URL", "AZURE_MULTI_TENANT_APP_NAME"} {
		if strings.Contains(ddl, forbidden) {
			t.Errorf("expected DDL to NOT contain read-only property %q, got:\n%s", forbidden, ddl)
		}
	}
}

// TestBuildStorageIntegrationDDL_GCS: Given GCS DESC rows, when DDL building
// runs, then provider and locations should appear without read-only metadata.
func TestBuildStorageIntegrationDDL_GCS(t *testing.T) {
	t.Parallel()

	rows := []Row{
		{"PROPERTY": "ENABLED", "PROPERTY_VALUE": "true"},
		{"PROPERTY": "STORAGE_PROVIDER", "PROPERTY_VALUE": "GCS"},
		{"PROPERTY": "STORAGE_ALLOWED_LOCATIONS", "PROPERTY_VALUE": "gcs://mybucket/mypath/"},
		// Read-only property that must NOT appear.
		{"PROPERTY": "STORAGE_GCP_SERVICE_ACCOUNT", "PROPERTY_VALUE": "snowflake-sa@project.iam.gserviceaccount.com"},
	}

	ddl, ok := BuildStorageIntegrationDDL("MY_GCS_INTEGRATION", rows)
	if !ok {
		t.Fatal("expected DDL to be built successfully")
	}

	if !strings.Contains(ddl, "STORAGE_PROVIDER = 'GCS'") {
		t.Errorf("expected GCS provider in DDL, got:\n%s", ddl)
	}
	if !strings.Contains(ddl, "STORAGE_ALLOWED_LOCATIONS = ('gcs://mybucket/mypath/')") {
		t.Errorf("expected allowed locations in DDL, got:\n%s", ddl)
	}
	if strings.Contains(ddl, "STORAGE_GCP_SERVICE_ACCOUNT") {
		t.Errorf("expected DDL to NOT contain read-only property STORAGE_GCP_SERVICE_ACCOUNT, got:\n%s", ddl)
	}
}

// TestBuildStorageIntegrationDDL_DefaultsAllowedLocationsToStar: Given no
// allowed locations, when DDL building runs, then '*' should be used.
func TestBuildStorageIntegrationDDL_DefaultsAllowedLocationsToStar(t *testing.T) {
	t.Parallel()

	rows := []Row{
		{"PROPERTY": "ENABLED", "PROPERTY_VALUE": "true"},
		{"PROPERTY": "STORAGE_PROVIDER", "PROPERTY_VALUE": "S3"},
		{"PROPERTY": "STORAGE_AWS_ROLE_ARN", "PROPERTY_VALUE": "arn:aws:iam::123:role/r"},
		// No STORAGE_ALLOWED_LOCATIONS row.
	}

	ddl, ok := BuildStorageIntegrationDDL("WIDE_OPEN", rows)
	if !ok {
		t.Fatal("expected DDL to be built successfully")
	}
	if !strings.Contains(ddl, "STORAGE_ALLOWED_LOCATIONS = ('*')") {
		t.Errorf("expected wildcard allowed locations, got:\n%s", ddl)
	}
}

// TestBuildStorageIntegrationDDL_EmptyRows: Given no DESC rows, when DDL
// building runs, then it should fail deterministically.
func TestBuildStorageIntegrationDDL_EmptyRows(t *testing.T) {
	t.Parallel()

	_, ok := BuildStorageIntegrationDDL("EMPTY", []Row{})
	if ok {
		t.Fatal("expected failure for empty DESC rows")
	}
}

// TestBuildStorageIntegrationDDL_MissingProvider: Given rows without provider,
// when DDL building runs, then it should fail deterministically.
func TestBuildStorageIntegrationDDL_MissingProvider(t *testing.T) {
	t.Parallel()

	rows := []Row{
		{"PROPERTY": "ENABLED", "PROPERTY_VALUE": "true"},
		// No STORAGE_PROVIDER row.
	}

	_, ok := BuildStorageIntegrationDDL("NO_PROVIDER", rows)
	if ok {
		t.Fatal("expected failure when STORAGE_PROVIDER is absent")
	}
}

// TestStorageLocationsToTuple: Given comma-separated locations, when tuple
// conversion runs, then output should be normalized and safely quoted.
func TestStorageLocationsToTuple(t *testing.T) {
	t.Parallel()

	cases := []struct {
		input string
		want  string
	}{
		{"s3://a/", "'s3://a/'"},
		{"s3://a/,s3://b/", "'s3://a/', 's3://b/'"},
		{"s3://a/ , s3://b/ ", "'s3://a/', 's3://b/'"},
		{"*", "'*'"},
	}

	for _, c := range cases {
		got := storageLocationsToTuple(c.input)
		if got != c.want {
			t.Errorf("storageLocationsToTuple(%q) = %q, want %q", c.input, got, c.want)
		}
	}
}

// TestBuildStorageIntegrationDDL_S3GOV: Given S3GOV DESC rows, when DDL
// building runs, then STORAGE_PROVIDER should be 'S3GOV' and ARN emitted.
func TestBuildStorageIntegrationDDL_S3GOV(t *testing.T) {
	t.Parallel()

	rows := []Row{
		{"PROPERTY": "STORAGE_PROVIDER", "PROPERTY_VALUE": "S3GOV"},
		{"PROPERTY": "ENABLED", "PROPERTY_VALUE": "true"},
		{"PROPERTY": "STORAGE_AWS_ROLE_ARN", "PROPERTY_VALUE": "arn:aws-us-gov:iam::123:role/my-role"},
		{"PROPERTY": "STORAGE_ALLOWED_LOCATIONS", "PROPERTY_VALUE": "s3://gov-bucket/"},
	}

	ddl, ok := BuildStorageIntegrationDDL("GOV_INTEGRATION", rows)
	if !ok {
		t.Fatal("expected DDL to be built successfully for S3GOV")
	}

	for _, want := range []string{
		"STORAGE_PROVIDER = 'S3GOV'",
		"STORAGE_AWS_ROLE_ARN = 'arn:aws-us-gov:iam::123:role/my-role'",
		"STORAGE_ALLOWED_LOCATIONS = ('s3://gov-bucket/')",
	} {
		if !strings.Contains(ddl, want) {
			t.Errorf("expected S3GOV DDL to contain %q, got:\n%s", want, ddl)
		}
	}
}

// TestBuildStorageIntegrationDDL_S3CHINA: Given S3CHINA DESC rows, when DDL
// building runs, then STORAGE_PROVIDER should be 'S3CHINA'.
func TestBuildStorageIntegrationDDL_S3CHINA(t *testing.T) {
	t.Parallel()

	rows := []Row{
		{"PROPERTY": "STORAGE_PROVIDER", "PROPERTY_VALUE": "S3CHINA"},
		{"PROPERTY": "ENABLED", "PROPERTY_VALUE": "true"},
		{"PROPERTY": "STORAGE_AWS_ROLE_ARN", "PROPERTY_VALUE": "arn:aws-cn:iam::456:role/china-role"},
		{"PROPERTY": "STORAGE_ALLOWED_LOCATIONS", "PROPERTY_VALUE": "s3://cn-bucket/"},
	}

	ddl, ok := BuildStorageIntegrationDDL("CHINA_INTEGRATION", rows)
	if !ok {
		t.Fatal("expected DDL to be built successfully for S3CHINA")
	}

	if !strings.Contains(ddl, "STORAGE_PROVIDER = 'S3CHINA'") {
		t.Errorf("expected STORAGE_PROVIDER = 'S3CHINA' in DDL, got:\n%s", ddl)
	}
}

// TestStorageLocationsToTupleWhitespaceOnly: Given a comma-separated string
// where every part is blank, when storageLocationsToTuple runs, then blanks
// should be silently skipped and an empty string returned.
func TestStorageLocationsToTupleWhitespaceOnly(t *testing.T) {
	t.Parallel()

	got := storageLocationsToTuple("  ,  ,  ")
	if got != "" {
		t.Fatalf("expected empty string for all-whitespace input, got %q", got)
	}
}

// TestDescRowsToMapSkipsMissingPropertyKey: Given a row that has no PROPERTY
// key, when descRowsToMap runs, then that row should be silently skipped and
// not cause a panic or corrupt the result map.
func TestDescRowsToMapSkipsMissingPropertyKey(t *testing.T) {
	t.Parallel()

	rows := []Row{
		{"PROPERTY_VALUE": "orphan"}, // no PROPERTY key — must be skipped
		{"PROPERTY": "ENABLED", "PROPERTY_VALUE": "true"},
	}

	props := descRowsToMap(rows)
	if _, found := props[""]; found {
		t.Fatal("expected orphan row to be skipped, but empty key was added")
	}
	if props["ENABLED"] != "true" {
		t.Fatalf("expected ENABLED=true, got %q", props["ENABLED"])
	}
}
