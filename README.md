# golang-snowflake-reverse-engineer

`golang-snowflake-reverse-engineer` is a command-line tool that connects to Snowflake with key-pair authentication, walks every view in the target database's `INFORMATION_SCHEMA` schema, and writes one `.sql` file per view.

For each row in a view, the tool attempts to derive a Snowflake object identity and fetch its DDL with `GET_DDL`. If the row shape does not map cleanly to a supported object, the tool writes a deterministic SQL comment containing the row metadata instead. If a view has no rows, the tool writes:

```sql
/* No data found in the view <view name> */
```

## Requirements

- Go `1.26+` for native builds
- A Snowflake user configured for key-pair authentication
- An RSA private key file in a format supported by the Go SSH parser
- Access to the target database and its `INFORMATION_SCHEMA`

## CLI usage

```bash
go run ./cmd/snowflake-reverse-engineer \
  --user demo_user \
  --account demo_account \
  --warehouse demo_wh \
  --database demo_db \
  --output-dir ./output \
  --log-dir ./logs \
  --private-key ./keys/rsa_key.p8 \
  --max-connections 3 \
  --timestamped-output \
  --verbose
```

### Flags

- `--user`: Snowflake user name
- `--account`: Snowflake account identifier
- `--warehouse`: Snowflake warehouse name
- `--database`: Snowflake database name
- `--output-dir`: Directory path for generated SQL files
- `--log-dir`: Directory path for the log file
- `--private-key`: Directory path and file name of the private key file
- `--max-connections`: Optional. Default `3`, minimum `1`, maximum `9`
- `--passphrase`: Optional. Private key passphrase. Defaults to empty
- `--compact-packages`: Optional. Groups `INFORMATION_SCHEMA.PACKAGES` rows by package name, version, and language, and emits one line with a runtime list per group
- `--compact-packages-max-runtimes`: Optional. Caps runtimes shown per compact package group. Default `0` (unlimited)
- `--compact-packages-omit-truncation-count`: Optional and enabled by default. Omits the `(truncated, N more)` suffix when runtime capping is active
- `--timestamped-output`: Optional. Appends the run timestamp to output and log file names
- `--verbose`: Optional. Enables extra runtime logging

## Output behavior

- One file is generated for each view in `<database>.INFORMATION_SCHEMA`
- Processing is concurrent, limited by `--max-connections`
- `PACKAGES` output can be compacted with `--compact-packages` to reduce very large files
- Use `--compact-packages-max-runtimes` with `--compact-packages` to keep each package line shorter when many runtimes exist
- `(truncated, N more)` suffixes are omitted by default to minimize bytes when runtime capping is enabled
- `storage_integrations.sql` is always written. It contains a `CREATE STORAGE INTEGRATION IF NOT EXISTS` statement for every storage-type integration found via `SHOW INTEGRATIONS` + `DESC STORAGE INTEGRATION`. If no storage integrations exist the file contains a `/* No data found */` comment. Read-only Snowflake-managed properties (`STORAGE_AWS_IAM_USER_ARN`, `STORAGE_AWS_EXTERNAL_ID`, `AZURE_CONSENT_URL`, `AZURE_MULTI_TENANT_APP_NAME`, `STORAGE_GCP_SERVICE_ACCOUNT`) are excluded from the generated DDL.
- The log file records:
  - all input parameters with the passphrase redacted
  - the row count for each processed view
  - the number of SQL statements generated per view
  - a run summary

## Generating a key pair

Snowflake key-pair authentication requires a PKCS#8 RSA private key and the corresponding public key registered on the Snowflake user.

### Generate an unencrypted private key

```bash
mkdir -p keys
openssl genrsa 4096 | openssl pkcs8 -topk8 -inform PEM -out keys/rsa_key.p8 -nocrypt
chmod 600 keys/rsa_key.p8
```

### Generate a passphrase-protected private key

```bash
mkdir -p keys
openssl genrsa 4096 | openssl pkcs8 -topk8 -inform PEM -out keys/rsa_key.p8
chmod 600 keys/rsa_key.p8
```

OpenSSL will prompt for a passphrase. Pass the same value to the tool via `--passphrase` at runtime.

### Derive the public key

```bash
openssl rsa -in keys/rsa_key.p8 -pubout -out keys/rsa_key.pub
```

### Register the public key with Snowflake

Strip the PEM header/footer lines, then set the key on your Snowflake user:

```sql
ALTER USER demo_user SET RSA_PUBLIC_KEY='<contents of keys/rsa_key.pub minus header and footer lines>';
```

You can extract just the key body with:

```bash
grep -v "^-----" keys/rsa_key.pub | tr -d '\n'
```

Paste the output as the value inside the single quotes in the `ALTER USER` statement.

## Native development

```bash
go mod tidy
go test ./...
govulncheck ./...
go build ./cmd/snowflake-reverse-engineer
```

If `govulncheck` is not installed locally, install it with:

```bash
go install golang.org/x/vuln/cmd/govulncheck@latest
```

## Taskfile usage

This repository includes a `Taskfile.yml` for common development workflows.

Build the project binary:

```bash
task build
```

Run tests:

```bash
task test
```

Run the CLI locally with arguments:

```bash
task run -- --user demo_user --account demo_account --warehouse demo_wh --database demo_db --output-dir ./output --log-dir ./logs --private-key ./keys/rsa_key.p8
```

Other available tasks:

```bash
task tidy
task comment-check
task vuln
task docker-build
task podman-build
task clean
```

## Container build

Build for the current platform:

```bash
docker build -t snowflake-reverse-engineer:local .
```

Build for multiple platforms with `buildx`:

```bash
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t snowflake-reverse-engineer:multi \
  .
```

Load a single target platform into the local Docker image store:

```bash
docker buildx build \
  --platform linux/amd64 \
  --load \
  -t snowflake-reverse-engineer:amd64 \
  .
```

### Podman build

Build using the repository `Containerfile` and `.containerignore`:

```bash
podman build \
  -f Containerfile \
  --ignorefile .containerignore \
  -t snowflake-reverse-engineer:podman \
  .
```

Build for a specific architecture:

```bash
podman build \
  -f Containerfile \
  --ignorefile .containerignore \
  --arch amd64 \
  -t snowflake-reverse-engineer:podman-amd64 \
  .
```

## Container run

Mount directories for the private key, logs, and output files:

```bash
docker run --rm \
  -v "$PWD/keys:/keys:ro" \
  -v "$PWD/output:/output" \
  -v "$PWD/logs:/logs" \
  snowflake-reverse-engineer:local \
  --user demo_user \
  --account demo_account \
  --warehouse demo_wh \
  --database demo_db \
  --output-dir /output \
  --log-dir /logs \
  --private-key /keys/rsa_key.p8 \
  --max-connections 3
```

If the private key is encrypted, add `--passphrase` to the container command.

### Podman run

Run with the same mounts and arguments using Podman:

```bash
podman run --rm \
  -v "$PWD/keys:/keys:ro" \
  -v "$PWD/output:/output" \
  -v "$PWD/logs:/logs" \
  snowflake-reverse-engineer:podman \
  --user demo_user \
  --account demo_account \
  --warehouse demo_wh \
  --database demo_db \
  --output-dir /output \
  --log-dir /logs \
  --private-key /keys/rsa_key.p8 \
  --max-connections 3
```

## Notes on DDL generation

- The tool prefers `GET_DDL` for rows that clearly identify Snowflake objects such as tables, views, sequences, procedures, functions, tasks, stages, pipes, streams, and file formats.
- Some `INFORMATION_SCHEMA` views describe metadata rather than first-class objects. In those cases, the tool writes a SQL comment with the row payload so every row still produces deterministic output.
