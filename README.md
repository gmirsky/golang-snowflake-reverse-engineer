# golang-snowflake-reverse-engineer

`golang-snowflake-reverse-engineer` is a command-line tool that connects to Snowflake with key-pair authentication, walks every view in the target database's `INFORMATION_SCHEMA` schema, and writes one `.sql` file per view.

For each row in a view, the tool attempts to derive a Snowflake object identity and fetch its DDL with `GET_DDL`. If the row shape does not map cleanly to a supported object, the tool writes a deterministic SQL comment containing the row metadata instead. If a view has no rows, the tool writes:

```sql
/* No data found in the view <view name> */
```

## Guide map

- Setup and prerequisites
  - Requirements, toolchain, platform notes
- Running the tool
  - CLI usage, flags, and output behavior
- Security scanner configuration
  - GitGuardian and Cycode ignore files
- Credentials and keys
  - Snowflake key generation and registration
- Development workflow
  - Local development, coverage, Taskfile workflows, script maintenance
- Container usage
  - Docker/Podman build and run examples
- Reference
  - DDL generation notes and dependency diagrams

## Setup and prerequisites

### Requirements

- Go `1.26+` for native builds
- A Snowflake user configured for key-pair authentication
- An RSA private key file in a format supported by the Go SSH parser
- Access to the target database and its `INFORMATION_SCHEMA`

### Prerequisite tools

Install these tools before running build and test workflows.

#### Required for core build and tests

- Go (`1.26+`)
  - Used by `go build`, `go test`, integration tests, and dependency graph generation.
  - Quick check: `go version`
- Bash (`4+` recommended)
  - All maintenance scripts in `scripts/` are Bash scripts.
  - Quick check: `bash --version`
- bats-core
  - Required for script test suites in `scripts/*.bats` and `task test-bats`.
  - Quick check: `bats --version`
- Task (go-task)
  - Required if you use `task ...` workflows from `Taskfile.yml`.
  - You can run equivalent raw commands manually if Task is not installed.
  - Quick check: `task --version`
- OpenSSL
  - Required to generate and inspect RSA key-pair files for Snowflake auth.
  - Quick check: `openssl version`
- curl
  - Required by `scripts/update_github_actions.sh` for GitHub API calls.
  - Quick check: `curl --version`
- Perl
  - Required by in-place replacement logic in `scripts/rename_module_path.sh` and `scripts/update_chainguard_images.sh`.
  - Quick check: `perl -v`

#### Required for specific workflows

- Docker with Buildx
  - Required for `task docker-build` and `scripts/update_chainguard_images.sh`.
  - Quick checks: `docker --version` and `docker buildx version`
- Google Container Structure Test (`container-structure-test`)
  - Required for `task docker-structure-test`.
  - Quick check: `container-structure-test version`
- Podman
  - Required for `task podman-build` and Podman container workflows.
  - Quick check: `podman --version`
- govulncheck
  - Used for vulnerability scanning.
  - `task vuln` installs it automatically if missing.
- markdownlint
  - Required for `task markdown-check` Markdown style validation.
  - Quick check: `markdownlint --version`
- lychee
  - Required for `task markdown-check` link validation.
  - Quick check: `lychee --version`
- write-good
  - Required for `task markdown-check` prose quality checks.
  - Quick check: `write-good --version`
- cloc
  - Required for `task cloc` line-of-code reports.
  - `task cloc-check` verifies availability and prints install hints when missing.
  - Quick check: `cloc --version`

#### Platform notes

- macOS (Homebrew)
  - Typical install set: `brew install go task bats-core openssl curl perl`
  - Docker and Podman are installed separately via their apps/package casks.
- Ubuntu/Debian
  - Typical install set: `sudo apt-get install golang-go bats curl perl openssl`
  - Install Task from go-task docs or via package manager if available on your distro version.
- Windows
  - Recommended: use WSL2 Ubuntu for full Bash + bats compatibility.
  - Native PowerShell/CMD execution is not the primary script target for this repository.

#### Tips and hints

- Prefer Task targets for consistency: `task test`, `task test-bats`, `task test-integration`.
- Integration tests need live Snowflake credentials (`SNOWFLAKE_*` env vars).
- Set `GITHUB_TOKEN` before running actions checks/updates to reduce GitHub API rate-limit failures.
- `rg` (ripgrep) is optional but recommended; scripts automatically use it when available for faster file discovery.
- If `bats` is missing, run `task test` first for Go/unit checks, then add bats-core and run `task test-bats`.

#### Quick verification

Run this once from your shell to verify required tools are available:

```bash
for cmd in go bash bats task openssl curl perl; do
  if command -v "$cmd" >/dev/null 2>&1; then
    printf 'ok: %s -> %s\n' "$cmd" "$(command -v "$cmd")"
  else
    printf 'missing: %s\n' "$cmd"
  fi
done
```

Check optional workflow tools:

```bash
for cmd in docker podman rg; do
  if command -v "$cmd" >/dev/null 2>&1; then
    printf 'optional ok: %s -> %s\n' "$cmd" "$(command -v "$cmd")"
  else
    printf 'optional missing: %s\n' "$cmd"
  fi
done

if command -v docker >/dev/null 2>&1; then
  docker buildx version || echo 'docker buildx plugin is missing'
fi
```

## Running the tool

### CLI usage

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

### Output behavior

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

## Security scanner configuration

### Security scanner ignore files

This repository uses scanner-specific ignore files to suppress known test-only dummy secrets.

#### GitGuardian (`ggshield`)

- File: `.gitguardian.yaml`
- Scope: local repo scan behavior for GitGuardian CLI (`ggshield`)
- Current ignore:
  - `secret.ignored_paths` includes `internal/config/config_test.go`

Current file content:

```yaml
version: 2
secret:
  ignored_paths:
    - internal/config/config_test.go
```

#### Cycode CLI

- File: `.cycode/config.yaml`
- Scope: local repo scan behavior for Cycode CLI
- Current ignore:
  - `exclusions.secret.paths` includes `internal/config/config_test.go`

Current file content:

```yaml
exclusions:
  secret:
    paths:
      - internal/config/config_test.go
```

#### Why this exists

`internal/config/config_test.go` contains a dummy passphrase test value (`"s3cr3t"`) used in unit tests. These ignore entries prevent local secret scanners from flagging that known non-production test fixture.

#### Maintenance guidance

- Keep ignores as narrow as possible (prefer file/path-specific rules over global value ignores).
- If the dummy test data moves to another file, update both ignore files in the same commit.
- Do not add real credentials to tests or source files; these ignores are only for non-sensitive fixtures.

#### Contributor note

When changing test fixtures that look like secrets (for example, dummy passwords, tokens, or keys), verify scanner behavior before merging:

1. If the fixture file path changed, update both `.gitguardian.yaml` and `.cycode/config.yaml`.
2. Keep the ignore rule path-scoped to the minimum required file.
3. Prefer clearly fake placeholders over realistic credential formats whenever possible.

## Credentials and keys

### Generating a key pair

Snowflake key-pair authentication requires a PKCS#8 RSA private key and the corresponding public key registered on the Snowflake user.

#### Generate an unencrypted private key

```bash
mkdir -p keys
openssl genrsa 4096 | openssl pkcs8 -topk8 -inform PEM -out keys/rsa_key.p8 -nocrypt
chmod 600 keys/rsa_key.p8
```

#### Generate a passphrase-protected private key

```bash
mkdir -p keys
openssl genrsa 4096 | openssl pkcs8 -topk8 -inform PEM -out keys/rsa_key.p8
chmod 600 keys/rsa_key.p8
```

OpenSSL will prompt for a passphrase. Pass the same value to the tool via `--passphrase` at runtime.

#### Derive the public key

```bash
openssl rsa -in keys/rsa_key.p8 -pubout -out keys/rsa_key.pub
```

#### Register the public key with Snowflake

Strip the PEM header/footer lines, then set the key on your Snowflake user:

```sql
ALTER USER demo_user SET RSA_PUBLIC_KEY='<contents of keys/rsa_key.pub minus header and footer lines>';
```

You can extract just the key body with:

```bash
grep -v "^-----" keys/rsa_key.pub | tr -d '\n'
```

Paste the output as the value inside the single quotes in the `ALTER USER` statement.

## Development workflow

### Native development

```bash
go mod tidy
go test ./...
bats ./scripts/*.bats
govulncheck ./...
go build ./cmd/snowflake-reverse-engineer
```

If `govulncheck` is not installed locally, install it with:

```bash
go install golang.org/x/vuln/cmd/govulncheck@latest
```

### Go code coverage

Use Go's built-in coverage tooling to calculate statement coverage for this repository.

#### Repository-wide coverage (default test suite)

Generate a coverage profile and print per-function plus total coverage:

```bash
task coverage
```

Equivalent raw commands:

```bash
go test ./... -coverprofile=coverage.out
go tool cover -func=coverage.out
```

The final `total:` line is the repository-wide statement coverage percentage.

#### HTML coverage report

Generate and open an interactive report:

```bash
task coverage-html
```

Equivalent raw commands:

```bash
go test ./... -coverprofile=coverage.out
go tool cover -html=coverage.out
```

This opens a browser view with covered and uncovered lines highlighted.

#### Package-specific coverage

Measure one package (example: `internal/snowflake`):

```bash
go test ./internal/snowflake -coverprofile=coverage-snowflake.out
go tool cover -func=coverage-snowflake.out
```

Useful when you are improving tests for a specific package.

#### Cross-package instrumentation

Include all packages in coverage instrumentation while running tests:

```bash
go test ./... -coverpkg=./... -coverprofile=coverage-allpkgs.out
go tool cover -func=coverage-allpkgs.out
```

This is stricter than default coverage and can reveal gaps in code exercised indirectly.

#### Integration coverage (optional)

If integration test prerequisites are configured (`SNOWFLAKE_*` env vars), run:

```bash
go test -tags integration -timeout 300s ./... -coverprofile=coverage-integration.out
go tool cover -func=coverage-integration.out
```

Tip: Keep unit-test and integration coverage profiles separate so it is clear what each suite contributes.

#### Cleanup generated coverage files

```bash
task coverage-clean
```

Equivalent raw command:

```bash
rm -f coverage.out coverage-*.out
```

### Taskfile usage

This repository includes a `Taskfile.yml` for common development workflows.

Build the project binary:

```bash
task build
```

Run tests:

```bash
task test
```

Run bats-core tests for maintenance scripts:

```bash
task test-bats
```

Current bats-core script test coverage:

- `scripts/check_comment_style.bats` validates exported-comment style checks
- `scripts/check_markdown_quality.bats` validates markdown quality checker behavior
- `scripts/update_chainguard_images.bats` validates digest check/update behavior
- `scripts/update_github_actions.bats` validates action version check/update behavior
- `scripts/rename_module_path.bats` validates module path rewrite behavior
- `scripts/generate_module_dependency_diagram.bats` validates diagram generation/update behavior

Run markdown quality checks:

```bash
task markdown-check
```

Verify cloc is installed:

```bash
task cloc-check
```

Generate line-of-code reports:

```bash
task cloc
```

This writes:

- `cloc-report.md` (markdown summary)
- `cloc-report.json` (machine-readable report)

Run integration tests against a live Snowflake account:

```bash
task test-integration
```

Run the CLI locally with arguments:

```bash
task run -- --user demo_user --account demo_account --warehouse demo_wh --database demo_db --output-dir ./output --log-dir ./logs --private-key ./keys/rsa_key.p8
```

Other available tasks:

```bash
task tidy
task comment-check
task markdown-check
task cloc-check
task cloc
task test-bats
task test-integration
task vuln
task coverage
task coverage-html
task coverage-clean
task image-check
task image-update
task actions-check
task actions-update
task module-rename -- github.com/example/golang-snowflake-reverse-engineer
task module-diagram-print
task module-diagram-update
task docker-build
task docker-structure-test
task podman-build
task clean
```

#### Dockerfile structure test (Google Container Structure Test)

The repository includes a Dockerfile-focused Container Structure Test config at
`test/dockerfile-structure-test.yaml`.

Run via Taskfile:

```bash
task docker-structure-test
```

Equivalent raw commands:

```bash
docker build -t snowflake-reverse-engineer:cst .
container-structure-test test --image snowflake-reverse-engineer:cst --config ./test/dockerfile-structure-test.yaml
```

The test validates runtime image metadata (`WORKDIR`, `USER`, `ENTRYPOINT`) and
checks that the CLI binary exists at `/usr/local/bin/snowflake-reverse-engineer`.

The `.github/workflows/dockerfile-structure-test.yml` workflow runs these tests
automatically on pushes and pull requests that touch `Dockerfile`,
`Containerfile`, or `test/dockerfile-structure-test.yaml`.

#### Updating pinned Chainguard image digests

The repository includes `scripts/update_chainguard_images.sh` to keep pinned
Chainguard image digests in `Dockerfile` and `Containerfile` up to date.

Check only (no file changes). Exits `1` when digests are out of date:

```bash
bash ./scripts/update_chainguard_images.sh --check
```

Update both files in place when newer digests are available:

```bash
bash ./scripts/update_chainguard_images.sh --update
```

Equivalent optional Taskfile wrappers:

```bash
task image-check
task image-update
```

#### Updating GitHub Actions versions in workflow YAML files

The repository includes `scripts/update_github_actions.sh` to check and update
action versions referenced by YAML files in `.github/workflows/`.

Check only (no file changes). Exits `1` when one or more actions are out of date:

```bash
bash ./scripts/update_github_actions.sh --check
```

Update action references in place to latest stable release tags:

```bash
bash ./scripts/update_github_actions.sh --update
```

Equivalent optional Taskfile wrappers:

```bash
task actions-check
task actions-update
```

#### Renaming the Go module path for template use

The repository includes `scripts/rename_module_path.sh` to rewrite the current
module path from `go.mod` across repository text files such as Go source,
docs, and maintenance scripts.

Preview the files that would change:

```bash
bash ./scripts/rename_module_path.sh --dry-run github.com/example/golang-snowflake-reverse-engineer
```

Apply the rename in place:

```bash
bash ./scripts/rename_module_path.sh github.com/example/golang-snowflake-reverse-engineer
```

Equivalent optional Taskfile wrapper:

```bash
task module-rename -- github.com/example/golang-snowflake-reverse-engineer
```

#### Script maintenance notes

The `scripts` directory contains small automation helpers that are safe to run
locally and in CI. Use this section as a quick maintenance guide.

`scripts/check_comment_style.sh`

- Purpose: Enforces exported Go function doc comments to include a
  `// <FuncName>:` prefix and the `Given` / `when` / `then` keywords.
- Inputs: Scans all `.go` files in the repository (uses `rg` when available,
  otherwise falls back to `find`).
- Exit codes:
  - `0`: all files pass.
  - `1`: one or more style violations were found.

`scripts/check_markdown_quality.sh`

- Purpose: Runs automated Markdown quality checks across repository `.md` files.
- Tools used:
  - `markdownlint` for Markdown style/structure checks
  - `lychee` for link validation (offline mode)
  - `write-good` for prose quality checks
- Configuration: uses `.markdownlint.json` when present.
- Exit codes:
  - `0`: all Markdown checks passed.
  - `1`: required tooling missing or at least one checker failed.

`scripts/update_chainguard_images.sh`

- Purpose: Checks or updates pinned Chainguard image digests in
  `Dockerfile` and `Containerfile`.
- Modes:
  - `--check`: reports drift and exits non-zero when digests are out of date.
  - `--update`: updates both files in place when newer digests exist.
- Requirements: Docker with `buildx` support.
- Exit codes:
  - `0`: up to date or successfully updated.
  - `1`: required tool missing or drift detected in check mode.
  - `2`: invalid CLI arguments.

`scripts/update_github_actions.sh`

- Purpose: Checks or updates `uses:` references in YAML files under `.github/workflows/`
  to latest stable GitHub release tags.
- Modes:
  - `--check`: reports outdated action refs and exits non-zero when drift exists.
  - `--update`: updates outdated refs in place.
- Requirements: `curl` (and optional `GITHUB_TOKEN` to reduce GitHub API
  rate-limit issues).
- Exit codes:
  - `0`: up to date or successfully updated.
  - `1`: required tool missing, API lookup failure, or drift in check mode.
  - `2`: invalid CLI arguments.

`scripts/rename_module_path.sh`

- Purpose: Rewrites the current Go module path from `go.mod` across repository
  text files for fork/template workflows.
- Modes:
  - `--dry-run`: prints files that would be updated without changing them.
- Requirements: `perl` available in `PATH` (`rg` is used when available,
  otherwise the script falls back to `find`).
- Notes: Skips generated/runtime directories such as `bin/`, `logs/`,
  `output/`, and `keys/`.
- Exit codes:
  - `0`: rename succeeded, dry run succeeded, or no matching files were found.
  - `1`: required tools/files missing or replacement failed.
  - `2`: invalid CLI arguments.

`scripts/generate_module_dependency_diagram.sh`

- Purpose: Generates multiple Mermaid dependency diagrams from `go mod graph`
  (direct dependencies plus chunked transitive dependencies) and updates the
  managed module graph block in `README.md`.
- Modes:
  - `--print`: prints generated Mermaid content to stdout.
  - `--update-readme`: updates the managed diagram block in place.
  - `--chunk-size N`: sets max nodes per transitive chunk diagram
    (default: `20`).
- Requirements: Go toolchain available in `PATH`.
- Exit codes:
  - `0`: diagram generated successfully.
  - `1`: required tools/files missing.
  - `2`: invalid CLI arguments.

## Container usage

### Container build

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

### Container run

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

## Reference

### Notes on DDL generation

- The tool prefers `GET_DDL` for rows that clearly identify Snowflake objects such as tables, views, sequences, procedures, functions, tasks, stages, pipes, streams, and file formats.
- Some `INFORMATION_SCHEMA` views describe metadata rather than first-class objects. In those cases, the tool writes a SQL comment with the row payload so every row still produces deterministic output.

### Go module dependency diagram

This diagram set is generated from `go mod graph`.
Run the script from the repository root:

```bash
# Print the generated Mermaid diagram to stdout
bash ./scripts/generate_module_dependency_diagram.sh --print

# Regenerate and write the managed diagram block in README.md
bash ./scripts/generate_module_dependency_diagram.sh --update-readme

# Regenerate with smaller transitive chunks for easier viewing
bash ./scripts/generate_module_dependency_diagram.sh --update-readme --chunk-size 20
```

Equivalent optional Taskfile wrappers:

```bash
task module-diagram-print
task module-diagram-update
```

<!-- MODULE_DEP_GRAPH_START -->
### 1) Direct Dependencies

```mermaid
flowchart TD
  classDef root fill:#dff6dd,stroke:#2f855a,stroke-width:2px
  N1["github.com/99designs/go-keychain"]
  N2["github.com/99designs/keyring"]
  N3["github.com/Azure/azure-sdk-for-go/sdk/azcore"]
  N4["github.com/Azure/azure-sdk-for-go/sdk/internal"]
  N5["github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"]
  N6["github.com/BurntSushi/toml"]
  N7["github.com/DATA-DOG/go-sqlmock"]
  N8["github.com/apache/arrow-go/v18"]
  N9["github.com/aws/aws-sdk-go-v2"]
  N10["github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream"]
  N11["github.com/aws/aws-sdk-go-v2/config"]
  N12["github.com/aws/aws-sdk-go-v2/credentials"]
  N13["github.com/aws/aws-sdk-go-v2/feature/ec2/imds"]
  N14["github.com/aws/aws-sdk-go-v2/feature/s3/manager"]
  N15["github.com/aws/aws-sdk-go-v2/internal/configsources"]
  N16["github.com/aws/aws-sdk-go-v2/internal/endpoints/v2"]
  N17["github.com/aws/aws-sdk-go-v2/internal/ini"]
  N18["github.com/aws/aws-sdk-go-v2/internal/v4a"]
  N19["github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding"]
  N20["github.com/aws/aws-sdk-go-v2/service/internal/checksum"]
  N21["github.com/aws/aws-sdk-go-v2/service/internal/presigned-url"]
  N22["github.com/aws/aws-sdk-go-v2/service/internal/s3shared"]
  N23["github.com/aws/aws-sdk-go-v2/service/s3"]
  N24["github.com/aws/aws-sdk-go-v2/service/sso"]
  N25["github.com/aws/aws-sdk-go-v2/service/ssooidc"]
  N26["github.com/aws/aws-sdk-go-v2/service/sts"]
  N27["github.com/aws/smithy-go"]
  N28["github.com/danieljoos/wincred"]
  N29["github.com/dvsekhvalnov/jose2go"]
  N30["github.com/gabriel-vasile/mimetype"]
  N31["github.com/gmirsky/golang-snowflake-reverse-engineer"]
  class N31 root
  N32["github.com/goccy/go-json"]
  N33["github.com/godbus/dbus"]
  N34["github.com/golang-jwt/jwt/v5"]
  N35["github.com/google/flatbuffers"]
  N36["github.com/gsterjov/go-libsecret"]
  N37["github.com/klauspost/compress"]
  N38["github.com/klauspost/cpuid/v2"]
  N39["github.com/mtibben/percent"]
  N40["github.com/pierrec/lz4/v4"]
  N41["github.com/pkg/browser"]
  N42["github.com/snowflakedb/gosnowflake/v2"]
  N43["github.com/zeebo/xxh3"]
  N44["go"]
  N45["go.opentelemetry.io/otel"]
  N46["go.opentelemetry.io/otel/trace"]
  N47["golang.org/x/crypto"]
  N48["golang.org/x/exp"]
  N49["golang.org/x/mod"]
  N50["golang.org/x/net"]
  N51["golang.org/x/oauth2"]
  N52["golang.org/x/sync"]
  N53["golang.org/x/sys"]
  N54["golang.org/x/telemetry"]
  N55["golang.org/x/term"]
  N56["golang.org/x/text"]
  N57["golang.org/x/tools"]
  N58["golang.org/x/xerrors"]
  N31 --> N1
  N31 --> N2
  N31 --> N3
  N31 --> N4
  N31 --> N5
  N31 --> N6
  N31 --> N7
  N31 --> N8
  N31 --> N9
  N31 --> N10
  N31 --> N11
  N31 --> N12
  N31 --> N13
  N31 --> N14
  N31 --> N15
  N31 --> N16
  N31 --> N17
  N31 --> N18
  N31 --> N19
  N31 --> N20
  N31 --> N21
  N31 --> N22
  N31 --> N23
  N31 --> N24
  N31 --> N25
  N31 --> N26
  N31 --> N27
  N31 --> N28
  N31 --> N29
  N31 --> N30
  N31 --> N32
  N31 --> N33
  N31 --> N34
  N31 --> N35
  N31 --> N36
  N31 --> N37
  N31 --> N38
  N31 --> N39
  N31 --> N40
  N31 --> N41
  N31 --> N42
  N31 --> N43
  N31 --> N44
  N31 --> N45
  N31 --> N46
  N31 --> N47
  N31 --> N48
  N31 --> N49
  N31 --> N50
  N31 --> N51
  N31 --> N52
  N31 --> N53
  N31 --> N54
  N31 --> N55
  N31 --> N56
  N31 --> N57
  N31 --> N58
```

### 2) Transitive Dependencies (Chunked)

Transitive dependencies are split into chunks of up to 20 nodes for readability.

#### Chunk 1: Azure + AWS

```mermaid
flowchart TD
  classDef root fill:#dff6dd,stroke:#2f855a,stroke-width:2px
  N1["atomicgo.dev/cursor"]
  N2["atomicgo.dev/keyboard"]
  N3["atomicgo.dev/schedule"]
  N4["cloud.google.com/go"]
  N5["github.com/99designs/go-keychain"]
  N6["github.com/99designs/keyring"]
  N7["github.com/Azure/azure-sdk-for-go/sdk/azcore"]
  N8["github.com/Azure/azure-sdk-for-go/sdk/azidentity"]
  N9["github.com/Azure/azure-sdk-for-go/sdk/internal"]
  N10["github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"]
  N11["github.com/AzureAD/microsoft-authentication-library-for-go"]
  N12["github.com/BurntSushi/toml"]
  N13["github.com/DATA-DOG/go-sqlmock"]
  N14["github.com/andybalholm/brotli"]
  N15["github.com/antlr4-go/antlr/v4"]
  N16["github.com/apache/arrow-go/v18"]
  N17["github.com/apache/thrift"]
  N18["github.com/aws/aws-sdk-go-v2"]
  N19["github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream"]
  N20["github.com/gmirsky/golang-snowflake-reverse-engineer"]
  class N20 root
  N6 --> N5
  N7 --> N9
  N10 --> N7
  N10 --> N8
  N10 --> N9
  N10 --> N11
  N16 --> N1
  N16 --> N2
  N16 --> N3
  N16 --> N4
  N16 --> N14
  N16 --> N15
  N16 --> N17
  N20 --> N5
  N20 --> N6
  N20 --> N7
  N20 --> N9
  N20 --> N10
  N20 --> N12
  N20 --> N13
  N20 --> N16
  N20 --> N18
  N20 --> N19
```

#### Chunk 2: AWS + containerd

```mermaid
flowchart TD
  classDef root fill:#dff6dd,stroke:#2f855a,stroke-width:2px
  N1["github.com/aws/aws-sdk-go-v2/config"]
  N2["github.com/aws/aws-sdk-go-v2/credentials"]
  N3["github.com/aws/aws-sdk-go-v2/feature/ec2/imds"]
  N4["github.com/aws/aws-sdk-go-v2/feature/s3/manager"]
  N5["github.com/aws/aws-sdk-go-v2/internal/configsources"]
  N6["github.com/aws/aws-sdk-go-v2/internal/endpoints/v2"]
  N7["github.com/aws/aws-sdk-go-v2/internal/ini"]
  N8["github.com/aws/aws-sdk-go-v2/internal/v4a"]
  N9["github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding"]
  N10["github.com/aws/aws-sdk-go-v2/service/internal/checksum"]
  N11["github.com/aws/aws-sdk-go-v2/service/internal/presigned-url"]
  N12["github.com/aws/aws-sdk-go-v2/service/internal/s3shared"]
  N13["github.com/aws/aws-sdk-go-v2/service/s3"]
  N14["github.com/aws/aws-sdk-go-v2/service/sso"]
  N15["github.com/aws/aws-sdk-go-v2/service/ssooidc"]
  N16["github.com/aws/aws-sdk-go-v2/service/sts"]
  N17["github.com/aws/smithy-go"]
  N18["github.com/gmirsky/golang-snowflake-reverse-engineer"]
  class N18 root
  N1 --> N2
  N1 --> N3
  N1 --> N5
  N1 --> N6
  N1 --> N7
  N1 --> N9
  N1 --> N11
  N1 --> N14
  N1 --> N15
  N1 --> N16
  N1 --> N17
  N2 --> N3
  N2 --> N5
  N2 --> N6
  N2 --> N9
  N2 --> N11
  N2 --> N14
  N2 --> N15
  N2 --> N16
  N2 --> N17
  N3 --> N17
  N4 --> N1
  N4 --> N2
  N4 --> N3
  N4 --> N5
  N4 --> N6
  N4 --> N7
  N4 --> N8
  N4 --> N9
  N4 --> N10
  N4 --> N11
  N4 --> N12
  N4 --> N13
  N4 --> N14
  N4 --> N15
  N4 --> N16
  N4 --> N17
  N5 --> N17
  N6 --> N17
  N8 --> N17
  N9 --> N17
  N10 --> N11
  N10 --> N17
  N11 --> N17
  N12 --> N17
  N13 --> N5
  N13 --> N6
  N13 --> N8
  N13 --> N9
  N13 --> N10
  N13 --> N11
  N13 --> N12
  N13 --> N17
  N14 --> N5
  N14 --> N6
  N14 --> N17
  N15 --> N5
  N15 --> N6
  N15 --> N17
  N16 --> N5
  N16 --> N6
  N16 --> N9
  N16 --> N11
  N16 --> N17
  N18 --> N1
  N18 --> N2
  N18 --> N3
  N18 --> N4
  N18 --> N5
  N18 --> N6
  N18 --> N7
  N18 --> N8
  N18 --> N9
  N18 --> N10
  N18 --> N11
  N18 --> N12
  N18 --> N13
  N18 --> N14
  N18 --> N15
  N18 --> N16
  N18 --> N17
```

#### Chunk 3: Google + golang-jwt

```mermaid
flowchart TD
  classDef root fill:#dff6dd,stroke:#2f855a,stroke-width:2px
  N1["github.com/danieljoos/wincred"]
  N2["github.com/davecgh/go-spew"]
  N3["github.com/dvsekhvalnov/jose2go"]
  N4["github.com/gabriel-vasile/mimetype"]
  N5["github.com/gmirsky/golang-snowflake-reverse-engineer"]
  class N5 root
  N6["github.com/goccy/go-json"]
  N7["github.com/godbus/dbus"]
  N8["github.com/golang-jwt/jwt/v5"]
  N9["github.com/google/flatbuffers"]
  N1 --> N2
  N5 --> N1
  N5 --> N3
  N5 --> N4
  N5 --> N6
  N5 --> N7
  N5 --> N8
  N5 --> N9
```

#### Chunk 4: klauspost + modern-go

```mermaid
flowchart TD
  classDef root fill:#dff6dd,stroke:#2f855a,stroke-width:2px
  N1["github.com/gmirsky/golang-snowflake-reverse-engineer"]
  class N1 root
  N2["github.com/gsterjov/go-libsecret"]
  N3["github.com/klauspost/compress"]
  N4["github.com/klauspost/cpuid/v2"]
  N5["github.com/kr/pty"]
  N6["github.com/kr/text"]
  N1 --> N2
  N1 --> N3
  N1 --> N4
  N6 --> N5
```

#### Chunk 5: tidwall + substrait-io

```mermaid
flowchart TD
  classDef root fill:#dff6dd,stroke:#2f855a,stroke-width:2px
  N1["github.com/gmirsky/golang-snowflake-reverse-engineer"]
  class N1 root
  N2["github.com/mtibben/percent"]
  N3["github.com/pierrec/lz4/v4"]
  N4["github.com/pkg/browser"]
  N5["github.com/snowflakedb/gosnowflake/v2"]
  N1 --> N2
  N1 --> N3
  N1 --> N4
  N1 --> N5
  N5 --> N2
  N5 --> N3
  N5 --> N4
```

#### Chunk 6: Go x + OpenTelemetry

```mermaid
flowchart TD
  classDef root fill:#dff6dd,stroke:#2f855a,stroke-width:2px
  N1["github.com/gmirsky/golang-snowflake-reverse-engineer"]
  class N1 root
  N2["github.com/zeebo/assert"]
  N3["github.com/zeebo/xxh3"]
  N4["go"]
  N5["go.opentelemetry.io/auto/sdk"]
  N6["go.opentelemetry.io/otel"]
  N7["go.opentelemetry.io/otel/metric"]
  N8["go.opentelemetry.io/otel/trace"]
  N9["golang.org/x/crypto"]
  N10["golang.org/x/exp"]
  N11["golang.org/x/mod"]
  N12["golang.org/x/net"]
  N13["golang.org/x/oauth2"]
  N14["golang.org/x/sync"]
  N15["golang.org/x/sys"]
  N16["golang.org/x/telemetry"]
  N17["golang.org/x/term"]
  N18["golang.org/x/text"]
  N1 --> N3
  N1 --> N4
  N1 --> N6
  N1 --> N8
  N1 --> N9
  N1 --> N10
  N1 --> N11
  N1 --> N12
  N1 --> N13
  N1 --> N14
  N1 --> N15
  N1 --> N16
  N1 --> N17
  N1 --> N18
  N3 --> N2
  N6 --> N4
  N6 --> N5
  N6 --> N7
  N6 --> N8
  N8 --> N4
  N8 --> N6
  N9 --> N4
  N9 --> N12
  N9 --> N15
  N9 --> N17
  N9 --> N18
  N10 --> N4
  N10 --> N11
  N10 --> N14
  N11 --> N4
  N12 --> N4
  N12 --> N9
  N12 --> N15
  N12 --> N17
  N12 --> N18
  N13 --> N4
  N14 --> N4
  N15 --> N4
  N16 --> N4
  N16 --> N11
  N16 --> N14
  N16 --> N15
  N17 --> N4
  N17 --> N15
  N18 --> N4
  N18 --> N11
  N18 --> N14
```

#### Chunk 7: Go x + toolchain

```mermaid
flowchart TD
  classDef root fill:#dff6dd,stroke:#2f855a,stroke-width:2px
  N1["github.com/gmirsky/golang-snowflake-reverse-engineer"]
  class N1 root
  N2["golang.org/x/tools"]
  N3["golang.org/x/xerrors"]
  N1 --> N2
  N1 --> N3
```

<!-- MODULE_DEP_GRAPH_END -->
