#!/usr/bin/env bash
set -euo pipefail

# Rename the repository's Go module path across source and docs files.
#
# This is intended for template/fork workflows where the repo is copied under a
# new owner or host and all self-imports need to be updated consistently.
#
# Exit codes:
# - 0: success
# - 1: required tools/files missing or replacement failure
# - 2: invalid arguments

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GO_MOD_FILE="$ROOT_DIR/go.mod"
DRY_RUN="no"

usage() {
  cat <<'EOF'
Usage: scripts/rename_module_path.sh [--dry-run] <new-module-path>

Arguments:
  <new-module-path>  Replacement Go module path, for example:
                     github.com/example/golang-snowflake-reverse-engineer

Options:
  --dry-run          Print files that would be updated without editing them
  -h, --help         Show this help text
EOF
}

NEW_MODULE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      DRY_RUN="yes"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    -* )
      echo "error: unknown argument: $1"
      usage
      exit 2
      ;;
    *)
      if [[ -n "$NEW_MODULE" ]]; then
        echo "error: only one new module path may be provided"
        usage
        exit 2
      fi
      NEW_MODULE="$1"
      shift
      ;;
  esac
done

if [[ -z "$NEW_MODULE" ]]; then
  echo "error: new module path is required"
  usage
  exit 2
fi

if [[ ! -f "$GO_MOD_FILE" ]]; then
  echo "error: go.mod not found: $GO_MOD_FILE"
  exit 1
fi

if ! command -v perl >/dev/null 2>&1; then
  echo "error: perl is required"
  exit 1
fi

if [[ "$NEW_MODULE" =~ [[:space:]] ]]; then
  echo "error: module path must not contain whitespace"
  exit 2
fi

if [[ ! "$NEW_MODULE" =~ ^[A-Za-z0-9._~/-]+$ ]]; then
  echo "error: module path contains unsupported characters"
  exit 2
fi

OLD_MODULE="$(awk '/^module[[:space:]]+/ { print $2; exit }' "$GO_MOD_FILE")"
if [[ -z "$OLD_MODULE" ]]; then
  echo "error: could not determine current module path from go.mod"
  exit 1
fi

if [[ "$NEW_MODULE" == "$OLD_MODULE" ]]; then
  echo "module path already set to $NEW_MODULE"
  exit 0
fi

cd "$ROOT_DIR"

changed_files=()
while IFS= read -r -d '' file_path; do
  file_path="${file_path#./}"

  case "$file_path" in
    bin/*|keys/*|logs/*|output/*)
      continue
      ;;
  esac

  if [[ ! -f "$file_path" ]]; then
    continue
  fi

  if ! grep -Iq . "$file_path"; then
    continue
  fi

  if grep -Fq "$OLD_MODULE" "$file_path"; then
    changed_files+=("$file_path")
  fi
done < <(
  if command -v rg >/dev/null 2>&1; then
    rg --files -uu -0 . \
      -g '!.git/**' \
      -g '!bin/**' \
      -g '!keys/**' \
      -g '!logs/**' \
      -g '!output/**'
  else
    find . \
      -path './.git' -prune -o \
      -path './bin' -prune -o \
      -path './keys' -prune -o \
      -path './logs' -prune -o \
      -path './output' -prune -o \
      -type f -print0
  fi
)

if [[ ${#changed_files[@]} -eq 0 ]]; then
  echo "no repository text files reference $OLD_MODULE"
  exit 0
fi

printf 'current module: %s\n' "$OLD_MODULE"
printf 'new module: %s\n' "$NEW_MODULE"

if [[ "$DRY_RUN" == "yes" ]]; then
  printf 'dry run: %d file(s) would be updated\n' "${#changed_files[@]}"
  printf '%s\n' "${changed_files[@]}"
  exit 0
fi

for file_path in "${changed_files[@]}"; do
  OLD_MODULE="$OLD_MODULE" NEW_MODULE="$NEW_MODULE" perl -0pi -e 's/\Q$ENV{OLD_MODULE}\E/$ENV{NEW_MODULE}/g' "$file_path"
done

printf 'updated %d file(s)\n' "${#changed_files[@]}"
printf '%s\n' "${changed_files[@]}"
echo "next: run 'go test ./...' and regenerate any derived docs if needed"