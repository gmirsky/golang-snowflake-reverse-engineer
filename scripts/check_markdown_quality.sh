#!/usr/bin/env bash
set -euo pipefail

# Validate Markdown quality across repository docs using three linters:
# - markdownlint for Markdown structure/style rules
# - lychee for link validation (offline mode for deterministic local checks)
# - write-good for prose quality
#
# Exit code contract:
# - 0: all checks passed
# - 1: at least one checker failed or required tooling is missing

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Prefer ripgrep for speed; fallback keeps compatibility on minimal systems.
if command -v rg >/dev/null 2>&1; then
  mapfile -t markdown_files < <(rg --files -g '**/*.md' "$ROOT_DIR")
else
  mapfile -t markdown_files < <(find "$ROOT_DIR" -type f -name '*.md' | sort)
fi

if [[ "${#markdown_files[@]}" -eq 0 ]]; then
  echo "No markdown files found"
  exit 0
fi

missing_tools=0
for tool in markdownlint lychee write-good; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "error: required tool not found: $tool"
    missing_tools=1
  fi
done
if [[ "$missing_tools" -ne 0 ]]; then
  echo "hint: install markdownlint, lychee, and write-good before running this check"
  exit 1
fi

status=0

echo "Running markdownlint on ${#markdown_files[@]} file(s)..."
if [[ -f "$ROOT_DIR/.markdownlint.json" ]]; then
  markdownlint --config "$ROOT_DIR/.markdownlint.json" "${markdown_files[@]}" || status=1
else
  markdownlint "${markdown_files[@]}" || status=1
fi

echo "Running lychee (offline) on ${#markdown_files[@]} file(s)..."
lychee --offline --no-progress "${markdown_files[@]}" || status=1

echo "Running write-good on ${#markdown_files[@]} file(s)..."
# Keep write-good focused on high-signal checks for technical docs by disabling
# rules that tend to produce excessive noise for command-heavy documentation.
write-good \
  --no-passive \
  --no-weasel \
  --no-tooWordy \
  --no-illusion \
  --no-adverb \
  "${markdown_files[@]}" || status=1

if [[ "$status" -ne 0 ]]; then
  echo "Markdown quality check failed"
  exit 1
fi

echo "Markdown quality check passed"
