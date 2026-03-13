#!/usr/bin/env bash
set -euo pipefail

# Check/update GitHub Action versions in workflow YAML files to latest stable release tags.
# Supports lines like:
#   uses: owner/repo@ref
#   uses: owner/repo@ref # optional comment
# Exit code contract:
# - 0 on success or already up-to-date
# - 1 when tooling is missing, API lookup fails, or check mode detects drift
# - 2 for invalid arguments

# Resolve root from script location so execution is location-independent.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKFLOW_DIR="$ROOT_DIR/.github/workflows"
MODE="update"

# Default to update mode; check mode is useful for CI policy enforcement.
if [[ "${1-}" == "--check" ]]; then
  MODE="check"
elif [[ "${1-}" == "--update" || -z "${1-}" ]]; then
  MODE="update"
else
  echo "Usage: $0 [--check|--update]"
  exit 2
fi

if [[ ! -d "$WORKFLOW_DIR" ]]; then
  echo "error: workflow directory not found: $WORKFLOW_DIR"
  exit 1
fi

if ! command -v curl >/dev/null 2>&1; then
  echo "error: curl is required"
  exit 1
fi

github_api_get_latest_tag() {
  local repo="$1"
  # /releases/latest returns the newest non-prerelease, non-draft stable release.
  local url="https://api.github.com/repos/${repo}/releases/latest"
  local response tag

  # Use token when provided to reduce API rate-limit issues in CI and local runs.
  if [[ -n "${GITHUB_TOKEN-}" ]]; then
    response="$(curl -fsSL \
      -H "Accept: application/vnd.github+json" \
      -H "Authorization: Bearer ${GITHUB_TOKEN}" \
      "$url")"
  else
    response="$(curl -fsSL \
      -H "Accept: application/vnd.github+json" \
      "$url")"
  fi

  # Parse "tag_name" without jq to keep dependencies minimal.
  tag="$(printf '%s\n' "$response" | sed -n 's/^[[:space:]]*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n1)"

  if [[ -z "$tag" ]]; then
    # Send this to stderr so tooling can distinguish operational errors from status.
    echo "error: could not determine latest release tag for $repo" >&2
    return 1
  fi

  printf '%s\n' "$tag"
}

# Counters provide user-facing summary and CI-friendly behavior.
changed=0
outdated=0
checked=0
workflow_files=()

shopt -s nullglob
for workflow_file in "$WORKFLOW_DIR"/*.yml "$WORKFLOW_DIR"/*.yaml; do
  workflow_files+=("$workflow_file")
done
shopt -u nullglob

if [[ "${#workflow_files[@]}" -eq 0 ]]; then
  echo "error: no workflow YAML files found in $WORKFLOW_DIR"
  exit 1
fi

for workflow_file in "${workflow_files[@]}"; do
  # Build output in a temp file first, then swap into place atomically when needed.
  tmp_file="$(mktemp)"

  while IFS= read -r line; do
    # Match "uses: owner/repo@ref" with optional trailing comment.
    if [[ "$line" =~ ^([[:space:]]*uses:[[:space:]]*)([A-Za-z0-9._-]+/[A-Za-z0-9._-]+)@([^[:space:]#]+)([[:space:]]*#.*)?$ ]]; then
      prefix="${BASH_REMATCH[1]}"
      repo="${BASH_REMATCH[2]}"
      current_ref="${BASH_REMATCH[3]}"

      # Resolve latest stable tag per action repository.
      latest_tag="$(github_api_get_latest_tag "$repo")"
      checked=$((checked + 1))

      if [[ "$current_ref" != "$latest_tag" ]]; then
        outdated=$((outdated + 1))
        echo "outdated: $(basename "$workflow_file"): $repo ($current_ref -> $latest_tag)"

        if [[ "$MODE" == "update" ]]; then
          # Keep indentation/prefix; replace ref with latest tag and normalize comment.
          line="${prefix}${repo}@${latest_tag} # ${latest_tag}"
          changed=$((changed + 1))
        fi
      fi
    fi

    printf '%s\n' "$line" >> "$tmp_file"
  done < "$workflow_file"

  if [[ "$MODE" == "update" && -f "$tmp_file" ]] && ! cmp -s "$tmp_file" "$workflow_file"; then
    # Atomic replace avoids partially written files if the process is interrupted.
    mv "$tmp_file" "$workflow_file"
  else
    rm -f "$tmp_file"
  fi
done

# In check mode, return failure when drift exists so this can gate PRs.
if [[ "$MODE" == "check" ]]; then
  echo "checked: $checked action reference(s)"
  if [[ "$outdated" -gt 0 ]]; then
    echo "status: out-of-date"
    exit 1
  fi

  echo "status: up-to-date"
  exit 0
fi

if [[ "$changed" -gt 0 ]]; then
  echo "updated: $changed action reference(s)"
  exit 0
fi

echo "checked: $checked action reference(s)"
echo "status: already up-to-date (no changes made)"
