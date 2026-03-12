#!/usr/bin/env bash
set -euo pipefail

# Enforce a lightweight doc comment convention for exported Go functions:
# 1) Immediate comment block above function starts with "// <FuncName>:"
# 2) Comment block includes "Given", "when", and "then".
# This script is intentionally strict and exits non-zero on first file-level failure
# so it can be used as a CI gate.

# Prefer ripgrep for speed across larger repositories.
# Fallback to find+sed keeps the script usable on machines without rg.
if command -v rg >/dev/null 2>&1; then
  mapfile -t files < <(rg --files -g '**/*.go')
else
  mapfile -t files < <(find . -type f -name '*.go' | sed 's#^./##')
fi

# Aggregate status across all files so the output shows every violation found
# in one run, instead of failing fast on only the first bad file.
status=0

for file in "${files[@]}"; do
  # Parse each file with awk because function/comment association is easier to
  # express as a simple line-state machine than with grep-only patterns.
  awk '
    function reset_block() {
      # Clear currently tracked comment block state.
      block = ""
      first = ""
      count = 0
    }

    {
      line = $0

      if (line ~ /^\/\//) {
        # Capture the first comment line separately for prefix validation,
        # and also accumulate the full block for keyword checks.
        if (count == 0) {
          first = line
        }
        block = block line "\n"
        count++
        next
      }

      if (line ~ /^[[:space:]]*$/) {
        # Blank lines break immediate adjacency, so comments above a blank line
        # are not considered function doc comments.
        reset_block()
        next
      }

      if (line ~ /^func[[:space:]]+/) {
        # Normalize the signature into a function name by removing:
        # - leading "func"
        # - optional receiver section for methods
        # - parameter list and return values
        signature = line
        sub(/^func[[:space:]]+/, "", signature)

        if (signature ~ /^\(/) {
          sub(/^\([^)]*\)[[:space:]]*/, "", signature)
        }

        name = signature
        sub(/\(.*/, "", name)

        if (name ~ /^[A-Z]/) {
          # Only exported symbols are enforced. Unexported symbols are ignored.
          expected = "// " name ":"
          if (index(first, expected) != 1) {
            printf("%s:%d missing style doc comment for %s (expected prefix \"%s\")\n", FILENAME, NR, name, expected)
            failed = 1
          }

          # Keep this check intentionally case-sensitive for a stable style rule.
          if (block !~ /Given/ || block !~ /when/ || block !~ /then/) {
            printf("%s:%d doc comment for %s must include Given/when/then\n", FILENAME, NR, name)
            failed = 1
          }
        }

        # Always reset after evaluating a function so comments do not bleed into
        # the next declaration.
        reset_block()
        next
      }

      # Any non-comment, non-function line breaks the doc-comment context.
      reset_block()
    }

    END {
      # Returning non-zero from awk marks the file as failed for this pass.
      if (failed) {
        exit 1
      }
    }
  ' "$file" || status=1
done

# Exit code contract:
# - 0: all files passed
# - 1: at least one violation exists
if [[ "$status" -ne 0 ]]; then
  echo "Comment style check failed"
  exit 1
fi

echo "Comment style check passed"
