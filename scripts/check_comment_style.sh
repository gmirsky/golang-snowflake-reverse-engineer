#!/usr/bin/env bash
set -euo pipefail

# Enforce a lightweight doc comment convention for exported Go functions:
# 1) Immediate comment block above function starts with "// <FuncName>:"
# 2) Comment block includes "Given", "when", and "then".

if command -v rg >/dev/null 2>&1; then
  mapfile -t files < <(rg --files -g '**/*.go')
else
  mapfile -t files < <(find . -type f -name '*.go' | sed 's#^./##')
fi

status=0

for file in "${files[@]}"; do
  awk '
    function reset_block() {
      block = ""
      first = ""
      count = 0
    }

    {
      line = $0

      if (line ~ /^\/\//) {
        if (count == 0) {
          first = line
        }
        block = block line "\n"
        count++
        next
      }

      if (line ~ /^[[:space:]]*$/) {
        reset_block()
        next
      }

      if (line ~ /^func[[:space:]]+/) {
        signature = line
        sub(/^func[[:space:]]+/, "", signature)

        if (signature ~ /^\(/) {
          sub(/^\([^)]*\)[[:space:]]*/, "", signature)
        }

        name = signature
        sub(/\(.*/, "", name)

        if (name ~ /^[A-Z]/) {
          expected = "// " name ":"
          if (index(first, expected) != 1) {
            printf("%s:%d missing style doc comment for %s (expected prefix \"%s\")\n", FILENAME, NR, name, expected)
            failed = 1
          }

          if (block !~ /Given/ || block !~ /when/ || block !~ /then/) {
            printf("%s:%d doc comment for %s must include Given/when/then\n", FILENAME, NR, name)
            failed = 1
          }
        }

        reset_block()
        next
      }

      reset_block()
    }

    END {
      if (failed) {
        exit 1
      }
    }
  ' "$file" || status=1
done

if [[ "$status" -ne 0 ]]; then
  echo "Comment style check failed"
  exit 1
fi

echo "Comment style check passed"
