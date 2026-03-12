#!/usr/bin/env bash
set -euo pipefail

# Generate readable Mermaid dependency diagrams from `go mod graph`.
#
# Instead of one very large diagram, this script emits multiple diagrams:
# 1) Direct dependencies from the root module.
# 2) Chunked transitive dependency subgraphs.
#
# It can print markdown to stdout or update a managed block in README.
#
# Exit codes:
# - 0: success
# - 1: required tools/files missing
# - 2: invalid arguments

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
README_FILE="$ROOT_DIR/README.md"
MARKER_START="<!-- MODULE_DEP_GRAPH_START -->"
MARKER_END="<!-- MODULE_DEP_GRAPH_END -->"
CHUNK_SIZE=20
MODE="update-readme"

usage() {
  cat <<'EOF'
Usage: scripts/generate_module_dependency_diagram.sh [--print|--update-readme] [--chunk-size N]

Options:
  --print          Print generated markdown (with mermaid blocks) to stdout
  --update-readme  Update README.md managed dependency-diagram block (default)
  --chunk-size N   Number of nodes per transitive chunk diagram (default: 20)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --print)
      MODE="print"
      shift
      ;;
    --update-readme)
      MODE="update-readme"
      shift
      ;;
    --chunk-size)
      if [[ $# -lt 2 ]]; then
        echo "error: --chunk-size requires a value"
        exit 2
      fi
      CHUNK_SIZE="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1"
      usage
      exit 2
      ;;
  esac
done

if ! [[ "$CHUNK_SIZE" =~ ^[0-9]+$ ]] || [[ "$CHUNK_SIZE" -le 0 ]]; then
  echo "error: --chunk-size must be a positive integer"
  exit 2
fi

if ! command -v go >/dev/null 2>&1; then
  echo "error: go is required"
  exit 1
fi

if [[ "$MODE" == "update-readme" && ! -f "$README_FILE" ]]; then
  echo "error: README file not found: $README_FILE"
  exit 1
fi

edges_file="$(mktemp)"
readme_tmp="$(mktemp)"
output_md="$(mktemp)"
workdir="$(mktemp -d)"
cleanup() {
  rm -f "$edges_file" "$readme_tmp" "$output_md"
  rm -rf "$workdir"
}
trap cleanup EXIT

# Build stable dependency edges without versions, so diagram churn is reduced.
go mod graph \
  | awk '{
      split($1, a, "@");
      split($2, b, "@");
      from = a[1];
      to = b[1];
      if (from != "" && to != "" && from != to) {
        printf("%s\t%s\n", from, to);
      }
    }' \
  | sort -u > "$edges_file"

root_module="$(go list -m)"

# Build a Mermaid diagram from an edge list file.
# The node map is local per diagram so IDs remain compact and readable.
write_mermaid_diagram() {
  local edges_path="$1"
  local include_root_class="$2"
  local nodes_path="$workdir/nodes.tmp"
  local map_path="$workdir/map.tmp"

  : > "$nodes_path"
  : > "$map_path"

  # Node set from both edge endpoints.
  cut -f1 "$edges_path" > "$nodes_path" || true
  cut -f2 "$edges_path" >> "$nodes_path" || true
  sort -u "$nodes_path" -o "$nodes_path"

  # If there are no edges, still print a valid empty diagram.
  if [[ ! -s "$nodes_path" ]]; then
    echo "flowchart TD"
    echo "  Empty[\"No dependencies in this slice\"]"
    return
  fi

  node_index=0
  while IFS= read -r node; do
    node_index=$((node_index + 1))
    printf '%s\tN%s\n' "$node" "$node_index" >> "$map_path"
  done < "$nodes_path"

  echo "flowchart TD"
  echo "  classDef root fill:#dff6dd,stroke:#2f855a,stroke-width:2px"

  while IFS=$'\t' read -r module node_id; do
    escaped_module="${module//\"/\\\"}"
    printf '  %s["%s"]\n' "$node_id" "$escaped_module"
    if [[ "$include_root_class" == "yes" && "$module" == "$root_module" ]]; then
      printf '  class %s root\n' "$node_id"
    fi
  done < "$map_path"

  while IFS=$'\t' read -r from to; do
    from_id="$(awk -F '\t' -v m="$from" '$1 == m { print $2; exit }' "$map_path")"
    to_id="$(awk -F '\t' -v m="$to" '$1 == m { print $2; exit }' "$map_path")"
    if [[ -n "$from_id" && -n "$to_id" ]]; then
      printf '  %s --> %s\n' "$from_id" "$to_id"
    fi
  done < "$edges_path"
}

# Build a short human-readable chunk label from the dominant module namespaces
# in the chunk. This makes section titles easier to scan in README.
describe_chunk_name() {
  local chunk_path="$1"
  awk '
    function ns(mod, a, n) {
      n = split(mod, a, "/")
      if (n >= 3 && a[1] == "github.com") return a[1] "/" a[2]
      if (n >= 3 && a[1] == "golang.org") return a[1] "/" a[2]
      if (n >= 2) return a[1] "/" a[2]
      return mod
    }
    {
      print ns($1)
    }
  ' "$chunk_path" \
    | sort \
    | uniq -c \
    | sort -nr \
    | head -2 \
    | awk '{print $2}'
}

# Convert technical namespace prefixes into reader-friendly labels.
format_namespace_label() {
  local ns="$1"
  case "$ns" in
    github.com/aws)
      echo "AWS"
      ;;
    github.com/Azure)
      echo "Azure"
      ;;
    cloud.google.com/go)
      echo "Google Cloud"
      ;;
    golang.org/x)
      echo "Go x"
      ;;
    go.opentelemetry.io/otel)
      echo "OpenTelemetry"
      ;;
    github.com/snowflakedb)
      echo "Snowflake"
      ;;
    github.com/apache)
      echo "Apache"
      ;;
    github.com/google)
      echo "Google"
      ;;
    github.com/AzureAD)
      echo "Azure AD"
      ;;
    github.com/*)
      echo "${ns#github.com/}"
      ;;
    *)
      echo "$ns"
      ;;
  esac
}

# 1) Direct dependency diagram: root -> direct modules.
direct_edges="$workdir/direct_edges.tsv"
awk -F '\t' -v r="$root_module" '$1 == r { print }' "$edges_file" > "$direct_edges"

# Build markdown content for all diagrams.
{
  echo "### 1) Direct Dependencies"
  echo
  echo '```mermaid'
  write_mermaid_diagram "$direct_edges" "yes"
  echo '```'
  echo

  echo "### 2) Transitive Dependencies (Chunked)"
  echo
  echo "Transitive dependencies are split into chunks of up to ${CHUNK_SIZE} nodes for readability."
  echo

  # Nodes participating in non-root edges represent transitive graph internals.
  transitive_nodes="$workdir/transitive_nodes.txt"
  awk -F '\t' -v r="$root_module" '$1 != r { print $1; print $2 }' "$edges_file" | sort -u > "$transitive_nodes"

  if [[ ! -s "$transitive_nodes" ]]; then
    echo "No transitive dependencies found."
  else
    # Split deterministic node list into numbered chunk files.
    split -l "$CHUNK_SIZE" "$transitive_nodes" "$workdir/chunk_"

    chunk_num=0
    for chunk_file in "$workdir"/chunk_*; do
      [[ -e "$chunk_file" ]] || continue
      chunk_num=$((chunk_num + 1))
      chunk_name_raw="$(describe_chunk_name "$chunk_file")"
      chunk_name=""
      while IFS= read -r ns; do
        [[ -n "$ns" ]] || continue
        label="$(format_namespace_label "$ns")"
        if [[ -z "$chunk_name" ]]; then
          chunk_name="$label"
        else
          chunk_name="$chunk_name + $label"
        fi
      done <<< "$chunk_name_raw"

      if [[ -z "$chunk_name" ]]; then
        chunk_name="Misc"
      fi

      chunk_edges="$workdir/chunk_edges_${chunk_num}.tsv"

      # Include edges fully inside the chunk plus root->chunk edges to provide entry context.
      awk -F '\t' -v r="$root_module" '
        NR == FNR {
          in_chunk[$1] = 1
          next
        }
        (in_chunk[$1] && in_chunk[$2]) || ($1 == r && in_chunk[$2]) {
          print
        }
      ' "$chunk_file" "$edges_file" > "$chunk_edges"

      echo "#### Chunk ${chunk_num}: ${chunk_name}"
      echo
      echo '```mermaid'
      write_mermaid_diagram "$chunk_edges" "yes"
      echo '```'
      echo
    done
  fi
} > "$output_md"

if [[ "$MODE" == "print" ]]; then
  cat "$output_md"
  exit 0
fi

# Compose managed README block.
managed_block_file="$workdir/managed_block.md"
{
  echo "$MARKER_START"
  cat "$output_md"
  echo "$MARKER_END"
} > "$managed_block_file"

# Replace existing managed block when present, otherwise append a new section.
if grep -q "$MARKER_START" "$README_FILE" && grep -q "$MARKER_END" "$README_FILE"; then
  awk -v start="$MARKER_START" -v end="$MARKER_END" -v block="$managed_block_file" '
    $0 == start {
      while ((getline line < block) > 0) {
        print line
      }
      in_block = 1
      next
    }
    $0 == end {
      in_block = 0
      next
    }
    !in_block {
      print
    }
  ' "$README_FILE" > "$readme_tmp"
else
  cat "$README_FILE" > "$readme_tmp"
  cat >> "$readme_tmp" <<EOF

## Go module dependency diagram

This diagram set is generated from \`go mod graph\`.
Run \`bash ./scripts/generate_module_dependency_diagram.sh --update-readme\` to refresh it.

$MARKER_START
EOF
  cat "$output_md" >> "$readme_tmp"
  cat >> "$readme_tmp" <<EOF
$MARKER_END
EOF
fi

mv "$readme_tmp" "$README_FILE"

echo "README updated with chunked module dependency diagrams"
