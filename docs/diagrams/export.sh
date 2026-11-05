#!/usr/bin/env bash
# Export the project Mermaid diagrams to SVG and PNG.
# Usage: ./export.sh
# Requires either the Mermaid CLI (`mmdc`) or Docker.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="$ROOT/exports"
mkdir -p "$OUT"

# name:max-size-for-png
DIAGRAMS=(
  "system-architecture:3000"
  "dfd-level-0:2200"
  "dfd-level-1:3600"
  "sequence-diagram:2800"
  "activity-diagram:3200"
  "component-diagram:2800"
  "state-diagram:2400"
  "erd:1600"
)

render() {
  local name="$1"
  local size="$2"
  local input="$ROOT/$name.mmd"
  local svg="$OUT/$name.svg"
  local png="$OUT/$name.png"

  if command -v mmdc >/dev/null 2>&1; then
    mmdc -i "$input" -o "$svg"
    mmdc -i "$input" -o "$png" -b white --size "$size"
  elif command -v docker >/dev/null 2>&1; then
    local image="${MERMAID_IMAGE:-minlag/mermaid-cli:latest}"
    docker run --rm -u "$(id -u):$(id -g)" \
      -v "$ROOT:/data" "$image" \
      -i "/data/$name.mmd" -o "/data/exports/$name.svg"
    docker run --rm -u "$(id -u):$(id -g)" \
      -v "$ROOT:/data" "$image" \
      -i "/data/$name.mmd" -o "/data/exports/$name.png" -b white --size "$size"
  else
    echo "error: install the Mermaid CLI (mmdc) or Docker" >&2
    exit 1
  fi
}

for entry in "${DIAGRAMS[@]}"; do
  name="${entry%%:*}"
  size="${entry##*:}"
  echo "==> $name"
  render "$name" "$size"
done

echo "Exports written to $OUT"
