#!/usr/bin/env bash
set -euo pipefail

# Compute project root relative to script location
ROOT="$(cd "$(dirname "$0")" && pwd)"

# Freeze using venv/bin/pip
PIP="$ROOT/venv/bin/pip"

if [ ! -x "$PIP" ]; then
  echo "Error: $PIP not found or not executable. Did you create the venv?" >&2
  exit 1
fi

## Generate requirements.txt (exclude local path packages like bootstrap)
$PIP freeze \
  | grep -vE '^(bootstrap\s+@|.*file://)' \
  > "$ROOT/requirements.txt"

echo "✅ requirements.txt written at $ROOT/requirements.txt"
