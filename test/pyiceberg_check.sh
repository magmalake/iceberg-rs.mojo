#!/usr/bin/env bash
#
# Cross-implementation check: read the table the Mojo test just wrote with
# PyIceberg (the reference Python implementation) and assert it sees the same
# rows. This is the whole point of the bridge — if iceberg-rust and PyIceberg
# disagree, the bug is ours, and the native magmalake stack would inherit it.
#
# Needs `uv` on PATH; the venv is throwaway and lives under build/.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [ ! -f build/pyiceberg_env.sh ]; then
  echo "build/pyiceberg_env.sh missing — run 'pixi run test' first" >&2
  exit 1
fi
# shellcheck disable=SC1091
source build/pyiceberg_env.sh

if ! command -v uv >/dev/null 2>&1; then
  echo "SKIP: uv not on PATH, cannot build the PyIceberg venv" >&2
  exit 0
fi

VENV="build/pyiceberg-venv"
if [ ! -d "$VENV" ]; then
  uv venv --python 3.12 "$VENV" >/dev/null
  VIRTUAL_ENV="$VENV" uv pip install --quiet "pyiceberg[sql-sqlite,pyarrow]" >/dev/null
fi

VIRTUAL_ENV="$VENV" "$VENV/bin/python" test/pyiceberg_check.py
