#!/usr/bin/env bash
set -euo pipefail

if (( $# < 1 )); then
  echo "usage: $0 <prompt_path> [workspace]" >&2
  exit 2
fi

PROMPT_PATH="$1"
WORKSPACE="${2:-$(cd "$(dirname "$0")/.." && pwd)}"

cat "$PROMPT_PATH" | codex exec \
  --dangerously-bypass-approvals-and-sandbox \
  -C "$WORKSPACE" \
  -
