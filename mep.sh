#!/bin/bash

set -euo pipefail

usage() {
  echo "Usage: $0 -n COUNT [-s SECONDS]"
  exit 1
}

count=""
sleep_seconds=1200

while getopts ":n:s:" opt; do
  case "$opt" in
    n)
      count="$OPTARG"
      ;;
    s)
      sleep_seconds="$OPTARG"
      ;;
    *)
      usage
      ;;
  esac
done

shift $((OPTIND - 1))

if [[ -z "$count" ]]; then
  usage
fi

if ! [[ "$count" =~ ^[1-9][0-9]*$ ]]; then
  echo "COUNT must be a positive integer"
  exit 1
fi

if ! [[ "$sleep_seconds" =~ ^[0-9]+$ ]]; then
  echo "SECONDS must be a non-negative integer"
  exit 1
fi

prompt="Read AGENT_CONDUCTOR.md and continue work from last complete entry. Use uv run."

for ((i = 1; i <= count; i++)); do
  codex exec --dangerously-bypass-approvals-and-sandbox "$prompt"

  if (( i < count )) && (( sleep_seconds > 0 )); then
    sleep "$sleep_seconds"
  fi
done
