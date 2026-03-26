#!/usr/bin/env bash
set -euo pipefail

CMD='codex exec --yolo "read AGENT_CONDUCTOR.md and working through the list of sites for scraper creation using subagents. do another 10 entries."'

cycles=20
interval=1800  # 30 minutes in seconds

for ((i=1; i<=cycles; i++)); do
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Cycle $i/$cycles: running command"
  eval "$CMD"

  if (( i < cycles )); then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Sleeping for 30 minutes"
    sleep "$interval"
  fi
done

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Completed $cycles cycles"
