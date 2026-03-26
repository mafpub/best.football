#!/bin/bash
# Deploy best.football to ha1 server
set -e

cd "$(dirname "$0")/.."

SKIP_BUILD=0
if [[ "${1:-}" == "--skip-build" ]]; then
  SKIP_BUILD=1
  shift
fi

if [[ $SKIP_BUILD -eq 0 ]]; then
  echo "Building static site..."
  uv run python scripts/build_site.py
else
  echo "Skipping local build; syncing existing artifacts..."
fi

echo "Syncing htdocs to ha1..."
rsync -avz --delete htdocs/ ha1:/home/bestfootball/htdocs/

echo "Syncing API code..."
rsync -avz api/ ha1:/home/bestfootball/api/
rsync -avz pipeline/ ha1:/home/bestfootball/pipeline/
rsync -avz templates/ ha1:/home/bestfootball/templates/

echo "Syncing dependencies..."
rsync -avz pyproject.toml uv.lock ha1:/home/bestfootball/

echo "Syncing database..."
rsync -avz data/best_football.db ha1:/home/bestfootball/data/

echo "Installing dependencies on server..."
ssh ha1 "cd /home/bestfootball && /home/bestfootball/.local/bin/uv sync"

echo "Restarting API service..."
ssh ha1 "sudo systemctl restart bestfootball-api"

echo "Deploy complete!"
echo "Site: https://best.football"
echo "API: https://best.football/api/health"
