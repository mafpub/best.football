#!/bin/bash
# Deploy best.football to ha1 server
set -e

cd "$(dirname "$0")/.."

echo "Building static site..."
uv run python scripts/build_site.py

echo "Syncing htdocs to ha1..."
rsync -avz --delete htdocs/ ha1:/home/bestfootball/htdocs/

echo "Syncing API code..."
rsync -avz api/ ha1:/home/bestfootball/api/
rsync -avz pipeline/ ha1:/home/bestfootball/pipeline/
rsync -avz templates/ ha1:/home/bestfootball/templates/

echo "Syncing database..."
rsync -avz data/best_football.db ha1:/home/bestfootball/data/

echo "Restarting API service..."
ssh ha1 "sudo systemctl restart bestfootball-api"

echo "Deploy complete!"
echo "Site: https://best.football"
echo "API: https://best.football/api/health"
