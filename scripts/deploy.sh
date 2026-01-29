#!/bin/bash
# Deploy best.football to Cloudflare Pages
set -e

cd "$(dirname "$0")/.."

echo "Building static site..."
uv run python scripts/build_site.py

echo "Deploying to Cloudflare Pages..."
wrangler pages deploy htdocs --project-name=best-football

echo "Deploy complete!"
