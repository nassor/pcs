#!/usr/bin/env bash
# Build the site and rewrite absolute base_url URLs to per-page relative paths
# so the output in public/ can be browsed directly via file://.
#
# Usage:  ./build-local.sh
set -euo pipefail

cd "$(dirname "$0")"

# Use a sentinel base_url that's easy to grep/replace.
SENTINEL="http://__local__"

zola build --base-url "$SENTINEL"

python3 - <<'PY'
import os, re
from pathlib import Path

root = Path("public")
sentinel = "http://__local__/"

for html in root.rglob("*.html"):
    rel_dir = html.parent.relative_to(root)
    depth = len(rel_dir.parts)
    prefix = "./" if depth == 0 else "../" * depth
    text = html.read_text(encoding="utf-8")
    # Replace SENTINEL/path → prefix + path (drops the protocol+host).
    text = text.replace(sentinel, prefix)
    # Also strip a trailing "http://__local__" with no slash (rare).
    text = text.replace("http://__local__", prefix.rstrip("/"))
    html.write_text(text, encoding="utf-8")

print(f"rewrote URLs in {sum(1 for _ in root.rglob('*.html'))} HTML files")
PY

echo
echo "Open: file://$(pwd)/public/index.html"
