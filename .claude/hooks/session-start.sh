#!/bin/bash
set -euo pipefail

# Only run in remote (Claude Code on the web) environments
if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

# Kill any existing server on port 8080
fuser -k 8080/tcp 2>/dev/null || true

# Start HTTP server for static HTML files
cd "${CLAUDE_PROJECT_DIR:-/home/user/OEE}"
nohup python3 -m http.server 8080 > /tmp/http-server.log 2>&1 &

echo "HTTP server started on port 8080"
