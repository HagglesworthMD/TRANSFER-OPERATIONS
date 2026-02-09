#!/usr/bin/env bash
cd "$(dirname "$0")"
echo "Starting Transfer-Bot Dashboard..."
echo "Open http://localhost:8050 in your browser"
echo
python -m backend.server
