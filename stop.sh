#!/usr/bin/env bash
# Stop relay + wacli sync milik proyek ini (via pidfile, fallback pkill).
set -uo pipefail
cd "$(dirname "$0")"

stop_pidfile() {
  local f=$1 name=$2
  if [ -f "$f" ]; then
    kill "$(cat "$f")" 2>/dev/null || true
    rm -f "$f"
    echo "stopped $name"
  fi
}

stop_pidfile relay.pid relay
stop_pidfile sync.pid "wacli sync"

pkill -f "python3 relay.py" 2>/dev/null || true
pkill -f "wacli sync --follow" 2>/dev/null || true
echo "done"
