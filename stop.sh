#!/usr/bin/env bash
# Stop relay + wacli sync milik proyek ini (lewat pidfile, fallback pkill).
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

stop_pidfile relay.pid "wa-relay"
stop_pidfile sync.pid "wacli sync"

pkill -x wa-relay 2>/dev/null || true
pkill -f "wacli sync --follow" 2>/dev/null || true
echo "done"
