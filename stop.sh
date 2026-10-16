#!/usr/bin/env bash
# Stop wa-relay.
# Kalau systemd user service terpasang -> pakai systemctl; kalau tidak -> fallback pidfile/pkill.
set -uo pipefail
cd "$(dirname "$0")"

if systemctl --user cat wa-relay.service >/dev/null 2>&1; then
  echo "stopping via systemd..."
  systemctl --user stop wa-relay-sync.service 2>/dev/null || true
  systemctl --user stop wa-relay.service 2>/dev/null || true
  echo "done"
  exit 0
fi

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
