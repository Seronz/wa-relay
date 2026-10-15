#!/usr/bin/env bash
# Idempotent start: build + jalankan wa-relay (Go) dan wacli sync --follow (webhook ke relay).
# Semua proses dilepas (setsid + nohup + </dev/null) agar shell pemanggil tidak ikut tertahan.
set -euo pipefail
cd "$(dirname "$0")"

SECRET=$(python3 -c "import json;print(json.load(open('config.json')).get('webhook_secret',''))")
PORT=$(python3 -c "import json;print(json.load(open('config.json')).get('port',8791))")
LOG=${LOG_FILE:-relay.log}
SYNC_LOG=${SYNC_LOG:-sync.log}

# bangun binary jika belum ada
if [ ! -x ./wa-relay ]; then
  echo "building wa-relay..."
  go build -o wa-relay .
fi

# (a) hentikan wacli sync --follow lama untuk store ini
if [ -f sync.pid ]; then
  kill "$(cat sync.pid)" 2>/dev/null || true
  rm -f sync.pid
fi
pkill -f "wacli sync --follow" 2>/dev/null || true
sleep 0.5

# (b) hentikan relay lama (pidfile, lalu pemakai port)
if [ -f relay.pid ]; then
  kill "$(cat relay.pid)" 2>/dev/null || true
  rm -f relay.pid
fi
OLD=$(ss -ltnp 2>/dev/null | grep ":${PORT} " | grep -oP 'pid=\K[0-9]+' | sort -u || true)
for p in $OLD; do kill "$p" 2>/dev/null || true; done
sleep 0.5

# (c) jalankan relay
setsid nohup ./wa-relay >>"$LOG" 2>&1 </dev/null &
echo $! > relay.pid
echo "relay pid: $(cat relay.pid)"

# (d) jalankan wacli sync dengan webhook
setsid nohup wacli sync --follow --max-messages 3000 --presence-mode quiet \
  --webhook "http://127.0.0.1:${PORT}/hook" \
  --webhook-secret "$SECRET" \
  --webhook-allow-private \
  --webhook-events message \
  >>"$SYNC_LOG" 2>&1 </dev/null &
echo $! > sync.pid
echo "sync pid: $(cat sync.pid)"

sleep 1
echo "--- tail $LOG ---"
tail -n 10 "$LOG" || true
echo "--- tail $SYNC_LOG ---"
tail -n 10 "$SYNC_LOG" || true
