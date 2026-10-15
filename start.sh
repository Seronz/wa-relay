#!/usr/bin/env bash
# Idempotent start: restart wacli sync (webhook ke relay) + relay.py.
# Kelola sync di sini; kalau relay saja yang perlu di-restart, jalankan relay.py manual.
set -euo pipefail
cd "$(dirname "$0")"

SECRET=$(python3 -c "import json;print(json.load(open('config.json'))['webhook_secret'])")
PORT=$(python3 -c "import json;print(json.load(open('config.json'))['port'])")
LOG=${LOG_FILE:-relay.log}
SYNC_LOG=${SYNC_LOG:-sync.log}

# (a) kill wacli sync --follow lama untuk store ini (hanya proses sync, bukan wacli lain)
pkill -f "wacli sync --follow" 2>/dev/null || true
sleep 0.5

# (b) kill relay.py lama di port
if [ -f relay.pid ]; then
  kill "$(cat relay.pid)" 2>/dev/null || true
  rm -f relay.pid
fi
OLD=$(ss -ltnp 2>/dev/null | grep ":${PORT} " | grep -oP 'pid=\K[0-9]+' | sort -u || true)
for p in $OLD; do kill "$p" 2>/dev/null || true; done
sleep 0.5

# (c) start relay
nohup python3 relay.py >>"$LOG" 2>&1 &
echo $! > relay.pid
echo "relay pid: $(cat relay.pid)"

# (d) start wacli sync dengan webhook
nohup wacli sync --follow --max-messages 3000 --presence-mode quiet \
  --webhook "http://127.0.0.1:${PORT}/hook" \
  --webhook-secret "$SECRET" \
  --webhook-allow-private \
  --webhook-events message \
  >>"$SYNC_LOG" 2>&1 &
echo $! > sync.pid
echo "sync pid: $(cat sync.pid)"

sleep 1
echo "--- tail $LOG ---"
tail -n 10 "$LOG" || true
echo "--- tail $SYNC_LOG ---"
tail -n 10 "$SYNC_LOG" || true
