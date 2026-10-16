#!/usr/bin/env bash
# Wrapper untuk menjalankan `wacli sync --follow` dengan secret & port dari config.json.
# Dipakai oleh systemd user service (wa-relay-sync.service). Jalankan di foreground.
set -euo pipefail
cd "$(dirname "$0")"

CONFIG=${CONFIG:-config.json}
PYTHON=${PYTHON:-/usr/bin/python3}
WACLI=${WACLI:-/home/linuxbrew/.linuxbrew/bin/wacli}

secret=$("$PYTHON" -c "import json;print(json.load(open('$CONFIG')).get('webhook_secret',''))")
port=$("$PYTHON" -c "import json;print(json.load(open('$CONFIG')).get('port',8791))")

exec "$WACLI" sync --follow --max-messages 3000 --presence-mode quiet \
  --webhook "http://127.0.0.1:${port}/hook" \
  --webhook-secret "$secret" \
  --webhook-allow-private \
  --webhook-events message
