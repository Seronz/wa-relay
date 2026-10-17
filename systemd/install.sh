#!/usr/bin/env bash
# Pasang wa-relay sebagai systemd user service (always-on, auto-restart, start saat boot).
# Idempotent: aman dijalankan berulang.
set -euo pipefail

REPO=$(cd "$(dirname "$0")/.." && pwd)
DEST="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
mkdir -p "$DEST"

echo "repo : $REPO"
echo "dest : $DEST"

for unit in wa-relay.service wa-relay-sync.service; do
  sed -e "s|__BASE_DIR__|$REPO|g" -e "s|__HOME__|$HOME|g" \
    "$REPO/systemd/${unit}.in" > "$DEST/$unit"
  echo "installed: $DEST/$unit"
done

chmod +x "$REPO/run-sync.sh" 2>/dev/null || true
[ -x "$REPO/wa-relay" ] || (cd "$REPO" && go build -o wa-relay ./cmd/wa-relay)

systemctl --user daemon-reload
systemctl --user enable --now wa-relay.service wa-relay-sync.service

# biar service tetap jalan walau tidak ada sesi login (headless)
if ! loginctl enable-linger "$USER" 2>/dev/null; then
  echo "PERINGATAN: gagal enable-linger, jalankan manual: sudo loginctl enable-linger $USER"
fi

echo "--- status ---"
systemctl --user --no-pager --lines=0 status wa-relay.service wa-relay-sync.service | grep -E "Active:" || true
