# wa-relay

Jembatan lokal: pesan WhatsApp (hanya dari owner) → webhook HTTP lokal → `opencode run` → balasan dikirim balik ke WhatsApp via `wacli send text`. Ditulis dalam **Go** (single binary, tanpa dependensi runtime).

## Cara kerja (alur)

```
Owner kirim pesan WhatsApp
        │
        ▼
wacli sync --follow  ──webhook──►  wa-relay (Go)  (127.0.0.1:8791/hook)
                                      │  verifikasi HMAC, filter owner,
                                      │  dedupe ID, respon 200 segera
                                      ▼
                                  antrean (queue) ──1 worker goroutine──►
                                      │
                                      ▼
                              opencode run --format json
                                      │
                                      ▼
                              parse jawaban agent ──► wacli send text ──► Owner
```

Pesan `ping` (case-insensitive) dibalas `pong` langsung tanpa memanggil agent.

## Spesifikasi payload & signature (wacli)

- Endpoint: `POST /hook`
- Header: `X-Wacli-Signature: sha256=<hex hmac-sha256(raw body, secret)>`
- Payload pesan (flat JSON, tanpa field `EventType`):

```json
{"Chat":"62812xxxxxxx@s.whatsapp.net","ID":"3EB0...","SenderJID":"62812xxxxxxx@s.whatsapp.net","Timestamp":"2026-10-15T10:00:00Z","FromMe":false,"Text":"hi","ChatName":"Owner"}
```

- `Media`: objek atau `null`.
- Receipt punya `EventType":"receipt"`, presence `EventType":"chat_presence"` — keduanya diabaikan.

## Konfigurasi (`config.json`)

| Field | Arti |
|---|---|
| `host` / `port` | Alamat listen relay (default `127.0.0.1:8791`) |
| `webhook_secret` | Secret HMAC untuk verifikasi signature |
| `owner_jid` / `owner_phone` | JID & nomor owner (satu-satunya pengirim yang diproses) |
| `workdir` | `--dir` untuk `opencode run` |
| `opencode_bin` / `wacli_bin` | Path binary |
| `timeout_seconds` | Timeout `opencode run` (default 480) |
| `log_file` | Log file relay |

`opencode.json` berisi permission untuk agent yang di-spawn relay: allow read/edit/glob/grep/list, `external_directory` allow-all, bash allow-all tapi **last-match-wins deny** untuk `rm -rf *`, `sudo *`, `git push *`. Longgarkan/ketatkan sesuai kebutuhan.

## Build & pemakaian

```bash
go build -o wa-relay .   # atau langsung ./start.sh (auto-build kalau binary belum ada)
./start.sh               # idempotent: restart sync + relay, baca secret dari config.json
./stop.sh                # stop keduanya (pidfile + fallback pkill)
go test ./...            # jalankan test
```

Log: `relay.log` (relay) dan `sync.log` (wacli sync). Health check: `curl http://127.0.0.1:8791/health` → `{"ok":true}`.

## Keamanan

- Hanya listen di `127.0.0.1` (localhost).
- Setiap webhook diverifikasi HMAC-SHA256; signature salah → 401.
- Hanya pesan dari `owner_jid`, bukan `FromMe`, bukan grup/channel/status, teks kosong diabaikan.
- Dedupe by message `ID` (500 terakhir, disimpan di `state/seen.json`).
- Policy permission agent di `opencode.json` (bisa diketatkan).

## Mengubah owner / limit

- Owner: edit `owner_jid` + `owner_phone` di `config.json`, lalu `./start.sh`.
- Timeout agent: `timeout_seconds`.
- Secret: ganti `webhook_secret` (mis. `python3 -c "import secrets;print(secrets.token_hex(32))"`) di `config.json` dan di wacli sync — `start.sh` membacanya otomatis dari config.
