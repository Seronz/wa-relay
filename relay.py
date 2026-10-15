#!/usr/bin/env python3
"""wa-relay: WhatsApp (owner) -> local HTTP hook -> opencode run -> reply to WhatsApp."""

import hashlib
import hmac
import http.server
import json
import logging
import os
import queue
import signal
import subprocess
import sys
import threading
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(BASE_DIR, "config.json"), encoding="utf-8") as f:
    CFG = json.load(f)

PORT = int(CFG.get("port", 8791))
HOST = CFG.get("host", "127.0.0.1")
SECRET = CFG.get("webhook_secret", "")
OWNER_JID = CFG.get("owner_jid", "")
OWNER_PHONE = CFG.get("owner_phone", "")
WORKDIR = CFG.get("workdir", BASE_DIR)
OPENCODE_BIN = CFG.get("opencode_bin", "opencode")
WACLI_BIN = CFG.get("wacli_bin", "wacli")
TIMEOUT = int(CFG.get("timeout_seconds", 480))
LOG_FILE = CFG.get("log_file", os.path.join(BASE_DIR, "relay.log"))
STATE_DIR = os.path.join(BASE_DIR, "state")
SEEN_PATH = os.path.join(STATE_DIR, "seen.json")
MAX_SEEN = 500
CHUNK_SIZE = 3000

PROMPT_TEMPLATE = (
    "Ini tugas otomatis dari WhatsApp relay untuk pemilikmu (owner). "
    "Pesan masuk dari owner:\n---\n{msg}\n---\n"
    "Jawaban akhirmu akan dikirim balik ke WhatsApp owner. "
    "Akhiri dengan balasan singkat berupa teks biasa dalam bahasa Indonesia santai, "
    "tanpa judul markdown, tanpa tabel, tanpa fence kode."
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(LOG_FILE, encoding="utf-8")],
)
log = logging.getLogger("wa-relay")

seen_lock = threading.Lock()
seen_ids = []

if os.path.isfile(SEEN_PATH):
    try:
        with open(SEEN_PATH, encoding="utf-8") as f:
            seen_ids = list(json.load(f))
    except Exception:
        seen_ids = []

shutdown_event = threading.Event()
msg_queue = queue.Queue()


def persist_seen():
    with seen_lock:
        data = list(seen_ids[-MAX_SEEN:])
    tmp = SEEN_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.replace(tmp, SEEN_PATH)


def is_duplicate(msg_id):
    with seen_lock:
        if msg_id in seen_ids:
            return True
        seen_ids.append(msg_id)
        del seen_ids[:-MAX_SEEN]
    threading.Thread(target=persist_seen, daemon=True).start()
    return False


def verify_signature(body: bytes, header: str) -> bool:
    if not SECRET:
        return True
    if not header:
        return False
    if header.startswith("sha256="):
        header = header[len("sha256="):]
    expected = hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, header.strip().lower())


def extract_reply(stdout: str) -> str:
    parts = []
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            ev = json.loads(line)
        except Exception:
            continue
        if ev.get("type") == "text":
            text = (ev.get("part") or {}).get("text", "")
            if text:
                parts.append(text)
    if parts:
        return "\n".join(parts).strip()
    # fallback: last non-empty chunk of stdout
    for line in reversed(stdout.splitlines()):
        if line.strip():
            return line.strip()
    return ""


def run_opencode(prompt: str):
    cmd = [OPENCODE_BIN, "run", "--dir", WORKDIR, "--format", "json", "--title", "wa-relay", prompt]
    start = time.time()
    try:
        proc = subprocess.run(
            cmd,
            cwd=BASE_DIR,
            capture_output=True,
            text=True,
            timeout=TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        log.error("opencode timeout after %ss", TIMEOUT)
        return None, "(timeout: agent terlalu lama)"
    except FileNotFoundError:
        log.error("binary not found: %s", OPENCODE_BIN)
        return None, "(error: opencode tidak ditemukan)"
    dur = time.time() - start
    log.info("opencode done exit=%s duration=%.1fs stdout=%dB stderr=%dB",
             proc.returncode, dur, len(proc.stdout or ""), len(proc.stderr or ""))
    if proc.returncode != 0:
        err = (proc.stderr or "").strip()[-400:]
        return None, f"(error: opencode exit {proc.returncode}" + (f": {err})" if err else ")")
    return proc.stdout, None


def wa_send(text: str) -> bool:
    try:
        proc = subprocess.run(
            [WACLI_BIN, "send", "text", "--to", OWNER_PHONE, "--message", text],
            capture_output=True,
            text=True,
            timeout=120,
        )
    except Exception as e:
        log.error("wacli send failed: %s", e)
        return False
    if proc.returncode != 0:
        log.error("wacli send exit=%s stderr=%s", proc.returncode, (proc.stderr or "").strip()[:300])
        return False
    return True


def send_chunked(text: str):
    text = text.strip() or "(kosong)"
    chunks = [text[i:i + CHUNK_SIZE] for i in range(0, len(text), CHUNK_SIZE)] or ["(kosong)"]
    for i, chunk in enumerate(chunks):
        ok = wa_send(chunk)
        log.info("wacli send chunk %d/%d ok=%s", i + 1, len(chunks), ok)
        if not ok and i == 0 and not text.strip() == "(kosong)":
            wa_send("(kosong)")


def handle_message(msg_id: str, text: str):
    start = time.time()
    log.info("task start id=%s text_len=%d", msg_id, len(text))
    if text.strip().lower() == "ping":
        send_chunked("pong")
        log.info("task done id=%s fast-path duration=%.1fs", msg_id, time.time() - start)
        return
    stdout, err_or_note = run_opencode(PROMPT_TEMPLATE.format(msg=text.strip()))
    if stdout is None:
        send_chunked(err_or_note)
        log.info("task failed id=%s duration=%.1fs", msg_id, time.time() - start)
        return
    reply = extract_reply(stdout)
    send_chunked(reply)
    log.info("task done id=%s reply_len=%d duration=%.1fs", msg_id, len(reply), time.time() - start)


def worker():
    while not shutdown_event.is_set():
        try:
            item = msg_queue.get(timeout=0.5)
        except queue.Empty:
            continue
        try:
            handle_message(item["id"], item["text"])
        except Exception:
            log.exception("worker error on id=%s", item.get("id"))
        finally:
            msg_queue.task_done()


class HookHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _respond(self, code: int, payload: dict):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        try:
            if self.path == "/health":
                self._respond(200, {"ok": True})
            else:
                self._respond(404, {"ok": False, "error": "not found"})
        except Exception:
            log.exception("GET handler error")

    def do_POST(self):
        try:
            self._handle_post()
        except Exception:
            log.exception("POST handler error")
            try:
                self._respond(500, {"ok": False, "error": "internal"})
            except Exception:
                pass

    def _handle_post(self):
        if self.path != "/hook":
            self._respond(404, {"ok": False, "error": "not found"})
            return
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        if not verify_signature(body, self.headers.get("X-Wacli-Signature", "")):
            log.warning("bad signature from %s", self.client_address[0])
            self._respond(401, {"ok": False, "error": "bad signature"})
            return
        try:
            msg = json.loads(body)
        except Exception:
            self._respond(400, {"ok": False, "error": "invalid json"})
            return

        # ignore receipts / presence / anything non-message
        if msg.get("EventType") not in (None, ""):
            self._respond(200, {"ok": True, "ignored": "event"})
            return
        if msg.get("FromMe"):
            self._respond(200, {"ok": True, "ignored": "fromme"})
            return
        sender = msg.get("SenderJID") or msg.get("Chat") or ""
        chat = msg.get("Chat") or ""
        if OWNER_JID and sender != OWNER_JID:
            self._respond(200, {"ok": True, "ignored": "not-owner"})
            return
        if chat.endswith("@g.us") or chat.endswith("@newsletter") or chat == "status@broadcast":
            self._respond(200, {"ok": True, "ignored": "group/status"})
            return
        text = (msg.get("Text") or "").strip()
        if not text:
            self._respond(200, {"ok": True, "ignored": "empty"})
            return
        msg_id = msg.get("ID") or ""
        if not msg_id or is_duplicate(msg_id):
            self._respond(200, {"ok": True, "ignored": "duplicate"})
            return

        msg_queue.put({"id": msg_id, "text": text})
        log.info("queued id=%s chat=%s len=%d", msg_id, chat, len(text))
        self._respond(200, {"ok": True, "queued": True})

    def log_message(self, fmt, *args):
        log.debug("http %s", fmt % args)


def main():
    os.makedirs(STATE_DIR, exist_ok=True)
    t = threading.Thread(target=worker, daemon=True, name="relay-worker")
    t.start()

    server = http.server.ThreadingHTTPServer((HOST, PORT), HookHandler)

    def shutdown(sig, frame):
        log.info("signal %s: shutting down", sig)
        shutdown_event.set()
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    log.info("wa-relay listening on http://%s:%s/hook workdir=%s owner=%s", HOST, PORT, WORKDIR, OWNER_JID)
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        shutdown_event.set()
        server.server_close()
        try:
            persist_seen()
        except Exception:
            pass
        log.info("stopped")


if __name__ == "__main__":
    main()
