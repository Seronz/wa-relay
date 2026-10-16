package relay

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

type hookPayload struct {
	Chat      string          `json:"Chat"`
	ID        string          `json:"ID"`
	SenderJID string          `json:"SenderJID"`
	Timestamp string          `json:"Timestamp"`
	FromMe    bool            `json:"FromMe"`
	Text      string          `json:"Text"`
	ChatName  string          `json:"ChatName"`
	Media     json.RawMessage `json:"Media"`
	EventType string          `json:"EventType"`
}

func verifySignature(secret string, body []byte, header string) bool {
	if secret == "" {
		return true
	}
	header = strings.TrimSpace(header)
	if header == "" {
		return false
	}
	header = strings.TrimPrefix(header, "sha256=")
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	expected := hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(expected), []byte(strings.ToLower(header)))
}

func isBlockedJID(jid string) bool {
	return strings.HasSuffix(jid, "@g.us") ||
		strings.HasSuffix(jid, "@newsletter") ||
		jid == "status@broadcast"
}

// shouldIgnore returns the reason (mirroring the Python relay) when a payload
// must not be processed.
func shouldIgnore(cfg *Config, msg *hookPayload) (string, bool) {
	if msg.EventType != "" && msg.EventType != "message" {
		return "event", true
	}
	if msg.FromMe {
		return "fromme", true
	}
	sender := msg.SenderJID
	if sender == "" {
		sender = msg.Chat
	}
	if cfg.OwnerJID != "" && sender != cfg.OwnerJID {
		return "not-owner", true
	}
	if isBlockedJID(msg.Chat) || isBlockedJID(sender) {
		return "group/status", true
	}
	if strings.TrimSpace(msg.Text) == "" {
		return "empty", true
	}
	return "", false
}

func respond(w http.ResponseWriter, code int, payload map[string]any) {
	body, err := json.Marshal(payload)
	if err != nil {
		http.Error(w, `{"ok":false,"error":"internal"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(body)
}

// Routes mengembalikan handler HTTP untuk endpoint relay.
func (r *Relay) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		respond(w, http.StatusOK, map[string]any{"ok": true})
	})
	mux.HandleFunc("POST /hook", r.handleHook)
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		respond(w, http.StatusNotFound, map[string]any{"ok": false, "error": "not found"})
	})
	return mux
}

func (r *Relay) handleHook(w http.ResponseWriter, req *http.Request) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		respond(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "read body"})
		return
	}
	if !verifySignature(r.cfg.WebhookSecret, body, req.Header.Get("X-Wacli-Signature")) {
		r.log.Warn("bad signature from %s", req.RemoteAddr)
		respond(w, http.StatusUnauthorized, map[string]any{"ok": false, "error": "bad signature"})
		return
	}
	var msg hookPayload
	if err := json.Unmarshal(body, &msg); err != nil {
		respond(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "invalid json"})
		return
	}
	if reason, ok := shouldIgnore(r.cfg, &msg); ok {
		respond(w, http.StatusOK, map[string]any{"ok": true, "ignored": reason})
		return
	}
	if msg.ID == "" || r.seen.Seen(msg.ID) {
		respond(w, http.StatusOK, map[string]any{"ok": true, "ignored": "duplicate"})
		return
	}
	item := taskItem{ID: msg.ID, Text: strings.TrimSpace(msg.Text), Chat: chatTarget(r.cfg, &msg), Lane: Classify(r.cfg, msg.Text)}
	select {
	case r.chatQueue <- item:
		r.log.Info("queued id=%s len=%d lane=chat", item.ID, len(item.Text))
		respond(w, http.StatusOK, map[string]any{"ok": true, "queued": true})
	case r.workQueue <- item:
		r.log.Info("queued id=%s len=%d lane=work", item.ID, len(item.Text))
		respond(w, http.StatusOK, map[string]any{"ok": true, "queued": true})
	case r.queue <- item:
		r.log.Info("queued id=%s len=%d lane=unified", item.ID, len(item.Text))
		respond(w, http.StatusOK, map[string]any{"ok": true, "queued": true})
	default:
		respond(w, http.StatusOK, map[string]any{"ok": true, "ignored": "queue-full"})
	}
}

// chatTarget memilih identifier chat untuk mark-read: chat asli pesan bila
// ada, lalu sender, lalu fallback ke identitas owner dari config. Hasilnya
// dibawa lewat taskItem.Chat dan dipakai handleMessage tepat sebelum balasan
// final dikirim.
func chatTarget(cfg *Config, msg *hookPayload) string {
	switch {
	case msg.Chat != "":
		return msg.Chat
	case msg.SenderJID != "":
		return msg.SenderJID
	case cfg.OwnerJID != "":
		return cfg.OwnerJID
	default:
		return cfg.OwnerPhone
	}
}
