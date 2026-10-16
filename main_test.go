package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func testConfig(t *testing.T) *Config {
	t.Helper()
	base := t.TempDir()
	return &Config{
		Host:           "127.0.0.1",
		Port:           8791,
		WebhookSecret:  "topsecret",
		OwnerJID:       "6281234567890@s.whatsapp.net",
		OwnerPhone:     "6281234567890",
		Workdir:        base,
		OpencodeBin:    "opencode",
		WacliBin:       "wacli",
		TimeoutSeconds: 1,
		LogFile:        filepath.Join(base, "relay.log"),
		BaseDir:        base,
	}
}

func newTestRelay(t *testing.T, cfg *Config, runner TaskRunner, sender Sender) *Relay {
	t.Helper()
	log, err := newLogger(io.Discard, filepath.Join(t.TempDir(), "test.log"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(log.Close)
	return &Relay{
		cfg:    cfg,
		log:    log,
		seen:   LoadSeen(filepath.Join(t.TempDir(), "seen.json"), maxSeen),
		queue:  make(chan taskItem, 16),
		runner: runner,
		sender: sender,
	}
}

type stubSender struct {
	mu   sync.Mutex
	sent []string
	fail bool
}

func (s *stubSender) Send(text string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, text)
	if s.fail {
		return errors.New("send failed")
	}
	return nil
}

func (s *stubSender) messages() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.sent...)
}

type stubRunner struct {
	mu     sync.Mutex
	stdout string
	stderr string
	err    error
	calls  int
	prompt string
}

func (s *stubRunner) Run(_ context.Context, prompt string) (string, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.prompt = prompt
	return s.stdout, s.stderr, s.err
}

func signBody(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func postHook(t *testing.T, r *Relay, body []byte, sig string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/hook", bytes.NewReader(body))
	if sig != "" {
		req.Header.Set("X-Wacli-Signature", sig)
	}
	rec := httptest.NewRecorder()
	r.routes().ServeHTTP(rec, req)
	return rec
}

func decodeBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("invalid json response %q: %v", rec.Body.String(), err)
	}
	return out
}

func TestVerifySignature(t *testing.T) {
	secret := "s3cret"
	body := []byte(`{"Chat":"x"}`)
	valid := signBody(secret, body)

	cases := []struct {
		name   string
		secret string
		body   []byte
		header string
		want   bool
	}{
		{"good", secret, body, valid, true},
		{"good without prefix", secret, body, strings.TrimPrefix(valid, "sha256="), true},
		{"good uppercase hex", secret, body, "sha256=" + strings.ToUpper(strings.TrimPrefix(valid, "sha256=")), true},
		{"bad digest", secret, body, "sha256=deadbeef", false},
		{"wrong secret", "other", body, valid, false},
		{"changed body", secret, []byte(`{"Chat":"y"}`), valid, false},
		{"missing header", secret, body, "", false},
		{"empty secret skips check", "", body, "", true},
		{"empty secret ignores bad header", "", body, "sha256=nonsense", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := verifySignature(tc.secret, tc.body, tc.header); got != tc.want {
				t.Fatalf("verifySignature() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestHookRejectsBadSignature(t *testing.T) {
	cfg := testConfig(t)
	r := newTestRelay(t, cfg, &stubRunner{}, &stubSender{})
	body := []byte(`{"Chat":"6281234567890@s.whatsapp.net","ID":"m1","SenderJID":"6281234567890@s.whatsapp.net","Text":"hi"}`)

	rec := postHook(t, r, body, "sha256=deadbeef")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("code = %d, want 401", rec.Code)
	}
	if got := decodeBody(t, rec)["error"]; got != "bad signature" {
		t.Fatalf("error = %v, want bad signature", got)
	}

	if rec := postHook(t, r, body, ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("missing signature code = %d, want 401", rec.Code)
	}
	if rec := postHook(t, r, []byte(`{`), signBody(cfg.WebhookSecret, []byte(`{`))); rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid json code = %d, want 400", rec.Code)
	}
}

func validPayload() map[string]any {
	return map[string]any{
		"Chat":      "6281234567890@s.whatsapp.net",
		"ID":        "MSG1",
		"SenderJID": "6281234567890@s.whatsapp.net",
		"Timestamp": "2026-10-15T10:00:00Z",
		"FromMe":    false,
		"Text":      "halo",
		"ChatName":  "Owner",
		"Media":     nil,
	}
}

func TestHookIgnoreFilters(t *testing.T) {
	cases := []struct {
		name       string
		mutate     func(map[string]any)
		clearOwner bool
		want       string
	}{
		{"event receipt", func(m map[string]any) { m["EventType"] = "receipt" }, false, "event"},
		{"event presence", func(m map[string]any) { m["EventType"] = "chat_presence" }, false, "event"},
		{"from me", func(m map[string]any) { m["FromMe"] = true }, false, "fromme"},
		{"not owner", func(m map[string]any) { m["SenderJID"] = "6289999999999@s.whatsapp.net" }, false, "not-owner"},
		{"group chat", func(m map[string]any) { m["Chat"] = "1234-5678@g.us"; m["SenderJID"] = "1234-5678@g.us" }, true, "group/status"},
		{"newsletter", func(m map[string]any) { m["Chat"] = "1234@newsletter"; m["SenderJID"] = "1234@newsletter" }, true, "group/status"},
		{"status broadcast", func(m map[string]any) { m["Chat"] = "status@broadcast"; m["SenderJID"] = "status@broadcast" }, true, "group/status"},
		{"sender is group", func(m map[string]any) { m["SenderJID"] = "9999@g.us" }, true, "group/status"},
		{"empty text", func(m map[string]any) { m["Text"] = "" }, false, "empty"},
		{"whitespace text", func(m map[string]any) { m["Text"] = "   \n\t " }, false, "empty"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig(t)
			if tc.clearOwner {
				cfg.OwnerJID = ""
			}
			r := newTestRelay(t, cfg, &stubRunner{}, &stubSender{})
			payload := validPayload()
			tc.mutate(payload)
			body, err := json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
			rec := postHook(t, r, body, signBody(cfg.WebhookSecret, body))
			if rec.Code != http.StatusOK {
				t.Fatalf("code = %d, want 200", rec.Code)
			}
			if got := decodeBody(t, rec)["ignored"]; got != tc.want {
				t.Fatalf("ignored = %v, want %v", got, tc.want)
			}
			if len(r.queue) != 0 {
				t.Fatal("nothing should be queued")
			}
		})
	}
}

func TestHookQueuesValidMessageAndDedupes(t *testing.T) {
	cfg := testConfig(t)
	r := newTestRelay(t, cfg, &stubRunner{}, &stubSender{})
	payload := validPayload()
	payload["Text"] = "  halo dunia  "
	payload["EventType"] = "message"
	body, _ := json.Marshal(payload)

	rec := postHook(t, r, body, signBody(cfg.WebhookSecret, body))
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200", rec.Code)
	}
	got := decodeBody(t, rec)
	if got["queued"] != true || got["ok"] != true {
		t.Fatalf("response = %v, want queued true", got)
	}
	select {
	case item := <-r.queue:
		if item.ID != "MSG1" || item.Text != "halo dunia" {
			t.Fatalf("queued item = %+v", item)
		}
	default:
		t.Fatal("expected an item in queue")
	}

	rec = postHook(t, r, body, signBody(cfg.WebhookSecret, body))
	if got := decodeBody(t, rec)["ignored"]; got != "duplicate" {
		t.Fatalf("second post ignored = %v, want duplicate", got)
	}
}

func TestHealthEndpoint(t *testing.T) {
	r := newTestRelay(t, testConfig(t), &stubRunner{}, &stubSender{})
	rec := httptest.NewRecorder()
	r.routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200", rec.Code)
	}
	if got := decodeBody(t, rec)["ok"]; got != true {
		t.Fatalf("ok = %v, want true", got)
	}

	rec = httptest.NewRecorder()
	r.routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/nope", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("not found code = %d, want 404", rec.Code)
	}
}

func TestSeenStorePersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seen.json")
	s := LoadSeen(path, maxSeen)
	if s.Seen("m1") {
		t.Fatal("m1 should be new")
	}
	if !s.Seen("m1") {
		t.Fatal("m1 should be duplicate")
	}
	if s.Seen("m2") {
		t.Fatal("m2 should be new")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("seen file not written: %v", err)
	}
	var ids []string
	if err := json.Unmarshal(data, &ids); err != nil {
		t.Fatalf("seen file invalid: %v", err)
	}
	if len(ids) != 2 || ids[0] != "m1" || ids[1] != "m2" {
		t.Fatalf("ids = %v, want [m1 m2]", ids)
	}

	reloaded := LoadSeen(path, maxSeen)
	if !reloaded.Seen("m1") {
		t.Fatal("m1 should still be duplicate after reload")
	}
	if reloaded.Seen("m3") {
		t.Fatal("m3 should be new")
	}
}

func TestSeenStoreKeepsLast500(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seen.json")
	s := LoadSeen(path, maxSeen)
	for i := 0; i < 600; i++ {
		s.Seen(fmt.Sprintf("x%d", i))
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	if err := json.Unmarshal(data, &ids); err != nil {
		t.Fatal(err)
	}
	if len(ids) != maxSeen {
		t.Fatalf("persisted len = %d, want %d", len(ids), maxSeen)
	}
	if ids[0] != "x100" || ids[len(ids)-1] != "x599" {
		t.Fatalf("window = [%s..%s], want [x100..x599]", ids[0], ids[len(ids)-1])
	}
	if len(s.ids) != maxSeen {
		t.Fatalf("memory len = %d, want %d", len(s.ids), maxSeen)
	}
	if !s.Seen("x599") || s.Seen("x600") {
		t.Fatal("dedupe window incorrect after eviction")
	}
}

func TestChunkText(t *testing.T) {
	cases := []struct {
		name string
		text string
		want []int
	}{
		{"short", "abc", []int{3}},
		{"exact", strings.Repeat("a", 3000), []int{3000}},
		{"one over", strings.Repeat("a", 3001), []int{3000, 1}},
		{"7000", strings.Repeat("a", 7000), []int{3000, 3000, 1000}},
		{"empty", "", []int{8}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			chunks := chunkText(tc.text, defaultChunk)
			if len(chunks) != len(tc.want) {
				t.Fatalf("got %d chunks %v, want %v", len(chunks), chunks, tc.want)
			}
			for i, want := range tc.want {
				if len([]rune(chunks[i])) != want {
					t.Fatalf("chunk %d len = %d, want %d", i, len([]rune(chunks[i])), want)
				}
			}
		})
	}
	if got := chunkText("", defaultChunk); len(got) != 1 || got[0] != "(kosong)" {
		t.Fatalf("empty text = %v, want [(kosong)]", got)
	}
	// Multi-byte runes must not be split.
	chunks := chunkText(strings.Repeat("é", 3001), defaultChunk)
	if len(chunks) != 2 || len([]rune(chunks[0])) != 3000 || chunks[1] != "é" {
		t.Fatalf("multi-byte chunks = %d (%d runes first)", len(chunks), len([]rune(chunks[0])))
	}
}

func TestExtractReply(t *testing.T) {
	cases := []struct {
		name   string
		stdout string
		want   string
	}{
		{"real format", `{"type":"text","part":{"text":"OK"}}`, "OK"},
		{
			"multiple text parts with noise",
			"{\"type\":\"step-start\",\"part\":{}}\n" +
				"{\"type\":\"text\",\"part\":{\"text\":\"baris satu\"}}\n" +
				"{\"type\":\"tool\",\"part\":{\"text\":\"abaikan\"}}\n" +
				"{\"type\":\"text\",\"part\":{\"text\":\"baris dua\"}}\n",
			"baris satu\nbaris dua",
		},
		{"fallback text field", `{"msg":"x","text":"jawaban cadangan"}`, "jawaban cadangan"},
		{"fallback nested text field", `{"data":{"text":"nested"}}`, "nested"},
		{"messy fallback", "bukan json\n\n   \njawaban terakhir  \n", "jawaban terakhir"},
		{"empty", "", ""},
		{"only invalid json", "{not json}\nstill not json\n", "still not json"},
		{"text type without part text falls back", `{"type":"text","part":{}}` + "\n" + `{"type":"log","text":"lain"}`, "lain"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := extractReply(tc.stdout); got != tc.want {
				t.Fatalf("extractReply() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestWorkerPingFastPath(t *testing.T) {
	runner := &stubRunner{}
	sender := &stubSender{}
	r := newTestRelay(t, testConfig(t), runner, sender)

	r.handleMessage(context.Background(), taskItem{ID: "1", Text: "  PiNg "})
	if runner.calls != 0 {
		t.Fatalf("runner calls = %d, want 0", runner.calls)
	}
	got := sender.messages()
	if len(got) != 1 || got[0] != "pong" {
		t.Fatalf("sent = %v, want [pong]", got)
	}
}

func TestWorkerAgentReply(t *testing.T) {
	runner := &stubRunner{stdout: `{"type":"text","part":{"text":"halo dari agent"}}`}
	sender := &stubSender{}
	r := newTestRelay(t, testConfig(t), runner, sender)

	r.handleMessage(context.Background(), taskItem{ID: "2", Text: "apa kabar"})
	got := sender.messages()
	if len(got) != 1 || got[0] != "halo dari agent" {
		t.Fatalf("sent = %v", got)
	}
	if runner.calls != 1 {
		t.Fatalf("runner calls = %d, want 1", runner.calls)
	}
	if !strings.Contains(runner.prompt, "apa kabar") || !strings.Contains(runner.prompt, "WhatsApp") {
		t.Fatalf("prompt not wrapped as expected: %q", runner.prompt)
	}
}

func TestWorkerEmptyReplySendsKosong(t *testing.T) {
	sender := &stubSender{}
	r := newTestRelay(t, testConfig(t), &stubRunner{stdout: ""}, sender)
	r.handleMessage(context.Background(), taskItem{ID: "3", Text: "kosong"})
	got := sender.messages()
	if len(got) != 1 || got[0] != "(kosong)" {
		t.Fatalf("sent = %v, want [(kosong)]", got)
	}
}

func TestWorkerRunErrorSendsErrorNote(t *testing.T) {
	sender := &stubSender{}
	r := newTestRelay(t, testConfig(t), &stubRunner{err: errors.New("boom"), stderr: "agent gagal"}, sender)
	r.handleMessage(context.Background(), taskItem{ID: "4", Text: "error"})
	got := sender.messages()
	if len(got) != 1 || !strings.HasPrefix(got[0], "(error:") {
		t.Fatalf("sent = %v, want an error note", got)
	}
}

func TestWorkerLongReplyIsChunked(t *testing.T) {
	long := strings.Repeat("z", 7000)
	runner := &stubRunner{stdout: fmt.Sprintf(`{"type":"text","part":{"text":%q}}`, long)}
	sender := &stubSender{}
	r := newTestRelay(t, testConfig(t), runner, sender)
	r.handleMessage(context.Background(), taskItem{ID: "5", Text: "panjang"})
	got := sender.messages()
	if len(got) != 3 {
		t.Fatalf("sent %d chunks, want 3", len(got))
	}
	if len([]rune(got[0])) != 3000 || len([]rune(got[1])) != 3000 || len([]rune(got[2])) != 1000 {
		t.Fatalf("chunk sizes = %d/%d/%d", len([]rune(got[0])), len([]rune(got[1])), len([]rune(got[2])))
	}
}

func TestExecRunnerArgsIncludesAgent(t *testing.T) {
	withAgent := strings.Join((&ExecRunner{Workdir: "/w", Agent: "stepi"}).args("halo"), " ")
	wantWith := "run --dir /w --format json --title wa-relay --agent stepi halo"
	if withAgent != wantWith {
		t.Fatalf("args = %q, want %q", withAgent, wantWith)
	}

	withoutAgent := strings.Join((&ExecRunner{Workdir: "/w"}).args("halo"), " ")
	wantWithout := "run --dir /w --format json --title wa-relay halo"
	if withoutAgent != wantWithout {
		t.Fatalf("args = %q, want %q", withoutAgent, wantWithout)
	}
}

func TestExecRunnerCapturesStdout(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "fake-opencode")
	if err := os.WriteFile(script, []byte("#!/bin/sh\necho '{\"type\":\"text\",\"part\":{\"text\":\"OK\"}}'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	runner := &ExecRunner{Bin: script, Workdir: dir, BaseDir: dir, Timeout: 5 * time.Second}
	stdout, _, err := runner.Run(context.Background(), "hi")
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got := extractReply(stdout); got != "OK" {
		t.Fatalf("extractReply = %q, want OK", got)
	}
}

func TestExecRunnerNotFound(t *testing.T) {
	runner := &ExecRunner{Bin: "definitely-not-a-real-binary-xyz", Timeout: time.Second}
	if _, _, err := runner.Run(context.Background(), "hi"); !errors.Is(err, exec.ErrNotFound) {
		t.Fatalf("err = %v, want exec.ErrNotFound", err)
	}
}

func TestExecRunnerTimeoutKillsProcessGroup(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "fake-opencode")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nsleep 30\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	runner := &ExecRunner{Bin: script, Workdir: dir, BaseDir: dir, Timeout: 200 * time.Millisecond}
	start := time.Now()
	_, _, err := runner.Run(context.Background(), "hi")
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("timeout took %s, process group not killed", elapsed)
	}
}

func TestLoggerNoDuplicateWhenStdoutIsSameFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "relay.log")
	out, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()

	log, err := newLogger(out, path)
	if err != nil {
		t.Fatal(err)
	}
	log.Info("halo")
	log.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 1 {
		t.Fatalf("log lines = %d, want 1 (tidak boleh ganda): %q", len(lines), string(data))
	}
	if !strings.Contains(lines[0], "halo") {
		t.Fatalf("log tidak berisi pesan: %q", string(data))
	}
}

func TestLoggerWritesToBothWhenDifferentTargets(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "relay.log")
	outPath := filepath.Join(dir, "stdout.log")
	out, err := os.OpenFile(outPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}

	log, err := newLogger(out, logPath)
	if err != nil {
		t.Fatal(err)
	}
	log.Info("ke-dua")
	log.Close()
	out.Close()

	for _, p := range []string{logPath, outPath} {
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("baca %s: %v", p, err)
		}
		if !strings.Contains(string(data), "ke-dua") {
			t.Fatalf("%s tidak berisi log: %q", p, string(data))
		}
	}
}
