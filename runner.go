package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	maxSeen       = 500
	defaultChunk  = 3000
	sendTimeout   = 120 * time.Second
	stderrMaxRune = 400
)

type SeenStore struct {
	mu   sync.Mutex
	ids  []string
	set  map[string]struct{}
	path string
	max  int
}

func LoadSeen(path string, max int) *SeenStore {
	s := &SeenStore{path: path, max: max, set: make(map[string]struct{})}
	if max <= 0 {
		s.max = maxSeen
	}
	data, err := os.ReadFile(path)
	if err == nil {
		var ids []string
		if json.Unmarshal(data, &ids) == nil {
			if len(ids) > s.max {
				ids = ids[len(ids)-s.max:]
			}
			for _, id := range ids {
				if _, dup := s.set[id]; !dup {
					s.ids = append(s.ids, id)
					s.set[id] = struct{}{}
				}
			}
		}
	}
	return s
}

// Seen reports whether id was already recorded. New ids are recorded and persisted.
func (s *SeenStore) Seen(id string) bool {
	s.mu.Lock()
	if _, dup := s.set[id]; dup {
		s.mu.Unlock()
		return true
	}
	s.set[id] = struct{}{}
	s.ids = append(s.ids, id)
	if len(s.ids) > s.max {
		for _, old := range s.ids[:len(s.ids)-s.max] {
			delete(s.set, old)
		}
		s.ids = s.ids[len(s.ids)-s.max:]
	}
	s.mu.Unlock()
	s.persist()
	return false
}

func (s *SeenStore) persist() {
	s.mu.Lock()
	data, err := json.Marshal(s.ids)
	s.mu.Unlock()
	if err != nil {
		return
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return
	}
	os.Rename(tmp, s.path)
}

type taskItem struct {
	ID   string `json:"id"`
	Text string `json:"text"`
}

// TaskRunner runs the agent for a prompt and returns raw stdout/stderr.
type TaskRunner interface {
	Run(ctx context.Context, prompt string) (stdout string, stderr string, err error)
}

type ExecRunner struct {
	Bin     string
	Workdir string
	BaseDir string
	Timeout time.Duration
}

func (r *ExecRunner) Run(ctx context.Context, prompt string) (string, string, error) {
	ctx, cancel := context.WithTimeout(ctx, r.Timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, r.Bin, "run", "--dir", r.Workdir, "--format", "json", "--title", "wa-relay", prompt)
	cmd.Dir = r.BaseDir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if ctx.Err() == context.DeadlineExceeded {
		return stdout.String(), stderr.String(), context.DeadlineExceeded
	}
	return stdout.String(), stderr.String(), err
}

// Sender delivers a reply message to the owner.
type Sender interface {
	Send(text string) error
}

type WacliSender struct {
	Bin   string
	Phone string
}

func (s *WacliSender) Send(text string) error {
	ctx, cancel := context.WithTimeout(context.Background(), sendTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, s.Bin, "send", "text", "--to", s.Phone, "--message", text)
	out, err := cmd.CombinedOutput()
	if err != nil {
		detail := truncateRunes(strings.TrimSpace(string(out)), 300)
		if detail != "" {
			return fmt.Errorf("%v: %s", err, detail)
		}
		return err
	}
	return nil
}

type Relay struct {
	cfg    *Config
	log    *Logger
	seen   *SeenStore
	queue  chan taskItem
	runner TaskRunner
	sender Sender
}

func (r *Relay) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case item, ok := <-r.queue:
			if !ok {
				return
			}
			r.handleMessage(ctx, item)
		}
	}
}

func (r *Relay) handleMessage(ctx context.Context, item taskItem) {
	start := time.Now()
	r.log.Info("task start id=%s text_len=%d", item.ID, len(item.Text))
	if strings.EqualFold(strings.TrimSpace(item.Text), "ping") {
		r.sendChunked("pong")
		r.log.Info("task done id=%s fast-path duration=%.1fs", item.ID, time.Since(start).Seconds())
		return
	}
	prompt := fmt.Sprintf(promptTemplate, strings.TrimSpace(item.Text))
	stdout, stderr, err := r.runner.Run(ctx, prompt)
	if err != nil {
		r.log.Error("opencode failed: %v (stderr %dB)", err, len(stderr))
		r.sendChunked(errorNote(err, stderr))
		r.log.Info("task failed id=%s duration=%.1fs", item.ID, time.Since(start).Seconds())
		return
	}
	r.log.Info("opencode done duration=%.1fs stdout=%dB stderr=%dB", time.Since(start).Seconds(), len(stdout), len(stderr))
	reply := extractReply(stdout)
	r.sendChunked(reply)
	r.log.Info("task done id=%s reply_len=%d duration=%.1fs", item.ID, len(reply), time.Since(start).Seconds())
}

func errorNote(err error, stderr string) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "(error: timeout: agent terlalu lama)"
	}
	var notFound *exec.Error
	if errors.Is(err, exec.ErrNotFound) || errors.As(err, &notFound) {
		return "(error: opencode tidak ditemukan)"
	}
	var exitErr *exec.ExitError
	code := "unknown"
	if errors.As(err, &exitErr) {
		code = fmt.Sprintf("%d", exitErr.ExitCode())
	}
	note := "(error: opencode exit " + code
	if tail := truncateRunes(strings.TrimSpace(stderr), stderrMaxRune); tail != "" {
		note += ": " + tail
	}
	return note + ")"
}

func (r *Relay) sendChunked(text string) {
	text = strings.TrimSpace(text)
	if text == "" {
		text = "(kosong)"
	}
	chunks := chunkText(text, defaultChunk)
	for i, chunk := range chunks {
		err := r.sender.Send(chunk)
		if err != nil {
			r.log.Error("wacli send chunk %d/%d ok=false err=%v", i+1, len(chunks), err)
			if i == 0 && text != "(kosong)" {
				r.sender.Send("(kosong)")
			}
			continue
		}
		r.log.Info("wacli send chunk %d/%d ok=true", i+1, len(chunks))
	}
}

func chunkText(text string, size int) []string {
	if size <= 0 {
		size = defaultChunk
	}
	runes := []rune(text)
	if len(runes) == 0 {
		return []string{"(kosong)"}
	}
	var out []string
	for i := 0; i < len(runes); i += size {
		end := i + size
		if end > len(runes) {
			end = len(runes)
		}
		out = append(out, string(runes[i:end]))
	}
	return out
}

// extractReply pulls the agent's answer out of NDJSON stdout. It never panics.
func extractReply(stdout string) (result string) {
	defer func() {
		if recover() != nil {
			result = ""
		}
	}()
	var parts []string
	var fallbacks []string
	for _, line := range strings.Split(stdout, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var obj map[string]any
		if json.Unmarshal([]byte(line), &obj) != nil {
			continue
		}
		if t, _ := obj["type"].(string); t == "text" {
			if part, ok := obj["part"].(map[string]any); ok {
				if text, ok := part["text"].(string); ok && text != "" {
					parts = append(parts, text)
				}
			}
		}
		fallbacks = append(fallbacks, collectTextFields(obj)...)
	}
	if len(parts) > 0 {
		return strings.TrimSpace(strings.Join(parts, "\n"))
	}
	if len(fallbacks) > 0 {
		return strings.TrimSpace(strings.Join(fallbacks, "\n"))
	}
	lines := strings.Split(stdout, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if strings.TrimSpace(lines[i]) != "" {
			return strings.TrimSpace(lines[i])
		}
	}
	return ""
}

func collectTextFields(v any) []string {
	var out []string
	switch t := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if k == "text" {
				if s, ok := t[k].(string); ok && s != "" {
					out = append(out, s)
					continue
				}
			}
			out = append(out, collectTextFields(t[k])...)
		}
	case []any:
		for _, item := range t {
			out = append(out, collectTextFields(item)...)
		}
	}
	return out
}

func truncateRunes(s string, max int) string {
	runes := []rune(s)
	if len(runes) <= max {
		return s
	}
	return string(runes[:max])
}
