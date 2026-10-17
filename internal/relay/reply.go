package relay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"sort"
	"strings"
)

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

// parseSessionID mengambil sessionID dari event NDJSON pertama yang memuatnya.
func parseSessionID(stdout string) string {
	for _, line := range strings.Split(stdout, "\n") {
		line = strings.TrimSpace(line)
		if len(line) < 2 || line[0] != '{' {
			continue
		}
		var ev struct {
			SessionID string `json:"sessionID"`
		}
		if json.Unmarshal([]byte(line), &ev) == nil && ev.SessionID != "" {
			return ev.SessionID
		}
	}
	return ""
}

var thinkingRe = regexp.MustCompile(`(?s)<think(?:ing)?>.*?</think(?:ing)?>`)

var (
	thinkingClosers = []string{"</thinking>", "</think>"}
	thinkingOpeners = []string{"<thinking>", "<think>"}
)

// stripReasoning membuang blok/tag reasoning yang kadang ikut tercampur di dalam
// part teks (mis. "</thinking>" atau "</think>" nyasar), supaya yang terkirim
// hanya balasan asli.
func stripReasoning(s string) string {
	if !strings.Contains(s, "think") {
		return s
	}
	s = thinkingRe.ReplaceAllString(s, "")
	// Tag penutup tanpa pembuka: ambil teks setelah kemunculan terakhir.
	best, tagLen := -1, 0
	for _, tag := range thinkingClosers {
		if i := strings.LastIndex(s, tag); i > best {
			best, tagLen = i, len(tag)
		}
	}
	if best >= 0 {
		s = s[best+tagLen:]
	}
	// Tag pembuka tanpa penutup: buang dari kemunculan pertama sampai akhir.
	best, tagLen = -1, 0
	for _, tag := range thinkingOpeners {
		if i := strings.Index(s, tag); i >= 0 && (best < 0 || i < best) {
			best, tagLen = i, len(tag)
		}
	}
	if best >= 0 {
		s = s[:best]
	}
	return s
}

// extractReply pulls the agent's answer out of NDJSON stdout. It never panics.
func extractReply(stdout string) (result string) {
	defer func() {
		if recover() != nil {
			result = ""
		}
		result = strings.TrimSpace(stripReasoning(result))
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
