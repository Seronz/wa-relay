package relay

import (
	"strings"
	"sync"
	"unicode/utf8"
)

// Lane menentukan kanal (antrean + sesi) untuk satu pesan masuk.
type Lane string

const (
	// LaneChat mewakili chat WhatsApp ringan. Dijalankan pada sesi chat
	// miliknya sendiri (chat_session_file, default state/session.json) lewat
	// worker khusus, supaya ingatan percakapan tetap utuh dan balasan tidak
	// pernah menunggu tugas berat.
	LaneChat Lane = "chat"
	// LaneWork mewakili tugas berat (coding/eksekusi). Dijalankan pada
	// sesi/worker terpisah (work_session_file, atau sesi baru tiap task bila
	// tidak diset), sehingga tidak pernah menyentuh sesi chat.
	LaneWork Lane = "work"
)

// defaultHeavyKeywords memicu routing ke lane work. Dipilih dari istilah
// coding/eksekusi (EN+ID) supaya pesan obrolan santai tetap di lane chat dan
// tidak kehilangan ingatan percakapan karena salah klasifikasi.
var defaultHeavyKeywords = []string{
	"fix", "bug", "bugfix", "debug", "debugging", "debugger", "error", "crash",
	"exception", "traceback", "stacktrace", "stack trace", "hotfix", "patch",
	"refactor", "rebase", "rollback",
	"kode", "koding", "coding", "code", "script", "program", "programming",
	"function", "fungsi", "method", "class", "variable", "variabel", "syntax", "sintaks",
	"deploy", "deployment", "build", "compile", "kompilasi",
	"test", "testing", "install", "setup", "dependency", "dependensi",
	"commit", "push", "pull", "merge", "branch", "git", "repo", "repository",
	"server", "backend", "frontend", "docker", "container", "kubernetes", "nginx",
	"database", "query", "migration", "endpoint", "api",
	"config", "konfigurasi", "environment",
	"latency", "performa", "performance", "optimasi", "optimization",
	"file", "folder", "directory", "direktori",
	"python", "golang", "nodejs", "npm", "rust", "java", "typescript", "javascript",
	"readme", "dokumentasi", "issue", "relay", "webhook",
}

// Classify menentukan lane sebuah pesan: ping//reset selalu chat; pesan yang
// melebihi light_max_len atau mengandung kata kunci berat (lihat
// defaultHeavyKeywords + config heavy_keywords) -> work; sisanya chat.
// Klasifikasi dilakukan saat pesan masuk (hook) supaya pesan ringan langsung
// masuk antrean chat sendiri dan tidak antre di belakang tugas berat.
func Classify(cfg *Config, text string) Lane {
	t := strings.TrimSpace(text)
	if t == "" || isFastCommand(t) {
		return LaneChat
	}
	maxLen := cfg.LightMaxLen
	if maxLen <= 0 {
		maxLen = defaultLightMaxLen
	}
	if utf8.RuneCountInString(t) > maxLen {
		return LaneWork
	}
	if re := cfg.HeavyPattern(); re != nil && re.MatchString(t) {
		return LaneWork
	}
	return LaneChat
}

// isFastCommand melaporkan apakah teks adalah perintah fast-path (tanpa
// pemanggilan agent) yang ditangani langsung oleh handleMessage.
func isFastCommand(t string) bool {
	return strings.EqualFold(t, "ping") || t == "/reset"
}

// sessionGate mengunci sesi berdasarkan kunci (id sesi, atau path store saat
// id masih kosong) sehingga tidak pernah ada dua `opencode run` pada sesi/id
// yang sama pada saat bersamaan. Kunci berbeda tetap bisa jalan paralel.
type sessionGate struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

// lock mengambil kunci key dan mengembalikan fungsi unlock (aman dipanggil
// lebih dari sekali). Tidak mengunci apa pun bila key kosong.
func (g *sessionGate) lock(key string) func() {
	if key == "" {
		return func() {}
	}
	g.mu.Lock()
	if g.locks == nil {
		g.locks = make(map[string]*sync.Mutex)
	}
	m, ok := g.locks[key]
	if !ok {
		m = new(sync.Mutex)
		g.locks[key] = m
	}
	g.mu.Unlock()
	m.Lock()
	var once sync.Once
	return func() { once.Do(m.Unlock) }
}
