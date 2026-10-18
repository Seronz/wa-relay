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

// defaultHandoverKeywords adalah frasa pemicu serah terima tugas ke tim teknis.
// Dipakai saat bos memerintah stepi secara eksplisit (stepi sebagai
// resepsionis), mis. "serahin aja tugasnya ke Pak Budi". Perilaku ini terpisah
// dari routing otomatis (AutoWorkRouting).
var defaultHandoverKeywords = []string{
	"serahin", "serahkan", "serahin aja", "serah aja", "limpahkan", "limpahin",
	"delegasikan", "delegasiin", "pak budi", "pakbudi", "tim teknis", "tim build",
}

// Classify menentukan lane sebuah pesan. Secara default semuanya masuk lane
// chat (stepi resepsionis): tugas berat hanya diserahkan ke lane work saat bos
// memerintah eksplisit lewat frasa serah terima (lihat Config.IsHandover).
// Routing otomatis berdasarkan panjang/kata kunci (defaultHeavyKeywords +
// config heavy_keywords) hanya aktif bila cfg.AutoWorkRouting diset true.
func Classify(cfg *Config, text string) Lane {
	t := strings.TrimSpace(text)
	if t == "" || isFastCommand(t) {
		return LaneChat
	}
	if cfg == nil || !cfg.AutoWorkRouting {
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

// isResetCommand melaporkan apakah teks adalah perintah untuk mereset sesi
// obrolan. "/reset" adalah bentuk lama; "!restart" alias yang sama. Keduanya
// case-insensitive dan boleh diapit spasi.
func isResetCommand(t string) bool {
	s := strings.TrimSpace(t)
	return strings.EqualFold(s, "/reset") || strings.EqualFold(s, "!restart")
}

// isFastCommand melaporkan apakah teks adalah perintah fast-path (tanpa
// pemanggilan agent) yang ditangani langsung oleh handleMessage.
func isFastCommand(t string) bool {
	return strings.EqualFold(t, "ping") || isResetCommand(t)
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
