package relay

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"
)

// delegationPromptTemplate diteruskan ke stepi (agent chat) saat ada tugas
// berat: dia meringkas requirement dan meminta konfirmasi bos sebelum
// delegasi ke tim teknis.
const delegationPromptTemplate = "Pesan WhatsApp dari bos:\n---\n%s\n---\n" +
	"Tugas ini terlihat teknis/berat. Kamu sebagai asisten eksekutif harus:" +
	"\n1. Ringkaskan requirement-nya sebagai daftar angka (1, 2, 3, dst), bahasa Indonesia santai dan singkat." +
	"\n2. Akhiri dengan konfirmasi ke bos bahwa tugas ini akan didelegasikan ke tim teknis, contoh: \"Aku delegasikan tugas ini ke tim teknis, bos setuju?\"" +
	"\nJangan mulai pengerjaan sendiri, jangan janjikan hasil, cuma rangkumin requirement dan tanya setuju nggak." +
	" Langsung ke balasan, tanpa proses berpikir, tanpa judul markdown, tanpa tabel, tanpa fence kode."

// buildPromptTemplate diteruskan ke agent teknis (build) untuk mengerjakan tugas.
const buildPromptTemplate = "Tugas teknis berikut dibawah tangan kamu. Selesaikan sebaik-baiknya (gunakan tools bila perlu)." +
	" Hasil akhir akan dirangkum dan dikirim ke bos lewat WA, jadi fokus pada pengerjaan, bukan format balasan." +
	"\n\nTugas: %s"

// summarizePromptTemplate diteruskan ke stepi setelah agent teknis selesai,
// agar hasil teknis dirangkum untuk bos lewat WA.
const summarizePromptTemplate = "Hasil pengerjaan tim teknis untuk tugas:\n---\n%s\n---\n" +
	"Rangkum hasilnya untuk bos lewat WhatsApp, bahasa Indonesia santai, singkat dan padat." +
	" Sampaikan status selesai + poin penting saja. Jangan terlalu teknis kecuali dibutuhkan." +
	" Langsung ke balasan, tanpa proses berpikir, tanpa judul markdown, tanpa tabel, tanpa fence kode."

// busyPromptTemplate diteruskan ke stepi saat semua slot build terpakai, supaya
// bos dikabari bahwa tim sedang sibuk semua (bukan dibiarkan menunggu diam).
const busyPromptTemplate = "Semua slot tim teknis (build) sedang terpakai, jadi tugas berat ini belum bisa didelegasikan. " +
	"Sampaikan ke bos dengan singkat dan natural (bahasa Indonesia santai/gaul) bahwa tim lagi sibuk semua " +
	"dan tugasnya akan dikerjakan begitu ada yang kosong." +
	" Langsung ke balasan, tanpa proses berpikir, tanpa judul markdown, tanpa tabel, tanpa fence kode."

// busyFallback dipakai kalau stepi gagal menyusun pesan sibuk, supaya bos tetap
// mendapat kabar.
const busyFallback = "Tim lagi sibuk semua, nanti aku kabari ya."

// confirmationRe cocok balasan singkat yang berarti "setuju" dari bos.
var confirmationRe = regexp.MustCompile(`(?i)^\s*(ya|iya|setuju|oke|ok|gas|lanjut|proses|approve|yes|y)\b`)

// Delegation adalah tugas berat yang menunggu persetujuan bos sebelum
// didelegasikan ke agent teknis.
type Delegation struct {
	ID        string    `json:"id"`
	Text      string    `json:"text"`
	Chat      string    `json:"chat"`
	CreatedAt time.Time `json:"created_at"`
}

func (d *Delegation) expired(ttl time.Duration) bool {
	return d == nil || (ttl > 0 && time.Since(d.CreatedAt) > ttl)
}

// DelegationStore menyimpan satu delegasi pending per chat dalam file JSON
// agar survive restart.
type DelegationStore struct {
	mu   sync.Mutex
	path string
	ttl  time.Duration
	d    *Delegation
}

func LoadDelegationStore(path string, ttl time.Duration) *DelegationStore {
	s := &DelegationStore{path: path, ttl: ttl}
	b, err := os.ReadFile(path)
	if err != nil || len(b) == 0 {
		return s
	}
	var d Delegation
	if jerr := json.Unmarshal(b, &d); jerr != nil {
		return s
	}
	s.d = &d
	return s
}

func (s *DelegationStore) Get(chat string) (*Delegation, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.d == nil || s.d.Chat != chat || s.d.expired(s.ttl) {
		return nil, false
	}
	return s.d, true
}

func (s *DelegationStore) Set(d *Delegation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.d = d
	s.persist()
}

func (s *DelegationStore) Clear(chat string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.d != nil && s.d.Chat == chat {
		s.d = nil
		s.persist()
	}
}

func (s *DelegationStore) persist() {
	if s.path == "" {
		return
	}
	var b []byte
	var err error
	if s.d != nil {
		b, err = json.MarshalIndent(s.d, "", "  ")
		if err != nil {
			return
		}
	}
	dir := filepath.Dir(s.path)
	_ = os.MkdirAll(dir, 0o700)
	_ = writeFileAtomic(s.path, b, 0o600)
}
