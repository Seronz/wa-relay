package relay

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

// State adalah isi file sesi: id opencode plus metadata umur pakai. Metadata ini
// yang bikin sesi bisa di-rollover otomatis (nganggur kelamaan, umur kelamaan,
// atau terlalu banyak turn), jadi konteks tidak tumbuh tanpa batas.
type State struct {
	ID        string    `json:"id"`
	StartedAt time.Time `json:"started_at,omitzero"`
	LastUsed  time.Time `json:"last_used,omitzero"`
	Turns     int       `json:"turns,omitempty"`
	// Rolled menghitung berapa kali sesi sudah di-rollover (buat observasi).
	Rolled int `json:"rolled,omitempty"`
	// PrevID menyimpan id sesi sebelumnya sebagai jejak, supaya rollover masih
	// bisa dibalikin manual kalau ternyata butuh konteks lama.
	PrevID string `json:"prev_id,omitempty"`
}

// RollPolicy menentukan kapan sesi dianggap basi. Nilai 0 mematikan aturan itu.
type RollPolicy struct {
	Idle     time.Duration // rollover kalau nganggur lebih lama dari ini
	MaxAge   time.Duration // rollover kalau umur sesi melebihi ini
	MaxTurns int           // rollover setelah sekian pesan
}

// SessionStore menyimpan sesi opencode untuk obrolan, supaya pesan WhatsApp
// berikutnya lanjut di konteks yang sama (agent tidak lupa pesan sebelumnya).
type SessionStore struct {
	mu    sync.Mutex
	path  string
	state State
}

func LoadSession(path string) *SessionStore {
	s := &SessionStore{path: path}
	if data, err := os.ReadFile(path); err == nil {
		var v State
		if json.Unmarshal(data, &v) == nil {
			s.state = v
		}
	}
	return s
}

func (s *SessionStore) ID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.ID
}

// Snapshot menyalin metadata sesi saat ini (buat logging dan test).
func (s *SessionStore) Snapshot() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// Set menyimpan id sesi baru. Set("") mengosongkan sesi sekaligus mereset
// metadata umur (dipakai oleh /reset). Jejak PrevID/Rolled dipertahankan.
func (s *SessionStore) Set(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id == "" {
		s.state = State{PrevID: s.state.PrevID, Rolled: s.state.Rolled}
	} else if s.state.ID != id {
		s.state = State{
			ID:        id,
			StartedAt: time.Now(),
			LastUsed:  time.Now(),
			Turns:     1,
			PrevID:    s.state.PrevID,
			Rolled:    s.state.Rolled,
		}
	}
	return s.persistLocked()
}

// Begin dipanggil saat sebuah pesan akan diproses. Returnnya adalah id sesi yang
// dipakai: id lama kalau masih layak, atau "" kalau sesi harus di-rollover
// (pesan ini memulai sesi baru). reason berisi alasan rollover untuk logging:
// "idle", "age", "turns", atau "" kalau sesi diteruskan.
//
// Sesi lama tidak dihapus dari DB, cuma dilepas pointer-nya (tersimpan di
// prev_id) — jadi masih bisa dipanggil balik manual.
func (s *SessionStore) Begin(now time.Time, p RollPolicy) (id string, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state.ID == "" {
		return "", ""
	}
	if reason = s.expired(now, p); reason != "" {
		s.state = State{PrevID: s.state.ID, Rolled: s.state.Rolled + 1}
		_ = s.persistLocked()
		return "", reason
	}
	if s.state.StartedAt.IsZero() {
		// File sesi lama (sebelum ada metadata) — mulai hitung umur dari sini.
		s.state.StartedAt = now
	}
	s.state.LastUsed = now
	s.state.Turns++
	_ = s.persistLocked()
	return s.state.ID, ""
}

// expired memeriksa ketiga aturan; yang pertama kena dipakai sebagai alasan.
func (s *SessionStore) expired(now time.Time, p RollPolicy) string {
	if p.Idle > 0 && !s.state.LastUsed.IsZero() && now.Sub(s.state.LastUsed) > p.Idle {
		return "idle"
	}
	if p.MaxAge > 0 && !s.state.StartedAt.IsZero() && now.Sub(s.state.StartedAt) > p.MaxAge {
		return "age"
	}
	if p.MaxTurns > 0 && s.state.Turns >= p.MaxTurns {
		return "turns"
	}
	return ""
}

// Path mengembalikan lokasi file store.
func (s *SessionStore) Path() string {
	if s == nil {
		return ""
	}
	return s.path
}

// persistLocked menulis state ke disk secara atomik. Pemanggil harus sudah
// memegang lock.
func (s *SessionStore) persistLocked() error {
	data, err := json.Marshal(s.state)
	if err != nil {
		return err
	}
	return writeFileAtomic(s.path, data, 0o600)
}
