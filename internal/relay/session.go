package relay

import (
	"encoding/json"
	"os"
	"sync"
)

// SessionStore menyimpan id sesi opencode untuk obrolan, supaya pesan WhatsApp
// berikutnya lanjut di konteks yang sama (agent tidak lupa pesan sebelumnya).
type SessionStore struct {
	mu   sync.Mutex
	path string
	id   string
}

func LoadSession(path string) *SessionStore {
	s := &SessionStore{path: path}
	if data, err := os.ReadFile(path); err == nil {
		var v struct {
			ID string `json:"id"`
		}
		if json.Unmarshal(data, &v) == nil {
			s.id = v.ID
		}
	}
	return s
}

func (s *SessionStore) ID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.id
}

func (s *SessionStore) Set(id string) error {
	s.mu.Lock()
	s.id = id
	data, err := json.Marshal(struct {
		ID string `json:"id"`
	}{ID: id})
	s.mu.Unlock()
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}
