package relay

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

// SeenStore mengingat id pesan terakhir (dengan batas max) untuk dedupe, dan
// menuliskannya ke disk secara berkala (debounce).
type SeenStore struct {
	mu       sync.Mutex
	ids      []string
	set      map[string]struct{}
	path     string
	max      int
	dirty    bool
	period   time.Duration
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

func LoadSeen(path string, max int) *SeenStore {
	s := &SeenStore{
		path:   path,
		max:    max,
		set:    make(map[string]struct{}),
		period: seenFlushPeriod,
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	if max <= 0 {
		s.max = MaxSeen
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
	go s.flushLoop()
	return s
}

// flushLoop menulis state ke disk secara berkala, tapi hanya kalau ada
// perubahan (dirty). Jadi pesan yang datang beruntun tidak memicu I/O
// berkali-kali.
func (s *SeenStore) flushLoop() {
	defer close(s.done)
	ticker := time.NewTicker(s.period)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			s.persist()
		}
	}
}

// Seen reports whether id was already recorded. New ids ditandai dirty saja;
// penulisan file ditunda dan di-flush berkala oleh flushLoop (debounce I/O).
func (s *SeenStore) Seen(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.set[id]; dup {
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
	s.dirty = true
	return false
}

// Flush memaksa penulisan state ke disk sekarang (mis. saat shutdown).
func (s *SeenStore) Flush() {
	s.persist()
}

// Close menghentikan flush berkala lalu menulis sisa perubahan terakhir.
func (s *SeenStore) Close() {
	s.stopOnce.Do(func() {
		close(s.stop)
		<-s.done
	})
	s.persist()
}

func (s *SeenStore) persist() {
	s.mu.Lock()
	if !s.dirty {
		s.mu.Unlock()
		return
	}
	s.dirty = false
	data, err := json.Marshal(s.ids)
	s.mu.Unlock()
	if err != nil {
		s.markDirty()
		return
	}
	if err := writeFileAtomic(s.path, data, 0o644); err != nil {
		s.markDirty()
	}
}

func (s *SeenStore) markDirty() {
	s.mu.Lock()
	s.dirty = true
	s.mu.Unlock()
}
