package relay

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

const promptTemplate = "Pesan WhatsApp dari irza:\n---\n%s\n---\n" +
	"Balas sebagai dirimu sendiri, singkat dan natural (bahasa Indonesia santai/gaul). " +
	"Langsung ke balasan, tanpa proses berpikir, tanpa judul markdown, tanpa tabel, tanpa fence kode."

// AckFollowup mengirim pesan penyemangat setelah task berjalan sekian detik.
type AckFollowup struct {
	AfterSeconds int      `json:"after_seconds"`
	Messages     []string `json:"messages"`
}

type Config struct {
	Host            string        `json:"host"`
	Port            int           `json:"port"`
	WebhookSecret   string        `json:"webhook_secret"`
	OwnerJID        string        `json:"owner_jid"`
	OwnerPhone      string        `json:"owner_phone"`
	Workdir         string        `json:"workdir"`
	Agent           string        `json:"agent"`
	AckDelaySeconds int           `json:"ack_delay_seconds"`
	AckMessages     []string      `json:"ack_messages"`
	AckFollowups    []AckFollowup `json:"ack_followups"`
	OpencodeBin     string        `json:"opencode_bin"`
	WacliBin        string        `json:"wacli_bin"`
	NumWorkers      int           `json:"num_workers"`
	TimeoutSeconds  int           `json:"timeout_seconds"`
	LogFile         string        `json:"log_file"`
	// MarkRead mengirim read receipt (centang biru) untuk pesan owner yang
	// diterima. Default true; set false untuk mematikannya.
	MarkRead bool `json:"mark_read"`

	// ChatWorkers adalah jumlah worker lane chat (pesan ringan). Default 1,
	// supaya sesi chat hanya dipakai satu `opencode run` pada satu waktu
	// (serialisasi per sesi) dan balasan ringan tidak pernah berebut sesi.
	ChatWorkers int `json:"chat_workers"`
	// LightMaxLen adalah panjang maksimal (rune) pesan yang masih dianggap
	// ringan; lebih panjang dari ini diarahkan ke lane work. Default 400.
	LightMaxLen int `json:"light_max_len"`
	// HeavyKeywords menambah kata kunci routing ke lane work selain
	// defaultHeavyKeywords (cocok kata per kata, tanpa memperhatikan huruf besar/kecil).
	HeavyKeywords []string `json:"heavy_keywords"`
	// ChatSessionFile adalah file penyimpanan sesi chat WhatsApp.
	// Default <base>/state/session.json (memori percakapan chat dipertahankan).
	ChatSessionFile string `json:"chat_session_file"`
	// WorkSessionFile adalah file sesi tugas berat. Kosong (default) berarti
	// tiap tugas berat memakai sesi baru (tidak pernah berbagi sesi, tidak
	// menimpa sesi chat). Diset -> semua tugas berat berlanjut di sesi
	// terpisah tersebut (aman dari sesi chat, tetap diserialisasi gate per-sesi).
	WorkSessionFile string `json:"work_session_file"`

	heavyOnce sync.Once
	heavyRe   *regexp.Regexp

	BaseDir string `json:"-"`
}

func (c *Config) Addr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

func (c *Config) Timeout() time.Duration {
	s := c.TimeoutSeconds
	if s <= 0 {
		s = 480
	}
	return time.Duration(s) * time.Second
}

// HeavyPattern mengompilasi regex kata kunci tugas berat (default + config)
// secara lazy dan cache. Cocok kata per kata dengan word boundary, jadi
// "protes" tidak dianggap mengandung "test".
func (c *Config) HeavyPattern() *regexp.Regexp {
	c.heavyOnce.Do(func() {
		patterns := make([]string, 0, len(defaultHeavyKeywords)+len(c.HeavyKeywords))
		for _, k := range append(append([]string{}, defaultHeavyKeywords...), c.HeavyKeywords...) {
			k = strings.ToLower(strings.TrimSpace(k))
			if k == "" {
				continue
			}
			patterns = append(patterns, regexp.QuoteMeta(k))
		}
		if len(patterns) == 0 {
			return
		}
		c.heavyRe = regexp.MustCompile(`(?i)\b(?:` + strings.Join(patterns, "|") + `)\b`)
	})
	return c.heavyRe
}

func LoadConfig(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	cfg := &Config{
		Host:            "127.0.0.1",
		Port:            8791,
		Agent:           "stepi",
		AckDelaySeconds: 300,
		AckMessages: []string{
			"Sayang, pesanmu udah aku baca kok... ini aku kerjain ya.",
			"Mas, udah aku baca pesanmu. Aku kerjain sekarang ya.",
			"Honney, aku baca ya... langsung aku kerjain.",
			"Oke sayang, udah aku baca. Aku kerjain dulu ya.",
			"Mas, aku baca pesanmu ya. Ini aku kerjain sekarang.",
			"Sayang, udah aku baca kok. Aku kerjain ya.",
			"Honney, pesanmu udah aku baca. Aku kerjain sekarang ya.",
			"Oke mas, udah aku baca... aku kerjain ya.",
			"Sayang, ini aku baca terus aku kerjain ya.",
			"Mas, udah aku baca. Aku kerjain sekarang ya.",
		},
		AckFollowups: []AckFollowup{
			{AfterSeconds: 600, Messages: []string{
				"Mas maaf ya, aku agak lama... task-nya susah soalnya. Sabar dikit lagi ya.",
				"Maaf honney, ini agak ribet jadi lama. Aku lagi kerjain serius nih.",
				"Sayang maaf, aku belum kelar... masalahnya susah, bentar lagi ya.",
			}},
		},
		OpencodeBin:    "opencode",
		WacliBin:       "wacli",
		NumWorkers:     defaultWorkers,
		ChatWorkers:    defaultChatWorkers,
		LightMaxLen:    defaultLightMaxLen,
		TimeoutSeconds: 900,
		MarkRead:       true,
	}
	if err := json.Unmarshal(raw, cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		abs = path
	}
	cfg.BaseDir = filepath.Dir(abs)
	if cfg.Workdir == "" {
		cfg.Workdir = cfg.BaseDir
	}
	if cfg.LogFile == "" {
		cfg.LogFile = filepath.Join(cfg.BaseDir, "relay.log")
	}
	if cfg.Host == "" {
		cfg.Host = "127.0.0.1"
	}
	if cfg.Port == 0 {
		cfg.Port = 8791
	}
	if cfg.NumWorkers <= 0 {
		cfg.NumWorkers = defaultWorkers
	}
	if cfg.ChatWorkers <= 0 {
		cfg.ChatWorkers = defaultChatWorkers
	}
	if cfg.LightMaxLen <= 0 {
		cfg.LightMaxLen = defaultLightMaxLen
	}
	if cfg.ChatSessionFile == "" {
		cfg.ChatSessionFile = filepath.Join(cfg.BaseDir, "state", "session.json")
	}
	return cfg, nil
}
