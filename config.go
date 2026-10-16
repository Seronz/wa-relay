package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const promptTemplate = "Pesan WhatsApp dari irza:\n---\n%s\n---\n" +
	"Balas sebagai dirimu sendiri, singkat dan natural (bahasa Indonesia santai/gaul). " +
	"Langsung ke balasan, tanpa judul markdown, tanpa tabel, tanpa fence kode."

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
		TimeoutSeconds: 900,
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
	return cfg, nil
}
