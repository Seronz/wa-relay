package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const promptTemplate = "Ini tugas otomatis dari WhatsApp relay untuk pemilikmu (owner). " +
	"Pesan masuk dari owner:\n---\n%s\n---\n" +
	"Jawaban akhirmu akan dikirim balik ke WhatsApp owner. " +
	"Akhiri dengan balasan singkat berupa teks biasa dalam bahasa Indonesia santai, " +
	"tanpa judul markdown, tanpa tabel, tanpa fence kode."

type Config struct {
	Host           string `json:"host"`
	Port           int    `json:"port"`
	WebhookSecret  string `json:"webhook_secret"`
	OwnerJID       string `json:"owner_jid"`
	OwnerPhone     string `json:"owner_phone"`
	Workdir        string `json:"workdir"`
	OpencodeBin    string `json:"opencode_bin"`
	WacliBin       string `json:"wacli_bin"`
	TimeoutSeconds int    `json:"timeout_seconds"`
	LogFile        string `json:"log_file"`

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
		Host:           "127.0.0.1",
		Port:           8791,
		OpencodeBin:    "opencode",
		WacliBin:       "wacli",
		TimeoutSeconds: 480,
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
	return cfg, nil
}
