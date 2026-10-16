package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

const queueBuffer = 256

func main() {
	configPath := flag.String("config", "config.json", "path ke config.json")
	flag.Parse()

	cfg, err := LoadConfig(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(1)
	}
	log, err := NewLogger(cfg.LogFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "log:", err)
		os.Exit(1)
	}
	defer log.Close()

	stateDir := filepath.Join(cfg.BaseDir, "state")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "state dir:", err)
		os.Exit(1)
	}
	seen := LoadSeen(filepath.Join(stateDir, "seen.json"), maxSeen)

	relay := &Relay{
		cfg:    cfg,
		log:    log,
		seen:   seen,
		queue:  make(chan taskItem, queueBuffer),
		runner: &ExecRunner{Bin: cfg.OpencodeBin, Workdir: cfg.Workdir, BaseDir: cfg.BaseDir, Agent: cfg.Agent, Timeout: cfg.Timeout()},
		sender: &WacliSender{Bin: cfg.WacliBin, Phone: cfg.OwnerPhone},
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go relay.worker(ctx)

	srv := &http.Server{
		Addr:              cfg.Addr(),
		Handler:           relay.routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		log.Info("signal: shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Error("shutdown: %v", err)
		}
	}()

	log.Info("wa-relay listening on http://%s/hook workdir=%s", cfg.Addr(), cfg.Workdir)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("server: %v", err)
		os.Exit(1)
	}
	seen.persist()
	log.Info("stopped")
}
