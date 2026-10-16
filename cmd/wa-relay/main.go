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

	"github.com/Seronz/wa-relay/internal/relay"
)

func main() {
	configPath := flag.String("config", "config.json", "path ke config.json")
	flag.Parse()

	cfg, err := relay.LoadConfig(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(1)
	}
	log, err := relay.NewLogger(cfg.LogFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "log:", err)
		os.Exit(1)
	}
	defer log.Close()

	rollPolicy := cfg.SessionRollPolicy()

	stateDir := filepath.Join(cfg.BaseDir, "state")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "state dir:", err)
		os.Exit(1)
	}
	seen := relay.LoadSeen(filepath.Join(stateDir, "seen.json"), relay.MaxSeen)
	session := relay.LoadSession(filepath.Join(stateDir, "session.json"))
	workSession := (*relay.SessionStore)(nil)
	if cfg.WorkSessionFile != "" {
		workSession = relay.LoadSession(cfg.WorkSessionFile)
	} else {
		workSession = relay.LoadSession(filepath.Join(stateDir, "work-session.json"))
	}
	delegations := relay.LoadDelegationStore(filepath.Join(stateDir, "delegations.json"), cfg.DelegationTTL())

	r := relay.NewRelayWithWork(
		cfg,
		log,
		seen,
		session,
		workSession,
		&relay.ExecRunner{Bin: cfg.OpencodeBin, Workdir: cfg.Workdir, BaseDir: cfg.BaseDir, Agent: cfg.Agent, Timeout: cfg.Timeout()},
		&relay.WacliSender{Bin: cfg.WacliBin, Phone: cfg.OwnerPhone},
		&relay.WacliMarkReader{Bin: cfg.WacliBin},
	)
	r.SetWorkRunner(&relay.ExecRunner{Bin: cfg.OpencodeBin, Workdir: cfg.Workdir, BaseDir: cfg.BaseDir, Agent: cfg.WorkAgentName(), Timeout: cfg.Timeout()})
	r.SetDelegations(delegations)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go r.StartWorkers(ctx)

	srv := &http.Server{
		Addr:              cfg.Addr(),
		Handler:           r.Routes(),
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
	log.Info("session rollover policy idle=%s max_age=%s max_turns=%d (0 = mati)",
		rollPolicy.Idle, rollPolicy.MaxAge, rollPolicy.MaxTurns)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("server: %v", err)
		os.Exit(1)
	}
	seen.Close()
	log.Info("stopped")
}
