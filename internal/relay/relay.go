package relay

import (
	"context"
	"time"
)

type Relay struct {
	cfg         *Config
	log         *Logger
	seen        *SeenStore
	session     *SessionStore // sesi chat WhatsApp (dipertahankan ingatannya)
	workSession *SessionStore // sesi tugas berat (opsional, terpisah dari chat)
	queue       chan taskItem // antrean legacy (masih dipakai test)
	chatQueue   chan taskItem
	workQueue   chan taskItem
	runner      TaskRunner
	workRunner  TaskRunner       // runner khusus lane work; nil = pakai runner
	delegations *DelegationStore // delegasi pending menunggu konfirmasi bos
	sender      Sender
	reader      MarkReader
	gate        sessionGate
}

// NewRelay menyusun Relay lengkap dengan antrean dan dependensinya.
func NewRelay(cfg *Config, log *Logger, seen *SeenStore, session *SessionStore, runner TaskRunner, sender Sender, reader MarkReader) *Relay {
	return NewRelayWithWork(cfg, log, seen, session, nil, runner, sender, reader)
}

// NewRelayWithWork menyusun Relay dengan sesi kerja terpisah (opsional).
func NewRelayWithWork(cfg *Config, log *Logger, seen *SeenStore, chatSession, workSession *SessionStore, runner TaskRunner, sender Sender, reader MarkReader) *Relay {
	return &Relay{
		cfg:         cfg,
		log:         log,
		seen:        seen,
		session:     chatSession,
		workSession: workSession,
		chatQueue:   make(chan taskItem, queueBuffer),
		workQueue:   make(chan taskItem, queueBuffer),
		queue:       make(chan taskItem, queueBuffer), // legacy support for tests
		runner:      runner,
		sender:      sender,
		reader:      reader,
	}
}

// SetWorkRunner memasang runner khusus lane work (agent teknis). Bila tidak
// dipanggil, lane work memakai runner chat (fallback).
func (r *Relay) SetWorkRunner(runner TaskRunner) {
	r.workRunner = runner
}

// SetDelegations memasang penyimpanan delegasi pending (konfirmasi bos).
func (r *Relay) SetDelegations(store *DelegationStore) {
	r.delegations = store
}

func (r *Relay) chatWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case item, ok := <-r.chatQueue:
			if !ok {
				return
			}
			r.handleMessage(ctx, item)
		case item, ok := <-r.queue:
			if !ok {
				continue
			}
			r.handleMessage(ctx, item)
		}
	}
}

func (r *Relay) workWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case item, ok := <-r.workQueue:
			if !ok {
				return
			}
			r.handleMessage(ctx, item)
		case item, ok := <-r.queue:
			if !ok {
				continue
			}
			r.handleMessage(ctx, item)
		}
	}
}

// runnerFor memilih runner sesuai lane: lane work memakai runner terpisah
// (agent teknis) supaya tugas berat tidak dikerjakan persona chat. Bila
// workRunner nil, fallback ke runner biasa.
func (r *Relay) runnerFor(lane Lane) TaskRunner {
	if lane == LaneWork && r.workRunner != nil {
		return r.workRunner
	}
	return r.runner
}

// StartWorkers menjalankan worker pool yang dipisah per lane, supaya balasan
// chat tidak pernah antre di belakang tugas berat: chat_workers worker khusus
// chatQueue, num_workers worker khusus workQueue. Keduanya juga membaca queue
// legacy (masih dipakai oleh test).
func (r *Relay) StartWorkers(ctx context.Context) {
	n := r.cfg.NumWorkers
	if n <= 0 {
		n = defaultWorkers
	}
	chat := r.cfg.ChatWorkers
	if chat <= 0 {
		chat = defaultChatWorkers
	}
	for i := 0; i < chat; i++ {
		go r.chatWorker(ctx)
	}
	for i := 0; i < n; i++ {
		go r.workWorker(ctx)
	}
	r.log.Info("worker pool started chat=%d work=%d", chat, n)
}

// markRead menandai chat pesan owner sebagai sudah dibaca lewat wacli.
// Dipanggil dari handleMessage tepat sebelum balasan final dikirim, supaya
// pesan yang masuk masih centang dua abu-abu dan hanya berubah biru ketika
// balasan sudah disiapkan (termasuk untuk fast-path ping/reset dan balasan
// error). Privasi read receipt diserahkan ke wacli lewat flag --receipts
// (mengikuti setting privasi user), tidak diatur di sini.
func (r *Relay) markRead(chat string) {
	if r.reader == nil || !r.cfg.MarkRead || chat == "" {
		return
	}
	if err := r.reader.MarkRead(chat); err != nil {
		r.log.Warn("mark-read failed chat=%s err=%v", chat, err)
		return
	}
	r.log.Info("mark-read ok chat=%s", chat)
}

// logRollover mencatat alasan sesi di-rollover supaya kelihatan di relay.log.
func (r *Relay) logRollover(lane, reason string) {
	if reason == "" {
		return
	}
	r.log.Info("session rollover lane=%s reason=%s -> mulai sesi baru", lane, reason)
}

// sessionFor mengembalikan SessionStore untuk sebuah lane (bisa nil).
func (r *Relay) sessionFor(lane Lane) *SessionStore {
	if lane == LaneWork {
		return r.workSession
	}
	return r.session
}

// sessionPathFor mengembalikan lokasi file sesi untuk sebuah lane.
func (r *Relay) sessionPathFor(lane Lane) string {
	if lane == LaneWork {
		return r.workSessionPath()
	}
	return r.chatSessionPath()
}

// beginSession memilih id sesi untuk lane, melepas sesi yang sudah basi sesuai
// kebijakan rollover. Return "" berarti pesan ini memulai sesi baru.
func (r *Relay) beginSession(lane Lane, now time.Time) string {
	store := r.sessionFor(lane)
	if store == nil {
		return ""
	}
	id, reason := store.Begin(now, r.cfg.SessionRollPolicy())
	r.logRollover(string(lane), reason)
	return id
}

// saveSessionFromOutput menyimpan id sesi baru yang muncul di stdout agent,
// dipakai ketika pesan tadi memulai sesi baru (beginSession mengembalikan "").
func (r *Relay) saveSessionFromOutput(lane Lane, stdout string) {
	store := r.sessionFor(lane)
	if store == nil {
		return
	}
	id := parseSessionID(stdout)
	if id == "" {
		return
	}
	if err := store.Set(id); err != nil {
		r.log.Error("session save failed: %v", err)
		return
	}
	r.log.Info("session saved id=%s", id)
}

// gateKey menentukan kunci penguncian sesi: id sesi bila ada, atau kombinasi
// lane + path store + id task saat sesi masih kosong.
func (r *Relay) gateKey(lane Lane, sessionID, fallbackID string) string {
	if sessionID != "" {
		return sessionID
	}
	return string(lane) + ":" + r.sessionPathFor(lane) + ":" + fallbackID
}

// run menjalankan prompt lewat runner lane terkait, diserialisasi per sesi
// lewat gate supaya tidak ada dua `opencode run` pada sesi yang sama.
func (r *Relay) run(ctx context.Context, lane Lane, sessionID, fallbackID, prompt string) (string, string, error) {
	unlock := r.gate.lock(r.gateKey(lane, sessionID, fallbackID))
	defer unlock()
	return r.runnerFor(lane).Run(ctx, prompt, sessionID)
}

func (r *Relay) chatSessionPath() string {
	if r != nil && r.session != nil {
		return r.session.Path()
	}
	return ""
}

func (r *Relay) workSessionPath() string {
	if r != nil && r.workSession != nil {
		return r.workSession.Path()
	}
	return r.cfg.WorkSessionFile
}
