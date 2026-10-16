package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	MaxSeen            = 500
	queueBuffer        = 256
	defaultChunk       = 3000
	sendTimeout        = 120 * time.Second
	markReadTimeout    = 30 * time.Second
	stderrMaxRune      = 400
	seenFlushPeriod    = 2 * time.Second
	defaultWorkers     = 3
	defaultChatWorkers = 1
	defaultLightMaxLen = 400

	// Default rollover sesi: 24 jam nganggur, umur maksimum 7 hari, atau 60
	// pesan — mana yang lebih dulu kena. Lihat Config.SessionRollPolicy.
	defaultSessionIdleSeconds   = 24 * 60 * 60
	defaultSessionMaxAgeSeconds = 7 * 24 * 60 * 60
	defaultSessionMaxTurns      = 60

	// defaultWorkAgent adalah agent opencode untuk lane work (tugas teknis),
	// supaya tidak memakai persona chat.
	defaultWorkAgent = "build"

	// defaultDelegationTTLSeconds adalah batas waktu menunggu konfirmasi bos
	// sebelum delegasi kadaluarsa (default 30 menit).
	defaultDelegationTTLSeconds = 1800
)

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
		s.mu.Lock()
		s.dirty = true
		s.mu.Unlock()
		return
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		s.mu.Lock()
		s.dirty = true
		s.mu.Unlock()
		return
	}
	os.Rename(tmp, s.path)
}

type taskItem struct {
	ID   string `json:"id"`
	Text string `json:"text"`
	// Chat adalah target mark-read untuk pesan ini, sudah dihitung handleHook
	// lewat chatTarget (Chat -> SenderJID -> OwnerJID -> OwnerPhone).
	Chat string `json:"chat"`
	Lane Lane   `json:"lane"`
}

// TaskRunner runs the agent for a prompt and returns raw stdout/stderr.
// sessionID, bila tidak kosong, melanjutkan sesi opencode yang sama.
type TaskRunner interface {
	Run(ctx context.Context, prompt, sessionID string) (stdout string, stderr string, err error)
}

type ExecRunner struct {
	Bin     string
	Workdir string
	BaseDir string
	Agent   string
	Timeout time.Duration
}

// args builds the `opencode run` argument list. Exposed for testing.
func (r *ExecRunner) args(prompt, sessionID string) []string {
	args := []string{"run", "--dir", r.Workdir, "--format", "json", "--title", "wa-relay"}
	if r.Agent != "" {
		args = append(args, "--agent", r.Agent)
	}
	if sessionID != "" {
		args = append(args, "--session", sessionID)
	}
	return append(args, prompt)
}

func (r *ExecRunner) Run(ctx context.Context, prompt, sessionID string) (string, string, error) {
	ctx, cancel := context.WithTimeout(ctx, r.Timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, r.Bin, r.args(prompt, sessionID)...)
	cmd.Dir = r.BaseDir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if ctx.Err() == context.DeadlineExceeded {
		return stdout.String(), stderr.String(), context.DeadlineExceeded
	}
	return stdout.String(), stderr.String(), err
}

// Sender delivers a reply message to the owner.
type Sender interface {
	Send(text string) error
}

type WacliSender struct {
	Bin   string
	Phone string
}

func (s *WacliSender) Send(text string) error {
	ctx, cancel := context.WithTimeout(context.Background(), sendTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, s.Bin, "send", "text", "--to", s.Phone, "--message", text)
	out, err := cmd.CombinedOutput()
	if err != nil {
		detail := truncateRunes(strings.TrimSpace(string(out)), 300)
		if detail != "" {
			return fmt.Errorf("%v: %s", err, detail)
		}
		return err
	}
	return nil
}

// MarkReader menandai satu chat sebagai sudah dibaca (read receipt).
type MarkReader interface {
	MarkRead(chat string) error
}

type WacliMarkReader struct {
	Bin string
}

// args builds the `wacli chats mark-read` argument list. Exposed for testing.
// Flag --receipts yang mengirim read receipt diserahkan sepenuhnya ke wacli,
// termasuk soal privasi read receipt milik user.
func (m *WacliMarkReader) args(chat string) []string {
	return []string{"chats", "mark-read", "--chat", chat, "--receipts"}
}

func (m *WacliMarkReader) MarkRead(chat string) error {
	ctx, cancel := context.WithTimeout(context.Background(), markReadTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, m.Bin, m.args(chat)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		detail := truncateRunes(strings.TrimSpace(string(out)), stderrMaxRune)
		if detail != "" {
			return fmt.Errorf("%v: %s", err, detail)
		}
		return err
	}
	return nil
}

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

func (r *Relay) handleMessage(ctx context.Context, item taskItem) {
	start := time.Now()
	r.log.Info("task start id=%s text_len=%d", item.ID, len(item.Text))
	if strings.EqualFold(strings.TrimSpace(item.Text), "ping") {
		r.markRead(item.Chat)
		r.sendChunked("pong")
		r.log.Info("task done id=%s fast-path duration=%.1fs", item.ID, time.Since(start).Seconds())
		return
	}
	// "/reset" menghapus sesi, jadi pesan berikutnya mulai obrolan baru.
	if strings.TrimSpace(item.Text) == "/reset" {
		if r.session != nil {
			if err := r.session.Set(""); err != nil {
				r.log.Error("session reset failed: %v", err)
			}
		}
		if r.delegations != nil {
			r.delegations.Clear(item.Chat)
		}
		r.markRead(item.Chat)
		r.sendChunked("oke, kita mulai dari awal ya 😊")
		r.log.Info("task done id=%s reset duration=%.1fs", item.ID, time.Since(start).Seconds())
		return
	}

	// Konfirmasi bos ("setuju", dst) ke delegasi pending: jalankan pipeline
	// delegasi di background supaya chat tidak terblokir sambil build jalan.
	if confirmationRe.MatchString(item.Text) {
		if d, ok := r.delegations.Get(item.Chat); ok {
			r.delegations.Clear(item.Chat)
			go r.runDelegation(context.WithoutCancel(ctx), item, d)
			r.markRead(item.Chat)
			r.log.Info("delegation confirmed id=%s delegate_id=%s", item.ID, d.ID)
			return
		}
		r.delegations.Clear(item.Chat)
	}

	// Tugas berat: stepi duluan meringkas requirement dan meminta konfirmasi
	// bos. Tim teknis baru dikerjakan setelah bos balas konfirmasi.
	if item.Lane == LaneWork {
		r.proposeDelegation(ctx, item)
		return
	}
	// Ack pertama & follow-up hanya dikirim kalau task masih jalan (lihat
	// sendScheduled). Task yang cepat selesai cukup dapat jawaban langsung.
	taskCtx, cancelTask := context.WithCancel(ctx)
	defer cancelTask()

	// sendScheduled TIDAK boleh ikut context handleMessage, karena context itu
	// di-cancel begitu handleMessage return dan bisa memotong followup terakhir
	// pas reply final keburu dikirim. Pakai context yang dilepas (WithoutCancel),
	// lalu stop eksplisit setelah reply final terkirim sambil menunggu kiriman
	// yang sedang jalan selesai dulu.
	schedCtx, stopSchedule := context.WithCancel(context.WithoutCancel(ctx))
	scheduleDone := make(chan struct{})
	go func() {
		defer close(scheduleDone)
		r.sendScheduled(schedCtx, item)
	}()
	defer func() {
		stopSchedule()
		<-scheduleDone
	}()

	// Lanjutkan sesi opencode yang sama supaya agent ingat obrolan sebelumnya.
	// Sesi yang sudah basi (nganggur kelamaan, umur kelamaan, atau kebanyakan
	// pesan) di-rollover di sini: id-nya dilepas supaya pesan ini mulai sesi
	// baru. Sesi lama tidak dihapus dari DB, cuma dicatat di prev_id.
	sessionID := ""
	rollPolicy := r.cfg.SessionRollPolicy()
	if item.Lane == LaneWork {
		if r.workSession != nil {
			id, reason := r.workSession.Begin(time.Now(), rollPolicy)
			sessionID = id
			r.logRollover("work", reason)
		}
	} else {
		if r.session != nil {
			id, reason := r.session.Begin(time.Now(), rollPolicy)
			sessionID = id
			r.logRollover("chat", reason)
		}
	}
	prompt := fmt.Sprintf(promptTemplate, strings.TrimSpace(item.Text))
	lockKey := sessionID
	if lockKey == "" {
		if item.Lane == LaneWork {
			lockKey = "work:" + r.workSessionPath() + ":" + item.ID
		} else {
			lockKey = "chat:" + r.chatSessionPath() + ":" + item.ID
		}
	}
	unlock := r.gate.lock(lockKey)
	stdout, stderr, err := r.runnerFor(item.Lane).Run(taskCtx, prompt, sessionID)
	unlock()
	if sessionID == "" {
		if item.Lane == LaneWork {
			if r.workSession != nil {
				if id := parseSessionID(stdout); id != "" {
					if serr := r.workSession.Set(id); serr != nil {
						r.log.Error("session save failed: %v", serr)
					} else {
						r.log.Info("session saved id=%s", id)
					}
				}
			}
		} else {
			if r.session != nil {
				if id := parseSessionID(stdout); id != "" {
					if serr := r.session.Set(id); serr != nil {
						r.log.Error("session save failed: %v", serr)
					} else {
						r.log.Info("session saved id=%s", id)
					}
				}
			}
		}
	}
	if err != nil {
		r.log.Error("opencode failed: %v (stderr %dB)", err, len(stderr))
		r.markRead(item.Chat)
		r.sendChunked(errorNote(err, stderr))
		r.log.Info("task failed id=%s duration=%.1fs", item.ID, time.Since(start).Seconds())
		return
	}
	r.log.Info("opencode done duration=%.1fs stdout=%dB stderr=%dB", time.Since(start).Seconds(), len(stdout), len(stderr))
	reply := extractReply(stdout)
	r.markRead(item.Chat)
	r.sendChunked(reply)
	r.log.Info("task done id=%s reply_len=%d duration=%.1fs", item.ID, len(reply), time.Since(start).Seconds())
}

// proposeDelegation mengarahkan stepi untuk meringkas requirement tugas berat
// dan meminta konfirmasi bos sebelum delegasi ke tim teknis.
func (r *Relay) proposeDelegation(ctx context.Context, item taskItem) {
	sessionID := ""
	if r.session != nil {
		id, reason := r.session.Begin(time.Now(), r.cfg.SessionRollPolicy())
		sessionID = id
		r.logRollover("chat", reason)
	}
	prompt := fmt.Sprintf(delegationPromptTemplate, strings.TrimSpace(item.Text))
	lockKey := sessionID
	if lockKey == "" {
		lockKey = "chat:" + r.chatSessionPath() + ":" + item.ID
	}
	unlock := r.gate.lock(lockKey)
	stdout, _, err := r.runnerFor(LaneChat).Run(ctx, prompt, sessionID)
	unlock()
	if err != nil {
		r.log.Error("delegation proposal failed: %v", err)
		r.markRead(item.Chat)
		r.sendChunked(errorNote(err, ""))
		return
	}
	reply := extractReply(stdout)
	r.markRead(item.Chat)
	r.sendChunked(reply)
	if r.delegations != nil {
		r.delegations.Set(&Delegation{
			ID:        item.ID,
			Text:      item.Text,
			Chat:      item.Chat,
			CreatedAt: time.Now(),
		})
	}
	r.log.Info("delegation proposed id=%s", item.ID)
}

// runDelegation menjalankan pipeline delegasi di background: agent teknis
// (build) mengerjakan tugas, lalu stepi merangkum hasilnya untuk bos. Jalankan
// dalam goroutine sendiri agar chat tidak terblokir sambil build berjalan.
func (r *Relay) runDelegation(ctx context.Context, item taskItem, d *Delegation) {
	schedCtx, stopSchedule := context.WithCancel(context.WithoutCancel(ctx))
	scheduleDone := make(chan struct{})
	go func() {
		defer close(scheduleDone)
		r.sendScheduled(schedCtx, item)
	}()
	defer func() {
		stopSchedule()
		<-scheduleDone
	}()

	// 1. Agent teknis mengerjakan tugas (sesi work terpisah).
	sessionID := ""
	if r.workSession != nil {
		id, reason := r.workSession.Begin(time.Now(), r.cfg.SessionRollPolicy())
		sessionID = id
		r.logRollover("work", reason)
	}
	buildPrompt := fmt.Sprintf(buildPromptTemplate, d.Text)
	lockKey := sessionID
	if lockKey == "" {
		lockKey = "work:" + r.workSessionPath() + ":" + d.ID
	}
	unlock := r.gate.lock(lockKey)
	stdout, stderr, err := r.runnerFor(LaneWork).Run(ctx, buildPrompt, sessionID)
	unlock()
	if err != nil {
		r.log.Error("delegation build failed: %v (stderr %dB)", err, len(stderr))
		r.markRead(d.Chat)
		r.sendChunked(errorNote(err, stderr))
		r.log.Info("delegation failed id=%s delegate_id=%s duration=%.1fs", item.ID, d.ID, time.Since(d.CreatedAt).Seconds())
		return
	}
	r.log.Info("delegation build done delegate_id=%s stdout=%dB", d.ID, len(stdout))

	// 2. Stepi merangkum hasil untuk bos (sesi chat).
	sumSessionID := ""
	if r.session != nil {
		id, reason := r.session.Begin(time.Now(), r.cfg.SessionRollPolicy())
		sumSessionID = id
		r.logRollover("chat", reason)
	}
	sumPrompt := fmt.Sprintf(summarizePromptTemplate, stdout)
	lockKey = sumSessionID
	if lockKey == "" {
		lockKey = "chat:" + r.chatSessionPath() + ":" + d.ID
	}
	unlock = r.gate.lock(lockKey)
	sumStdout, _, err := r.runnerFor(LaneChat).Run(ctx, sumPrompt, sumSessionID)
	unlock()
	if err != nil {
		r.log.Error("delegation summarize failed: %v", err)
		r.markRead(d.Chat)
		r.sendChunked(errorNote(err, ""))
		return
	}
	reply := extractReply(sumStdout)
	r.markRead(d.Chat)
	r.sendChunked(reply)
	r.log.Info("delegation done delegate_id=%s reply_len=%d duration=%.1fs", d.ID, len(reply), time.Since(d.CreatedAt).Seconds())
}

// logRollover mencatat alasan sesi di-rollover supaya kelihatan di relay.log.
func (r *Relay) logRollover(lane, reason string) {
	if reason == "" {
		return
	}
	r.log.Info("session rollover lane=%s reason=%s -> mulai sesi baru", lane, reason)
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

func pickRandom(messages []string) string {
	pool := make([]string, 0, len(messages))
	for _, m := range messages {
		if strings.TrimSpace(m) != "" {
			pool = append(pool, m)
		}
	}
	switch len(pool) {
	case 0:
		return ""
	case 1:
		return pool[0]
	default:
		return pool[rand.Intn(len(pool))]
	}
}

// sendScheduled mengirim ack pertama (setelah ack_delay_seconds) dan follow-up
// berikutnya, tapi hanya selama task belum selesai. after_seconds dihitung dari
// awal task. ctx di sini sudah dilepas dari context handleMessage
// (context.WithoutCancel), jadi pengiriman yang sedang jalan tidak kepotong saat
// handleMessage return; pemberhentian diatur eksplisit oleh pemanggil setelah
// reply final terkirim.
func (r *Relay) sendScheduled(ctx context.Context, item taskItem) {
	start := time.Now()
	if r.cfg.AckDelaySeconds > 0 && len(r.cfg.AckMessages) > 0 {
		if !waitUntil(ctx, start, r.cfg.AckDelaySeconds) {
			return
		}
		r.deliver(item, "ack", r.cfg.AckDelaySeconds, pickRandom(r.cfg.AckMessages))
	}
	for _, fu := range r.cfg.AckFollowups {
		if fu.AfterSeconds <= 0 {
			continue
		}
		if !waitUntil(ctx, start, fu.AfterSeconds) {
			return
		}
		r.deliver(item, "followup", fu.AfterSeconds, pickRandom(fu.Messages))
	}
}

// waitUntil menunggu sampai start+afterSeconds, atau berhenti (return false)
// kalau ctx dibatalkan lebih dulu.
func waitUntil(ctx context.Context, start time.Time, afterSeconds int) bool {
	wait := time.Until(start.Add(time.Duration(afterSeconds) * time.Second))
	if wait < 0 {
		wait = 0
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (r *Relay) deliver(item taskItem, kind string, afterSeconds int, msg string) {
	if msg == "" {
		return
	}
	if err := r.sender.Send(msg); err != nil {
		r.log.Error("%s send failed id=%s after=%ds err=%v", kind, item.ID, afterSeconds, err)
		return
	}
	r.log.Info("%s sent id=%s after=%ds", kind, item.ID, afterSeconds)
}

func errorNote(err error, stderr string) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "(error: timeout: agent terlalu lama)"
	}
	var notFound *exec.Error
	if errors.Is(err, exec.ErrNotFound) || errors.As(err, &notFound) {
		return "(error: opencode tidak ditemukan)"
	}
	var exitErr *exec.ExitError
	code := "unknown"
	if errors.As(err, &exitErr) {
		code = fmt.Sprintf("%d", exitErr.ExitCode())
	}
	note := "(error: opencode exit " + code
	if tail := truncateRunes(strings.TrimSpace(stderr), stderrMaxRune); tail != "" {
		note += ": " + tail
	}
	return note + ")"
}

func (r *Relay) sendChunked(text string) {
	text = strings.TrimSpace(text)
	if text == "" {
		text = "(kosong)"
	}
	chunks := chunkText(text, defaultChunk)
	for i, chunk := range chunks {
		err := r.sender.Send(chunk)
		if err != nil {
			r.log.Error("wacli send chunk %d/%d ok=false err=%v", i+1, len(chunks), err)
			if i == 0 && text != "(kosong)" {
				r.sender.Send("(kosong)")
			}
			continue
		}
		r.log.Info("wacli send chunk %d/%d ok=true", i+1, len(chunks))
	}
}

func chunkText(text string, size int) []string {
	if size <= 0 {
		size = defaultChunk
	}
	runes := []rune(text)
	if len(runes) == 0 {
		return []string{"(kosong)"}
	}
	var out []string
	for i := 0; i < len(runes); i += size {
		end := i + size
		if end > len(runes) {
			end = len(runes)
		}
		out = append(out, string(runes[i:end]))
	}
	return out
}

// parseSessionID mengambil sessionID dari event NDJSON pertama yang memuatnya.
func parseSessionID(stdout string) string {
	for _, line := range strings.Split(stdout, "\n") {
		line = strings.TrimSpace(line)
		if len(line) < 2 || line[0] != '{' {
			continue
		}
		var ev struct {
			SessionID string `json:"sessionID"`
		}
		if json.Unmarshal([]byte(line), &ev) == nil && ev.SessionID != "" {
			return ev.SessionID
		}
	}
	return ""
}

var thinkingRe = regexp.MustCompile(`(?s)<think(?:ing)?>.*?</think(?:ing)?>`)

var (
	thinkingClosers = []string{"</thinking>", "</think>"}
	thinkingOpeners = []string{"<thinking>", "<think>"}
)

// stripReasoning membuang blok/tag reasoning yang kadang ikut tercampur di dalam
// part teks (mis. "</thinking>" atau "</think>" nyasar), supaya yang terkirim
// hanya balasan asli.
func stripReasoning(s string) string {
	if !strings.Contains(s, "think") {
		return s
	}
	s = thinkingRe.ReplaceAllString(s, "")
	// Tag penutup tanpa pembuka: ambil teks setelah kemunculan terakhir.
	best, tagLen := -1, 0
	for _, tag := range thinkingClosers {
		if i := strings.LastIndex(s, tag); i > best {
			best, tagLen = i, len(tag)
		}
	}
	if best >= 0 {
		s = s[best+tagLen:]
	}
	// Tag pembuka tanpa penutup: buang dari kemunculan pertama sampai akhir.
	best, tagLen = -1, 0
	for _, tag := range thinkingOpeners {
		if i := strings.Index(s, tag); i >= 0 && (best < 0 || i < best) {
			best, tagLen = i, len(tag)
		}
	}
	if best >= 0 {
		s = s[:best]
	}
	return s
}

// extractReply pulls the agent's answer out of NDJSON stdout. It never panics.
func extractReply(stdout string) (result string) {
	defer func() {
		if recover() != nil {
			result = ""
		}
		result = strings.TrimSpace(stripReasoning(result))
	}()
	var parts []string
	var fallbacks []string
	for _, line := range strings.Split(stdout, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var obj map[string]any
		if json.Unmarshal([]byte(line), &obj) != nil {
			continue
		}
		if t, _ := obj["type"].(string); t == "text" {
			if part, ok := obj["part"].(map[string]any); ok {
				if text, ok := part["text"].(string); ok && text != "" {
					parts = append(parts, text)
				}
			}
		}
		fallbacks = append(fallbacks, collectTextFields(obj)...)
	}
	if len(parts) > 0 {
		return strings.TrimSpace(strings.Join(parts, "\n"))
	}
	if len(fallbacks) > 0 {
		return strings.TrimSpace(strings.Join(fallbacks, "\n"))
	}
	lines := strings.Split(stdout, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if strings.TrimSpace(lines[i]) != "" {
			return strings.TrimSpace(lines[i])
		}
	}
	return ""
}

func collectTextFields(v any) []string {
	var out []string
	switch t := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if k == "text" {
				if s, ok := t[k].(string); ok && s != "" {
					out = append(out, s)
					continue
				}
			}
			out = append(out, collectTextFields(t[k])...)
		}
	case []any:
		for _, item := range t {
			out = append(out, collectTextFields(item)...)
		}
	}
	return out
}

func truncateRunes(s string, max int) string {
	runes := []rune(s)
	if len(runes) <= max {
		return s
	}
	return string(runes[:max])
}
