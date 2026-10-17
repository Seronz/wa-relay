package relay

import (
	"context"
	"fmt"
	"strings"
	"time"
)

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
	sessionID := r.beginSession(item.Lane, time.Now())
	prompt := fmt.Sprintf(promptTemplate, strings.TrimSpace(item.Text))
	stdout, stderr, usedID, err := r.runRecovering(taskCtx, r.runnerFor(item.Lane), item.Lane, sessionID, item.ID, prompt)
	if usedID == "" {
		r.saveSessionFromOutput(item.Lane, stdout)
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
// dan meminta konfirmasi bos sebelum delegasi ke tim teknis. Bila semua slot
// build sedang terpakai, tugas tidak diusulkan: stepi langsung mengabari bos
// bahwa tim sedang sibuk semua.
func (r *Relay) proposeDelegation(ctx context.Context, item taskItem) {
	if !r.workAvailable() {
		r.sendBusy(ctx, item)
		r.log.Info("delegation busy id=%s", item.ID)
		return
	}
	sessionID := r.beginSession(LaneChat, time.Now())
	prompt := fmt.Sprintf(delegationPromptTemplate, strings.TrimSpace(item.Text))
	stdout, _, usedID, err := r.runRecovering(ctx, r.runnerFor(LaneChat), LaneChat, sessionID, item.ID, prompt)
	if usedID == "" {
		r.saveSessionFromOutput(LaneChat, stdout)
	}
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

// sendBusy meminta stepi mengabari bos bahwa semua slot build sedang terpakai.
// Kalau stepi gagal, pesan cadangan tetap dikirim supaya bos tidak didiamkan.
func (r *Relay) sendBusy(ctx context.Context, item taskItem) {
	r.markRead(item.Chat)
	prompt := strings.TrimSpace(r.cfg.WorkBusyPrompt)
	if prompt == "" {
		prompt = busyPromptTemplate
	}
	sessionID := r.beginSession(LaneChat, time.Now())
	stdout, _, usedID, err := r.runRecovering(ctx, r.runnerFor(LaneChat), LaneChat, sessionID, item.ID, prompt)
	if usedID == "" {
		r.saveSessionFromOutput(LaneChat, stdout)
	}
	if err != nil {
		r.log.Error("busy notice failed: %v", err)
		r.sendChunked(busyFallback)
		return
	}
	reply := extractReply(stdout)
	if reply == "" {
		reply = busyFallback
	}
	r.sendChunked(reply)
}

// runDelegation menjalankan pipeline delegasi di background: agent teknis
// (build) mengerjakan tugas, lalu stepi merangkum hasilnya untuk bos. Jalankan
// dalam goroutine sendiri agar chat tidak terblokir sambil build berjalan.
func (r *Relay) runDelegation(ctx context.Context, item taskItem, d *Delegation) {
	// Pesan satu slot build. Bila dua-duanya sudah terpakai, jangan antre diam:
	// kabari bos lewat stepi bahwa tim lagi sibuk semua.
	buildRunner, releaseBuild, ok := r.acquireWork()
	if !ok {
		r.sendBusy(ctx, taskItem{ID: item.ID, Chat: d.Chat})
		r.log.Info("delegation busy at confirm id=%s delegate_id=%s", item.ID, d.ID)
		return
	}
	defer releaseBuild()

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

	// 1. Agent teknis mengerjakan tugas (slot + sesi work terpisah).
	buildID := r.beginSession(LaneWork, time.Now())
	buildPrompt := fmt.Sprintf(buildPromptTemplate, d.Text)
	stdout, stderr, _, err := r.runRecovering(ctx, buildRunner, LaneWork, buildID, d.ID, buildPrompt)
	if err != nil {
		r.log.Error("delegation build failed: %v (stderr %dB)", err, len(stderr))
		r.markRead(d.Chat)
		r.sendChunked(errorNote(err, stderr))
		r.log.Info("delegation failed id=%s delegate_id=%s duration=%.1fs", item.ID, d.ID, time.Since(d.CreatedAt).Seconds())
		return
	}
	r.log.Info("delegation build done delegate_id=%s stdout=%dB", d.ID, len(stdout))

	// 2. Stepi merangkum hasil untuk bos (sesi chat).
	sumID := r.beginSession(LaneChat, time.Now())
	sumPrompt := fmt.Sprintf(summarizePromptTemplate, stdout)
	sumStdout, _, usedSumID, err := r.runRecovering(ctx, r.runnerFor(LaneChat), LaneChat, sumID, d.ID, sumPrompt)
	if usedSumID == "" {
		r.saveSessionFromOutput(LaneChat, sumStdout)
	}
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
