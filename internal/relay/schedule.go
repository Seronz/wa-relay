package relay

import (
	"context"
	"math/rand"
	"strings"
	"time"
)

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
