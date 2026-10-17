package relay

import "time"

const (
	// MaxSeen adalah jumlah id pesan terakhir yang diingat untuk dedupe.
	MaxSeen = 500

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
