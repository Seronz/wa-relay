package relay

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

type Logger struct {
	mu   sync.Mutex
	out  io.Writer
	file *os.File
}

func NewLogger(logPath string) (*Logger, error) {
	return newLogger(os.Stdout, logPath)
}

func newLogger(out io.Writer, logPath string) (*Logger, error) {
	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}
	// Hindari log ganda ketika stdout/stderr diarahkan ke file yang sama dengan
	// log_file (mis. `start.sh` menjalankan `./wa-relay >>relay.log 2>&1`).
	if f, ok := out.(*os.File); ok {
		if outInfo, err := f.Stat(); err == nil {
			if fileInfo, err := file.Stat(); err == nil && os.SameFile(outInfo, fileInfo) {
				out = io.Discard
			}
		}
	}
	return &Logger{out: out, file: file}, nil
}

func (l *Logger) log(level, format string, args ...any) {
	line := fmt.Sprintf("%s %s %s\n", time.Now().Format("2006-01-02 15:04:05"), level, fmt.Sprintf(format, args...))
	l.mu.Lock()
	defer l.mu.Unlock()
	io.WriteString(l.out, line)
	l.file.WriteString(line)
}

func (l *Logger) Info(format string, args ...any)  { l.log("INFO", format, args...) }
func (l *Logger) Warn(format string, args ...any)  { l.log("WARN", format, args...) }
func (l *Logger) Error(format string, args ...any) { l.log("ERROR", format, args...) }

func (l *Logger) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.file.Close()
}
