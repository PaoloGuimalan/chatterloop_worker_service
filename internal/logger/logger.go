package logger

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
)

const (
	reset  = "\033[0m"
	bold   = "\033[1m"
	gray   = "\033[90m"
	red    = "\033[31m"
	green  = "\033[32m"
	yellow = "\033[33m"
	blue   = "\033[34m"
	cyan   = "\033[36m"
)

type handler struct {
	mu    *sync.Mutex
	out   io.Writer
	level slog.Level
	attrs []slog.Attr
}

// Setup installs a colored console logger as the slog default, so plain
// slog.Info calls anywhere in the project pick it up.
func Setup(level slog.Level) {
	slog.SetDefault(slog.New(&handler{
		mu:    &sync.Mutex{},
		out:   os.Stdout,
		level: level,
	}))
}

func (h *handler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level
}

func (h *handler) Handle(_ context.Context, r slog.Record) error {
	var sb strings.Builder

	sb.WriteString(gray);sb.WriteString(r.Time.Format("15:04:05"));sb.WriteString(reset);sb.WriteString(" ")
	sb.WriteString(levelColor(r.Level));sb.WriteString(fmt.Sprintf("%-5s", r.Level.String()));sb.WriteString(reset);sb.WriteString(" ")
	sb.WriteString(r.Message)

	appendAttr := func(a slog.Attr) bool {
		sb.WriteString(" " + cyan);sb.WriteString(a.Key);sb.WriteString(reset);sb.WriteString("=");sb.WriteString(a.Value.String())
		return true
	}
	for _, a := range h.attrs {
		appendAttr(a)
	}
	r.Attrs(appendAttr)

	sb.WriteString("\n")

	h.mu.Lock()
	defer h.mu.Unlock()

	_, err := io.WriteString(h.out, sb.String())
	return err
}

func (h *handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	// Copy rather than append in place: slog may call this concurrently from
	// sibling loggers sharing the same backing array.
	merged := make([]slog.Attr, 0, len(h.attrs)+len(attrs))
	merged = append(merged, h.attrs...)
	merged = append(merged, attrs...)

	return &handler{mu: h.mu, out: h.out, level: h.level, attrs: merged}
}

func (h *handler) WithGroup(string) slog.Handler { return h }

func levelColor(l slog.Level) string {
	switch {
	case l >= slog.LevelError:
		return red + bold
	case l >= slog.LevelWarn:
		return yellow
	case l >= slog.LevelInfo:
		return green
	default:
		return blue
	}
}

// Banner prints the startup block. Never pass the DSN or password here.
func Banner(addr, driver, host, database string) {
	fmt.Println()
	fmt.Println(bold + "  worker_service" + reset)
	fmt.Println(gray + "  ─────────────────────────────────────────" + reset)
	fmt.Printf("  %-10s %s\n", "listen", bold+"http://localhost"+addr+reset)
	fmt.Printf("  %-10s %s\n", "driver", driver)
	fmt.Printf("  %-10s %s\n", "database", database)
	fmt.Printf("  %-10s %s\n", "host", host)
	fmt.Println(gray + "  ─────────────────────────────────────────" + reset)
	fmt.Println(gray + "  Ctrl+C to stop" + reset)
	fmt.Println()
}