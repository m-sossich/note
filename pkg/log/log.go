package log

import (
	"log/slog"
	"os"
)

// Level aliases slog.Level so callers don't need to import log/slog directly.
type Level = slog.Level

var currentLevel slog.LevelVar

// Setup installs a text slog handler on stdout. Call once before Start.
func Setup(level Level) {
	currentLevel.Set(level)
	h := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: &currentLevel})
	slog.SetDefault(slog.New(h))
}

func SetLevel(level Level) {
	currentLevel.Set(level)
}
