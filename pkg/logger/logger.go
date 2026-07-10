package logger

import (
	"log/slog"
	"os"
	"github.com/kiryuhakipyatok/aloh-signalling/internal/config"
)

type Logger struct {
	*slog.Logger
}

const (
	local = "local"
	dev   = "dev"
	prod  = "prod"
)

func NewLogger(cfg config.App) *Logger {
	var log *slog.Logger
	env := cfg.Env

	switch env {
	case local:
		log = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	case dev:
		log = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	case prod:
		log = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}

	logger := &Logger{
		log.With(
			slog.String("env", env),
			slog.String("app", cfg.Name),
			slog.String("version", cfg.Version),
		),
	}
	return logger
}

func (l *Logger) Info(msg string, args ...any) {
	l.Logger.Info(msg, args...)
}

func (l *Logger) Error(msg string, args ...any) {
	l.Logger.Error(msg, args...)
}

func (l *Logger) Debug(msg string, args ...any) {
	l.Logger.Debug(msg, args...)
}

func (l *Logger) AddOp(op string) *Logger {
	return &Logger{l.Logger.With(slog.String("op", op))}
}

func Err(err error) slog.Attr {
	return slog.Any("error", err)
}

func Attr(key string, val any) slog.Attr {
	return slog.Any(key, val)
}

func NewLogData(attrs ...any) []any {
	ld := make([]any, 0, len(attrs))
	ld = append(ld, attrs...)
	return ld
}
