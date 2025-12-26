package logger

import (
	"log/slog"
	"os"
)

type Logger struct {
	*slog.Logger
}

const (
	local = "local"
	dev   = "dev"
	prod  = "prod"
)

func NewLogger(env string) *Logger {
	var log *slog.Logger

	switch env {
	case local:
		log = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	case dev:
		log = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	case prod:
		log = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}

	logger := &Logger{
		log.With(slog.String("env", env)),
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
