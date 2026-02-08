package app

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"test/internal/config"
	"test/internal/domain/repository"
	"test/internal/domain/services"
	"test/internal/server"
	"test/pkg/logger"
	"test/pkg/validator"
)

func Run() {
	path := os.Getenv("CONFIG_PATH")
	cfg := config.NewConfig(path)
	logger := logger.NewLogger(cfg.App)
	logger.Info("app config is loaded")
	validator := validator.NewValidator()
	connRepo := repository.NewConnectionsRepo()
	sessRepo := repository.NewSessionsRepo()
	logger.Info("repos are created")
	signallingSerivice := services.NewSignallingService(connRepo, sessRepo, validator, logger)
	logger.Info("services are created")
	serv := server.NewServer(cfg.Server, logger)
	logger.Info("server is started")
	ctx, cancel := context.WithCancel(context.Background())
	quitChan := make(chan os.Signal, 1)
	signal.Notify(quitChan, syscall.SIGTERM, syscall.SIGINT)
	go serv.AcceptConnections(ctx, signallingSerivice.ServeConnection)
	<-quitChan
	logger.Info("app is closing...")
	cancel()
	close(quitChan)
	logger.Info("server is closing...")
	serv.Close()
	logger.Info("server is closed")
	logger.Info("app is closed")
}
