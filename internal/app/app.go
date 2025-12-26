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
)

func Run() {
	path := os.Getenv("CONFIG_PATH")
	cfg := config.NewConfig(path)
	logger := logger.NewLogger(cfg.App.Env)
	log := logger.AddOp("app.Run")
	log.Info("app config loaded")
	connRepo := repository.NewConnectionsRepo()
	log.Info("repos created")
	signallingSerivice := services.NewSignallingService(connRepo, logger)
	log.Info("services created")
	serv := server.NewServer(cfg.Server, logger)
	log.Info("server started")
	ctx, cancel := context.WithCancel(context.Background())
	quitChan := make(chan os.Signal, 1)
	signal.Notify(quitChan, syscall.SIGTERM, syscall.SIGINT)
	go serv.AcceptConnections(ctx, signallingSerivice.ServeConnection)
	<-quitChan
	log.Info("app closing...")
	cancel()
	close(quitChan)
	log.Info("app closed")
}
