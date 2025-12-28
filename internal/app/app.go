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
	log.Info("app config is loaded")
	connRepo := repository.NewConnectionsRepo()
	log.Info("repos are created")
	signallingSerivice := services.NewSignallingService(connRepo, logger)
	log.Info("services are created")
	serv := server.NewServer(cfg.Server, logger)
	log.Info("server is started")
	ctx, cancel := context.WithCancel(context.Background())
	quitChan := make(chan os.Signal, 1)
	signal.Notify(quitChan, syscall.SIGTERM, syscall.SIGINT)
	go serv.AcceptConnections(ctx, signallingSerivice.ServeConnection)
	<-quitChan
	log.Info("app is closing...")
	cancel()
	close(quitChan)
	log.Info("server is closing...")
	serv.Close()
	log.Info("server is closed")
	log.Info("app is closed")
}
