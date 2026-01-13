package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"test/internal/config"
	"test/pkg/logger"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlog"
)

type Server struct {
	Listener *quic.Listener
	Logger   *logger.Logger
	Cfg      config.Server
}

func loadCerts(certPath string) (*tls.Certificate, error) {
	certFile := fmt.Sprintf("%s/server.crt", certPath)
	keyFile := fmt.Sprintf("%s/server.key", certPath)
	certs, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	return &certs, nil
}

func NewServer(cfg config.Server, l *logger.Logger) *Server {
	udpConn, err := net.ListenUDP("udp4", &net.UDPAddr{Port: cfg.Port})
	if err != nil {
		panic(fmt.Errorf("failed to listen UDP: %w", err))
	}
	tr := quic.Transport{
		Conn: udpConn,
	}
	certs, err := loadCerts(cfg.CertsPath)
	if err != nil {
		panic(fmt.Errorf("failed to load tls certificates: %w", err))
	}
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
		GetCertificate: func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
			return loadCerts(cfg.CertsPath)
		},
		Certificates: []tls.Certificate{*certs},
		NextProtos:   cfg.NextProtos,
	}
	quicConfig := &quic.Config{
		MaxIdleTimeout:       cfg.IdleTimeout,
		MaxIncomingStreams:   cfg.MaxIncomingStreams,
		EnableDatagrams:      true,
		HandshakeIdleTimeout: cfg.HandshakeTimeout,
		KeepAlivePeriod:      cfg.KeepAlivePeriodTimeout,
		Tracer:               qlog.DefaultConnectionTracer,
	}
	listener, err := tr.Listen(tlsConfig, quicConfig)
	if err != nil {
		panic(fmt.Errorf("failed to start listen QUIC connections: %w", err))
	}
	return &Server{
		Listener: listener,
		Cfg:      cfg,
		Logger:   l,
	}
}

func (s *Server) AcceptConnections(ctx context.Context, handler func(ctx context.Context, conn *quic.Conn) error) {
	op := "server.AcceptConnections"
	log := s.Logger.AddOp(op)
	log.Info("accepting connections...")
	var (
		addr string
		wg   sync.WaitGroup
	)
	go func() {
		wg.Wait()
	}()

	for {
		conn, err := s.Listener.Accept(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
				return
			}
			log.Error("failed to accept connection", logger.Err(err))
			continue
		} else {
			addr = conn.RemoteAddr().String()
			log.Info("new connection", logger.Attr("address", conn.RemoteAddr().String()))
		}
		wg.Go(func() {
			if err := handler(ctx, conn); err != nil {
				log.Error("connection handler exited with error", logger.Attr("address", addr), logger.Err(err))
			} else {
				log.Info("connection handler exited normally", logger.Attr("address", addr))
			}
		})
	}

}

func (s *Server) Close() {
	if err := s.Listener.Close(); err != nil {
		panic(fmt.Errorf("failed to close server"))
	}
}
