package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"test/internal/domain/repository"
	"test/internal/protocols"
	"test/pkg/errs"
	"test/pkg/logger"

	"github.com/quic-go/quic-go"
)

type SignallingService interface {
	ServeConnection(ctx context.Context, conn *quic.Conn) error
}

type signalService struct {
	ConnectionRepo repository.ConnectionsRepo
	Logger         *logger.Logger
}

func NewSignallingService(cr repository.ConnectionsRepo, l *logger.Logger) SignallingService {
	return &signalService{
		ConnectionRepo: cr,
		Logger:         l,
	}
}

const (
	REGTYPE     = "reg"
	CONNTYPE    = "conn"
	DISCONNTYPE = "disconn"

	MAXP2PUSERS = 3
)

func (ss *signalService) ServeConnection(ctx context.Context, conn *quic.Conn) error {
	op := "signalService.ServeConnection"
	log := ss.Logger.AddOp(op)
	log.Info("connection serving...", logger.Attr("address", conn.RemoteAddr()))
	stream, err := conn.AcceptStream(ctx)
	if err != nil {
		if cErr := ss.checkErr(ctx, err); cErr != nil {
			log.Error("failed to accept stream", logger.Err(cErr))
			return cErr
		}
		return nil
	}
	log.Info("new stream", logger.Attr("id", stream.StreamID()))
	decoder := json.NewDecoder(stream)
	var (
		msg    protocols.Message
		errMsg string
	)
	addr := conn.RemoteAddr().String()
	if err := decoder.Decode(&msg); err != nil {
		errMsg = "invalid message"
		log.Error(errMsg, logger.Err(err))
		ss.writeMsg(stream, errMsg, addr)
		return errs.ErrDecodeMsg(op)
	}
	if msg.Type != REGTYPE {
		err = errs.ErrWrongMessageType(op)
		log.Error(errMsg, logger.Attr("msgType", msg.Type), logger.Err(err))
		ss.writeMsg(stream, err.Error(), addr)
		return err
	}
	regMsg, err := protocols.ToRegisterConnectMessage(msg.Data)
	if err != nil {
		err = errs.ErrInvalidProtocol(op)
		log.Error(errMsg, logger.Err(err))
		ss.writeMsg(stream, err.Error(), addr)
		return err
	}
	if err := ss.ConnectionRepo.AddConnect(ctx, regMsg.ID, conn); err != nil {
		log.Error("failed to store connect", logger.Attr("userID", regMsg.ID))
		return errs.NewAppError(op, err)
	}
	log.Info("user is registered", logger.Attr("userID", regMsg.ID))
	if err := ss.writeMsg(stream, "success", addr); err != nil {
		return errs.NewAppError(op, err)
	}
	defer func() {
		if err := ss.ConnectionRepo.DeleteConnect(ctx, regMsg.ID); err != nil {
			log.Error("failed to delete connect", logger.Attr("userID", regMsg.ID))
		}
		log.Info("connect is deleted", logger.Attr("userID", regMsg.ID))
	}()
	return ss.commandLoop(ctx, decoder, stream, conn)
}

func (ss *signalService) commandLoop(ctx context.Context, decoder *json.Decoder, stream *quic.Stream, conn *quic.Conn) error {
	op := "signalService.commandLoop"
	log := ss.Logger.AddOp(op)
	log.Info("serving connection in command loop...")
	addr := conn.RemoteAddr().String()
	defer func() {
		if err := stream.Close(); err != nil {
			log.Error("failed to close command stream", logger.Attr("address", addr), logger.Err(err))
		} else {
			log.Info("command stream is closed", logger.Attr("address", addr))
		}
	}()
	select {
	case <-stream.Context().Done():
		log.Info("command stream is done")
		return nil
	default:
		for {
			var msg protocols.Message
			err := decoder.Decode(&msg)
			if err != nil {
				if cErr := ss.checkErr(ctx, err); cErr != nil {
					log.Error("failed to decode msg", logger.Err(err))
					return errs.NewAppError(op, cErr)
				}
				return nil
			}
			switch msg.Type {
			case CONNTYPE:
				go func() {
					if err := ss.proxing(ctx, stream, msg.Data, conn); err != nil {
						log.Error("proxing failed", logger.Err(err))
						return
					}
				}()
			case DISCONNTYPE:
				if err := conn.CloseWithError(0, "user disconnected"); err != nil {
					log.Error("faield to disconnect user", logger.Err(err))
					return err
				}
				log.Info("user disconnected successfully", logger.Attr("address", addr))
				return nil
			default:
				log.Error("unknown command", logger.Attr("msgType", msg.Type))
				ss.writeMsg(stream, "unknown command", addr)
			}
		}
	}
}

func (ss *signalService) proxing(ctx context.Context, stream *quic.Stream, connectData []byte, conn *quic.Conn) error {
	op := "signalService.proxing"
	log := ss.Logger.AddOp(op)

	log.Info("proxing...")
	userAddr := conn.RemoteAddr().String()
	connMsg, err := protocols.ToConnectToUserMessage(connectData)
	if err != nil {
		ss.writeMsg(stream, err.Error(), userAddr)
		return errs.NewAppError(op, err)
	}
	log.Info("getting receivers connections")
	var errMsg string
	receiverConns, err := ss.ConnectionRepo.GetConnects(ctx, connMsg.RecevierIDs)
	if err != nil {
		log.Error(errMsg, logger.Err(err))
		ss.writeMsg(stream, err.Error(), userAddr)
		return errs.NewAppError(op, err)
	}

	gErrChan := make(chan error, 1)
	var gWg sync.WaitGroup
	for _, rConn := range receiverConns {
		if len(receiverConns) <= MAXP2PUSERS {
			gWg.Go(func() {
				ss.streamProxing(ctx, conn, rConn, stream, gErrChan)
			})
		} else {
			gWg.Go(func() {
				ss.datagramProxing(ctx, stream, conn, rConn, gErrChan)
			})
		}
	}
	go func() {
		gWg.Wait()
		close(gErrChan)
	}()
	if err = <-gErrChan; err != nil {
		return err
	}
	return nil
}

func (ss *signalService) checkErr(ctx context.Context, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
		return nil
	}
	op := "signalService.checkErr"
	log := ss.Logger.AddOp(op)
	log.Info("error checking...")
	if errors.Is(err, io.EOF) {
		log.Info("connection closed (EOF)")
		return nil
	}
	var appErr *quic.ApplicationError
	if errors.As(err, &appErr) {
		if appErr.ErrorCode == 0 {
			log.Info("client is done")
			return nil
		}
	}
	log.Info("error checked")
	return errs.NewAppError(op, err)

}

func (ss *signalService) writeMsg(stream *quic.Stream, msg string, addr string) error {
	op := "signalService.writeMsg"
	log := ss.Logger.AddOp(op)
	log.Info("writting message...")
	if _, err := stream.Write([]byte(msg + "\n")); err != nil {
		log.Error("failed to write message", logger.Err(err))
		return errs.NewAppError(op, err)
	}
	log.Info("message written", logger.Attr("streamId", stream.StreamID()), logger.Attr("address", addr))
	return nil
}

func (ss *signalService) datagramProxing(ctx context.Context, stream *quic.Stream, uConn, rConn *quic.Conn, gErrChan chan error) {
	op := "signalService.datagramStream"
	log := ss.Logger.AddOp(op)
	log.Info("datagram proxing...")
	rAddr := rConn.RemoteAddr().String()
	uAddr := uConn.RemoteAddr().String()
	errChan := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Go(func() {
		if err := rConn.SendDatagram([]byte(fmt.Sprintf("new datagram connect with: %s", uAddr))); err != nil {
			errChan <- err
			return
		}
		log.Info("user's datagram pipe is opened", logger.Attr("address", uAddr))

		for {
			p, err := uConn.ReceiveDatagram(ctx)
			if err != nil {
				errChan <- err
				break
			}
			if err := rConn.SendDatagram([]byte(p)); err != nil {
				errChan <- err
				break
			}
		}
	})
	wg.Go(func() {
		if err := uConn.SendDatagram([]byte(fmt.Sprintf("new datagram connect with: %s", rAddr))); err != nil {
			errChan <- err
			return
		}
		log.Info("receiver's datagram pipe is opened", logger.Attr("address", rAddr))
		for {
			p, err := rConn.ReceiveDatagram(ctx)
			if err != nil {
				errChan <- err
				break
			}
			if err := uConn.SendDatagram([]byte(p)); err != nil {
				errChan <- err
				break
			}
		}
	})

	log.Info("users are connected with datagram pipes", logger.Attr("address", uAddr), logger.Attr("address", rAddr))
	if err := ss.writeMsg(stream, "connected successfully", uAddr); err != nil {
		gErrChan <- errs.NewAppError(op, err)
	}
	res := <-errChan
	if err := ss.checkErr(ctx, res); err != nil {
		log.Error("proxing connection is broken", logger.Err(err))
	} else {
		log.Info("proxing connection closed normally")
	}
	wg.Wait()
}

func (ss *signalService) streamProxing(ctx context.Context, uConn, rConn *quic.Conn, stream *quic.Stream, gErrChan chan error) {
	op := "signalService.streamProxing"
	log := ss.Logger.AddOp(op)
	log.Info("stream proxing...")
	userAddr := uConn.RemoteAddr().String()
	receiverAddr := rConn.RemoteAddr().String()
	logUserAddr := logger.Attr("userAddress", userAddr)
	logReceiverAddr := logger.Attr("receiverAddress", receiverAddr)
	receiverStream, err := rConn.OpenStreamSync(ctx)
	var errMsg string
	if err != nil {
		errMsg = "failed to open receiver's stream"
		log.Error(errMsg, logger.Err(err), logReceiverAddr)
		ss.writeMsg(stream, errMsg, receiverAddr)
		gErrChan <- errs.NewAppError(op, err)
	}
	log.Info("receiver's stream is opened")
	log.Info("opening user's stream")
	userStream, err := uConn.OpenStreamSync(ctx)
	if err != nil {
		errMsg = "failed to open user's stream"
		log.Error(errMsg, logger.Err(err))
		ss.writeMsg(stream, errMsg, userAddr)
		gErrChan <- errs.NewAppError(op, err)
	}
	log.Info("user's stream is opened", logUserAddr)
	if cErr := ss.writeMsg(receiverStream, fmt.Sprintf("new connection with: %s\n", userAddr), receiverAddr); cErr != nil {
		gErrChan <- errs.NewAppError(op, cErr)
	}
	if cErr := ss.writeMsg(userStream, fmt.Sprintf("new connection with: %s\n", receiverAddr), userAddr); cErr != nil {
		gErrChan <- errs.NewAppError(op, cErr)
	}

	errChan := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Go(func() {
		_, err := io.Copy(userStream, receiverStream)
		if err != nil {
			errChan <- err
		}

	})
	wg.Go(func() {
		_, err := io.Copy(receiverStream, userStream)
		if err != nil {
			errChan <- err
		}

	})
	log.Info("users are connected with streams", logUserAddr, logReceiverAddr)
	if err := ss.writeMsg(stream, "connected successfully", userAddr); err != nil {
		gErrChan <- errs.NewAppError(op, err)
	}
	res := <-errChan
	if err := ss.checkErr(ctx, res); err != nil {
		log.Error("proxing connection is broken", logger.Err(err))
	} else {
		log.Info("proxing connection closed normally")
	}
	wg.Go(func() {
		log.Info("user's stream closing...", logUserAddr)
		if err := userStream.Close(); err != nil {
			log.Error("failed to close user's stream", logger.Err(err), logUserAddr)
		} else {
			log.Info("user's stream is closed", logUserAddr)
		}
	})
	wg.Go(func() {
		log.Info("receiver's stream closing...", logReceiverAddr)
		if err := receiverStream.Close(); err != nil {
			log.Error("failed to close receiver's stream", logger.Err(err), logReceiverAddr)
		} else {
			log.Info("receiver's stream is closed", logReceiverAddr)
		}
	})
	wg.Wait()

}
