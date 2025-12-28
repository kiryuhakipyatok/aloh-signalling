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
	regType     = "reg"
	connType    = "conn"
	disconnType = "disconn"
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
		errMsg = "failed to decode msg"
		log.Error(errMsg, logger.Err(err))
		ss.writeMsg(stream, errMsg, addr)
		return err
	}
	if msg.Type != regType {
		errMsg = "wrong message type to register"
		log.Error(errMsg, logger.Attr("msgType", msg.Type), logger.Err(err))
		ss.writeMsg(stream, errMsg, addr)
		return err
	}
	regMsg, err := protocols.ToRegisterConnectMessage(msg.Data)
	if err != nil {
		errMsg = "failed to unmarshal registration data"
		log.Error(errMsg, logger.Err(err))
		ss.writeMsg(stream, errMsg, addr)
		return err
	}
	if err := ss.ConnectionRepo.AddConnect(ctx, regMsg.ID, conn); err != nil {
		log.Error("failed to store connect", logger.Attr("userID", regMsg.ID))
		return errs.NewAppError(op, err)
	}
	log.Info("user registered", logger.Attr("userID", regMsg.ID))
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
	op := "commandLoop"
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
			case connType:
				go func() {
					if err := ss.proxing(ctx, stream, msg.Data, conn); err != nil {
						log.Error("proxing failed", logger.Err(err))
						return
					}
				}()
			case disconnType:
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
		ss.writeMsg(stream, "failed to unmarshal connection data", userAddr)
		return errs.NewAppError(op, err)
	}
	log.Info("getting receiver connecting")
	var errMsg string
	receiverConn, err := ss.ConnectionRepo.GetConnect(ctx, connMsg.RecevierID)
	if err != nil {
		errMsg = "failed to get receiver"
		log.Error(errMsg, logger.Err(err))
		ss.writeMsg(stream, err.Error(), userAddr)
		return errs.NewAppError(op, err)
	}
	log.Info("opening receivers stream")
	receiverStream, err := receiverConn.OpenStreamSync(ctx)
	if err != nil {
		errMsg = "failed to open receiver stream"
		log.Error(errMsg, logger.Err(err))
		ss.writeMsg(stream, errMsg, userAddr)
		return errs.NewAppError(op, err)
	}
	log.Info("receivers stream is opened")
	receiverAddr := receiverConn.RemoteAddr().String()
	log.Info("opening users stream")
	userStream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		errMsg = "failed to open user stream"
		log.Error(errMsg, logger.Err(err))
		ss.writeMsg(stream, errMsg, receiverAddr)
		return errs.NewAppError(op, err)
	}
	log.Info("users stream is opened")
	if cErr := ss.writeMsg(receiverStream, fmt.Sprintf("new connection with: %s\n", userAddr), receiverAddr); cErr != nil {
		return errs.NewAppError(op, cErr)
	}
	if cErr := ss.writeMsg(userStream, fmt.Sprintf("new connection with: %s\n", receiverAddr), userAddr); cErr != nil {
		return errs.NewAppError(op, cErr)
	}
	logUserAddr := logger.Attr("userAddress", userAddr)
	logReceiverAddr := logger.Attr("receiverAddress", receiverAddr)
	log.Info("users are connected", logUserAddr, logReceiverAddr)
	errChan := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Go(func() {
		_, err := io.Copy(userStream, receiverStream)
		errChan <- err
	})
	wg.Go(func() {
		_, err := io.Copy(receiverStream, userStream)
		errChan <- err
	})
	if err := ss.writeMsg(stream, "connected successfully", userAddr); err != nil {
		return errs.NewAppError(op, err)
	}
	res := <-errChan
	if res != nil {
		if err := ss.checkErr(ctx, res); err != nil {
			log.Error("proxing connection is broken", logger.Err(err))
		} else {
			log.Info("proxing connection closed normally")
		}
	}

	wg.Go(func() {
		log.Info("user stream closing...", logUserAddr)
		if err := userStream.Close(); err != nil {
			log.Error("failed to close user stream", logger.Err(err), logUserAddr)
		} else {
			log.Info("user stream is closed", logUserAddr)
		}
	})
	wg.Go(func() {
		log.Info("receiver stream closing...", logReceiverAddr)
		if err := receiverStream.Close(); err != nil {
			log.Error("failed to close receiver stream", logger.Err(err), logReceiverAddr)
		} else {
			log.Info("receiver stream is closed", logReceiverAddr)
		}
	})
	wg.Wait()
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
			log.Info("client done")
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
