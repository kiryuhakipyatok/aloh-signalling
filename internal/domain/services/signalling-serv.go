package services

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"test/internal/domain/repository"
	"test/internal/protocols"
	"test/pkg/errs"
	"test/pkg/logger"
	"test/pkg/validator"

	"github.com/quic-go/quic-go"
)

type SignallingService interface {
	ServeConnection(ctx context.Context, conn *quic.Conn) error
}

type signalService struct {
	ConnectionRepo repository.ConnectionsRepo
	Logger         *logger.Logger
	Validator      *validator.Validator
}

func NewSignallingService(cr repository.ConnectionsRepo, v *validator.Validator, l *logger.Logger) SignallingService {
	return &signalService{
		ConnectionRepo: cr,
		Logger:         l,
		Validator:      v,
	}
}

const (
	regType = iota
	sendType
	disconnType
)

type userConnection struct {
	userId     string
	quicConn   *quic.Conn
	ctrlStream *quic.Stream
	decoder    *json.Decoder
}

func (ss *signalService) ServeConnection(ctx context.Context, conn *quic.Conn) error {
	op := "signalService.ServeConnection"
	log := ss.Logger.AddOp(op)
	log.Info("connection serving...", logger.Attr("address", conn.RemoteAddr().String()))
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
		msg protocols.Message
	)
	addr := conn.RemoteAddr().String()
	userConnection := &userConnection{
		quicConn:   conn,
		ctrlStream: stream,
		decoder:    decoder,
	}

	if err := ss.processMsg(userConnection, &msg, addr); err != nil {
		log.Error("failed to produce message", logger.Err(err))
	}

	if *msg.Type != regType {

		log.Error("invalid message type", logger.Attr("msgType", msg.Type))
		typeErr, merr := protocols.InvalidTypeErrorMessage(msg.Id)
		if merr != nil {
			log.Error("failed to build stream error message", logger.Err(merr))
		}
		ss.writeMsg(stream, typeErr, addr)
		return err
	}
	regMsg, err := protocols.ToRegisterConnectMessage(ss.Validator, msg.Data)
	if err != nil {
		return ss.processError(userConnection, err, msg.Id)
	}
	if err := ss.ConnectionRepo.AddConnect(ctx, regMsg.ID, conn); err != nil {
		log.Error("faield to add connect", logger.Attr("userID", regMsg.ID), logger.Err(err))
		return ss.processError(userConnection, err, regMsg.ID)
	}
	log.Info("user is registered", logger.Attr("userID", regMsg.ID))
	if err := ss.writeMsg(stream, []byte("success"), addr); err != nil {
		return errs.NewAppError(op, err)
	}
	defer func() {
		if err := ss.ConnectionRepo.DeleteConnect(ctx, regMsg.ID); err != nil {
			log.Error("faield to delete connect", logger.Attr("userID", regMsg.ID), logger.Err(err))
			ss.processError(userConnection, err, regMsg.ID)
		}
		log.Info("connect is deleted", logger.Attr("userID", regMsg.ID))
	}()
	userConnection.userId = regMsg.ID
	return ss.commandLoop(ctx, userConnection)
}

func (ss *signalService) commandLoop(ctx context.Context, uc *userConnection) error {
	op := "signalService.commandLoop"
	log := ss.Logger.AddOp(op)
	log.Info("serving connection in command loop...")
	addr := uc.quicConn.RemoteAddr().String()
	defer func() {
		if err := uc.ctrlStream.Close(); err != nil {
			log.Error("failed to close command stream", logger.Attr("userId", uc.userId), logger.Err(err))
			streamErr, merr := protocols.StreamErrorMessage("")
			if merr != nil {
				log.Error("failed to build stream error message", logger.Err(merr))
			}
			ss.writeMsg(uc.ctrlStream, streamErr, addr)
		} else {
			log.Info("command stream is closed", logger.Attr("address", addr))
		}
	}()

	for {
		select {
		case <-uc.ctrlStream.Context().Done():
			log.Info("command stream is done", logger.Attr("address", addr))
			return nil
		case <-ctx.Done():
			log.Info("server context is done", logger.Attr("address", addr))
			return ctx.Err()
		default:
		}
		var msg protocols.Message
		if err := ss.processMsg(uc, &msg, addr); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			log.Error("failed to process message", logger.Err(err))
			return errs.NewAppError(op, err)
		}
		switch *msg.Type {
		case sendType:
			go func() {
				if err := ss.sendMsg(ctx, uc, msg.Data, msg.Id); err != nil {
					log.Error("message sending is failed", logger.Err(err), logger.Attr("address", addr))
					return
				}
			}()
		case disconnType:
			log.Info("user disconnecting...", logger.Attr("address", addr))
			if err := uc.quicConn.CloseWithError(0, "user disconnected"); err != nil {
				log.Error("faield to disconnect user", logger.Err(err))
				return err
			}
			log.Info("user disconnected successfully", logger.Attr("address", addr))
			return nil
		default:
			log.Error("invalid message type", logger.Attr("msgType", msg.Type))
			typeErr, merr := protocols.InvalidTypeErrorMessage(msg.Id)
			if merr != nil {
				log.Error("failed to build stream error message", logger.Err(merr))
			}
			ss.writeMsg(uc.ctrlStream, typeErr, addr)
		}
	}

}

func (ss *signalService) sendMsg(ctx context.Context, uc *userConnection, payloadData []byte, msgId string) error {
	op := "signalService.sendMsg"
	log := ss.Logger.AddOp(op)

	log.Info("message sending...")
	userAddr := uc.quicConn.RemoteAddr().String()
	sendPayloadMsg, err := protocols.ToSendPayloadMessage(ss.Validator, payloadData)
	if err != nil {
		log.Error("failed to cast send message", logger.Err(err))
		return ss.processError(uc, err, msgId)
	}
	replyMsg, err := protocols.NewReplyMessage(uc.userId, sendPayloadMsg.Payload)
	if err != nil {
		log.Error("failed to send cast reply message", logger.Err(err))
		return ss.processError(uc, err, msgId)
	}
	log.Info("getting receivers connections")
	receiverConns, err := ss.ConnectionRepo.GetConnects(ctx, sendPayloadMsg.RecevierIDs)
	if err != nil {
		log.Error("failed to get contacts", logger.Err(err))
		return ss.processError(uc, err, msgId)
	}

	var wg sync.WaitGroup
	log.Info("opening receivers streams")
	for _, rConn := range receiverConns {
		wg.Go(func() {
			receiverAddr := rConn.RemoteAddr().String()
			logUserAddr := logger.Attr("userAddress", userAddr)
			logReceiverAddr := logger.Attr("receiverAddress", receiverAddr)
			receiverStream, err := rConn.OpenUniStreamSync(ctx)
			if err != nil {
				log.Error("failed to open receiver's stream", logger.Err(err), logReceiverAddr)
				ss.processError(uc, err, msgId)
			}
			defer func() {
				log.Info("closing receiver's stream", logReceiverAddr)
				if err := receiverStream.Close(); err != nil {
					log.Error("failed to close receiver's stream", logReceiverAddr)
					ss.processError(uc, err, msgId)
				} else {
					log.Info("receiver's stream is closed successfully", logReceiverAddr)
				}
			}()
			log.Info("receiver's stream is opened")
			if _, err := receiverStream.Write(replyMsg); err != nil {
				log.Error("failed to write message to receiver", logger.Err(err))
				ss.processError(uc, err, msgId)
			}

			log.Info("message sended successfully", logUserAddr, logReceiverAddr)
			ss.writeMsg(uc.ctrlStream, []byte("message sended successfully"), msgId)
		})
	}

	wg.Wait()

	return nil
}
