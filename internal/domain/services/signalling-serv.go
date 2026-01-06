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

func (ss *signalService) validate(s any) {
	if err := ss.Validator.Validate.Struct(s); err != nil {

	}
}

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
		errMsg = "invalid protocol"
		log.Error(errMsg, logger.Err(err))
		dataErr, merr := protocols.InvalidDataErrorMessage(msg.Id, errMsg)
		if merr != nil {
			log.Error("failed to build stream error message")

		}
		ss.writeMsg(stream, dataErr, addr)
		return errs.ErrDecodeMsg(op)
	}
	if err := ss.Validator.Validate.Struct(msg); err != nil {
		errMsg := "validation error"
		log.Error(errMsg, logger.Err(err))
		valErr, merr := protocols.ValidationErrorMessage(msg.Id, errMsg)
		if merr != nil {
			log.Error("failed to build stream error message")
		}
		ss.writeMsg(stream, valErr, addr)

		return errs.ErrValidation(op)
	}

	if *msg.Type != regType {
		err = errs.ErrWrongMessageType(op)
		log.Error(errMsg, logger.Attr("msgType", msg.Type), logger.Err(err))
		dataErr, merr := protocols.InvalidDataErrorMessage(msg.Id, err.Error())
		if merr != nil {
			log.Error("failed to build stream error message")

		}
		ss.writeMsg(stream, dataErr, addr)
		return err
	}
	regMsg, err := protocols.ToRegisterConnectMessage(ss.Validator, msg.Data)
	if err != nil {
		errM := errs.ErrInvalidProtocol(op)
		log.Error(errM.Error(), logger.Err(err))
		dataErr, merr := protocols.InvalidDataErrorMessage(msg.Id, errM.Error())
		if merr != nil {
			log.Error("failed to build stream error message")

		}
		ss.writeMsg(stream, dataErr, addr)
		return err
	}
	if err := ss.ConnectionRepo.AddConnect(ctx, regMsg.ID, conn); err != nil {
		log.Error(err.Error(), logger.Attr("userID", regMsg.ID))
		servErr, merr := protocols.InternalServerErrorMessage(msg.Id, err.Error())
		if merr != nil {
			log.Error("failed to build stream error message")

		}
		ss.writeMsg(stream, servErr, addr)
		return errs.NewAppError(op, err)
	}
	log.Info("user is registered", logger.Attr("userID", regMsg.ID))
	if err := ss.writeMsg(stream, []byte("success"), addr); err != nil {
		return errs.NewAppError(op, err)
	}
	defer func() {
		if err := ss.ConnectionRepo.DeleteConnect(ctx, regMsg.ID); err != nil {
			log.Error(err.Error(), logger.Attr("userID", regMsg.ID))
			servErr, merr := protocols.InternalServerErrorMessage(msg.Id, err.Error())
			if merr != nil {
				log.Error("failed to build stream error message")
			}
			ss.writeMsg(stream, servErr, addr)
		}
		log.Info("connect is deleted", logger.Attr("userID", regMsg.ID))
	}()
	return ss.commandLoop(ctx, decoder, stream, conn, regMsg.ID, msg.Id)
}

func (ss *signalService) commandLoop(ctx context.Context, decoder *json.Decoder, stream *quic.Stream, conn *quic.Conn, userId, msgId string) error {
	op := "signalService.commandLoop"
	log := ss.Logger.AddOp(op)
	log.Info("serving connection in command loop...")
	addr := conn.RemoteAddr().String()
	defer func() {
		if err := stream.Close(); err != nil {
			log.Error("failed to close command stream", logger.Attr("address", addr), logger.Err(err))
			streamErr, merr := protocols.StreamErrorMessage(msgId, err.Error())
			if merr != nil {
				log.Error("failed to build stream error message")
			}
			ss.writeMsg(stream, streamErr, addr)
		} else {
			log.Info("command stream is closed", logger.Attr("address", addr))
		}
	}()
	select {
	case <-stream.Context().Done():
		log.Info("command stream is done", logger.Attr("address", addr))
		return nil
	default:
		for {
			var msg protocols.Message
			err := decoder.Decode(&msg)
			if err != nil {
				if cErr := ss.checkErr(ctx, err); cErr != nil {
					log.Error("failed to decode msg", logger.Err(err))
					streamErr, merr := protocols.InvalidDataErrorMessage(msg.Id, err.Error())
					if merr != nil {
						log.Error("failed to build stream error message")

					}
					ss.writeMsg(stream, streamErr, addr)
					return errs.NewAppError(op, cErr)
				}
				return nil
			}
			if err := ss.Validator.Validate.Struct(msg); err != nil {
				errMsg := "validation error"
				valErr, merr := protocols.ValidationErrorMessage(msg.Id, errMsg)
				if merr != nil {
					log.Error("failed to build stream error message")
				}
				ss.writeMsg(stream, valErr, addr)
				log.Error(errMsg, logger.Err(err))
				return errs.ErrValidation(op)
			}
			switch *msg.Type {
			case sendType:
				go func() {
					if err := ss.proxing(ctx, stream, msg.Data, conn, userId, msg.Id); err != nil {
						log.Error("proxing failed", logger.Err(err), logger.Attr("address", addr))
						// streamErr, merr := protocols.InternalServerErrorMessage(msg.Id, err.Error())
						// if merr != nil {
						// 	log.Error("failed to build stream error message")
						// }
						// ss.writeMsg(stream, streamErr, addr)
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
				errMsg := "unknown command"
				log.Error(errMsg, logger.Attr("msgType", msg.Type))
				streamErr, merr := protocols.InvalidDataErrorMessage(msg.Id, errMsg)
				if merr != nil {
					log.Error("failed to build stream error message")

				}
				ss.writeMsg(stream, streamErr, addr)
			}
		}
	}
}

func (ss *signalService) proxing(ctx context.Context, stream *quic.Stream, payloadData []byte, conn *quic.Conn, userId, msgId string) error {
	op := "signalService.proxing"
	log := ss.Logger.AddOp(op)

	log.Info("proxing...")
	userAddr := conn.RemoteAddr().String()
	sendPayloadMsg, err := protocols.ToSendPayloadMessage(ss.Validator, payloadData)
	if err != nil {
		log.Error(err.Error(), logger.Err(err))
		streamErr, merr := protocols.InvalidDataErrorMessage(msgId, err.Error())
		if merr != nil {
			log.Error("failed to build stream error message")

		}
		ss.writeMsg(stream, streamErr, userAddr)
		return errs.NewAppError(op, err)
	}
	replyMsg, err := protocols.NewReplyMessage(userId, sendPayloadMsg.Payload)
	if err != nil {
		log.Error(err.Error(), logger.Err(err))
		streamErr, merr := protocols.InvalidDataErrorMessage(msgId, err.Error())
		if merr != nil {
			log.Error("failed to build stream error message")

		}
		ss.writeMsg(stream, streamErr, userAddr)
		return errs.NewAppError(op, err)
	}
	log.Info("getting receivers connections")
	var errMsg string
	receiverConns, err := ss.ConnectionRepo.GetConnects(ctx, sendPayloadMsg.RecevierIDs)
	if err != nil {
		log.Error(err.Error(), logger.Err(err))
		streamErr, merr := protocols.InternalServerErrorMessage(msgId, err.Error())
		if merr != nil {
			log.Error("failed to build stream error message")

		}
		ss.writeMsg(stream, streamErr, userAddr)
		return errs.NewAppError(op, err)
	}

	// gErrChan := make(chan error, 1)
	var wg sync.WaitGroup
	log.Info("opening receivers streams")
	for _, rConn := range receiverConns {
		wg.Go(func() {
			receiverAddr := rConn.RemoteAddr().String()
			logUserAddr := logger.Attr("userAddress", userAddr)
			logReceiverAddr := logger.Attr("receiverAddress", receiverAddr)
			receiverStream, err := rConn.OpenUniStreamSync(ctx)
			if err != nil {
				errMsg = "failed to open receiver's stream"
				log.Error(errMsg, logger.Err(err), logReceiverAddr)
				streamErr, merr := protocols.StreamErrorMessage(msgId, errMsg)
				if merr != nil {
					log.Error("failed to build stream error message")
					// gErrChan <- errs.NewAppError(op, merr)
				}
				ss.writeMsg(stream, streamErr, receiverAddr)
				// gErrChan <- errs.NewAppError(op, err)

			}
			defer func() {
				log.Info("closing receiver's stream", logReceiverAddr)
				if err := receiverStream.Close(); err != nil {
					errMsg := "failed to close receiver's stream"
					log.Error(errMsg, logReceiverAddr)
					streamErr, merr := protocols.StreamErrorMessage(msgId, errMsg)
					if merr != nil {
						log.Error("failed to build stream error message")
						// gErrChan <- errs.NewAppError(op, merr)
					}
					ss.writeMsg(stream, streamErr, receiverAddr)
					// gErrChan <- errs.NewAppError(op, err)
				} else {
					log.Info("receiver's stream is closed successfully", logReceiverAddr)
				}
			}()
			log.Info("receiver's stream is opened")
			if _, err := receiverStream.Write(replyMsg); err != nil {
				log.Error("failed to write message to receiver", logger.Err(err))
				streamErr, merr := protocols.StreamErrorMessage(msgId, errMsg)
				if merr != nil {
					log.Error("failed to build stream error message")
					// gErrChan <- errs.NewAppError(op, merr)
				}
				ss.writeMsg(stream, streamErr, receiverAddr)
			}

			log.Info("message sended successfully", logUserAddr, logReceiverAddr)
			ss.writeMsg(stream, []byte("message sended successfully"), msgId)

		})
	}
	// go func() {
	wg.Wait()
	// close(gErrChan)
	// }()
	// if err = <-gErrChan; err != nil {
	// 	return err
	// }
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

func (ss *signalService) writeMsg(stream *quic.Stream, msg []byte, addr string) error {
	op := "signalService.writeMsg"
	log := ss.Logger.AddOp(op)
	log.Info("writting message...")
	if _, err := stream.Write(msg); err != nil {
		log.Error("failed to write message", logger.Err(err))
		return errs.NewAppError(op, err)
	}
	log.Info("message written", logger.Attr("streamId", stream.StreamID()), logger.Attr("address", addr))
	return nil
}
