package services

import (
	"context"
	"encoding/json"
	"strings"
	"test/internal/domain/models"
	"test/internal/domain/repository"
	"test/internal/protocols"
	"test/pkg/errs"
	"test/pkg/logger"
	"test/pkg/validator"

	"github.com/quic-go/quic-go"
	"golang.org/x/sync/errgroup"
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
	REG_TYPE = iota
	STREAM_TYPE
	DATAGRAM_TYPE
	DISCONN_TYPE
	GET_ONLINE_TYPE
)

type userConnection struct {
	userId     string
	quicConn   *quic.Conn
	ctrlStream *quic.Stream
	decoder    *json.Decoder
}

func (ss *signalService) ServeConnection(ctx context.Context, conn *quic.Conn) error {
	var (
		op      = "signalService.ServeConnection"
		log     = ss.Logger.AddOp(op)
		logAddr = logger.Attr("address", conn.RemoteAddr().String())
	)

	log.Info("connection serving...", logAddr)
	stream, err := conn.AcceptStream(ctx)
	if err != nil {
		if cErr := checkErr(ctx, err); cErr != nil {
			log.Error("failed to accept stream", logger.Err(cErr), logAddr)
			return cErr
		}
		return nil
	}
	logStreamId := logger.Attr("streamId", stream.StreamID())
	log.Info("new stream", logStreamId, logAddr)
	decoder := json.NewDecoder(stream)
	var msg protocols.Message

	userConnection := &userConnection{
		quicConn:   conn,
		ctrlStream: stream,
		decoder:    decoder,
	}
	if err := ss.processMsg(userConnection, &msg); err != nil {
		if cerr := checkErr(ctx, err); cerr != nil {
			log.Error("failed to process message", logger.Err(cerr), logAddr)
			ss.closeConnection(ctx, userConnection, 1, "protocol violation")
			return errs.NewAppError(op, err)
		}
		ss.closeConnection(ctx, userConnection, 0, "client done")
		return nil
	}

	logMsgId := logger.Attr("msgId", msg.Id)

	if *msg.Type != REG_TYPE {
		msgIdLog := logger.Attr("msgType", msg.Type)
		log.Error("wrong message type", logger.NewLogData(msgIdLog, logMsgId)...)
		if perr := processError(ctx, userConnection, err, msg.Id); perr != nil {
			log.Error("failed to proccess error", logger.NewLogData(logger.Err(perr), msgIdLog, logMsgId)...)
		}
		return errs.NewAppError(op, err)
	}
	regMsg, err := protocols.ToRegisterConnectMessage(ss.Validator, msg.Data)
	if err != nil {
		log.Error("failed to cast message", logger.NewLogData(logger.Err(err), logMsgId)...)
		if perr := processError(ctx, userConnection, err, msg.Id); perr != nil {
			log.Error("failed to proccess error", logger.NewLogData(logger.Err(perr), logMsgId)...)
		}
		return errs.NewAppError(op, err)
	}
	user := &models.User{
		ID:      regMsg.ID,
		Connect: conn,
	}
	logUserId := logger.Attr("userID", user.ID)
	logUserData := logger.NewLogData(logMsgId, logUserId)
	if err := ss.ConnectionRepo.AddConnect(ctx, user); err != nil {
		log.Error("failed to add connect", logger.NewLogData(logMsgId, logger.Err(err))...)
		if perr := processError(ctx, userConnection, err, msg.Id); perr != nil {
			log.Error("failed to proccess error", logger.NewLogData(logger.Err(perr), logUserId, logUserId)...)
		}
		return errs.NewAppError(op, err)
	}
	defer func() {
		if err := ss.ConnectionRepo.DeleteConnect(ctx, user.ID); err != nil {
			log.Error("failed to delete connect", logUserId, logger.Err(err))
			if err := processError(ctx, userConnection, err, ""); err != nil {
				log.Error("failed to process error", logger.Err(err), logUserId)
			}
		} else {
			log.Info("connect is deleted successfully", logUserId)
		}
	}()
	log.Info("user is registered", logUserData...)
	if err := writeSuccessMsg(ctx, stream, msg.Id); err != nil {
		log.Error("failed to write success message", logger.NewLogData(logMsgId, logger.Err(err))...)
		return errs.NewAppError(op, err)
	}
	userConnection.userId = user.ID
	return ss.commandLoop(ctx, userConnection)
}

func (ss *signalService) commandLoop(ctx context.Context, uc *userConnection) error {
	var (
		op        = "signalService.commandLoop"
		log       = ss.Logger.AddOp(op)
		logUserId = logger.Attr("userID", uc.userId)
	)

	log.Info("serving connection in command loop...", logUserId)

	for {
		select {
		case <-uc.ctrlStream.Context().Done():
			log.Info("command stream is done", logUserId)
			return nil
		case <-ctx.Done():
			log.Info("server context is done", logUserId)
			return ctx.Err()
		default:
		}
		var msg protocols.Message
		if err := ss.processMsg(uc, &msg); err != nil {
			if cerr := checkErr(ctx, err); cerr != nil {
				log.Error("failed to process message", logger.Err(cerr), logUserId)
				ss.closeConnection(ctx, uc, 1, "protocol violation")
				return errs.NewAppError(op, err)
			}
			ss.closeConnection(ctx, uc, 0, "client done")
			return nil
		}
		logMsgId := logger.Attr("msgId", msg.Id)
		switch *msg.Type {
		case DATAGRAM_TYPE:
			go func() {
				if err := ss.datagramProxing(ctx, uc, &msg); err != nil {
					log.Error("failed to datagram proxing", logger.NewLogData(logger.Err(err), logMsgId, logUserId)...)
					if perr := processError(ctx, uc, err, msg.Id); perr != nil {
						log.Error("failed to proccess error", logger.NewLogData(logger.Err(perr), logUserId, logUserId)...)
					}
					return
				}
				if err := writeSuccessMsg(ctx, uc.ctrlStream, msg.Id); err != nil {
					log.Error("failed to write success message", logger.NewLogData(logger.Err(err), logMsgId)...)
					return
				}
			}()
		case STREAM_TYPE:
			go func() {
				if err := ss.sendMsg(ctx, uc, &msg); err != nil {
					log.Error("message sending is failed", logger.NewLogData(logger.Err(err), logMsgId, logUserId)...)
					if perr := processError(ctx, uc, err, msg.Id); perr != nil {
						log.Error("failed to proccess error", logger.NewLogData(logger.Err(perr), logUserId, logUserId)...)
					}
					return
				}
				if err := writeSuccessMsg(ctx, uc.ctrlStream, msg.Id); err != nil {
					log.Error("failed to write success message", logger.NewLogData(logger.Err(err), logMsgId)...)
					return
				}
			}()
		case DISCONN_TYPE:
			log.Info("user disconnecting...", logUserId)
			if err := ss.closeConnection(ctx, uc, 0, "user disconnected"); err != nil {
				log.Error("failed when user is disconnecting", logUserId, logger.Err(err))
				if perr := processError(ctx, uc, err, msg.Id); perr != nil {
					log.Error("failed to proccess error", logger.NewLogData(logger.Err(perr), logUserId, logUserId)...)
				}
				return errs.NewAppError(op, err)
			}
			log.Info("user disconnected successfully", logUserId)
			return nil
		case GET_ONLINE_TYPE:
			go func() {
				if err := ss.fetchOnline(ctx, uc, msg.Id); err != nil {
					log.Error("failed to fetch online connects")
					if perr := processError(ctx, uc, err, msg.Id); perr != nil {
						log.Error("failed to proccess error", logger.NewLogData(logger.Err(perr), logUserId, logUserId)...)
					}
					return
				}
				if err := writeSuccessMsg(ctx, uc.ctrlStream, msg.Id); err != nil {
					log.Error("failed to write success message", logger.NewLogData(logger.Err(err), logMsgId)...)
					return
				}
			}()

		default:
			log.Error("invalid message type", logger.NewLogData(logger.Attr("msgType", msg.Type), logMsgId, logUserId)...)
			processError(ctx, uc, errs.ErrWriteMsgBase, msg.Id)
		}
	}

}

func (ss *signalService) sendMsg(ctx context.Context, uc *userConnection, msg *protocols.Message) error {
	var (
		op           = "signalService.sendMsg"
		log          = ss.Logger.AddOp(op)
		userId       = uc.userId
		msgId        = msg.Id
		payloadData  = msg.Data
		logUserId    = logger.Attr("userId", userId)
		logMsgId     = logger.Attr("msgId", msgId)
		userLogsData = logger.NewLogData(logUserId, logMsgId)
	)

	log.Info("message sending...", userLogsData...)

	sendPayloadMsg, err := protocols.ToSendPayloadMessage(ss.Validator, payloadData)
	if err != nil {
		log.Error("failed to cast send message", logger.NewLogData(logger.Err(err), logUserId, logMsgId)...)

		return err
	}
	replyMsg, err := protocols.NewReplyMessage(uc.userId, sendPayloadMsg.Payload)
	if err != nil {
		log.Error("failed to cast reply message", logger.NewLogData(logger.Err(err), logUserId, logMsgId)...)

		return err
	}
	log.Info("getting receivers connections", userLogsData...)
	receivers, err := ss.ConnectionRepo.GetConnects(ctx, sendPayloadMsg.RecevierIDs)
	if err != nil {
		log.Error("failed to get contacts", logger.NewLogData(logger.Err(err), logUserId, logMsgId)...)

		return err
	}
	log.Info("receivers connections received successfully")
	g, gCtx := errgroup.WithContext(ctx)
	log.Info("opening receivers streams", userLogsData...)
	for _, r := range receivers {
		logReceiverId := logger.Attr("receiverId", r.ID)
		receiverLogsData := logger.NewLogData(logMsgId, logReceiverId)
		g.Go(func() error {
			receiverStream, err := r.Connect.OpenUniStreamSync(gCtx)
			if err != nil {
				log.Error("failed to open receiver's stream", logger.NewLogData(logger.Err(err), logReceiverId)...)

				return err
			}

			defer func() {
				log.Info("closing receiver's stream", receiverLogsData...)
				if err := receiverStream.Close(); err != nil {
					log.Error("failed to close receiver's stream", logger.NewLogData(logger.Err(err), logReceiverId, logMsgId)...)

				} else {
					log.Info("receiver's stream is closed successfully", receiverLogsData...)
				}
			}()
			log.Info("receiver's stream is opened", receiverLogsData...)
			if _, err := receiverStream.Write(replyMsg); err != nil {
				log.Error("failed to write message to receiver", logger.NewLogData(logger.Err(err), logReceiverId, logMsgId)...)

				return err
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		if cerr := checkErr(ctx, err); cerr == nil {
			return cerr
		}
		log.Error("failed to send message to receivers", logger.NewLogData(logger.Err(err), logMsgId)...)
		return errs.NewAppError(op, err)
	}
	log.Info("message sended successfully", userLogsData...)

	return nil
}

func (ss *signalService) datagramProxing(ctx context.Context, uc *userConnection, msg *protocols.Message) error {
	var (
		op          = "signalService.datagramStream"
		log         = ss.Logger.AddOp(op)
		msgId       = msg.Id
		logUserId   = logger.Attr("userId", uc.userId)
		logMsgId    = logger.Attr("msgId", msgId)
		logUserData = logger.NewLogData(logUserId, logMsgId)
	)

	log.Info("datagram proxing...", logUserData...)
	datagramProxingData, err := protocols.ToDatagramProxingMessage(ss.Validator, msg.Data)
	if err != nil {
		log.Error("failed to cast datagram proxing message", logger.NewLogData(logger.Err(err), logUserId, logMsgId)...)
		return errs.NewAppError(op, err)
	}

	log.Info("getting receivers connections", logUserData...)
	receivers, err := ss.ConnectionRepo.GetConnects(ctx, datagramProxingData.RecevierIDs)
	if err != nil {
		log.Error("failed to get contacts", logger.NewLogData(logger.Err(err), logUserId, logMsgId)...)
		return errs.NewAppError(op, err)
	}
	log.Info("receivers connections received successfully")
	g, gCtx := errgroup.WithContext(ctx)
	log.Info("opening pipes with receivers...", logUserData...)
	for _, r := range receivers {
		logReceiverId := logger.Attr("receiverId", r.ID)
		receiverLogsData := logger.NewLogData(logMsgId, logReceiverId, logUserId)
		g.Go(func() error {
			log.Info("user's datagram pipe is opened", receiverLogsData...)
			for {
				p, err := uc.quicConn.ReceiveDatagram(gCtx)
				if err != nil {
					log.Error("failed to receive datagram from receiver", logger.NewLogData(logger.Err(err), logUserId, logReceiverId, logMsgId)...)
					return err
				}
				if err := r.Connect.SendDatagram([]byte(p)); err != nil {
					log.Error("failed to send datagram to receiver", logger.NewLogData(logger.Err(err), logUserId, logReceiverId, logMsgId)...)
					return err
				}
			}

		})
		g.Go(func() error {
			log.Info("receiver's datagram pipe is opened", receiverLogsData...)
			for {
				p, err := r.Connect.ReceiveDatagram(gCtx)
				if err != nil {
					log.Error("failed to receive datagram from user", logger.NewLogData(logger.Err(err), logReceiverId, logUserId, logMsgId)...)
					return err
				}
				if err := uc.quicConn.SendDatagram([]byte(p)); err != nil {
					log.Error("failed to send datagram to user", logger.NewLogData(logger.Err(err), logReceiverId, logUserId, logMsgId)...)
					return err
				}
			}
		})
	}

	log.Info("users are connected with datagram pipes", logUserData...)

	if err := g.Wait(); err != nil {
		if cerr := checkErr(ctx, err); cerr == nil {
			return cerr
		}
		log.Error("failed to send prox datagrams", logger.NewLogData(logger.Err(err), logUserId, logUserId)...)
		return errs.NewAppError(op, err)
	}
	log.Info("datagram proxing closing successfully", logUserData...)
	return nil
}

func (ss *signalService) closeConnection(ctx context.Context, uc *userConnection, code int, desc string) error {
	var (
		op        = "signalService.closeConnection"
		log       = ss.Logger.AddOp(op)
		logUserId = logger.Attr("userId", uc.userId)
	)

	log.Info("connection closing...", logUserId)
	uc.ctrlStream.CancelWrite(quic.StreamErrorCode(1))
	select {
	case <-uc.ctrlStream.Context().Done():
	case <-ctx.Done():
	}
	if clerr := uc.quicConn.CloseWithError(quic.ApplicationErrorCode(code), desc); clerr != nil {
		log.Error("failed to close connection", logger.Err(clerr), logUserId)
		return errs.NewAppError(op, clerr)
	}
	log.Info("connection closed successfully", logUserId)
	return nil
}

func (ss *signalService) fetchOnline(ctx context.Context, uc *userConnection, msgId string) error {
	var (
		op          = "signalService.getOnfetchOnlineline"
		log         = ss.Logger.AddOp(op)
		logUserId   = logger.Attr("userId", uc.userId)
		logMsgId    = logger.Attr("msgId", msgId)
		logUserData = logger.NewLogData(logUserId, logMsgId)
	)
	log.Info("fetching all connects ids")
	connectsIds, err := ss.ConnectionRepo.FetchAll(ctx)
	if err != nil {
		log.Error("failed to fetch all connects ids", logger.Err(err))
		return errs.NewAppError(op, err)
	}
	payload := []byte(strings.Join(connectsIds, ","))

	replyMsg, err := protocols.NewReplyMessage(uc.userId, payload)
	if err != nil {
		log.Error("failed to cast reply message", logger.NewLogData(logger.Err(err), logUserId, logMsgId)...)
		return errs.NewAppError(op, err)
	}
	if err := writeMsg(ctx, uc.ctrlStream, replyMsg); err != nil {
		log.Error("failed to write message to user", logger.NewLogData(logger.Err(err), logUserId, logMsgId)...)
		return errs.NewAppError(op, err)
	}
	log.Info("all connects ids fetched successfully", logUserData...)
	return nil
}
