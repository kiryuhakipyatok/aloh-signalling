package services

import (
	"context"
	"encoding/json"

	"github.com/kiryuhakipyatok/aloh-signalling/internal/config"
	"github.com/kiryuhakipyatok/aloh-signalling/internal/domain/models"
	"github.com/kiryuhakipyatok/aloh-signalling/internal/domain/repository"
	"github.com/kiryuhakipyatok/aloh-signalling/internal/protocols"
	"github.com/kiryuhakipyatok/aloh-signalling/pkg/errs"
	"github.com/kiryuhakipyatok/aloh-signalling/pkg/logger"
	"github.com/kiryuhakipyatok/aloh-signalling/pkg/validator"

	"github.com/google/uuid"
	"github.com/quic-go/quic-go"
)

type SignallingService interface {
	ServeConnection(ctx context.Context, conn *quic.Conn) error
}

type signalService struct {
	ConnectionRepo repository.ConnectionsRepo
	SessionRepo    repository.SessionsRepo
	Logger         *logger.Logger
	Validator      *validator.Validator
	Cfg            config.Signaling
}

func NewSignallingService(cr repository.ConnectionsRepo, cfg config.Signaling, sr repository.SessionsRepo, v *validator.Validator, l *logger.Logger) SignallingService {
	return &signalService{
		ConnectionRepo: cr,
		SessionRepo:    sr,
		Logger:         l,
		Validator:      v,
		Cfg:            cfg,
	}
}

type userConnection struct {
	userId     uuid.UUID
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

	if *msg.Type != protocols.REG_TYPE {
		msgIdLog := logger.Attr("msgType", msg.Type)
		log.Error("wrong message type", logger.NewLogData(msgIdLog, logMsgId)...)
		if perr := processError(ctx, userConnection, err, msg.Id); perr != nil {
			log.Error("failed to proccess error", logger.NewLogData(logger.Err(perr), msgIdLog, logMsgId)...)
		}
		return errs.NewAppError(op, err)
	}
	userIdData, err := protocols.ToUserIdMessage(ss.Validator, msg.Data)
	if err != nil {
		log.Error("failed to cast message", logger.NewLogData(logger.Err(err), logMsgId)...)
		if perr := processError(ctx, userConnection, err, msg.Id); perr != nil {
			log.Error("failed to proccess error", logger.NewLogData(logger.Err(perr), logMsgId)...)
		}
		return errs.NewAppError(op, err)
	}
	userId := userIdData.ID
	user := &models.Connection{
		ID:      userId,
		Connect: conn,
	}
	logUserId := logger.Attr("userID", user.ID)
	logUserData := logger.NewLogData(logMsgId, logUserId)
	old, err := ss.ConnectionRepo.AddConnect(ctx, user)
	if err != nil {
		log.Error("failed to add connect", logger.NewLogData(logMsgId, logger.Err(err))...)
		if perr := processError(ctx, userConnection, err, msg.Id); perr != nil {
			log.Error("failed to proccess error", logger.NewLogData(logger.Err(perr), logUserId, logUserId)...)
		}
		return errs.NewAppError(op, err)
	}
	if old != nil {
		log.Info("new connect with existing user id", logMsgId)
		if err := old.Connect.CloseWithError(0, "new connect with existing user id"); err != nil {
			log.Error("failed to close old connect", logger.Err(err))
		}
		if err := ss.SessionRepo.DeleteSession(ctx, userId); err != nil {
			log.Error("failed to delete old session", logger.Err(err))
		}
	}
	session := models.Session{
		UserId:         userId,
		ConnectedUsers: make([]uuid.UUID, 0, 3),
	}
	if err := ss.SessionRepo.NewSession(ctx, session); err != nil {
		log.Error("failed to create new session", logger.NewLogData(logMsgId, logger.Err(err))...)
		if perr := processError(ctx, userConnection, err, msg.Id); perr != nil {
			log.Error("failed to proccess error", logger.NewLogData(logger.Err(perr), logUserId, logUserId)...)
		}
		return errs.NewAppError(op, err)
	}
	defer func() {
		if err := ss.ConnectionRepo.DeleteConnect(ctx, user.ID, user); err != nil {
			log.Error("failed to delete connect", logUserId, logger.Err(err))
		} else {
			log.Info("connect is deleted successfully", logUserId)
			if err := ss.SessionRepo.DeleteSession(ctx, user.ID); err != nil {
				log.Error("failed to delete session", logUserId, logger.Err(err))
			} else {
				log.Info("session is deleted successfully", logUserId)
			}
		}

	}()
	log.Info("user is registered", logUserData...)
	log.Info("creds generating...", logUserData...)
	username, password, err := ss.genCreds(ctx, session.UserId)
	if err != nil {
		log.Error("failed to generate credentials", logger.NewLogData(logMsgId, logger.Err(err))...)
		if perr := processError(ctx, userConnection, err, msg.Id); perr != nil {
			log.Error("failed to proccess error", logger.NewLogData(logger.Err(perr), logUserId, logUserId)...)
		}
		return errs.NewAppError(op, err)
	}
	log.Info("creds generated", logUserData...)
	creds := protocols.CredsMessage{
		Username: username,
		Password: password,
	}
	payload, err := json.Marshal(creds)
	if err != nil {
		log.Error("failed to marshal sessions", logger.Err(err))
		return errs.NewAppError(op, err)
	}
	replyMsg, err := protocols.PayloadSuccessMessage(msg.Id, payload)
	if err != nil {
		log.Error("failed to cast reply message", logger.NewLogData(logger.Err(err), logUserId, logMsgId)...)
		return errs.NewAppError(op, err)
	}

	if err := writeMsg(ctx, userConnection.ctrlStream, replyMsg); err != nil {
		log.Error("failed to write message to user", logger.NewLogData(logger.Err(err), logUserId, logMsgId)...)
		return errs.NewAppError(op, err)
	}
	log.Info("creds sended successfully", logUserData...)
	userConnection.userId = user.ID
	return ss.commandLoop(ctx, userConnection)
}

func (ss *signalService) commandLoop(ctx context.Context, uc *userConnection) error {
	var (
		op        = "signalService.commandLoop"
		log       = ss.Logger.AddOp(op)
		logUserId = logger.Attr("userID", uc.userId.ID)
	)

	log.Info("serving connection in command loop...", logUserId)

	for {
		select {
		case <-uc.quicConn.Context().Done():
			log.Info("quic conn is done", logUserId)
			return nil
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
				//ss.closeConnection(ctx, uc, 1, "protocol violation")
				return errs.NewAppError(op, err)
			}
			//ss.closeConnection(ctx, uc, 0, "client done")
			return nil
		}
		logMsgId := logger.Attr("msgId", msg.Id)
		switch *msg.Type {
		case protocols.DATAGRAM_TYPE:
			go func() {
				if err := ss.datagramProxing(ctx, uc, &msg); err != nil {
					log.Error("failed to datagram proxing", logger.Err(err), logMsgId, logUserId)
					if perr := processError(ctx, uc, err, msg.Id); perr != nil {
						log.Error("failed to proccess error", logger.Err(perr), logUserId, logUserId)
					}
					return
				}
				if err := writeSuccessMsg(ctx, uc.ctrlStream, msg.Id); err != nil {
					log.Error("failed to write success message", logger.Err(err), logMsgId)
					return
				}
			}()
		case protocols.STREAM_TYPE:
			go func() {
				if err := ss.sendMsg(ctx, uc, &msg); err != nil {
					log.Error("message sending is failed", logger.Err(err), logMsgId, logUserId)
					if perr := processError(ctx, uc, err, msg.Id); perr != nil {
						log.Error("failed to proccess error", logger.Err(perr), logUserId, logUserId)
					}
					return
				}
				if err := writeSuccessMsg(ctx, uc.ctrlStream, msg.Id); err != nil {
					log.Error("failed to write success message", logger.Err(err), logMsgId)
					return
				}
			}()
		case protocols.DISCONN_TYPE:
			log.Info("user disconnecting...", logUserId)
			if err := ss.closeConnection(ctx, uc, 0, "user disconnected"); err != nil {
				log.Error("failed to close conenction", logger.Err(err), logMsgId, logUserId)
				if perr := processError(ctx, uc, err, msg.Id); perr != nil {
					log.Error("failed to proccess error", logger.Err(perr), logUserId, logUserId)
				}
				return errs.NewAppError(op, err)
			}
			log.Info("user disconnected successfully", logUserId)
			return nil
		case protocols.GET_ONLINE_TYPE:
			go func() {
				if err := ss.fetchOnline(ctx, uc, msg.Id); err != nil {
					log.Error("failed to fetch online connects", logger.Err(err), logMsgId, logUserId)
					if perr := processError(ctx, uc, err, msg.Id); perr != nil {
						log.Error("failed to proccess error", logger.Err(perr), logUserId, logUserId)
					}
					return
				}
			}()
		case protocols.ADD_IN_SESSION:
			go func() {
				if err := ss.addInSession(ctx, uc, &msg); err != nil {
					log.Error("failed to add user in session", logger.Err(err), logMsgId, logUserId)
					if perr := processError(ctx, uc, err, msg.Id); perr != nil {
						log.Error("failed to proccess error", logger.Err(perr), logUserId, logUserId)
					}
					return
				}
			}()
		case protocols.DELETE_FROM_SESSION:
			go func() {
				if err := ss.deleteFromSession(ctx, uc, &msg); err != nil {
					log.Error("failed to delete user from session", logger.Err(err), logMsgId, logUserId)
					if perr := processError(ctx, uc, err, msg.Id); perr != nil {
						log.Error("failed to proccess error", logger.Err(perr), logUserId, logUserId)
					}
					return
				}
			}()
		case protocols.GET_SESSIONS_BY_ID:
			go func() {
				if err := ss.fetchSessionsById(ctx, uc, &msg); err != nil {
					log.Error("failed to fetch sessions by id", logger.Err(err), logMsgId, logUserId)
					if perr := processError(ctx, uc, err, msg.Id); perr != nil {
						log.Error("failed to proccess error", logger.Err(perr), logUserId, logUserId)
					}
					return
				}
			}()
		case protocols.GET_ONLINE_FRIENDS:
			go func() {
				if err := ss.fetchOnlineFriends(ctx, uc, &msg); err != nil {
					log.Error("failed to fetch online friends", logger.Err(err), logMsgId, logUserId)
					if perr := processError(ctx, uc, err, msg.Id); perr != nil {
						log.Error("failed to proccess error", logger.Err(perr), logUserId, logUserId)
					}
					return
				}
			}()

		default:
			log.Error("invalid message type", logger.NewLogData(logger.Attr("msgType", msg.Type), logMsgId, logUserId)...)
			return processError(ctx, uc, errs.ErrWriteMsgBase, msg.Id)
		}
	}

}
