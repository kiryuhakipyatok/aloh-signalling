package services

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"sync"
	"time"

	"github.com/kiryuhakipyatok/aloh-signalling/internal/protocols"
	"github.com/kiryuhakipyatok/aloh-signalling/pkg/errs"
	"github.com/kiryuhakipyatok/aloh-signalling/pkg/logger"

	"github.com/google/uuid"
	"github.com/quic-go/quic-go"
	"golang.org/x/sync/errgroup"
)

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
	if err := ss.SessionRepo.DeleteSession(ctx, uc.userId); err != nil {
		log.Error("failed to delete session", logger.Err(err), logUserId)
		return errs.NewAppError(op, err)
	}
	log.Info("connection closed successfully", logUserId)
	return nil
}

func (ss *signalService) fetchOnline(ctx context.Context, uc *userConnection, msgId uuid.UUID) error {
	var (
		op          = "signalService.fetchOnline"
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
	payload, err := json.Marshal(connectsIds)
	if err != nil {
		log.Error("failed to marshal connects ids", logger.Err(err))
		return errs.NewAppError(op, err)
	}

	replyMsg, err := protocols.PayloadSuccessMessage(msgId, payload)
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

func (ss *signalService) fetchOnlineFriends(ctx context.Context, uc *userConnection, msg *protocols.Message) error {
	var (
		op          = "signalService.fetchOnline"
		log         = ss.Logger.AddOp(op)
		logUserId   = logger.Attr("userId", uc.userId)
		logMsgId    = logger.Attr("msgId", msg.Id)
		logUserData = logger.NewLogData(logUserId, logMsgId)
	)
	log.Info("fetching online friends ids", logUserData...)
	friendsMsg, err := protocols.ToFetchFriendsOnlineMessage(ss.Validator, msg.Data)
	if err != nil {
		log.Error("failed to cast fetch online friends msg", logger.Err(err), logMsgId, logUserId)
		return errs.NewAppError(op, err)
	}
	friendsOnline := make(map[uuid.UUID][]uuid.UUID, len(friendsMsg.FriendsIds))
	var (
		eg errgroup.Group
		mu sync.Mutex
	)
	for _, fi := range friendsMsg.FriendsIds {
		eg.Go(func() error {
			logFr := logger.Attr("friendId", fi)
			users, err := ss.SessionRepo.GetSessions(ctx, fi)
			if err != nil {
				if errors.Is(err, errs.ErrNotFoundBase) {
					log.Info("friend is offline or not exists", logFr)
					return nil
				}
				log.Error("failed to get sessions", logger.Err(err), logFr, logMsgId, logUserId)
				return errs.NewAppError(op, err)
			}
			mu.Lock()
			friendsOnline[fi] = users
			mu.Unlock()
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		log.Error("failed to fetch online friends", logger.Err(err), logMsgId, logUserId)
		return errs.NewAppError(op, err)
	}

	payload, err := json.Marshal(friendsOnline)
	if err != nil {
		log.Error("failed to marshal online friends", logger.Err(err))
		return errs.NewAppError(op, err)
	}

	replyMsg, err := protocols.PayloadSuccessMessage(msg.Id, payload)
	if err != nil {
		log.Error("failed to cast reply message", logger.Err(err), logUserId, logMsgId)
		return errs.NewAppError(op, err)
	}

	if err := writeMsg(ctx, uc.ctrlStream, replyMsg); err != nil {
		log.Error("failed to write message to user", logger.Err(err), logUserId, logMsgId)
		return errs.NewAppError(op, err)
	}
	log.Info("online friends fetched successfully", logUserData...)
	return nil
}

func (ss *signalService) addInSession(ctx context.Context, uc *userConnection, msg *protocols.Message) error {
	var (
		op          = "signalService.addSession"
		log         = ss.Logger.AddOp(op)
		logUserId   = logger.Attr("userId", uc.userId)
		logMsgId    = logger.Attr("msgId", msg.Id)
		logUserData = logger.NewLogData(logUserId, logMsgId)
	)
	log.Info("additing user in session...", logUserData...)
	userIdData, err := protocols.ToUserIdMessage(ss.Validator, msg.Data)
	if err != nil {
		log.Error("failed to cast add session message", logger.NewLogData(logger.Err(err), logMsgId, logUserId)...)
		return errs.NewAppError(op, err)
	}
	userId := userIdData.ID
	if err := ss.ConnectionRepo.IsExists(ctx, userId); err != nil {
		return errs.NewAppError(op, err)
	}

	if err := ss.SessionRepo.AddInSession(ctx, uc.userId, userId); err != nil {
		log.Error("failed to add in session", logger.NewLogData(logger.Err(err), logMsgId, logUserId)...)
		return errs.NewAppError(op, err)
	}
	if err := writeSuccessMsg(ctx, uc.ctrlStream, msg.Id); err != nil {
		log.Error("failed to write success message to user", logger.NewLogData(logger.Err(err), logUserId, logMsgId)...)
		return errs.NewAppError(op, err)
	}
	log.Info("session added sussessfully")
	return nil
}

func (ss *signalService) deleteFromSession(ctx context.Context, uc *userConnection, msg *protocols.Message) error {
	var (
		op          = "signalService.deleteFromSession"
		log         = ss.Logger.AddOp(op)
		logUserId   = logger.Attr("userId", uc.userId)
		logMsgId    = logger.Attr("msgId", msg.Id)
		logUserData = logger.NewLogData(logUserId, logMsgId)
	)
	log.Info("deleting user from session...", logUserData...)
	data, err := protocols.ToUserIdMessage(ss.Validator, msg.Data)
	if err != nil {
		log.Error("failed to cast delete from session message", logger.NewLogData(logger.Err(err), logMsgId, logUserId)...)
		return errs.NewAppError(op, err)
	}
	userId := data.ID
	if err := ss.ConnectionRepo.IsExists(ctx, userId); err != nil {
		return errs.NewAppError(op, err)
	}

	if err := ss.SessionRepo.DeleteFromSession(ctx, uc.userId, userId); err != nil {
		log.Error("failed to delete from session", logger.NewLogData(logger.Err(err), logMsgId, logUserId)...)
		return errs.NewAppError(op, err)
	}
	if err := writeSuccessMsg(ctx, uc.ctrlStream, msg.Id); err != nil {
		log.Error("failed to write success message to user", logger.NewLogData(logger.Err(err), logUserId, logMsgId)...)
		return errs.NewAppError(op, err)
	}
	log.Info("user deleted from session sussessfully")
	return nil
}

func (ss *signalService) fetchSessionsById(ctx context.Context, uc *userConnection, msg *protocols.Message) error {
	var (
		op          = "signalService.fetchSessionsById"
		log         = ss.Logger.AddOp(op)
		logUserId   = logger.Attr("userId", uc.userId)
		logMsgId    = logger.Attr("msgId", msg.Id)
		logUserData = logger.NewLogData(logUserId, logMsgId)
	)

	log.Info("fetching sessions by id...", logUserData...)

	userIdMsg, err := protocols.ToUserIdMessage(ss.Validator, msg.Data)
	if err != nil {
		log.Error("failed to cast add session message", logger.NewLogData(logger.Err(err), logMsgId, logUserId)...)
		return errs.NewAppError(op, err)
	}
	userId := userIdMsg.ID
	users, err := ss.SessionRepo.GetSessions(ctx, userId)
	if err != nil {
		log.Error("failed to get sessions", logger.NewLogData(logger.Err(err), logMsgId, logUserId)...)
		return errs.NewAppError(op, err)
	}
	payload, err := json.Marshal(users)
	if err != nil {
		log.Error("failed to marshal sessions", logger.Err(err))
		return errs.NewAppError(op, err)
	}

	replyMsg, err := protocols.PayloadSuccessMessage(msg.Id, payload)
	if err != nil {
		log.Error("failed to cast reply message", logger.NewLogData(logger.Err(err), logUserId, logMsgId)...)
		return errs.NewAppError(op, err)
	}

	if err := writeMsg(ctx, uc.ctrlStream, replyMsg); err != nil {
		log.Error("failed to write message to user", logger.NewLogData(logger.Err(err), logUserId, logMsgId)...)
		return errs.NewAppError(op, err)
	}
	log.Info("sessions fetched successfully", logUserData...)
	return nil
}

func (ss *signalService) genCreds(ctx context.Context, id uuid.UUID) (string, string, error) {
	op := "utils.genCreds"
	select {
	case <-ctx.Done():
		return "", "", errs.ErrRequestTimeout(op)
	default:
	}
	exp := time.Now().Add(ss.Cfg.CredsTTL).Unix()
	username := fmt.Sprintf("%d:%s", exp, id.String())
	fmt.Println(ss.Cfg.Secret)
	mac := hmac.New(func() hash.Hash { return sha1.New() }, []byte(ss.Cfg.Secret))
	mac.Write([]byte(username))
	hash := mac.Sum(nil)
	password := base64.StdEncoding.EncodeToString(hash)
	return username, password, nil
}
