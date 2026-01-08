package services

import (
	"context"
	"errors"
	"io"
	"test/internal/protocols"
	"test/pkg/errs"
	"test/pkg/logger"

	"github.com/quic-go/quic-go"
)

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

func (ss *signalService) writeMsg(stream *quic.Stream, msg []byte, userId string) error {
	op := "signalService.writeMsg"
	log := ss.Logger.AddOp(op)
	log.Info("writting message...")
	if _, err := stream.Write(msg); err != nil {
		log.Error("failed to write message", logger.Err(err))
		return errs.NewAppError(op, err)
	}
	log.Info("message written", logger.Attr("streamId", stream.StreamID()), logger.Attr("userID", userId))
	return nil
}

func (ss *signalService) processMsg(uc *userConnection, msg *protocols.Message, addr string) error {
	op := "signalService.produceMsg"
	log := ss.Logger.AddOp(op)
	if err := uc.decoder.Decode(msg); err != nil {
		if errors.Is(err, io.EOF) {
			return err
		}
		log.Error("failed to decode message", logger.Err(err))
		dataErr, merr := protocols.InvalidProtocolErrorMessage(msg.Id)
		if merr != nil {
			log.Error("failed to build stream error message")
		}
		ss.writeMsg(uc.ctrlStream, dataErr, addr)
		return errs.ErrDecodeMsg(op, err)
	}
	if err := ss.Validator.Validate.Struct(msg); err != nil {
		if errors.Is(err, io.EOF) {
			return err
		}
		log.Error("faield to validate message", logger.Err(err))
		dataErr, merr := protocols.InvalidProtocolErrorMessage(msg.Id)
		if merr != nil {
			log.Error("failed to build stream error message")
		}
		ss.writeMsg(uc.ctrlStream, dataErr, addr)

		return errs.ErrValidation(op, err)
	}
	return nil
}

func (ss *signalService) processError(uc *userConnection, err error, msgId string) error {
	var pErr []byte
	switch {
	case errors.Is(err, errs.ErrAlreadyExistsBase):
		pErr, err = protocols.ErrorAlreadyExists(msgId)
		if err != nil {
			return err
		}
		return ss.writeMsg(uc.ctrlStream, pErr, uc.userId)
	case errors.Is(err, errs.ErrNotFoundBase):
		pErr, err = protocols.ErrorNotFound(msgId)
		if err != nil {
			return err
		}
		return ss.writeMsg(uc.ctrlStream, pErr, uc.userId)
	case errors.Is(err, errs.ErrRequestTimeoutBase):
		pErr, err = protocols.ErrorRequestTimeout(msgId)
		if err != nil {
			return err
		}
	case errors.Is(err, errs.ErrValidationBase):
		pErr, err = protocols.InvalidProtocolErrorMessage(msgId)
		if err != nil {
			return err
		}
	case errors.Is(err, errs.ErrDecodeMsgBase):
		pErr, err = protocols.InvalidProtocolErrorMessage(msgId)
		if err != nil {
			return err
		}
	case errors.Is(err, errs.ErrInvalidJsonBase):
		pErr, err = protocols.InvalidProtocolErrorMessage(msgId)
		if err != nil {
			return err
		}
	default:
		pErr, err = protocols.InternalServerErrorMessage(msgId)
		if err != nil {
			return err
		}
	}
	ss.writeMsg(uc.ctrlStream, pErr, uc.userId)
	return nil
}
