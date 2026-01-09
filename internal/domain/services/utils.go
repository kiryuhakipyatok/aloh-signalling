package services

import (
	"context"
	"errors"
	"io"
	"test/internal/protocols"
	"test/pkg/errs"

	"github.com/quic-go/quic-go"
)

func checkErr(ctx context.Context, err error) error {

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
		return nil
	}

	if errors.Is(err, io.EOF) {
		return nil
	}
	var appErr *quic.ApplicationError
	if errors.As(err, &appErr) {
		if appErr.ErrorCode == 0 {
			return nil
		}
	}

	return err

}

func writeMsg(stream *quic.Stream, msg []byte) error {
	op := "utils.writeMsg"

	if _, err := stream.Write(msg); err != nil {

		return errs.NewAppError(op, err)
	}

	return nil
}

func (ss *signalService) processMsg(uc *userConnection, msg *protocols.Message) error {
	op := "utils.produceMsg"

	if err := uc.decoder.Decode(msg); err != nil {
		if errors.Is(err, io.EOF) {
			return err
		}
		dataErr, merr := protocols.InvalidProtocolErrorMessage(msg.Id)
		if merr != nil {
			return merr
		}
		if err := writeMsg(uc.ctrlStream, dataErr); err != nil {
			return err
		}
		return errs.ErrDecodeMsg(op, err)
	}
	if err := ss.Validator.Validate.Struct(msg); err != nil {
		if errors.Is(err, io.EOF) {
			return err
		}

		dataErr, merr := protocols.InvalidProtocolErrorMessage(msg.Id)
		if merr != nil {
			return merr
		}
		if err := writeMsg(uc.ctrlStream, dataErr); err != nil {
			return err
		}

		return errs.ErrValidation(op, err)
	}
	return nil
}

func writeSuccessMsg(stream *quic.Stream, msgId string) error {
	op := "utils.writeSuccessMsg"
	sm, err := protocols.SuccessMessage(msgId)
	if err != nil {
		return errs.NewAppError(op, err)
	}
	if err := writeMsg(stream, sm); err != nil {
		return errs.NewAppError(op, err)
	}
	return nil
}

func processError(uc *userConnection, err error, msgId string) error {
	var pErr []byte
	switch {
	case errors.Is(err, errs.ErrAlreadyExistsBase):
		pErr, err = protocols.ErrorAlreadyExistsMessage(msgId)
		if err != nil {
			return err
		}
		return writeMsg(uc.ctrlStream, pErr)
	case errors.Is(err, errs.ErrNotFoundBase):
		pErr, err = protocols.ErrorNotFoundMessage(msgId)
		if err != nil {
			return err
		}
		return writeMsg(uc.ctrlStream, pErr)
	case errors.Is(err, errs.ErrRequestTimeoutBase):
		pErr, err = protocols.ErrorRequestTimeoutMessage(msgId)
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
	if err := writeMsg(uc.ctrlStream, pErr); err != nil {
		return err
	}
	return nil
}
