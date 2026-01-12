package services

import (
	"context"
	"errors"
	"fmt"
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

func writeMsg(ctx context.Context, stream *quic.Stream, msg []byte) error {
	op := "utils.writeMsg"
	_, err := stream.Write(msg)
	if err != nil {
		return checkErr(ctx, errs.NewAppError(op, err))
	}
	return nil
}

func (ss *signalService) processMsg(uc *userConnection, msg *protocols.Message) error {
	op := "utils.produceMsg"

	if err := uc.decoder.Decode(msg); err != nil {
		fmt.Println("d")
		return errs.ErrDecodeMsg(op, err)
	}
	if err := ss.Validator.Validate.Struct(msg); err != nil {

		return errs.ErrValidation(op, err)
	}
	return nil
}

func writeSuccessMsg(ctx context.Context, stream *quic.Stream, msgId string) error {
	op := "utils.writeSuccessMsg"
	sm, err := protocols.SuccessMessage(msgId)
	if err != nil {
		return errs.NewAppError(op, err)
	}
	if err := writeMsg(ctx, stream, sm); err != nil {
		return errs.NewAppError(op, err)
	}
	return nil
}

func processError(ctx context.Context, uc *userConnection, err error, msgId string) error {
	var pErr []byte
	switch {
	case errors.Is(err, errs.ErrAlreadyExistsBase):
		pErr, err = protocols.ErrorAlreadyExistsMessage(msgId)
		if err != nil {
			return err
		}
	case errors.Is(err, errs.ErrNotFoundBase):
		pErr, err = protocols.ErrorNotFoundMessage(msgId)
		if err != nil {
			return err
		}
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
	case errors.Is(err, errs.ErrWrongMessageTypeBase):
		pErr, err = protocols.InvalidTypeErrorMessage(msgId)
		if err != nil {
			return err
		}
	default:
		pErr, err = protocols.InternalServerErrorMessage(msgId)
		if err != nil {
			return err
		}
	}

	if err := writeMsg(ctx, uc.ctrlStream, pErr); err != nil {
		return err
	}

	return nil
}
