package errs

import (
	"errors"
	"fmt"
)

var (
	ErrWriteMsgBase         = errors.New("failed to write message")
	ErrDecodeMsgBase        = errors.New("failed to decode message")
	ErrNotFoundBase         = errors.New("not found")
	ErrAlreadyExistsBase    = errors.New("already exists")
	ErrRequestTimeoutBase   = errors.New("request timeout")
	ErrWrongMessageTypeBase = errors.New("wrong message type")
	ErrInvalidProtocolBase  = errors.New("invalid protocol")
	ErrValidationBase       = errors.New("valdiation error")
)

type AppError struct {
	Op  string
	Err error
}

func (ae AppError) Error() string {
	return fmt.Sprintf("%v", ae.Err.Error())
}

func NewAppError(op string, err error) AppError {
	return AppError{
		Op:  op,
		Err: err,
	}
}

func ErrWriteMsg(op string) AppError {
	return AppError{Op: op, Err: ErrWriteMsgBase}
}

func ErrDecodeMsg(op string) AppError {
	return AppError{Op: op, Err: ErrDecodeMsgBase}
}

func ErrNotFound(op string) AppError {
	return AppError{Op: op, Err: ErrNotFoundBase}
}

func ErrAlreadyExists(op string) AppError {
	return AppError{Op: op, Err: ErrAlreadyExistsBase}
}

func ErrRequestTimeout(op string) AppError {
	return AppError{Op: op, Err: ErrRequestTimeoutBase}
}

func ErrWrongMessageType(op string) AppError {
	return AppError{Op: op, Err: ErrWrongMessageTypeBase}
}

func ErrInvalidProtocol(op string) AppError {
	return AppError{Op: op, Err: ErrInvalidProtocolBase}
}

func ErrValidation(op string) AppError {
	return AppError{Op: op, Err: ErrValidationBase}
}
