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
	ErrInvalidJsonBase      = errors.New("invalid json")
	ErrValidationBase       = errors.New("valdiation error")
)

type AppError struct {
	Op  string
	Err error
}

func (ae AppError) Error() string {
	return fmt.Sprintf("%v", ae.Err.Error())
}

func (ae AppError) Unwrap() error {
	return ae.Err
}

func NewAppError(op string, err error) AppError {
	return AppError{
		Op:  op,
		Err: err,
	}
}

func ErrWriteMsg(op string, err error) AppError {
	return AppError{Op: op, Err: fmt.Errorf("%w : %w", ErrWriteMsgBase, err)}
}

func ErrDecodeMsg(op string, err error) AppError {
	return AppError{Op: op, Err: fmt.Errorf("%w : %w", ErrDecodeMsgBase, err)}
}

func ErrNotFound(op string) AppError {
	return AppError{Op: op, Err: fmt.Errorf("%w", ErrNotFoundBase)}
}

func ErrAlreadyExists(op string) AppError {
	return AppError{Op: op, Err: fmt.Errorf("%w", ErrAlreadyExistsBase)}
}

func ErrRequestTimeout(op string) AppError {
	return AppError{Op: op, Err: fmt.Errorf("%w", ErrRequestTimeoutBase)}
}

func ErrWrongMessageType(op string) AppError {
	return AppError{Op: op, Err: fmt.Errorf("%w", ErrWrongMessageTypeBase)}
}

func ErrInvalidJson(op string, err error) AppError {
	return AppError{Op: op, Err: fmt.Errorf("%w : %w", ErrInvalidJsonBase, err)}
}

func ErrValidation(op string, err error) AppError {
	return AppError{Op: op, Err: fmt.Errorf("%w : %w", ErrValidationBase, err)}
}
