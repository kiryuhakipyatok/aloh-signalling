package protocols

import (
	"encoding/json"
	"fmt"
	"test/pkg/errs"
)

const (
	SUCCESS = iota
	INVALID_PROTOCOL
	STREAM_ERROR
	INTERNAL_ERROR
	NOT_FOUND
	ALREADY_EXISTS
	REQUEST_TIMEOUT
	INVALID_TYPE
)

type ResponseMessage struct {
	Code      uint   `json:"code"`
	MessageId string `json:"msgId"`
}

func (rm ResponseMessage) Error() string {
	return fmt.Sprintf("msgId: %s, code: %d", rm.MessageId, rm.Code)
}

func NewResponseMessage(mid string, code uint) ResponseMessage {
	return ResponseMessage{
		MessageId: mid,
		Code:      code,
	}
}

func StreamErrorMessage(mid string) ([]byte, error) {
	op := "protocols.StreamErrorMessage"
	em := NewResponseMessage(mid, STREAM_ERROR)
	return marshalError(op, em)
}

func InternalServerErrorMessage(mid string) ([]byte, error) {
	op := "protocols.InternalServerErrorMessage"
	em := NewResponseMessage(mid, INTERNAL_ERROR)
	return marshalError(op, em)
}

func InvalidProtocolErrorMessage(mid string) ([]byte, error) {
	op := "protocols.InvalidProtocolErrorMessage"
	em := NewResponseMessage(mid, INVALID_PROTOCOL)
	return marshalError(op, em)
}

func InvalidTypeErrorMessage(mid string) ([]byte, error) {
	op := "protocols.InvalidTypeErrorMessage"
	em := NewResponseMessage(mid, INVALID_TYPE)
	return marshalError(op, em)
}

func ErrorNotFoundMessage(mid string) ([]byte, error) {
	op := "protocols.ErrorNotFoundMessage"
	em := NewResponseMessage(mid, NOT_FOUND)
	return marshalError(op, em)
}

func ErrorAlreadyExistsMessage(mid string) ([]byte, error) {
	op := "protocols.ErrorAlreadyExistsMessage"
	em := NewResponseMessage(mid, ALREADY_EXISTS)
	return marshalError(op, em)
}

func ErrorRequestTimeoutMessage(mid string) ([]byte, error) {
	op := "protocols.ErrorRequestTimeoutMessage"
	em := NewResponseMessage(mid, REQUEST_TIMEOUT)
	return marshalError(op, em)
}

func SuccessMessage(mid string) ([]byte, error) {
	op := "protocols.SuccessMessage"
	sm := NewResponseMessage(mid, SUCCESS)
	return marshalError(op, sm)
}

func marshalError(op string, rm ResponseMessage) ([]byte, error) {
	errMsg, err := json.Marshal(rm)
	if err != nil {
		return nil, errs.ErrInvalidJson(op, err)
	}
	return errMsg, nil
}
