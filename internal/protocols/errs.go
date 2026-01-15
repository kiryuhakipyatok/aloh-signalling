package protocols

import (
	"encoding/json"
	"fmt"
	"test/pkg/errs"

	"github.com/quic-go/quic-go"
)

var (
	invalidProtocolError = "invalid protocol"
	streamError          = "stream error"
	internalError        = "internal server error"
	notFound             = "not found"
	alreadyExists        = "already exists"
	requestTimeout       = "request timeout"
	invalidType          = "wrong message type"
)

type ResponseMessage struct {
	Code      int8   `json:"code"`
	MessageId string `json:"msgId"`
	Msg       string `json:"msg"`
}

func (rm ResponseMessage) Error() string {
	return fmt.Sprintf("msgId: %s, code: %d, msg: %v", rm.MessageId, rm.Code, rm.Msg)
}

func NewResponseMessage(mid string, msg string, code int8) ResponseMessage {
	return ResponseMessage{
		MessageId: mid,
		Code:      code,
		Msg:       msg,
	}
}

func StreamErrorMessage(mid string) ([]byte, error) {
	op := "protocols.StreamErrorMessage"
	em := NewResponseMessage(mid, streamError, int8(quic.StreamStateError))
	return marshalError(op, em)
}

func InternalServerErrorMessage(mid string) ([]byte, error) {
	op := "protocols.InternalServerErrorMessage"
	em := NewResponseMessage(mid, internalError, int8(quic.InternalError))
	return marshalError(op, em)
}

func InvalidProtocolErrorMessage(mid string) ([]byte, error) {
	op := "protocols.InvalidProtocolErrorMessage"
	em := NewResponseMessage(mid, invalidProtocolError, int8(quic.ProtocolViolation))
	return marshalError(op, em)
}

func InvalidTypeErrorMessage(mid string) ([]byte, error) {
	op := "protocols.InvalidTypeErrorMessage"
	em := NewResponseMessage(mid, invalidType, int8(quic.ProtocolViolation))
	return marshalError(op, em)
}

func ErrorNotFoundMessage(mid string) ([]byte, error) {
	op := "protocols.ErrorNotFoundMessage"
	em := NewResponseMessage(mid, notFound, int8(quic.ProtocolViolation))
	return marshalError(op, em)
}

func ErrorAlreadyExistsMessage(mid string) ([]byte, error) {
	op := "protocols.ErrorAlreadyExistsMessage"
	em := NewResponseMessage(mid, alreadyExists, int8(quic.ProtocolViolation))
	return marshalError(op, em)
}

func ErrorRequestTimeoutMessage(mid string) ([]byte, error) {
	op := "protocols.ErrorRequestTimeoutMessage"
	em := NewResponseMessage(mid, requestTimeout, int8(quic.InternalError))
	return marshalError(op, em)
}

func SuccessMessage(mid string) ([]byte, error) {
	op := "protocols.SuccessMessage"
	sm := NewResponseMessage(mid, "success", int8(quic.NoError))
	return marshalError(op, sm)
}

func marshalError(op string, rm ResponseMessage) ([]byte, error) {
	errMsg, err := json.Marshal(rm)
	if err != nil {
		return nil, errs.ErrInvalidJson(op, err)
	}
	return errMsg, nil
}
