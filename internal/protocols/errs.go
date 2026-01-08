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
)

type ErrorMessage struct {
	Code      int8   `json:"code"`
	MessageId string `json:"msgId"`
	Err       string `json:"error"`
}

func (em ErrorMessage) Error() string {
	return fmt.Sprintf("msgId: %s, code: %d, error: %v", em.MessageId, em.Code, em.Err)
}

func NewErrorMessage(mid string, errMsg string, code int8) ErrorMessage {
	return ErrorMessage{
		MessageId: mid,
		Code:      code,
		Err:       errMsg,
	}
}

func StreamErrorMessage(mid string) ([]byte, error) {
	op := "protocols.StreamErrorMessage"
	em := NewErrorMessage(mid, streamError, int8(quic.StreamStateError))
	return marshalError(op, em)
}

func InternalServerErrorMessage(mid string) ([]byte, error) {
	op := "protocols.InternalServerErrorMessage"
	em := NewErrorMessage(mid, internalError, int8(quic.InternalError))
	return marshalError(op, em)
}

func InvalidProtocolErrorMessage(mid string) ([]byte, error) {
	op := "protocols.InvalidProtocolErrorMessage"
	em := NewErrorMessage(mid, invalidProtocolError, int8(quic.ProtocolViolation))
	return marshalError(op, em)
}

func InvalidTypeErrorMessage(mid string) ([]byte, error) {
	op := "protocols.InvalidTypeErrorMessage"
	em := NewErrorMessage(mid, invalidProtocolError, int8(quic.ProtocolViolation))
	return marshalError(op, em)
}

func ErrorNotFound(mid string) ([]byte, error) {
	op := "protocols.ErrorNotFound"
	em := NewErrorMessage(mid, notFound, int8(quic.ProtocolViolation))
	return marshalError(op, em)
}

func ErrorAlreadyExists(mid string) ([]byte, error) {
	op := "protocols.ErrorAlreadyExists"
	em := NewErrorMessage(mid, alreadyExists, int8(quic.ProtocolViolation))
	return marshalError(op, em)
}

func ErrorRequestTimeout(mid string) ([]byte, error) {
	op := "protocols.ErrorRequestTimeout"
	em := NewErrorMessage(mid, requestTimeout, int8(quic.InternalError))
	return marshalError(op, em)
}

func marshalError(op string, em ErrorMessage) ([]byte, error) {
	errMsg, err := json.Marshal(em)
	if err != nil {
		return nil, errs.ErrInvalidJson(op, err)
	}
	return errMsg, nil
}
