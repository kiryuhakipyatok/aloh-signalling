package protocols

import (
	"encoding/json"
	"fmt"
	"github.com/kiryuhakipyatok/aloh-signalling/pkg/errs"

	"github.com/google/uuid"
)

const (
	SUCCESS = iota
	NOT_FOUND
	ALREADY_EXISTS
	REQUEST_TIMEOUT
	PAYLOAD_SUCCESS
	INVALID_PROTOCOL
	STREAM_ERROR
	INVALID_TYPE
	INTERNAL_ERROR
)

type ResponseMessage struct {
	Code      uint            `json:"code"`
	MessageId uuid.UUID       `json:"msgId"`
	Payload   json.RawMessage `json:"payload"`
}

func (rm ResponseMessage) Error() string {
	return fmt.Sprintf("msgId: %s, code: %d", rm.MessageId.String(), rm.Code)
}

func NewResponseMessage(mid uuid.UUID, code uint, payload []byte) ResponseMessage {
	return ResponseMessage{
		MessageId: mid,
		Code:      code,
		Payload:   payload,
	}
}

func StreamErrorMessage(mid uuid.UUID) ([]byte, error) {
	op := "protocols.StreamErrorMessage"
	em := NewResponseMessage(mid, STREAM_ERROR, nil)
	return marshalResponse(op, em)
}

func InternalServerErrorMessage(mid uuid.UUID) ([]byte, error) {
	op := "protocols.InternalServerErrorMessage"
	em := NewResponseMessage(mid, INTERNAL_ERROR, nil)
	return marshalResponse(op, em)
}

func InvalidProtocolErrorMessage(mid uuid.UUID) ([]byte, error) {
	op := "protocols.InvalidProtocolErrorMessage"
	em := NewResponseMessage(mid, INVALID_PROTOCOL, nil)
	return marshalResponse(op, em)
}

func InvalidTypeErrorMessage(mid uuid.UUID) ([]byte, error) {
	op := "protocols.InvalidTypeErrorMessage"
	em := NewResponseMessage(mid, INVALID_TYPE, nil)
	return marshalResponse(op, em)
}

func ErrorNotFoundMessage(mid uuid.UUID) ([]byte, error) {
	op := "protocols.ErrorNotFoundMessage"
	em := NewResponseMessage(mid, NOT_FOUND, nil)
	return marshalResponse(op, em)
}

func ErrorAlreadyExistsMessage(mid uuid.UUID) ([]byte, error) {
	op := "protocols.ErrorAlreadyExistsMessage"
	em := NewResponseMessage(mid, ALREADY_EXISTS, nil)
	return marshalResponse(op, em)
}

func ErrorRequestTimeoutMessage(mid uuid.UUID) ([]byte, error) {
	op := "protocols.ErrorRequestTimeoutMessage"
	em := NewResponseMessage(mid, REQUEST_TIMEOUT, nil)
	return marshalResponse(op, em)
}

func SuccessMessage(mid uuid.UUID) ([]byte, error) {
	op := "protocols.SuccessMessage"
	sm := NewResponseMessage(mid, SUCCESS, nil)
	return marshalResponse(op, sm)
}

func PayloadSuccessMessage(mid uuid.UUID, data []byte) ([]byte, error) {
	op := "protocols.PayloadSuccessMessage"
	sm := NewResponseMessage(mid, PAYLOAD_SUCCESS, data)
	return marshalResponse(op, sm)
}

func marshalResponse(op string, rm ResponseMessage) ([]byte, error) {
	respMsg, err := json.Marshal(rm)
	if err != nil {
		return nil, errs.ErrInvalidJson(op, err)
	}
	return respMsg, nil
}
