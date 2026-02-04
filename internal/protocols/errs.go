package protocols

import (
	"encoding/json"
	"fmt"
	"test/pkg/errs"
)

const (
	SUCCESS = iota
	PAYLOAD_SUCCESS
	INVALID_PROTOCOL
	STREAM_ERROR
	INTERNAL_ERROR
	NOT_FOUND
	ALREADY_EXISTS
	REQUEST_TIMEOUT
	INVALID_TYPE
)

type ResponseMessage struct {
	Code      uint            `json:"code"`
	MessageId string          `json:"msgId"`
	Payload   json.RawMessage `json:"payload"`
}

func (rm ResponseMessage) Error() string {
	return fmt.Sprintf("msgId: %s, code: %d", rm.MessageId, rm.Code)
}

func NewResponseMessage(mid string, code uint, payload []byte) ResponseMessage {
	return ResponseMessage{
		MessageId: mid,
		Code:      code,
		Payload:   payload,
	}
}

func StreamErrorMessage(mid string) ([]byte, error) {
	op := "protocols.StreamErrorMessage"
	em := NewResponseMessage(mid, STREAM_ERROR, []byte{})
	return marshalResponse(op, em)
}

func InternalServerErrorMessage(mid string) ([]byte, error) {
	op := "protocols.InternalServerErrorMessage"
	em := NewResponseMessage(mid, INTERNAL_ERROR, []byte{})
	return marshalResponse(op, em)
}

func InvalidProtocolErrorMessage(mid string) ([]byte, error) {
	op := "protocols.InvalidProtocolErrorMessage"
	em := NewResponseMessage(mid, INVALID_PROTOCOL, []byte{})
	return marshalResponse(op, em)
}

func InvalidTypeErrorMessage(mid string) ([]byte, error) {
	op := "protocols.InvalidTypeErrorMessage"
	em := NewResponseMessage(mid, INVALID_TYPE, []byte{})
	return marshalResponse(op, em)
}

func ErrorNotFoundMessage(mid string) ([]byte, error) {
	op := "protocols.ErrorNotFoundMessage"
	em := NewResponseMessage(mid, NOT_FOUND, []byte{})
	return marshalResponse(op, em)
}

func ErrorAlreadyExistsMessage(mid string) ([]byte, error) {
	op := "protocols.ErrorAlreadyExistsMessage"
	em := NewResponseMessage(mid, ALREADY_EXISTS, []byte{})
	return marshalResponse(op, em)
}

func ErrorRequestTimeoutMessage(mid string) ([]byte, error) {
	op := "protocols.ErrorRequestTimeoutMessage"
	em := NewResponseMessage(mid, REQUEST_TIMEOUT, []byte{})
	return marshalResponse(op, em)
}

func SuccessMessage(mid string) ([]byte, error) {
	op := "protocols.SuccessMessage"
	sm := NewResponseMessage(mid, SUCCESS, []byte{})
	return marshalResponse(op, sm)
}

func PayloadSuccessMessage(mid string, data []byte) ([]byte, error) {
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
