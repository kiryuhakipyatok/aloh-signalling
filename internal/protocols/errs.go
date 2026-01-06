package protocols

import (
	"encoding/json"
	"fmt"
	"test/pkg/errs"

	"github.com/quic-go/quic-go"
)

type ErrorMessage struct {
	Code      int8   `json:"code"`
	MessageId string `json:"msgId"`
	Err       string `json:"error"`
}

func NewErrorMessage(mid string, errMsg string, code int8) ErrorMessage {
	return ErrorMessage{
		MessageId: mid,
		Code:      code,
		Err:       errMsg,
	}
}

func StreamErrorMessage(mid string, errMsg string) ([]byte, error) {
	op := "protocols.StreamErrorMessage"
	em := NewErrorMessage(mid, errMsg, int8(quic.StreamStateError))
	return marshalError(op, em)
}

func InternalServerErrorMessage(mid string, errMsg string) ([]byte, error) {
	op := "protocols.InternalServerErrorMessage"
	em := NewErrorMessage(mid, errMsg, int8(quic.InternalError))
	return marshalError(op, em)
}

func InvalidDataErrorMessage(mid string, errMsg string) ([]byte, error) {
	op := "protocols.InvalidDataErrorMessage"
	em := NewErrorMessage(mid, errMsg, int8(quic.ProtocolViolation))
	return marshalError(op, em)
}

func marshalError(op string, em ErrorMessage) ([]byte, error) {
	streamErrMsg, err := json.Marshal(em)
	if err != nil {
		return nil, errs.ErrInvalidProtocol(op)
	}
	fmt.Println(string(streamErrMsg))
	return streamErrMsg, nil
}
