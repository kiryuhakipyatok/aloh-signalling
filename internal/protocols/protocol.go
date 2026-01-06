package protocols

import (
	"encoding/json"
	"test/pkg/errs"
)

type Message struct {
	Id   string          `json:"id"`
	Type uint8           `json:"type"`
	Data json.RawMessage `json:"data"`
}

type RegisterConnectMessage struct {
	ID string `json:"id"`
}

type SendPayloadMessage struct {
	RecevierIDs []string        `json:"ids"`
	Payload     json.RawMessage `json:"payload"`
}

type ReplyMessage struct {
	Sender  string          `json:"id"`
	Payload json.RawMessage `json:"payload"`
}

func ToRegisterConnectMessage(data json.RawMessage) (*RegisterConnectMessage, error) {
	op := "protocols.ToRegisterConnectMessage"
	regMsg := &RegisterConnectMessage{}
	if err := json.Unmarshal(data, regMsg); err != nil {
		return nil, errs.ErrInvalidProtocol(op)
	}
	return regMsg, nil
}

func ToSendPayloadMessage(data json.RawMessage) (*SendPayloadMessage, error) {
	op := "protocols.ToSendPayloadMessage"
	connectMsg := &SendPayloadMessage{}
	if err := json.Unmarshal(data, connectMsg); err != nil {
		return nil, errs.ErrInvalidProtocol(op)
	}
	return connectMsg, nil
}

func NewReplyMessage(senderId string, pyaload json.RawMessage) ([]byte, error) {
	op := "protocols.NewReplyMessage"
	rm := ReplyMessage{
		Sender:  senderId,
		Payload: pyaload,
	}
	replyMsg, err := json.Marshal(rm)
	if err != nil {
		return nil, errs.ErrInvalidProtocol(op)
	}
	return replyMsg, nil
}
