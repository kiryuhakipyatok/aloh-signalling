package protocols

import (
	"encoding/json"
	"test/pkg/errs"
)

type Message struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

type RegisterConnectMessage struct {
	ID string `json:"id"`
}

type ConnectToUserMessage struct {
	RecevierIDs []string `json:"ids"`
}

func ToRegisterConnectMessage(data json.RawMessage) (*RegisterConnectMessage, error) {
	op := "protocols.ToRegisterConnectMessage"
	regMsg := &RegisterConnectMessage{}
	if err := json.Unmarshal(data, regMsg); err != nil {
		return nil, errs.ErrInvalidProtocol(op)
	}
	return regMsg, nil
}

func ToConnectToUserMessage(data json.RawMessage) (*ConnectToUserMessage, error) {
	op := "protocols.ToConnectToUserMessage"
	connectMsg := &ConnectToUserMessage{}
	if err := json.Unmarshal(data, connectMsg); err != nil {
		return nil, errs.ErrInvalidProtocol(op)
	}
	return connectMsg, nil
}
