package protocols

import "encoding/json"

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
	regMsg := &RegisterConnectMessage{}
	if err := json.Unmarshal(data, regMsg); err != nil {
		return nil, err
	}
	return regMsg, nil
}

func ToConnectToUserMessage(data json.RawMessage) (*ConnectToUserMessage, error) {
	connectMsg := &ConnectToUserMessage{}
	if err := json.Unmarshal(data, connectMsg); err != nil {
		return nil, err
	}
	return connectMsg, nil
}
