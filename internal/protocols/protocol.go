package protocols

import (
	"encoding/json"

	"github.com/kiryuhakipyatok/aloh-signalling/pkg/errs"

	"github.com/kiryuhakipyatok/aloh-signalling/pkg/validator"
)

type Message struct {
	Id   string          `json:"id" validate:"required,min=1"`
	Type *uint8          `json:"type" validate:"required"`
	Data json.RawMessage `json:"data" validate:"required"`
}

type UserId struct {
	ID string `json:"id" validate:"required,min=1"`
}
type SendPayloadMessage struct {
	RecevierIDs []string `json:"ids" validate:"required,min=1"`
	Payload     []byte   `json:"payload" validate:"required"`
}

type DatagramProxingMessage struct {
	RecevierIDs []string `json:"ids" validate:"required,min=1"`
}

type FetchFriendsOnline struct {
	FriendsIds []string `json:"friendsIds" validate:"required,min=1"`
}

type CredsMessage struct {
	Username string `json:"username" validate:"required,min=1"`
	Password string `json:"password" validate:"required,min=1"`
}

type ReplyMessage struct {
	Sender  string `json:"sender-id" validate:"required,min=1"`
	Payload []byte `json:"payload" validate:"required"`
}

func ToUserIdMessage(v *validator.Validator, data json.RawMessage) (*UserId, error) {
	op := "protocols.ToUserIdMessage"
	idMsg := &UserId{}
	if err := json.Unmarshal(data, idMsg); err != nil {
		return nil, errs.ErrInvalidJson(op, err)
	}
	if err := v.Validate.Struct(idMsg); err != nil {
		return nil, errs.ErrValidation(op, err)
	}
	return idMsg, nil
}

func ToSendPayloadMessage(v *validator.Validator, data json.RawMessage) (*SendPayloadMessage, error) {
	op := "protocols.ToSendPayloadMessage"
	connectMsg := &SendPayloadMessage{}
	if err := json.Unmarshal(data, connectMsg); err != nil {
		return nil, errs.ErrInvalidJson(op, err)
	}
	if err := v.Validate.Struct(connectMsg); err != nil {
		return nil, errs.ErrValidation(op, err)
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
		return nil, errs.ErrInvalidJson(op, err)
	}
	return replyMsg, nil
}

func ToDatagramProxingMessage(v *validator.Validator, data json.RawMessage) (*DatagramProxingMessage, error) {
	op := "protocols.ToDatagramProxingMessage"
	datagramMsg := &DatagramProxingMessage{}
	if err := json.Unmarshal(data, datagramMsg); err != nil {
		return nil, errs.ErrInvalidJson(op, err)
	}
	if err := v.Validate.Struct(datagramMsg); err != nil {
		return nil, errs.ErrValidation(op, err)
	}
	return datagramMsg, nil
}

func ToFetchFriendsOnlineMessage(v *validator.Validator, data json.RawMessage) (*FetchFriendsOnline, error) {
	op := "protocols.ToFetchFriendsOnlineMessage"
	friendsMsg := &FetchFriendsOnline{}
	if err := json.Unmarshal(data, friendsMsg); err != nil {
		return nil, errs.ErrInvalidJson(op, err)
	}
	if err := v.Validate.Struct(friendsMsg); err != nil {
		return nil, errs.ErrValidation(op, err)
	}
	return friendsMsg, nil
}
