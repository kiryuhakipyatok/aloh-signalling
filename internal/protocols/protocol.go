package protocols

import (
	"encoding/json"

	"github.com/kiryuhakipyatok/aloh-signalling/pkg/errs"

	"github.com/kiryuhakipyatok/aloh-signalling/pkg/validator"

	"github.com/google/uuid"
)

type Message struct {
	Id   uuid.UUID       `json:"id" validate:"required,uuid"`
	Type *uint8          `json:"type" validate:"required"`
	Data json.RawMessage `json:"data" validate:"required"`
}

type SendPayloadMessage struct {
	RecevierIDs []uuid.UUID `json:"ids" validate:"required,min=1"`
	Payload     []byte      `json:"payload" validate:"required"`
}

type DatagramProxingMessage struct {
	RecevierIDs []uuid.UUID `json:"ids" validate:"required,min=1"`
}

type UserId struct {
	ID uuid.UUID `json:"id" validate:"required,uuid"`
}

// type UserData struct {
// 	Data models.UserData `json:"user-data" validate:"required"`
// }

type FetchFriendsOnline struct {
	FriendsIds []uuid.UUID `json:"friendsIds" validate:"required,min=1"`
}

type CredsMessage struct {
	Username string `json:"username" validate:"required,min=1"`
	Password string `json:"password" validate:"required,min=1"`
}

type ReplyMessage struct {
	Sender  uuid.UUID `json:"sender" validate:"required"`
	Payload []byte    `json:"payload" validate:"required"`
}

func ToUserIdMessage(v *validator.Validator, data json.RawMessage) (*UserId, error) {
	op := "protocols.ToUserIdMessage"
	userIdMsg := &UserId{}
	if err := json.Unmarshal(data, userIdMsg); err != nil {
		return nil, errs.ErrInvalidJson(op, err)
	}
	if err := v.Validate.Struct(userIdMsg); err != nil {
		return nil, errs.ErrValidation(op, err)
	}
	return userIdMsg, nil
}

// func ToUserDataMessage(v *validator.Validator, data json.RawMessage) (*UserData, error) {
// 	op := "protocols.ToUserDataMessage"
// 	userDataMsg := &UserData{}
// 	if err := json.Unmarshal(data, userDataMsg); err != nil {
// 		return nil, errs.ErrInvalidJson(op, err)
// 	}
// 	if err := v.Validate.Struct(userDataMsg); err != nil {
// 		return nil, errs.ErrValidation(op, err)
// 	}
// 	return userDataMsg, nil
// }

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

func NewReplyMessage(sender uuid.UUID, pyaload json.RawMessage) ([]byte, error) {
	op := "protocols.NewReplyMessage"
	rm := ReplyMessage{
		Sender:  sender,
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
