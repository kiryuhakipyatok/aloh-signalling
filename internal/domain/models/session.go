package models

import "github.com/google/uuid"

type Session struct {
	UserId         uuid.UUID
	ConnectedUsers map[uuid.UUID]UserData
}
