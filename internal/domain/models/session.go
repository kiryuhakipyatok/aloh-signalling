package models

import "github.com/google/uuid"

type Session struct {
	UserId         uuid.UUID
	ConnectedUsers []uuid.UUID
}