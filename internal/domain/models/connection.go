package models

import (
	"github.com/google/uuid"
	"github.com/quic-go/quic-go"
)

type Connection struct {
	ID      uuid.UUID
	Connect *quic.Conn
}