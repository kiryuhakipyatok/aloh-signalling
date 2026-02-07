package models

import "github.com/quic-go/quic-go"

type Connection struct {
	ID      string
	Connect *quic.Conn
}
