package models

import "github.com/quic-go/quic-go"

type User struct {
	ID      string
	Connect *quic.Conn
}
