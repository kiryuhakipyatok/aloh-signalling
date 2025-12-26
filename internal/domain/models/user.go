package models

import "github.com/quic-go/quic-go"

type User struct {
	ID      string
	Name    string
	Connect *quic.Conn
}
