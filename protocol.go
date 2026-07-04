package alohsignalling

import "github.com/kiryuhakipyatok/aloh-signalling/internal/protocols"

type (
	Message                = protocols.Message
	UserId                 = protocols.Message
	SendPayloadMessage     = protocols.SendPayloadMessage
	DatagramProxingMessage = protocols.DatagramProxingMessage
	CredsMessage           = protocols.CredsMessage
	ReplyMessage           = protocols.ReplyMessage
)