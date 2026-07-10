package alohsignalling

import "github.com/kiryuhakipyatok/aloh-signalling/internal/protocols"

type (
	Message                = protocols.Message
	UserId                 = protocols.UserId
	SendPayloadMessage     = protocols.SendPayloadMessage
	DatagramProxingMessage = protocols.DatagramProxingMessage
	CredsMessage           = protocols.CredsMessage
	ReplyMessage           = protocols.ReplyMessage
	ResponseMessage        = protocols.ResponseMessage
	FetchFriendsOnline     = protocols.FetchFriendsOnline
)
