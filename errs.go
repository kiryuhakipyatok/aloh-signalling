package alohsignalling

import "github.com/kiryuhakipyatok/aloh-signalling/internal/protocols"

const (
	SUCCESS          = protocols.SUCCESS
	NOT_FOUND        = protocols.NOT_FOUND
	ALREADY_EXISTS   = protocols.ALREADY_EXISTS
	REQUEST_TIMEOUT  = protocols.REQUEST_TIMEOUT
	PAYLOAD_SUCCESS  = protocols.PAYLOAD_SUCCESS
	INVALID_PROTOCOL = protocols.INVALID_PROTOCOL
	STREAM_ERROR     = protocols.STREAM_ERROR
	INVALID_TYPE     = protocols.INVALID_TYPE
	INTERNAL_ERROR   = protocols.INTERNAL_ERROR
)
