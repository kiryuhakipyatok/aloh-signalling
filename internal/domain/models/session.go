package models

type Session struct {
	UserId         string
	ConnectedUsers map[string]UserData
}