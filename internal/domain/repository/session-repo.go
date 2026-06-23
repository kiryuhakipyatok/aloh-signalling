package repository

import (
	"context"
	"errors"
	"sync"
	"test/internal/domain/models"
	"test/pkg/errs"
)

type SessionsRepo interface {
	NewSession(ctx context.Context, session models.Session) error
	AddInSession(ctx context.Context, sessionId string, user models.UserData) error
	DeleteSession(ctx context.Context, sessionId string) error
	DeleteFromSession(ctx context.Context, sessionId, userId string) error
	GetSessions(ctx context.Context, sessionId string) ([]string, error)
}

type sessionRepo struct {
	sync.Map
}

func NewSessionsRepo() SessionsRepo {
	return &sessionRepo{}
}

func (sr *sessionRepo) NewSession(ctx context.Context, session models.Session) error {
	op := "sessionRepo.NewSession"
	select {
	case <-ctx.Done():
		return errs.ErrRequestTimeout(op)
	default:
		if _, ok := sr.LoadOrStore(session.UserId, session); ok {
			return errs.ErrAlreadyExists(op)
		}
		return nil
	}
}

func (sr *sessionRepo) AddInSession(ctx context.Context, sessionId string, user models.UserData) error {
	op := "sessionRepo.AddInSession"
	select {
	case <-ctx.Done():
		return errs.ErrRequestTimeout(op)
	default:
		val, ok := sr.Load(sessionId)
		if !ok {
			return errs.ErrNotFound(op)
		}
		session, ok := val.(models.Session)
		if !ok {
			return errors.New("invalid value type")
		}
		session.ConnectedUsers[user.ID] = user
		return nil
	}
}

func (cr *sessionRepo) DeleteSession(ctx context.Context, sessionId string) error {
	op := "connectionsRepo.DeleteSession"
	select {
	case <-ctx.Done():
		return errs.ErrRequestTimeout(op)
	default:
		cr.Delete(sessionId)
		cr.Range(func(key, value any) bool {
			session, ok := value.(models.Session)
			if !ok {
				return false
			}
			delete(session.ConnectedUsers, sessionId)
			return true
		})
		return nil
	}
}

func (sr *sessionRepo) DeleteFromSession(ctx context.Context, sessionId, userId string) error {
	op := "sessionRepo.AddInSession"
	select {
	case <-ctx.Done():
		return errs.ErrRequestTimeout(op)
	default:
		val, ok := sr.Load(sessionId)
		if !ok {
			return errs.ErrNotFound(op)
		}
		session, ok := val.(models.Session)
		if !ok {
			return errors.New("invalid value type")
		}
		delete(session.ConnectedUsers, userId)
		return nil
	}
}

func (sr *sessionRepo) GetSessions(ctx context.Context, sessionId string) ([]string, error) {
	op := "sessionRepo.GetSessions"
	select {
	case <-ctx.Done():
		return nil, errs.ErrRequestTimeout(op)
	default:
		connectedUsers := []string{}
		val, ok := sr.Load(sessionId)
		if !ok {
			return nil, errs.ErrNotFound(op)
		}
		session, ok := val.(models.Session)
		if !ok {
			return nil, errs.ErrInvalidType(op)
		}

		for id := range session.ConnectedUsers {
			connectedUsers = append(connectedUsers, id)
		}

		return connectedUsers, nil
	}
}
