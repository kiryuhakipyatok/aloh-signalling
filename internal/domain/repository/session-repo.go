package repository

import (
	"context"
	"errors"
	"slices"
	"sync"

	"github.com/kiryuhakipyatok/aloh-signalling/internal/domain/models"
	"github.com/kiryuhakipyatok/aloh-signalling/pkg/errs"

	"github.com/google/uuid"
)

type SessionsRepo interface {
	NewSession(ctx context.Context, session models.Session) error
	AddInSession(ctx context.Context, sessionId, userId uuid.UUID) error
	DeleteSession(ctx context.Context, sessionId uuid.UUID) error
	DeleteFromSession(ctx context.Context, sessionId, userId uuid.UUID) error
	GetSessions(ctx context.Context, sessionId uuid.UUID) ([]uuid.UUID, error)
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

func (sr *sessionRepo) AddInSession(ctx context.Context, sessionId, userId uuid.UUID) error {
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
		session.ConnectedUsers = append(session.ConnectedUsers, userId)
		sr.Swap(sessionId, session)
		return nil
	}
}

func (cr *sessionRepo) DeleteSession(ctx context.Context, sessionId uuid.UUID) error {
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
			session.ConnectedUsers = deleteFromSessionConns(session.ConnectedUsers, sessionId)
			return true
		})
		return nil
	}
}

func (sr *sessionRepo) DeleteFromSession(ctx context.Context, sessionId, userId uuid.UUID) error {
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
		session.ConnectedUsers = deleteFromSessionConns(session.ConnectedUsers, userId)
		sr.Swap(sessionId, session)
		return nil
	}
}

func (sr *sessionRepo) GetSessions(ctx context.Context, sessionId uuid.UUID) ([]uuid.UUID, error) {
	op := "sessionRepo.GetSessions"
	select {
	case <-ctx.Done():
		return nil, errs.ErrRequestTimeout(op)
	default:
		val, ok := sr.Load(sessionId)
		if !ok {
			return nil, errs.ErrNotFound(op)
		}
		session, ok := val.(models.Session)
		if !ok {
			return nil, errs.ErrInvalidType(op)
		}

		return session.ConnectedUsers, nil
	}
}

func deleteFromSessionConns(conns []uuid.UUID, user uuid.UUID) []uuid.UUID {
	return slices.DeleteFunc(conns, func(u uuid.UUID) bool {
		return u == user
	})
}
