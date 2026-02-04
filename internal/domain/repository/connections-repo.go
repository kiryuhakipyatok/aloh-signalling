package repository

import (
	"context"
	"errors"
	"sync"
	"test/internal/domain/models"
	"test/pkg/errs"
)

type ConnectionsRepo interface {
	AddConnect(ctx context.Context, user *models.User) error
	DeleteConnect(ctx context.Context, id string) error
	GetConnects(ctx context.Context, ids []string) ([]models.User, error)
	FetchAll(ctx context.Context) ([]string, error)
}

type connectionsRepo struct {
	sync.Map
}

func NewConnectionsRepo() ConnectionsRepo {
	return &connectionsRepo{}
}

func (cr *connectionsRepo) AddConnect(ctx context.Context, user *models.User) error {
	op := "connectionsRepo.AddConnect"
	select {
	case <-ctx.Done():
		return errs.ErrRequestTimeout(op)
	default:
		if _, ok := cr.LoadOrStore(user.ID, *user); ok {
			return errs.ErrAlreadyExists(op)
		}
		return nil
	}
}

func (cr *connectionsRepo) DeleteConnect(ctx context.Context, id string) error {
	op := "connectionsRepo.DeleteConnect"
	select {
	case <-ctx.Done():
		return errs.ErrRequestTimeout(op)
	default:
		if _, ok := cr.LoadAndDelete(id); !ok {
			return errs.ErrNotFound(op)
		}
		return nil
	}
}

func (cr *connectionsRepo) GetConnects(ctx context.Context, ids []string) ([]models.User, error) {
	op := "connectionsRepo.GetConnects"
	users := []models.User{}
	select {
	case <-ctx.Done():
		return nil, errs.ErrRequestTimeout(op)
	default:
		for _, id := range ids {
			val, ok := cr.Load(id)
			if !ok {
				return nil, errs.ErrNotFound(op)
			}
			user, ok := val.(models.User)
			if !ok {
				return nil, errors.New("invalid value type")
			}
			users = append(users, user)
		}
		return users, nil
	}
}

func (cr *connectionsRepo) FetchAll(ctx context.Context) ([]string, error) {
	op := "connectionsRepo.GetAll"
	ids := []string{}
	select {
	case <-ctx.Done():
		return nil, errs.ErrRequestTimeout(op)
	default:
		cr.Range(func(key, value any) bool {
			id, ok := key.(string)
			if !ok {
				return false
			}
			ids = append(ids, id)
			return true
		})
	}
	return ids, nil
}
