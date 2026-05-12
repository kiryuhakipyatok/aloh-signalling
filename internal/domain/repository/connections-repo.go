package repository

import (
	"context"
	"errors"
	"sync"
	"test/internal/domain/models"
	"test/pkg/errs"
)

type ConnectionsRepo interface {
	AddConnect(ctx context.Context, connection *models.Connection) (*models.Connection, error)
	DeleteConnect(ctx context.Context, id string, conn *models.Connection) error
	GetConnects(ctx context.Context, ids []string) ([]*models.Connection, error)
	FetchAll(ctx context.Context) ([]string, error)
	IsExists(ctx context.Context, id string) error
}

type connectionsRepo struct {
	sync.Map
}

func NewConnectionsRepo() ConnectionsRepo {
	return &connectionsRepo{}
}

func (cr *connectionsRepo) AddConnect(ctx context.Context, connection *models.Connection) (*models.Connection, error) {
	op := "connectionsRepo.AddConnect"
	var (
		oldConn *models.Connection
	)
	select {
	case <-ctx.Done():
		return nil, errs.ErrRequestTimeout(op)
	default:
		var val any
		val, old := cr.Swap(connection.ID, connection)
		if old {
			oldConn = val.(*models.Connection)
		}
		//return errs.ErrAlreadyExists(op)
		return oldConn, nil
	}
}

func (cr *connectionsRepo) DeleteConnect(ctx context.Context, id string, conn *models.Connection) error {
	op := "connectionsRepo.DeleteConnect"
	select {
	case <-ctx.Done():
		return errs.ErrRequestTimeout(op)
	default:
		deleted := cr.CompareAndDelete(id, conn)
		if !deleted {
			return errs.ErrNotFound(op)
		}
		return nil
	}
}

func (cr *connectionsRepo) GetConnects(ctx context.Context, ids []string) ([]*models.Connection, error) {
	op := "connectionsRepo.GetConnects"
	users := []*models.Connection{}
	select {
	case <-ctx.Done():
		return nil, errs.ErrRequestTimeout(op)
	default:
		for _, id := range ids {
			val, ok := cr.Load(id)
			if !ok {
				return nil, errs.ErrNotFound(op)
			}
			connection, ok := val.(*models.Connection)
			if !ok {
				return nil, errors.New("invalid value type")
			}
			users = append(users, connection)
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

func (cr *connectionsRepo) IsExists(ctx context.Context, id string) error {
	op := "connectionsRepo.IsExists"
	select {
	case <-ctx.Done():
		return errs.ErrRequestTimeout(op)
	default:
		if _, ok := cr.Load(id); !ok {
			return errs.ErrNotFound(op)
		}
		return nil
	}
}
