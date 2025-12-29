package repository

import (
	"context"
	"errors"
	"sync"
	"test/pkg/errs"

	"github.com/quic-go/quic-go"
)

type ConnectionsRepo interface {
	AddConnect(ctx context.Context, id string, conn *quic.Conn) error
	DeleteConnect(ctx context.Context, id string) error
	GetConnects(ctx context.Context, ids []string) ([]*quic.Conn, error)
}

type connectionsRepo struct {
	sync.Map
}

func NewConnectionsRepo() ConnectionsRepo {
	return &connectionsRepo{}
}

func (cr *connectionsRepo) AddConnect(ctx context.Context, id string, conn *quic.Conn) error {
	op := "connectionsRepo.AddConnect"
	select {
	case <-ctx.Done():
		return errs.ErrRequestTimeout(op)
	default:
		if _, ok := cr.LoadOrStore(id, conn); ok {
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

func (cr *connectionsRepo) GetConnects(ctx context.Context, ids []string) ([]*quic.Conn, error) {
	op := "connectionsRepo.GetConnect"
	connects := []*quic.Conn{}
	select {
	case <-ctx.Done():
		return nil, errs.ErrRequestTimeout(op)
	default:
		for _, id := range ids {
			val, ok := cr.Load(id)
			if !ok {
				return nil, errs.ErrNotFound(op)
			}
			conn, ok := val.(*quic.Conn)
			if !ok {
				return nil, errors.New("invalid value type")
			}
			connects = append(connects, conn)
		}
		return connects, nil
	}
}
