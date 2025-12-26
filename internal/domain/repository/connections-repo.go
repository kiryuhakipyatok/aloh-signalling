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
	GetConnect(ctx context.Context, id string) (*quic.Conn, error)
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

func (cr *connectionsRepo) GetConnect(ctx context.Context, id string) (*quic.Conn, error) {
	op := "connectionsRepo.GetConnect"
	select {
	case <-ctx.Done():
		return nil, errs.ErrRequestTimeout(op)
	default:
		val, ok := cr.Load(id)
		if !ok {
			return nil, errs.ErrNotFound(op)
		}
		conn, ok := val.(*quic.Conn)
		if !ok {
			return nil, errors.New("invalid value type")
		}
		return conn, nil
	}
}
