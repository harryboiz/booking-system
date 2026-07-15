package repository

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"ticket/shared/model/entity"
)

var ErrTicketNotFound = errors.New("ticket not found")

// TicketRepository contains persistence operations for tickets and their event stats.
type TicketRepository interface {
	GetDoneTicketByID(context.Context, int64, uuid.UUID) (entity.TicketDone, error)
	GetDoneTicketByClientOrderID(context.Context, int64, string) (entity.TicketDone, error)
	FindPendingTicketsByEventIDs(context.Context, []int64) ([]entity.Ticket, error)
	FindDoneTicketsByEventIDs(context.Context, []int64) ([]entity.TicketDone, error)
	FindUserTicketsByEventIDs(context.Context, []int64) ([]entity.UserTicket, error)
	FindPendingTicketsByIDs(context.Context, []uuid.UUID) ([]entity.Ticket, error)
	FindDoneTicketsByIDs(context.Context, []uuid.UUID) ([]entity.TicketDone, error)
	FindExpiredPendingTickets(context.Context, time.Time, int) ([]entity.Ticket, error)
	PersistTicketChanges(
		context.Context,
		[]entity.Ticket,
		[]entity.Ticket,
		[]entity.TicketDone,
		[]entity.Event,
		[]entity.UserTicket,
	) error
}
