package repository

import (
	"context"

	"github.com/google/uuid"

	"ticket/shared/model/entity"
)

// TicketRepository contains persistence operations for tickets and their event stats.
type TicketRepository interface {
	ReconcileEventStats(context.Context, []int) ([]entity.Event, error)
	FindEventsByIDs(context.Context, []int64) ([]entity.Event, error)
	FindPendingTicketsByIDs(context.Context, []uuid.UUID) ([]entity.Ticket, error)
	FindDoneTicketsByIDs(context.Context, []uuid.UUID) ([]entity.TicketDone, error)
	PersistTicketChanges(
		context.Context,
		[]entity.Ticket,
		[]entity.Ticket,
		[]entity.TicketDone,
		[]entity.Event,
	) error
}
