package repository

import (
	"context"
	"time"

	"ticket/shared/model/entity"
)

type ExpiredTicketRepository interface {
	FindExpiredPendingTickets(context.Context, time.Time, int) ([]entity.Ticket, error)
}
