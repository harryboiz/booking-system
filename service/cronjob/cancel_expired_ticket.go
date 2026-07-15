package cronjob

import (
	"context"
	"errors"
	"log/slog"
	"time"

	sharedkafka "ticket/shared/kafka"
	"ticket/shared/repository"
)

const statusCancel = "cancel"

type Publisher interface {
	Publish(context.Context, sharedkafka.UpdatedTicket) error
}

// CancelExpiredTicket periodically publishes cancellation requests for every pending ticket
// that has exceeded its confirmation window.
type CancelExpiredTicket struct {
	repository   repository.ExpiredTicketRepository
	publisher    Publisher
	cancelAfter  time.Duration
	pollInterval time.Duration
	batchSize    int
	logger       *slog.Logger
	now          func() time.Time
}

func NewCancelExpiredTicket(
	repository repository.ExpiredTicketRepository,
	publisher Publisher,
	cancelAfter time.Duration,
	pollInterval time.Duration,
	batchSize int,
	logger *slog.Logger,
) *CancelExpiredTicket {
	if logger == nil {
		logger = slog.Default()
	}
	return &CancelExpiredTicket{
		repository: repository, publisher: publisher,
		cancelAfter: cancelAfter, pollInterval: pollInterval,
		batchSize: batchSize, logger: logger, now: time.Now,
	}
}

func (job *CancelExpiredTicket) Run(ctx context.Context) {
	job.pollAndLog(ctx)
	ticker := time.NewTicker(job.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			job.pollAndLog(ctx)
		}
	}
}

func (job *CancelExpiredTicket) pollAndLog(ctx context.Context) {
	count, err := job.poll(ctx)
	if err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			job.logger.Error("cannot publish expired ticket cancellations", "error", err)
		}
		return
	}
	if count > 0 {
		job.logger.Info("published expired ticket cancellations", "tickets", count)
	}
}

func (job *CancelExpiredTicket) poll(ctx context.Context) (int, error) {
	cutoff := job.now().UTC().Add(-job.cancelAfter)
	tickets, err := job.repository.FindExpiredPendingTickets(ctx, cutoff, job.batchSize)
	if err != nil {
		return 0, err
	}

	published := 0
	var publishErrors []error
	for _, ticket := range tickets {
		message := sharedkafka.UpdatedTicket{
			ID: ticket.ID, UserID: ticket.UserID, EventID: ticket.EventID,
			ClientOrderID: ticket.ClientOrderID, Status: statusCancel,
		}
		if err := job.publisher.Publish(ctx, message); err != nil {
			publishErrors = append(publishErrors, err)
			continue
		}
		published++
	}
	return published, errors.Join(publishErrors...)
}
