package cronjob

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	sharedkafka "ticket/shared/kafka"
	"ticket/shared/repository"
)

const statusCancel = "cancel"

type Publisher interface {
	Publish(context.Context, sharedkafka.UpdatedTicket) error
}

type RefundProcessor interface {
	RefundTicket(context.Context, uuid.UUID, int64) (bool, error)
}

// CancelExpiredTicket refunds captured PayPal payments and publishes
// cancellation requests for pending tickets that exceeded their confirmation
// window.
type CancelExpiredTicket struct {
	repository   repository.TicketRepository
	publisher    Publisher
	refunder     RefundProcessor
	cancelAfter  time.Duration
	pollInterval time.Duration
	batchSize    int
	logger       *slog.Logger
	now          func() time.Time
}

func NewCancelExpiredTicket(
	repository repository.TicketRepository,
	publisher Publisher,
	refunder RefundProcessor,
	cancelAfter time.Duration,
	pollInterval time.Duration,
	batchSize int,
	logger *slog.Logger,
) *CancelExpiredTicket {
	if logger == nil {
		logger = slog.Default()
	}
	return &CancelExpiredTicket{
		repository: repository, publisher: publisher, refunder: refunder,
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
			job.logger.Error("cannot refund or publish expired ticket cancellations", "error", err)
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
	var processingErrors []error
	for _, ticket := range tickets {
		if _, err := job.refunder.RefundTicket(ctx, ticket.ID, ticket.UserID); err != nil {
			processingErrors = append(
				processingErrors,
				fmt.Errorf("refund ticket %s: %w", ticket.ID, err),
			)
			continue
		}
		message := sharedkafka.UpdatedTicket{
			ID: ticket.ID, UserID: ticket.UserID, EventID: ticket.EventID,
			ClientOrderID: ticket.ClientOrderID, Status: statusCancel,
		}
		if err := job.publisher.Publish(ctx, message); err != nil {
			processingErrors = append(
				processingErrors,
				fmt.Errorf("publish cancellation for ticket %s: %w", ticket.ID, err),
			)
			continue
		}
		published++
	}
	return published, errors.Join(processingErrors...)
}
