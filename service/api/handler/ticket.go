package handler

import (
	"context"
	"net/http"
	"strings"

	"github.com/google/uuid"

	"ticket/service/api/apierror"
	"ticket/service/api/apiresponse"
	"ticket/service/api/dto"
	"ticket/service/api/validation"
	"ticket/shared/kafka"
)

type TicketHandler struct {
	inventory TicketInventory
	publisher TicketPublisher
}

type TicketInventory interface {
	ClientOrderIDExists(ctx context.Context, userID int64, clientOrderID string) (bool, error)
	HasAvailable(ctx context.Context, eventID int64) (bool, error)
}

type TicketPublisher interface {
	Publish(ctx context.Context, ticket kafka.UpdatedTicket) error
}

func NewTicketHandler(
	inventory TicketInventory,
	publisher TicketPublisher,
) *TicketHandler {
	return &TicketHandler{inventory: inventory, publisher: publisher}
}

func (handler *TicketHandler) CreatePendingTicket(w http.ResponseWriter, r *http.Request) {
	input, err := validation.ValidateCreatePendingTicket(r)
	if err != nil {
		apierror.Write(w, err)
		return
	}

	clientOrderID := strings.TrimSpace(input.ClientOrderID)
	exists, err := handler.inventory.ClientOrderIDExists(r.Context(), input.UserID, clientOrderID)
	if err != nil {
		apierror.Write(w, err)
		return
	}
	if exists {
		apierror.Write(w, apierror.New(http.StatusConflict, "client_order_id already exists"))
		return
	}

	available, err := handler.inventory.HasAvailable(r.Context(), input.EventID)
	if err != nil {
		apierror.Write(w, err)
		return
	}
	if !available {
		apierror.Write(w, apierror.New(http.StatusConflict, "tickets sold out"))
		return
	}

	message := kafka.UpdatedTicket{
		ID:            uuid.New(),
		UserID:        input.UserID,
		EventID:       input.EventID,
		ClientOrderID: clientOrderID,
		Status:        "pending",
	}
	if err := handler.publisher.Publish(r.Context(), message); err != nil {
		apierror.Write(w, err)
		return
	}

	apiresponse.Write(w, http.StatusAccepted, dto.PendingTicket{
		TicketID: message.ID,
	})
}
