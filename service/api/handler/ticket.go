package handler

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/google/uuid"

	"ticket/service/api/apierror"
	"ticket/service/api/apiresponse"
	"ticket/service/api/dto"
	"ticket/service/api/validation"
	"ticket/shared/kafka"
	"ticket/shared/model/entity"
	"ticket/shared/repository"
)

const (
	ticketStatusPending = "pending"
	ticketStatusConfirm = "confirm"
)

type TicketHandler struct {
	ticketCache TicketCache
	ticketStore repository.TicketRepository
	eventCache  EventCache
	publisher   TicketPublisher
}

type TicketCache interface {
	GetOrderID(ctx context.Context, userID int64, clientOrderID string) (uuid.UUID, error)
	GetTicketByID(ctx context.Context, ticketID uuid.UUID) (entity.Ticket, error)
	SetOrderID(ctx context.Context, userID int64, clientOrderID string, orderID uuid.UUID) error
}

type EventCache interface {
	GetEventByID(ctx context.Context, eventID int64) (entity.Event, error)
}

type TicketPublisher interface {
	Publish(ctx context.Context, ticket kafka.UpdatedTicket) error
}

func NewTicketHandler(
	ticketCache TicketCache,
	ticketStore repository.TicketRepository,
	eventCache EventCache,
	publisher TicketPublisher,
) *TicketHandler {
	return &TicketHandler{
		ticketCache: ticketCache,
		ticketStore: ticketStore,
		eventCache:  eventCache,
		publisher:   publisher,
	}
}

func (handler *TicketHandler) GetTicketByID(w http.ResponseWriter, r *http.Request) {
	input, err := validation.ValidateGetTicket(r)
	if err != nil {
		apierror.Write(w, err)
		return
	}

	ticketID := input.TicketID
	if ticketID == uuid.Nil {
		ticketID, err = handler.ticketCache.GetOrderID(
			r.Context(), input.UserID, input.ClientOrderID,
		)
		if err != nil {
			apierror.Write(w, err)
			return
		}
	}

	if ticketID != uuid.Nil {
		pendingTicket, cacheErr := handler.ticketCache.GetTicketByID(r.Context(), ticketID)
		if cacheErr != nil {
			apierror.Write(w, cacheErr)
			return
		}
		if pendingTicket.ID != uuid.Nil {
			if pendingTicket.UserID != input.UserID ||
				(input.ClientOrderID != "" && pendingTicket.ClientOrderID != input.ClientOrderID) {
				apierror.Write(w, apierror.New(http.StatusNotFound, "ticket not found"))
				return
			}
			apiresponse.Write(w, http.StatusOK, ticketDTO(pendingTicket))
			return
		}
	}

	var doneTicket entity.TicketDone
	if input.TicketID != uuid.Nil {
		doneTicket, err = handler.ticketStore.GetDoneTicketByID(
			r.Context(), input.UserID, input.TicketID,
		)
	} else {
		doneTicket, err = handler.ticketStore.GetDoneTicketByClientOrderID(
			r.Context(), input.UserID, input.ClientOrderID,
		)
	}
	if err != nil {
		if errors.Is(err, repository.ErrTicketNotFound) {
			apierror.Write(w, apierror.Wrap(http.StatusNotFound, "ticket not found", err))
			return
		}
		apierror.Write(w, err)
		return
	}

	apiresponse.Write(w, http.StatusOK, doneTicketDTO(doneTicket))
}

func (handler *TicketHandler) CreatePendingTicket(w http.ResponseWriter, r *http.Request) {
	input, err := validation.ValidateCreatePendingTicket(r)
	if err != nil {
		apierror.Write(w, err)
		return
	}

	clientOrderID := strings.TrimSpace(input.ClientOrderID)
	orderID, err := handler.ticketCache.GetOrderID(r.Context(), input.UserID, clientOrderID)
	if err != nil {
		apierror.Write(w, err)
		return
	}
	if orderID != uuid.Nil {
		apiresponse.Write(w, http.StatusAccepted, dto.PendingTicket{TicketID: orderID})
		return
	}

	event, err := handler.eventCache.GetEventByID(r.Context(), input.EventID)
	if err != nil {
		apierror.Write(w, err)
		return
	}
	remainingTickets := int64(event.TotalTickets) - event.PendingTickets - event.ConfirmTickets
	if remainingTickets <= 0 {
		apierror.Write(w, apierror.New(http.StatusConflict, "tickets sold out"))
		return
	}

	message := kafka.UpdatedTicket{
		ID:            uuid.New(),
		UserID:        input.UserID,
		EventID:       input.EventID,
		ClientOrderID: clientOrderID,
		Status:        ticketStatusPending,
	}
	if err := handler.publisher.Publish(r.Context(), message); err != nil {
		apierror.Write(w, err)
		return
	}
	if err := handler.ticketCache.SetOrderID(
		r.Context(), input.UserID, clientOrderID, message.ID,
	); err != nil {
		apierror.Write(w, err)
		return
	}

	apiresponse.Write(w, http.StatusAccepted, dto.PendingTicket{
		TicketID: message.ID,
	})
}

func (handler *TicketHandler) ConfirmTicket(w http.ResponseWriter, r *http.Request) {
	input, err := validation.ValidateConfirmTicket(r)
	if err != nil {
		apierror.Write(w, err)
		return
	}

	ticket, err := handler.ticketCache.GetTicketByID(r.Context(), input.TicketID)
	if err != nil {
		apierror.Write(w, err)
		return
	}
	if ticket.ID == uuid.Nil {
		apierror.Write(w, apierror.New(http.StatusNotFound, "ticket not found"))
		return
	}
	if ticket.Status != ticketStatusPending {
		apierror.Write(w, apierror.New(http.StatusConflict, "ticket is not pending"))
		return
	}
	if ticket.UserID != input.UserID {
		apierror.Write(w, apierror.New(http.StatusForbidden, "ticket does not belong to user"))
		return
	}

	message := kafka.UpdatedTicket{
		ID:            ticket.ID,
		UserID:        ticket.UserID,
		EventID:       ticket.EventID,
		ClientOrderID: ticket.ClientOrderID,
		Status:        ticketStatusConfirm,
	}
	if err := handler.publisher.Publish(r.Context(), message); err != nil {
		apierror.Write(w, err)
		return
	}

	apiresponse.Write(w, http.StatusAccepted, dto.PendingTicket{TicketID: ticket.ID})
}

func ticketDTO(ticket entity.Ticket) dto.Ticket {
	return dto.Ticket{
		ID:            ticket.ID,
		EventID:       ticket.EventID,
		UserID:        ticket.UserID,
		ClientOrderID: ticket.ClientOrderID,
		Status:        ticket.Status,
		CreatedAt:     ticket.CreatedAt,
		UpdatedAt:     ticket.UpdatedAt,
	}
}

func doneTicketDTO(ticket entity.TicketDone) dto.Ticket {
	return dto.Ticket{
		ID:            ticket.ID,
		EventID:       ticket.EventID,
		UserID:        ticket.UserID,
		ClientOrderID: ticket.ClientOrderID,
		Status:        ticket.Status,
		CreatedAt:     ticket.CreatedAt,
		UpdatedAt:     ticket.UpdatedAt,
	}
}
