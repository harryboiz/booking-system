package handler

import (
	"errors"
	"net/http"
	"strconv"

	"ticket/service/api/apierror"
	"ticket/service/api/apiresponse"
	"ticket/service/api/dto"
	"ticket/service/api/validation"
	"ticket/shared/model/entity"
	"ticket/shared/repository"
)

type EventHandler struct {
	store repository.EventRepository
}

func NewEventHandler(store repository.EventRepository) *EventHandler {
	return &EventHandler{store: store}
}

func (h *EventHandler) CreateEvent(w http.ResponseWriter, r *http.Request) {
	input, err := validation.ValidateCreateEvent(r)
	if err != nil {
		apierror.Write(w, err)
		return
	}
	event, err := h.store.Create(r.Context(), entityFromInput(input))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	w.Header().Set("Location", "/events/"+strconv.FormatInt(event.ID, 10))
	apiresponse.Write(w, http.StatusCreated, eventDTO(event))
}

func (h *EventHandler) ListEvents(w http.ResponseWriter, r *http.Request) {
	if err := validation.ValidateListEvents(r); err != nil {
		apierror.Write(w, err)
		return
	}
	events, err := h.store.List(r.Context())
	if err != nil {
		writeStoreError(w, err)
		return
	}
	result := make([]dto.Event, 0, len(events))
	for _, event := range events {
		result = append(result, eventDTO(event))
	}
	apiresponse.Write(w, http.StatusOK, result)
}

func (h *EventHandler) GetEvent(w http.ResponseWriter, r *http.Request) {
	id, err := validation.ValidateGetEvent(r)
	if err != nil {
		apierror.Write(w, err)
		return
	}
	event, err := h.store.Get(r.Context(), id)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	apiresponse.Write(w, http.StatusOK, eventDTO(event))
}

func (h *EventHandler) UpdateEvent(w http.ResponseWriter, r *http.Request) {
	id, input, err := validation.ValidateUpdateEvent(r)
	if err != nil {
		apierror.Write(w, err)
		return
	}
	event, err := h.store.Update(r.Context(), id, entityFromInput(input))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	apiresponse.Write(w, http.StatusOK, eventDTO(event))
}

func (h *EventHandler) DeleteEvent(w http.ResponseWriter, r *http.Request) {
	id, err := validation.ValidateDeleteEvent(r)
	if err != nil {
		apierror.Write(w, err)
		return
	}
	if err := h.store.Delete(r.Context(), id); err != nil {
		writeStoreError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func entityFromInput(input dto.EventInput) entity.Event {
	return entity.Event{
		Name:         input.Name,
		Description:  input.Description,
		DateTime:     input.DateTime,
		TotalTickets: input.TotalTickets,
		TicketPrice:  input.TicketPrice,
	}
}

func eventDTO(event entity.Event) dto.Event {
	return dto.Event{
		ID:           strconv.FormatInt(event.ID, 10),
		Name:         event.Name,
		Description:  event.Description,
		DateTime:     event.DateTime,
		TotalTickets: event.TotalTickets,
		TicketPrice:  event.TicketPrice,
	}
}

func writeStoreError(w http.ResponseWriter, err error) {
	if errors.Is(err, repository.ErrEventNotFound) {
		apierror.Write(w, apierror.Wrap(http.StatusNotFound, "event not found", err))
		return
	}
	apierror.Write(w, err)
}
