package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"ticket/service/api/dto"
	"ticket/shared/kafka"
	"ticket/shared/model/entity"
)

const validPendingTicket = `{
  "user_id": 10,
  "event_id": 20,
  "client_order_id": " order-123 "
}`

type fakeInventory struct {
	orderID              uuid.UUID
	getOrderIDError      error
	getOrderIDCalls      int
	checkedUserID        int64
	checkedClientOrderID string
	setOrderIDError      error
	setOrderIDCalls      int
	setOrderUserID       int64
	setClientOrderID     string
	setOrderID           uuid.UUID
	event                entity.Event
	eventError           error
	eventChecks          int
}

func (inventory *fakeInventory) GetOrderID(
	_ context.Context,
	userID int64,
	clientOrderID string,
) (uuid.UUID, error) {
	inventory.getOrderIDCalls++
	inventory.checkedUserID = userID
	inventory.checkedClientOrderID = clientOrderID
	return inventory.orderID, inventory.getOrderIDError
}

func (inventory *fakeInventory) SetOrderID(
	_ context.Context,
	userID int64,
	clientOrderID string,
	orderID uuid.UUID,
) error {
	inventory.setOrderIDCalls++
	inventory.setOrderUserID = userID
	inventory.setClientOrderID = clientOrderID
	inventory.setOrderID = orderID
	return inventory.setOrderIDError
}

func (inventory *fakeInventory) GetEventByID(context.Context, int64) (entity.Event, error) {
	inventory.eventChecks++
	return inventory.event, inventory.eventError
}

type fakePublisher struct {
	message kafka.UpdatedTicket
	err     error
	calls   int
}

func (publisher *fakePublisher) Publish(_ context.Context, message kafka.UpdatedTicket) error {
	publisher.calls++
	publisher.message = message
	return publisher.err
}

func pendingTicketRequest(handler *TicketHandler, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/tickets/pending", bytes.NewBufferString(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.CreatePendingTicket(response, request)
	return response
}

func TestCreatePendingTicketPublishesMessage(t *testing.T) {
	inventory := &fakeInventory{event: entity.Event{ID: 20, TotalTickets: 1}}
	publisher := &fakePublisher{}
	response := pendingTicketRequest(NewTicketHandler(inventory, inventory, publisher), validPendingTicket)

	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if publisher.calls != 1 {
		t.Fatalf("publish calls = %d", publisher.calls)
	}
	if inventory.checkedUserID != 10 || inventory.checkedClientOrderID != "order-123" {
		t.Fatalf(
			"duplicate check = (%d, %q)",
			inventory.checkedUserID,
			inventory.checkedClientOrderID,
		)
	}
	if publisher.message.ID == uuid.Nil {
		t.Fatal("message id is nil")
	}
	if publisher.message.UserID != 10 || publisher.message.EventID != 20 ||
		publisher.message.ClientOrderID != "order-123" || publisher.message.Status != "pending" {
		t.Fatalf("unexpected message: %+v", publisher.message)
	}
	if inventory.setOrderIDCalls != 1 || inventory.setOrderUserID != 10 ||
		inventory.setClientOrderID != "order-123" || inventory.setOrderID != publisher.message.ID {
		t.Fatalf(
			"cached order = calls:%d user:%d client:%q order:%s",
			inventory.setOrderIDCalls,
			inventory.setOrderUserID,
			inventory.setClientOrderID,
			inventory.setOrderID,
		)
	}
	var result dto.PendingTicket
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.TicketID != publisher.message.ID {
		t.Fatalf("ticket_id = %s, want %s", result.TicketID, publisher.message.ID)
	}
}

func TestCreatePendingTicketReturnsExistingOrderID(t *testing.T) {
	orderID := uuid.New()
	inventory := &fakeInventory{orderID: orderID}
	publisher := &fakePublisher{}
	response := pendingTicketRequest(NewTicketHandler(inventory, inventory, publisher), validPendingTicket)

	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var result dto.PendingTicket
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.TicketID != orderID {
		t.Fatalf("ticket_id = %s, want %s", result.TicketID, orderID)
	}
	if inventory.eventChecks != 0 {
		t.Fatalf("event checks = %d", inventory.eventChecks)
	}
	if publisher.calls != 0 {
		t.Fatalf("publish calls = %d", publisher.calls)
	}
	if inventory.setOrderIDCalls != 0 {
		t.Fatalf("set order id calls = %d", inventory.setOrderIDCalls)
	}
}

func TestCreatePendingTicketReturnsSoldOut(t *testing.T) {
	inventory := &fakeInventory{event: entity.Event{ID: 20, TotalTickets: 100, PendingTickets: 80, ConfirmTickets: 20}}
	publisher := &fakePublisher{}
	response := pendingTicketRequest(
		NewTicketHandler(inventory, inventory, publisher),
		validPendingTicket,
	)

	if response.Code != http.StatusConflict {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if publisher.calls != 0 {
		t.Fatalf("publish calls = %d", publisher.calls)
	}
}

func TestCreatePendingTicketReturnsErrorWhenPublishFails(t *testing.T) {
	inventory := &fakeInventory{event: entity.Event{ID: 20, TotalTickets: 1}}
	publisher := &fakePublisher{err: errors.New("kafka unavailable")}
	response := pendingTicketRequest(NewTicketHandler(inventory, inventory, publisher), validPendingTicket)

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if inventory.eventChecks != 1 {
		t.Fatalf("event checks = %d", inventory.eventChecks)
	}
	if inventory.setOrderIDCalls != 0 {
		t.Fatalf("set order id calls = %d", inventory.setOrderIDCalls)
	}
}

func TestCreatePendingTicketReturnsErrorWhenSetOrderIDFails(t *testing.T) {
	inventory := &fakeInventory{
		event:           entity.Event{ID: 20, TotalTickets: 1},
		setOrderIDError: errors.New("redis unavailable"),
	}
	publisher := &fakePublisher{}
	response := pendingTicketRequest(NewTicketHandler(inventory, inventory, publisher), validPendingTicket)

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if publisher.calls != 1 || inventory.setOrderIDCalls != 1 {
		t.Fatalf("publish calls = %d, set order id calls = %d", publisher.calls, inventory.setOrderIDCalls)
	}
}

func TestCreatePendingTicketReturnsErrorWhenRedisCheckFails(t *testing.T) {
	inventory := &fakeInventory{getOrderIDError: errors.New("redis unavailable")}
	publisher := &fakePublisher{}
	response := pendingTicketRequest(NewTicketHandler(inventory, inventory, publisher), validPendingTicket)

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if inventory.eventChecks != 0 || publisher.calls != 0 {
		t.Fatalf("event checks = %d, publish calls = %d", inventory.eventChecks, publisher.calls)
	}
}

func TestCreatePendingTicketReturnsErrorWhenEventCacheFails(t *testing.T) {
	inventory := &fakeInventory{eventError: errors.New("redis unavailable")}
	publisher := &fakePublisher{}
	response := pendingTicketRequest(NewTicketHandler(inventory, inventory, publisher), validPendingTicket)

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if publisher.calls != 0 {
		t.Fatalf("publish calls = %d", publisher.calls)
	}
}

func TestCreatePendingTicketValidation(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"invalid user", `{"user_id":0,"event_id":1,"client_order_id":"order-1"}`},
		{"invalid event", `{"user_id":1,"event_id":-1,"client_order_id":"order-1"}`},
		{"missing client order", `{"user_id":1,"event_id":1,"client_order_id":" "}`},
		{"unknown field", `{"user_id":1,"event_id":1,"client_order_id":"order-1","extra":true}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := pendingTicketRequest(
				NewTicketHandler(&fakeInventory{}, &fakeInventory{}, &fakePublisher{}),
				test.body,
			)
			if response.Code != http.StatusBadRequest && response.Code != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
		})
	}
}
