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
)

const validPendingTicket = `{
  "user_id": 10,
  "event_id": 20,
  "client_order_id": " order-123 "
}`

type fakeInventory struct {
	clientOrderExists      bool
	clientOrderExistsError error
	clientOrderChecks      int
	checkedUserID          int64
	checkedClientOrderID   string
	available              bool
	availableError         error
	availableChecks        int
}

func (inventory *fakeInventory) ClientOrderIDExists(
	_ context.Context,
	userID int64,
	clientOrderID string,
) (bool, error) {
	inventory.clientOrderChecks++
	inventory.checkedUserID = userID
	inventory.checkedClientOrderID = clientOrderID
	return inventory.clientOrderExists, inventory.clientOrderExistsError
}

func (inventory *fakeInventory) HasAvailable(context.Context, int64) (bool, error) {
	inventory.availableChecks++
	return inventory.available, inventory.availableError
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
	inventory := &fakeInventory{available: true}
	publisher := &fakePublisher{}
	response := pendingTicketRequest(NewTicketHandler(inventory, publisher), validPendingTicket)

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
	var result dto.PendingTicket
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.TicketID != publisher.message.ID {
		t.Fatalf("ticket_id = %s, want %s", result.TicketID, publisher.message.ID)
	}
}

func TestCreatePendingTicketReturnsConflictForExistingClientOrderID(t *testing.T) {
	inventory := &fakeInventory{clientOrderExists: true, available: true}
	publisher := &fakePublisher{}
	response := pendingTicketRequest(NewTicketHandler(inventory, publisher), validPendingTicket)

	if response.Code != http.StatusConflict {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if got := response.Body.String(); got != "{\"error\":\"client_order_id already exists\"}\n" {
		t.Fatalf("body = %q", got)
	}
	if inventory.availableChecks != 0 {
		t.Fatalf("available checks = %d", inventory.availableChecks)
	}
	if publisher.calls != 0 {
		t.Fatalf("publish calls = %d", publisher.calls)
	}
}

func TestCreatePendingTicketReturnsSoldOut(t *testing.T) {
	inventory := &fakeInventory{available: false}
	publisher := &fakePublisher{}
	response := pendingTicketRequest(
		NewTicketHandler(inventory, publisher),
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
	inventory := &fakeInventory{available: true}
	publisher := &fakePublisher{err: errors.New("kafka unavailable")}
	response := pendingTicketRequest(NewTicketHandler(inventory, publisher), validPendingTicket)

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if inventory.availableChecks != 1 {
		t.Fatalf("available checks = %d", inventory.availableChecks)
	}
}

func TestCreatePendingTicketReturnsErrorWhenRedisCheckFails(t *testing.T) {
	inventory := &fakeInventory{clientOrderExistsError: errors.New("redis unavailable")}
	publisher := &fakePublisher{}
	response := pendingTicketRequest(NewTicketHandler(inventory, publisher), validPendingTicket)

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if inventory.availableChecks != 0 || publisher.calls != 0 {
		t.Fatalf("available checks = %d, publish calls = %d", inventory.availableChecks, publisher.calls)
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
				NewTicketHandler(&fakeInventory{available: true}, &fakePublisher{}),
				test.body,
			)
			if response.Code != http.StatusBadRequest && response.Code != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
		})
	}
}
