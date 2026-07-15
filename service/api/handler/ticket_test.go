package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"ticket/service/api/dto"
	"ticket/shared/kafka"
	"ticket/shared/model/entity"
	"ticket/shared/repository"
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
	ticket               entity.Ticket
	getTicketError       error
	getTicketCalls       int
	checkedTicketID      uuid.UUID
	doneTicket           entity.TicketDone
	getDoneTicketError   error
	getDoneByIDCalls     int
	getDoneByClientCalls int
	checkedDoneUserID    int64
	checkedDoneTicketID  uuid.UUID
	checkedDoneClientID  string
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

func (inventory *fakeInventory) GetTicketByID(
	_ context.Context,
	ticketID uuid.UUID,
) (entity.Ticket, error) {
	inventory.getTicketCalls++
	inventory.checkedTicketID = ticketID
	return inventory.ticket, inventory.getTicketError
}

func (inventory *fakeInventory) GetDoneTicketByID(
	_ context.Context,
	userID int64,
	ticketID uuid.UUID,
) (entity.TicketDone, error) {
	inventory.getDoneByIDCalls++
	inventory.checkedDoneUserID = userID
	inventory.checkedDoneTicketID = ticketID
	return inventory.doneTicket, inventory.getDoneTicketError
}

func (inventory *fakeInventory) GetDoneTicketByClientOrderID(
	_ context.Context,
	userID int64,
	clientOrderID string,
) (entity.TicketDone, error) {
	inventory.getDoneByClientCalls++
	inventory.checkedDoneUserID = userID
	inventory.checkedDoneClientID = clientOrderID
	return inventory.doneTicket, inventory.getDoneTicketError
}

func (inventory *fakeInventory) FindPendingTicketsByEventIDs(
	context.Context,
	[]int64,
) ([]entity.Ticket, error) {
	return nil, nil
}

func (inventory *fakeInventory) FindDoneTicketsByEventIDs(
	context.Context,
	[]int64,
) ([]entity.TicketDone, error) {
	return nil, nil
}

func (inventory *fakeInventory) FindPendingTicketsByIDs(
	context.Context,
	[]uuid.UUID,
) ([]entity.Ticket, error) {
	return nil, nil
}

func (inventory *fakeInventory) FindDoneTicketsByIDs(
	context.Context,
	[]uuid.UUID,
) ([]entity.TicketDone, error) {
	return nil, nil
}

func (inventory *fakeInventory) PersistTicketChanges(
	context.Context,
	[]entity.Ticket,
	[]entity.Ticket,
	[]entity.TicketDone,
	[]entity.Event,
) error {
	return nil
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

func confirmTicketRequest(
	handler *TicketHandler,
	userID int64,
	ticketID uuid.UUID,
) *httptest.ResponseRecorder {
	body := fmt.Sprintf(`{"user_id":%d,"ticket_id":%q}`, userID, ticketID)
	request := httptest.NewRequest(http.MethodPost, "/tickets/confirm", bytes.NewBufferString(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ConfirmTicket(response, request)
	return response
}

func getTicketRequest(handler *TicketHandler, query string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodGet, "/tickets?"+query, nil)
	response := httptest.NewRecorder()
	handler.GetTicketByID(response, request)
	return response
}

func TestGetTicketByIDReturnsPendingTicketFromRedis(t *testing.T) {
	ticketID := uuid.New()
	inventory := &fakeInventory{ticket: entity.Ticket{
		ID: ticketID, EventID: 20, UserID: 10, ClientOrderID: "order-123", Status: ticketStatusPending,
	}}
	handler := NewTicketHandler(inventory, inventory, inventory, &fakePublisher{})

	response := getTicketRequest(handler, "user_id=10&ticket_id="+ticketID.String())

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if inventory.getOrderIDCalls != 0 || inventory.getTicketCalls != 1 {
		t.Fatalf(
			"redis calls = order:%d ticket:%d",
			inventory.getOrderIDCalls,
			inventory.getTicketCalls,
		)
	}
	if inventory.getDoneByIDCalls != 0 || inventory.getDoneByClientCalls != 0 {
		t.Fatalf(
			"database calls = id:%d client:%d",
			inventory.getDoneByIDCalls,
			inventory.getDoneByClientCalls,
		)
	}
	var result dto.Ticket
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.ID != ticketID || result.UserID != 10 || result.Status != ticketStatusPending {
		t.Fatalf("ticket = %+v", result)
	}
}

func TestGetTicketByClientOrderIDReturnsPendingTicketFromRedis(t *testing.T) {
	ticketID := uuid.New()
	inventory := &fakeInventory{
		orderID: ticketID,
		ticket: entity.Ticket{
			ID: ticketID, EventID: 20, UserID: 10, ClientOrderID: "order-123", Status: ticketStatusPending,
		},
	}
	handler := NewTicketHandler(inventory, inventory, inventory, &fakePublisher{})

	response := getTicketRequest(handler, "user_id=10&client_order_id=%20order-123%20")

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if inventory.getOrderIDCalls != 1 || inventory.checkedClientOrderID != "order-123" ||
		inventory.getTicketCalls != 1 || inventory.checkedTicketID != ticketID {
		t.Fatalf(
			"redis lookup = order calls:%d client:%q ticket calls:%d ticket:%s",
			inventory.getOrderIDCalls,
			inventory.checkedClientOrderID,
			inventory.getTicketCalls,
			inventory.checkedTicketID,
		)
	}
	if inventory.getDoneByClientCalls != 0 {
		t.Fatalf("database calls = %d", inventory.getDoneByClientCalls)
	}
}

func TestGetTicketByIDFallsBackToDoneTicket(t *testing.T) {
	ticketID := uuid.New()
	inventory := &fakeInventory{doneTicket: entity.TicketDone{
		ID: ticketID, EventID: 20, UserID: 10, ClientOrderID: "order-123", Status: ticketStatusConfirm,
	}}
	handler := NewTicketHandler(inventory, inventory, inventory, &fakePublisher{})

	response := getTicketRequest(handler, "user_id=10&ticket_id="+ticketID.String())

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if inventory.getTicketCalls != 1 || inventory.getDoneByIDCalls != 1 ||
		inventory.checkedDoneUserID != 10 || inventory.checkedDoneTicketID != ticketID {
		t.Fatalf(
			"lookups = redis:%d db:%d user:%d ticket:%s",
			inventory.getTicketCalls,
			inventory.getDoneByIDCalls,
			inventory.checkedDoneUserID,
			inventory.checkedDoneTicketID,
		)
	}
	var result dto.Ticket
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.ID != ticketID || result.Status != ticketStatusConfirm {
		t.Fatalf("ticket = %+v", result)
	}
}

func TestGetTicketByClientOrderIDFallsBackToDoneTicket(t *testing.T) {
	ticketID := uuid.New()
	inventory := &fakeInventory{doneTicket: entity.TicketDone{
		ID: ticketID, EventID: 20, UserID: 10, ClientOrderID: "order-123", Status: ticketStatusConfirm,
	}}
	handler := NewTicketHandler(inventory, inventory, inventory, &fakePublisher{})

	response := getTicketRequest(handler, "user_id=10&client_order_id=order-123")

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if inventory.getOrderIDCalls != 1 || inventory.getTicketCalls != 0 ||
		inventory.getDoneByClientCalls != 1 || inventory.checkedDoneUserID != 10 ||
		inventory.checkedDoneClientID != "order-123" {
		t.Fatalf(
			"lookups = order:%d redis ticket:%d db:%d user:%d client:%q",
			inventory.getOrderIDCalls,
			inventory.getTicketCalls,
			inventory.getDoneByClientCalls,
			inventory.checkedDoneUserID,
			inventory.checkedDoneClientID,
		)
	}
}

func TestGetTicketByIDReturnsNotFound(t *testing.T) {
	ticketID := uuid.New()
	inventory := &fakeInventory{getDoneTicketError: repository.ErrTicketNotFound}
	handler := NewTicketHandler(inventory, inventory, inventory, &fakePublisher{})

	response := getTicketRequest(handler, "user_id=10&ticket_id="+ticketID.String())

	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestGetTicketByIDValidation(t *testing.T) {
	ticketID := uuid.New().String()
	tests := []string{
		"ticket_id=" + ticketID,
		"user_id=0&ticket_id=" + ticketID,
		"user_id=10",
		"user_id=10&ticket_id=" + ticketID + "&client_order_id=order-123",
		"user_id=10&ticket_id=not-a-uuid",
		"user_id=10&client_order_id=order-123&extra=true",
	}
	handler := NewTicketHandler(&fakeInventory{}, &fakeInventory{}, &fakeInventory{}, &fakePublisher{})

	for _, query := range tests {
		response := getTicketRequest(handler, query)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("query = %q, status = %d, body = %s", query, response.Code, response.Body.String())
		}
	}
}

func TestCreatePendingTicketPublishesMessage(t *testing.T) {
	inventory := &fakeInventory{event: entity.Event{ID: 20, TotalTickets: 1}}
	publisher := &fakePublisher{}
	response := pendingTicketRequest(NewTicketHandler(inventory, inventory, inventory, publisher), validPendingTicket)

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
	response := pendingTicketRequest(NewTicketHandler(inventory, inventory, inventory, publisher), validPendingTicket)

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
		NewTicketHandler(inventory, inventory, inventory, publisher),
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
	response := pendingTicketRequest(NewTicketHandler(inventory, inventory, inventory, publisher), validPendingTicket)

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
	response := pendingTicketRequest(NewTicketHandler(inventory, inventory, inventory, publisher), validPendingTicket)

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
	response := pendingTicketRequest(NewTicketHandler(inventory, inventory, inventory, publisher), validPendingTicket)

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
	response := pendingTicketRequest(NewTicketHandler(inventory, inventory, inventory, publisher), validPendingTicket)

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
				NewTicketHandler(&fakeInventory{}, &fakeInventory{}, &fakeInventory{}, &fakePublisher{}),
				test.body,
			)
			if response.Code != http.StatusBadRequest && response.Code != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
		})
	}
}

func TestConfirmTicketPublishesConfirmMessage(t *testing.T) {
	ticketID := uuid.New()
	inventory := &fakeInventory{ticket: entity.Ticket{
		ID: ticketID, EventID: 20, UserID: 10, ClientOrderID: "order-123", Status: ticketStatusPending,
	}}
	publisher := &fakePublisher{}
	response := confirmTicketRequest(NewTicketHandler(inventory, inventory, inventory, publisher), 10, ticketID)

	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if inventory.getTicketCalls != 1 || inventory.checkedTicketID != ticketID {
		t.Fatalf("ticket lookup = calls:%d id:%s", inventory.getTicketCalls, inventory.checkedTicketID)
	}
	want := kafka.UpdatedTicket{
		ID: ticketID, EventID: 20, UserID: 10, ClientOrderID: "order-123", Status: ticketStatusConfirm,
	}
	if publisher.calls != 1 || publisher.message != want {
		t.Fatalf("published = calls:%d message:%+v", publisher.calls, publisher.message)
	}
	var result dto.PendingTicket
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.TicketID != ticketID {
		t.Fatalf("ticket_id = %s, want %s", result.TicketID, ticketID)
	}
}

func TestConfirmTicketReturnsNotFound(t *testing.T) {
	ticketID := uuid.New()
	inventory := &fakeInventory{}
	publisher := &fakePublisher{}
	response := confirmTicketRequest(NewTicketHandler(inventory, inventory, inventory, publisher), 10, ticketID)

	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if publisher.calls != 0 {
		t.Fatalf("publish calls = %d", publisher.calls)
	}
}

func TestConfirmTicketReturnsForbiddenForAnotherUser(t *testing.T) {
	ticketID := uuid.New()
	inventory := &fakeInventory{ticket: entity.Ticket{
		ID: ticketID, EventID: 20, UserID: 11, ClientOrderID: "order-123", Status: ticketStatusPending,
	}}
	publisher := &fakePublisher{}
	response := confirmTicketRequest(NewTicketHandler(inventory, inventory, inventory, publisher), 10, ticketID)

	if response.Code != http.StatusForbidden {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if publisher.calls != 0 {
		t.Fatalf("publish calls = %d", publisher.calls)
	}
}

func TestConfirmTicketReturnsConflictWhenNotPending(t *testing.T) {
	ticketID := uuid.New()
	inventory := &fakeInventory{ticket: entity.Ticket{
		ID: ticketID, EventID: 20, UserID: 10, ClientOrderID: "order-123", Status: ticketStatusConfirm,
	}}
	publisher := &fakePublisher{}
	response := confirmTicketRequest(NewTicketHandler(inventory, inventory, inventory, publisher), 10, ticketID)

	if response.Code != http.StatusConflict {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if publisher.calls != 0 {
		t.Fatalf("publish calls = %d", publisher.calls)
	}
}

func TestConfirmTicketReturnsErrorWhenRedisFails(t *testing.T) {
	ticketID := uuid.New()
	inventory := &fakeInventory{getTicketError: errors.New("redis unavailable")}
	publisher := &fakePublisher{}
	response := confirmTicketRequest(NewTicketHandler(inventory, inventory, inventory, publisher), 10, ticketID)

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if publisher.calls != 0 {
		t.Fatalf("publish calls = %d", publisher.calls)
	}
}

func TestConfirmTicketReturnsErrorWhenPublishFails(t *testing.T) {
	ticketID := uuid.New()
	inventory := &fakeInventory{ticket: entity.Ticket{
		ID: ticketID, EventID: 20, UserID: 10, ClientOrderID: "order-123", Status: ticketStatusPending,
	}}
	publisher := &fakePublisher{err: errors.New("kafka unavailable")}
	response := confirmTicketRequest(NewTicketHandler(inventory, inventory, inventory, publisher), 10, ticketID)

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestConfirmTicketValidation(t *testing.T) {
	inventory := &fakeInventory{}
	handler := NewTicketHandler(inventory, inventory, inventory, &fakePublisher{})

	if got := confirmTicketRequest(handler, 0, uuid.New()); got.Code != http.StatusUnprocessableEntity {
		t.Fatalf("invalid user status = %d, body = %s", got.Code, got.Body.String())
	}
	if got := confirmTicketRequest(handler, 10, uuid.Nil); got.Code != http.StatusUnprocessableEntity {
		t.Fatalf("missing ticket status = %d, body = %s", got.Code, got.Body.String())
	}
}
