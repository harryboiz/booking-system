package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"ticket/service/api/dto"
	"ticket/shared/model/entity"
	"ticket/shared/repository"
)

const validEvent = `{
  "name": "Go Conference",
  "description": "A conference for Go developers",
  "date_time": "2026-09-10T09:00:00+07:00",
  "total_tickets": 200,
  "ticket_price": 49.5
}`

type fakeEventRepository struct {
	events map[string]entity.Event
	nextID int
}

func newFakeEventRepository() *fakeEventRepository {
	return &fakeEventRepository{events: make(map[string]entity.Event)}
}

func (r *fakeEventRepository) Create(_ context.Context, event entity.Event) (entity.Event, error) {
	r.nextID++
	event.ID = int64(r.nextID)
	event.Name = strings.TrimSpace(event.Name)
	r.events[strconv.Itoa(r.nextID)] = event
	return event, nil
}

func (r *fakeEventRepository) List(context.Context) ([]entity.Event, error) {
	events := make([]entity.Event, 0, len(r.events))
	for id := 1; id <= r.nextID; id++ {
		if event, ok := r.events[strconv.Itoa(id)]; ok {
			events = append(events, event)
		}
	}
	return events, nil
}

func (r *fakeEventRepository) Get(_ context.Context, id string) (entity.Event, error) {
	event, ok := r.events[id]
	if !ok {
		return entity.Event{}, repository.ErrEventNotFound
	}
	return event, nil
}

func (r *fakeEventRepository) Update(_ context.Context, id string, event entity.Event) (entity.Event, error) {
	if _, ok := r.events[id]; !ok {
		return entity.Event{}, repository.ErrEventNotFound
	}
	event.ID, _ = strconv.ParseInt(id, 10, 64)
	event.Name = strings.TrimSpace(event.Name)
	r.events[id] = event
	return event, nil
}

func (r *fakeEventRepository) Delete(_ context.Context, id string) error {
	if _, ok := r.events[id]; !ok {
		return repository.ErrEventNotFound
	}
	delete(r.events, id)
	return nil
}

func newRouter(store repository.EventRepository) http.Handler {
	apiHandler := NewHeathCheckHandler()
	eventHandler := NewEventHandler(store)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", apiHandler.Health)
	mux.HandleFunc("POST /events", eventHandler.CreateEvent)
	mux.HandleFunc("GET /events", eventHandler.ListEvents)
	mux.HandleFunc("GET /events/{id}", eventHandler.GetEvent)
	mux.HandleFunc("PUT /events/{id}", eventHandler.UpdateEvent)
	mux.HandleFunc("DELETE /events/{id}", eventHandler.DeleteEvent)
	return mux
}

func request(t *testing.T, router http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, req)
	return response
}

func TestEventCRUD(t *testing.T) {
	router := newRouter(newFakeEventRepository())

	created := request(t, router, http.MethodPost, "/events", validEvent)
	if created.Code != http.StatusCreated {
		t.Fatalf("create status = %d, body = %s", created.Code, created.Body.String())
	}
	var event dto.Event
	if err := json.NewDecoder(created.Body).Decode(&event); err != nil {
		t.Fatal(err)
	}
	if event.ID != "1" || event.Name != "Go Conference" {
		t.Fatalf("unexpected created event: %+v", event)
	}

	got := request(t, router, http.MethodGet, "/events/1", "")
	if got.Code != http.StatusOK {
		t.Fatalf("get status = %d", got.Code)
	}

	updatedBody := `{
      "name":"Updated Event",
      "description":"Updated",
      "date_time":"2026-10-01T18:30:00Z",
      "total_tickets":50,
      "ticket_price":10
    }`
	updated := request(t, router, http.MethodPut, "/events/1", updatedBody)
	if updated.Code != http.StatusOK {
		t.Fatalf("update status = %d, body = %s", updated.Code, updated.Body.String())
	}
	if err := json.NewDecoder(updated.Body).Decode(&event); err != nil {
		t.Fatal(err)
	}
	if event.ID != "1" || event.Name != "Updated Event" {
		t.Fatalf("unexpected updated event: %+v", event)
	}

	listed := request(t, router, http.MethodGet, "/events", "")
	if listed.Code != http.StatusOK {
		t.Fatalf("list status = %d", listed.Code)
	}
	var events []dto.Event
	if err := json.NewDecoder(listed.Body).Decode(&events); err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("event count = %d", len(events))
	}

	deleted := request(t, router, http.MethodDelete, "/events/1", "")
	if deleted.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d", deleted.Code)
	}
	missing := request(t, router, http.MethodGet, "/events/1", "")
	if missing.Code != http.StatusNotFound {
		t.Fatalf("get deleted status = %d", missing.Code)
	}
}

func TestCreateEventValidation(t *testing.T) {
	router := newRouter(newFakeEventRepository())
	tests := []struct {
		name string
		body string
	}{
		{"empty name", `{"name":"","date_time":"2026-09-10T09:00:00Z","total_tickets":1,"ticket_price":1}`},
		{"missing date", `{"name":"Event","total_tickets":1,"ticket_price":1}`},
		{"negative tickets", `{"name":"Event","date_time":"2026-09-10T09:00:00Z","total_tickets":-1,"ticket_price":1}`},
		{"negative price", `{"name":"Event","date_time":"2026-09-10T09:00:00Z","total_tickets":1,"ticket_price":-1}`},
		{"tickets exceed database limit", `{"name":"Event","date_time":"2026-09-10T09:00:00Z","total_tickets":2147483648,"ticket_price":1}`},
		{"price exceeds database limit", `{"name":"Event","date_time":"2026-09-10T09:00:00Z","total_tickets":1,"ticket_price":10000000000}`},
		{"unknown field", `{"name":"Event","date_time":"2026-09-10T09:00:00Z","total_tickets":1,"ticket_price":1,"extra":true}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := request(t, router, http.MethodPost, "/events", tt.body)
			if response.Code != http.StatusBadRequest && response.Code != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
		})
	}
}

func TestNotFoundAndMethodNotAllowed(t *testing.T) {
	router := newRouter(newFakeEventRepository())
	if got := request(t, router, http.MethodGet, "/events/999", ""); got.Code != http.StatusNotFound {
		t.Fatalf("not found status = %d", got.Code)
	}
	if got := request(t, router, http.MethodPatch, "/events/1", validEvent); got.Code != http.StatusMethodNotAllowed {
		t.Fatalf("method not allowed status = %d", got.Code)
	}
}

func TestEventHandlerRequestValidation(t *testing.T) {
	router := newRouter(newFakeEventRepository())
	tests := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{"list query parameters", http.MethodGet, "/events?limit=10", ""},
		{"get invalid id", http.MethodGet, "/events/not-a-number", ""},
		{"update invalid id", http.MethodPut, "/events/0", validEvent},
		{"delete invalid id", http.MethodDelete, "/events/-1", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := request(t, router, tt.method, tt.path, tt.body)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
		})
	}
}
