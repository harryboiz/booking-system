package api

import (
	"net/http"

	"ticket/service/api/handler"
)

func NewHandler(eventHandler *handler.EventHandler, ticketHandler *handler.TicketHandler) http.Handler {
	mux := http.NewServeMux()
	registerHealthCheckRoutes(mux)
	registerEventRoutes(mux, eventHandler)
	registerTicketRoutes(mux, ticketHandler)
	return mux
}

func registerHealthCheckRoutes(mux *http.ServeMux) {
	apiHandler := handler.NewHeathCheckHandler()
	mux.HandleFunc("GET /health", apiHandler.Health)
}

func registerEventRoutes(mux *http.ServeMux, eventHandler *handler.EventHandler) {
	mux.HandleFunc("POST /events", eventHandler.CreateEvent)
	mux.HandleFunc("GET /events", eventHandler.ListEvents)
	mux.HandleFunc("GET /events/{id}", eventHandler.GetEvent)
	mux.HandleFunc("PUT /events/{id}", eventHandler.UpdateEvent)
	mux.HandleFunc("DELETE /events/{id}", eventHandler.DeleteEvent)
}

func registerTicketRoutes(mux *http.ServeMux, ticketHandler *handler.TicketHandler) {
	mux.HandleFunc("POST /tickets/pending", ticketHandler.CreatePendingTicket)
}
