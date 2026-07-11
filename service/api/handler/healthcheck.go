package handler

import (
	"net/http"

	"ticket/service/api/apiresponse"
)

type HeathCheckHandler struct{}

func NewHeathCheckHandler() *HeathCheckHandler {
	return &HeathCheckHandler{}
}

func (api *HeathCheckHandler) Health(w http.ResponseWriter, _ *http.Request) {
	apiresponse.Write(w, http.StatusOK, map[string]string{"status": "ok"})
}
