package api

import (
	"net/http"
	"strings"
	"time"

	"github.com/agent-observatory/agent-observatory-go/internal/model"
)

func (s *Server) handleListProjects(w http.ResponseWriter, r *http.Request) {
	limit, err := parseQueryInt(r, "limit", 50, 1, 200)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "bad_request", "invalid limit", false)
		return
	}
	offset, err := parseQueryInt(r, "offset", 0, 0, 1000000)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "bad_request", "invalid offset", false)
		return
	}
	q := r.URL.Query()
	rangeName := strings.TrimSpace(q.Get("range"))
	if rangeName == "" {
		rangeName = "month"
	}
	page, err := s.store.ListProjects(r.Context(), model.ProjectListFilter{
		Limit:  limit,
		Offset: offset,
		Query:  q.Get("q"),
		Range:  rangeName,
		Agent:  strings.TrimSpace(q.Get("agent")),
		Model:  strings.TrimSpace(q.Get("model")),
		Now:    time.Now().UTC(),
	})
	if err != nil {
		writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) handleGetProject(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	detail, err := s.store.GetProject(r.Context(), id, time.Now().UTC())
	if err != nil {
		writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, detail)
}

func (s *Server) handleProjectTimeseries(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rangeName := strings.TrimSpace(r.URL.Query().Get("range"))
	if rangeName == "" {
		rangeName = "day"
	}
	ts, err := s.store.GetProjectTimeseries(r.Context(), id, rangeName, time.Now().UTC())
	if err != nil {
		writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, ts)
}
