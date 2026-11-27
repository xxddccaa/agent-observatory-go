package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/agent-observatory/agent-observatory-go/internal/config"
	"github.com/agent-observatory/agent-observatory-go/internal/indexer"
	"github.com/agent-observatory/agent-observatory-go/internal/model"
	"github.com/agent-observatory/agent-observatory-go/internal/storage"
)

const (
	ServiceHeaderName  = "X-Agent-Observatory-Service"
	ServiceHeaderValue = config.AppName
)

type ctxKey string

const requestIDKey ctxKey = "request_id"

type Server struct {
	cfg       config.Config
	store     *storage.Store
	indexer   *indexer.Indexer
	webFS     fs.FS
	startedAt string
	handler   http.Handler
}

func NewServer(cfg config.Config, store *storage.Store, idx *indexer.Indexer, webFS fs.FS) *Server {
	s := &Server{
		cfg:       cfg,
		store:     store,
		indexer:   idx,
		webFS:     webFS,
		startedAt: model.FormatUTC(time.Now()),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/meta", s.handleMeta)
	mux.HandleFunc("GET /api/health/live", s.handleLive)
	mux.HandleFunc("GET /api/health/ready", s.handleReady)
	mux.HandleFunc("GET /api/health", s.handleReady)

	mux.HandleFunc("GET /api/system/status", s.handleSystemStatus)
	mux.HandleFunc("GET /api/system/logs", s.handleSystemLogs)

	mux.HandleFunc("GET /api/sessions", s.handleListSessions)
	mux.HandleFunc("GET /api/sessions/{id}", s.handleGetSession)
	mux.HandleFunc("GET /api/sessions/{id}/messages", s.handleSessionMessages)
	mux.HandleFunc("GET /api/sessions/{id}/events", s.handleSessionEvents)
	mux.HandleFunc("GET /api/sessions/{id}/turns", s.handleSessionTurns)
	mux.HandleFunc("GET /api/sessions/{id}/tools", s.handleSessionTools)
	mux.HandleFunc("GET /api/sessions/{id}/search", s.handleSessionSearch)
	mux.HandleFunc("GET /api/sessions/{id}/export", s.handleSessionExport)

	mux.HandleFunc("GET /api/projects", s.handleListProjects)
	mux.HandleFunc("GET /api/projects/{id}", s.handleGetProject)
	mux.HandleFunc("GET /api/projects/{id}/timeseries", s.handleProjectTimeseries)

	mux.HandleFunc("GET /api/insights/overview", s.handleInsightsOverview)
	mux.HandleFunc("GET /api/insights/timeseries", s.handleInsightsTimeseries)
	mux.HandleFunc("GET /api/insights/latency", s.handleInsightsLatency)
	mux.HandleFunc("GET /api/insights/tools", s.handleInsightsTools)
	mux.HandleFunc("GET /api/insights/skills", s.handleInsightsSkills)
	mux.HandleFunc("GET /api/insights/agents", s.handleInsightsAgents)
	mux.HandleFunc("GET /api/insights/sources", s.handleInsightsSources)
	mux.HandleFunc("GET /api/insights/sessions/{id}/timeline", s.handleSessionTimeline)
	mux.HandleFunc("GET /api/insights/evidence", s.handleListEvidence)
	mux.HandleFunc("GET /api/insights/evidence/{id}", s.handleGetEvidence)
	mux.HandleFunc("GET /api/insights/export", s.handleInsightsExport)

	mux.HandleFunc("/", s.handleWebOrNotFound)

	s.handler = s.withRequestID(s.withAccessGuard(mux))
	return s
}

func (s *Server) Handler() http.Handler {
	return s.handler
}

func (s *Server) handleMeta(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, model.MetaResponse{
		App:         config.AppName,
		Version:     config.Version,
		DataDirHash: config.DataDirHash(s.cfg.DataDir),
	})
}

func (s *Server) handleLive(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, model.LiveHealth{
		Status:  "ok",
		Version: config.Version,
	})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.buildReadiness(r.Context()))
}

func (s *Server) buildReadiness(ctx context.Context) model.ReadyHealth {
	probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	components := map[string]model.ComponentHealth{}
	recovery := []string{}
	status := "ok"

	if err := s.store.Ping(probeCtx); err != nil {
		status = "degraded"
		components["sqlite"] = model.ComponentHealth{
			Status:  "down",
			Healthy: false,
			Message: "SQLite store is unreachable.",
			Detail:  "SQLite store is unreachable.",
		}
		recovery = append(recovery, "Inspect observatory.db permissions and local logs.")
	} else {
		components["sqlite"] = model.ComponentHealth{
			Status:  "up",
			Healthy: true,
		}
	}

	if s.indexer != nil {
		snap := s.indexer.Snapshot()
		if snap.LastError != "" {
			status = "degraded"
			components["indexer"] = model.ComponentHealth{
				Status:        "degraded",
				Healthy:       false,
				Message:       "One or more session sources failed during indexing.",
				LastSeenAt:    snap.LastSeenAt,
				LastSuccessAt: snap.LastSuccessAt,
			}
			recovery = append(recovery, "Inspect local logs for source parse or permission errors.")
		} else {
			compStatus := "up"
			if !snap.Running && snap.LastSeenAt == "" {
				compStatus = "starting"
			}
			components["indexer"] = model.ComponentHealth{
				Status:        compStatus,
				Healthy:       true,
				LastSeenAt:    snap.LastSeenAt,
				LastSuccessAt: snap.LastSuccessAt,
			}
		}
	}

	return model.ReadyHealth{
		Status:          status,
		Version:         config.Version,
		Environment:     s.cfg.Environment,
		Components:      components,
		RecoveryActions: recovery,
	}
}

func (s *Server) handleWebOrNotFound(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/api/") || r.URL.Path == "/api" {
		writeError(w, r, http.StatusNotFound, "not_found", "Not found", false)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed", false)
		return
	}
	if s.webFS == nil {
		writeJSON(w, http.StatusOK, map[string]string{
			"name":    config.AppName,
			"version": config.Version,
		})
		return
	}

	cleaned := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
	if cleaned != "" && cleaned != "." {
		if info, err := fs.Stat(s.webFS, cleaned); err == nil && !info.IsDir() {
			s.serveEmbeddedFile(w, r, cleaned)
			return
		}
	}
	s.serveEmbeddedFile(w, r, "index.html")
}

func (s *Server) serveEmbeddedFile(w http.ResponseWriter, r *http.Request, name string) {
	file, err := s.webFS.Open(name)
	if err != nil {
		writeError(w, r, http.StatusNotFound, "not_found", "Not found", false)
		return
	}
	defer func() { _ = file.Close() }()

	stat, err := file.Stat()
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "internal_error", "Failed to inspect asset", true)
		return
	}
	rs, ok := file.(io.ReadSeeker)
	if !ok {
		data, readErr := io.ReadAll(file)
		if readErr != nil {
			writeError(w, r, http.StatusInternalServerError, "internal_error", "Failed to read asset", true)
			return
		}
		http.ServeContent(w, r, name, stat.ModTime(), strings.NewReader(string(data)))
		return
	}
	// ServeContent avoids http.FileServer's 301 redirect loop when serving index.html for "/".
	http.ServeContent(w, r, name, stat.ModTime(), rs)
}

func (s *Server) withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := strings.TrimSpace(r.Header.Get("X-Request-ID"))
		if reqID == "" {
			reqID = newRequestID()
		}
		w.Header().Set("X-Request-ID", reqID)
		w.Header().Set(ServiceHeaderName, ServiceHeaderValue)
		if strings.HasPrefix(r.URL.Path, "/api/") {
			w.Header().Set("Cache-Control", "no-store")
		}
		ctx := context.WithValue(r.Context(), requestIDKey, reqID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Server) withAccessGuard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		allowed := allowedHosts(s.cfg)
		if allowed != nil {
			hostName := extractHostname(r.Host)
			if _, ok := allowed[hostName]; !ok {
				writeError(
					w,
					r,
					http.StatusForbidden,
					"host_not_allowed",
					"This host name is not allowed. Open the observatory via 127.0.0.1 or localhost, or list the name in AGENT_OBS_ALLOWED_HOSTS.",
					false,
				)
				return
			}
		}

		if reason := crossSiteReason(r.Method, r.Host, r.Header); reason != "" {
			writeError(
				w,
				r,
				http.StatusForbidden,
				"cross_origin_refused",
				"State-changing requests must come from the observatory itself.",
				false,
			)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func allowedHosts(cfg config.Config) map[string]struct{} {
	loopback := map[string]struct{}{
		"127.0.0.1": {},
		"localhost": {},
		"::1":       {},
	}
	if len(cfg.AllowedHosts) > 0 {
		out := make(map[string]struct{}, len(loopback)+len(cfg.AllowedHosts))
		for k := range loopback {
			out[k] = struct{}{}
		}
		for _, item := range cfg.AllowedHosts {
			normalized := strings.ToLower(strings.TrimSpace(item))
			if normalized == "*" {
				return nil
			}
			if normalized != "" {
				out[normalized] = struct{}{}
			}
		}
		return out
	}

	bindHost := extractHostname(cfg.ListenAddr)
	if bindHost == "localhost" || bindHost == "::1" || strings.HasPrefix(bindHost, "127.") {
		return loopback
	}
	// Explicit non-loopback bind (e.g. 0.0.0.0) allows network callers.
	return nil
}

func extractHostname(hostPort string) string {
	trimmed := strings.TrimSpace(hostPort)
	if trimmed == "" {
		return ""
	}
	if host, _, err := net.SplitHostPort(trimmed); err == nil {
		return strings.ToLower(strings.Trim(host, "[]"))
	}
	return strings.ToLower(strings.Trim(trimmed, "[]"))
}

func crossSiteReason(method string, host string, headers http.Header) string {
	switch strings.ToUpper(method) {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return ""
	}
	if origin := headers.Get("Origin"); origin != "" {
		if origin == "null" {
			return "opaque origin"
		}
		parsed, err := url.Parse(origin)
		if err != nil || parsed.Host == "" {
			return "malformed origin"
		}
		if !strings.EqualFold(parsed.Host, host) {
			return "cross-origin request"
		}
		return ""
	}
	if strings.EqualFold(headers.Get("Sec-Fetch-Site"), "cross-site") {
		return "cross-site request"
	}
	return ""
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, r *http.Request, status int, code, message string, retryable bool) {
	reqID, _ := r.Context().Value(requestIDKey).(string)
	writeJSON(w, status, model.ErrorEnvelope{
		Error: model.ErrorDetail{
			Code:      code,
			Message:   message,
			RequestID: reqID,
			Retryable: retryable,
		},
	})
}

func writeStoreError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, storage.ErrNotFound):
		writeError(w, r, http.StatusNotFound, "not_found", "Resource not found", false)
	case errors.Is(err, storage.ErrInvalidCursor), errors.Is(err, storage.ErrInvalidFilter):
		writeError(w, r, http.StatusBadRequest, "bad_request", err.Error(), false)
	default:
		writeError(w, r, http.StatusInternalServerError, "internal_error", "Internal storage error", true)
	}
}

func parseQueryInt(r *http.Request, name string, defVal, minVal, maxVal int) (int, error) {
	raw := strings.TrimSpace(r.URL.Query().Get(name))
	if raw == "" {
		return defVal, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < minVal || n > maxVal {
		return 0, storage.ErrInvalidFilter
	}
	return n, nil
}

func parseQueryBool(r *http.Request, name string) (*bool, error) {
	raw := strings.TrimSpace(r.URL.Query().Get(name))
	if raw == "" {
		return nil, nil
	}
	b, err := strconv.ParseBool(raw)
	if err != nil {
		return nil, storage.ErrInvalidFilter
	}
	return &b, nil
}

func newRequestID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "req-fallback"
	}
	return hex.EncodeToString(raw[:])
}
