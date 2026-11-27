package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/agent-observatory/agent-observatory-go/internal/config"
	"github.com/agent-observatory/agent-observatory-go/internal/model"
	"github.com/agent-observatory/agent-observatory-go/internal/storage"
	"github.com/agent-observatory/agent-observatory-go/internal/webui"
)

func newTestServer(t *testing.T) *Server {
	t.Helper()
	dir := t.TempDir()
	store, err := storage.Open(context.Background(), dir)
	if err != nil {
		t.Fatalf("storage.Open() error = %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	webFS, err := webui.FS()
	if err != nil {
		t.Fatalf("webui.FS() error = %v", err)
	}
	cfg := config.Config{
		DataDir:     dir,
		ListenAddr:  config.DefaultListenAddr,
		Environment: "test",
	}
	return NewServer(cfg, store, webFS)
}

func TestMetaAndHealthEndpoints(t *testing.T) {
	srv := newTestServer(t)

	for _, path := range []string{"/api/meta", "/api/health/live", "/api/health/ready"} {
		req := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8125"+path, nil)
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("%s status = %d, want 200", path, rec.Code)
		}
		if got := rec.Header().Get(ServiceHeaderName); got != ServiceHeaderValue {
			t.Fatalf("%s %s = %q, want %q", path, ServiceHeaderName, got, ServiceHeaderValue)
		}
		if got := rec.Header().Get("Cache-Control"); got != "no-store" {
			t.Fatalf("%s Cache-Control = %q, want no-store", path, got)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8125/api/meta", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	var meta model.MetaResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &meta); err != nil {
		t.Fatalf("decode meta json: %v", err)
	}
	if meta.App != config.AppName || len(meta.DataDirHash) != 16 {
		t.Fatalf("unexpected meta payload: %+v", meta)
	}
}

func TestHostAllowlistAndCSRFAccessGuard(t *testing.T) {
	srv := newTestServer(t)

	// DNS rebinding attempt via unexpected Host must be rejected.
	req := httptest.NewRequest(http.MethodGet, "http://evil.example:8125/api/meta", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("evil Host status = %d, want 403", rec.Code)
	}

	// Cross-origin POST must be rejected even on 127.0.0.1.
	csrfReq := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8125/api/meta", strings.NewReader("{}"))
	csrfReq.Header.Set("Origin", "http://evil.example")
	csrfRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(csrfRec, csrfReq)
	if csrfRec.Code != http.StatusForbidden {
		t.Fatalf("cross-origin POST status = %d, want 403", csrfRec.Code)
	}

	// Embedded SPA HTML renders on "/" and unknown non-API routes without 301 loop.
	spaReq := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8125/history/123", nil)
	spaRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(spaRec, spaReq)
	if spaRec.Code != http.StatusOK || !strings.Contains(spaRec.Body.String(), "Agent Observatory") {
		t.Fatalf("SPA fallback code=%d body=%q", spaRec.Code, spaRec.Body.String())
	}

	// Unknown /api/* returns JSON ErrorEnvelope 404, not SPA HTML.
	api404Req := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8125/api/nonexistent", nil)
	api404Rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(api404Rec, api404Req)
	if api404Rec.Code != http.StatusNotFound {
		t.Fatalf("unknown /api/ status = %d, want 404", api404Rec.Code)
	}
}
