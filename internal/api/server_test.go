package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent-observatory/agent-observatory-go/internal/adapters"
	"github.com/agent-observatory/agent-observatory-go/internal/config"
	"github.com/agent-observatory/agent-observatory-go/internal/indexer"
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
	return NewServer(cfg, store, nil, webFS)
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

func TestV1EndpointsWithIndexedDataAndSecretRedaction(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	if err := config.EnsureDirs(dataDir); err != nil {
		t.Fatal(err)
	}
	store, err := storage.Open(ctx, dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	projDir := filepath.Join(t.TempDir(), "repo-api")
	if err := os.MkdirAll(filepath.Join(projDir, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	codexDir := filepath.Join(t.TempDir(), "codex")
	if err := os.MkdirAll(codexDir, 0o755); err != nil {
		t.Fatal(err)
	}
	tsNow := time.Now().UTC().Format("2006-01-02T15:04:05Z")
	secretKey := "sk-1234567890abcdefghijklmnop"
	rollout := strings.Join([]string{
		`{"timestamp":"` + tsNow + `","type":"session_meta","payload":{"id":"thr-api","cwd":"` + filepath.ToSlash(projDir) + `"}}`,
		`{"timestamp":"` + tsNow + `","type":"event_msg","payload":{"type":"task_started"}}`,
		`{"timestamp":"` + tsNow + `","type":"event_msg","payload":{"type":"user_message","message":"Use key ` + secretKey + `"}}`,
		`{"timestamp":"` + tsNow + `","type":"response_item","payload":{"type":"function_call","call_id":"call-api","name":"exec_command","arguments":"{\"cmd\":\"echo ` + secretKey + `\"}"}}`,
		`{"timestamp":"` + tsNow + `","type":"response_item","payload":{"type":"function_call_output","call_id":"call-api","output":"Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.abcdef123456"}}`,
		`{"timestamp":"` + tsNow + `","type":"event_msg","payload":{"type":"agent_message","message":"Done"}}`,
		`{"timestamp":"` + tsNow + `","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":10,"output_tokens":5,"total_tokens":15},"last_token_usage":{"input_tokens":10,"output_tokens":5,"total_tokens":15}}}}`,
		`{"timestamp":"` + tsNow + `","type":"event_msg","payload":{"type":"task_complete"}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(filepath.Join(codexDir, "rollout-api.jsonl"), []byte(rollout), 0o644); err != nil {
		t.Fatal(err)
	}

	idx := indexer.New(store, indexer.Options{
		CustomAdapters: []adapters.Adapter{adapters.NewCodexAdapter(codexDir)},
	})
	if _, err := idx.RunOnce(ctx, true); err != nil {
		t.Fatal(err)
	}

	// Write a log entry containing a secret to verify /api/system/logs redaction.
	logFile := filepath.Join(dataDir, "logs", "observatory.log")
	if err := os.WriteFile(logFile, []byte("started with key "+secretKey+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	webFS, _ := webui.FS()
	srv := NewServer(config.Config{DataDir: dataDir, ListenAddr: config.DefaultListenAddr, Environment: "test"}, store, idx, webFS)

	getJSON := func(path string, wantCode int) []byte {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8125"+path, nil)
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)
		if rec.Code != wantCode {
			t.Fatalf("GET %s = %d, want %d: %s", path, rec.Code, wantCode, rec.Body.String())
		}
		return rec.Body.Bytes()
	}

	var listRes model.SessionListResult
	if err := json.Unmarshal(getJSON("/api/sessions", http.StatusOK), &listRes); err != nil || len(listRes.Sessions) != 1 {
		t.Fatalf("GET /api/sessions unexpected: %+v, err=%v", listRes, err)
	}
	sessID := listRes.Sessions[0].ID

	// Verify messages, events, tools, export, and system logs redact secrets while DB retains raw JSON.
	for _, path := range []string{
		"/api/sessions/" + sessID,
		"/api/sessions/" + sessID + "/messages",
		"/api/sessions/" + sessID + "/events",
		"/api/sessions/" + sessID + "/turns",
		"/api/sessions/" + sessID + "/tools",
		"/api/sessions/" + sessID + "/search?q=Bearer",
		"/api/sessions/" + sessID + "/export?format=markdown",
		"/api/sessions/" + sessID + "/export?format=json",
		"/api/insights/sessions/" + sessID + "/timeline",
		"/api/system/status",
		"/api/system/logs",
	} {
		body := string(getJSON(path, http.StatusOK))
		if strings.Contains(body, secretKey) || strings.Contains(body, "eyJhbGciOiJIUzI1NiJ9.abcdef123456") {
			t.Fatalf("secret leaked in response for %s: %s", path, body)
		}
	}

	var projects model.ProjectsPage
	if err := json.Unmarshal(getJSON("/api/projects?range=all", http.StatusOK), &projects); err != nil || projects.Total != 1 {
		t.Fatalf("GET /api/projects unexpected: %+v, err=%v", projects, err)
	}
	projID := projects.Items[0].ID
	_ = getJSON("/api/projects/"+projID, http.StatusOK)
	_ = getJSON("/api/projects/"+projID+"/timeseries?range=day", http.StatusOK)

	for _, path := range []string{
		"/api/insights/overview",
		"/api/insights/timeseries",
		"/api/insights/latency",
		"/api/insights/tools",
		"/api/insights/skills",
		"/api/insights/agents",
		"/api/insights/sources",
		"/api/insights/evidence",
		"/api/insights/export?format=csv",
		"/api/insights/export?format=json",
	} {
		_ = getJSON(path, http.StatusOK)
	}

	// Verify 404 and 400 error envelopes.
	_ = getJSON("/api/sessions/does-not-exist", http.StatusNotFound)
	_ = getJSON("/api/sessions/"+sessID+"/export?format=xml", http.StatusBadRequest)
	_ = getJSON("/api/projects/does-not-exist", http.StatusNotFound)
}
