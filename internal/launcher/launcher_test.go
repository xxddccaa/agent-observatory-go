package launcher

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-observatory/agent-observatory-go/internal/config"
	"github.com/agent-observatory/agent-observatory-go/internal/model"
)

func TestControlChannelStatusAndStopRoundtrip(t *testing.T) {
	dir := t.TempDir()
	if err := config.EnsureDirs(dir); err != nil {
		t.Fatalf("EnsureDirs: %v", err)
	}

	var stopped atomic.Bool
	var srv *ControlServer
	var err error
	srv, err = StartControlServer(
		dir,
		func() StatusInfo {
			return StatusInfo{
				Status:     "running",
				PID:        os.Getpid(),
				ListenAddr: "127.0.0.1:8125",
				URL:        "http://127.0.0.1:8125",
				DataDir:    dir,
				LogPath:    LogPath(dir),
				StartedAt:  "2026-11-26T00:00:00.000000Z",
				Version:    config.Version,
			}
		},
		func() {
			stopped.Store(true)
			_ = srv.Close()
		},
	)
	if err != nil {
		t.Fatalf("StartControlServer: %v", err)
	}

	info, err := QueryStatus(context.Background(), dir)
	if err != nil {
		t.Fatalf("QueryStatus: %v", err)
	}
	if info.Status != "running" || info.PID != os.Getpid() || info.URL != "http://127.0.0.1:8125" {
		t.Fatalf("unexpected status info: %+v", info)
	}

	if err := RequestStopAndWait(context.Background(), dir, 5*time.Second); err != nil {
		t.Fatalf("RequestStopAndWait: %v", err)
	}
	if !stopped.Load() {
		t.Fatal("expected stop callback to be invoked")
	}

	if _, err := QueryStatus(context.Background(), dir); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("QueryStatus after stop err = %v, want ErrNotRunning", err)
	}
	if err := RequestStopAndWait(context.Background(), dir, time.Second); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("second RequestStopAndWait err = %v, want ErrNotRunning", err)
	}
}

func TestPortFallbackSkipsThirdPartyApp(t *testing.T) {
	// Occupy a free base port with a non-observatory HTTP server.
	occupierLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen occupier: %v", err)
	}
	defer func() { _ = occupierLn.Close() }()

	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"app":"other-service"}`))
		}),
	}
	go func() { _ = srv.Serve(occupierLn) }()
	defer func() { _ = srv.Close() }()

	_, portStr, _ := net.SplitHostPort(occupierLn.Addr().String())
	basePort, _ := strconv.Atoi(portStr)

	ln, err := ListenWithAppAwareFallback(context.Background(), net.JoinHostPort("127.0.0.1", portStr))
	if err != nil {
		t.Fatalf("ListenWithAppAwareFallback: %v", err)
	}
	defer func() { _ = ln.Close() }()

	_, actualPortStr, _ := net.SplitHostPort(ln.Addr().String())
	actualPort, _ := strconv.Atoi(actualPortStr)
	if actualPort <= basePort {
		t.Fatalf("expected fallback port > %d, got %d", basePort, actualPort)
	}
}

func TestPortFallbackReusesSameAppWithoutSwitching(t *testing.T) {
	occupierLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen mock same-app: %v", err)
	}
	defer func() { _ = occupierLn.Close() }()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/meta", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Agent-Observatory-Service", config.AppName)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(model.MetaResponse{
			App:         config.AppName,
			Version:     config.Version,
			DataDirHash: "0123456789abcdef",
		})
	})
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(occupierLn) }()
	defer func() { _ = srv.Close() }()

	_, err = ListenWithAppAwareFallback(context.Background(), occupierLn.Addr().String())
	var sameErr *SameAppRunningError
	if !errors.As(err, &sameErr) {
		t.Fatalf("expected SameAppRunningError, got %v", err)
	}
	if sameErr.URL != "http://"+occupierLn.Addr().String() {
		t.Fatalf("reused URL = %q, want http://%s", sameErr.URL, occupierLn.Addr().String())
	}
}

func TestPortZeroSkipsFallback(t *testing.T) {
	ln, err := ListenWithAppAwareFallback(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("ListenWithAppAwareFallback(:0): %v", err)
	}
	_ = ln.Close()
}

func TestValidateLoopbackBrowserURLAndAddressMapping(t *testing.T) {
	valid := []string{
		"http://127.0.0.1:8125",
		"http://localhost:8125",
		"http://[::1]:8125",
	}
	for _, raw := range valid {
		if err := ValidateLoopbackBrowserURL(raw); err != nil {
			t.Fatalf("ValidateLoopbackBrowserURL(%q) unexpected error: %v", raw, err)
		}
	}

	invalid := []string{
		"",
		"http://192.168.1.20:8125",
		"http://evil.example:8125",
		"javascript:alert(1)",
		"file:///etc/passwd",
		"http://user:pass@127.0.0.1:8125",
		"http://127.0.0.1:8125/#frag",
	}
	for _, raw := range invalid {
		if err := ValidateLoopbackBrowserURL(raw); err == nil {
			t.Fatalf("ValidateLoopbackBrowserURL(%q) expected error, got nil", raw)
		}
	}

	if got := BrowserURLForListenAddr("0.0.0.0:8125"); got != "http://127.0.0.1:8125" {
		t.Fatalf("BrowserURLForListenAddr(0.0.0.0:8125) = %q", got)
	}
	if got := BrowserURLForListenAddr("192.168.1.5:8125"); got != "" {
		t.Fatalf("BrowserURLForListenAddr(192.168.1.5:8125) = %q, want empty", got)
	}
}

func TestRotateLogAndChildEnvCleanup(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "observatory.log")
	if err := os.WriteFile(logPath, []byte(strings.Repeat("x", 128)), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := RotateLogIfNeeded(logPath, 64); err != nil {
		t.Fatalf("RotateLogIfNeeded: %v", err)
	}
	if _, err := os.Stat(logPath + ".1"); err != nil {
		t.Fatalf("expected rotated log .1: %v", err)
	}
	if _, err := os.Stat(logPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected original log path rotated away, stat err = %v", err)
	}

	if runtime.GOOS != "windows" {
		f, openedPath, err := OpenLogFile(dir)
		if err != nil {
			t.Fatalf("OpenLogFile: %v", err)
		}
		_ = f.Close()
		info, err := os.Stat(openedPath)
		if err != nil {
			t.Fatalf("Stat(%s): %v", openedPath, err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("log perm = %o, want 0600", info.Mode().Perm())
		}
	}

	t.Setenv(EnvDaemonChild, "1")
	t.Setenv(EnvHandshakeFD, "99")
	t.Setenv(EnvReadyFile, "/tmp/ready.json")
	isChild, _ := ReadAndClearChildEnv()
	if !isChild {
		t.Fatal("expected isChild=true")
	}
	for _, key := range []string{EnvDaemonChild, EnvHandshakeFD, EnvReadyFile} {
		if got := os.Getenv(key); got != "" {
			t.Fatalf("expected %s unset after ReadAndClearChildEnv, got %q", key, got)
		}
	}
}
