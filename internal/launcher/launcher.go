package launcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/agent-observatory/agent-observatory-go/internal/config"
	"github.com/agent-observatory/agent-observatory-go/internal/model"
)

const (
	MaxPortFallbackAttempts = 20
	ProbeTimeout            = 800 * time.Millisecond
	ProbeDialTimeout        = 400 * time.Millisecond
	ProbeMaxBodyBytes       = 4096
	ReadinessTimeout        = 15 * time.Second
	StopWaitTimeout         = 10 * time.Second
	MaxLogBytes             = 10 * 1024 * 1024

	EnvDaemonChild = "AGENT_OBS_DAEMON_CHILD"
	EnvHandshakeFD = "AGENT_OBS_HANDSHAKE_FD"
	EnvReadyFile   = "AGENT_OBS_READY_FILE"
)

var (
	ErrNotRunning     = errors.New("agent-observatory is not running")
	ErrAlreadyRunning = errors.New("agent-observatory is already running")
)

type SameAppRunningError struct {
	URL string
}

func (e *SameAppRunningError) Error() string {
	return fmt.Sprintf("agent-observatory is already running at %s", e.URL)
}

type StatusInfo struct {
	Status     string `json:"status"`
	PID        int    `json:"pid"`
	ListenAddr string `json:"listen_addr"`
	URL        string `json:"url"`
	DataDir    string `json:"data_dir"`
	LogPath    string `json:"log_path"`
	StartedAt  string `json:"started_at"`
	Version    string `json:"version"`
}

type HandshakePayload struct {
	OK         bool   `json:"ok"`
	URL        string `json:"url,omitempty"`
	ListenAddr string `json:"listen_addr,omitempty"`
	PID        int    `json:"pid,omitempty"`
	Reused     bool   `json:"reused,omitempty"`
	Error      string `json:"error,omitempty"`
}

// ListenWithAppAwareFallback binds listenAddr, probing occupied fixed ports
// to reuse an existing agent-observatory instance or stepping port+1 up to 20 times.
func ListenWithAppAwareFallback(ctx context.Context, listenAddr string) (net.Listener, error) {
	host, portStr, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid listen address %q: %w", listenAddr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 0 || port > 65535 {
		return nil, fmt.Errorf("invalid listen port in %q", listenAddr)
	}

	// Port 0 explicitly requests an ephemeral port from the OS with no fallback loop.
	if port == 0 {
		ln, listenErr := net.Listen("tcp", listenAddr)
		if listenErr != nil {
			return nil, listenErr
		}
		return ln, nil
	}

	var lastErr error
	for attempt := 0; attempt <= MaxPortFallbackAttempts; attempt++ {
		candidatePort := port + attempt
		if candidatePort > 65535 {
			break
		}
		candidateAddr := net.JoinHostPort(host, strconv.Itoa(candidatePort))
		ln, listenErr := net.Listen("tcp", candidateAddr)
		if listenErr == nil {
			return ln, nil
		}
		if !isAddrInUse(listenErr) {
			return nil, listenErr
		}
		lastErr = listenErr

		if probeURL, sameApp := ProbeApp(ctx, host, candidatePort); sameApp {
			return nil, &SameAppRunningError{URL: probeURL}
		}
	}

	return nil, fmt.Errorf("no free port found starting at %s after %d attempts: %w", listenAddr, MaxPortFallbackAttempts, lastErr)
}

func ProbeApp(ctx context.Context, bindHost string, port int) (string, bool) {
	probeHost := probeTargetHost(bindHost)
	baseURL := "http://" + net.JoinHostPort(probeHost, strconv.Itoa(port))

	client := &http.Client{
		Timeout: ProbeTimeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Transport: &http.Transport{
			DialContext: (&net.Dialer{Timeout: ProbeDialTimeout}).DialContext,
		},
	}

	reqCtx, cancel := context.WithTimeout(ctx, ProbeTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, baseURL+"/api/meta", nil)
	if err != nil {
		return "", false
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", false
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", false
	}
	if resp.Header.Get("X-Agent-Observatory-Service") != config.AppName {
		return "", false
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, ProbeMaxBodyBytes))
	if err != nil {
		return "", false
	}
	var meta model.MetaResponse
	if err := json.Unmarshal(body, &meta); err != nil {
		return "", false
	}
	if meta.App != config.AppName {
		return "", false
	}
	return baseURL, true
}

func probeTargetHost(bindHost string) string {
	trimmed := strings.Trim(strings.TrimSpace(bindHost), "[]")
	switch trimmed {
	case "", "0.0.0.0":
		return "127.0.0.1"
	case "::":
		return "::1"
	default:
		return trimmed
	}
}

func BrowserURLForListenAddr(actualAddr string) string {
	host, port, err := net.SplitHostPort(actualAddr)
	if err != nil {
		return ""
	}
	cleanHost := strings.Trim(strings.TrimSpace(host), "[]")
	switch {
	case cleanHost == "", cleanHost == "0.0.0.0", cleanHost == "::":
		cleanHost = "127.0.0.1"
	case cleanHost == "localhost", cleanHost == "::1", strings.HasPrefix(cleanHost, "127."):
		// Keep loopback host as bound.
	default:
		return ""
	}
	candidate := "http://" + net.JoinHostPort(cleanHost, port)
	if err := ValidateLoopbackBrowserURL(candidate); err != nil {
		return ""
	}
	return candidate
}

func ValidateLoopbackBrowserURL(rawURL string) error {
	if rawURL == "" {
		return errors.New("empty browser url")
	}
	if strings.ContainsAny(rawURL, "\r\n\t\x00 ") {
		return errors.New("browser url contains forbidden control or whitespace characters")
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("parse browser url: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("unsupported browser url scheme %q", parsed.Scheme)
	}
	if parsed.User != nil || parsed.Fragment != "" {
		return errors.New("browser url must not contain credentials or fragment")
	}
	host := strings.ToLower(strings.Trim(parsed.Hostname(), "[]"))
	if host == "localhost" || host == "::1" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip != nil && ip.IsLoopback() {
		return nil
	}
	return fmt.Errorf("refusing to open non-loopback browser url host %q", host)
}

func OpenBrowser(rawURL string) error {
	if err := ValidateLoopbackBrowserURL(rawURL); err != nil {
		return err
	}
	return openBrowserPlatform(rawURL)
}

func LogPath(dataDir string) string {
	return filepath.Join(dataDir, "logs", "observatory.log")
}

func OpenLogFile(dataDir string) (*os.File, string, error) {
	path := LogPath(dataDir)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, "", fmt.Errorf("create log dir: %w", err)
	}
	if err := RotateLogIfNeeded(path, MaxLogBytes); err != nil {
		return nil, "", err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, "", fmt.Errorf("open log file %s: %w", path, err)
	}
	if runtime.GOOS != "windows" {
		if err := os.Chmod(path, 0o600); err != nil {
			_ = file.Close()
			return nil, "", fmt.Errorf("chmod log file %s: %w", path, err)
		}
	}
	return file, path, nil
}

func RotateLogIfNeeded(path string, maxBytes int64) error {
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("stat log file %s: %w", path, err)
	}
	if info.Size() < maxBytes {
		return nil
	}
	backup := path + ".1"
	_ = os.Remove(backup)
	if err := os.Rename(path, backup); err != nil {
		return fmt.Errorf("rotate log file %s: %w", path, err)
	}
	if runtime.GOOS != "windows" {
		_ = os.Chmod(backup, 0o600)
	}
	return nil
}

func QueryStatus(ctx context.Context, dataDir string) (StatusInfo, error) {
	client := controlHTTPClient(dataDir)
	reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, "http://control.local/status", nil)
	if err != nil {
		return StatusInfo{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return StatusInfo{}, ErrNotRunning
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return StatusInfo{}, fmt.Errorf("control status returned HTTP %d", resp.StatusCode)
	}
	var info StatusInfo
	if err := json.NewDecoder(io.LimitReader(resp.Body, 16*1024)).Decode(&info); err != nil {
		return StatusInfo{}, fmt.Errorf("decode control status: %w", err)
	}
	return info, nil
}

func RequestStopAndWait(ctx context.Context, dataDir string, timeout time.Duration) error {
	client := controlHTTPClient(dataDir)
	reqCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, "http://control.local/stop", nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return ErrNotRunning
	}
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("control stop returned HTTP %d", resp.StatusCode)
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, dialErr := dialControl(ctx, dataDir)
		if dialErr != nil {
			return nil
		}
		_ = conn.Close()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(30 * time.Millisecond):
		}
	}
	return fmt.Errorf("timed out waiting for daemon control channel to close")
}

func controlHTTPClient(dataDir string) *http.Client {
	return &http.Client{
		Timeout: 2 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return dialControl(ctx, dataDir)
			},
		},
	}
}

type ControlServer struct {
	listener net.Listener
	server   *http.Server
	endpoint string
}

func StartControlServer(
	dataDir string,
	statusFn func() StatusInfo,
	stopFn func(),
) (*ControlServer, error) {
	ln, endpoint, err := listenControl(dataDir)
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(statusFn())
	})
	mux.HandleFunc("POST /stop", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		go stopFn()
	})

	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 2 * time.Second,
	}
	go func() {
		_ = srv.Serve(ln)
	}()

	return &ControlServer{
		listener: ln,
		server:   srv,
		endpoint: endpoint,
	}, nil
}

func (c *ControlServer) Close() error {
	if c == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := c.server.Shutdown(ctx)
	_ = c.listener.Close()
	cleanupControlEndpoint(c.endpoint)
	return err
}
