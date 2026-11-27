package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/agent-observatory/agent-observatory-go/internal/config"
	"github.com/agent-observatory/agent-observatory-go/internal/launcher"
	"github.com/agent-observatory/agent-observatory-go/internal/model"
)

func buildTestBinary(t *testing.T) string {
	t.Helper()
	binPath := filepath.Join(t.TempDir(), "agent-observatory")
	if runtime.GOOS == "windows" {
		binPath += ".exe"
	}
	build := exec.Command("go", "build", "-o", binPath, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build failed: %v\n%s", err, out)
	}
	return binPath
}

func reserveFreePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve free port: %v", err)
	}
	defer func() { _ = ln.Close() }()
	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	port, _ := strconv.Atoi(portStr)
	return port
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestVersionCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"--version"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run(--version) = %d, stderr=%q", code, stderr.String())
	}
	if !strings.HasPrefix(stdout.String(), "agent-observatory ") {
		t.Fatalf("unexpected version output: %q", stdout.String())
	}
}

func TestForegroundSubprocessServesHealthAndShutsDown(t *testing.T) {
	binPath := buildTestBinary(t)
	dataDir := filepath.Join(t.TempDir(), "data")
	cmd := exec.Command(binPath, "--foreground", "--listen", "127.0.0.1:0", "--data-dir", dataDir)
	cmd.Env = append(os.Environ(), "HOME="+t.TempDir())
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe: %v", err)
	}
	var stderr lockedBuffer
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	})

	lineCh := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		if scanner.Scan() {
			lineCh <- scanner.Text()
		}
		close(lineCh)
	}()

	var baseURL string
	select {
	case line := <-lineCh:
		const prefix = "listening on "
		if !strings.HasPrefix(line, prefix) {
			t.Fatalf("unexpected startup line %q (stderr=%q)", line, stderr.String())
		}
		baseURL = strings.TrimSpace(strings.TrimPrefix(line, prefix))
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for foreground server startup (stderr=%q)", stderr.String())
	}

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(baseURL + "/api/health/ready")
	if err != nil {
		t.Fatalf("GET /api/health/ready failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ready status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Agent-Observatory-Service"); got != "agent-observatory" {
		t.Fatalf("service header = %q, want agent-observatory", got)
	}

	if err := cmd.Process.Signal(os.Signal(syscall.SIGTERM)); err != nil {
		t.Fatalf("signal SIGTERM: %v", err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("foreground server wait after SIGTERM: %v (stderr=%q)", err, stderr.String())
	}
}

func TestDaemonLifecycleReleaseParentStatusReuseAndStop(t *testing.T) {
	binPath := buildTestBinary(t)
	dataDir := filepath.Join(t.TempDir(), "data")
	port := reserveFreePort(t)
	listenAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))

	// First invocation spawns detached child, waits for readiness, and returns quickly.
	isolatedHome := t.TempDir()
	startCmd := exec.Command(binPath, "--data-dir", dataDir, "--listen", listenAddr, "--no-open")
	startCmd.Env = append(os.Environ(), "HOME="+isolatedHome)
	startOut, err := startCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("daemon start failed: %v\n%s", err, startOut)
	}
	if !strings.Contains(string(startOut), "http://") {
		t.Fatalf("expected URL in start output, got %q", string(startOut))
	}
	t.Cleanup(func() {
		_ = exec.Command(binPath, "stop", "--data-dir", dataDir).Run()
	})

	statusCmd := exec.Command(binPath, "status", "--json", "--data-dir", dataDir)
	statusOut, err := statusCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("status failed: %v\n%s", err, statusOut)
	}
	var status launcher.StatusInfo
	if err := json.Unmarshal(statusOut, &status); err != nil {
		t.Fatalf("decode status json: %v\n%s", err, statusOut)
	}
	if status.Status != "running" || status.PID <= 0 || status.URL == "" {
		t.Fatalf("unexpected daemon status: %+v", status)
	}

	resp, err := (&http.Client{Timeout: 3 * time.Second}).Get(status.URL + "/api/meta")
	if err != nil {
		t.Fatalf("GET %s/api/meta: %v", status.URL, err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("meta status = %d, want 200", resp.StatusCode)
	}

	// Second invocation reuses the running instance via control channel without changing PID.
	secondCmd := exec.Command(binPath, "--data-dir", dataDir, "--listen", listenAddr, "--no-open")
	secondOut, err := secondCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("second launch failed: %v\n%s", err, secondOut)
	}
	if !strings.Contains(string(secondOut), "already running") {
		t.Fatalf("expected reuse notice on second launch, got %q", string(secondOut))
	}

	status2Out, err := exec.Command(binPath, "status", "--json", "--data-dir", dataDir).CombinedOutput()
	if err != nil {
		t.Fatalf("second status failed: %v", err)
	}
	var status2 launcher.StatusInfo
	_ = json.Unmarshal(status2Out, &status2)
	if status2.PID != status.PID {
		t.Fatalf("PID changed across second launch: %d -> %d", status.PID, status2.PID)
	}

	stopOut, err := exec.Command(binPath, "stop", "--data-dir", dataDir).CombinedOutput()
	if err != nil {
		t.Fatalf("stop failed: %v\n%s", err, stopOut)
	}

	// After stop returns, status must fail non-zero and second stop must stay idempotent (exit 0).
	if err := exec.Command(binPath, "status", "--data-dir", dataDir).Run(); err == nil {
		t.Fatal("expected status after stop to exit non-zero")
	}
	if out, err := exec.Command(binPath, "stop", "--data-dir", dataDir).CombinedOutput(); err != nil {
		t.Fatalf("idempotent stop failed: %v\n%s", err, out)
	}
}

func TestDaemonPortFallbackAndSameAppReuseSubprocess(t *testing.T) {
	binPath := buildTestBinary(t)

	// Case 1: non-observatory service occupies port -> daemon increments port +1.
	thirdPartyLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen third-party: %v", err)
	}
	thirdPartySrv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}),
	}
	go func() { _ = thirdPartySrv.Serve(thirdPartyLn) }()
	defer func() {
		_ = thirdPartySrv.Close()
		_ = thirdPartyLn.Close()
	}()

	dataDir1 := filepath.Join(t.TempDir(), "fallback")
	cmd1 := exec.Command(binPath, "--data-dir", dataDir1, "--listen", thirdPartyLn.Addr().String(), "--no-open")
	cmd1.Env = append(os.Environ(), "HOME="+t.TempDir())
	out1, err := cmd1.CombinedOutput()
	if err != nil {
		t.Fatalf("fallback daemon launch failed: %v\n%s", err, out1)
	}
	t.Cleanup(func() {
		_ = exec.Command(binPath, "stop", "--data-dir", dataDir1).Run()
	})
	status1Raw, err := exec.Command(binPath, "status", "--json", "--data-dir", dataDir1).CombinedOutput()
	if err != nil {
		t.Fatalf("status fallback daemon failed: %v\n%s", err, status1Raw)
	}
	var status1 launcher.StatusInfo
	_ = json.Unmarshal(status1Raw, &status1)
	if status1.ListenAddr == thirdPartyLn.Addr().String() {
		t.Fatalf("expected fallback listen addr != %s", thirdPartyLn.Addr().String())
	}

	// Case 2: our app already occupies port (even with a different data-dir) -> do not switch port, reuse and exit 0.
	sameAppLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen mock same-app: %v", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/meta", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Agent-Observatory-Service", config.AppName)
		_ = json.NewEncoder(w).Encode(model.MetaResponse{
			App:         config.AppName,
			Version:     config.Version,
			DataDirHash: "fedcba9876543210",
		})
	})
	sameAppSrv := &http.Server{Handler: mux}
	go func() { _ = sameAppSrv.Serve(sameAppLn) }()
	defer func() {
		_ = sameAppSrv.Close()
		_ = sameAppLn.Close()
	}()

	dataDir2 := filepath.Join(t.TempDir(), "reuse")
	out2, err := exec.Command(binPath, "--data-dir", dataDir2, "--listen", sameAppLn.Addr().String(), "--no-open").CombinedOutput()
	if err != nil {
		t.Fatalf("same-app reuse launch failed: %v\n%s", err, out2)
	}
	expectedURL := "http://" + sameAppLn.Addr().String()
	if !strings.Contains(string(out2), expectedURL) {
		t.Fatalf("expected reused URL %q in output %q", expectedURL, string(out2))
	}
	if err := exec.Command(binPath, "status", "--data-dir", dataDir2).Run(); err == nil {
		t.Fatal("expected second data-dir not to spawn a duplicate daemon")
	}
}

func TestIndexOnceSubcommandWithExtraRoot(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	extraRoot := filepath.Join(t.TempDir(), "extra-custom")
	if err := os.MkdirAll(extraRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(extraRoot, "session.jsonl"), []byte(`{"timestamp":"2026-11-26T12:00:00Z","role":"user","text":"hello index once"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	t.Setenv("HOME", t.TempDir())
	t.Setenv("AGENT_OBS_EXTRA_ROOTS", "custom:"+extraRoot)
	var stdout, stderr bytes.Buffer
	code := run([]string{"index", "--once", "--json", "--data-dir", dataDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run(index --once) = %d, stderr=%q", code, stderr.String())
	}
	var stats model.CycleStats
	if err := json.Unmarshal(stdout.Bytes(), &stats); err != nil {
		t.Fatalf("decode index --once json: %v\n%s", err, stdout.String())
	}
	if !stats.Finished || stats.Indexed < 1 {
		t.Fatalf("expected >= 1 indexed session from extra root, got %+v", stats)
	}
}

func TestShortAliasSymlinkLaunchStatusAndStop(t *testing.T) {
	binPath := buildTestBinary(t)
	aliasDir := t.TempDir()
	aliasName := "aobs"
	if runtime.GOOS == "windows" {
		aliasName = "aobs.exe"
	}
	aliasPath := filepath.Join(aliasDir, aliasName)
	if runtime.GOOS == "windows" {
		if err := os.Link(binPath, aliasPath); err != nil {
			t.Fatalf("link alias binary: %v", err)
		}
	} else {
		if err := os.Symlink(binPath, aliasPath); err != nil {
			t.Fatalf("symlink alias binary: %v", err)
		}
	}

	verOut, err := exec.Command(aliasPath, "--version").CombinedOutput()
	if err != nil {
		t.Fatalf("aobs --version failed: %v\n%s", err, verOut)
	}
	if !strings.HasPrefix(string(verOut), "agent-observatory ") {
		t.Fatalf("unexpected aobs --version output: %q", string(verOut))
	}

	helpOut, err := exec.Command(aliasPath, "--help").CombinedOutput()
	if err != nil {
		t.Fatalf("aobs --help failed: %v\n%s", err, helpOut)
	}
	if !strings.Contains(string(helpOut), "short alias: aobs") {
		t.Fatalf("expected help output to document short alias aobs, got %q", string(helpOut))
	}

	dataDir := filepath.Join(t.TempDir(), "data")
	homeDir := t.TempDir()
	t.Cleanup(func() {
		cmd := exec.Command(aliasPath, "stop", "--data-dir", dataDir)
		cmd.Env = append(os.Environ(), "HOME="+homeDir)
		_ = cmd.Run()
	})

	startCmd := exec.Command(aliasPath, "--data-dir", dataDir, "--listen", "127.0.0.1:0", "--no-open")
	startCmd.Env = append(os.Environ(), "HOME="+homeDir)
	startOut, err := startCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("aobs start failed: %v\n%s", err, startOut)
	}

	statusCmd := exec.Command(aliasPath, "status", "--json", "--data-dir", dataDir)
	statusCmd.Env = append(os.Environ(), "HOME="+homeDir)
	statusOut, err := statusCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("aobs status failed: %v\n%s", err, statusOut)
	}
	var info launcher.StatusInfo
	if err := json.Unmarshal(statusOut, &info); err != nil {
		t.Fatalf("decode status json: %v\n%s", err, statusOut)
	}
	if info.Status != "running" || info.URL == "" {
		t.Fatalf("unexpected status info via aobs: %+v", info)
	}

	stopCmd := exec.Command(aliasPath, "stop", "--data-dir", dataDir)
	stopCmd.Env = append(os.Environ(), "HOME="+homeDir)
	if out, err := stopCmd.CombinedOutput(); err != nil {
		t.Fatalf("aobs stop failed: %v\n%s", err, out)
	}
}
