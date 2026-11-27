//go:build windows

package launcher

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/Microsoft/go-winio"
	"github.com/agent-observatory/agent-observatory-go/internal/config"
	"golang.org/x/sys/windows"
)

func isAddrInUse(err error) bool {
	return errors.Is(err, windows.WSAEADDRINUSE) || errors.Is(err, syscall.Errno(10048))
}

func controlEndpoint(dataDir string) string {
	return `\\.\pipe\` + config.AppName + "-" + config.DataDirHash(dataDir)
}

func currentUserSDDL() string {
	token := windows.GetCurrentProcessToken()
	user, err := token.GetTokenUser()
	if err != nil || user == nil || user.User.Sid == nil {
		return ""
	}
	sid := user.User.Sid.String()
	return fmt.Sprintf("O:%sD:P(A;;GA;;;%s)", sid, sid)
}

func listenControl(dataDir string) (net.Listener, string, error) {
	pipePath := controlEndpoint(dataDir)
	timeout := 300 * time.Millisecond
	if conn, err := winio.DialPipe(pipePath, &timeout); err == nil {
		_ = conn.Close()
		return nil, "", ErrAlreadyRunning
	}

	pipeCfg := &winio.PipeConfig{
		SecurityDescriptor: currentUserSDDL(),
	}
	ln, err := winio.ListenPipe(pipePath, pipeCfg)
	if err != nil {
		return nil, "", fmt.Errorf("listen named pipe %s: %w", pipePath, err)
	}
	return ln, pipePath, nil
}

func dialControl(ctx context.Context, dataDir string) (net.Conn, error) {
	pipePath := controlEndpoint(dataDir)
	dialCtx, cancel := context.WithTimeout(ctx, 800*time.Millisecond)
	defer cancel()
	return winio.DialPipeContext(dialCtx, pipePath)
}

func cleanupControlEndpoint(_ string) {}

type DaemonLock struct {
	file *os.File
}

func AcquireDaemonLock(dataDir string) (*DaemonLock, error) {
	lockPath := filepath.Join(dataDir, "run", "daemon.lock")
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open daemon lock: %w", err)
	}
	var overlapped windows.Overlapped
	err = windows.LockFileEx(
		windows.Handle(f.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0,
		1,
		0,
		&overlapped,
	)
	if err != nil {
		_ = f.Close()
		return nil, ErrAlreadyRunning
	}
	return &DaemonLock{file: f}, nil
}

func (l *DaemonLock) Release() {
	if l == nil || l.file == nil {
		return
	}
	var overlapped windows.Overlapped
	_ = windows.UnlockFileEx(windows.Handle(l.file.Fd()), 0, 1, 0, &overlapped)
	_ = l.file.Close()
	l.file = nil
}

func ReadAndClearChildEnv() (bool, func(HandshakePayload) error) {
	isChild := os.Getenv(EnvDaemonChild) == "1"
	readyPath := os.Getenv(EnvReadyFile)
	// Clear internal handshake variables immediately so child servers or tools never inherit them.
	_ = os.Unsetenv(EnvDaemonChild)
	_ = os.Unsetenv(EnvHandshakeFD)
	_ = os.Unsetenv(EnvReadyFile)

	if !isChild {
		return false, nil
	}
	reported := false
	return true, func(payload HandshakePayload) error {
		if reported || readyPath == "" {
			return nil
		}
		reported = true
		data, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		tmp := readyPath + ".tmp"
		if err := os.WriteFile(tmp, append(data, '\n'), 0o600); err != nil {
			return err
		}
		return os.Rename(tmp, readyPath)
	}
}

func SpawnDetachedAndAwait(ctx context.Context, exePath string, childArgs []string, dataDir string) (HandshakePayload, error) {
	logFile, logPath, err := OpenLogFile(dataDir)
	if err != nil {
		return HandshakePayload{}, err
	}
	defer func() { _ = logFile.Close() }()

	runDir := filepath.Join(dataDir, "run")
	if err := os.MkdirAll(runDir, 0o700); err != nil {
		return HandshakePayload{}, err
	}
	var token [8]byte
	_, _ = rand.Read(token[:])
	readyPath := filepath.Join(runDir, "ready-"+hex.EncodeToString(token[:])+".json")
	_ = os.Remove(readyPath)
	defer func() { _ = os.Remove(readyPath) }()

	cmd := exec.Command(exePath, childArgs...)
	cmd.Stdin = nil
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.Env = append(filteredEnv(),
		EnvDaemonChild+"=1",
		EnvReadyFile+"="+readyPath,
	)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: windows.DETACHED_PROCESS | windows.CREATE_NEW_PROCESS_GROUP,
	}

	if err := cmd.Start(); err != nil {
		return HandshakePayload{}, fmt.Errorf("spawn daemon child: %w", err)
	}

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- cmd.Wait()
	}()

	deadline := time.NewTimer(ReadinessTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(30 * time.Millisecond)
	defer ticker.Stop()

	for {
		if payload, ok, readErr := tryReadReadyFile(readyPath); ok {
			if readErr != nil {
				return HandshakePayload{}, readErr
			}
			if !payload.OK {
				return HandshakePayload{}, errors.New(payload.Error)
			}
			return payload, nil
		}

		select {
		case waitErr := <-waitCh:
			if payload, ok, _ := tryReadReadyFile(readyPath); ok {
				if payload.OK {
					return payload, nil
				}
				return HandshakePayload{}, errors.New(payload.Error)
			}
			return HandshakePayload{}, fmt.Errorf("daemon exited during startup (%v; see %s)", waitErr, logPath)
		case <-deadline.C:
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
			return HandshakePayload{}, fmt.Errorf("timed out waiting %s for daemon readiness (see %s)", ReadinessTimeout, logPath)
		case <-ctx.Done():
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
			return HandshakePayload{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func tryReadReadyFile(path string) (HandshakePayload, bool, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return HandshakePayload{}, false, nil
	}
	var payload HandshakePayload
	if unmarshalErr := json.Unmarshal(raw, &payload); unmarshalErr != nil {
		return HandshakePayload{}, true, fmt.Errorf("invalid readiness payload: %w", unmarshalErr)
	}
	return payload, true, nil
}

func filteredEnv() []string {
	raw := os.Environ()
	out := make([]string, 0, len(raw))
	for _, entry := range raw {
		if strings.HasPrefix(entry, EnvDaemonChild+"=") ||
			strings.HasPrefix(entry, EnvHandshakeFD+"=") ||
			strings.HasPrefix(entry, EnvReadyFile+"=") {
			continue
		}
		out = append(out, entry)
	}
	return out
}

func openBrowserPlatform(rawURL string) error {
	return exec.Command("rundll32", "url.dll,FileProtocolHandler", rawURL).Start()
}
