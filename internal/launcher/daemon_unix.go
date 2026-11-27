//go:build !windows

package launcher

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/agent-observatory/agent-observatory-go/internal/config"
)

func isAddrInUse(err error) bool {
	return errors.Is(err, syscall.EADDRINUSE)
}

func controlEndpoint(dataDir string) string {
	clean := filepath.Clean(dataDir)
	if abs, err := filepath.Abs(clean); err == nil {
		clean = abs
	}
	primary := filepath.Join(clean, "run", "control.sock")
	// Unix sun_path is capped at 104 bytes on macOS (108 on Linux); long --data-dir paths fall back to /tmp.
	if len(primary) < 100 {
		return primary
	}
	return filepath.Join("/tmp", fmt.Sprintf("agobs-%d", os.Getuid()), "c-"+config.DataDirHash(clean)+".sock")
}

func listenControl(dataDir string) (net.Listener, string, error) {
	sockPath := controlEndpoint(dataDir)
	sockDir := filepath.Dir(sockPath)
	if err := os.MkdirAll(sockDir, 0o700); err != nil {
		return nil, "", fmt.Errorf("create socket dir: %w", err)
	}
	if err := os.Chmod(sockDir, 0o700); err != nil {
		return nil, "", fmt.Errorf("chmod socket dir: %w", err)
	}

	if _, err := os.Stat(sockPath); err == nil {
		conn, dialErr := net.DialTimeout("unix", sockPath, 300*time.Millisecond)
		if dialErr == nil {
			_ = conn.Close()
			return nil, "", ErrAlreadyRunning
		}
		// Socket exists without an active listener (e.g. after SIGKILL); clean it before rebinding.
		_ = os.Remove(sockPath)
	}

	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		return nil, "", fmt.Errorf("listen control socket %s: %w", sockPath, err)
	}
	if err := os.Chmod(sockPath, 0o600); err != nil {
		_ = ln.Close()
		_ = os.Remove(sockPath)
		return nil, "", fmt.Errorf("chmod control socket %s: %w", sockPath, err)
	}
	return ln, sockPath, nil
}

func dialControl(ctx context.Context, dataDir string) (net.Conn, error) {
	sockPath := controlEndpoint(dataDir)
	d := net.Dialer{Timeout: 800 * time.Millisecond}
	return d.DialContext(ctx, "unix", sockPath)
}

func cleanupControlEndpoint(endpoint string) {
	if endpoint != "" {
		_ = os.Remove(endpoint)
	}
}

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
	_ = os.Chmod(lockPath, 0o600)
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, ErrAlreadyRunning
	}
	return &DaemonLock{file: f}, nil
}

func (l *DaemonLock) Release() {
	if l == nil || l.file == nil {
		return
	}
	_ = syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	_ = l.file.Close()
	l.file = nil
}

func ReadAndClearChildEnv() (bool, func(HandshakePayload) error) {
	isChild := os.Getenv(EnvDaemonChild) == "1"
	fdRaw := os.Getenv(EnvHandshakeFD)
	readyFile := os.Getenv(EnvReadyFile)
	// Clear internal handshake variables immediately so child servers or tools never inherit them.
	_ = os.Unsetenv(EnvDaemonChild)
	_ = os.Unsetenv(EnvHandshakeFD)
	_ = os.Unsetenv(EnvReadyFile)

	if !isChild {
		return false, nil
	}
	_ = readyFile
	fd, err := strconv.Atoi(strings.TrimSpace(fdRaw))
	if err != nil || fd < 3 {
		return true, func(HandshakePayload) error { return nil }
	}
	pipe := os.NewFile(uintptr(fd), "handshake-pipe")
	reported := false
	return true, func(payload HandshakePayload) error {
		if reported || pipe == nil {
			return nil
		}
		reported = true
		data, marshalErr := json.Marshal(payload)
		if marshalErr != nil {
			_ = pipe.Close()
			return marshalErr
		}
		_, writeErr := pipe.Write(append(data, '\n'))
		closeErr := pipe.Close()
		if writeErr != nil {
			return writeErr
		}
		return closeErr
	}
}

func SpawnDetachedAndAwait(ctx context.Context, exePath string, childArgs []string, dataDir string) (HandshakePayload, error) {
	logFile, logPath, err := OpenLogFile(dataDir)
	if err != nil {
		return HandshakePayload{}, err
	}
	defer func() { _ = logFile.Close() }()

	readPipe, writePipe, err := os.Pipe()
	if err != nil {
		return HandshakePayload{}, fmt.Errorf("create readiness pipe: %w", err)
	}
	defer func() { _ = readPipe.Close() }()

	cmd := exec.Command(exePath, childArgs...)
	cmd.Stdin = nil
	// Redirect directly to the log file so no stdout/stderr pipe stays open after parent exits.
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.ExtraFiles = []*os.File{writePipe}
	cmd.Env = append(filteredEnv(),
		EnvDaemonChild+"=1",
		EnvHandshakeFD+"=3",
	)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	if err := cmd.Start(); err != nil {
		_ = writePipe.Close()
		return HandshakePayload{}, fmt.Errorf("spawn daemon child: %w", err)
	}
	// Close parent's copy of writePipe immediately so EOF is observed if child crashes early.
	_ = writePipe.Close()

	type readResult struct {
		payload HandshakePayload
		err     error
	}
	resCh := make(chan readResult, 1)
	go func() {
		scanner := bufio.NewScanner(readPipe)
		if !scanner.Scan() {
			if scanErr := scanner.Err(); scanErr != nil {
				resCh <- readResult{err: scanErr}
				return
			}
			resCh <- readResult{err: errors.New("daemon exited before reporting readiness")}
			return
		}
		var payload HandshakePayload
		if unmarshalErr := json.Unmarshal(scanner.Bytes(), &payload); unmarshalErr != nil {
			resCh <- readResult{err: fmt.Errorf("invalid readiness payload: %w", unmarshalErr)}
			return
		}
		resCh <- readResult{payload: payload}
	}()

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- cmd.Wait()
	}()

	timer := time.NewTimer(ReadinessTimeout)
	defer timer.Stop()

	select {
	case res := <-resCh:
		if res.err != nil {
			return HandshakePayload{}, fmt.Errorf("%w (see %s)", res.err, logPath)
		}
		if !res.payload.OK {
			msg := res.payload.Error
			if msg == "" {
				msg = "daemon reported startup failure"
			}
			return HandshakePayload{}, errors.New(msg)
		}
		return res.payload, nil
	case waitErr := <-waitCh:
		// Drain resCh briefly in case child wrote readiness right before exiting (e.g. port reuse).
		select {
		case res := <-resCh:
			if res.err == nil && res.payload.OK {
				return res.payload, nil
			}
			if res.err == nil && res.payload.Error != "" {
				return HandshakePayload{}, errors.New(res.payload.Error)
			}
		case <-time.After(150 * time.Millisecond):
		}
		return HandshakePayload{}, fmt.Errorf("daemon exited during startup (%v; see %s)", waitErr, logPath)
	case <-timer.C:
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		return HandshakePayload{}, fmt.Errorf("timed out waiting %s for daemon readiness (see %s)", ReadinessTimeout, logPath)
	case <-ctx.Done():
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		return HandshakePayload{}, ctx.Err()
	}
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
	var cmd *exec.Cmd
	if runtime.GOOS == "darwin" {
		cmd = exec.Command("open", rawURL)
	} else {
		cmd = exec.Command("xdg-open", rawURL)
	}
	return cmd.Start()
}
