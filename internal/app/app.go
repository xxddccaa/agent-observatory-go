package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/agent-observatory/agent-observatory-go/internal/api"
	"github.com/agent-observatory/agent-observatory-go/internal/config"
	"github.com/agent-observatory/agent-observatory-go/internal/indexer"
	"github.com/agent-observatory/agent-observatory-go/internal/launcher"
	"github.com/agent-observatory/agent-observatory-go/internal/model"
	"github.com/agent-observatory/agent-observatory-go/internal/storage"
	"github.com/agent-observatory/agent-observatory-go/internal/webui"
)

type RunOptions struct {
	Stdout io.Writer
	Stderr io.Writer
}

func RunServer(ctx context.Context, cfg config.Config, opts RunOptions) int {
	if opts.Stdout == nil {
		opts.Stdout = io.Discard
	}
	if opts.Stderr == nil {
		opts.Stderr = io.Discard
	}

	isChild, reportHandshake := launcher.ReadAndClearChildEnv()
	fail := func(msg string) int {
		if isChild && reportHandshake != nil {
			_ = reportHandshake(launcher.HandshakePayload{
				OK:    false,
				Error: msg,
			})
		}
		_, _ = fmt.Fprintln(opts.Stderr, msg)
		return 1
	}

	if err := config.EnsureDirs(cfg.DataDir); err != nil {
		return fail(fmt.Sprintf("init data dir: %v", err))
	}

	lock, err := launcher.AcquireDaemonLock(cfg.DataDir)
	if err != nil {
		if errors.Is(err, launcher.ErrAlreadyRunning) {
			if status, ok := waitForRunningStatus(ctx, cfg.DataDir, 2*time.Second); ok && isChild {
				_ = reportHandshake(launcher.HandshakePayload{
					OK:         true,
					Reused:     true,
					URL:        status.URL,
					ListenAddr: status.ListenAddr,
					PID:        status.PID,
				})
				return 0
			}
			return fail(fmt.Sprintf("another %s instance already owns %s", config.AppName, cfg.DataDir))
		}
		return fail(fmt.Sprintf("acquire daemon lock: %v", err))
	}
	defer lock.Release()

	var controlSrv *launcher.ControlServer
	// Close the control channel after SQLite closes so `stop` waits for WAL checkpointing.
	defer func() {
		if controlSrv != nil {
			_ = controlSrv.Close()
		}
	}()

	ln, err := launcher.ListenWithAppAwareFallback(ctx, cfg.ListenAddr)
	if err != nil {
		var sameApp *launcher.SameAppRunningError
		if errors.As(err, &sameApp) && isChild {
			_ = reportHandshake(launcher.HandshakePayload{
				OK:     true,
				Reused: true,
				URL:    sameApp.URL,
			})
			return 0
		}
		return fail(err.Error())
	}

	actualAddr := ln.Addr().String()
	browserURL := launcher.BrowserURLForListenAddr(actualAddr)
	displayURL := browserURL
	if displayURL == "" {
		displayURL = "http://" + actualAddr
	}

	runCtx, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()

	store, err := storage.Open(runCtx, cfg.DataDir)
	if err != nil {
		_ = ln.Close()
		return fail(fmt.Sprintf("open sqlite: %v", err))
	}
	defer func() { _ = store.Close() }()

	webFS, err := webui.FS()
	if err != nil {
		_ = ln.Close()
		return fail(fmt.Sprintf("load embedded webui: %v", err))
	}

	startedAt := model.FormatUTC(time.Now())
	logPath := launcher.LogPath(cfg.DataDir)
	pid := os.Getpid()

	var stopOnce sync.Once
	triggerStop := func() {
		stopOnce.Do(cancel)
	}

	controlSrv, err = launcher.StartControlServer(
		cfg.DataDir,
		func() launcher.StatusInfo {
			return launcher.StatusInfo{
				Status:     "running",
				PID:        pid,
				ListenAddr: actualAddr,
				URL:        displayURL,
				DataDir:    cfg.DataDir,
				LogPath:    logPath,
				StartedAt:  startedAt,
				Version:    config.Version,
			}
		},
		triggerStop,
	)
	if err != nil {
		_ = ln.Close()
		return fail(fmt.Sprintf("start control channel: %v", err))
	}

	idx := indexer.New(store, indexer.Options{
		ExtraRoots:   cfg.ExtraRoots,
		PollInterval: time.Duration(config.DefaultPollMillis) * time.Millisecond,
	})
	var idxWg sync.WaitGroup
	idxWg.Add(1)
	go func() {
		defer idxWg.Done()
		idx.RunLoop(runCtx)
	}()

	srv := api.NewServer(cfg, store, idx, webFS)
	httpSrv := &http.Server{
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- httpSrv.Serve(ln)
	}()

	if isChild && reportHandshake != nil {
		if err := reportHandshake(launcher.HandshakePayload{
			OK:         true,
			URL:        browserURL,
			ListenAddr: actualAddr,
			PID:        pid,
		}); err != nil {
			_, _ = fmt.Fprintf(opts.Stderr, "write readiness handshake: %v\n", err)
		}
	} else {
		_, _ = fmt.Fprintf(opts.Stdout, "listening on %s\n", displayURL)
	}

	select {
	case <-runCtx.Done():
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = httpSrv.Shutdown(shutdownCtx)
		idxWg.Wait()
		return 0
	case serveErr := <-errCh:
		cancel()
		idxWg.Wait()
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			_, _ = fmt.Fprintf(opts.Stderr, "http server error: %v\n", serveErr)
			return 1
		}
		return 0
	}
}

// RunIndexOnce executes a single synchronous full indexing scan and writes results to SQLite.
func RunIndexOnce(ctx context.Context, cfg config.Config, asJSON bool, stdout, stderr io.Writer) int {
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	if err := config.EnsureDirs(cfg.DataDir); err != nil {
		_, _ = fmt.Fprintf(stderr, "init data dir: %v\n", err)
		return 1
	}
	store, err := storage.Open(ctx, cfg.DataDir)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "open sqlite: %v\n", err)
		return 1
	}
	defer func() { _ = store.Close() }()

	idx := indexer.New(store, indexer.Options{
		ExtraRoots: cfg.ExtraRoots,
	})
	stats, err := idx.RunOnce(ctx, true)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "index failed: %v\n", err)
		return 1
	}
	if asJSON {
		_ = json.NewEncoder(stdout).Encode(stats)
	} else {
		_, _ = fmt.Fprintf(stdout, "discovered=%d indexed=%d unchanged=%d failures=%d cycle_ms=%.1f\n",
			stats.Discovered, stats.Indexed, stats.Unchanged, stats.Failures, stats.CycleMS)
	}
	return 0
}

func waitForRunningStatus(ctx context.Context, dataDir string, timeout time.Duration) (launcher.StatusInfo, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		status, err := launcher.QueryStatus(ctx, dataDir)
		if err == nil {
			return status, true
		}
		select {
		case <-ctx.Done():
			return launcher.StatusInfo{}, false
		case <-time.After(50 * time.Millisecond):
		}
	}
	return launcher.StatusInfo{}, false
}
