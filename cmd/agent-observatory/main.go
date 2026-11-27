package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/agent-observatory/agent-observatory-go/internal/app"
	"github.com/agent-observatory/agent-observatory-go/internal/config"
	"github.com/agent-observatory/agent-observatory-go/internal/launcher"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	return runWithDeps(args, stdout, stderr, launcher.OpenBrowser)
}

func runWithDeps(
	args []string,
	stdout, stderr io.Writer,
	openBrowser func(string) error,
) int {
	if len(args) > 0 {
		switch args[0] {
		case "version", "--version", "-V":
			_, _ = fmt.Fprintf(stdout, "%s %s\n", config.AppName, config.Version)
			return 0
		case "help", "--help", "-h":
			printUsage(stdout)
			return 0
		case "status":
			return runStatus(args[1:], stdout, stderr)
		case "stop":
			return runStop(args[1:], stdout, stderr)
		case "index":
			return runIndex(args[1:], stdout, stderr)
		case "up":
			args = args[1:]
		}
	}

	cfg, err := config.LoadFromEnv()
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "config error: %v\n", err)
		return 1
	}

	fs := flag.NewFlagSet(config.AppName, flag.ContinueOnError)
	fs.SetOutput(stderr)
	listen := fs.String("listen", cfg.ListenAddr, "HTTP listen address")
	dataDir := fs.String("data-dir", cfg.DataDir, "Private data directory")
	allowHosts := fs.String("allow-host", strings.Join(cfg.AllowedHosts, ","), "Comma-separated extra allowed Host names")
	foreground := fs.Bool("foreground", false, "Run in the foreground and log to stdout/stderr")
	noOpen := fs.Bool("no-open", false, "Do not open browser automatically")
	daemonChild := fs.Bool("daemon-child", false, "Internal flag for detached daemon startup")

	if err := fs.Parse(args); err != nil {
		return 2
	}
	cfg.ListenAddr = *listen
	cfg.DataDir = *dataDir
	cfg.Foreground = *foreground
	cfg.NoOpen = *noOpen
	if *allowHosts != "" {
		cfg.AllowedHosts = config.SplitCSV(*allowHosts)
	}

	if cfg.Foreground || *daemonChild {
		return app.RunServer(context.Background(), cfg, app.RunOptions{
			Stdout: stdout,
			Stderr: stderr,
		})
	}

	return runDetachedLauncher(context.Background(), cfg, stdout, stderr, openBrowser)
}

func runDetachedLauncher(
	ctx context.Context,
	cfg config.Config,
	stdout, stderr io.Writer,
	openBrowser func(string) error,
) int {
	if err := config.EnsureDirs(cfg.DataDir); err != nil {
		_, _ = fmt.Fprintf(stderr, "init data dir: %v\n", err)
		return 1
	}

	if status, err := launcher.QueryStatus(ctx, cfg.DataDir); err == nil {
		maybeOpenBrowser(status.URL, cfg.NoOpen, stderr, openBrowser)
		_, _ = fmt.Fprintf(stdout, "%s is already running (pid=%d, url=%s)\n", config.AppName, status.PID, status.URL)
		return 0
	}

	exePath, err := os.Executable()
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "resolve executable path: %v\n", err)
		return 1
	}

	childArgs := []string{
		"--daemon-child",
		"--data-dir", cfg.DataDir,
		"--listen", cfg.ListenAddr,
	}
	if len(cfg.AllowedHosts) > 0 {
		childArgs = append(childArgs, "--allow-host", strings.Join(cfg.AllowedHosts, ","))
	}

	payload, err := launcher.SpawnDetachedAndAwait(ctx, exePath, childArgs, cfg.DataDir)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "%v\n", err)
		return 1
	}

	targetURL := payload.URL
	if targetURL == "" && payload.ListenAddr != "" {
		targetURL = "http://" + payload.ListenAddr
	}
	maybeOpenBrowser(payload.URL, cfg.NoOpen, stderr, openBrowser)
	if payload.Reused {
		_, _ = fmt.Fprintf(stdout, "reusing running %s at %s\n", config.AppName, targetURL)
	} else {
		_, _ = fmt.Fprintf(stdout, "started %s (pid=%d, url=%s)\n", config.AppName, payload.PID, targetURL)
	}
	return 0
}

func maybeOpenBrowser(rawURL string, noOpen bool, stderr io.Writer, openBrowser func(string) error) {
	if noOpen || rawURL == "" || openBrowser == nil {
		return
	}
	if err := launcher.ValidateLoopbackBrowserURL(rawURL); err != nil {
		return
	}
	if err := openBrowser(rawURL); err != nil {
		_, _ = fmt.Fprintf(stderr, "warning: could not open browser for %s: %v\n", rawURL, err)
	}
}

func runStatus(args []string, stdout, stderr io.Writer) int {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "config error: %v\n", err)
		return 1
	}
	fs := flag.NewFlagSet(config.AppName+" status", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dataDir := fs.String("data-dir", cfg.DataDir, "Private data directory")
	asJSON := fs.Bool("json", false, "Print status as JSON")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	info, err := launcher.QueryStatus(context.Background(), *dataDir)
	if err != nil {
		if *asJSON {
			_ = json.NewEncoder(stdout).Encode(map[string]string{
				"status":   "stopped",
				"data_dir": *dataDir,
			})
		} else {
			_, _ = fmt.Fprintf(stderr, "%s is not running (data-dir=%s)\n", config.AppName, *dataDir)
		}
		return 1
	}

	if *asJSON {
		_ = json.NewEncoder(stdout).Encode(info)
		return 0
	}
	_, _ = fmt.Fprintf(
		stdout,
		"status: %s\npid: %d\nlisten_addr: %s\nurl: %s\nlog_path: %s\n",
		info.Status, info.PID, info.ListenAddr, info.URL, info.LogPath,
	)
	return 0
}

func runStop(args []string, stdout, stderr io.Writer) int {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "config error: %v\n", err)
		return 1
	}
	fs := flag.NewFlagSet(config.AppName+" stop", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dataDir := fs.String("data-dir", cfg.DataDir, "Private data directory")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	err = launcher.RequestStopAndWait(context.Background(), *dataDir, launcher.StopWaitTimeout)
	if err != nil {
		if errors.Is(err, launcher.ErrNotRunning) {
			_, _ = fmt.Fprintf(stdout, "%s is not running\n", config.AppName)
			return 0
		}
		_, _ = fmt.Fprintf(stderr, "stop failed: %v\n", err)
		return 1
	}
	_, _ = fmt.Fprintf(stdout, "%s stopped\n", config.AppName)
	return 0
}

func runIndex(args []string, stdout, stderr io.Writer) int {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "config error: %v\n", err)
		return 1
	}
	fs := flag.NewFlagSet(config.AppName+" index", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dataDir := fs.String("data-dir", cfg.DataDir, "Private data directory")
	once := fs.Bool("once", true, "Run a single full indexing cycle and exit")
	asJSON := fs.Bool("json", false, "Print cycle stats as JSON")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if !*once {
		_, _ = fmt.Fprintln(stderr, "only --once is supported for the index subcommand; omit index or use --foreground for continuous indexing")
		return 2
	}
	cfg.DataDir = *dataDir
	return app.RunIndexOnce(context.Background(), cfg, *asJSON, stdout, stderr)
}

func printUsage(w io.Writer) {
	_, _ = fmt.Fprintf(w, `Usage: %s [command] [flags] (short alias: aobs)

Commands:
  up (default)   Start background daemon if needed and open Web UI
  status         Show running status, PID, listen address, URL, and log path
  stop           Stop background daemon gracefully and wait for exit
  index --once   Run a single full discovery + indexing cycle and exit
  version        Print version and exit

Flags:
  --foreground          Run in foreground (logs to stdout/stderr)
  --listen string       HTTP listen address (default %s)
  --data-dir string     Private data directory
  --allow-host string   Comma-separated extra allowed Host headers
  --no-open             Do not open browser automatically
`, config.AppName, config.DefaultListenAddr)
}
