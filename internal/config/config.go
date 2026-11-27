package config

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// Version is overridden at release build time via -ldflags.
var Version = "0.1.0-dev"

const (
	AppName           = "agent-observatory"
	DefaultListenAddr = "127.0.0.1:8125"
	DefaultPollMillis = 2000
)

type Config struct {
	DataDir      string
	ListenAddr   string
	AllowedHosts []string
	ExtraRoots   map[string][]string
	Environment  string
	Foreground   bool
	NoOpen       bool
}

func DefaultDataDir() (string, error) {
	if runtime.GOOS == "windows" {
		if base := strings.TrimSpace(os.Getenv("LOCALAPPDATA")); base != "" {
			return filepath.Join(base, "agent-observatory-go"), nil
		}
		dir, err := os.UserConfigDir()
		if err != nil {
			return "", fmt.Errorf("resolve user config dir: %w", err)
		}
		return filepath.Join(dir, "agent-observatory-go"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve user home dir: %w", err)
	}
	return filepath.Join(home, ".agent-observatory-go"), nil
}

func LoadFromEnv() (Config, error) {
	dataDir := strings.TrimSpace(os.Getenv("AGENT_OBS_HOME"))
	if dataDir == "" {
		var err error
		dataDir, err = DefaultDataDir()
		if err != nil {
			return Config{}, err
		}
	}
	absDir, err := filepath.Abs(dataDir)
	if err == nil {
		dataDir = absDir
	}

	listenAddr := strings.TrimSpace(os.Getenv("AGENT_OBS_LISTEN"))
	if listenAddr == "" {
		host := strings.TrimSpace(os.Getenv("AGENT_OBS_HOST"))
		port := strings.TrimSpace(os.Getenv("AGENT_OBS_PORT"))
		if host != "" || port != "" {
			if host == "" {
				host = "127.0.0.1"
			}
			if port == "" {
				port = "8125"
			}
			listenAddr = net.JoinHostPort(host, port)
		} else {
			listenAddr = DefaultListenAddr
		}
	}

	env := strings.TrimSpace(os.Getenv("AGENT_OBS_ENVIRONMENT"))
	if env == "" {
		env = "production"
	}

	return Config{
		DataDir:      dataDir,
		ListenAddr:   listenAddr,
		AllowedHosts: SplitCSV(os.Getenv("AGENT_OBS_ALLOWED_HOSTS")),
		ExtraRoots:   ParseExtraRoots(os.Getenv("AGENT_OBS_EXTRA_ROOTS")),
		Environment:  env,
	}, nil
}

func DataDirHash(dataDir string) string {
	sum := sha256.Sum256([]byte(filepath.Clean(dataDir)))
	return hex.EncodeToString(sum[:])[:16]
}

func EnsureDirs(dataDir string) error {
	for _, dir := range []string{
		dataDir,
		filepath.Join(dataDir, "run"),
		filepath.Join(dataDir, "logs"),
	} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("create private dir %s: %w", dir, err)
		}
		if runtime.GOOS != "windows" {
			// Enforce 0700 even if the directory pre-existed with looser umask permissions.
			if err := os.Chmod(dir, 0o700); err != nil {
				return fmt.Errorf("chmod private dir %s: %w", dir, err)
			}
		}
	}
	return nil
}

func SplitCSV(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func ParseExtraRoots(raw string) map[string][]string {
	items := SplitCSV(raw)
	if len(items) == 0 {
		return nil
	}
	roots := make(map[string][]string)
	for _, item := range items {
		agent, path, ok := strings.Cut(item, ":")
		if !ok {
			continue
		}
		agent = strings.TrimSpace(strings.ToLower(agent))
		path = strings.TrimSpace(path)
		if agent == "" || path == "" {
			continue
		}
		roots[agent] = append(roots[agent], path)
	}
	return roots
}
