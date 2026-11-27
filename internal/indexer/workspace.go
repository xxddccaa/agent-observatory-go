package indexer

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/agent-observatory/agent-observatory-go/internal/model"
	"github.com/agent-observatory/agent-observatory-go/internal/storage"
)

var unixSystemPaths = map[string]bool{
	"/": true, "/bin": true, "/boot": true, "/dev": true, "/etc": true,
	"/home": true, "/lib": true, "/lib64": true, "/opt": true, "/proc": true,
	"/root": true, "/run": true, "/sbin": true, "/srv": true, "/sys": true,
	"/tmp": true, "/usr": true, "/var": true,
}

type workspaceClassCache struct {
	mu      sync.Mutex
	entries map[string]struct {
		at         time.Time
		kind       string
		confidence string
	}
}

var classCache = &workspaceClassCache{
	entries: make(map[string]struct {
		at         time.Time
		kind       string
		confidence string
	}),
}

// NormalizeCWD normalizes an execution directory to an absolute cleaned path.
func NormalizeCWD(raw *string) string {
	if raw == nil {
		return ""
	}
	trimmed := strings.TrimSpace(*raw)
	if trimmed == "" || strings.IndexByte(trimmed, 0) >= 0 {
		return ""
	}
	if strings.HasPrefix(trimmed, "~/") || strings.HasPrefix(trimmed, `~\`) {
		if home, err := os.UserHomeDir(); err == nil {
			trimmed = filepath.Join(home, trimmed[2:])
		}
	}
	if !filepath.IsAbs(trimmed) {
		return ""
	}
	cleaned := filepath.Clean(trimmed)
	if resolved, err := filepath.EvalSymlinks(cleaned); err == nil {
		cleaned = resolved
	}
	return cleaned
}

// ClassifyWorkspace assigns a deterministic workspace kind and confidence with TTL caching.
func ClassifyWorkspace(normalizedPath string) (string, string) {
	now := time.Now()
	classCache.mu.Lock()
	if hit, ok := classCache.entries[normalizedPath]; ok && now.Sub(hit.at) < 5*time.Minute {
		classCache.mu.Unlock()
		return hit.kind, hit.confidence
	}
	classCache.mu.Unlock()

	kind, conf := classifyWorkspaceUncached(normalizedPath)

	classCache.mu.Lock()
	if len(classCache.entries) >= 4096 {
		classCache.entries = make(map[string]struct {
			at         time.Time
			kind       string
			confidence string
		})
	}
	classCache.entries[normalizedPath] = struct {
		at         time.Time
		kind       string
		confidence string
	}{at: now, kind: kind, confidence: conf}
	classCache.mu.Unlock()
	return kind, conf
}

func classifyWorkspaceUncached(normalizedPath string) (string, string) {
	if unixSystemPaths[normalizedPath] {
		return "system_directory", "high"
	}
	if runtime.GOOS == "windows" && len(normalizedPath) <= 3 && strings.HasSuffix(normalizedPath, `\`) {
		return "system_directory", "high"
	}

	if info, err := os.Stat(normalizedPath); err == nil && info.IsDir() {
		cur := normalizedPath
		for {
			if _, gitErr := os.Stat(filepath.Join(cur, ".git")); gitErr == nil {
				return "persistent_project", "high"
			}
			parent := filepath.Dir(cur)
			if parent == cur {
				break
			}
			cur = parent
		}
	}

	lowName := strings.ToLower(filepath.Base(normalizedPath))
	parent := filepath.Dir(normalizedPath)
	if (parent == "/tmp" || parent == "/private/tmp") && (strings.HasPrefix(lowName, "pytest-") || strings.HasPrefix(lowName, ".pytest-") || strings.HasPrefix(lowName, "tmppytest-")) {
		return "test_fixture", "high"
	}
	if strings.Contains(normalizedPath, "pytest-of-") {
		return "test_fixture", "high"
	}
	if (parent == "/tmp" || parent == "/private/tmp") && (strings.HasPrefix(lowName, "tmp") || strings.HasPrefix(lowName, "rcw-") || strings.HasPrefix(lowName, "task-")) {
		return "ephemeral_task", "high"
	}
	if strings.Contains(normalizedPath, "/rcw-") {
		return "ephemeral_task", "high"
	}

	if _, err := os.Stat(normalizedPath); err != nil {
		return "deleted_path", "low"
	}
	return "unknown", "low"
}

func linkWorkspaceTx(ctx context.Context, tx *sql.Tx, store *storage.Store, sess *model.AgentSession, nowStr string) error {
	normalized := NormalizeCWD(sess.CWD)
	if normalized == "" {
		sess.CWD = nil
		return nil
	}
	sum := sha256.Sum256([]byte(normalized))
	wsID := "workspace-" + hex.EncodeToString(sum[:])[:48]
	kind, conf := ClassifyWorkspace(normalized)
	name := filepath.Base(normalized)
	if name == "" || name == "." || name == string(filepath.Separator) {
		name = normalized
	}
	ws := model.Workspace{
		ID:                  wsID,
		Path:                normalized,
		NormalizedPath:      normalized,
		DisplayName:         name,
		IsAllowed:           true,
		WorkspaceKind:       kind,
		WorkspaceConfidence: conf,
		CreatedAt:           nowStr,
		UpdatedAt:           nowStr,
	}
	if err := store.UpsertWorkspaceTx(ctx, tx, ws); err != nil {
		return err
	}
	sess.WorkspaceID = &wsID
	return nil
}
