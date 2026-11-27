package api

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/agent-observatory/agent-observatory-go/internal/model"
	"github.com/agent-observatory/agent-observatory-go/internal/redact"
)

var allowedLogComponents = map[string]bool{
	"all":        true,
	"api":        true,
	"indexer":    true,
	"app":        true,
	"worker":     true,
	"supervisor": true,
	"frontend":   true,
}

type systemLogEntry struct {
	Timestamp string `json:"timestamp"`
	Component string `json:"component"`
	Level     string `json:"level"`
	Message   string `json:"message"`
}

func (s *Server) handleSystemStatus(w http.ResponseWriter, r *http.Request) {
	readiness := s.buildReadiness(r.Context())
	components := make(map[string]any, len(readiness.Components)+1)
	for name, comp := range readiness.Components {
		entry := map[string]any{
			"status":          comp.Status,
			"healthy":         comp.Healthy,
			"runtime_status":  comp.Status,
		}
		if comp.Message != "" {
			entry["message"] = redact.Strings(comp.Message)
		}
		if comp.LastSeenAt != "" {
			entry["last_seen_at"] = comp.LastSeenAt
		}
		if comp.LastSuccessAt != "" {
			entry["last_success_at"] = comp.LastSuccessAt
		}
		if name == "indexer" && s.indexer != nil {
			snap := s.indexer.Snapshot()
			entry["last_cycle"] = snap.LastCycle
			entry["active_sessions"] = snap.ActiveSessions
			entry["detected_agents"] = snap.DetectedAgents
			if snap.LastError != "" {
				entry["last_error"] = redact.Strings(snap.LastError)
			}
		}
		components[name] = entry
	}
	components["api"] = map[string]any{
		"status":         "up",
		"healthy":        true,
		"runtime_status": "running",
		"pid":            os.Getpid(),
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status":       readiness.Status,
		"environment":  s.cfg.Environment,
		"generated_at": model.FormatUTC(time.Now()),
		"supervisor": map[string]any{
			"status":     "running",
			"pid":        os.Getpid(),
			"started_at": s.startedAt,
		},
		"components":       components,
		"recovery_actions": readiness.RecoveryActions,
		"recommendations":  readiness.RecoveryActions,
	})
}

func (s *Server) handleSystemLogs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	component := strings.TrimSpace(q.Get("component"))
	if component == "" {
		component = "all"
	}
	if !allowedLogComponents[component] {
		writeError(w, r, http.StatusBadRequest, "bad_request", "invalid log component", false)
		return
	}
	limit, err := parseQueryInt(r, "limit", 100, 1, 500)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "bad_request", "invalid limit", false)
		return
	}
	offset := 0
	if rawCursor := strings.TrimSpace(q.Get("cursor")); rawCursor != "" {
		decoded, decErr := base64.RawURLEncoding.DecodeString(rawCursor)
		if decErr != nil {
			writeError(w, r, http.StatusBadRequest, "bad_request", "invalid log cursor", false)
			return
		}
		parsedOff, convErr := strconv.Atoi(string(decoded))
		if convErr != nil || parsedOff < 0 {
			writeError(w, r, http.StatusBadRequest, "bad_request", "invalid log cursor", false)
			return
		}
		offset = parsedOff
	}

	entries := s.readRecentLogs(component, offset, limit)
	hasMore := len(entries) > limit
	if hasMore {
		entries = entries[:limit]
	}
	var nextCursor *string
	if hasMore {
		enc := base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(offset + len(entries))))
		nextCursor = &enc
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items":       entries,
		"next_cursor": nextCursor,
	})
}

func (s *Server) readRecentLogs(component string, offset, limit int) []systemLogEntry {
	logDir := filepath.Join(s.cfg.DataDir, "logs")
	paths := []string{
		filepath.Join(logDir, "observatory.log"),
		filepath.Join(logDir, "observatory.log.1"),
	}

	var all []systemLogEntry
	for _, p := range paths {
		f, err := os.Open(p)
		if err != nil {
			continue
		}
		var fileEntries []systemLogEntry
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			entry := parseLogLine(line)
			if component != "all" && entry.Component != component {
				continue
			}
			fileEntries = append(fileEntries, entry)
		}
		_ = f.Close()
		// Reverse per-file so newest lines appear first.
		for i, j := 0, len(fileEntries)-1; i < j; i, j = i+1, j-1 {
			fileEntries[i], fileEntries[j] = fileEntries[j], fileEntries[i]
		}
		all = append(all, fileEntries...)
	}

	if offset >= len(all) {
		return []systemLogEntry{}
	}
	end := offset + limit + 1
	if end > len(all) {
		end = len(all)
	}
	return all[offset:end]
}

func parseLogLine(line string) systemLogEntry {
	var raw map[string]any
	if json.Unmarshal([]byte(line), &raw) == nil && raw != nil {
		ts, _ := raw["ts"].(string)
		if ts == "" {
			ts, _ = raw["timestamp"].(string)
		}
		comp, _ := raw["component"].(string)
		if comp == "" {
			comp = "app"
		}
		lvl, _ := raw["level"].(string)
		if lvl == "" {
			lvl = "info"
		}
		msg, _ := raw["message"].(string)
		return systemLogEntry{
			Timestamp: ts,
			Component: comp,
			Level:     lvl,
			Message:   redact.Strings(msg),
		}
	}
	return systemLogEntry{
		Timestamp: model.FormatUTC(time.Now()),
		Component: "app",
		Level:     "info",
		Message:   redact.Strings(line),
	}
}
