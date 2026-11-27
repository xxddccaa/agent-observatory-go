package discovery

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// KnownLayouts maps supported coding agents to their default read-only session roots.
var KnownLayouts = map[string][]string{
	"codex":  {"~/.codex/sessions"},
	"claude": {"~/.claude/projects"},
	"gemini": {"~/.gemini/tmp"},
	"agy":    {"~/.gemini/antigravity-cli/brain"},
	"xdcode": {"~/.xdcode/sessions", "~/.xdcode/archived_sessions"},
	"pi":     {"~/.pi/agent/sessions"},
	"dsh":    {"~/.dsh/sessions"},
}

// ImplementedAgents lists coding agents with specialized read-only adapters.
var ImplementedAgents = map[string]bool{
	"codex":  true,
	"claude": true,
	"gemini": true,
	"agy":    true,
	"xdcode": true,
	"pi":     true,
	"dsh":    true,
}

// DetectedRoot describes one probed local agent directory.
type DetectedRoot struct {
	Agent       string `json:"agent"`
	Path        string `json:"path"`
	Exists      bool   `json:"exists"`
	Implemented bool   `json:"implemented"`
}

// AgentSummary summarizes discovered roots per agent for system health responses.
type AgentSummary struct {
	Exists      bool           `json:"exists"`
	Implemented bool           `json:"implemented"`
	Roots       []RootPresence `json:"roots"`
}

// RootPresence reports whether one root directory exists.
type RootPresence struct {
	Path   string `json:"path"`
	Exists bool   `json:"exists"`
}

// ExpandPath expands a leading "~/" using the current user's home directory.
func ExpandPath(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "~" {
		if home, err := os.UserHomeDir(); err == nil {
			return home
		}
		return trimmed
	}
	if strings.HasPrefix(trimmed, "~/") || strings.HasPrefix(trimmed, `~\`) {
		if home, err := os.UserHomeDir(); err == nil {
			return filepath.Join(home, trimmed[2:])
		}
	}
	return trimmed
}

// ProbeAgents probes known and extra roots without mutating the filesystem.
func ProbeAgents(extraRoots, layouts map[string][]string) []DetectedRoot {
	base := layouts
	if base == nil {
		base = KnownLayouts
	}
	merged := make(map[string][]string, len(base)+len(extraRoots))
	for agent, roots := range base {
		merged[agent] = append([]string(nil), roots...)
	}
	for agent, roots := range extraRoots {
		norm := strings.ToLower(strings.TrimSpace(agent))
		if norm == "" {
			continue
		}
		merged[norm] = append(merged[norm], roots...)
	}

	agents := make([]string, 0, len(merged))
	for agent := range merged {
		agents = append(agents, agent)
	}
	sort.Strings(agents)

	type seenKey struct {
		agent string
		real  string
	}
	seen := make(map[seenKey]bool)
	out := make([]DetectedRoot, 0, len(agents))

	for _, agent := range agents {
		for _, rawRoot := range merged[agent] {
			expanded := ExpandPath(rawRoot)
			real := CachedResolve(expanded)
			key := seenKey{agent: agent, real: real}
			if seen[key] {
				continue
			}
			seen[key] = true
			_, err := os.Stat(expanded)
			out = append(out, DetectedRoot{
				Agent:       agent,
				Path:        expanded,
				Exists:      err == nil,
				Implemented: ImplementedAgents[agent],
			})
		}
	}
	return out
}

// DetectedAgents returns sorted agent names that have at least one existing root.
func DetectedAgents(extraRoots, layouts map[string][]string) []string {
	set := make(map[string]bool)
	for _, root := range ProbeAgents(extraRoots, layouts) {
		if root.Exists {
			set[root.Agent] = true
		}
	}
	out := make([]string, 0, len(set))
	for agent := range set {
		out = append(out, agent)
	}
	sort.Strings(out)
	return out
}

// Summary groups probed roots by agent for status/health endpoints.
func Summary(extraRoots, layouts map[string][]string) map[string]AgentSummary {
	out := make(map[string]AgentSummary)
	for _, root := range ProbeAgents(extraRoots, layouts) {
		entry, ok := out[root.Agent]
		if !ok {
			entry = AgentSummary{Implemented: root.Implemented}
		}
		entry.Exists = entry.Exists || root.Exists
		entry.Roots = append(entry.Roots, RootPresence{Path: root.Path, Exists: root.Exists})
		out[root.Agent] = entry
	}
	return out
}
