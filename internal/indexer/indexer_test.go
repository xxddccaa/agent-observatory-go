package indexer

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/agent-observatory/agent-observatory-go/internal/adapters"
	"github.com/agent-observatory/agent-observatory-go/internal/model"
	"github.com/agent-observatory/agent-observatory-go/internal/storage"
)

func TestIndexerEndToEndIncrementalAndErrorIsolation(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	store, err := storage.Open(ctx, dataDir)
	if err != nil {
		t.Fatalf("storage.Open() error = %v", err)
	}
	defer func() { _ = store.Close() }()

	// Create a fake git repository workspace to verify workspace classification + linking.
	projDir := filepath.Join(t.TempDir(), "my-repo")
	if err := os.MkdirAll(filepath.Join(projDir, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}

	codexRoot := filepath.Join(t.TempDir(), "codex")
	if err := os.MkdirAll(codexRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	rolloutPath := filepath.Join(codexRoot, "rollout-1.jsonl")
	initial := strings.Join([]string{
		`{"timestamp":"2026-11-26T10:00:00Z","type":"session_meta","payload":{"id":"thr-1","cwd":"` + filepath.ToSlash(projDir) + `"}}`,
		`{"timestamp":"2026-11-26T10:00:01Z","type":"event_msg","payload":{"type":"task_started"}}`,
		`{"timestamp":"2026-11-26T10:00:02Z","type":"event_msg","payload":{"type":"user_message","message":"First step"}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(rolloutPath, []byte(initial), 0o644); err != nil {
		t.Fatal(err)
	}

	// Add an unreadable file under a custom generic root (on non-root Unix) to test error isolation.
	genericRoot := filepath.Join(t.TempDir(), "custom")
	if err := os.MkdirAll(genericRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	okCustom := filepath.Join(genericRoot, "ok.jsonl")
	if err := os.WriteFile(okCustom, []byte(`{"timestamp":"2026-11-26T10:05:00Z","role":"user","text":"Custom hi"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	badCustom := filepath.Join(genericRoot, "unreadable.jsonl")
	if err := os.WriteFile(badCustom, []byte(`{}`+"\n"), 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(badCustom, 0o644) })

	idx := New(store, Options{
		CustomAdapters: []adapters.Adapter{
			adapters.NewCodexAdapter(codexRoot),
			adapters.NewGenericAdapter(genericRoot, "custom"),
		},
	})

	stats1, err := idx.RunOnce(ctx, true)
	if err != nil {
		t.Fatalf("RunOnce(1) error = %v", err)
	}
	expectedIndexed := 2
	if runtime.GOOS != "windows" && os.Geteuid() != 0 {
		if stats1.Failures != 1 {
			t.Fatalf("expected 1 isolated failure for unreadable file, got %+v", stats1)
		}
	} else {
		expectedIndexed = 3
	}
	if stats1.Indexed != expectedIndexed {
		t.Fatalf("stats1.Indexed = %d, want %d (%+v)", stats1.Indexed, expectedIndexed, stats1)
	}

	// Verify sessions and workspace classification persisted in SQLite.
	sessions, err := store.ListSessions(ctx, model.SessionListFilter{Limit: 10})
	if err != nil || len(sessions.Sessions) != expectedIndexed {
		t.Fatalf("ListSessions = %+v, err=%v", sessions, err)
	}
	projects, err := store.ListProjects(ctx, model.ProjectListFilter{Range: "all", Now: time.Date(2026, 11, 27, 0, 0, 0, 0, time.UTC)})
	if err != nil || projects.Total != 1 || projects.Items[0].WorkspaceKind != "persistent_project" {
		t.Fatalf("expected linked persistent_project workspace, got %+v, err=%v", projects, err)
	}

	// Second RunOnce without changes must plan UNCHANGED and index 0 sessions.
	stats2, err := idx.RunOnce(ctx, false)
	if err != nil {
		t.Fatalf("RunOnce(2) error = %v", err)
	}
	if stats2.Indexed != 0 {
		t.Fatalf("expected 0 indexed on unchanged cycle, got %+v (lastErr=%q)", stats2, idx.Snapshot().LastError)
	}

	// Append a line to rolloutPath -> RunOnce(false) incrementally appends without dropping old events.
	f, err := os.OpenFile(rolloutPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString(`{"timestamp":"2026-11-26T10:00:05Z","type":"event_msg","payload":{"type":"agent_message","message":"Done"}}` + "\n" +
		`{"timestamp":"2026-11-26T10:00:06Z","type":"event_msg","payload":{"type":"task_complete"}}` + "\n")
	_ = f.Close()

	stats3, err := idx.RunOnce(ctx, false)
	if err != nil {
		t.Fatalf("RunOnce(3) error = %v", err)
	}
	if stats3.Indexed != 1 {
		t.Fatalf("expected 1 indexed after append, got %+v (lastErr=%q)", stats3, idx.Snapshot().LastError)
	}

	sessID := ""
	for _, s := range sessions.Sessions {
		if s.Agent == "codex" {
			sessID = s.ID
			break
		}
	}
	msgs, err := store.ListSessionMessages(ctx, sessID)
	if err != nil || len(msgs) != 2 {
		t.Fatalf("expected 2 messages after append, got %+v, err=%v", msgs, err)
	}

	// Verify RunLoop runs and cancels cleanly.
	loopCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		idx.RunLoop(loopCtx)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunLoop did not stop promptly on context cancel")
	}

	snap := idx.Snapshot()
	if snap.StartedAt == "" || !snap.LastCycle.Finished {
		t.Fatalf("unexpected indexer snapshot: %+v", snap)
	}
}

func TestIndexerClaudeAndGeminiSubagentsWithoutParentFKFailure(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	store, err := storage.Open(ctx, dataDir)
	if err != nil {
		t.Fatalf("storage.Open() error = %v", err)
	}
	defer func() { _ = store.Close() }()

	claudeRoot := filepath.Join(t.TempDir(), "claude-projects")
	claudeSubDir := filepath.Join(claudeRoot, "-tmp-demo", "thr-claude-parent-missing", "subagents")
	if err := os.MkdirAll(claudeSubDir, 0o755); err != nil {
		t.Fatal(err)
	}
	claudeSubFile := filepath.Join(claudeSubDir, "agent-1.jsonl")
	claudeLine := `{"type":"user","timestamp":"2026-11-27T02:00:00Z","cwd":"/tmp/demo","message":{"role":"user","content":"subagent prompt"}}` + "\n"
	if err := os.WriteFile(claudeSubFile, []byte(claudeLine), 0o644); err != nil {
		t.Fatal(err)
	}

	geminiHome := filepath.Join(t.TempDir(), "gemini-home")
	geminiSubDir := filepath.Join(geminiHome, "tmp", "demo-slug", "chats", "thr-gemini-parent-missing")
	if err := os.MkdirAll(geminiSubDir, 0o755); err != nil {
		t.Fatal(err)
	}
	geminiSubFile := filepath.Join(geminiSubDir, "sub-1.jsonl")
	geminiLines := strings.Join([]string{
		`{"type":"session_meta","sessionId":"sub-gem-1","timestamp":"2026-11-27T02:01:00Z"}`,
		`{"type":"user","timestamp":"2026-11-27T02:01:01Z","text":"subagent gemini"}`,
	}, "\n") + "\n"
	if err := os.WriteFile(geminiSubFile, []byte(geminiLines), 0o644); err != nil {
		t.Fatal(err)
	}

	idx := New(store, Options{
		CustomAdapters: []adapters.Adapter{
			adapters.NewClaudeAdapter(claudeRoot),
			adapters.NewGeminiAdapter(geminiHome),
		},
	})

	stats, err := idx.RunOnce(ctx, true)
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if stats.Failures != 0 || stats.Indexed != 2 || idx.Snapshot().LastError != "" {
		t.Fatalf("expected 0 failures and 2 indexed subagents, got stats=%+v lastErr=%q", stats, idx.Snapshot().LastError)
	}

	sessions, err := store.ListSessions(ctx, model.SessionListFilter{IncludeSubagents: true, Limit: 10})
	if err != nil || len(sessions.Sessions) != 2 {
		t.Fatalf("ListSessions(IncludeSubagents=true) = %+v, err=%v", sessions, err)
	}
	for _, s := range sessions.Sessions {
		if !s.IsSubagent || s.ParentSessionID == nil || *s.ParentSessionID == "" {
			t.Fatalf("expected subagent parent_session_id preserved, got %+v", s)
		}
	}
}
