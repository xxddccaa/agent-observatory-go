package adapters

import (
	"bytes"
	"context"
	"database/sql"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/agent-observatory/agent-observatory-go/internal/model"
	"github.com/agent-observatory/agent-observatory-go/internal/storage"
	"github.com/klauspost/compress/zstd"
)

func assignFakeEventIDs(events []model.AgentEvent) []model.AgentEvent {
	out := make([]model.AgentEvent, len(events))
	copy(out, events)
	for i := range out {
		if out[i].ID == 0 {
			out[i].ID = int64(i + 1)
		}
	}
	return out
}

func TestJSONLBoundedStreamingOversizedInvalidUTF8AndNUL(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bounded.jsonl")

	// Include: valid line with raw NUL/invalid UTF-8, oversized line (> maxLineBytes), corrupt JSON, and unterminated tail.
	var buf bytes.Buffer
	buf.WriteString(`{"type":"user","text":"hi` + "\x00" + `there"}` + "\n")
	buf.Write(append([]byte(`{"type":"huge","pad":"`), bytes.Repeat([]byte("a"), 256)...))
	buf.WriteString(`"}` + "\n")
	buf.WriteString("{corrupt\n")
	buf.WriteString(`{"type":"partial"`) // unterminated

	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}

	lines, err := readJSONLLines(path, 0, 128, 64, false)
	if err != nil {
		t.Fatalf("readJSONLLines() error = %v", err)
	}
	if len(lines) != 3 {
		t.Fatalf("readJSONLLines len = %d, want 3 (unterminated partial skipped)", len(lines))
	}
	if !lines[1].Oversized || len(lines[1].Prefix) != 64 || len(lines[1].OversizedHash) != 64 {
		t.Fatalf("expected oversized line metadata, got %+v", lines[1])
	}

	sanitized := sanitizeUTF8AndNUL([]byte("abc\x00\xffdef"))
	if bytes.Contains(sanitized, []byte{0}) || !strings.Contains(string(sanitized), "abc") {
		t.Fatalf("unexpected sanitized output: %q", sanitized)
	}
	if MaxLineBytes != 16*1024*1024 {
		t.Fatalf("MaxLineBytes = %d, want 16 MiB", MaxLineBytes)
	}
}

func TestCodexAdapterDiscoverParseProjectAndZstd(t *testing.T) {
	root := t.TempDir()
	subDir := filepath.Join(root, "2026", "11")
	if err := os.MkdirAll(subDir, 0o755); err != nil {
		t.Fatal(err)
	}

	plainPath := filepath.Join(subDir, "rollout-main.jsonl")
	initialLines := strings.Join([]string{
		`{"timestamp":"2026-11-26T10:00:00.000Z","type":"session_meta","payload":{"id":"thr-main","cwd":"/tmp/work","originator":"codex_cli","cli_version":"0.149.0","model_provider":"openai","git":{"branch":"main"}}}`,
		`{"timestamp":"2026-11-26T10:00:01.000Z","type":"turn_context","payload":{"model":"gpt-5.5"}}`,
		`{"timestamp":"2026-11-26T10:00:02.000Z","type":"event_msg","payload":{"type":"task_started","turn_id":"t-1"}}`,
		`{"timestamp":"2026-11-26T10:00:02.500Z","type":"event_msg","payload":{"type":"user_message","message":"Run tests"}}`,
		`{"timestamp":"2026-11-26T10:00:03.000Z","type":"event_msg","payload":{"type":"agent_reasoning","text":"Checking test suite"}}`,
		`{"timestamp":"2026-11-26T10:00:04.000Z","type":"response_item","payload":{"type":"function_call","call_id":"c1","name":"exec_command","arguments":"{\"cmd\":\"go test ./...\"}"}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(plainPath, []byte(initialLines), 0o644); err != nil {
		t.Fatal(err)
	}

	// Sibling .jsonl.zst for plainPath must be deduplicated during discovery, while standalone .jsonl.zst is discovered.
	enc, _ := zstd.NewWriter(nil)
	if err := os.WriteFile(plainPath+".zst", enc.EncodeAll([]byte(initialLines), nil), 0o644); err != nil {
		t.Fatal(err)
	}
	subZstPath := filepath.Join(subDir, "rollout-sub.jsonl.zst")
	subLines := `{"timestamp":"2026-11-26T10:01:00.000Z","type":"session_meta","payload":{"id":"thr-sub","session_id":"thr-main","cwd":"/tmp/work"}}` + "\n" +
		`{"timestamp":"2026-11-26T10:01:01.000Z","type":"event_msg","payload":{"type":"future_unknown_kind"}}` + "\n"
	if err := os.WriteFile(subZstPath, enc.EncodeAll([]byte(subLines), nil), 0o644); err != nil {
		t.Fatal(err)
	}

	adapter := NewCodexAdapter(root)
	sources, err := adapter.Discover(true)
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}
	if len(sources) != 2 {
		t.Fatalf("Discover len = %d, want 2 (sibling zst deduplicated): %+v", len(sources), sources)
	}

	var mainSrc, subSrc SourceDescriptor
	for _, s := range sources {
		if strings.HasSuffix(s.Path, "rollout-main.jsonl") {
			mainSrc = s
		} else {
			subSrc = s
		}
	}

	plan, err := adapter.Plan(mainSrc, nil)
	if err != nil || plan.Action != PlanRebuild {
		t.Fatalf("initial Plan = %+v, err=%v, want rebuild", plan, err)
	}
	parsed, err := adapter.Parse(plan, nil)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if len(parsed.NewEvents) != 6 {
		t.Fatalf("parsed events = %d, want 6", len(parsed.NewEvents))
	}

	// Append tool output, final reply, token count, and task_complete; verify incremental PlanAppend.
	appended := strings.Join([]string{
		`{"timestamp":"2026-11-26T10:00:05.000Z","type":"response_item","payload":{"type":"function_call_output","call_id":"c1","output":"ok\nCommand exited with code 0"}}`,
		`{"timestamp":"2026-11-26T10:00:06.000Z","type":"event_msg","payload":{"type":"agent_message","message":"All tests passed."}}`,
		`{"timestamp":"2026-11-26T10:00:06.100Z","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":100,"cached_input_tokens":20,"output_tokens":50,"reasoning_output_tokens":10,"total_tokens":180},"last_token_usage":{"input_tokens":100,"cached_input_tokens":20,"output_tokens":50,"reasoning_output_tokens":10,"total_tokens":180}}}}`,
		`{"timestamp":"2026-11-26T10:00:07.000Z","type":"event_msg","payload":{"type":"task_complete"}}`,
	}, "\n") + "\n"
	f, err := os.OpenFile(plainPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString(appended)
	_ = f.Close()

	cp := &model.IndexCheckpoint{
		ParserVersion:     adapter.ParserVersion(),
		Status:            "indexed",
		LastIndexedOffset: parsed.Session.LastIndexedOffset,
		LastIndexedLine:   parsed.Session.LastIndexedLine,
		CursorJSON:        parsed.CursorJSON,
		FingerprintJSON:   parsed.FingerprintJSON,
	}
	appendPlan, err := adapter.Plan(mainSrc, cp)
	if err != nil || appendPlan.Action != PlanAppend {
		t.Fatalf("Plan after append = %+v, err=%v, want append", appendPlan, err)
	}
	parsed2, err := adapter.Parse(appendPlan, &parsed.Session)
	if err != nil {
		t.Fatalf("Parse(append) error = %v", err)
	}
	if len(parsed2.NewEvents) != 4 {
		t.Fatalf("appended events = %d, want 4", len(parsed2.NewEvents))
	}

	allEvents := assignFakeEventIDs(append(parsed.NewEvents, parsed2.NewEvents...))
	sess, proj, err := adapter.Project(parsed2.Session, allEvents)
	if err != nil {
		t.Fatalf("Project() error = %v", err)
	}
	if sess.Model == nil || *sess.Model != "gpt-5.5" || sess.TurnCount != 1 || sess.TotalTokens != 180 {
		t.Fatalf("unexpected codex projected session: %+v", sess)
	}
	if len(proj.Messages) != 3 || len(proj.ToolCalls) != 1 || proj.ToolCalls[0].Status != "ok" || proj.Turns[0].ValidationStatus != "passed" {
		t.Fatalf("unexpected codex projection: msgs=%d tools=%+v turns=%+v", len(proj.Messages), proj.ToolCalls, proj.Turns)
	}

	// Verify standalone .jsonl.zst parses subagent metadata and unknown event safely.
	subPlan, _ := adapter.Plan(subSrc, nil)
	subParsed, err := adapter.Parse(subPlan, nil)
	if err != nil {
		t.Fatalf("Parse(sub zst) error = %v", err)
	}
	if !subParsed.Session.IsSubagent || subParsed.Session.ParentSessionID == nil || *subParsed.Session.ParentSessionID != "thr-main" {
		t.Fatalf("expected subagent metadata, got %+v", subParsed.Session)
	}
	if subParsed.NewEvents[1].IsKnown || subParsed.NewEvents[1].NormalizedType != "unknown" {
		t.Fatalf("expected unknown event fallback, got %+v", subParsed.NewEvents[1])
	}
}

func TestClaudeAdapterDiscoverSubagentTokensAndToolPairing(t *testing.T) {
	root := t.TempDir()
	subDir := filepath.Join(root, "proj-a", "thr-parent", "subagents")
	if err := os.MkdirAll(subDir, 0o755); err != nil {
		t.Fatal(err)
	}
	subPath := filepath.Join(subDir, "agent-1.jsonl")
	content := strings.Join([]string{
		`{"type":"user","uuid":"u1","sessionId":"thr-child","cwd":"/repo/app","version":"1.2.3","gitBranch":"feat/x","slug":"sub-task","timestamp":"2026-11-26T11:00:00Z","message":{"role":"user","content":"Inspect config"}}`,
		`{"type":"assistant","uuid":"a1","sessionId":"thr-child","timestamp":"2026-11-26T11:00:01Z","message":{"role":"assistant","model":"claude-sonnet-4","content":[{"type":"thinking","thinking":"Checking files"},{"type":"tool_use","id":"tu-1","name":"read_file","input":{"path":"go.mod"}}],"usage":{"input_tokens":50,"cache_read_input_tokens":20,"cache_creation_input_tokens":10,"output_tokens":30,"output_tokens_details":{"thinking_tokens":12}}}}`,
		`{"type":"user","uuid":"u2","sessionId":"thr-child","timestamp":"2026-11-26T11:00:02Z","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"tu-1","content":"module x","is_error":false}]}}`,
		`{"type":"assistant","uuid":"a2","sessionId":"thr-child","timestamp":"2026-11-26T11:00:03Z","message":{"role":"assistant","content":[{"type":"text","text":"Done checking."}]}}`,
		`{"type":"summary","timestamp":"2026-11-26T11:00:04Z","summary":"Inspected go.mod"}`,
	}, "\n") + "\n"
	if err := os.WriteFile(subPath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	adapter := NewClaudeAdapter(root)
	sources, err := adapter.Discover(true)
	if err != nil || len(sources) != 1 {
		t.Fatalf("Claude Discover = %+v, err=%v", sources, err)
	}
	plan, _ := adapter.Plan(sources[0], nil)
	parsed, err := adapter.Parse(plan, nil)
	if err != nil {
		t.Fatalf("Claude Parse error = %v", err)
	}
	sess, proj, err := adapter.Project(parsed.Session, assignFakeEventIDs(parsed.NewEvents))
	if err != nil {
		t.Fatalf("Claude Project error = %v", err)
	}
	if !sess.IsSubagent || sess.ParentSessionID == nil || *sess.ParentSessionID != "thr-parent" {
		t.Fatalf("expected claude subagent parent thr-parent, got %+v", sess)
	}
	if sess.TurnCount != 1 || sess.TotalTokens != 110 || sess.CacheCreationInputTokens != 10 || sess.ReasoningOutputTokens != 12 {
		t.Fatalf("unexpected claude token/turn totals: %+v", sess)
	}
	if len(proj.ToolCalls) != 1 || proj.ToolCalls[0].Status != "ok" || *proj.ToolCalls[0].OutputText != "module x" {
		t.Fatalf("unexpected claude tools: %+v", proj.ToolCalls)
	}
	if len(proj.Messages) != 4 {
		t.Fatalf("expected 4 claude messages (user, thinking, final, summary), got %d", len(proj.Messages))
	}
}

func TestGeminiAdapterPatchRewindLegacyAndWorkspaceResolution(t *testing.T) {
	geminiHome := t.TempDir()
	projDir := filepath.Join(geminiHome, "tmp", "demo-slug")
	chatsDir := filepath.Join(projDir, "chats")
	subDir := filepath.Join(chatsDir, "sess-main")
	if err := os.MkdirAll(subDir, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(geminiHome, "projects.json"), []byte(`{"projects":{"/Users/dev/demo-project":"demo-slug"}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(projDir, ".project_root"), []byte("/Users/dev/demo-project\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	jsonlPath := filepath.Join(chatsDir, "session-1.jsonl")
	lines := strings.Join([]string{
		`{"sessionId":"sess-main","startTime":"2026-11-26T09:00:00Z","lastUpdated":"2026-11-26T09:01:00Z"}`,
		`{"id":"m1","type":"user","timestamp":"2026-11-26T09:00:01Z","content":[{"text":"<session_context>env</session_context>\nFirst prompt"}]}`,
		`{"id":"m2","type":"gemini","timestamp":"2026-11-26T09:00:02Z","model":"gemini-2.5-pro","content":"Working...","thoughts":[{"subject":"Plan","description":"Step 1"}],"toolCalls":[{"id":"tc1","name":"run_shell_command","args":{"command":"ls"},"status":"executing"}]}`,
		// Incremental snapshot with same id m2 updates content, result, and tokens (must NOT duplicate!).
		`{"id":"m2","type":"gemini","timestamp":"2026-11-26T09:00:04Z","model":"gemini-2.5-pro","content":"Finished","tokens":{"input":10,"cached":2,"output":5,"thoughts":3,"total":20},"toolCalls":[{"id":"tc1","name":"run_shell_command","args":{"command":"ls"},"status":"success","result":"ok\nExit Code: 0"}]}`,
		`{"id":"m3","type":"user","timestamp":"2026-11-26T09:00:05Z","content":"Bad branch"}`,
		`{"$rewindTo":"m3"}`,
		`{"$set":{"summary":"Demo summary","lastUpdated":"2026-11-26T09:00:10Z"}}`,
		`{"$patch":{"id":"m2","content":"Patched final"}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(jsonlPath, []byte(lines), 0o644); err != nil {
		t.Fatal(err)
	}
	// Sibling .json with same stem should be skipped, while distinct legacy .json is discovered.
	if err := os.WriteFile(filepath.Join(chatsDir, "session-1.json"), []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}
	legacyPath := filepath.Join(chatsDir, "session-legacy.json")
	if err := os.WriteFile(legacyPath, []byte(`{"sessionId":"sess-leg","startTime":"2026-11-25T10:00:00Z","messages":[{"id":"lm1","type":"user","timestamp":"2026-11-25T10:00:01Z","content":"Legacy question"}]}`), 0o644); err != nil {
		t.Fatal(err)
	}

	adapter := NewGeminiAdapter(geminiHome)
	sources, err := adapter.Discover(true)
	if err != nil || len(sources) != 2 {
		t.Fatalf("Gemini Discover = %+v, err=%v, want 2", sources, err)
	}

	var mainSrc, legSrc SourceDescriptor
	for _, s := range sources {
		if s.Format == "jsonl" {
			mainSrc = s
		} else {
			legSrc = s
		}
	}

	plan, _ := adapter.Plan(mainSrc, nil)
	parsed, err := adapter.Parse(plan, nil)
	if err != nil {
		t.Fatalf("Gemini Parse error = %v", err)
	}
	sess, proj, err := adapter.Project(parsed.Session, assignFakeEventIDs(parsed.NewEvents))
	if err != nil {
		t.Fatalf("Gemini Project error = %v", err)
	}
	if sess.CWD == nil || *sess.CWD != "/Users/dev/demo-project" {
		t.Fatalf("expected gemini workspace /Users/dev/demo-project, got %v", sess.CWD)
	}
	if sess.AITitle == nil || *sess.AITitle != "Demo summary" || sess.TotalTokens != 20 {
		t.Fatalf("unexpected gemini session metadata: %+v", sess)
	}
	// m3 was rewound, so only context + m1 + m2 reasoning + m2 patched final remain.
	foundPatched := false
	for _, m := range proj.Messages {
		if m.Text == "Bad branch" {
			t.Fatal("rewound message m3 must not remain in projection")
		}
		if m.Text == "Patched final" {
			foundPatched = true
		}
	}
	if !foundPatched || len(proj.ToolCalls) != 1 || proj.ToolCalls[0].Status != "completed" {
		t.Fatalf("unexpected gemini projection: foundPatched=%v tools=%+v", foundPatched, proj.ToolCalls)
	}

	// Legacy .json session.
	legPlan, _ := adapter.Plan(legSrc, nil)
	legParsed, err := adapter.Parse(legPlan, nil)
	if err != nil {
		t.Fatalf("Gemini legacy Parse error = %v", err)
	}
	legSess, legProj, err := adapter.Project(legParsed.Session, assignFakeEventIDs(legParsed.NewEvents))
	if err != nil {
		t.Fatalf("Gemini legacy Project error = %v", err)
	}
	if legSess.ThreadID != "sess-leg" || len(legProj.Messages) != 1 {
		t.Fatalf("unexpected legacy session: %+v msgs=%d", legSess, len(legProj.Messages))
	}
}

func TestAgyAdapterPositionalPairingAndReadOnlySummariesWithoutSHM(t *testing.T) {
	agyHome := t.TempDir()
	logsDir := filepath.Join(agyHome, "brain", "conv-123", ".system_generated", "logs")
	if err := os.MkdirAll(logsDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Create conversation_summaries.db without WAL so immutable=1 is used and no -shm is ever created.
	sumDB := filepath.Join(agyHome, "conversation_summaries.db")
	dsn := (&url.URL{Scheme: "file", Path: filepath.ToSlash(sumDB)}).String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
		CREATE TABLE conversation_summaries (
			conversation_id TEXT PRIMARY KEY,
			title TEXT,
			workspace_uris TEXT,
			parent_conversation_id TEXT
		);
		INSERT INTO conversation_summaries VALUES (
			'conv-123',
			'Agy Summary Title',
			'["file:///Users/dev/agy-proj"]',
			'conv-parent'
		);
	`)
	_ = db.Close()
	if err != nil {
		t.Fatal(err)
	}

	transcriptPath := filepath.Join(logsDir, "transcript_full.jsonl")
	lines := strings.Join([]string{
		`{"type":"USER_INPUT","step_index":0,"created_at":"2026-11-26T08:00:00Z","content":"<metadata/>\n<USER_REQUEST>\nFix build\n</USER_REQUEST>"}`,
		`{"type":"PLANNER_RESPONSE","step_index":1,"created_at":"2026-11-26T08:00:01Z","model":"gemini-3-pro","thinking":"Checking","content":"Running command","input_tokens":40,"cache_read_tokens":10,"output_tokens":20,"tool_calls":[{"name":"run_command","args":{"CommandLine":"go test"}}]}`,
		`{"type":"GENERIC","step_index":2,"created_at":"2026-11-26T08:00:03Z","status":"DONE","content":"Completed At: 2026-11-26T08:00:02.500Z\nCommand exited with code 0"}`,
		`{"type":"SYSTEM_MESSAGE","step_index":3,"created_at":"2026-11-26T08:00:04Z","content":"Background reminder"}`,
	}, "\n") + "\n"
	if err := os.WriteFile(transcriptPath, []byte(lines), 0o644); err != nil {
		t.Fatal(err)
	}

	adapter := NewAgyAdapter(agyHome, func(dbPath string, hasWAL bool) (map[string]model.AgyConversationMeta, error) {
		return storage.ReadAgyConversationSummaries(context.Background(), dbPath, hasWAL)
	})
	sources, err := adapter.Discover(true)
	if err != nil || len(sources) != 1 {
		t.Fatalf("Agy Discover = %+v, err=%v", sources, err)
	}
	plan, _ := adapter.Plan(sources[0], nil)
	parsed, err := adapter.Parse(plan, nil)
	if err != nil {
		t.Fatalf("Agy Parse error = %v", err)
	}
	sess, proj, err := adapter.Project(parsed.Session, assignFakeEventIDs(parsed.NewEvents))
	if err != nil {
		t.Fatalf("Agy Project error = %v", err)
	}

	// Verify -shm was NOT created in agyHome.
	if _, err := os.Stat(sumDB + "-shm"); !os.IsNotExist(err) {
		t.Fatalf("expected no -shm file created in agy dir, stat err = %v", err)
	}
	if sess.AITitle == nil || *sess.AITitle != "Agy Summary Title" || sess.CWD == nil || *sess.CWD != "/Users/dev/agy-proj" {
		t.Fatalf("unexpected agy metadata: %+v", sess)
	}
	if !sess.IsSubagent || sess.ParentSessionID == nil || *sess.ParentSessionID != "conv-parent" {
		t.Fatalf("expected agy parent conv-parent, got %+v", sess)
	}
	if sess.TotalTokens != 70 || len(proj.ToolCalls) != 1 || proj.ToolCalls[0].Status != "ok" {
		t.Fatalf("unexpected agy projection: sess=%+v tools=%+v", sess, proj.ToolCalls)
	}
	if proj.Messages[0].Text != "Fix build" {
		t.Fatalf("expected <USER_REQUEST> unwrapped to 'Fix build', got %q", proj.Messages[0].Text)
	}
}

func TestGenericAdapterFallback(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "custom.jsonl")
	content := strings.Join([]string{
		`{"timestamp":"2026-11-26T07:00:00Z","type":"msg","role":"user","text":"Hello custom"}`,
		`{"timestamp":"2026-11-26T07:00:02Z","type":"msg","role":"assistant","content":[{"text":"Hi back"}]}`,
		`{not-valid-json`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	adapter := NewGenericAdapter(root, "custom")
	sources, err := adapter.Discover(true)
	if err != nil || len(sources) != 1 {
		t.Fatalf("Generic Discover = %+v, err=%v", sources, err)
	}
	plan, _ := adapter.Plan(sources[0], nil)
	parsed, err := adapter.Parse(plan, nil)
	if err != nil {
		t.Fatalf("Generic Parse error = %v", err)
	}
	sess, proj, err := adapter.Project(parsed.Session, assignFakeEventIDs(parsed.NewEvents))
	if err != nil {
		t.Fatalf("Generic Project error = %v", err)
	}
	if len(parsed.NewEvents) != 3 || parsed.NewEvents[2].NormalizedType != "parse_error" {
		t.Fatalf("unexpected generic events: %+v", parsed.NewEvents)
	}
	if sess.DisplayTitle == nil || *sess.DisplayTitle != "Hello custom" || len(proj.Messages) != 2 {
		t.Fatalf("unexpected generic projection: sess=%+v msgs=%d", sess, len(proj.Messages))
	}
}

func TestXDCodeAdapterDiscoverParseAndProject(t *testing.T) {
	root := t.TempDir()
	subDir := filepath.Join(root, "2026", "11", "03")
	if err := os.MkdirAll(subDir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(subDir, "rollout-2026-11-03T14-24-01-xd-1.jsonl")
	content := strings.Join([]string{
		`{"timestamp":"2026-11-03T06:24:01.470Z","type":"session_meta","payload":{"id":"xd-1","cwd":"/tmp/xdrepo","originator":"xdcode-tui","cli_version":"0.163.0","model_provider":"ZhiYue"}}`,
		`{"timestamp":"2026-11-03T06:24:02Z","type":"turn_context","payload":{"model":"gpt-6.1-sol"}}`,
		`{"timestamp":"2026-11-03T06:24:03Z","type":"event_msg","payload":{"type":"task_started","turn_id":"t-1"}}`,
		`{"timestamp":"2026-11-03T06:24:04Z","type":"event_msg","payload":{"type":"user_message","message":"Inspect xdcode"}}`,
		`{"timestamp":"2026-11-03T06:24:05Z","type":"event_msg","payload":{"type":"agent_message","message":"XDCode ready"}}`,
		`{"timestamp":"2026-11-03T06:24:06Z","type":"event_msg","payload":{"type":"task_complete"}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	adapter := NewXDCodeAdapter(root)
	sources, err := adapter.Discover(true)
	if err != nil || len(sources) != 1 {
		t.Fatalf("XDCode Discover = %+v, err=%v", sources, err)
	}
	if sources[0].Agent != "xdcode" || sources[0].Kind != "xdcode_jsonl" || !strings.HasPrefix(sources[0].Identity, "xdcode:") {
		t.Fatalf("unexpected xdcode descriptor: %+v", sources[0])
	}
	plan, err := adapter.Plan(sources[0], nil)
	if err != nil || plan.Action != PlanRebuild {
		t.Fatalf("XDCode Plan = %+v, err=%v", plan, err)
	}
	parsed, err := adapter.Parse(plan, nil)
	if err != nil {
		t.Fatalf("XDCode Parse error = %v", err)
	}
	sess, proj, err := adapter.Project(parsed.Session, assignFakeEventIDs(parsed.NewEvents))
	if err != nil {
		t.Fatalf("XDCode Project error = %v", err)
	}
	if sess.Agent != "xdcode" || sess.SourceKind != "xdcode_jsonl" || sess.TurnCount != 1 || len(proj.Messages) != 2 {
		t.Fatalf("unexpected xdcode session/messages: sess=%+v msgs=%d", sess, len(proj.Messages))
	}
	if sess.RolloutRealPath == nil || !strings.HasPrefix(*sess.RolloutRealPath, "xdcode:") {
		t.Fatalf("expected xdcode prefixed RolloutRealPath, got %v", sess.RolloutRealPath)
	}
}

func TestPiAdapterActiveBranchIncrementalAndBounded(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "sessions", "session-pi.jsonl")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	rows := []string{
		`{"type":"session","version":3,"id":"pi-session-1","timestamp":"2026-08-21T10:00:00Z","cwd":"/tmp/pi-project"}`,
		`{"type":"model_change","id":"m1","parentId":null,"timestamp":"2026-08-21T10:00:01Z","provider":"demo","modelId":"pi-model"}`,
		`{"type":"message","id":"sys1","parentId":"m1","timestamp":"2026-08-21T10:00:03Z","message":{"role":"system","content":[{"type":"text","text":"system policy"}]}}`,
		`{"type":"message","id":"u1","parentId":"sys1","timestamp":"2026-08-21T10:00:04Z","message":{"role":"user","content":[{"type":"text","text":"hello"}]}}`,
		`{"type":"message","id":"a1","parentId":"u1","timestamp":"2026-08-21T10:00:05Z","message":{"role":"assistant","model":"pi-model","stopReason":"toolUse","content":[{"type":"thinking","thinking":"reasoning summary"},{"type":"text","text":"working"},{"type":"toolCall","id":"call-1","name":"bash","arguments":{"command":"printf ok"}}],"usage":{"input":3,"output":10,"cacheRead":2,"cacheWrite":1,"reasoning":5,"totalTokens":16}}}`,
		`{"type":"message","id":"abandoned","parentId":"a1","timestamp":"2026-08-21T10:00:06Z","message":{"role":"assistant","content":[{"type":"text","text":"abandoned branch"}],"stopReason":"stop"}}`,
		`{"type":"message","id":"tr1","parentId":"a1","timestamp":"2026-08-21T10:00:07Z","message":{"role":"toolResult","toolCallId":"call-1","toolName":"bash","content":[{"type":"text","text":"ok"}],"details":{"exitCode":0},"isError":false}}`,
		`{"type":"message","id":"a2","parentId":"tr1","timestamp":"2026-08-21T10:00:08Z","message":{"role":"assistant","content":[{"type":"text","text":"done"}],"stopReason":"stop","usage":{"input":1,"output":2,"cacheRead":0,"cacheWrite":0,"reasoning":0,"totalTokens":3}}}`,
		`{"type":"future_custom","id":"unknown-1","parentId":"a2","timestamp":"2026-08-21T10:00:09Z","payload":{"opaque":true}}`,
		`{"type":"compaction","id":"compact-1","parentId":"unknown-1","timestamp":"2026-08-21T10:00:10Z","summary":"compacted context"}`,
	}
	if err := os.WriteFile(path, []byte(strings.Join(rows, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	adapter := NewPiAdapter(root)
	sources, err := adapter.Discover(true)
	if err != nil || len(sources) != 1 {
		t.Fatalf("Pi Discover = %+v, err=%v", sources, err)
	}
	plan1, err := adapter.Plan(sources[0], nil)
	if err != nil || plan1.Action != PlanRebuild {
		t.Fatalf("Pi initial Plan = %+v, err=%v", plan1, err)
	}
	parsed1, err := adapter.Parse(plan1, nil)
	if err != nil {
		t.Fatalf("Pi Parse error = %v", err)
	}
	allEvents := assignFakeEventIDs(parsed1.NewEvents)
	sess, proj, err := adapter.Project(parsed1.Session, allEvents)
	if err != nil {
		t.Fatalf("Pi Project error = %v", err)
	}
	if sess.Agent != "pi" || sess.SourceKind != "pi_jsonl" || sess.ThreadID != "pi-session-1" {
		t.Fatalf("unexpected Pi identity fields: %+v", sess)
	}
	if sess.DisplayTitle == nil || *sess.DisplayTitle != "hello" || sess.Model == nil || *sess.Model != "pi-model" {
		t.Fatalf("unexpected Pi metadata: %+v", sess)
	}
	if sess.TurnCount != 1 || sess.InputTokens != 4 || sess.CachedInputTokens != 2 || sess.CacheCreationInputTokens != 1 || sess.OutputTokens != 12 || sess.ReasoningOutputTokens != 5 || sess.TotalTokens != 19 {
		t.Fatalf("unexpected Pi token/turn aggregation: %+v", sess)
	}
	for _, msg := range proj.Messages {
		if strings.Contains(msg.Text, "abandoned branch") {
			t.Fatalf("expected abandoned branch excluded from projected messages, got %q", msg.Text)
		}
	}
	if len(proj.ToolCalls) != 1 || proj.ToolCalls[0].Status != "ok" || proj.ToolCalls[0].ExitCode == nil || *proj.ToolCalls[0].ExitCode != 0 {
		t.Fatalf("unexpected Pi tool call projection: %+v", proj.ToolCalls)
	}

	// Unterminated trailing line must not advance cursor past the complete newline boundary.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString(`{"type":"message","id":"u2","parentId":"compact-1","timestamp":"2026-08-21T10:01:00Z","message":{"role":"user","content":[{"type":"text","text":"second"}]}}` + "\n" + `{"type":"message"`)
	_ = f.Close()

	cp := &model.IndexCheckpoint{
		ParserVersion:     adapter.ParserVersion(),
		Status:            "indexed",
		FileSize:          sess.FileSize,
		FileMtimeNs:       sess.FileMtimeNs,
		LastIndexedOffset: sess.LastIndexedOffset,
		LastIndexedLine:   sess.LastIndexedLine,
		CursorJSON:        parsed1.CursorJSON,
		FingerprintJSON:   parsed1.FingerprintJSON,
	}
	plan2, err := adapter.Plan(sources[0], cp)
	if err != nil || plan2.Action != PlanAppend {
		t.Fatalf("Pi PlanAppend expected after append, got %+v, err=%v", plan2, err)
	}
	parsed2, err := adapter.Parse(plan2, &sess)
	if err != nil {
		t.Fatalf("Pi Parse append error = %v", err)
	}
	if len(parsed2.NewEvents) != 1 {
		t.Fatalf("expected only 1 complete appended Pi event (partial tail ignored), got %d", len(parsed2.NewEvents))
	}
}

func TestDshAdapterZstdStreamAndProjection(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "x", "session.jsonl.zstd")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	rows := strings.Join([]string{
		`{"type":"session","id":"s1","createdAt":"2026-01-01T00:00:00Z","cwd":"/repo","parentSession":"p"}`,
		`{"type":"turn/start","data":{"turn":1},"time":"2026-01-01T00:00:01Z"}`,
		`{"type":"user/message","data":{"turn":1,"role":"user","source":{"kind":"user"},"content":[{"type":"text","text":"hello"}]}}`,
		`{"type":"request/context","data":{"provider":"stepcode","model":"dsh-model"}}`,
		`{"type":"assistant/message","data":{"turn":1,"step":1,"message":{"role":"assistant","content":[{"type":"reasoning","text":"think"},{"type":"text","text":"world"}]},"usage":{"inputTokens":2,"outputTokens":3}}}`,
		`{"type":"tool/call","data":{"turn":1,"step":1,"callId":"c1","name":"shell","arguments":"{\"cmd\":\"ls\"}"}}`,
		`{"type":"tool/result","data":{"turn":1,"step":1,"message":{"role":"tool","content":[{"type":"tool-result","toolCallId":"c1","content":[{"type":"text","text":"ok"}]}]}}}`,
		`{"type":"turn/end","data":{"turn":1,"reason":{"kind":"completed"}}}`,
		`{"type":"mystery","data":{"x":"nul:\u0000"}}`,
	}, "\n") + "\n"
	enc, _ := zstd.NewWriter(nil)
	if err := os.WriteFile(path, enc.EncodeAll([]byte(rows), nil), 0o644); err != nil {
		t.Fatal(err)
	}

	adapter := NewDshAdapter(root)
	sources, err := adapter.Discover(true)
	if err != nil || len(sources) != 1 {
		t.Fatalf("DSH Discover = %+v, err=%v", sources, err)
	}
	plan, err := adapter.Plan(sources[0], nil)
	if err != nil || plan.Action != PlanRebuild {
		t.Fatalf("DSH Plan = %+v, err=%v", plan, err)
	}
	parsed, err := adapter.Parse(plan, nil)
	if err != nil {
		t.Fatalf("DSH Parse error = %v", err)
	}
	sess, proj, err := adapter.Project(parsed.Session, assignFakeEventIDs(parsed.NewEvents))
	if err != nil {
		t.Fatalf("DSH Project error = %v", err)
	}
	if sess.Agent != "dsh" || sess.SourceKind != "dsh_jsonl" || sess.ThreadID != "s1" {
		t.Fatalf("unexpected DSH identity: %+v", sess)
	}
	if sess.ParentSessionID == nil || *sess.ParentSessionID != "p" || !sess.IsSubagent {
		t.Fatalf("expected parentSession=p, got %+v", sess)
	}
	if sess.Model == nil || *sess.Model != "dsh-model" || sess.TotalTokens != 5 || sess.TurnCount != 1 {
		t.Fatalf("unexpected DSH metrics/model: %+v", sess)
	}
	if len(proj.ToolCalls) != 1 || proj.ToolCalls[0].OutputText == nil || *proj.ToolCalls[0].OutputText != "ok" || proj.ToolCalls[0].Status != "completed" {
		t.Fatalf("unexpected DSH tool calls: %+v", proj.ToolCalls)
	}
	if len(proj.Messages) != 3 {
		t.Fatalf("expected 3 projected messages (user+reasoning+final), got %d", len(proj.Messages))
	}
	lastEv := parsed.NewEvents[len(parsed.NewEvents)-1]
	if lastEv.IsKnown || strings.Contains(string(lastEv.RawJSON), "\x00") || !strings.Contains(string(lastEv.RawJSON), "nul:") {
		t.Fatalf("expected unknown sanitized last event, got known=%v raw=%s", lastEv.IsKnown, lastEv.RawJSON)
	}
}
