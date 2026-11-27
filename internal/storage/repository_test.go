package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/agent-observatory/agent-observatory-go/internal/model"
)

func TestSchemaCoversAllElevenTablesAndForeignKeyIndexes(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = store.Close() }()

	expectedTables := []string{
		"schema_migrations",
		"workspaces",
		"agent_sessions",
		"index_checkpoints",
		"agent_events",
		"agent_turns",
		"messages",
		"tool_calls",
		"local_data_sources",
		"evidence_records",
		"activity_spans",
		"thread_edges",
	}

	for _, table := range expectedTables {
		var name string
		err := store.Reader().QueryRowContext(
			ctx,
			`SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`,
			table,
		).Scan(&name)
		if err != nil {
			t.Fatalf("expected table %s to exist: %v", table, err)
		}
	}

	// Verify every foreign key column is the leftmost column of at least one index.
	for _, table := range expectedTables {
		fkRows, err := store.Reader().QueryContext(ctx, fmt.Sprintf(`PRAGMA foreign_key_list(%q)`, table))
		if err != nil {
			t.Fatalf("foreign_key_list(%s): %v", table, err)
		}
		var fkCols []string
		for fkRows.Next() {
			var id, seq int
			var refTable, fromCol, toCol, onUpdate, onDelete, match string
			if err := fkRows.Scan(&id, &seq, &refTable, &fromCol, &toCol, &onUpdate, &onDelete, &match); err != nil {
				_ = fkRows.Close()
				t.Fatalf("scan foreign_key_list(%s): %v", table, err)
			}
			fkCols = append(fkCols, fromCol)
		}
		_ = fkRows.Close()

		if len(fkCols) == 0 {
			continue
		}

		idxRows, err := store.Reader().QueryContext(ctx, fmt.Sprintf(`PRAGMA index_list(%q)`, table))
		if err != nil {
			t.Fatalf("index_list(%s): %v", table, err)
		}
		var idxNames []string
		for idxRows.Next() {
			var seq, unique, partial int
			var name, origin string
			if err := idxRows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
				_ = idxRows.Close()
				t.Fatalf("scan index_list(%s): %v", table, err)
			}
			idxNames = append(idxNames, name)
		}
		_ = idxRows.Close()

		firstIndexedCols := make(map[string]bool)
		for _, idxName := range idxNames {
			infoRows, err := store.Reader().QueryContext(ctx, fmt.Sprintf(`PRAGMA index_info(%q)`, idxName))
			if err != nil {
				t.Fatalf("index_info(%s): %v", idxName, err)
			}
			for infoRows.Next() {
				var seqno, cid int
				var colName sql.NullString
				if err := infoRows.Scan(&seqno, &cid, &colName); err != nil {
					_ = infoRows.Close()
					t.Fatalf("scan index_info(%s): %v", idxName, err)
				}
				if seqno == 0 && colName.Valid {
					firstIndexedCols[colName.String] = true
				}
			}
			_ = infoRows.Close()
		}

		for _, fkCol := range fkCols {
			if !firstIndexedCols[fkCol] {
				t.Fatalf("table %s foreign key column %s is not covered as leftmost column of any index", table, fkCol)
			}
		}
	}
}

func TestZstdJSONRoundtripAndValidation(t *testing.T) {
	raw := []byte(`{"type":"response_item","payload":{"text":"hello world","count":42}}`)
	compressed, hash, err := CompressJSON(raw)
	if err != nil {
		t.Fatalf("CompressJSON() error = %v", err)
	}
	if len(compressed) == 0 || len(hash) != 64 {
		t.Fatalf("unexpected compressed len=%d hash=%q", len(compressed), hash)
	}

	decoded, err := DecompressJSON(compressed)
	if err != nil {
		t.Fatalf("DecompressJSON() error = %v", err)
	}
	if string(decoded) != string(raw) {
		t.Fatalf("decoded JSON mismatch: got %s, want %s", decoded, raw)
	}

	if _, _, err := CompressJSON([]byte(`{"broken":`)); err == nil {
		t.Fatal("expected invalid JSON to fail CompressJSON")
	}
	if _, err := DecompressJSON([]byte("not-zstd-bytes")); err == nil {
		t.Fatal("expected corrupted zstd bytes to fail DecompressJSON")
	}
}

func TestRepositoryIngestQueriesCascadesAndAnalytics(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = store.Close() }()

	now := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)
	ts1 := model.FormatUTC(now.Add(-2 * time.Hour))
	ts2 := model.FormatUTC(now.Add(-1 * time.Hour))
	ts3 := model.FormatUTC(now.Add(-20 * time.Minute))
	tsOld := model.FormatUTC(now.AddDate(0, -2, 0))

	if err := store.WithWriterTx(ctx, func(tx *sql.Tx) error {
		if err := store.UpsertWorkspaceTx(ctx, tx, model.Workspace{
			ID:                  "ws-1",
			Path:                "/Users/dev/alpha_repo",
			NormalizedPath:      "/Users/dev/alpha_repo",
			DisplayName:         "alpha_repo",
			IsAllowed:           true,
			WorkspaceKind:       "persistent_project",
			WorkspaceConfidence: "high",
			CreatedAt:           tsOld,
			UpdatedAt:           ts3,
		}); err != nil {
			return err
		}
		if err := store.UpsertWorkspaceTx(ctx, tx, model.Workspace{
			ID:                  "ws-2",
			Path:                "/Users/dev/100%beta",
			NormalizedPath:      "/Users/dev/100%beta",
			DisplayName:         "100%beta",
			IsAllowed:           true,
			WorkspaceKind:       "ephemeral_task",
			WorkspaceConfidence: "medium",
			CreatedAt:           ts2,
			UpdatedAt:           ts2,
		}); err != nil {
			return err
		}

		ws1 := "ws-1"
		ws2 := "ws-2"
		modelGPT := "gpt-5"
		modelClaude := "claude-sonnet-4"
		title1 := "Refactor parser pipeline"
		title2 := "Debug 100% failure"
		subTitle := "Child worker"
		parentID := "sess-1"

		for _, sess := range []model.AgentSession{
			{
				ID:             "sess-1",
				Agent:          "codex",
				SourceKind:     "codex_jsonl",
				SourceIdentity: "sha256:sess1",
				ThreadID:       "thr-1",
				WorkspaceID:    &ws1,
				Model:          &modelGPT,
				DisplayTitle:   &title1,
				TurnCount:      2,
				TotalTokens:    1500,
				InputTokens:    1000,
				OutputTokens:   500,
				ParserVersion:  "1",
				IndexStatus:    "indexed",
				StartTime:      &tsOld,
				EndTime:        &ts3,
				LastActivityAt: &ts3,
				CreatedAt:      tsOld,
				UpdatedAt:      ts3,
			},
			{
				ID:             "sess-2",
				Agent:          "claude",
				SourceKind:     "claude_jsonl",
				SourceIdentity: "sha256:sess2",
				ThreadID:       "thr-2",
				WorkspaceID:    &ws2,
				Model:          &modelClaude,
				DisplayTitle:   &title2,
				TurnCount:      1,
				TotalTokens:    400,
				InputTokens:    300,
				OutputTokens:   100,
				IsOngoing:      true,
				ParserVersion:  "1",
				IndexStatus:    "indexed",
				StartTime:      &ts2,
				LastActivityAt: &ts2,
				CreatedAt:      ts2,
				UpdatedAt:      ts2,
			},
			{
				ID:              "sess-sub",
				Agent:           "codex",
				SourceKind:      "codex_jsonl",
				SourceIdentity:  "sha256:sesssub",
				ThreadID:        "thr-sub",
				WorkspaceID:     &ws1,
				Model:           &modelGPT,
				DisplayTitle:    &subTitle,
				IsSubagent:      true,
				ParentSessionID: &parentID,
				TurnCount:       1,
				TotalTokens:     100,
				ParserVersion:   "1",
				IndexStatus:     "indexed",
				StartTime:       &ts1,
				EndTime:         &ts1,
				LastActivityAt:  &ts1,
				CreatedAt:       ts1,
				UpdatedAt:       ts1,
			},
		} {
			if err := store.UpsertSessionTx(ctx, tx, sess); err != nil {
				return err
			}
		}

		if err := store.UpsertCheckpointTx(ctx, tx, model.IndexCheckpoint{
			ID:                "cp-1",
			SourceKind:        "codex_jsonl",
			SourceIdentity:    "sha256:sess1",
			SourcePath:        "/tmp/s1.jsonl",
			RealPath:          "/tmp/s1.jsonl",
			AgentSessionID:    "sess-1",
			ParserVersion:     "1",
			Status:            "indexed",
			LastIndexedOffset: 512,
			CursorJSON:        `{"offset":512}`,
			FingerprintJSON:   `{"size":512}`,
			CreatedAt:         ts1,
			UpdatedAt:         ts3,
		}); err != nil {
			return err
		}

		evIDs, err := store.InsertEventsTx(ctx, tx, "sess-1", []model.AgentEvent{
			{
				EventIndex:     0,
				LineNumber:     1,
				ByteOffset:     0,
				EventTimestamp: &ts1,
				NormalizedType: "user_message",
				RawJSON:        json.RawMessage(`{"type":"user_message","text":"Check 100% coverage"}`),
				IsKnown:        true,
				ParserVersion:  "1",
				CreatedAt:      ts1,
			},
			{
				EventIndex:     1,
				LineNumber:     2,
				ByteOffset:     64,
				EventTimestamp: &ts3,
				NormalizedType: "tool_call",
				RawJSON:        json.RawMessage(`{"type":"function_call","name":"exec_command"}`),
				IsKnown:        true,
				ParserVersion:  "1",
				CreatedAt:      ts3,
			},
		})
		if err != nil {
			return err
		}

		dur1 := int64(800)
		dur2 := int64(2500)
		durOld := int64(150000)
		lat1 := int64(120)
		toolMS := int64(400)
		if err := store.ReplaceSessionProjectionTx(ctx, tx, "sess-1", SessionProjection{
			Turns: []model.AgentTurn{
				{
					ID:                  "turn-old",
					Agent:               "codex",
					AgentSessionID:      "sess-1",
					TurnIndex:           0,
					Status:              "completed",
					StartedAt:           &tsOld,
					CompletedAt:         &tsOld,
					InputTokens:         600,
					OutputTokens:        200,
					TotalTokens:         800,
					DurationMS:          &durOld,
					ValidationStatus:    "passed",
					Outcome:             "completed",
					MetricConfidence:    "derived",
					CreatedAt:           tsOld,
					UpdatedAt:           tsOld,
				},
				{
					ID:                  "turn-1",
					Agent:               "codex",
					AgentSessionID:      "sess-1",
					TurnIndex:           1,
					Status:              "completed",
					StartedAt:           &ts1,
					CompletedAt:         &ts3,
					InputTokens:         400,
					OutputTokens:        300,
					TotalTokens:         700,
					DurationMS:          &dur2,
					FirstAgentLatencyMS: &lat1,
					ActiveToolMS:        &toolMS,
					ValidationStatus:    "passed",
					Outcome:             "completed",
					MetricConfidence:    "exact",
					CreatedAt:           ts1,
					UpdatedAt:           ts3,
				},
			},
			Messages: []model.Message{
				{
					ID:             "msg-1",
					AgentSessionID: "sess-1",
					TurnID:         strPtr("turn-1"),
					EventID:        &evIDs[0],
					Role:           "user",
					Channel:        "final",
					Sequence:       0,
					Text:           "Check 100% coverage",
					MarkdownText:   strPtr("Check 100% coverage"),
					CopyText:       "Check 100% coverage",
					Timestamp:      &ts1,
					IsVisible:      true,
					CreatedAt:      ts1,
				},
			},
			ToolCalls: []model.ToolCall{
				{
					ID:             "tool-1",
					Agent:          "codex",
					AgentSessionID: "sess-1",
					TurnID:         strPtr("turn-1"),
					EventID:        &evIDs[1],
					CallID:         "call-1",
					ToolName:       "exec_command",
					ToolKind:       "exec",
					ArgumentsJSON:  json.RawMessage(`{"cmd":"go test ./..."}`),
					CommandText:    strPtr("go test 100% ./..."),
					OutputText:     strPtr("PASS"),
					ExitCode:       intPtr(0),
					Status:         "ok",
					IsMutating:     false,
					StartedAt:      &ts1,
					CompletedAt:    &ts3,
					CreatedAt:      ts1,
				},
				{
					ID:             "tool-2",
					Agent:          "codex",
					AgentSessionID: "sess-1",
					TurnID:         strPtr("turn-1"),
					EventID:        &evIDs[1],
					CallID:         "call-2",
					ToolName:       "exec_command",
					ToolKind:       "exec",
					CommandText:    strPtr("go vet ./..."),
					OutputText:     strPtr("exit status 1"),
					ExitCode:       intPtr(1),
					Status:         "failed",
					StartedAt:      &ts2,
					CompletedAt:    &ts3,
					CreatedAt:      ts2,
				},
			},
			Spans: []model.ActivitySpan{
				{
					ID:             "span-1",
					Agent:          "codex",
					AgentSessionID: "sess-1",
					SourceKind:     "rollout",
					SpanType:       "turn",
					Name:           strPtr("Turn 2"),
					Status:         "completed",
					StartedAt:      ts1,
					CompletedAt:    &ts3,
					DurationMS:     &dur1,
					Confidence:     "exact",
					MetadataJSON:   json.RawMessage(`{"turn_index":1}`),
					CreatedAt:      ts1,
				},
			},
		}); err != nil {
			return err
		}

		// Second session has one recent turn for agent comparison.
		if err := store.ReplaceSessionProjectionTx(ctx, tx, "sess-2", SessionProjection{
			Turns: []model.AgentTurn{
				{
					ID:               "turn-2",
					Agent:            "claude",
					AgentSessionID:   "sess-2",
					TurnIndex:        0,
					Status:           "failed",
					StartedAt:        &ts2,
					CompletedAt:      &ts2,
					InputTokens:      300,
					OutputTokens:     100,
					TotalTokens:      400,
					DurationMS:       &dur1,
					ValidationStatus: "failed",
					Outcome:          "failed",
					MetricConfidence: "derived",
					CreatedAt:        ts2,
					UpdatedAt:        ts2,
				},
			},
		}); err != nil {
			return err
		}

		if err := store.UpsertLocalDataSourceTx(ctx, tx, model.LocalDataSource{
			ID:              "src-1",
			Agent:           "codex",
			SourceKind:      "codex_sqlite",
			SourceIdentity:  "src:codex-state",
			SourcePath:      "/tmp/state.sqlite",
			SourceFormat:    "sqlite",
			ParserVersion:   "1",
			Status:          "indexed",
			RecordCount:     1,
			ByteSize:        4096,
			LastSuccessAt:   &ts3,
			CursorJSON:      `{}`,
			FingerprintJSON: `{}`,
			CreatedAt:       ts1,
			UpdatedAt:       ts3,
		}); err != nil {
			return err
		}

		if err := store.UpsertEvidenceRecordTx(ctx, tx, model.EvidenceRecord{
			ID:                "ev-rec-1",
			SourceID:          "src-1",
			SourceRecordID:    "rec-1",
			AgentSessionID:    strPtr("sess-1"),
			AgentEventID:      &evIDs[0],
			ThreadID:          strPtr("thr-1"),
			Category:          "session_note",
			Level:             strPtr("info"),
			RecordedAt:        &ts3,
			Summary:           strPtr("Evidence summary"),
			RawJSON:           json.RawMessage(`{"note":"hello"}`),
			SourceLocatorJSON: json.RawMessage(`{"line":1}`),
			MetadataJSON:      json.RawMessage(`{}`),
			CreatedAt:         ts3,
		}); err != nil {
			return err
		}

		if err := store.UpsertThreadEdgeTx(ctx, tx, model.ThreadEdge{
			ID:             "edge-1",
			SourceID:       "src-1",
			ParentThreadID: "thr-1",
			ChildThreadID:  "thr-sub",
			Status:         "completed",
			EventTime:      &ts1,
			MetadataJSON:   json.RawMessage(`{}`),
			CreatedAt:      ts1,
		}); err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatalf("WithWriterTx ingest error = %v", err)
	}

	// Verify session listing excludes subagents by default and escapes LIKE % wildcard.
	listRes, err := store.ListSessions(ctx, model.SessionListFilter{Limit: 10})
	if err != nil {
		t.Fatalf("ListSessions() error = %v", err)
	}
	if len(listRes.Sessions) != 2 {
		t.Fatalf("ListSessions default len = %d, want 2", len(listRes.Sessions))
	}
	if listRes.Sessions[0].ID != "sess-1" || listRes.Sessions[1].ID != "sess-2" {
		t.Fatalf("unexpected session order: %s, %s", listRes.Sessions[0].ID, listRes.Sessions[1].ID)
	}

	// Cursor pagination across 1-item pages.
	page1, err := store.ListSessions(ctx, model.SessionListFilter{Limit: 1})
	if err != nil || page1.NextCursor == nil || len(page1.Sessions) != 1 || page1.Sessions[0].ID != "sess-1" {
		t.Fatalf("page1 unexpected: %+v, err=%v", page1, err)
	}
	page2, err := store.ListSessions(ctx, model.SessionListFilter{Limit: 1, Cursor: *page1.NextCursor})
	if err != nil || page2.NextCursor != nil || len(page2.Sessions) != 1 || page2.Sessions[0].ID != "sess-2" {
		t.Fatalf("page2 unexpected: %+v, err=%v", page2, err)
	}

	// Literal % search must only match sess-2 ("Debug 100% failure"), not sess-1.
	pctRes, err := store.ListSessions(ctx, model.SessionListFilter{Limit: 10, Query: "100%"})
	if err != nil {
		t.Fatalf("ListSessions(100%%) error = %v", err)
	}
	if len(pctRes.Sessions) != 1 || pctRes.Sessions[0].ID != "sess-2" {
		t.Fatalf("literal %% search matched %+v, want only sess-2", pctRes.Sessions)
	}

	// Verify zstd decompression in ListSessionEvents + SessionSearch literal % matching.
	events, err := store.ListSessionEvents(ctx, "sess-1")
	if err != nil {
		t.Fatalf("ListSessionEvents() error = %v", err)
	}
	if len(events) != 2 || !strings.Contains(string(events[0].RawJSON), "Check 100% coverage") {
		t.Fatalf("unexpected decoded events: %+v", events)
	}

	matches, err := store.SearchSession(ctx, "sess-1", "100%", 10)
	if err != nil {
		t.Fatalf("SearchSession() error = %v", err)
	}
	if len(matches) != 2 || matches[0].Kind != "message" || matches[1].Kind != "tool" {
		t.Fatalf("unexpected search matches: %+v", matches)
	}

	// Projects analytics: all_time vs day window.
	projPage, err := store.ListProjects(ctx, model.ProjectListFilter{Range: "day", Now: now, Limit: 10})
	if err != nil {
		t.Fatalf("ListProjects() error = %v", err)
	}
	if projPage.Total != 2 || projPage.Items[0].ID != "ws-1" {
		t.Fatalf("unexpected projects page: %+v", projPage)
	}
	if projPage.Items[0].AllTime.TotalTokens != 1500 || projPage.Items[0].SelectedRange.TotalTokens != 700 {
		t.Fatalf("ws-1 token windows mismatch: all=%+v day=%+v", projPage.Items[0].AllTime, projPage.Items[0].SelectedRange)
	}

	projDetail, err := store.GetProject(ctx, "ws-1", now)
	if err != nil {
		t.Fatalf("GetProject() error = %v", err)
	}
	if projDetail.Totals.AllTime.TotalTokens != 1500 || projDetail.Totals.Day.TotalTokens != 700 {
		t.Fatalf("GetProject totals mismatch: %+v", projDetail.Totals)
	}

	tsProj, err := store.GetProjectTimeseries(ctx, "ws-1", "day", now)
	if err != nil {
		t.Fatalf("GetProjectTimeseries() error = %v", err)
	}
	if tsProj.Bucket != "hour" || len(tsProj.Points) != 1 || tsProj.Points[0].TotalTokens != 700 {
		t.Fatalf("GetProjectTimeseries unexpected: %+v", tsProj)
	}

	// Insights analytics over last 24h.
	filters := model.InsightFilters{
		Since: now.Add(-24 * time.Hour),
		Until: now,
	}
	overview, err := store.GetInsightsOverview(ctx, filters)
	if err != nil {
		t.Fatalf("GetInsightsOverview() error = %v", err)
	}
	if overview.Totals.SessionCount != 2 || overview.Totals.TurnCount != 2 || overview.Totals.TotalTokens != 1100 {
		t.Fatalf("overview totals unexpected: %+v", overview.Totals)
	}

	tsInsights, err := store.GetInsightsTimeseries(ctx, filters)
	if err != nil {
		t.Fatalf("GetInsightsTimeseries() error = %v", err)
	}
	if tsInsights.Bucket != "hour" || len(tsInsights.Points) == 0 || len(tsInsights.ByAgent) != 2 {
		t.Fatalf("GetInsightsTimeseries unexpected: %+v", tsInsights)
	}

	projFiltered, err := store.ListProjects(ctx, model.ProjectListFilter{Range: "all", Agent: "claude", Query: "100%", Now: now, Limit: 10})
	if err != nil {
		t.Fatalf("ListProjects filtered error = %v", err)
	}
	if projFiltered.Total != 1 || len(projFiltered.Items) != 1 || projFiltered.Items[0].ID != "ws-2" {
		t.Fatalf("ListProjects filtered unexpected: %+v", projFiltered)
	}

	sources, err := store.GetInsightsSources(ctx, "codex", "", "")
	if err != nil || len(sources.Sources) != 1 || sources.Sources[0].ID != "src-1" {
		t.Fatalf("GetInsightsSources unexpected: %+v, err=%v", sources, err)
	}

	timeline, err := store.GetSessionTimeline(ctx, "sess-1", 100)
	if err != nil || len(timeline.Spans) != 1 || timeline.Spans[0].ID != "span-1" {
		t.Fatalf("GetSessionTimeline unexpected: %+v, err=%v", timeline, err)
	}

	evList, err := store.ListEvidence(ctx, filters, "src-1", "", "", "", 10)
	if err != nil || len(evList.Items) != 1 || evList.Items[0].ID != "ev-rec-1" {
		t.Fatalf("ListEvidence unexpected: %+v, err=%v", evList, err)
	}

	latency, err := store.GetInsightsLatency(ctx, filters)
	if err != nil {
		t.Fatalf("GetInsightsLatency() error = %v", err)
	}
	if latency.Summary.TurnDurationMS.Count != 2 || latency.Summary.TurnDurationMS.P50 <= 0 {
		t.Fatalf("latency summary unexpected: %+v", latency.Summary)
	}

	tools, err := store.GetInsightsTools(ctx, filters, "", "")
	if err != nil {
		t.Fatalf("GetInsightsTools() error = %v", err)
	}
	if len(tools.Items) != 1 || tools.Items[0].CallCount != 2 || tools.Items[0].FailureCount != 1 {
		t.Fatalf("tools insights unexpected: %+v", tools.Items)
	}

	agents, err := store.GetInsightsAgents(ctx, filters)
	if err != nil {
		t.Fatalf("GetInsightsAgents() error = %v", err)
	}
	if len(agents.Edges) != 1 || len(agents.Nodes) != 2 {
		t.Fatalf("agents graph unexpected: %+v", agents)
	}

	evDetail, err := store.GetEvidence(ctx, "ev-rec-1")
	if err != nil {
		t.Fatalf("GetEvidence() error = %v", err)
	}
	if !strings.Contains(string(evDetail.RawJSON), "hello") {
		t.Fatalf("GetEvidence raw_json unexpected: %s", evDetail.RawJSON)
	}

	// Cascade deletion: deleting sess-1 removes checkpoint, events, turns, messages, tool_calls, spans.
	if err := store.DeleteSession(ctx, "sess-1"); err != nil {
		t.Fatalf("DeleteSession(sess-1) error = %v", err)
	}
	if _, err := store.GetSession(ctx, "sess-1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
	for _, table := range []string{"index_checkpoints", "agent_events", "agent_turns", "messages", "tool_calls", "activity_spans"} {
		var cnt int
		if err := store.Reader().QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE agent_session_id = 'sess-1'", table)).Scan(&cnt); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if cnt != 0 {
			t.Fatalf("expected 0 remaining rows in %s after cascade delete, got %d", table, cnt)
		}
	}
	// Evidence record had ON DELETE SET NULL on agent_session_id, so it survives while source_id exists.
	evAfter, err := store.GetEvidence(ctx, "ev-rec-1")
	if err != nil {
		t.Fatalf("evidence should survive session delete via ON DELETE SET NULL: %v", err)
	}
	if evAfter.AgentSessionID != nil {
		t.Fatalf("expected evidence agent_session_id=nil after session delete, got %v", *evAfter.AgentSessionID)
	}
	// Deleting local_data_source cascades evidence_records and thread_edges.
	if err := store.DeleteLocalDataSource(ctx, "src-1"); err != nil {
		t.Fatalf("DeleteLocalDataSource(src-1) error = %v", err)
	}
	if _, err := store.GetEvidence(ctx, "ev-rec-1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected evidence cascade deleted with source, got %v", err)
	}
}

func TestSubagentAllowsNativeParentThreadIDWithoutFKFailure(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = store.Close() }()

	ts := model.FormatUTC(time.Date(2026, 11, 27, 4, 0, 0, 0, time.UTC))
	parentThread := "native-parent-thread-not-indexed"
	err = store.WithWriterTx(ctx, func(tx *sql.Tx) error {
		return store.UpsertSessionTx(ctx, tx, model.AgentSession{
			ID:              "sess-orphan-sub",
			Agent:           "claude",
			SourceKind:      "claude_jsonl",
			SourceIdentity:  "sha256:orphan-sub",
			ThreadID:        "agent-123",
			IsSubagent:      true,
			ParentSessionID: &parentThread,
			ParserVersion:   "1",
			IndexStatus:     "indexed",
			CreatedAt:       ts,
			UpdatedAt:       ts,
		})
	})
	if err != nil {
		t.Fatalf("UpsertSessionTx() with unindexed parent thread failed: %v", err)
	}

	got, err := store.GetSession(ctx, "sess-orphan-sub")
	if err != nil || got.ParentSessionID == nil || *got.ParentSessionID != parentThread {
		t.Fatalf("unexpected GetSession result: got=%+v err=%v", got, err)
	}
}

func strPtr(v string) *string { return &v }
func intPtr(v int) *int       { return &v }
