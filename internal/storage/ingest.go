package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/agent-observatory/agent-observatory-go/internal/model"
)

// SessionProjection holds the derived rows rebuilt together for one session.
type SessionProjection struct {
	Turns     []model.AgentTurn
	Messages  []model.Message
	ToolCalls []model.ToolCall
	Spans     []model.ActivitySpan
}

// UpsertWorkspaceTx inserts or updates a workspace keyed by normalized_path.
func (s *Store) UpsertWorkspaceTx(ctx context.Context, tx *sql.Tx, ws model.Workspace) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO workspaces (
			id, path, normalized_path, display_name, git_remote, git_branch,
			is_allowed, workspace_kind, workspace_confidence, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(normalized_path) DO UPDATE SET
			path = excluded.path,
			display_name = excluded.display_name,
			git_remote = COALESCE(excluded.git_remote, workspaces.git_remote),
			git_branch = COALESCE(excluded.git_branch, workspaces.git_branch),
			is_allowed = excluded.is_allowed,
			workspace_kind = excluded.workspace_kind,
			workspace_confidence = excluded.workspace_confidence,
			updated_at = excluded.updated_at
	`,
		ws.ID,
		ws.Path,
		ws.NormalizedPath,
		ws.DisplayName,
		ws.GitRemote,
		ws.GitBranch,
		boolToInt(ws.IsAllowed),
		defaultString(ws.WorkspaceKind, "unknown"),
		defaultString(ws.WorkspaceConfidence, "low"),
		ws.CreatedAt,
		ws.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert workspace %s: %w", ws.NormalizedPath, err)
	}
	return nil
}

// UpsertSessionTx inserts or updates an agent_sessions row keyed by source_identity.
func (s *Store) UpsertSessionTx(ctx context.Context, tx *sql.Tx, sess model.AgentSession) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			id, agent, source_kind, source_identity,
			rollout_path, rollout_real_path, source_path,
			rollout_format, source_format, rollout_compression, source_compression,
			thread_id, workspace_id, cwd, git_branch, originator, cli_version,
			model, thread_name, ai_title, display_title, approval_mode,
			turn_count, total_tokens, input_tokens, cached_input_tokens,
			cache_creation_input_tokens, output_tokens, reasoning_output_tokens,
			is_ongoing, is_archived, is_subagent, parent_session_id, history_base_thread_id,
			parser_version, file_size, file_mtime_ns, file_sha256,
			last_indexed_offset, last_indexed_line, projected_event_index,
			index_status, last_error, start_time, end_time, last_activity_at,
			created_at, updated_at
		) VALUES (
			?, ?, ?, ?,
			?, ?, ?,
			?, ?, ?, ?,
			?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?,
			?, ?, ?, ?,
			?, ?, ?,
			?, ?, ?, ?, ?,
			?, ?, ?, ?,
			?, ?, ?,
			?, ?, ?, ?, ?,
			?, ?
		)
		ON CONFLICT(source_identity) DO UPDATE SET
			agent = excluded.agent,
			source_kind = excluded.source_kind,
			rollout_path = excluded.rollout_path,
			rollout_real_path = excluded.rollout_real_path,
			source_path = excluded.source_path,
			rollout_format = excluded.rollout_format,
			source_format = excluded.source_format,
			rollout_compression = excluded.rollout_compression,
			source_compression = excluded.source_compression,
			thread_id = excluded.thread_id,
			workspace_id = excluded.workspace_id,
			cwd = excluded.cwd,
			git_branch = excluded.git_branch,
			originator = excluded.originator,
			cli_version = excluded.cli_version,
			model = excluded.model,
			thread_name = excluded.thread_name,
			ai_title = excluded.ai_title,
			display_title = excluded.display_title,
			approval_mode = excluded.approval_mode,
			turn_count = excluded.turn_count,
			total_tokens = excluded.total_tokens,
			input_tokens = excluded.input_tokens,
			cached_input_tokens = excluded.cached_input_tokens,
			cache_creation_input_tokens = excluded.cache_creation_input_tokens,
			output_tokens = excluded.output_tokens,
			reasoning_output_tokens = excluded.reasoning_output_tokens,
			is_ongoing = excluded.is_ongoing,
			is_archived = excluded.is_archived,
			is_subagent = excluded.is_subagent,
			parent_session_id = excluded.parent_session_id,
			history_base_thread_id = excluded.history_base_thread_id,
			parser_version = excluded.parser_version,
			file_size = excluded.file_size,
			file_mtime_ns = excluded.file_mtime_ns,
			file_sha256 = excluded.file_sha256,
			last_indexed_offset = excluded.last_indexed_offset,
			last_indexed_line = excluded.last_indexed_line,
			projected_event_index = excluded.projected_event_index,
			index_status = excluded.index_status,
			last_error = excluded.last_error,
			start_time = excluded.start_time,
			end_time = excluded.end_time,
			last_activity_at = excluded.last_activity_at,
			updated_at = excluded.updated_at
	`,
		sess.ID, sess.Agent, sess.SourceKind, sess.SourceIdentity,
		sess.RolloutPath, sess.RolloutRealPath, sess.SourcePath,
		defaultString(sess.RolloutFormat, "jsonl"), defaultString(sess.SourceFormat, "jsonl"),
		sess.RolloutCompression, sess.SourceCompression,
		sess.ThreadID, sess.WorkspaceID, sess.CWD, sess.GitBranch, sess.Originator, sess.CLIVersion,
		sess.Model, sess.ThreadName, sess.AITitle, sess.DisplayTitle, sess.ApprovalMode,
		sess.TurnCount, sess.TotalTokens, sess.InputTokens, sess.CachedInputTokens,
		sess.CacheCreationInputTokens, sess.OutputTokens, sess.ReasoningOutputTokens,
		boolToInt(sess.IsOngoing), boolToInt(sess.IsArchived), boolToInt(sess.IsSubagent),
		sess.ParentSessionID, sess.HistoryBaseThreadID,
		defaultString(sess.ParserVersion, "1"), sess.FileSize, sess.FileMtimeNs, sess.FileSHA256,
		sess.LastIndexedOffset, sess.LastIndexedLine, sess.ProjectedEventIndex,
		defaultString(sess.IndexStatus, "discovered"), sess.LastError,
		sess.StartTime, sess.EndTime, sess.LastActivityAt,
		sess.CreatedAt, sess.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert agent_session %s: %w", sess.ID, err)
	}
	return nil
}

// UpsertCheckpointTx inserts or updates an index_checkpoints row keyed by source_identity.
func (s *Store) UpsertCheckpointTx(ctx context.Context, tx *sql.Tx, cp model.IndexCheckpoint) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO index_checkpoints (
			id, source_kind, source_identity, source_path, real_path,
			agent_session_id, parser_version, status,
			file_size, file_mtime_ns, last_indexed_offset, last_indexed_line,
			cursor_json, fingerprint_json, last_error_kind, last_error,
			last_attempt_at, last_success_at, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(source_identity) DO UPDATE SET
			source_kind = excluded.source_kind,
			source_path = excluded.source_path,
			real_path = excluded.real_path,
			agent_session_id = excluded.agent_session_id,
			parser_version = excluded.parser_version,
			status = excluded.status,
			file_size = excluded.file_size,
			file_mtime_ns = excluded.file_mtime_ns,
			last_indexed_offset = excluded.last_indexed_offset,
			last_indexed_line = excluded.last_indexed_line,
			cursor_json = excluded.cursor_json,
			fingerprint_json = excluded.fingerprint_json,
			last_error_kind = excluded.last_error_kind,
			last_error = excluded.last_error,
			last_attempt_at = excluded.last_attempt_at,
			last_success_at = excluded.last_success_at,
			updated_at = excluded.updated_at
	`,
		cp.ID, cp.SourceKind, cp.SourceIdentity, cp.SourcePath, cp.RealPath,
		cp.AgentSessionID, cp.ParserVersion, defaultString(cp.Status, "discovered"),
		cp.FileSize, cp.FileMtimeNs, cp.LastIndexedOffset, cp.LastIndexedLine,
		defaultJSON(cp.CursorJSON), defaultJSON(cp.FingerprintJSON),
		cp.LastErrorKind, cp.LastError,
		cp.LastAttemptAt, cp.LastSuccessAt, cp.CreatedAt, cp.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert checkpoint %s: %w", cp.SourceIdentity, err)
	}
	return nil
}

// GetCheckpointByIdentity looks up an existing ingestion checkpoint by canonical source identity.
func (s *Store) GetCheckpointByIdentity(ctx context.Context, sourceIdentity string) (model.IndexCheckpoint, error) {
	var cp model.IndexCheckpoint
	var errKind, errMsg, lastAttempt, lastSuccess sql.NullString
	err := s.reader.QueryRowContext(ctx, `
		SELECT
			id, source_kind, source_identity, source_path, real_path,
			agent_session_id, parser_version, status,
			file_size, file_mtime_ns, last_indexed_offset, last_indexed_line,
			cursor_json, fingerprint_json, last_error_kind, last_error,
			last_attempt_at, last_success_at, created_at, updated_at
		FROM index_checkpoints
		WHERE source_identity = ?
	`, sourceIdentity).Scan(
		&cp.ID, &cp.SourceKind, &cp.SourceIdentity, &cp.SourcePath, &cp.RealPath,
		&cp.AgentSessionID, &cp.ParserVersion, &cp.Status,
		&cp.FileSize, &cp.FileMtimeNs, &cp.LastIndexedOffset, &cp.LastIndexedLine,
		&cp.CursorJSON, &cp.FingerprintJSON, &errKind, &errMsg,
		&lastAttempt, &lastSuccess, &cp.CreatedAt, &cp.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return model.IndexCheckpoint{}, ErrNotFound
	}
	if err != nil {
		return model.IndexCheckpoint{}, fmt.Errorf("get checkpoint %s: %w", sourceIdentity, err)
	}
	cp.LastErrorKind = nullStrPtr(errKind)
	cp.LastError = nullStrPtr(errMsg)
	cp.LastAttemptAt = nullStrPtr(lastAttempt)
	cp.LastSuccessAt = nullStrPtr(lastSuccess)
	return cp, nil
}

// DeleteSessionEventsTx deletes all stored raw events for a session prior to a rebuild.
func (s *Store) DeleteSessionEventsTx(ctx context.Context, tx *sql.Tx, sessionID string) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM agent_events WHERE agent_session_id = ?`, sessionID); err != nil {
		return fmt.Errorf("delete session events %s: %w", sessionID, err)
	}
	return nil
}

// InsertEventsTx validates, zstd-compresses, and inserts ordered raw events, returning inserted row IDs.
func (s *Store) InsertEventsTx(ctx context.Context, tx *sql.Tx, sessionID string, events []model.AgentEvent) ([]int64, error) {
	if len(events) == 0 {
		return nil, nil
	}
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO agent_events (
			agent_session_id, event_index, line_number, byte_offset,
			event_timestamp, top_level_type, payload_type, normalized_type,
			raw_json_zstd, raw_hash, is_known, parser_version, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return nil, fmt.Errorf("prepare insert agent_events: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	ids := make([]int64, 0, len(events))
	for _, ev := range events {
		compressed, hash, compErr := CompressJSON(ev.RawJSON)
		if compErr != nil {
			return nil, fmt.Errorf("compress event %d for session %s: %w", ev.EventIndex, sessionID, compErr)
		}
		if ev.RawHash != "" {
			hash = ev.RawHash
		}
		res, execErr := stmt.ExecContext(ctx,
			sessionID,
			ev.EventIndex,
			ev.LineNumber,
			ev.ByteOffset,
			ev.EventTimestamp,
			ev.TopLevelType,
			ev.PayloadType,
			ev.NormalizedType,
			compressed,
			hash,
			boolToInt(ev.IsKnown),
			defaultString(ev.ParserVersion, "1"),
			ev.CreatedAt,
		)
		if execErr != nil {
			return nil, fmt.Errorf("insert event %d for session %s: %w", ev.EventIndex, sessionID, execErr)
		}
		rowID, idErr := res.LastInsertId()
		if idErr != nil {
			return nil, fmt.Errorf("last insert id for event %d: %w", ev.EventIndex, idErr)
		}
		ids = append(ids, rowID)
	}
	return ids, nil
}

// ReplaceSessionProjectionTx atomically replaces turns, messages, tool_calls, and rollout spans for a session.
func (s *Store) ReplaceSessionProjectionTx(ctx context.Context, tx *sql.Tx, sessionID string, proj SessionProjection) error {
	// Delete child tables before agent_turns to satisfy foreign-key ordering cleanly.
	for _, query := range []string{
		`DELETE FROM activity_spans WHERE agent_session_id = ? AND source_kind = 'rollout'`,
		`DELETE FROM messages WHERE agent_session_id = ?`,
		`DELETE FROM tool_calls WHERE agent_session_id = ?`,
		`DELETE FROM agent_turns WHERE agent_session_id = ?`,
	} {
		if _, err := tx.ExecContext(ctx, query, sessionID); err != nil {
			return fmt.Errorf("clear session projection %s: %w", sessionID, err)
		}
	}

	if err := insertTurnsTx(ctx, tx, sessionID, proj.Turns); err != nil {
		return err
	}
	if err := insertMessagesTx(ctx, tx, sessionID, proj.Messages); err != nil {
		return err
	}
	if err := insertToolCallsTx(ctx, tx, sessionID, proj.ToolCalls); err != nil {
		return err
	}
	if err := insertSpansTx(ctx, tx, sessionID, proj.Spans); err != nil {
		return err
	}
	return nil
}

func insertTurnsTx(ctx context.Context, tx *sql.Tx, sessionID string, turns []model.AgentTurn) error {
	if len(turns) == 0 {
		return nil
	}
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO agent_turns (
			id, agent, agent_session_id, turn_index, turn_id, status,
			started_at, completed_at, input_tokens, cached_input_tokens,
			cache_creation_input_tokens, output_tokens, reasoning_output_tokens, total_tokens,
			duration_ms, first_agent_latency_ms, active_tool_ms,
			validation_status, outcome, metric_confidence, error_kind, error_message,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("prepare insert agent_turns: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	for _, t := range turns {
		if _, err := stmt.ExecContext(ctx,
			t.ID, defaultString(t.Agent, "codex"), sessionID, t.TurnIndex, t.TurnID, defaultString(t.Status, "unknown"),
			t.StartedAt, t.CompletedAt, t.InputTokens, t.CachedInputTokens,
			t.CacheCreationInputTokens, t.OutputTokens, t.ReasoningOutputTokens, t.TotalTokens,
			t.DurationMS, t.FirstAgentLatencyMS, t.ActiveToolMS,
			defaultString(t.ValidationStatus, "unknown"), defaultString(t.Outcome, "unknown"),
			defaultString(t.MetricConfidence, "derived"), t.ErrorKind, t.ErrorMessage,
			t.CreatedAt, t.UpdatedAt,
		); err != nil {
			return fmt.Errorf("insert turn %d for %s: %w", t.TurnIndex, sessionID, err)
		}
	}
	return nil
}

func insertMessagesTx(ctx context.Context, tx *sql.Tx, sessionID string, msgs []model.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO messages (
			id, agent_session_id, turn_id, event_id, role, channel, sequence,
			text, markdown_text, copy_text, message_timestamp, is_visible, is_system_generated, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("prepare insert messages: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	for _, m := range msgs {
		if _, err := stmt.ExecContext(ctx,
			m.ID, sessionID, m.TurnID, m.EventID, m.Role, m.Channel, m.Sequence,
			m.Text, m.MarkdownText, m.CopyText, m.Timestamp,
			boolToInt(m.IsVisible), boolToInt(m.IsSystemGenerated), m.CreatedAt,
		); err != nil {
			return fmt.Errorf("insert message %d for %s: %w", m.Sequence, sessionID, err)
		}
	}
	return nil
}

func insertToolCallsTx(ctx context.Context, tx *sql.Tx, sessionID string, tools []model.ToolCall) error {
	if len(tools) == 0 {
		return nil
	}
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO tool_calls (
			id, agent, agent_session_id, turn_id, event_id, call_id, tool_name, tool_kind,
			arguments_json, command_text, output_text, exit_code, status, is_mutating,
			started_at, completed_at, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("prepare insert tool_calls: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	for _, tc := range tools {
		var argsJSON *string
		if len(tc.ArgumentsJSON) > 0 {
			raw := string(tc.ArgumentsJSON)
			argsJSON = &raw
		}
		if _, err := stmt.ExecContext(ctx,
			tc.ID, defaultString(tc.Agent, "codex"), sessionID, tc.TurnID, tc.EventID,
			tc.CallID, tc.ToolName, defaultString(tc.ToolKind, "unknown"),
			argsJSON, tc.CommandText, tc.OutputText, tc.ExitCode,
			defaultString(tc.Status, "pending"), boolToInt(tc.IsMutating),
			tc.StartedAt, tc.CompletedAt, tc.CreatedAt,
		); err != nil {
			return fmt.Errorf("insert tool_call %s for %s: %w", tc.CallID, sessionID, err)
		}
	}
	return nil
}

func insertSpansTx(ctx context.Context, tx *sql.Tx, sessionID string, spans []model.ActivitySpan) error {
	if len(spans) == 0 {
		return nil
	}
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO activity_spans (
			id, agent, agent_session_id, evidence_id, source_kind, source_record_id,
			thread_id, turn_id, span_type, name, status,
			started_at, completed_at, duration_ms, confidence, metadata_json, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("prepare insert activity_spans: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	for _, sp := range spans {
		if _, err := stmt.ExecContext(ctx,
			sp.ID, defaultString(sp.Agent, "codex"), sessionID, sp.EvidenceID,
			defaultString(sp.SourceKind, "rollout"), sp.SourceRecordID,
			sp.ThreadID, sp.TurnID, sp.SpanType, sp.Name, defaultString(sp.Status, "unknown"),
			sp.StartedAt, sp.CompletedAt, sp.DurationMS,
			defaultString(sp.Confidence, "derived"), defaultJSON(string(sp.MetadataJSON)), sp.CreatedAt,
		); err != nil {
			return fmt.Errorf("insert activity_span %s for %s: %w", sp.ID, sessionID, err)
		}
	}
	return nil
}

// UpsertLocalDataSourceTx inserts or updates a local_data_sources entry keyed by source_path.
func (s *Store) UpsertLocalDataSourceTx(ctx context.Context, tx *sql.Tx, src model.LocalDataSource) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO local_data_sources (
			id, agent, source_kind, source_identity, source_path, source_format, parser_version,
			status, record_count, byte_size,
			coverage_started_at, coverage_ended_at, backfill_cutoff_at,
			last_attempt_at, last_success_at, last_error_kind, last_error,
			cursor_json, fingerprint_json, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(source_path) DO UPDATE SET
			agent = excluded.agent,
			source_kind = excluded.source_kind,
			source_identity = excluded.source_identity,
			source_format = excluded.source_format,
			parser_version = excluded.parser_version,
			status = excluded.status,
			record_count = excluded.record_count,
			byte_size = excluded.byte_size,
			coverage_started_at = excluded.coverage_started_at,
			coverage_ended_at = excluded.coverage_ended_at,
			backfill_cutoff_at = excluded.backfill_cutoff_at,
			last_attempt_at = excluded.last_attempt_at,
			last_success_at = excluded.last_success_at,
			last_error_kind = excluded.last_error_kind,
			last_error = excluded.last_error,
			cursor_json = excluded.cursor_json,
			fingerprint_json = excluded.fingerprint_json,
			updated_at = excluded.updated_at
	`,
		src.ID, defaultString(src.Agent, "codex"), src.SourceKind, src.SourceIdentity,
		src.SourcePath, src.SourceFormat, src.ParserVersion,
		defaultString(src.Status, "discovered"), src.RecordCount, src.ByteSize,
		src.CoverageStartedAt, src.CoverageEndedAt, src.BackfillCutoffAt,
		src.LastAttemptAt, src.LastSuccessAt, src.LastErrorKind, src.LastError,
		defaultJSON(src.CursorJSON), defaultJSON(src.FingerprintJSON),
		src.CreatedAt, src.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert local_data_source %s: %w", src.SourcePath, err)
	}
	return nil
}

// UpsertEvidenceRecordTx stores an evidence record, compressing RawJSON with zstd when present.
func (s *Store) UpsertEvidenceRecordTx(ctx context.Context, tx *sql.Tx, rec model.EvidenceRecord) error {
	var rawZstd []byte
	rawHash := rec.RawHash
	if len(rec.RawJSON) > 0 {
		compressed, digest, err := CompressJSON(rec.RawJSON)
		if err != nil {
			return fmt.Errorf("compress evidence json %s: %w", rec.ID, err)
		}
		rawZstd = compressed
		if rawHash == "" {
			rawHash = digest
		}
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO evidence_records (
			id, source_id, source_record_id, agent_session_id, agent_event_id,
			thread_id, turn_id, category, level, target, recorded_at,
			summary, raw_text, raw_json_zstd, raw_hash,
			source_locator_json, metadata_json, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(source_id, source_record_id) DO UPDATE SET
			agent_session_id = excluded.agent_session_id,
			agent_event_id = excluded.agent_event_id,
			thread_id = excluded.thread_id,
			turn_id = excluded.turn_id,
			category = excluded.category,
			level = excluded.level,
			target = excluded.target,
			recorded_at = excluded.recorded_at,
			summary = excluded.summary,
			raw_text = excluded.raw_text,
			raw_json_zstd = excluded.raw_json_zstd,
			raw_hash = excluded.raw_hash,
			source_locator_json = excluded.source_locator_json,
			metadata_json = excluded.metadata_json
	`,
		rec.ID, rec.SourceID, rec.SourceRecordID, rec.AgentSessionID, rec.AgentEventID,
		rec.ThreadID, rec.TurnID, rec.Category, rec.Level, rec.Target, rec.RecordedAt,
		rec.Summary, rec.RawText, rawZstd, rawHash,
		defaultJSON(string(rec.SourceLocatorJSON)), defaultJSON(string(rec.MetadataJSON)), rec.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert evidence_record %s: %w", rec.ID, err)
	}
	return nil
}

// UpsertThreadEdgeTx inserts or updates a parent/child thread edge.
func (s *Store) UpsertThreadEdgeTx(ctx context.Context, tx *sql.Tx, edge model.ThreadEdge) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO thread_edges (
			id, source_id, parent_thread_id, child_thread_id, status, event_time, metadata_json, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(parent_thread_id, child_thread_id) DO UPDATE SET
			source_id = excluded.source_id,
			status = excluded.status,
			event_time = excluded.event_time,
			metadata_json = excluded.metadata_json
	`,
		edge.ID, edge.SourceID, edge.ParentThreadID, edge.ChildThreadID,
		edge.Status, edge.EventTime, defaultJSON(string(edge.MetadataJSON)), edge.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert thread_edge %s->%s: %w", edge.ParentThreadID, edge.ChildThreadID, err)
	}
	return nil
}

// DeleteSession removes an agent session and cascades to its child tables.
func (s *Store) DeleteSession(ctx context.Context, sessionID string) error {
	return s.WithWriterTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `DELETE FROM agent_sessions WHERE id = ?`, sessionID)
		if err != nil {
			return fmt.Errorf("delete session %s: %w", sessionID, err)
		}
		affected, _ := res.RowsAffected()
		if affected == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// DeleteLocalDataSource removes a local data source and cascades to evidence_records and thread_edges.
func (s *Store) DeleteLocalDataSource(ctx context.Context, sourceID string) error {
	return s.WithWriterTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `DELETE FROM local_data_sources WHERE id = ?`, sourceID)
		if err != nil {
			return fmt.Errorf("delete local_data_source %s: %w", sourceID, err)
		}
		affected, _ := res.RowsAffected()
		if affected == 0 {
			return ErrNotFound
		}
		return nil
	})
}
