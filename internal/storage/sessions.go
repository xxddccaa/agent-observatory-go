package storage

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/agent-observatory/agent-observatory-go/internal/model"
)

const sessionActivityExpr = `coalesce(s.last_activity_at, s.end_time, s.start_time, s.updated_at)`

// ListSessions returns cursor-paginated sessions ordered by activity descending.
func (s *Store) ListSessions(ctx context.Context, filter model.SessionListFilter) (model.SessionListResult, error) {
	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	} else if limit > 200 {
		limit = 200
	}

	var clauses []string
	var args []any

	if !filter.IncludeSubagents {
		clauses = append(clauses, `s.is_subagent = 0`)
	}
	if filter.WorkspaceID != "" {
		clauses = append(clauses, `s.workspace_id = ?`)
		args = append(args, filter.WorkspaceID)
	}
	if filter.IsOngoing != nil {
		clauses = append(clauses, `s.is_ongoing = ?`)
		args = append(args, boolToInt(*filter.IsOngoing))
	}
	if filter.Agent != "" {
		clauses = append(clauses, `s.agent = ?`)
		args = append(args, filter.Agent)
	}
	if filter.Cursor != "" {
		cursorTime, cursorID, err := decodeSessionCursor(filter.Cursor)
		if err != nil {
			return model.SessionListResult{}, err
		}
		clauses = append(clauses, fmt.Sprintf(`(%s < ? OR (%s = ? AND s.id < ?))`, sessionActivityExpr, sessionActivityExpr))
		args = append(args, cursorTime, cursorTime, cursorID)
	}
	if q := strings.TrimSpace(filter.Query); q != "" {
		pattern := "%" + EscapeLike(strings.ToLower(q)) + "%"
		clauses = append(clauses, `(
			LOWER(s.thread_id) LIKE ? ESCAPE '\' OR
			LOWER(COALESCE(s.cwd, '')) LIKE ? ESCAPE '\' OR
			LOWER(COALESCE(s.ai_title, '')) LIKE ? ESCAPE '\' OR
			LOWER(COALESCE(s.display_title, '')) LIKE ? ESCAPE '\' OR
			LOWER(s.source_kind) LIKE ? ESCAPE '\'
		)`)
		args = append(args, pattern, pattern, pattern, pattern, pattern)
	}

	whereSQL := ""
	if len(clauses) > 0 {
		whereSQL = "WHERE " + strings.Join(clauses, " AND ")
	}

	querySQL := fmt.Sprintf(`
		SELECT
			s.id, s.thread_id, s.rollout_path, s.cwd,
			s.workspace_id, w.workspace_kind, w.workspace_confidence,
			s.ai_title, s.display_title, s.agent, s.is_subagent, s.parent_session_id,
			s.source_kind, s.source_path, s.model, s.turn_count, s.total_tokens,
			s.is_ongoing, s.updated_at,
			coalesce(s.last_activity_at, s.end_time, s.start_time) AS activity_at,
			%s AS sort_activity_at,
			s.index_status
		FROM agent_sessions s
		LEFT JOIN workspaces w ON w.id = s.workspace_id
		%s
		ORDER BY sort_activity_at DESC, s.id DESC
		LIMIT ?
	`, sessionActivityExpr, whereSQL)
	args = append(args, limit+1)

	rows, err := s.reader.QueryContext(ctx, querySQL, args...)
	if err != nil {
		return model.SessionListResult{}, fmt.Errorf("list sessions: %w", err)
	}
	defer func() { _ = rows.Close() }()

	items := make([]model.SessionListItem, 0, limit)
	sortKeys := make([]string, 0, limit)
	hasMore := false

	for rows.Next() {
		var (
			item           model.SessionListItem
			rolloutPath    sql.NullString
			cwd            sql.NullString
			workspaceID    sql.NullString
			wsKind         sql.NullString
			wsConf         sql.NullString
			aiTitle        sql.NullString
			displayTitle   sql.NullString
			parentID       sql.NullString
			sourcePath     sql.NullString
			modelName      sql.NullString
			activityAt     sql.NullString
			sortActivityAt string
			isSubagentInt  int
			isOngoingInt   int
		)
		if err := rows.Scan(
			&item.ID, &item.ThreadID, &rolloutPath, &cwd,
			&workspaceID, &wsKind, &wsConf,
			&aiTitle, &displayTitle, &item.Agent, &isSubagentInt, &parentID,
			&item.SourceKind, &sourcePath, &modelName, &item.TurnCount, &item.TotalTokens,
			&isOngoingInt, &item.UpdatedAt, &activityAt, &sortActivityAt, &item.IndexStatus,
		); err != nil {
			return model.SessionListResult{}, fmt.Errorf("scan session row: %w", err)
		}
		if len(items) >= limit {
			hasMore = true
			break
		}
		item.RolloutPath = nullStrPtr(rolloutPath)
		item.CWD = nullStrPtr(cwd)
		item.WorkspaceID = nullStrPtr(workspaceID)
		item.WorkspaceKind = nullStrPtr(wsKind)
		item.WorkspaceConfidence = nullStrPtr(wsConf)
		item.AITitle = nullStrPtr(aiTitle)
		item.DisplayTitle = nullStrPtr(displayTitle)
		item.IsSubagent = isSubagentInt != 0
		item.ParentSessionID = nullStrPtr(parentID)
		item.SourcePath = nullStrPtr(sourcePath)
		item.Model = nullStrPtr(modelName)
		item.IsOngoing = isOngoingInt != 0
		item.ActivityAt = nullStrPtr(activityAt)
		item.LaunchSources = []string{}

		items = append(items, item)
		sortKeys = append(sortKeys, sortActivityAt)
	}
	if err := rows.Err(); err != nil {
		return model.SessionListResult{}, fmt.Errorf("iterate sessions: %w", err)
	}

	var nextCursor *string
	if hasMore && len(items) > 0 {
		lastIdx := len(items) - 1
		encoded := encodeSessionCursor(sortKeys[lastIdx], items[lastIdx].ID)
		nextCursor = &encoded
	}
	return model.SessionListResult{
		Sessions:   items,
		NextCursor: nextCursor,
	}, nil
}

// GetSession returns detail metadata for one indexed session.
func (s *Store) GetSession(ctx context.Context, sessionID string) (model.SessionDetail, error) {
	var (
		out          model.SessionDetail
		rolloutPath  sql.NullString
		cwd          sql.NullString
		activityAt   sql.NullString
		modelName    sql.NullString
		displayTitle sql.NullString
		aiTitle      sql.NullString
		parentID     sql.NullString
		sourcePath   sql.NullString
		isSubagent   int
		isOngoing    int
	)
	err := s.reader.QueryRowContext(ctx, `
		SELECT
			id, thread_id, rollout_path, cwd,
			coalesce(last_activity_at, end_time, start_time) AS activity_at,
			model, turn_count, total_tokens, display_title, ai_title,
			agent, is_subagent, parent_session_id, source_kind, source_path,
			is_ongoing, updated_at
		FROM agent_sessions
		WHERE id = ?
	`, sessionID).Scan(
		&out.ID, &out.ThreadID, &rolloutPath, &cwd, &activityAt,
		&modelName, &out.TurnCount, &out.TotalTokens, &displayTitle, &aiTitle,
		&out.Agent, &isSubagent, &parentID, &out.SourceKind, &sourcePath,
		&isOngoing, &out.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return model.SessionDetail{}, ErrNotFound
	}
	if err != nil {
		return model.SessionDetail{}, fmt.Errorf("get session %s: %w", sessionID, err)
	}
	out.RolloutPath = nullStrPtr(rolloutPath)
	out.CWD = nullStrPtr(cwd)
	out.ActivityAt = nullStrPtr(activityAt)
	out.Model = nullStrPtr(modelName)
	out.DisplayTitle = nullStrPtr(displayTitle)
	out.AITitle = nullStrPtr(aiTitle)
	out.IsSubagent = isSubagent != 0
	out.ParentSessionID = nullStrPtr(parentID)
	out.SourcePath = nullStrPtr(sourcePath)
	out.IsOngoing = isOngoing != 0
	out.LaunchSources = []string{}
	return out, nil
}

func (s *Store) ensureSessionExists(ctx context.Context, sessionID string) error {
	var exists int
	err := s.reader.QueryRowContext(ctx, `SELECT 1 FROM agent_sessions WHERE id = ?`, sessionID).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("check session %s: %w", sessionID, err)
	}
	return nil
}

// ListSessionMessages returns ordered projected messages for a session.
func (s *Store) ListSessionMessages(ctx context.Context, sessionID string) ([]model.Message, error) {
	if err := s.ensureSessionExists(ctx, sessionID); err != nil {
		return nil, err
	}
	rows, err := s.reader.QueryContext(ctx, `
		SELECT
			id, turn_id, event_id, role, channel, sequence,
			text, markdown_text, copy_text, message_timestamp,
			is_visible, is_system_generated, created_at
		FROM messages
		WHERE agent_session_id = ?
		ORDER BY sequence ASC
	`, sessionID)
	if err != nil {
		return nil, fmt.Errorf("list messages for %s: %w", sessionID, err)
	}
	defer func() { _ = rows.Close() }()

	out := []model.Message{}
	for rows.Next() {
		var (
			m          model.Message
			turnID     sql.NullString
			eventID    sql.NullInt64
			mdText     sql.NullString
			ts         sql.NullString
			visibleInt int
			sysGenInt  int
		)
		if err := rows.Scan(
			&m.ID, &turnID, &eventID, &m.Role, &m.Channel, &m.Sequence,
			&m.Text, &mdText, &m.CopyText, &ts,
			&visibleInt, &sysGenInt, &m.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan message for %s: %w", sessionID, err)
		}
		m.AgentSessionID = sessionID
		m.TurnID = nullStrPtr(turnID)
		m.EventID = nullInt64Ptr(eventID)
		m.MarkdownText = nullStrPtr(mdText)
		m.Timestamp = nullStrPtr(ts)
		m.IsVisible = visibleInt != 0
		m.IsSystemGenerated = sysGenInt != 0
		out = append(out, m)
	}
	return out, rows.Err()
}

// ListSessionEvents returns all raw events for a session with zstd payloads decompressed.
func (s *Store) ListSessionEvents(ctx context.Context, sessionID string) ([]model.AgentEvent, error) {
	if err := s.ensureSessionExists(ctx, sessionID); err != nil {
		return nil, err
	}
	rows, err := s.reader.QueryContext(ctx, `
		SELECT
			id, event_index, line_number, byte_offset, event_timestamp,
			top_level_type, payload_type, normalized_type,
			raw_json_zstd, raw_hash, is_known, parser_version, created_at
		FROM agent_events
		WHERE agent_session_id = ?
		ORDER BY event_index ASC
	`, sessionID)
	if err != nil {
		return nil, fmt.Errorf("list events for %s: %w", sessionID, err)
	}
	defer func() { _ = rows.Close() }()

	out := []model.AgentEvent{}
	for rows.Next() {
		var (
			ev         model.AgentEvent
			ts         sql.NullString
			topType    sql.NullString
			payType    sql.NullString
			compressed []byte
			knownInt   int
		)
		if err := rows.Scan(
			&ev.ID, &ev.EventIndex, &ev.LineNumber, &ev.ByteOffset, &ts,
			&topType, &payType, &ev.NormalizedType,
			&compressed, &ev.RawHash, &knownInt, &ev.ParserVersion, &ev.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan event for %s: %w", sessionID, err)
		}
		rawJSON, decErr := DecompressJSON(compressed)
		if decErr != nil {
			return nil, fmt.Errorf("decode event %d for %s: %w", ev.EventIndex, sessionID, decErr)
		}
		ev.AgentSessionID = sessionID
		ev.EventTimestamp = nullStrPtr(ts)
		ev.TopLevelType = nullStrPtr(topType)
		ev.PayloadType = nullStrPtr(payType)
		ev.RawJSON = rawJSON
		ev.IsKnown = knownInt != 0
		out = append(out, ev)
	}
	return out, rows.Err()
}

// ListSessionTurns returns ordered projected turns for a session.
func (s *Store) ListSessionTurns(ctx context.Context, sessionID string) ([]model.AgentTurn, error) {
	if err := s.ensureSessionExists(ctx, sessionID); err != nil {
		return nil, err
	}
	rows, err := s.reader.QueryContext(ctx, `
		SELECT
			id, agent, turn_index, turn_id, status, started_at, completed_at,
			input_tokens, cached_input_tokens, cache_creation_input_tokens,
			output_tokens, reasoning_output_tokens, total_tokens,
			duration_ms, first_agent_latency_ms, active_tool_ms,
			validation_status, outcome, metric_confidence, error_kind, error_message,
			created_at, updated_at
		FROM agent_turns
		WHERE agent_session_id = ?
		ORDER BY turn_index ASC
	`, sessionID)
	if err != nil {
		return nil, fmt.Errorf("list turns for %s: %w", sessionID, err)
	}
	defer func() { _ = rows.Close() }()

	out := []model.AgentTurn{}
	for rows.Next() {
		var (
			t          model.AgentTurn
			turnID     sql.NullString
			startedAt  sql.NullString
			compAt     sql.NullString
			durMS      sql.NullInt64
			firstMS    sql.NullInt64
			toolMS     sql.NullInt64
			errKind    sql.NullString
			errMsg     sql.NullString
		)
		if err := rows.Scan(
			&t.ID, &t.Agent, &t.TurnIndex, &turnID, &t.Status, &startedAt, &compAt,
			&t.InputTokens, &t.CachedInputTokens, &t.CacheCreationInputTokens,
			&t.OutputTokens, &t.ReasoningOutputTokens, &t.TotalTokens,
			&durMS, &firstMS, &toolMS,
			&t.ValidationStatus, &t.Outcome, &t.MetricConfidence, &errKind, &errMsg,
			&t.CreatedAt, &t.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan turn for %s: %w", sessionID, err)
		}
		t.AgentSessionID = sessionID
		t.TurnID = nullStrPtr(turnID)
		t.StartedAt = nullStrPtr(startedAt)
		t.CompletedAt = nullStrPtr(compAt)
		t.DurationMS = nullInt64Ptr(durMS)
		t.FirstAgentLatencyMS = nullInt64Ptr(firstMS)
		t.ActiveToolMS = nullInt64Ptr(toolMS)
		t.ErrorKind = nullStrPtr(errKind)
		t.ErrorMessage = nullStrPtr(errMsg)
		out = append(out, t)
	}
	return out, rows.Err()
}

// ListSessionTools returns ordered projected tool calls for a session.
func (s *Store) ListSessionTools(ctx context.Context, sessionID string) ([]model.ToolCall, error) {
	if err := s.ensureSessionExists(ctx, sessionID); err != nil {
		return nil, err
	}
	rows, err := s.reader.QueryContext(ctx, `
		SELECT
			id, agent, turn_id, event_id, call_id, tool_name, tool_kind,
			arguments_json, command_text, output_text, exit_code,
			status, is_mutating, started_at, completed_at, created_at
		FROM tool_calls
		WHERE agent_session_id = ?
		ORDER BY coalesce(started_at, created_at) ASC, id ASC
	`, sessionID)
	if err != nil {
		return nil, fmt.Errorf("list tools for %s: %w", sessionID, err)
	}
	defer func() { _ = rows.Close() }()

	out := []model.ToolCall{}
	for rows.Next() {
		var (
			tc          model.ToolCall
			turnID      sql.NullString
			eventID     sql.NullInt64
			argsJSON    sql.NullString
			cmdText     sql.NullString
			outText     sql.NullString
			exitCode    sql.NullInt64
			mutatingInt int
			startedAt   sql.NullString
			completedAt sql.NullString
		)
		if err := rows.Scan(
			&tc.ID, &tc.Agent, &turnID, &eventID, &tc.CallID, &tc.ToolName, &tc.ToolKind,
			&argsJSON, &cmdText, &outText, &exitCode,
			&tc.Status, &mutatingInt, &startedAt, &completedAt, &tc.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan tool_call for %s: %w", sessionID, err)
		}
		tc.AgentSessionID = sessionID
		tc.TurnID = nullStrPtr(turnID)
		tc.EventID = nullInt64Ptr(eventID)
		if argsJSON.Valid && json.Valid([]byte(argsJSON.String)) {
			tc.ArgumentsJSON = json.RawMessage(argsJSON.String)
		}
		tc.CommandText = nullStrPtr(cmdText)
		tc.OutputText = nullStrPtr(outText)
		tc.ExitCode = nullIntPtr(exitCode)
		tc.IsMutating = mutatingInt != 0
		tc.StartedAt = nullStrPtr(startedAt)
		tc.CompletedAt = nullStrPtr(completedAt)
		out = append(out, tc)
	}
	return out, rows.Err()
}

// SearchSession finds case-insensitive literal matches across messages and tool_calls.
func (s *Store) SearchSession(ctx context.Context, sessionID, query string, limit int) ([]model.SearchMatch, error) {
	if err := s.ensureSessionExists(ctx, sessionID); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 100
	} else if limit > 500 {
		limit = 500
	}
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return []model.SearchMatch{}, nil
	}
	pattern := "%" + EscapeLike(strings.ToLower(trimmed)) + "%"
	matches := make([]model.SearchMatch, 0, min(limit, 32))

	msgRows, err := s.reader.QueryContext(ctx, `
		SELECT id, sequence, text
		FROM messages
		WHERE agent_session_id = ? AND LOWER(text) LIKE ? ESCAPE '\'
		ORDER BY sequence ASC
		LIMIT ?
	`, sessionID, pattern, limit)
	if err != nil {
		return nil, fmt.Errorf("search session messages %s: %w", sessionID, err)
	}
	for msgRows.Next() {
		var id, text string
		var seq int
		if scanErr := msgRows.Scan(&id, &seq, &text); scanErr != nil {
			_ = msgRows.Close()
			return nil, scanErr
		}
		matches = append(matches, model.SearchMatch{
			Kind:    "message",
			ID:      id,
			Ref:     fmt.Sprintf("%d", seq),
			Snippet: makeSnippet(text, trimmed, 80),
		})
	}
	if err := msgRows.Err(); err != nil {
		_ = msgRows.Close()
		return nil, err
	}
	_ = msgRows.Close()

	if len(matches) >= limit {
		return matches[:limit], nil
	}

	toolRows, err := s.reader.QueryContext(ctx, `
		SELECT id, call_id, command_text, output_text
		FROM tool_calls
		WHERE agent_session_id = ? AND (
			LOWER(COALESCE(command_text, '')) LIKE ? ESCAPE '\' OR
			LOWER(COALESCE(output_text, '')) LIKE ? ESCAPE '\'
		)
		ORDER BY coalesce(started_at, created_at) ASC, id ASC
		LIMIT ?
	`, sessionID, pattern, pattern, limit-len(matches))
	if err != nil {
		return nil, fmt.Errorf("search session tools %s: %w", sessionID, err)
	}
	defer func() { _ = toolRows.Close() }()

	lowQuery := strings.ToLower(trimmed)
	for toolRows.Next() {
		var id, callID string
		var cmdText, outText sql.NullString
		if scanErr := toolRows.Scan(&id, &callID, &cmdText, &outText); scanErr != nil {
			return nil, scanErr
		}
		source := cmdText.String
		if !strings.Contains(strings.ToLower(source), lowQuery) {
			source = outText.String
		}
		matches = append(matches, model.SearchMatch{
			Kind:    "tool",
			ID:      id,
			Ref:     callID,
			Snippet: makeSnippet(source, trimmed, 80),
		})
	}
	return matches, toolRows.Err()
}

func makeSnippet(text, query string, radius int) string {
	if text == "" {
		return ""
	}
	runes := []rune(text)
	lowText := strings.ToLower(text)
	lowQuery := strings.ToLower(query)
	byteIdx := strings.Index(lowText, lowQuery)
	if byteIdx < 0 {
		if len(runes) > radius*2 {
			runes = runes[:radius*2]
		}
		return collapseWhitespace(string(runes))
	}
	runeStart := len([]rune(text[:byteIdx]))
	queryRunes := len([]rune(query))
	start := max(0, runeStart-radius)
	end := min(len(runes), runeStart+queryRunes+radius)
	snippet := collapseWhitespace(string(runes[start:end]))
	if start > 0 {
		snippet = "..." + snippet
	}
	if end < len(runes) {
		snippet += "..."
	}
	return snippet
}

func collapseWhitespace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func encodeSessionCursor(activityAt, sessionID string) string {
	raw, _ := json.Marshal([]string{activityAt, sessionID})
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeSessionCursor(encoded string) (string, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return "", "", ErrInvalidCursor
	}
	var pair []string
	if err := json.Unmarshal(raw, &pair); err != nil || len(pair) != 2 {
		return "", "", ErrInvalidCursor
	}
	parsed, err := model.ParseUTC(pair[0])
	if err != nil {
		return "", "", ErrInvalidCursor
	}
	if pair[1] == "" {
		return "", "", ErrInvalidCursor
	}
	return model.FormatUTC(parsed), pair[1], nil
}
