package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/agent-observatory/agent-observatory-go/internal/model"
)

const (
	maxInsightsTimeseriesPoints = 500
	maxInsightsAgentEdges       = 2000
	insightsTurnActivityExpr    = `coalesce(t.completed_at, t.started_at, t.created_at)`
	insightsToolActivityExpr    = `coalesce(tc.started_at, tc.completed_at, tc.created_at)`
)

var confidenceLevels = []string{"exact", "derived", "inferred"}

// GetInsightsOverview returns session/turn/token totals, quality breakdown, and per-agent comparison.
func (s *Store) GetInsightsOverview(ctx context.Context, filters model.InsightFilters) (model.InsightsOverview, error) {
	sinceStr, untilStr := model.FormatUTC(filters.Since), model.FormatUTC(filters.Until)
	turnWhere, turnArgs := buildTurnFilters(filters, sinceStr, untilStr)

	var totals model.OverviewTotals
	err := s.reader.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT
			COUNT(DISTINCT t.agent_session_id),
			COUNT(t.id),
			COALESCE(SUM(t.total_tokens), 0),
			COALESCE(SUM(t.input_tokens), 0),
			COALESCE(SUM(t.cached_input_tokens), 0),
			COALESCE(SUM(t.output_tokens), 0),
			COALESCE(SUM(t.reasoning_output_tokens), 0)
		FROM agent_turns t
		JOIN agent_sessions s ON s.id = t.agent_session_id
		WHERE %s
	`, turnWhere), turnArgs...).Scan(
		&totals.SessionCount, &totals.TurnCount, &totals.TotalTokens,
		&totals.InputTokens, &totals.CachedInputTokens,
		&totals.OutputTokens, &totals.ReasoningOutputTokens,
	)
	if err != nil {
		return model.InsightsOverview{}, fmt.Errorf("insights overview totals: %w", err)
	}

	outcomes, err := s.groupTurnCounts(ctx, "t.outcome", turnWhere, turnArgs)
	if err != nil {
		return model.InsightsOverview{}, err
	}
	validations, err := s.groupTurnCounts(ctx, "t.validation_status", turnWhere, turnArgs)
	if err != nil {
		return model.InsightsOverview{}, err
	}
	rawConf, err := s.groupTurnCounts(ctx, "t.metric_confidence", turnWhere, turnArgs)
	if err != nil {
		return model.InsightsOverview{}, err
	}

	toolWhere, toolArgs := buildToolFilters(filters, sinceStr, untilStr)
	toolRows, err := s.reader.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			tc.agent,
			COUNT(tc.id),
			COALESCE(SUM(CASE WHEN tc.status IN ('failed', 'error') OR (tc.exit_code IS NOT NULL AND tc.exit_code != 0) THEN 1 ELSE 0 END), 0)
		FROM tool_calls tc
		JOIN agent_sessions s ON s.id = tc.agent_session_id
		WHERE %s
		GROUP BY tc.agent
	`, toolWhere), toolArgs...)
	if err != nil {
		return model.InsightsOverview{}, fmt.Errorf("insights overview tool counts: %w", err)
	}
	toolByAgent := make(map[string][2]int)
	for toolRows.Next() {
		var agent string
		var calls, failures int
		if scanErr := toolRows.Scan(&agent, &calls, &failures); scanErr != nil {
			_ = toolRows.Close()
			return model.InsightsOverview{}, scanErr
		}
		toolByAgent[agent] = [2]int{calls, failures}
	}
	_ = toolRows.Close()

	agentRows, err := s.reader.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			t.agent,
			COUNT(DISTINCT t.agent_session_id),
			COUNT(t.id),
			COALESCE(SUM(t.total_tokens), 0)
		FROM agent_turns t
		JOIN agent_sessions s ON s.id = t.agent_session_id
		WHERE %s
		GROUP BY t.agent
		ORDER BY t.agent ASC
	`, turnWhere), turnArgs...)
	if err != nil {
		return model.InsightsOverview{}, fmt.Errorf("insights overview by_agent: %w", err)
	}
	defer func() { _ = agentRows.Close() }()

	byAgent := []model.AgentComparison{}
	for agentRows.Next() {
		var item model.AgentComparison
		if scanErr := agentRows.Scan(&item.Agent, &item.SessionCount, &item.TurnCount, &item.TotalTokens); scanErr != nil {
			return model.InsightsOverview{}, scanErr
		}
		counts := toolByAgent[item.Agent]
		item.ToolCallCount = counts[0]
		item.FailureCount = counts[1]
		byAgent = append(byAgent, item)
	}
	if err := agentRows.Err(); err != nil {
		return model.InsightsOverview{}, err
	}

	return model.InsightsOverview{
		Since:  sinceStr,
		Until:  untilStr,
		Totals: totals,
		Quality: model.InsightsQuality{
			Outcomes:   outcomes,
			Validation: validations,
			Confidence: normalizeConfidenceCounts(rawConf),
		},
		ByAgent: byAgent,
	}, nil
}

// GetInsightsTimeseries returns time-bucketed turns, tokens, outcomes, and per-agent breakdowns.
func (s *Store) GetInsightsTimeseries(ctx context.Context, filters model.InsightFilters) (model.InsightsTimeseries, error) {
	sinceStr, untilStr := model.FormatUTC(filters.Since), model.FormatUTC(filters.Until)
	bucketName := insightsTimeseriesBucket(filters.Since, filters.Until)
	bucketExpr := utcBucketExpr(bucketName, insightsTurnActivityExpr)
	turnWhere, turnArgs := buildTurnFilters(filters, sinceStr, untilStr)

	const successCond = `t.outcome IN ('completed', 'success', 'ok')`
	const failCond = `t.outcome IN ('failed', 'error', 'interrupted')`

	pointArgs := append(append([]any{}, turnArgs...), maxInsightsTimeseriesPoints)
	rows, err := s.reader.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			%s AS bucket,
			COUNT(t.id),
			COUNT(DISTINCT t.agent_session_id),
			COALESCE(SUM(t.total_tokens), 0),
			COALESCE(SUM(CASE WHEN %s THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN %s THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN NOT (%s) AND NOT (%s) THEN 1 ELSE 0 END), 0)
		FROM agent_turns t
		JOIN agent_sessions s ON s.id = t.agent_session_id
		WHERE %s
		GROUP BY bucket
		ORDER BY bucket ASC
		LIMIT ?
	`, bucketExpr, successCond, failCond, successCond, failCond, turnWhere), pointArgs...)
	if err != nil {
		return model.InsightsTimeseries{}, fmt.Errorf("insights timeseries points: %w", err)
	}
	defer func() { _ = rows.Close() }()

	points := []model.TimeseriesPoint{}
	for rows.Next() {
		var pt model.TimeseriesPoint
		if err := rows.Scan(
			&pt.Timestamp, &pt.TurnCount, &pt.SessionCount, &pt.TotalTokens,
			&pt.SuccessCount, &pt.FailureCount, &pt.UnknownCount,
		); err != nil {
			return model.InsightsTimeseries{}, err
		}
		points = append(points, pt)
	}
	if err := rows.Err(); err != nil {
		return model.InsightsTimeseries{}, err
	}

	agentRows, err := s.reader.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			t.agent,
			COUNT(DISTINCT t.agent_session_id),
			COUNT(t.id),
			COALESCE(SUM(t.total_tokens), 0),
			COALESCE(SUM(CASE WHEN %s THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN %s THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN NOT (%s) AND NOT (%s) THEN 1 ELSE 0 END), 0)
		FROM agent_turns t
		JOIN agent_sessions s ON s.id = t.agent_session_id
		WHERE %s
		GROUP BY t.agent
		ORDER BY t.agent ASC
	`, successCond, failCond, successCond, failCond, turnWhere), turnArgs...)
	if err != nil {
		return model.InsightsTimeseries{}, fmt.Errorf("insights timeseries by_agent: %w", err)
	}
	defer func() { _ = agentRows.Close() }()

	byAgent := []model.AgentComparison{}
	for agentRows.Next() {
		var item model.AgentComparison
		if err := agentRows.Scan(
			&item.Agent, &item.SessionCount, &item.TurnCount, &item.TotalTokens,
			&item.SuccessCount, &item.FailureCount, &item.UnknownCount,
		); err != nil {
			return model.InsightsTimeseries{}, err
		}
		byAgent = append(byAgent, item)
	}
	if err := agentRows.Err(); err != nil {
		return model.InsightsTimeseries{}, err
	}

	return model.InsightsTimeseries{
		Since:   sinceStr,
		Until:   untilStr,
		Bucket:  bucketName,
		Points:  points,
		ByAgent: byAgent,
	}, nil
}

// GetInsightsLatency computes turn, first-agent, and active-tool latency percentiles and histogram bins.
func (s *Store) GetInsightsLatency(ctx context.Context, filters model.InsightFilters) (model.InsightsLatency, error) {
	sinceStr, untilStr := model.FormatUTC(filters.Since), model.FormatUTC(filters.Until)
	turnWhere, turnArgs := buildTurnFilters(filters, sinceStr, untilStr)

	rows, err := s.reader.QueryContext(ctx, fmt.Sprintf(`
		SELECT t.duration_ms, t.first_agent_latency_ms, t.active_tool_ms
		FROM agent_turns t
		JOIN agent_sessions s ON s.id = t.agent_session_id
		WHERE %s
	`, turnWhere), turnArgs...)
	if err != nil {
		return model.InsightsLatency{}, fmt.Errorf("insights latency rows: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var durations, firstLatencies, activeTools []float64
	binCounts := map[string]int{}
	for rows.Next() {
		var dur, first, active sql.NullInt64
		if scanErr := rows.Scan(&dur, &first, &active); scanErr != nil {
			return model.InsightsLatency{}, scanErr
		}
		if dur.Valid {
			durations = append(durations, float64(dur.Int64))
			binCounts[durationBucket(dur.Int64)]++
		}
		if first.Valid {
			firstLatencies = append(firstLatencies, float64(first.Int64))
		}
		if active.Valid {
			activeTools = append(activeTools, float64(active.Int64))
		}
	}
	if err := rows.Err(); err != nil {
		return model.InsightsLatency{}, err
	}

	rawConf, err := s.groupTurnCounts(ctx, "t.metric_confidence", turnWhere, turnArgs)
	if err != nil {
		return model.InsightsLatency{}, err
	}

	dist := make([]model.LatencyBucket, 0, len(binCounts))
	bins := make([]string, 0, len(binCounts))
	for name := range binCounts {
		bins = append(bins, name)
	}
	sort.Strings(bins)
	for _, name := range bins {
		dist = append(dist, model.LatencyBucket{Range: name, Count: binCounts[name]})
	}

	return model.InsightsLatency{
		Since: sinceStr,
		Until: untilStr,
		Summary: model.LatencySummary{
			TurnDurationMS:      computeLatencyMeasure(durations),
			FirstAgentLatencyMS: computeLatencyMeasure(firstLatencies),
			ActiveToolMS:        computeLatencyMeasure(activeTools),
			Confidence:          normalizeConfidenceCounts(rawConf),
		},
		Distribution: dist,
	}, nil
}

// GetInsightsTools aggregates call counts, failure rates, and duration percentiles per tool.
func (s *Store) GetInsightsTools(ctx context.Context, filters model.InsightFilters, toolName, status string) (model.InsightsTools, error) {
	sinceStr, untilStr := model.FormatUTC(filters.Since), model.FormatUTC(filters.Until)
	toolWhere, toolArgs := buildToolFilters(filters, sinceStr, untilStr)
	if toolName != "" {
		toolWhere += ` AND tc.tool_name = ?`
		toolArgs = append(toolArgs, toolName)
	}
	if status != "" {
		toolWhere += ` AND tc.status = ?`
		toolArgs = append(toolArgs, status)
	}

	rows, err := s.reader.QueryContext(ctx, fmt.Sprintf(`
		SELECT tc.tool_name, tc.tool_kind, tc.agent, tc.status, tc.exit_code, tc.started_at, tc.completed_at
		FROM tool_calls tc
		JOIN agent_sessions s ON s.id = tc.agent_session_id
		WHERE %s
	`, toolWhere), toolArgs...)
	if err != nil {
		return model.InsightsTools{}, fmt.Errorf("insights tools query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	type toolKey struct {
		name string
		kind string
	}
	type toolAcc struct {
		calls     int
		failures  int
		durations []float64
	}
	byTool := map[toolKey]*toolAcc{}
	byAgentMap := map[string]*model.AgentComparison{}

	for rows.Next() {
		var (
			name, kind, agent, st string
			exitCode              sql.NullInt64
			startedAt, compAt     sql.NullString
		)
		if scanErr := rows.Scan(&name, &kind, &agent, &st, &exitCode, &startedAt, &compAt); scanErr != nil {
			return model.InsightsTools{}, scanErr
		}
		failed := st == "failed" || st == "error" || (exitCode.Valid && exitCode.Int64 != 0)
		key := toolKey{name: name, kind: kind}
		acc := byTool[key]
		if acc == nil {
			acc = &toolAcc{}
			byTool[key] = acc
		}
		acc.calls++
		if failed {
			acc.failures++
		}
		if startedAt.Valid && compAt.Valid {
			if stTime, err1 := model.ParseUTC(startedAt.String); err1 == nil {
				if endTime, err2 := model.ParseUTC(compAt.String); err2 == nil {
					ms := math.Max(0, float64(endTime.Sub(stTime).Microseconds())/1000.0)
					acc.durations = append(acc.durations, ms)
				}
			}
		}

		ag := byAgentMap[agent]
		if ag == nil {
			ag = &model.AgentComparison{Agent: agent}
			byAgentMap[agent] = ag
		}
		ag.ToolCallCount++
		if failed {
			ag.FailureCount++
		}
	}
	if err := rows.Err(); err != nil {
		return model.InsightsTools{}, err
	}

	items := make([]model.ToolInsight, 0, len(byTool))
	for k, acc := range byTool {
		measure := computeLatencyMeasure(acc.durations)
		rate := 0.0
		if acc.calls > 0 {
			rate = float64(acc.failures) / float64(acc.calls)
		}
		items = append(items, model.ToolInsight{
			ToolName:      k.name,
			ToolKind:      k.kind,
			CallCount:     acc.calls,
			FailureCount:  acc.failures,
			FailureRate:   rate,
			DurationCount: measure.Count,
			AverageMS:     measure.Average,
			P50MS:         measure.P50,
			P95MS:         measure.P95,
		})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].CallCount != items[j].CallCount {
			return items[i].CallCount > items[j].CallCount
		}
		return items[i].ToolName < items[j].ToolName
	})

	byAgent := make([]model.AgentComparison, 0, len(byAgentMap))
	for _, ag := range byAgentMap {
		byAgent = append(byAgent, *ag)
	}
	sort.Slice(byAgent, func(i, j int) bool { return byAgent[i].Agent < byAgent[j].Agent })

	return model.InsightsTools{
		Items:   items,
		ByAgent: byAgent,
	}, nil
}

// GetInsightsAgents returns parent/child thread delegation nodes and edges in the filter window.
func (s *Store) GetInsightsAgents(ctx context.Context, filters model.InsightFilters) (model.InsightsAgents, error) {
	sinceStr, untilStr := model.FormatUTC(filters.Since), model.FormatUTC(filters.Until)
	const edgeTimeExpr = `coalesce(e.event_time, e.created_at)`

	clauses := []string{edgeTimeExpr + ` >= ?`, edgeTimeExpr + ` < ?`}
	args := []any{sinceStr, untilStr}
	if filters.WorkspaceID != "" {
		clauses = append(clauses, `(cs.workspace_id = ? OR ps.workspace_id = ?)`)
		args = append(args, filters.WorkspaceID, filters.WorkspaceID)
	}
	if filters.Model != "" {
		clauses = append(clauses, `(cs.model = ? OR ps.model = ?)`)
		args = append(args, filters.Model, filters.Model)
	}
	if filters.Agent != "" {
		clauses = append(clauses, `(cs.agent = ? OR ps.agent = ?)`)
		args = append(args, filters.Agent, filters.Agent)
	}
	args = append(args, maxInsightsAgentEdges+1)

	rows, err := s.reader.QueryContext(ctx, fmt.Sprintf(`
		SELECT e.id, e.source_id, e.parent_thread_id, e.child_thread_id, e.status, e.event_time, e.metadata_json, e.created_at
		FROM thread_edges e
		LEFT JOIN agent_sessions cs ON cs.thread_id = e.child_thread_id
		LEFT JOIN agent_sessions ps ON ps.thread_id = e.parent_thread_id
		WHERE %s
		ORDER BY %s ASC, e.id ASC
		LIMIT ?
	`, strings.Join(clauses, " AND "), edgeTimeExpr), args...)
	if err != nil {
		return model.InsightsAgents{}, fmt.Errorf("insights agents edges: %w", err)
	}
	defer func() { _ = rows.Close() }()

	edges := []model.ThreadEdge{}
	threadSet := map[string]struct{}{}
	truncated := false
	for rows.Next() {
		var (
			edge     model.ThreadEdge
			evTime   sql.NullString
			metaJSON string
		)
		if scanErr := rows.Scan(&edge.ID, &edge.SourceID, &edge.ParentThreadID, &edge.ChildThreadID, &edge.Status, &evTime, &metaJSON, &edge.CreatedAt); scanErr != nil {
			return model.InsightsAgents{}, scanErr
		}
		if len(edges) >= maxInsightsAgentEdges {
			truncated = true
			break
		}
		edge.EventTime = nullStrPtr(evTime)
		edge.MetadataJSON = json.RawMessage(defaultJSON(metaJSON))
		edges = append(edges, edge)
		threadSet[edge.ParentThreadID] = struct{}{}
		threadSet[edge.ChildThreadID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return model.InsightsAgents{}, err
	}
	if len(threadSet) == 0 {
		return model.InsightsAgents{Nodes: []model.AgentNode{}, Edges: []model.ThreadEdge{}, Truncated: false}, nil
	}

	threadIDs := make([]string, 0, len(threadSet))
	placeholders := make([]string, 0, len(threadSet))
	inArgs := make([]any, 0, len(threadSet))
	for tid := range threadSet {
		threadIDs = append(threadIDs, tid)
		placeholders = append(placeholders, "?")
	}
	sort.Strings(threadIDs)
	for _, tid := range threadIDs {
		inArgs = append(inArgs, tid)
	}

	type nodeMeta struct {
		sessionID   string
		title       *string
		modelName   *string
		agent       string
		workspaceID *string
		isSubagent  bool
	}
	byThread := map[string]nodeMeta{}
	sRows, err := s.reader.QueryContext(ctx, fmt.Sprintf(`
		SELECT thread_id, id, display_title, ai_title, thread_name, model, agent, workspace_id, is_subagent
		FROM agent_sessions
		WHERE thread_id IN (%s)
	`, strings.Join(placeholders, ",")), inArgs...)
	if err != nil {
		return model.InsightsAgents{}, fmt.Errorf("insights agents sessions: %w", err)
	}
	defer func() { _ = sRows.Close() }()

	for sRows.Next() {
		var (
			tid, sid, ag                   string
			dispTitle, aiTitle, thrName    sql.NullString
			modelName, wsID                sql.NullString
			subInt                         int
		)
		if scanErr := sRows.Scan(&tid, &sid, &dispTitle, &aiTitle, &thrName, &modelName, &ag, &wsID, &subInt); scanErr != nil {
			return model.InsightsAgents{}, scanErr
		}
		var title *string
		for _, cand := range []sql.NullString{dispTitle, aiTitle, thrName} {
			if cand.Valid && cand.String != "" {
				v := cand.String
				title = &v
				break
			}
		}
		byThread[tid] = nodeMeta{
			sessionID:   sid,
			title:       title,
			modelName:   nullStrPtr(modelName),
			agent:       ag,
			workspaceID: nullStrPtr(wsID),
			isSubagent:  subInt != 0,
		}
	}
	if err := sRows.Err(); err != nil {
		return model.InsightsAgents{}, err
	}

	nodes := make([]model.AgentNode, 0, len(threadIDs))
	for _, tid := range threadIDs {
		node := model.AgentNode{ThreadID: tid}
		if info, ok := byThread[tid]; ok {
			sid := info.sessionID
			ag := info.agent
			sub := info.isSubagent
			node.SessionID = &sid
			node.Title = info.title
			node.Model = info.modelName
			node.Agent = &ag
			node.WorkspaceID = info.workspaceID
			node.IsSubagent = &sub
		}
		nodes = append(nodes, node)
	}

	return model.InsightsAgents{
		Nodes:     nodes,
		Edges:     edges,
		Truncated: truncated,
	}, nil
}

// GetInsightsSources lists registered local_data_sources filtered optionally by agent, source_kind, or status.
func (s *Store) GetInsightsSources(ctx context.Context, agent, sourceKind, status string) (model.InsightsSources, error) {
	var clauses []string
	var args []any
	if agent != "" {
		clauses = append(clauses, `agent = ?`)
		args = append(args, agent)
	}
	if sourceKind != "" {
		clauses = append(clauses, `source_kind = ?`)
		args = append(args, sourceKind)
	}
	if status != "" {
		clauses = append(clauses, `status = ?`)
		args = append(args, status)
	}
	whereSQL := ""
	if len(clauses) > 0 {
		whereSQL = "WHERE " + strings.Join(clauses, " AND ")
	}
	rows, err := s.reader.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			id, agent, source_kind, source_identity, source_path, source_format, parser_version,
			status, record_count, byte_size,
			coverage_started_at, coverage_ended_at, backfill_cutoff_at,
			last_attempt_at, last_success_at, last_error_kind, last_error,
			cursor_json, fingerprint_json, created_at, updated_at
		FROM local_data_sources
		%s
		ORDER BY source_kind ASC, source_path ASC
	`, whereSQL), args...)
	if err != nil {
		return model.InsightsSources{}, fmt.Errorf("insights sources: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []model.LocalDataSource{}
	for rows.Next() {
		var (
			src                          model.LocalDataSource
			covStart, covEnd, backfill   sql.NullString
			lastAttempt, lastSucc        sql.NullString
			errKind, errMsg              sql.NullString
		)
		if scanErr := rows.Scan(
			&src.ID, &src.Agent, &src.SourceKind, &src.SourceIdentity, &src.SourcePath,
			&src.SourceFormat, &src.ParserVersion, &src.Status, &src.RecordCount, &src.ByteSize,
			&covStart, &covEnd, &backfill, &lastAttempt, &lastSucc, &errKind, &errMsg,
			&src.CursorJSON, &src.FingerprintJSON, &src.CreatedAt, &src.UpdatedAt,
		); scanErr != nil {
			return model.InsightsSources{}, scanErr
		}
		src.CoverageStartedAt = nullStrPtr(covStart)
		src.CoverageEndedAt = nullStrPtr(covEnd)
		src.BackfillCutoffAt = nullStrPtr(backfill)
		src.LastAttemptAt = nullStrPtr(lastAttempt)
		src.LastSuccessAt = nullStrPtr(lastSucc)
		src.LastErrorKind = nullStrPtr(errKind)
		src.LastError = nullStrPtr(errMsg)
		out = append(out, src)
	}
	return model.InsightsSources{Sources: out}, rows.Err()
}

// GetSessionTimeline returns ordered activity spans for a session's waterfall chart.
func (s *Store) GetSessionTimeline(ctx context.Context, sessionID string, limit int) (model.SessionTimeline, error) {
	if err := s.ensureSessionExists(ctx, sessionID); err != nil {
		return model.SessionTimeline{}, err
	}
	if limit <= 0 {
		limit = 500
	} else if limit > 2000 {
		limit = 2000
	}
	rows, err := s.reader.QueryContext(ctx, `
		SELECT
			id, agent, evidence_id, source_kind, source_record_id,
			thread_id, turn_id, span_type, name, status,
			started_at, completed_at, duration_ms, confidence, metadata_json, created_at
		FROM activity_spans
		WHERE agent_session_id = ?
		ORDER BY started_at ASC, id ASC
		LIMIT ?
	`, sessionID, limit)
	if err != nil {
		return model.SessionTimeline{}, fmt.Errorf("session timeline %s: %w", sessionID, err)
	}
	defer func() { _ = rows.Close() }()

	spans := []model.ActivitySpan{}
	for rows.Next() {
		var (
			sp                   model.ActivitySpan
			evID, srcRecID       sql.NullString
			threadID, turnID     sql.NullString
			name, completedAt    sql.NullString
			durMS                sql.NullInt64
			metaJSON             string
		)
		if scanErr := rows.Scan(
			&sp.ID, &sp.Agent, &evID, &sp.SourceKind, &srcRecID,
			&threadID, &turnID, &sp.SpanType, &name, &sp.Status,
			&sp.StartedAt, &completedAt, &durMS, &sp.Confidence, &metaJSON, &sp.CreatedAt,
		); scanErr != nil {
			return model.SessionTimeline{}, scanErr
		}
		sp.AgentSessionID = sessionID
		sp.EvidenceID = nullStrPtr(evID)
		sp.SourceRecordID = nullStrPtr(srcRecID)
		sp.ThreadID = nullStrPtr(threadID)
		sp.TurnID = nullStrPtr(turnID)
		sp.Name = nullStrPtr(name)
		sp.CompletedAt = nullStrPtr(completedAt)
		sp.DurationMS = nullInt64Ptr(durMS)
		sp.MetadataJSON = json.RawMessage(defaultJSON(metaJSON))
		spans = append(spans, sp)
	}
	return model.SessionTimeline{SessionID: sessionID, Spans: spans}, rows.Err()
}

// ListEvidence lists evidence records matching time and metadata filters.
func (s *Store) ListEvidence(ctx context.Context, filters model.InsightFilters, sourceID, category, level, threadID string, limit int) (model.InsightEvidenceList, error) {
	if limit <= 0 {
		limit = 100
	} else if limit > 200 {
		limit = 200
	}
	const evTimeExpr = `coalesce(er.recorded_at, er.created_at)`
	clauses := []string{evTimeExpr + ` >= ?`, evTimeExpr + ` < ?`}
	args := []any{model.FormatUTC(filters.Since), model.FormatUTC(filters.Until)}
	if sourceID != "" {
		clauses = append(clauses, `er.source_id = ?`)
		args = append(args, sourceID)
	}
	if category != "" {
		clauses = append(clauses, `er.category = ?`)
		args = append(args, category)
	}
	if level != "" {
		clauses = append(clauses, `er.level = ?`)
		args = append(args, level)
	}
	if threadID != "" {
		clauses = append(clauses, `er.thread_id = ?`)
		args = append(args, threadID)
	}
	args = append(args, limit)

	rows, err := s.reader.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			er.id, er.source_id, ds.source_kind, er.source_record_id,
			er.agent_session_id, er.thread_id, er.turn_id, er.recorded_at,
			er.category, er.level, er.target, er.summary, length(coalesce(er.raw_text, ''))
		FROM evidence_records er
		JOIN local_data_sources ds ON ds.id = er.source_id
		WHERE %s
		ORDER BY %s DESC, er.id DESC
		LIMIT ?
	`, strings.Join(clauses, " AND "), evTimeExpr), args...)
	if err != nil {
		return model.InsightEvidenceList{}, fmt.Errorf("list evidence: %w", err)
	}
	defer func() { _ = rows.Close() }()

	items := []model.InsightEvidenceSummary{}
	for rows.Next() {
		var (
			item                             model.InsightEvidenceSummary
			sessID, tid, turnID, recordedAt  sql.NullString
			lvl, target, summary             sql.NullString
		)
		if scanErr := rows.Scan(
			&item.ID, &item.SourceID, &item.SourceKind, &item.SourceRecordID,
			&sessID, &tid, &turnID, &recordedAt,
			&item.Category, &lvl, &target, &summary, &item.RawSize,
		); scanErr != nil {
			return model.InsightEvidenceList{}, scanErr
		}
		item.SessionID = nullStrPtr(sessID)
		item.ThreadID = nullStrPtr(tid)
		item.TurnID = nullStrPtr(turnID)
		item.RecordedAt = nullStrPtr(recordedAt)
		item.Level = nullStrPtr(lvl)
		item.Target = nullStrPtr(target)
		item.Summary = nullStrPtr(summary)
		items = append(items, item)
	}
	return model.InsightEvidenceList{Items: items}, rows.Err()
}

// GetEvidence returns one evidence record with zstd JSON decoded (falling back to linked agent_event).
func (s *Store) GetEvidence(ctx context.Context, evidenceID string) (model.EvidenceRecord, error) {
	var (
		rec                             model.EvidenceRecord
		sessID, tid, turnID             sql.NullString
		evID                            sql.NullInt64
		lvl, target, recAt, sum, rawTxt sql.NullString
		rawZstd                         []byte
		locatorJSON, metaJSON           string
	)
	err := s.reader.QueryRowContext(ctx, `
		SELECT
			er.id, er.source_id, ds.source_kind, er.source_record_id,
			er.agent_session_id, er.agent_event_id, er.thread_id, er.turn_id,
			er.category, er.level, er.target, er.recorded_at, er.summary, er.raw_text,
			er.raw_json_zstd, er.raw_hash, er.source_locator_json, er.metadata_json, er.created_at
		FROM evidence_records er
		JOIN local_data_sources ds ON ds.id = er.source_id
		WHERE er.id = ?
	`, evidenceID).Scan(
		&rec.ID, &rec.SourceID, &rec.SourceKind, &rec.SourceRecordID,
		&sessID, &evID, &tid, &turnID,
		&rec.Category, &lvl, &target, &recAt, &sum, &rawTxt,
		&rawZstd, &rec.RawHash, &locatorJSON, &metaJSON, &rec.CreatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return model.EvidenceRecord{}, ErrNotFound
	}
	if err != nil {
		return model.EvidenceRecord{}, fmt.Errorf("get evidence %s: %w", evidenceID, err)
	}
	rec.AgentSessionID = nullStrPtr(sessID)
	rec.AgentEventID = nullInt64Ptr(evID)
	rec.ThreadID = nullStrPtr(tid)
	rec.TurnID = nullStrPtr(turnID)
	rec.Level = nullStrPtr(lvl)
	rec.Target = nullStrPtr(target)
	rec.RecordedAt = nullStrPtr(recAt)
	rec.Summary = nullStrPtr(sum)
	rec.RawText = nullStrPtr(rawTxt)
	rec.SourceLocatorJSON = json.RawMessage(defaultJSON(locatorJSON))
	rec.MetadataJSON = json.RawMessage(defaultJSON(metaJSON))

	if len(rawZstd) > 0 {
		decoded, decErr := DecompressJSON(rawZstd)
		if decErr != nil {
			return model.EvidenceRecord{}, fmt.Errorf("decode evidence %s json: %w", evidenceID, decErr)
		}
		rec.RawJSON = decoded
	} else if rec.AgentEventID != nil {
		var eventZstd []byte
		if evErr := s.reader.QueryRowContext(ctx, `SELECT raw_json_zstd FROM agent_events WHERE id = ?`, *rec.AgentEventID).Scan(&eventZstd); evErr == nil && len(eventZstd) > 0 {
			if decoded, decErr := DecompressJSON(eventZstd); decErr == nil {
				rec.RawJSON = decoded
			}
		}
	}
	return rec, nil
}

func buildTurnFilters(filters model.InsightFilters, sinceStr, untilStr string) (string, []any) {
	clauses := []string{insightsTurnActivityExpr + ` >= ?`, insightsTurnActivityExpr + ` < ?`}
	args := []any{sinceStr, untilStr}
	return appendSessionDimFilters(clauses, args, filters)
}

func buildToolFilters(filters model.InsightFilters, sinceStr, untilStr string) (string, []any) {
	clauses := []string{insightsToolActivityExpr + ` >= ?`, insightsToolActivityExpr + ` < ?`}
	args := []any{sinceStr, untilStr}
	return appendSessionDimFilters(clauses, args, filters)
}

func appendSessionDimFilters(clauses []string, args []any, filters model.InsightFilters) (string, []any) {
	if filters.WorkspaceID != "" {
		clauses = append(clauses, `s.workspace_id = ?`)
		args = append(args, filters.WorkspaceID)
	}
	if filters.Model != "" {
		clauses = append(clauses, `s.model = ?`)
		args = append(args, filters.Model)
	}
	if filters.Agent != "" {
		clauses = append(clauses, `s.agent = ?`)
		args = append(args, filters.Agent)
	}
	return strings.Join(clauses, " AND "), args
}

func (s *Store) groupTurnCounts(ctx context.Context, col, turnWhere string, turnArgs []any) (map[string]int, error) {
	rows, err := s.reader.QueryContext(ctx, fmt.Sprintf(`
		SELECT COALESCE(%s, 'unknown'), COUNT(t.id)
		FROM agent_turns t
		JOIN agent_sessions s ON s.id = t.agent_session_id
		WHERE %s
		GROUP BY %s
	`, col, turnWhere, col), turnArgs...)
	if err != nil {
		return nil, fmt.Errorf("group turns by %s: %w", col, err)
	}
	defer func() { _ = rows.Close() }()

	out := map[string]int{}
	for rows.Next() {
		var key string
		var cnt int
		if scanErr := rows.Scan(&key, &cnt); scanErr != nil {
			return nil, scanErr
		}
		out[key] = cnt
	}
	return out, rows.Err()
}

func normalizeConfidenceCounts(raw map[string]int) map[string]int {
	out := map[string]int{"unknown": 0}
	for _, level := range confidenceLevels {
		out[level] = raw[level]
	}
	for k, v := range raw {
		if k != "exact" && k != "derived" && k != "inferred" {
			out["unknown"] += v
		}
	}
	return out
}

func insightsTimeseriesBucket(since, until time.Time) string {
	dur := until.Sub(since)
	switch {
	case dur <= maxInsightsTimeseriesPoints*time.Hour:
		return "hour"
	case dur <= maxInsightsTimeseriesPoints*24*time.Hour:
		return "day"
	case dur <= maxInsightsTimeseriesPoints*7*24*time.Hour:
		return "week"
	case dur <= maxInsightsTimeseriesPoints*31*24*time.Hour:
		return "month"
	default:
		return "year"
	}
}

func durationBucket(ms int64) string {
	switch {
	case ms < 1000:
		return "under_1s"
	case ms < 5000:
		return "1s_5s"
	case ms < 30000:
		return "5s_30s"
	case ms < 120000:
		return "30s_2m"
	default:
		return "over_2m"
	}
}

func computeLatencyMeasure(values []float64) model.LatencyMeasure {
	n := len(values)
	if n == 0 {
		return model.LatencyMeasure{}
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	sum := 0.0
	for _, v := range sorted {
		sum += v
	}
	return model.LatencyMeasure{
		Count:   n,
		P50:     percentileCont(sorted, 0.50),
		P95:     percentileCont(sorted, 0.95),
		Average: sum / float64(n),
	}
}

// percentileCont matches PostgreSQL percentile_cont linear interpolation on a sorted slice.
func percentileCont(sorted []float64, p float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if n == 1 {
		return sorted[0]
	}
	pos := p * float64(n-1)
	lo := int(math.Floor(pos))
	hi := int(math.Ceil(pos))
	if lo == hi {
		return sorted[lo]
	}
	frac := pos - float64(lo)
	return sorted[lo] + frac*(sorted[hi]-sorted[lo])
}
