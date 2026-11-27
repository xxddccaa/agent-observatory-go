package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/agent-observatory/agent-observatory-go/internal/model"
)

const maxProjectTimeseriesPoints = 500
const projectTurnActivityExpr = `coalesce(t.completed_at, t.started_at, t.created_at)`

// ResolveProjectWindow translates "all" | "day" | "week" | "month" into UTC [since, until).
func ResolveProjectWindow(rangeName string, now time.Time) (*time.Time, time.Time, string, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	switch rangeName {
	case "", "month":
		since := now.AddDate(0, 0, -30)
		return &since, now, "month", nil
	case "all":
		return nil, now, "all", nil
	case "day":
		since := now.Add(-24 * time.Hour)
		return &since, now, "day", nil
	case "week":
		since := now.AddDate(0, 0, -7)
		return &since, now, "week", nil
	default:
		return nil, time.Time{}, "", fmt.Errorf("%w: range must be all, day, week, or month", ErrInvalidFilter)
	}
}

// ListProjects returns workspaces enriched with all-time and selected-range token totals.
func (s *Store) ListProjects(ctx context.Context, filter model.ProjectListFilter) (model.ProjectsPage, error) {
	since, until, normalizedRange, err := ResolveProjectWindow(filter.Range, filter.Now)
	if err != nil {
		return model.ProjectsPage{}, err
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	} else if limit > 200 {
		limit = 200
	}
	offset := max(0, filter.Offset)

	var whereClauses []string
	var whereArgs []any
	if q := strings.TrimSpace(filter.Query); q != "" {
		pattern := "%" + EscapeLike(strings.ToLower(q)) + "%"
		whereClauses = append(whereClauses, `(LOWER(w.display_name) LIKE ? ESCAPE '\' OR LOWER(w.path) LIKE ? ESCAPE '\')`)
		whereArgs = append(whereArgs, pattern, pattern)
	}
	if filter.Agent != "" || filter.Model != "" {
		var sub []string
		sub = append(sub, `s2.workspace_id = w.id`)
		if filter.Agent != "" {
			sub = append(sub, `s2.agent = ?`)
			whereArgs = append(whereArgs, filter.Agent)
		}
		if filter.Model != "" {
			sub = append(sub, `s2.model = ?`)
			whereArgs = append(whereArgs, filter.Model)
		}
		whereClauses = append(whereClauses, `EXISTS (SELECT 1 FROM agent_sessions s2 WHERE `+strings.Join(sub, " AND ")+`)`)
	}
	whereSQL := ""
	if len(whereClauses) > 0 {
		whereSQL = "WHERE " + strings.Join(whereClauses, " AND ")
	}

	var total int
	countSQL := `SELECT COUNT(*) FROM workspaces w ` + whereSQL
	if err := s.reader.QueryRowContext(ctx, countSQL, whereArgs...).Scan(&total); err != nil {
		return model.ProjectsPage{}, fmt.Errorf("count projects: %w", err)
	}

	var sessionJoin []string
	var joinArgs []any
	sessionJoin = append(sessionJoin, `s.workspace_id = w.id`)
	if filter.Agent != "" {
		sessionJoin = append(sessionJoin, `s.agent = ?`)
		joinArgs = append(joinArgs, filter.Agent)
	}
	if filter.Model != "" {
		sessionJoin = append(sessionJoin, `s.model = ?`)
		joinArgs = append(joinArgs, filter.Model)
	}

	untilStr := model.FormatUTC(until)
	allCond := projectTurnActivityExpr + ` < ?`
	selCond := projectTurnActivityExpr + ` < ?`
	var selArgs []any
	if since != nil {
		selCond = projectTurnActivityExpr + ` >= ? AND ` + projectTurnActivityExpr + ` < ?`
		selArgs = []any{model.FormatUTC(*since), untilStr}
	} else {
		selArgs = []any{untilStr}
	}

	// Build argument slice in placeholder order: joinArgs, allCond/selCond args, whereArgs, limit, offset.
	queryArgs := make([]any, 0, len(joinArgs)+12+len(whereArgs)+2)
	for range 5 {
		queryArgs = append(queryArgs, untilStr)
	}
	for range 5 {
		queryArgs = append(queryArgs, selArgs...)
	}
	queryArgs = append(queryArgs, joinArgs...)
	queryArgs = append(queryArgs, whereArgs...)
	queryArgs = append(queryArgs, limit, offset)

	querySQL := fmt.Sprintf(`
		SELECT
			w.id, w.display_name, w.path, w.is_allowed, w.workspace_kind, w.workspace_confidence,
			NULLIF(MAX(MAX(coalesce(%s, ''), coalesce(s.last_activity_at, s.end_time, s.start_time, ''))), '') AS last_active_at,
			COUNT(DISTINCT s.id) AS session_count,
			COUNT(t.id) AS turn_count,
			COALESCE(SUM(CASE WHEN %s THEN t.input_tokens ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN %s THEN t.cached_input_tokens ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN %s THEN t.output_tokens ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN %s THEN t.reasoning_output_tokens ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN %s THEN t.total_tokens ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN %s THEN t.input_tokens ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN %s THEN t.cached_input_tokens ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN %s THEN t.output_tokens ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN %s THEN t.reasoning_output_tokens ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN %s THEN t.total_tokens ELSE 0 END), 0)
		FROM workspaces w
		LEFT JOIN agent_sessions s ON %s
		LEFT JOIN agent_turns t ON t.agent_session_id = s.id
		%s
		GROUP BY w.id
		ORDER BY last_active_at DESC NULLS LAST, w.id DESC
		LIMIT ? OFFSET ?
	`,
		projectTurnActivityExpr,
		allCond, allCond, allCond, allCond, allCond,
		selCond, selCond, selCond, selCond, selCond,
		strings.Join(sessionJoin, " AND "),
		whereSQL,
	)

	rows, err := s.reader.QueryContext(ctx, querySQL, queryArgs...)
	if err != nil {
		return model.ProjectsPage{}, fmt.Errorf("list projects: %w", err)
	}
	defer func() { _ = rows.Close() }()

	items := []model.ProjectSummary{}
	for rows.Next() {
		var (
			item         model.ProjectSummary
			allowedInt   int
			lastActiveAt sql.NullString
		)
		if err := rows.Scan(
			&item.ID, &item.DisplayName, &item.Path, &allowedInt,
			&item.WorkspaceKind, &item.WorkspaceConfidence,
			&lastActiveAt, &item.SessionCount, &item.TurnCount,
			&item.AllTime.InputTokens, &item.AllTime.CachedInputTokens,
			&item.AllTime.OutputTokens, &item.AllTime.ReasoningOutputTokens, &item.AllTime.TotalTokens,
			&item.SelectedRange.InputTokens, &item.SelectedRange.CachedInputTokens,
			&item.SelectedRange.OutputTokens, &item.SelectedRange.ReasoningOutputTokens, &item.SelectedRange.TotalTokens,
		); err != nil {
			return model.ProjectsPage{}, fmt.Errorf("scan project row: %w", err)
		}
		item.IsAllowed = allowedInt != 0
		item.LastActiveAt = nullStrPtr(lastActiveAt)
		item.SelectedRange.Range = normalizedRange
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return model.ProjectsPage{}, err
	}

	var sinceStr *string
	if since != nil {
		formatted := model.FormatUTC(*since)
		sinceStr = &formatted
	}
	return model.ProjectsPage{
		Items: items,
		Total: total,
		Range: normalizedRange,
		Since: sinceStr,
		Until: untilStr,
	}, nil
}

// GetProject returns token totals across windows plus per-agent and per-model breakdowns.
func (s *Store) GetProject(ctx context.Context, workspaceID string, now time.Time) (model.ProjectDetail, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}

	var (
		meta       model.ProjectMeta
		allowedInt int
	)
	err := s.reader.QueryRowContext(ctx, `
		SELECT id, display_name, path, is_allowed, workspace_kind, workspace_confidence
		FROM workspaces
		WHERE id = ?
	`, workspaceID).Scan(
		&meta.ID, &meta.DisplayName, &meta.Path, &allowedInt,
		&meta.WorkspaceKind, &meta.WorkspaceConfidence,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return model.ProjectDetail{}, ErrNotFound
	}
	if err != nil {
		return model.ProjectDetail{}, fmt.Errorf("get project %s: %w", workspaceID, err)
	}
	meta.IsAllowed = allowedInt != 0

	var (
		counts      model.ProjectCounts
		firstActive sql.NullString
		lastActive  sql.NullString
	)
	if err := s.reader.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT
			COUNT(DISTINCT s.id),
			COUNT(t.id),
			NULLIF(MIN(MIN(
				coalesce(%s, '9999-12-31T23:59:59.999999Z'),
				coalesce(s.start_time, '9999-12-31T23:59:59.999999Z'),
				coalesce(s.end_time, '9999-12-31T23:59:59.999999Z'),
				coalesce(s.last_activity_at, '9999-12-31T23:59:59.999999Z')
			)), '9999-12-31T23:59:59.999999Z'),
			NULLIF(MAX(MAX(
				coalesce(%s, ''),
				coalesce(s.last_activity_at, s.end_time, s.start_time, '')
			)), '')
		FROM agent_sessions s
		LEFT JOIN agent_turns t ON t.agent_session_id = s.id
		WHERE s.workspace_id = ?
	`, projectTurnActivityExpr, projectTurnActivityExpr), workspaceID).Scan(
		&counts.SessionCount, &counts.TurnCount, &firstActive, &lastActive,
	); err != nil {
		return model.ProjectDetail{}, fmt.Errorf("project counts %s: %w", workspaceID, err)
	}
	meta.FirstActiveAt = nullStrPtr(firstActive)
	meta.LastActiveAt = nullStrPtr(lastActive)

	untilStr := model.FormatUTC(now)
	dayStr := model.FormatUTC(now.Add(-24 * time.Hour))
	weekStr := model.FormatUTC(now.AddDate(0, 0, -7))
	monthStr := model.FormatUTC(now.AddDate(0, 0, -30))

	totals, err := s.queryProjectTotals(ctx, workspaceID, untilStr, dayStr, weekStr, monthStr)
	if err != nil {
		return model.ProjectDetail{}, err
	}
	byAgent, err := s.queryProjectSplit(ctx, workspaceID, untilStr, true)
	if err != nil {
		return model.ProjectDetail{}, err
	}
	byModel, err := s.queryProjectSplit(ctx, workspaceID, untilStr, false)
	if err != nil {
		return model.ProjectDetail{}, err
	}

	return model.ProjectDetail{
		Project: meta,
		Totals:  totals,
		Counts:  counts,
		ByAgent: byAgent,
		ByModel: byModel,
	}, nil
}

func (s *Store) queryProjectTotals(ctx context.Context, workspaceID, untilStr, dayStr, weekStr, monthStr string) (model.ProjectDetailTotals, error) {
	var totals model.ProjectDetailTotals
	err := s.reader.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT
			COALESCE(SUM(CASE WHEN %s < ? THEN t.input_tokens ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN %s < ? THEN t.cached_input_tokens ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN %s < ? THEN t.output_tokens ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN %s < ? THEN t.reasoning_output_tokens ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN %s < ? THEN t.total_tokens ELSE 0 END), 0),

			COALESCE(SUM(CASE WHEN %s >= ? AND %s < ? THEN t.input_tokens ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN %s >= ? AND %s < ? THEN t.cached_input_tokens ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN %s >= ? AND %s < ? THEN t.output_tokens ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN %s >= ? AND %s < ? THEN t.reasoning_output_tokens ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN %s >= ? AND %s < ? THEN t.total_tokens ELSE 0 END), 0),

			COALESCE(SUM(CASE WHEN %s >= ? AND %s < ? THEN t.input_tokens ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN %s >= ? AND %s < ? THEN t.cached_input_tokens ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN %s >= ? AND %s < ? THEN t.output_tokens ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN %s >= ? AND %s < ? THEN t.reasoning_output_tokens ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN %s >= ? AND %s < ? THEN t.total_tokens ELSE 0 END), 0),

			COALESCE(SUM(CASE WHEN %s >= ? AND %s < ? THEN t.input_tokens ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN %s >= ? AND %s < ? THEN t.cached_input_tokens ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN %s >= ? AND %s < ? THEN t.output_tokens ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN %s >= ? AND %s < ? THEN t.reasoning_output_tokens ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN %s >= ? AND %s < ? THEN t.total_tokens ELSE 0 END), 0)
		FROM agent_turns t
		JOIN agent_sessions s ON s.id = t.agent_session_id
		WHERE s.workspace_id = ?
	`,
		projectTurnActivityExpr, projectTurnActivityExpr, projectTurnActivityExpr, projectTurnActivityExpr, projectTurnActivityExpr,
		projectTurnActivityExpr, projectTurnActivityExpr, projectTurnActivityExpr, projectTurnActivityExpr, projectTurnActivityExpr,
		projectTurnActivityExpr, projectTurnActivityExpr, projectTurnActivityExpr, projectTurnActivityExpr, projectTurnActivityExpr,
		projectTurnActivityExpr, projectTurnActivityExpr, projectTurnActivityExpr, projectTurnActivityExpr, projectTurnActivityExpr,
		projectTurnActivityExpr, projectTurnActivityExpr, projectTurnActivityExpr, projectTurnActivityExpr, projectTurnActivityExpr,
		projectTurnActivityExpr, projectTurnActivityExpr, projectTurnActivityExpr, projectTurnActivityExpr, projectTurnActivityExpr,
		projectTurnActivityExpr, projectTurnActivityExpr, projectTurnActivityExpr, projectTurnActivityExpr, projectTurnActivityExpr,
	),
		untilStr, untilStr, untilStr, untilStr, untilStr,
		dayStr, untilStr, dayStr, untilStr, dayStr, untilStr, dayStr, untilStr, dayStr, untilStr,
		weekStr, untilStr, weekStr, untilStr, weekStr, untilStr, weekStr, untilStr, weekStr, untilStr,
		monthStr, untilStr, monthStr, untilStr, monthStr, untilStr, monthStr, untilStr, monthStr, untilStr,
		workspaceID,
	).Scan(
		&totals.AllTime.InputTokens, &totals.AllTime.CachedInputTokens, &totals.AllTime.OutputTokens, &totals.AllTime.ReasoningOutputTokens, &totals.AllTime.TotalTokens,
		&totals.Day.InputTokens, &totals.Day.CachedInputTokens, &totals.Day.OutputTokens, &totals.Day.ReasoningOutputTokens, &totals.Day.TotalTokens,
		&totals.Week.InputTokens, &totals.Week.CachedInputTokens, &totals.Week.OutputTokens, &totals.Week.ReasoningOutputTokens, &totals.Week.TotalTokens,
		&totals.Month.InputTokens, &totals.Month.CachedInputTokens, &totals.Month.OutputTokens, &totals.Month.ReasoningOutputTokens, &totals.Month.TotalTokens,
	)
	if err != nil {
		return model.ProjectDetailTotals{}, fmt.Errorf("project totals %s: %w", workspaceID, err)
	}
	return totals, nil
}

func (s *Store) queryProjectSplit(ctx context.Context, workspaceID, untilStr string, byAgent bool) ([]model.ProjectSplit, error) {
	groupCol := "s.model"
	if byAgent {
		groupCol = "t.agent"
	}
	querySQL := fmt.Sprintf(`
		SELECT
			COALESCE(%s, 'unknown') AS dim,
			COUNT(DISTINCT t.agent_session_id),
			COUNT(t.id),
			COALESCE(SUM(CASE WHEN %s < ? THEN t.input_tokens ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN %s < ? THEN t.cached_input_tokens ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN %s < ? THEN t.output_tokens ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN %s < ? THEN t.reasoning_output_tokens ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN %s < ? THEN t.total_tokens ELSE 0 END), 0)
		FROM agent_turns t
		JOIN agent_sessions s ON s.id = t.agent_session_id
		WHERE s.workspace_id = ?
		GROUP BY %s
		ORDER BY dim ASC
	`, groupCol,
		projectTurnActivityExpr, projectTurnActivityExpr, projectTurnActivityExpr, projectTurnActivityExpr, projectTurnActivityExpr,
		groupCol,
	)

	rows, err := s.reader.QueryContext(ctx, querySQL, untilStr, untilStr, untilStr, untilStr, untilStr, workspaceID)
	if err != nil {
		return nil, fmt.Errorf("project split %s: %w", workspaceID, err)
	}
	defer func() { _ = rows.Close() }()

	out := []model.ProjectSplit{}
	for rows.Next() {
		var (
			dim   string
			split model.ProjectSplit
		)
		if err := rows.Scan(
			&dim, &split.SessionCount, &split.TurnCount,
			&split.InputTokens, &split.CachedInputTokens,
			&split.OutputTokens, &split.ReasoningOutputTokens, &split.TotalTokens,
		); err != nil {
			return nil, err
		}
		if byAgent {
			split.Agent = &dim
		} else {
			split.Model = &dim
		}
		out = append(out, split)
	}
	return out, rows.Err()
}

// GetProjectTimeseries returns hourly or daily UTC buckets for a workspace.
func (s *Store) GetProjectTimeseries(ctx context.Context, workspaceID, rangeName string, now time.Time) (model.ProjectTimeseries, error) {
	if rangeName != "day" && rangeName != "week" && rangeName != "month" {
		return model.ProjectTimeseries{}, fmt.Errorf("%w: range must be day, week, or month", ErrInvalidFilter)
	}
	var exists int
	if err := s.reader.QueryRowContext(ctx, `SELECT 1 FROM workspaces WHERE id = ?`, workspaceID).Scan(&exists); errors.Is(err, sql.ErrNoRows) {
		return model.ProjectTimeseries{}, ErrNotFound
	} else if err != nil {
		return model.ProjectTimeseries{}, err
	}

	since, until, _, err := ResolveProjectWindow(rangeName, now)
	if err != nil {
		return model.ProjectTimeseries{}, err
	}
	bucketName := "day"
	bucketExpr := utcBucketExpr("day", projectTurnActivityExpr)
	if rangeName == "day" {
		bucketName = "hour"
		bucketExpr = utcBucketExpr("hour", projectTurnActivityExpr)
	}

	sinceStr := model.FormatUTC(*since)
	untilStr := model.FormatUTC(until)
	rows, err := s.reader.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			%s AS bucket,
			COUNT(DISTINCT t.agent_session_id),
			COUNT(t.id),
			COALESCE(SUM(t.input_tokens), 0),
			COALESCE(SUM(t.cached_input_tokens), 0),
			COALESCE(SUM(t.output_tokens), 0),
			COALESCE(SUM(t.reasoning_output_tokens), 0),
			COALESCE(SUM(t.total_tokens), 0)
		FROM agent_turns t
		JOIN agent_sessions s ON s.id = t.agent_session_id
		WHERE s.workspace_id = ? AND %s >= ? AND %s < ?
		GROUP BY bucket
		ORDER BY bucket ASC
		LIMIT ?
	`, bucketExpr, projectTurnActivityExpr, projectTurnActivityExpr),
		workspaceID, sinceStr, untilStr, maxProjectTimeseriesPoints,
	)
	if err != nil {
		return model.ProjectTimeseries{}, fmt.Errorf("project timeseries %s: %w", workspaceID, err)
	}
	defer func() { _ = rows.Close() }()

	points := []model.ProjectTimeseriesPoint{}
	for rows.Next() {
		var pt model.ProjectTimeseriesPoint
		if err := rows.Scan(
			&pt.Timestamp, &pt.SessionCount, &pt.TurnCount,
			&pt.InputTokens, &pt.CachedInputTokens,
			&pt.OutputTokens, &pt.ReasoningOutputTokens, &pt.TotalTokens,
		); err != nil {
			return model.ProjectTimeseries{}, err
		}
		points = append(points, pt)
	}
	if err := rows.Err(); err != nil {
		return model.ProjectTimeseries{}, err
	}

	return model.ProjectTimeseries{
		Since:  sinceStr,
		Until:  untilStr,
		Bucket: bucketName,
		Points: points,
	}, nil
}

func utcBucketExpr(bucket, col string) string {
	switch bucket {
	case "hour":
		return fmt.Sprintf(`strftime('%%Y-%%m-%%dT%%H:00:00.000000Z', %s)`, col)
	case "day":
		return fmt.Sprintf(`strftime('%%Y-%%m-%%dT00:00:00.000000Z', %s)`, col)
	case "week":
		return fmt.Sprintf(`strftime('%%Y-%%m-%%dT00:00:00.000000Z', %s, 'weekday 0', '-6 days')`, col)
	case "month":
		return fmt.Sprintf(`strftime('%%Y-%%m-01T00:00:00.000000Z', %s)`, col)
	default:
		return fmt.Sprintf(`strftime('%%Y-01-01T00:00:00.000000Z', %s)`, col)
	}
}
