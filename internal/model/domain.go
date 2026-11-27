package model

import (
	"encoding/json"
	"time"
)

// Workspace represents a normalized working directory associated with sessions.
type Workspace struct {
	ID                  string  `json:"id"`
	Path                string  `json:"path"`
	NormalizedPath      string  `json:"normalized_path"`
	DisplayName         string  `json:"display_name"`
	GitRemote           *string `json:"git_remote,omitempty"`
	GitBranch           *string `json:"git_branch,omitempty"`
	IsAllowed           bool    `json:"is_allowed"`
	WorkspaceKind       string  `json:"workspace_kind"`
	WorkspaceConfidence string  `json:"workspace_confidence"`
	CreatedAt           string  `json:"created_at"`
	UpdatedAt           string  `json:"updated_at"`
}

// AgentSession represents a single indexed coding-agent session.
type AgentSession struct {
	ID                       string  `json:"id"`
	Agent                    string  `json:"agent"`
	SourceKind               string  `json:"source_kind"`
	SourceIdentity           string  `json:"source_identity"`
	RolloutPath              *string `json:"rollout_path"`
	RolloutRealPath          *string `json:"rollout_real_path,omitempty"`
	SourcePath               *string `json:"source_path"`
	RolloutFormat            string  `json:"rollout_format"`
	SourceFormat             string  `json:"source_format"`
	RolloutCompression       *string `json:"rollout_compression,omitempty"`
	SourceCompression        *string `json:"source_compression,omitempty"`
	ThreadID                 string  `json:"thread_id"`
	WorkspaceID              *string `json:"workspace_id"`
	CWD                      *string `json:"cwd"`
	GitBranch                *string `json:"git_branch,omitempty"`
	Originator               *string `json:"originator,omitempty"`
	CLIVersion               *string `json:"cli_version,omitempty"`
	Model                    *string `json:"model"`
	ThreadName               *string `json:"thread_name,omitempty"`
	AITitle                  *string `json:"ai_title"`
	DisplayTitle             *string `json:"display_title"`
	ApprovalMode             *string `json:"approval_mode,omitempty"`
	TurnCount                int     `json:"turn_count"`
	TotalTokens              int64   `json:"total_tokens"`
	InputTokens              int64   `json:"input_tokens"`
	CachedInputTokens        int64   `json:"cached_input_tokens"`
	CacheCreationInputTokens int64   `json:"cache_creation_input_tokens"`
	OutputTokens             int64   `json:"output_tokens"`
	ReasoningOutputTokens    int64   `json:"reasoning_output_tokens"`
	IsOngoing                bool    `json:"is_ongoing"`
	IsArchived               bool    `json:"is_archived"`
	IsSubagent               bool    `json:"is_subagent"`
	ParentSessionID          *string `json:"parent_session_id"`
	HistoryBaseThreadID      *string `json:"history_base_thread_id,omitempty"`
	ParserVersion            string  `json:"parser_version"`
	FileSize                 int64   `json:"file_size"`
	FileMtimeNs              int64   `json:"file_mtime_ns"`
	FileSHA256               *string `json:"file_sha256,omitempty"`
	LastIndexedOffset        int64   `json:"last_indexed_offset"`
	LastIndexedLine          int64   `json:"last_indexed_line"`
	ProjectedEventIndex      int     `json:"projected_event_index"`
	IndexStatus              string  `json:"index_status"`
	LastError                *string `json:"last_error,omitempty"`
	StartTime                *string `json:"start_time,omitempty"`
	EndTime                  *string `json:"end_time,omitempty"`
	LastActivityAt           *string `json:"last_activity_at,omitempty"`
	CreatedAt                string  `json:"created_at"`
	UpdatedAt                string  `json:"updated_at"`
}

// IndexCheckpoint stores durable per-source incremental indexing offsets and fingerprints.
type IndexCheckpoint struct {
	ID                string  `json:"id"`
	SourceKind        string  `json:"source_kind"`
	SourceIdentity    string  `json:"source_identity"`
	SourcePath        string  `json:"source_path"`
	RealPath          string  `json:"real_path"`
	AgentSessionID    string  `json:"agent_session_id"`
	ParserVersion     string  `json:"parser_version"`
	Status            string  `json:"status"`
	FileSize          int64   `json:"file_size"`
	FileMtimeNs       int64   `json:"file_mtime_ns"`
	LastIndexedOffset int64   `json:"last_indexed_offset"`
	LastIndexedLine   int64   `json:"last_indexed_line"`
	CursorJSON        string  `json:"cursor_json"`
	FingerprintJSON   string  `json:"fingerprint_json"`
	LastErrorKind     *string `json:"last_error_kind,omitempty"`
	LastError         *string `json:"last_error,omitempty"`
	LastAttemptAt     *string `json:"last_attempt_at,omitempty"`
	LastSuccessAt     *string `json:"last_success_at,omitempty"`
	CreatedAt         string  `json:"created_at"`
	UpdatedAt         string  `json:"updated_at"`
}

// AgentEvent stores one raw rollout event (decompressed from zstd on read).
type AgentEvent struct {
	ID             int64           `json:"-"`
	AgentSessionID string          `json:"-"`
	EventIndex     int             `json:"event_index"`
	LineNumber     int64           `json:"line_number"`
	ByteOffset     int64           `json:"byte_offset"`
	EventTimestamp *string         `json:"event_timestamp"`
	TopLevelType   *string         `json:"top_level_type"`
	PayloadType    *string         `json:"payload_type"`
	NormalizedType string          `json:"normalized_type"`
	RawJSON        json.RawMessage `json:"raw_json"`
	RawHash        string          `json:"-"`
	IsKnown        bool            `json:"is_known"`
	ParserVersion  string          `json:"-"`
	CreatedAt      string          `json:"-"`
}

// AgentTurn stores aggregated per-turn tokens, latency, and outcome metadata.
type AgentTurn struct {
	ID                       string  `json:"id"`
	Agent                    string  `json:"agent,omitempty"`
	AgentSessionID           string  `json:"agent_session_id,omitempty"`
	TurnIndex                int     `json:"turn_index"`
	TurnID                   *string `json:"turn_id"`
	Status                   string  `json:"status"`
	StartedAt                *string `json:"started_at"`
	CompletedAt              *string `json:"completed_at"`
	InputTokens              int64   `json:"input_tokens,omitempty"`
	CachedInputTokens        int64   `json:"cached_input_tokens,omitempty"`
	CacheCreationInputTokens int64   `json:"cache_creation_input_tokens,omitempty"`
	OutputTokens             int64   `json:"output_tokens,omitempty"`
	ReasoningOutputTokens    int64   `json:"reasoning_output_tokens,omitempty"`
	TotalTokens              int64   `json:"total_tokens"`
	DurationMS               *int64  `json:"duration_ms,omitempty"`
	FirstAgentLatencyMS      *int64  `json:"first_agent_latency_ms,omitempty"`
	ActiveToolMS             *int64  `json:"active_tool_ms,omitempty"`
	ValidationStatus         string  `json:"validation_status,omitempty"`
	Outcome                  string  `json:"outcome,omitempty"`
	MetricConfidence         string  `json:"metric_confidence,omitempty"`
	ErrorKind                *string `json:"error_kind"`
	ErrorMessage             *string `json:"error_message,omitempty"`
	CreatedAt                string  `json:"-"`
	UpdatedAt                string  `json:"-"`
}

// Message represents a projected user/assistant/system message.
type Message struct {
	ID                string  `json:"id"`
	AgentSessionID    string  `json:"-"`
	TurnID            *string `json:"turn_id"`
	EventID           *int64  `json:"-"`
	Role              string  `json:"role"`
	Channel           string  `json:"channel"`
	Sequence          int     `json:"sequence"`
	Text              string  `json:"text"`
	MarkdownText      *string `json:"markdown_text"`
	CopyText          string  `json:"copy_text"`
	Timestamp         *string `json:"timestamp"`
	IsVisible         bool    `json:"-"`
	IsSystemGenerated bool    `json:"-"`
	CreatedAt         string  `json:"-"`
}

// ToolCall represents a projected tool invocation and paired output.
type ToolCall struct {
	ID             string          `json:"id"`
	Agent          string          `json:"-"`
	AgentSessionID string          `json:"-"`
	TurnID         *string         `json:"-"`
	EventID        *int64          `json:"-"`
	CallID         string          `json:"call_id"`
	ToolName       string          `json:"tool_name"`
	ToolKind       string          `json:"tool_kind"`
	ArgumentsJSON  json.RawMessage `json:"arguments_json"`
	CommandText    *string         `json:"command_text"`
	OutputText     *string         `json:"output_text"`
	ExitCode       *int            `json:"exit_code"`
	Status         string          `json:"status"`
	IsMutating     bool            `json:"is_mutating"`
	StartedAt      *string         `json:"started_at"`
	CompletedAt    *string         `json:"completed_at,omitempty"`
	CreatedAt      string          `json:"-"`
}

// LocalDataSource represents a non-rollout local SQLite/log source and its cursor.
type LocalDataSource struct {
	ID                string  `json:"id"`
	Agent             string  `json:"agent"`
	SourceKind        string  `json:"source_kind"`
	SourceIdentity    string  `json:"source_identity,omitempty"`
	SourcePath        string  `json:"source_path"`
	SourceFormat      string  `json:"source_format"`
	ParserVersion     string  `json:"parser_version"`
	Status            string  `json:"status"`
	RecordCount       int64   `json:"record_count"`
	ByteSize          int64   `json:"byte_size"`
	CoverageStartedAt *string `json:"coverage_started_at"`
	CoverageEndedAt   *string `json:"coverage_ended_at"`
	BackfillCutoffAt  *string `json:"backfill_cutoff_at"`
	LastAttemptAt     *string `json:"last_attempt_at"`
	LastSuccessAt     *string `json:"last_success_at"`
	LastErrorKind     *string `json:"last_error_kind"`
	LastError         *string `json:"last_error"`
	CursorJSON        string  `json:"-"`
	FingerprintJSON   string  `json:"-"`
	CreatedAt         string  `json:"-"`
	UpdatedAt         string  `json:"updated_at"`
}

// EvidenceRecord stores local evidence metadata and optional compressed JSON payload.
type EvidenceRecord struct {
	ID                string          `json:"id"`
	SourceID          string          `json:"source_id"`
	SourceKind        string          `json:"source_kind,omitempty"`
	SourceRecordID    string          `json:"source_record_id"`
	AgentSessionID    *string         `json:"session_id"`
	AgentEventID      *int64          `json:"-"`
	ThreadID          *string         `json:"thread_id"`
	TurnID            *string         `json:"turn_id"`
	Category          string          `json:"category"`
	Level             *string         `json:"level"`
	Target            *string         `json:"target"`
	RecordedAt        *string         `json:"recorded_at"`
	Summary           *string         `json:"summary"`
	RawText           *string         `json:"raw_text"`
	RawJSON           json.RawMessage `json:"raw_json"`
	RawHash           string          `json:"raw_hash"`
	SourceLocatorJSON json.RawMessage `json:"source_locator"`
	MetadataJSON      json.RawMessage `json:"metadata"`
	CreatedAt         string          `json:"-"`
}

// ActivitySpan stores a normalized timed activity interval for timelines and analytics.
type ActivitySpan struct {
	ID             string          `json:"id"`
	Agent          string          `json:"-"`
	AgentSessionID string          `json:"-"`
	EvidenceID     *string         `json:"evidence_id"`
	SourceKind     string          `json:"source_kind"`
	SourceRecordID *string         `json:"source_record_id"`
	ThreadID       *string         `json:"thread_id,omitempty"`
	TurnID         *string         `json:"turn_id"`
	SpanType       string          `json:"type"`
	Name           *string         `json:"name"`
	Status         string          `json:"status"`
	StartedAt      string          `json:"started_at"`
	CompletedAt    *string         `json:"completed_at"`
	DurationMS     *int64          `json:"duration_ms"`
	Confidence     string          `json:"confidence"`
	MetadataJSON   json.RawMessage `json:"metadata,omitempty"`
	CreatedAt      string          `json:"-"`
}

// ThreadEdge links parent and child agent threads discovered in logs or rollouts.
type ThreadEdge struct {
	ID             string          `json:"id"`
	SourceID       string          `json:"-"`
	ParentThreadID string          `json:"source"`
	ChildThreadID  string          `json:"target"`
	Status         string          `json:"status"`
	EventTime      *string         `json:"event_time"`
	MetadataJSON   json.RawMessage `json:"metadata,omitempty"`
	CreatedAt      string          `json:"-"`
}

// SessionListItem matches the frontend HistorySession contract.
type SessionListItem struct {
	ID                  string   `json:"id"`
	ThreadID            string   `json:"thread_id"`
	RolloutPath         *string  `json:"rollout_path"`
	CWD                 *string  `json:"cwd"`
	WorkspaceID         *string  `json:"workspace_id"`
	WorkspaceKind       *string  `json:"workspace_kind"`
	WorkspaceConfidence *string  `json:"workspace_confidence"`
	AITitle             *string  `json:"ai_title"`
	DisplayTitle        *string  `json:"display_title"`
	Agent               string   `json:"agent"`
	IsSubagent          bool     `json:"is_subagent"`
	ParentSessionID     *string  `json:"parent_session_id"`
	SourceKind          string   `json:"source_kind"`
	SourcePath          *string  `json:"source_path"`
	Model               *string  `json:"model"`
	TurnCount           int      `json:"turn_count"`
	TotalTokens         int64    `json:"total_tokens"`
	IsOngoing           bool     `json:"is_ongoing"`
	UpdatedAt           string   `json:"updated_at"`
	ActivityAt          *string  `json:"activity_at"`
	IndexStatus         string   `json:"index_status"`
	LaunchSources       []string `json:"launch_sources"`
}

// SessionListFilter controls cursor-paginated session queries.
type SessionListFilter struct {
	Limit            int
	Cursor           string
	Query            string
	Agent            string
	WorkspaceID      string
	IsOngoing        *bool
	IncludeSubagents bool
}

// SessionListResult holds one page of sessions and the next opaque cursor.
type SessionListResult struct {
	Sessions   []SessionListItem `json:"sessions"`
	NextCursor *string           `json:"next_cursor"`
}

// SessionDetail matches the frontend HistoryDetail contract.
type SessionDetail struct {
	ID              string   `json:"id"`
	ThreadID        string   `json:"thread_id"`
	RolloutPath     *string  `json:"rollout_path"`
	CWD             *string  `json:"cwd"`
	ActivityAt      *string  `json:"activity_at"`
	Model           *string  `json:"model"`
	TurnCount       int      `json:"turn_count"`
	TotalTokens     int64    `json:"total_tokens"`
	DisplayTitle    *string  `json:"display_title"`
	AITitle         *string  `json:"ai_title"`
	Agent           string   `json:"agent"`
	IsSubagent      bool     `json:"is_subagent"`
	ParentSessionID *string  `json:"parent_session_id"`
	SourceKind      string   `json:"source_kind"`
	SourcePath      *string  `json:"source_path"`
	IsOngoing       bool     `json:"is_ongoing"`
	UpdatedAt       string   `json:"updated_at"`
	LaunchSources   []string `json:"launch_sources"`
}

// SearchMatch represents a single hit inside a session search.
type SearchMatch struct {
	Kind    string `json:"kind"`
	ID      string `json:"id"`
	Ref     string `json:"ref"`
	Snippet string `json:"snippet"`
}

// ProjectTokenTotals aggregates token usage for a project or window.
type ProjectTokenTotals struct {
	InputTokens           int64 `json:"input_tokens"`
	CachedInputTokens     int64 `json:"cached_input_tokens"`
	OutputTokens          int64 `json:"output_tokens"`
	ReasoningOutputTokens int64 `json:"reasoning_output_tokens"`
	TotalTokens           int64 `json:"total_tokens"`
}

// ProjectSelectedRangeTotals embeds range label alongside token totals.
type ProjectSelectedRangeTotals struct {
	Range                 string `json:"range"`
	InputTokens           int64  `json:"input_tokens"`
	CachedInputTokens     int64  `json:"cached_input_tokens"`
	OutputTokens          int64  `json:"output_tokens"`
	ReasoningOutputTokens int64  `json:"reasoning_output_tokens"`
	TotalTokens           int64  `json:"total_tokens"`
}

// ProjectSummary represents one project row in /api/projects.
type ProjectSummary struct {
	ID                  string                     `json:"id"`
	DisplayName         string                     `json:"display_name"`
	Path                string                     `json:"path"`
	IsAllowed           bool                       `json:"is_allowed"`
	WorkspaceKind       string                     `json:"workspace_kind"`
	WorkspaceConfidence string                     `json:"workspace_confidence"`
	LastActiveAt        *string                    `json:"last_active_at"`
	SessionCount        int                        `json:"session_count"`
	TurnCount           int                        `json:"turn_count"`
	AllTime             ProjectTokenTotals         `json:"all_time"`
	SelectedRange       ProjectSelectedRangeTotals `json:"selected_range"`
}

// ProjectListFilter filters and paginates /api/projects.
type ProjectListFilter struct {
	Limit  int
	Offset int
	Query  string
	Range  string
	Agent  string
	Model  string
	Now    time.Time
}

// ProjectsPage is returned by ListProjects.
type ProjectsPage struct {
	Items []ProjectSummary `json:"items"`
	Total int              `json:"total"`
	Range string           `json:"range"`
	Since *string          `json:"since"`
	Until string           `json:"until"`
}

// ProjectSplit breaks down project usage by agent or model.
type ProjectSplit struct {
	Agent                 *string `json:"agent,omitempty"`
	Model                 *string `json:"model,omitempty"`
	SessionCount          int     `json:"session_count"`
	TurnCount             int     `json:"turn_count"`
	InputTokens           int64   `json:"input_tokens"`
	CachedInputTokens     int64   `json:"cached_input_tokens"`
	OutputTokens          int64   `json:"output_tokens"`
	ReasoningOutputTokens int64   `json:"reasoning_output_tokens"`
	TotalTokens           int64   `json:"total_tokens"`
}

// ProjectMeta describes the workspace header in ProjectDetail.
type ProjectMeta struct {
	ID                  string  `json:"id"`
	DisplayName         string  `json:"display_name"`
	Path                string  `json:"path"`
	IsAllowed           bool    `json:"is_allowed"`
	WorkspaceKind       string  `json:"workspace_kind"`
	WorkspaceConfidence string  `json:"workspace_confidence"`
	FirstActiveAt       *string `json:"first_active_at"`
	LastActiveAt        *string `json:"last_active_at"`
}

// ProjectDetailTotals holds multi-window token summaries for a project.
type ProjectDetailTotals struct {
	AllTime ProjectTokenTotals `json:"all_time"`
	Day     ProjectTokenTotals `json:"day"`
	Week    ProjectTokenTotals `json:"week"`
	Month   ProjectTokenTotals `json:"month"`
}

// ProjectCounts holds session and turn counts for a project.
type ProjectCounts struct {
	SessionCount int `json:"session_count"`
	TurnCount    int `json:"turn_count"`
}

// ProjectDetail is returned by GetProject.
type ProjectDetail struct {
	Project ProjectMeta         `json:"project"`
	Totals  ProjectDetailTotals `json:"totals"`
	Counts  ProjectCounts       `json:"counts"`
	ByAgent []ProjectSplit      `json:"by_agent"`
	ByModel []ProjectSplit      `json:"by_model"`
}

// ProjectTimeseriesPoint is a single time bucket in a project timeseries.
type ProjectTimeseriesPoint struct {
	Timestamp             string `json:"timestamp"`
	SessionCount          int    `json:"session_count"`
	TurnCount             int    `json:"turn_count"`
	InputTokens           int64  `json:"input_tokens"`
	CachedInputTokens     int64  `json:"cached_input_tokens"`
	OutputTokens          int64  `json:"output_tokens"`
	ReasoningOutputTokens int64  `json:"reasoning_output_tokens"`
	TotalTokens           int64  `json:"total_tokens"`
}

// ProjectTimeseries is returned by GetProjectTimeseries.
type ProjectTimeseries struct {
	Since  string                   `json:"since"`
	Until  string                   `json:"until"`
	Bucket string                   `json:"bucket"`
	Points []ProjectTimeseriesPoint `json:"points"`
}

// InsightFilters scopes Insights queries to a UTC half-open window [Since, Until).
type InsightFilters struct {
	Since       time.Time
	Until       time.Time
	WorkspaceID string
	Model       string
	Agent       string
}

// OverviewTotals holds aggregate counts and token sums for Insights overview.
type OverviewTotals struct {
	SessionCount          int   `json:"session_count"`
	TurnCount             int   `json:"turn_count"`
	TotalTokens           int64 `json:"total_tokens"`
	InputTokens           int64 `json:"input_tokens"`
	CachedInputTokens     int64 `json:"cached_input_tokens"`
	OutputTokens          int64 `json:"output_tokens"`
	ReasoningOutputTokens int64 `json:"reasoning_output_tokens"`
}

// InsightsQuality breaks down turn outcomes, validation states, and confidence.
type InsightsQuality struct {
	Outcomes   map[string]int `json:"outcomes"`
	Validation map[string]int `json:"validation"`
	Confidence map[string]int `json:"confidence"`
}

// AgentComparison summarizes usage and reliability per agent.
type AgentComparison struct {
	Agent         string `json:"agent"`
	SessionCount  int    `json:"session_count"`
	TurnCount     int    `json:"turn_count"`
	TotalTokens   int64  `json:"total_tokens"`
	ToolCallCount int    `json:"tool_call_count"`
	SuccessCount  int    `json:"success_count,omitempty"`
	FailureCount  int    `json:"failure_count"`
	UnknownCount  int    `json:"unknown_count,omitempty"`
}

// InsightsOverview is returned by GetInsightsOverview.
type InsightsOverview struct {
	Since   string            `json:"since"`
	Until   string            `json:"until"`
	Totals  OverviewTotals    `json:"totals"`
	Quality InsightsQuality   `json:"quality"`
	ByAgent []AgentComparison `json:"by_agent"`
}

// TimeseriesPoint represents one UTC bucket in InsightsTimeseries.
type TimeseriesPoint struct {
	Timestamp    string `json:"timestamp"`
	TurnCount    int    `json:"turn_count"`
	SessionCount int    `json:"session_count"`
	TotalTokens  int64  `json:"total_tokens"`
	SuccessCount int    `json:"success_count"`
	FailureCount int    `json:"failure_count"`
	UnknownCount int    `json:"unknown_count"`
}

// InsightsTimeseries is returned by GetInsightsTimeseries.
type InsightsTimeseries struct {
	Since   string            `json:"since"`
	Until   string            `json:"until"`
	Bucket  string            `json:"bucket"`
	Points  []TimeseriesPoint `json:"points"`
	ByAgent []AgentComparison `json:"by_agent"`
}

// LatencyMeasure holds count, p50, p95, and average milliseconds.
type LatencyMeasure struct {
	Count   int     `json:"count"`
	P50     float64 `json:"p50"`
	P95     float64 `json:"p95"`
	Average float64 `json:"average"`
}

// LatencySummary groups the three latency dimensions plus confidence counts.
type LatencySummary struct {
	TurnDurationMS      LatencyMeasure `json:"turn_duration_ms"`
	FirstAgentLatencyMS LatencyMeasure `json:"first_agent_latency_ms"`
	ActiveToolMS        LatencyMeasure `json:"active_tool_ms"`
	Confidence          map[string]int `json:"confidence"`
}

// LatencyBucket is a duration histogram bin.
type LatencyBucket struct {
	Range string `json:"range"`
	Count int    `json:"count"`
}

// InsightsLatency is returned by GetInsightsLatency.
type InsightsLatency struct {
	Since        string          `json:"since"`
	Until        string          `json:"until"`
	Summary      LatencySummary  `json:"summary"`
	Distribution []LatencyBucket `json:"distribution"`
}

// ToolInsight aggregates calls, failures, and latency per tool.
type ToolInsight struct {
	ToolName      string  `json:"tool_name"`
	ToolKind      string  `json:"tool_kind"`
	CallCount     int     `json:"call_count"`
	FailureCount  int     `json:"failure_count"`
	FailureRate   float64 `json:"failure_rate"`
	DurationCount int     `json:"duration_count"`
	AverageMS     float64 `json:"average_ms"`
	P50MS         float64 `json:"p50_ms"`
	P95MS         float64 `json:"p95_ms"`
}

// InsightsTools is returned by GetInsightsTools.
type InsightsTools struct {
	Items   []ToolInsight     `json:"items"`
	ByAgent []AgentComparison `json:"by_agent"`
}

// AgentNode represents one thread in the subagent delegation graph.
type AgentNode struct {
	ThreadID    string  `json:"thread_id"`
	SessionID   *string `json:"session_id"`
	Title       *string `json:"title"`
	Model       *string `json:"model"`
	Agent       *string `json:"agent"`
	WorkspaceID *string `json:"workspace_id"`
	IsSubagent  *bool   `json:"is_subagent"`
	Outcome     *string `json:"outcome"`
	TokensUsed  *int64  `json:"tokens_used"`
}

// InsightsAgents is returned by GetInsightsAgents.
type InsightsAgents struct {
	Nodes     []AgentNode  `json:"nodes"`
	Edges     []ThreadEdge `json:"edges"`
	Truncated bool         `json:"truncated"`
}

// InsightsSources is returned by GetInsightsSources.
type InsightsSources struct {
	Sources []LocalDataSource `json:"sources"`
}

// SessionTimeline is returned by GetSessionTimeline.
type SessionTimeline struct {
	SessionID string         `json:"session_id"`
	Spans     []ActivitySpan `json:"spans"`
}

// InsightEvidenceSummary is a lightweight row in /api/insights/evidence.
type InsightEvidenceSummary struct {
	ID             string  `json:"id"`
	SourceID       string  `json:"source_id"`
	SourceKind     string  `json:"source_kind"`
	SourceRecordID string  `json:"source_record_id"`
	SessionID      *string `json:"session_id"`
	ThreadID       *string `json:"thread_id"`
	TurnID         *string `json:"turn_id"`
	RecordedAt     *string `json:"recorded_at"`
	Category       string  `json:"category"`
	Level          *string `json:"level"`
	Target         *string `json:"target"`
	Summary        *string `json:"summary"`
	RawSize        int     `json:"raw_size"`
}

// InsightEvidenceList is returned by ListEvidence.
type InsightEvidenceList struct {
	Items []InsightEvidenceSummary `json:"items"`
}

// SessionProjection groups the derived rows rebuilt atomically for one session.
type SessionProjection struct {
	Turns     []AgentTurn
	Messages  []Message
	ToolCalls []ToolCall
	Spans     []ActivitySpan
}

// AgyConversationMeta stores metadata read from agy's conversation_summaries.db.
type AgyConversationMeta struct {
	Title     string
	Workspace string
	ParentID  string
}

// CycleStats records counts and durations for a single indexer scan cycle.
type CycleStats struct {
	Discovered int     `json:"discovered"`
	Unchanged  int     `json:"unchanged"`
	Changed    int     `json:"changed"`
	Indexed    int     `json:"indexed"`
	Duplicates int     `json:"duplicates_skipped"`
	Failures   int     `json:"failures"`
	Finished   bool    `json:"finished"`
	DiscoverMS float64 `json:"discover_ms"`
	PlanMS     float64 `json:"plan_ms"`
	IngestMS   float64 `json:"ingest_ms"`
	CycleMS    float64 `json:"cycle_ms"`
}

// IndexerStatus summarizes live in-process indexer state for health/system endpoints.
type IndexerStatus struct {
	Status         string     `json:"status"`
	Running        bool       `json:"running"`
	StartedAt      string     `json:"started_at"`
	LastSeenAt     string     `json:"last_seen_at,omitempty"`
	LastSuccessAt  string     `json:"last_success_at,omitempty"`
	LastError      string     `json:"last_error,omitempty"`
	ActiveSessions int        `json:"active_sessions"`
	DetectedAgents []string   `json:"detected_agents"`
	LastCycle      CycleStats `json:"last_cycle"`
}
