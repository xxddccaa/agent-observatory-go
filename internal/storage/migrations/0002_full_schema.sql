-- Complete V1 SQLite schema: 11 business tables with foreign-key covering indexes.
-- Note: workspaces table was created in 0001_init.sql.

ALTER TABLE workspaces ADD COLUMN is_allowed INTEGER NOT NULL DEFAULT 1;

CREATE TABLE IF NOT EXISTS agent_sessions (
    id TEXT PRIMARY KEY,
    agent TEXT NOT NULL DEFAULT 'codex',
    source_kind TEXT NOT NULL DEFAULT 'codex_jsonl',
    source_identity TEXT NOT NULL UNIQUE,
    rollout_path TEXT,
    rollout_real_path TEXT UNIQUE,
    source_path TEXT,
    rollout_format TEXT NOT NULL DEFAULT 'jsonl',
    source_format TEXT NOT NULL DEFAULT 'jsonl',
    rollout_compression TEXT,
    source_compression TEXT,
    thread_id TEXT NOT NULL,
    workspace_id TEXT REFERENCES workspaces(id) ON DELETE SET NULL,
    cwd TEXT,
    git_branch TEXT,
    originator TEXT,
    cli_version TEXT,
    model TEXT,
    thread_name TEXT,
    ai_title TEXT,
    display_title TEXT,
    approval_mode TEXT,
    turn_count INTEGER NOT NULL DEFAULT 0,
    total_tokens INTEGER NOT NULL DEFAULT 0,
    input_tokens INTEGER NOT NULL DEFAULT 0,
    cached_input_tokens INTEGER NOT NULL DEFAULT 0,
    cache_creation_input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    reasoning_output_tokens INTEGER NOT NULL DEFAULT 0,
    is_ongoing INTEGER NOT NULL DEFAULT 0,
    is_archived INTEGER NOT NULL DEFAULT 0,
    is_subagent INTEGER NOT NULL DEFAULT 0,
    parent_session_id TEXT REFERENCES agent_sessions(id) ON DELETE SET NULL,
    history_base_thread_id TEXT,
    parser_version TEXT NOT NULL DEFAULT '1',
    file_size INTEGER NOT NULL DEFAULT 0,
    file_mtime_ns INTEGER NOT NULL DEFAULT 0,
    file_sha256 TEXT,
    last_indexed_offset INTEGER NOT NULL DEFAULT 0,
    last_indexed_line INTEGER NOT NULL DEFAULT 0,
    projected_event_index INTEGER NOT NULL DEFAULT -1,
    index_status TEXT NOT NULL DEFAULT 'discovered',
    last_error TEXT,
    start_time TEXT,
    end_time TEXT,
    last_activity_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_agent_sessions_workspace_id ON agent_sessions (workspace_id);
CREATE INDEX IF NOT EXISTS idx_agent_sessions_parent_session_id ON agent_sessions (parent_session_id);
CREATE INDEX IF NOT EXISTS idx_agent_sessions_thread_id ON agent_sessions (thread_id);
CREATE INDEX IF NOT EXISTS idx_agent_sessions_agent_updated ON agent_sessions (agent, updated_at);
CREATE INDEX IF NOT EXISTS idx_agent_sessions_last_activity ON agent_sessions (last_activity_at);
CREATE INDEX IF NOT EXISTS idx_agent_sessions_workspace_updated ON agent_sessions (workspace_id, updated_at);

CREATE TABLE IF NOT EXISTS index_checkpoints (
    id TEXT PRIMARY KEY,
    source_kind TEXT NOT NULL,
    source_identity TEXT NOT NULL UNIQUE,
    source_path TEXT NOT NULL,
    real_path TEXT NOT NULL,
    agent_session_id TEXT NOT NULL UNIQUE REFERENCES agent_sessions(id) ON DELETE CASCADE,
    parser_version TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'discovered',
    file_size INTEGER NOT NULL DEFAULT 0,
    file_mtime_ns INTEGER NOT NULL DEFAULT 0,
    last_indexed_offset INTEGER NOT NULL DEFAULT 0,
    last_indexed_line INTEGER NOT NULL DEFAULT 0,
    cursor_json TEXT NOT NULL DEFAULT '{}',
    fingerprint_json TEXT NOT NULL DEFAULT '{}',
    last_error_kind TEXT,
    last_error TEXT,
    last_attempt_at TEXT,
    last_success_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_index_checkpoints_agent_session_id ON index_checkpoints (agent_session_id);
CREATE INDEX IF NOT EXISTS idx_index_checkpoints_status_updated ON index_checkpoints (status, updated_at);

CREATE TABLE IF NOT EXISTS agent_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_session_id TEXT NOT NULL REFERENCES agent_sessions(id) ON DELETE CASCADE,
    event_index INTEGER NOT NULL,
    line_number INTEGER NOT NULL,
    byte_offset INTEGER NOT NULL,
    event_timestamp TEXT,
    top_level_type TEXT,
    payload_type TEXT,
    normalized_type TEXT NOT NULL,
    raw_json_zstd BLOB NOT NULL,
    raw_hash TEXT NOT NULL,
    is_known INTEGER NOT NULL DEFAULT 0,
    parser_version TEXT NOT NULL DEFAULT '1',
    created_at TEXT NOT NULL,
    UNIQUE (agent_session_id, event_index)
);

CREATE INDEX IF NOT EXISTS idx_agent_events_session_line ON agent_events (agent_session_id, line_number);

CREATE TABLE IF NOT EXISTS agent_turns (
    id TEXT PRIMARY KEY,
    agent TEXT NOT NULL DEFAULT 'codex',
    agent_session_id TEXT NOT NULL REFERENCES agent_sessions(id) ON DELETE CASCADE,
    turn_index INTEGER NOT NULL,
    turn_id TEXT,
    status TEXT NOT NULL DEFAULT 'unknown',
    started_at TEXT,
    completed_at TEXT,
    input_tokens INTEGER NOT NULL DEFAULT 0,
    cached_input_tokens INTEGER NOT NULL DEFAULT 0,
    cache_creation_input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    reasoning_output_tokens INTEGER NOT NULL DEFAULT 0,
    total_tokens INTEGER NOT NULL DEFAULT 0,
    duration_ms INTEGER,
    first_agent_latency_ms INTEGER,
    active_tool_ms INTEGER,
    validation_status TEXT NOT NULL DEFAULT 'unknown',
    outcome TEXT NOT NULL DEFAULT 'unknown',
    metric_confidence TEXT NOT NULL DEFAULT 'derived',
    error_kind TEXT,
    error_message TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (agent_session_id, turn_index)
);

CREATE INDEX IF NOT EXISTS idx_agent_turns_session_id ON agent_turns (agent_session_id);
CREATE INDEX IF NOT EXISTS idx_agent_turns_activity_time ON agent_turns (coalesce(completed_at, started_at, created_at));
CREATE INDEX IF NOT EXISTS idx_agent_turns_agent_started ON agent_turns (agent, started_at);
CREATE INDEX IF NOT EXISTS idx_agent_turns_outcome_started ON agent_turns (outcome, started_at);
CREATE INDEX IF NOT EXISTS idx_agent_turns_started ON agent_turns (started_at);

CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    agent_session_id TEXT NOT NULL REFERENCES agent_sessions(id) ON DELETE CASCADE,
    turn_id TEXT REFERENCES agent_turns(id) ON DELETE SET NULL,
    event_id INTEGER REFERENCES agent_events(id) ON DELETE SET NULL,
    role TEXT NOT NULL,
    channel TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    text TEXT NOT NULL,
    markdown_text TEXT,
    copy_text TEXT NOT NULL,
    message_timestamp TEXT,
    is_visible INTEGER NOT NULL DEFAULT 1,
    is_system_generated INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    UNIQUE (agent_session_id, sequence)
);

CREATE INDEX IF NOT EXISTS idx_messages_turn_id ON messages (turn_id);
CREATE INDEX IF NOT EXISTS idx_messages_event_id ON messages (event_id);

CREATE TABLE IF NOT EXISTS tool_calls (
    id TEXT PRIMARY KEY,
    agent TEXT NOT NULL DEFAULT 'codex',
    agent_session_id TEXT NOT NULL REFERENCES agent_sessions(id) ON DELETE CASCADE,
    turn_id TEXT REFERENCES agent_turns(id) ON DELETE SET NULL,
    event_id INTEGER REFERENCES agent_events(id) ON DELETE SET NULL,
    call_id TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    tool_kind TEXT NOT NULL DEFAULT 'unknown',
    arguments_json TEXT,
    command_text TEXT,
    output_text TEXT,
    exit_code INTEGER,
    status TEXT NOT NULL DEFAULT 'pending',
    is_mutating INTEGER NOT NULL DEFAULT 0,
    started_at TEXT,
    completed_at TEXT,
    created_at TEXT NOT NULL,
    UNIQUE (agent_session_id, call_id)
);

CREATE INDEX IF NOT EXISTS idx_tool_calls_turn_id ON tool_calls (turn_id);
CREATE INDEX IF NOT EXISTS idx_tool_calls_event_id ON tool_calls (event_id);
CREATE INDEX IF NOT EXISTS idx_tool_calls_agent_tool ON tool_calls (agent, tool_name);
CREATE INDEX IF NOT EXISTS idx_tool_calls_started_at ON tool_calls (started_at);
CREATE INDEX IF NOT EXISTS idx_tool_calls_session_started ON tool_calls (agent_session_id, started_at);

CREATE TABLE IF NOT EXISTS local_data_sources (
    id TEXT PRIMARY KEY,
    agent TEXT NOT NULL DEFAULT 'codex',
    source_kind TEXT NOT NULL,
    source_identity TEXT NOT NULL UNIQUE,
    source_path TEXT NOT NULL UNIQUE,
    source_format TEXT NOT NULL,
    parser_version TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'discovered',
    record_count INTEGER NOT NULL DEFAULT 0,
    byte_size INTEGER NOT NULL DEFAULT 0,
    coverage_started_at TEXT,
    coverage_ended_at TEXT,
    backfill_cutoff_at TEXT,
    last_attempt_at TEXT,
    last_success_at TEXT,
    last_error_kind TEXT,
    last_error TEXT,
    cursor_json TEXT NOT NULL DEFAULT '{}',
    fingerprint_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_local_data_sources_agent_status ON local_data_sources (agent, status);

CREATE TABLE IF NOT EXISTS evidence_records (
    id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL REFERENCES local_data_sources(id) ON DELETE CASCADE,
    source_record_id TEXT NOT NULL,
    agent_session_id TEXT REFERENCES agent_sessions(id) ON DELETE SET NULL,
    agent_event_id INTEGER REFERENCES agent_events(id) ON DELETE SET NULL,
    thread_id TEXT,
    turn_id TEXT,
    category TEXT NOT NULL,
    level TEXT,
    target TEXT,
    recorded_at TEXT,
    summary TEXT,
    raw_text TEXT,
    raw_json_zstd BLOB,
    raw_hash TEXT NOT NULL,
    source_locator_json TEXT NOT NULL DEFAULT '{}',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    UNIQUE (source_id, source_record_id)
);

CREATE INDEX IF NOT EXISTS idx_evidence_records_source_id ON evidence_records (source_id);
CREATE INDEX IF NOT EXISTS idx_evidence_records_agent_session_id ON evidence_records (agent_session_id);
CREATE INDEX IF NOT EXISTS idx_evidence_records_agent_event_id ON evidence_records (agent_event_id);
CREATE INDEX IF NOT EXISTS idx_evidence_records_recorded_at ON evidence_records (recorded_at);

CREATE TABLE IF NOT EXISTS activity_spans (
    id TEXT PRIMARY KEY,
    agent TEXT NOT NULL DEFAULT 'codex',
    agent_session_id TEXT NOT NULL REFERENCES agent_sessions(id) ON DELETE CASCADE,
    evidence_id TEXT REFERENCES evidence_records(id) ON DELETE SET NULL,
    source_kind TEXT NOT NULL DEFAULT 'rollout',
    source_record_id TEXT,
    thread_id TEXT,
    turn_id TEXT,
    span_type TEXT NOT NULL,
    name TEXT,
    status TEXT NOT NULL DEFAULT 'unknown',
    started_at TEXT NOT NULL,
    completed_at TEXT,
    duration_ms INTEGER,
    confidence TEXT NOT NULL DEFAULT 'derived',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_activity_spans_agent_session_id ON activity_spans (agent_session_id);
CREATE INDEX IF NOT EXISTS idx_activity_spans_evidence_id ON activity_spans (evidence_id);
CREATE INDEX IF NOT EXISTS idx_activity_spans_started_at ON activity_spans (started_at);

CREATE TABLE IF NOT EXISTS thread_edges (
    id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL REFERENCES local_data_sources(id) ON DELETE CASCADE,
    parent_thread_id TEXT NOT NULL,
    child_thread_id TEXT NOT NULL,
    status TEXT NOT NULL,
    event_time TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    UNIQUE (parent_thread_id, child_thread_id)
);

CREATE INDEX IF NOT EXISTS idx_thread_edges_source_id ON thread_edges (source_id);
CREATE INDEX IF NOT EXISTS idx_thread_edges_parent_thread_id ON thread_edges (parent_thread_id);
