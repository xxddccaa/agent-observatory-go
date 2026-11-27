-- pragma:disable-foreign-keys
-- Remove the self-referential foreign key on agent_sessions.parent_session_id so subagent
-- rollouts storing parent native thread_id (or arriving before/without their parent) ingest cleanly.
CREATE TABLE agent_sessions_new (
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
    parent_session_id TEXT,
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

INSERT INTO agent_sessions_new (
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
)
SELECT
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
FROM agent_sessions;

DROP TABLE agent_sessions;
ALTER TABLE agent_sessions_new RENAME TO agent_sessions;

CREATE INDEX IF NOT EXISTS idx_agent_sessions_workspace_id ON agent_sessions (workspace_id);
CREATE INDEX IF NOT EXISTS idx_agent_sessions_parent_session_id ON agent_sessions (parent_session_id);
CREATE INDEX IF NOT EXISTS idx_agent_sessions_thread_id ON agent_sessions (thread_id);
CREATE INDEX IF NOT EXISTS idx_agent_sessions_agent_updated ON agent_sessions (agent, updated_at);
CREATE INDEX IF NOT EXISTS idx_agent_sessions_last_activity ON agent_sessions (last_activity_at);
CREATE INDEX IF NOT EXISTS idx_agent_sessions_workspace_updated ON agent_sessions (workspace_id, updated_at);
