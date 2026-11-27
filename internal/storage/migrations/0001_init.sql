-- Baseline schema for Phase 0 skeleton; subsequent tables arrive in Phase 2 migrations.
CREATE TABLE IF NOT EXISTS workspaces (
    id TEXT PRIMARY KEY,
    path TEXT NOT NULL,
    normalized_path TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    git_remote TEXT,
    git_branch TEXT,
    workspace_kind TEXT NOT NULL DEFAULT 'unknown',
    workspace_confidence TEXT NOT NULL DEFAULT 'low',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
