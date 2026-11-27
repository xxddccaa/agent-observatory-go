package storage

import (
	"context"
	"testing"
)

func TestOpenEnablesWALAndReadIsolation(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() {
		if closeErr := store.Close(); closeErr != nil {
			t.Fatalf("Close() error = %v", closeErr)
		}
	}()

	if err := store.Ping(ctx); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}

	var mode string
	if err := store.Reader().QueryRowContext(ctx, "PRAGMA journal_mode;").Scan(&mode); err != nil {
		t.Fatalf("query journal_mode error = %v", err)
	}
	if mode != "wal" {
		t.Fatalf("journal_mode = %q, want wal", mode)
	}

	var applied string
	if err := store.Reader().QueryRowContext(ctx, "SELECT version FROM schema_migrations WHERE version = '0001_init'").Scan(&applied); err != nil {
		t.Fatalf("query schema_migrations error = %v", err)
	}

	// Reader pool enforces query_only=ON so HTTP queries cannot mutate state.
	if _, err := store.Reader().ExecContext(
		ctx,
		`INSERT INTO workspaces (id, path, normalized_path, display_name, created_at, updated_at)
		 VALUES ('w1', '/tmp/a', '/tmp/a', 'a', '2026-11-26T00:00:00.000000Z', '2026-11-26T00:00:00.000000Z')`,
	); err == nil {
		t.Fatal("expected write on reader pool to fail under query_only=ON")
	}

	if _, err := store.Writer().ExecContext(
		ctx,
		`INSERT INTO workspaces (id, path, normalized_path, display_name, created_at, updated_at)
		 VALUES ('w1', '/tmp/a', '/tmp/a', 'a', '2026-11-26T00:00:00.000000Z', '2026-11-26T00:00:00.000000Z')`,
	); err != nil {
		t.Fatalf("insert via writer pool error = %v", err)
	}

	var count int
	if err := store.Reader().QueryRowContext(ctx, "SELECT COUNT(*) FROM workspaces").Scan(&count); err != nil {
		t.Fatalf("select workspaces error = %v", err)
	}
	if count != 1 {
		t.Fatalf("workspace count = %d, want 1", count)
	}
}
