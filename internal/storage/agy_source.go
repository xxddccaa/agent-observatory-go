package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/agent-observatory/agent-observatory-go/internal/model"
)

// ReadAgyConversationSummaries reads title, workspace, and parent links from agy's SQLite index
// using immutable=1 when no WAL file exists so -shm/-wal files are never created.
func ReadAgyConversationSummaries(ctx context.Context, dbPath string, hasWAL bool) (map[string]model.AgyConversationMeta, error) {
	absPath, err := filepath.Abs(dbPath)
	if err != nil {
		absPath = dbPath
	}
	params := url.Values{}
	params.Set("mode", "ro")
	params.Add("_pragma", "query_only(ON)")
	params.Add("_pragma", "busy_timeout(1000)")
	if !hasWAL {
		params.Set("immutable", "1")
	}
	dsn := (&url.URL{
		Scheme:   "file",
		Path:     filepath.ToSlash(absPath),
		RawQuery: params.Encode(),
	}).String()

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open agy summaries %s: %w", dbPath, err)
	}
	defer func() { _ = db.Close() }()

	colRows, err := db.QueryContext(ctx, `PRAGMA table_info(conversation_summaries)`)
	if err != nil {
		return nil, fmt.Errorf("inspect agy conversation_summaries: %w", err)
	}
	available := make(map[string]bool)
	for colRows.Next() {
		var cid int
		var name, colType string
		var notNull, pk int
		var dflt sql.NullString
		if scanErr := colRows.Scan(&cid, &name, &colType, &notNull, &dflt, &pk); scanErr != nil {
			_ = colRows.Close()
			return nil, scanErr
		}
		available[name] = true
	}
	_ = colRows.Close()
	if !available["conversation_id"] {
		return map[string]model.AgyConversationMeta{}, nil
	}

	cols := []string{"conversation_id"}
	for _, col := range []string{"title", "workspace_uris", "parent_conversation_id"} {
		if available[col] {
			cols = append(cols, col)
		}
	}

	rows, err := db.QueryContext(ctx, fmt.Sprintf(`SELECT %s FROM conversation_summaries`, strings.Join(cols, ", ")))
	if err != nil {
		return nil, fmt.Errorf("query agy conversation_summaries: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make(map[string]model.AgyConversationMeta)
	vals := make([]sql.NullString, len(cols))
	scanDest := make([]any, len(cols))
	for i := range vals {
		scanDest[i] = &vals[i]
	}

	for rows.Next() {
		if scanErr := rows.Scan(scanDest...); scanErr != nil {
			return nil, scanErr
		}
		convID := strings.TrimSpace(vals[0].String)
		if convID == "" {
			continue
		}
		var meta model.AgyConversationMeta
		for i, col := range cols[1:] {
			raw := vals[i+1].String
			switch col {
			case "title":
				meta.Title = strings.TrimSpace(raw)
			case "workspace_uris":
				meta.Workspace = extractFirstWorkspaceURI(raw)
			case "parent_conversation_id":
				meta.ParentID = strings.TrimSpace(raw)
			}
		}
		out[convID] = meta
	}
	return out, rows.Err()
}

func extractFirstWorkspaceURI(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	var uris []string
	if err := json.Unmarshal([]byte(raw), &uris); err != nil {
		return ""
	}
	for _, rawURI := range uris {
		u, err := url.Parse(rawURI)
		if err == nil && u.Scheme == "file" && u.Path != "" {
			return u.Path
		}
		if strings.HasPrefix(rawURI, "/") {
			return rawURI
		}
	}
	return ""
}
