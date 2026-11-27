package storage

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/agent-observatory/agent-observatory-go/internal/model"
	_ "modernc.org/sqlite"
)

var (
	ErrNotFound      = errors.New("record not found")
	ErrInvalidCursor = errors.New("invalid cursor")
	ErrInvalidFilter = errors.New("invalid filter")
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Store isolates a single SQLite writer connection from concurrent read-only queries.
type Store struct {
	dbPath string
	writer *sql.DB
	reader *sql.DB
}

func Open(ctx context.Context, dataDir string) (*Store, error) {
	dbPath := filepath.Join(dataDir, "observatory.db")
	writerDSN := sqliteDSN(dbPath, false)
	writer, err := sql.Open("sqlite", writerDSN)
	if err != nil {
		return nil, fmt.Errorf("open sqlite writer: %w", err)
	}
	writer.SetMaxOpenConns(1)
	writer.SetMaxIdleConns(1)

	if err := verifyWAL(ctx, writer); err != nil {
		_ = writer.Close()
		return nil, err
	}
	if err := migrate(ctx, writer); err != nil {
		_ = writer.Close()
		return nil, err
	}

	readerDSN := sqliteDSN(dbPath, true)
	reader, err := sql.Open("sqlite", readerDSN)
	if err != nil {
		_ = writer.Close()
		return nil, fmt.Errorf("open sqlite reader: %w", err)
	}
	reader.SetMaxOpenConns(4)
	reader.SetMaxIdleConns(4)

	if err := reader.PingContext(ctx); err != nil {
		_ = reader.Close()
		_ = writer.Close()
		return nil, fmt.Errorf("ping sqlite reader: %w", err)
	}

	return &Store{
		dbPath: dbPath,
		writer: writer,
		reader: reader,
	}, nil
}

func sqliteDSN(dbPath string, readOnly bool) string {
	params := url.Values{}
	params.Add("_pragma", "journal_mode(WAL)")
	params.Add("_pragma", "synchronous(NORMAL)")
	params.Add("_pragma", "foreign_keys(ON)")
	params.Add("_pragma", "busy_timeout(5000)")
	params.Add("_pragma", "temp_store(MEMORY)")
	if readOnly {
		params.Add("_pragma", "query_only(ON)")
	}
	u := url.URL{
		Scheme:   "file",
		Path:     filepath.ToSlash(dbPath),
		RawQuery: params.Encode(),
	}
	return u.String()
}

func verifyWAL(ctx context.Context, db *sql.DB) error {
	var mode string
	if err := db.QueryRowContext(ctx, "PRAGMA journal_mode=WAL;").Scan(&mode); err != nil {
		return fmt.Errorf("enable sqlite WAL: %w", err)
	}
	if !strings.EqualFold(mode, "wal") {
		return fmt.Errorf("expected sqlite WAL journal mode, got %q", mode)
	}
	return nil
}

func migrate(ctx context.Context, db *sql.DB) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("pin sqlite migration conn: %w", err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version TEXT PRIMARY KEY,
			applied_at TEXT NOT NULL
		);
	`); err != nil {
		return fmt.Errorf("ensure schema_migrations: %w", err)
	}

	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("read embedded migrations: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		version := strings.TrimSuffix(entry.Name(), ".sql")
		var existing string
		err := conn.QueryRowContext(ctx, "SELECT version FROM schema_migrations WHERE version = ?", version).Scan(&existing)
		if err == nil {
			continue
		}
		if err != sql.ErrNoRows {
			return fmt.Errorf("check migration %s: %w", version, err)
		}

		sqlBytes, err := migrationsFS.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return fmt.Errorf("read migration %s: %w", version, err)
		}
		sqlText := string(sqlBytes)
		disableFK := strings.Contains(sqlText, "-- pragma:disable-foreign-keys")
		if disableFK {
			// SQLite ignores PRAGMA foreign_keys=OFF inside an active transaction.
			if _, err := conn.ExecContext(ctx, "PRAGMA foreign_keys = OFF;"); err != nil {
				return fmt.Errorf("disable foreign keys for %s: %w", version, err)
			}
		}

		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			if disableFK {
				_, _ = conn.ExecContext(ctx, "PRAGMA foreign_keys = ON;")
			}
			return fmt.Errorf("begin migration %s: %w", version, err)
		}
		if _, err := tx.ExecContext(ctx, sqlText); err != nil {
			_ = tx.Rollback()
			if disableFK {
				_, _ = conn.ExecContext(ctx, "PRAGMA foreign_keys = ON;")
			}
			return fmt.Errorf("exec migration %s: %w", version, err)
		}
		if _, err := tx.ExecContext(
			ctx,
			"INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
			version,
			model.FormatUTC(time.Now()),
		); err != nil {
			_ = tx.Rollback()
			if disableFK {
				_, _ = conn.ExecContext(ctx, "PRAGMA foreign_keys = ON;")
			}
			return fmt.Errorf("record migration %s: %w", version, err)
		}
		if err := tx.Commit(); err != nil {
			if disableFK {
				_, _ = conn.ExecContext(ctx, "PRAGMA foreign_keys = ON;")
			}
			return fmt.Errorf("commit migration %s: %w", version, err)
		}
		if disableFK {
			if _, err := conn.ExecContext(ctx, "PRAGMA foreign_keys = ON;"); err != nil {
				return fmt.Errorf("restore foreign keys after %s: %w", version, err)
			}
			rows, err := conn.QueryContext(ctx, "PRAGMA foreign_key_check;")
			if err != nil {
				return fmt.Errorf("foreign key check after %s: %w", version, err)
			}
			hasViolation := rows.Next()
			_ = rows.Close()
			if hasViolation {
				return fmt.Errorf("foreign key integrity violation after migration %s", version)
			}
		}
	}
	return nil
}

func (s *Store) Ping(ctx context.Context) error {
	var ok int
	return s.reader.QueryRowContext(ctx, "SELECT 1").Scan(&ok)
}

func (s *Store) Reader() *sql.DB {
	return s.reader
}

func (s *Store) Writer() *sql.DB {
	return s.writer
}

// WithWriterTx executes fn inside a serialized SQLite writer transaction.
func (s *Store) WithWriterTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin writer tx: %w", err)
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit writer tx: %w", err)
	}
	return nil
}

// EscapeLike escapes SQL LIKE wildcards (\, %, _) for ESCAPE '\'.
func EscapeLike(term string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return replacer.Replace(term)
}

func nullStrPtr(v sql.NullString) *string {
	if !v.Valid {
		return nil
	}
	val := v.String
	return &val
}

func nullIntPtr(v sql.NullInt64) *int {
	if !v.Valid {
		return nil
	}
	val := int(v.Int64)
	return &val
}

func nullInt64Ptr(v sql.NullInt64) *int64 {
	if !v.Valid {
		return nil
	}
	val := v.Int64
	return &val
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func defaultString(val, fallback string) string {
	if val == "" {
		return fallback
	}
	return val
}

func defaultJSON(val string) string {
	if strings.TrimSpace(val) == "" {
		return "{}"
	}
	return val
}

func (s *Store) Close() error {
	var firstErr error
	if s.reader != nil {
		if err := s.reader.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.writer != nil {
		// Truncate WAL cleanly on orderly shutdown so the single .db stays compact.
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, _ = s.writer.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE);")
		cancel()
		if err := s.writer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
