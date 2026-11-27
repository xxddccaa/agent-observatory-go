package discovery

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestProbeAgentsDetectsKnownAndExtraRoots(t *testing.T) {
	home := t.TempDir()
	codexDir := filepath.Join(home, "codex-sessions")
	extraDir := filepath.Join(home, "custom-agent")
	if err := os.MkdirAll(codexDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(extraDir, 0o755); err != nil {
		t.Fatal(err)
	}

	layouts := map[string][]string{
		"codex":  {codexDir},
		"claude": {filepath.Join(home, "missing-claude")},
	}
	extra := map[string][]string{
		"custom": {extraDir},
	}

	roots := ProbeAgents(extra, layouts)
	if len(roots) != 3 {
		t.Fatalf("ProbeAgents len = %d, want 3: %+v", len(roots), roots)
	}
	detected := DetectedAgents(extra, layouts)
	if len(detected) != 2 || detected[0] != "codex" || detected[1] != "custom" {
		t.Fatalf("DetectedAgents = %v, want [codex custom]", detected)
	}
}

func TestTreeCacheIncrementalScanAndSymlinkSafety(t *testing.T) {
	root := t.TempDir()
	subA := filepath.Join(root, "2026", "11")
	subB := filepath.Join(root, "empty")
	if err := os.MkdirAll(subA, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(subB, 0o755); err != nil {
		t.Fatal(err)
	}

	file1 := filepath.Join(subA, "rollout-1.jsonl")
	if err := os.WriteFile(file1, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Create a directory symlink to an outside directory containing a matching file;
	// TreeCache must not descend into symlinked subdirectories.
	if runtime.GOOS != "windows" {
		outside := t.TempDir()
		if err := os.WriteFile(filepath.Join(outside, "rollout-outside.jsonl"), []byte("{}\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, filepath.Join(root, "sym-outside")); err != nil {
			t.Fatal(err)
		}
	}

	now := time.Now()
	clock := func() time.Time { return now }
	cache := NewTreeCache(root, "rollout-*.jsonl*", clock)

	matches := cache.Glob(true)
	if len(matches) != 1 || matches[0] != file1 {
		t.Fatalf("initial Glob = %v, want [%s]", matches, file1)
	}

	// Advance virtual clock beyond hot window and racy window; empty dir should not be re-listed on fast cycle.
	now = now.Add(20 * time.Minute)
	// Age mtimes on disk so racy check does not keep subB hot.
	oldTime := time.Now().Add(-20 * time.Minute)
	for _, p := range []string{root, filepath.Join(root, "2026"), subA, subB} {
		_ = os.Chtimes(p, oldTime, oldTime)
	}
	// Rebuild once with old mtimes so racy flags clear.
	cache = NewTreeCache(root, "rollout-*.jsonl*", func() time.Time { return time.Now() })
	_ = cache.Glob(true)
	relistedBefore := cache.Stats().Relisted

	// Fast poll when nothing changed should not re-list any directory.
	matches = cache.Glob(false)
	if len(matches) != 1 {
		t.Fatalf("fast Glob len = %d, want 1", len(matches))
	}
	if cache.Stats().Relisted != relistedBefore {
		t.Fatalf("expected 0 relists on unchanged fast poll, got %d -> %d", relistedBefore, cache.Stats().Relisted)
	}

	// Adding a new file inside structural directory subA updates subA's mtime and is picked up on fast poll.
	file2 := filepath.Join(subA, "rollout-2.jsonl")
	if err := os.WriteFile(file2, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	matches = cache.Glob(false)
	if len(matches) != 2 {
		t.Fatalf("fast Glob after new file = %v, want 2 matches", matches)
	}
}

func TestSQLiteSignatureIgnoresSHMAndDetectsWALOrCounter(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "summaries.db")

	// Construct a minimal 100-byte SQLite header with magic + big-endian change counter at bytes 24..28.
	header := make([]byte, 100)
	copy(header[:16], []byte("SQLite format 3\x00"))
	header[24], header[25], header[26], header[27] = 0, 0, 0, 1
	if err := os.WriteFile(dbPath, header, 0o600); err != nil {
		t.Fatal(err)
	}

	sig1, err := ReadSQLiteSignature(dbPath)
	if err != nil {
		t.Fatalf("ReadSQLiteSignature: %v", err)
	}
	if sig1.ChangeCounter == nil || *sig1.ChangeCounter != 1 || sig1.HasWAL {
		t.Fatalf("unexpected initial signature: %+v", sig1)
	}

	// Writing -shm must NOT alter the signature.
	shmPath := dbPath + "-shm"
	if err := os.WriteFile(shmPath, []byte("shm-noise"), 0o600); err != nil {
		t.Fatal(err)
	}
	sigAfterSHM, err := ReadSQLiteSignature(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if !sig1.Equal(sigAfterSHM) {
		t.Fatalf("expected -shm write not to change signature: %+v vs %+v", sig1, sigAfterSHM)
	}

	// Writing -wal MUST alter the signature.
	walPath := dbPath + "-wal"
	if err := os.WriteFile(walPath, []byte("wal-frame"), 0o600); err != nil {
		t.Fatal(err)
	}
	sigAfterWAL, err := ReadSQLiteSignature(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if sig1.Equal(sigAfterWAL) || !sigAfterWAL.HasWAL {
		t.Fatalf("expected -wal write to change signature and set HasWAL=true")
	}

	// Bumping the change counter in-place (keeping file size identical) MUST alter the signature.
	header[27] = 2
	if err := os.WriteFile(dbPath, header, 0o600); err != nil {
		t.Fatal(err)
	}
	sigAfterCounter, err := ReadSQLiteSignature(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if sigAfterWAL.Equal(sigAfterCounter) || sigAfterCounter.ChangeCounter == nil || *sigAfterCounter.ChangeCounter != 2 {
		t.Fatalf("expected change counter bump to alter signature: %+v", sigAfterCounter)
	}
}

func TestKnownLayoutsIncludeXDCodePiAndDsh(t *testing.T) {
	for _, agent := range []string{"codex", "claude", "gemini", "agy", "xdcode", "pi", "dsh"} {
		if !ImplementedAgents[agent] {
			t.Fatalf("expected ImplementedAgents[%q] = true", agent)
		}
		if len(KnownLayouts[agent]) == 0 {
			t.Fatalf("expected non-empty KnownLayouts[%q]", agent)
		}
	}
}
