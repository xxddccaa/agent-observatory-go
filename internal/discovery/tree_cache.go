package discovery

import (
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const (
	defaultValidateInterval = 10 * time.Second
	defaultRebuildInterval  = 600 * time.Second
	defaultHotWindow        = 600 * time.Second
	defaultRacyWindow       = 2 * time.Second

	resolveCacheTTL        = 60 * time.Second
	resolveCacheMaxEntries = 50000
)

// TreeCacheStats reports cumulative directory scan counters.
type TreeCacheStats struct {
	Rebuilds int
	Relisted int
	Checked  int
}

type dirEntry struct {
	mtimeNs int64
	racy    bool
	subdirs []string
	matches []string
}

// TreeCache caches recursive filename matches and only re-lists directories whose mtime changed.
type TreeCache struct {
	mu          sync.Mutex
	root        string
	pattern     string
	clock       func() time.Time
	dirs        map[string]*dirEntry
	structural  map[string]bool
	cached      []string
	hasBuilt    bool
	builtAt     time.Time
	validatedAt time.Time
	stats       TreeCacheStats
}

// NewTreeCache creates a directory tree cache for root matching basename pattern.
func NewTreeCache(root, pattern string, clock func() time.Time) *TreeCache {
	if clock == nil {
		clock = time.Now
	}
	return &TreeCache{
		root:       filepath.Clean(root),
		pattern:    pattern,
		clock:      clock,
		dirs:       make(map[string]*dirEntry),
		structural: make(map[string]bool),
	}
}

// Stats returns a snapshot of TreeCache counters.
func (tc *TreeCache) Stats() TreeCacheStats {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.stats
}

// Glob returns matching paths under root, performing either a full validation or a hot-only check.
func (tc *TreeCache) Glob(full bool) []string {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	now := tc.clock()
	if !tc.hasBuilt || now.Sub(tc.builtAt) >= defaultRebuildInterval {
		tc.rebuild(now)
	} else {
		validate := full || now.Sub(tc.validatedAt) >= defaultValidateInterval
		tc.refresh(now, validate)
	}

	if tc.cached == nil {
		out := make([]string, 0)
		for _, entry := range tc.dirs {
			out = append(out, entry.matches...)
		}
		sort.Strings(out)
		tc.cached = out
	}
	res := make([]string, len(tc.cached))
	copy(res, tc.cached)
	return res
}

func (tc *TreeCache) rebuild(now time.Time) {
	tc.dirs = make(map[string]*dirEntry)
	tc.scanTree(tc.root, now)
	tc.recomputeStructural()
	tc.builtAt = now
	tc.validatedAt = now
	tc.hasBuilt = true
	tc.stats.Rebuilds++
}

func (tc *TreeCache) refresh(now time.Time, validate bool) {
	if _, ok := tc.dirs[tc.root]; !ok {
		tc.scanTree(tc.root, now)
		tc.recomputeStructural()
		if validate {
			tc.validatedAt = now
		}
		return
	}

	candidates := make([]string, 0, len(tc.dirs))
	for path, entry := range tc.dirs {
		if validate || tc.isHot(path, entry, now) {
			candidates = append(candidates, path)
		}
	}

	changed := false
	for _, path := range candidates {
		entry, ok := tc.dirs[path]
		if !ok {
			continue
		}
		tc.stats.Checked++
		info, err := os.Stat(path)
		if err != nil {
			tc.drop(path)
			changed = true
			continue
		}
		if entry.racy || info.ModTime().UnixNano() != entry.mtimeNs {
			if tc.relist(path, info, now) {
				changed = true
			}
		}
	}
	if changed {
		tc.recomputeStructural()
	}
	if validate {
		tc.validatedAt = now
	}
}

func (tc *TreeCache) isHot(path string, entry *dirEntry, now time.Time) bool {
	if entry.racy || tc.structural[path] {
		return true
	}
	return now.Sub(time.Unix(0, entry.mtimeNs)) < defaultHotWindow
}

func (tc *TreeCache) relist(path string, info os.FileInfo, now time.Time) bool {
	listing := tc.listDir(path, info, now)
	if listing == nil {
		tc.drop(path)
		return true
	}
	old := tc.dirs[path]
	tc.dirs[path] = listing
	tc.stats.Relisted++

	if old != nil {
		newSub := make(map[string]bool, len(listing.subdirs))
		for _, sub := range listing.subdirs {
			newSub[sub] = true
		}
		for _, gone := range old.subdirs {
			if !newSub[gone] {
				tc.drop(gone)
			}
		}
	}
	for _, sub := range listing.subdirs {
		if _, ok := tc.dirs[sub]; !ok {
			tc.scanTree(sub, now)
		}
	}
	if old == nil {
		return true
	}
	return !equalStringSlices(old.subdirs, listing.subdirs) || !equalStringSlices(old.matches, listing.matches)
}

func (tc *TreeCache) scanTree(top string, now time.Time) {
	stack := []string{top}
	for len(stack) > 0 {
		path := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		info, err := os.Stat(path)
		if err != nil || !info.IsDir() {
			continue
		}
		listing := tc.listDir(path, info, now)
		if listing == nil {
			continue
		}
		tc.dirs[path] = listing
		tc.stats.Relisted++
		for _, sub := range listing.subdirs {
			if _, ok := tc.dirs[sub]; !ok {
				stack = append(stack, sub)
			}
		}
	}
}

func (tc *TreeCache) listDir(path string, info os.FileInfo, now time.Time) *dirEntry {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil
	}
	var subdirs []string
	var matches []string
	for _, item := range entries {
		name := item.Name()
		fullPath := filepath.Join(path, name)
		if ok, _ := filepath.Match(tc.pattern, name); ok {
			matches = append(matches, fullPath)
		}
		// Never follow symlinked subdirectories so cyclic or external trees are skipped.
		if item.Type()&os.ModeSymlink == 0 && item.IsDir() {
			subdirs = append(subdirs, fullPath)
		}
	}
	mtimeNs := info.ModTime().UnixNano()
	diff := now.Sub(time.Unix(0, mtimeNs))
	if diff < 0 {
		diff = -diff
	}
	return &dirEntry{
		mtimeNs: mtimeNs,
		racy:    diff < defaultRacyWindow,
		subdirs: subdirs,
		matches: matches,
	}
}

func (tc *TreeCache) drop(top string) {
	stack := []string{top}
	for len(stack) > 0 {
		path := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		entry, ok := tc.dirs[path]
		if !ok {
			continue
		}
		delete(tc.dirs, path)
		stack = append(stack, entry.subdirs...)
	}
}

func (tc *TreeCache) recomputeStructural() {
	tc.cached = nil
	structural := map[string]bool{tc.root: true}
	for path, entry := range tc.dirs {
		if len(entry.matches) == 0 {
			continue
		}
		cur := path
		for !structural[cur] && (cur == tc.root || len(cur) > len(tc.root)) {
			structural[cur] = true
			parent := filepath.Dir(cur)
			if parent == cur {
				break
			}
			cur = parent
		}
	}
	tc.structural = structural
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type resolvedEntry struct {
	real string
	at   time.Time
}

var (
	resolveMu    sync.RWMutex
	resolveCache = make(map[string]resolvedEntry)
)

// CachedResolve resolves symlinks and cleans path with a short-lived bounded cache.
func CachedResolve(raw string) string {
	clean := filepath.Clean(raw)
	now := time.Now()

	resolveMu.RLock()
	if hit, ok := resolveCache[clean]; ok && now.Sub(hit.at) < resolveCacheTTL {
		resolveMu.RUnlock()
		return hit.real
	}
	resolveMu.RUnlock()

	resolved, err := filepath.EvalSymlinks(clean)
	if err != nil {
		if abs, absErr := filepath.Abs(clean); absErr == nil {
			resolved = abs
		} else {
			resolved = clean
		}
	} else if abs, absErr := filepath.Abs(resolved); absErr == nil {
		resolved = abs
	}

	resolveMu.Lock()
	if len(resolveCache) >= resolveCacheMaxEntries {
		resolveCache = make(map[string]resolvedEntry)
	}
	resolveCache[clean] = resolvedEntry{real: resolved, at: now}
	resolveMu.Unlock()
	return resolved
}
