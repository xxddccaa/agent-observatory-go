package indexer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/agent-observatory/agent-observatory-go/internal/adapters"
	"github.com/agent-observatory/agent-observatory-go/internal/discovery"
	"github.com/agent-observatory/agent-observatory-go/internal/model"
	"github.com/agent-observatory/agent-observatory-go/internal/storage"
)

const (
	defaultPollInterval = 2 * time.Second
	fullCycleInterval   = 60 * time.Second
	racyWindow          = 2 * time.Second
	settledTTL          = time.Hour
)

// Options configures the Indexer service.
type Options struct {
	ExtraRoots     map[string][]string
	Layouts        map[string][]string
	CustomAdapters []adapters.Adapter
	PollInterval   time.Duration
	OnCycleError   func(stats model.CycleStats, summary string)
}

type settledEntry struct {
	sig       string
	cpKey     string
	settledAt time.Time
}

// Indexer coordinates read-only discovery, planning, ingestion, and SQLite persistence.
type Indexer struct {
	store        *storage.Store
	adapters     []adapters.Adapter
	extraRoots   map[string][]string
	layouts      map[string][]string
	pollInterval time.Duration
	onCycleError func(stats model.CycleStats, summary string)

	mu            sync.RWMutex
	settled       map[string]settledEntry
	status        model.IndexerStatus
	lastLoggedErr string
}

// New constructs an Indexer for V1 built-in adapters plus configured extra roots.
func New(store *storage.Store, opts Options) *Indexer {
	poll := opts.PollInterval
	if poll <= 0 {
		poll = defaultPollInterval
	}

	var registered []adapters.Adapter
	if len(opts.CustomAdapters) > 0 {
		registered = append(registered, opts.CustomAdapters...)
	} else {
		registered = buildDefaultAdapters(store, opts.ExtraRoots, opts.Layouts)
	}

	nowStr := model.FormatUTC(time.Now())
	return &Indexer{
		store:        store,
		adapters:     registered,
		extraRoots:   opts.ExtraRoots,
		layouts:      opts.Layouts,
		pollInterval: poll,
		onCycleError: opts.OnCycleError,
		settled:      make(map[string]settledEntry),
		status: model.IndexerStatus{
			Status:    "starting",
			StartedAt: nowStr,
		},
	}
}

func buildDefaultAdapters(store *storage.Store, extraRoots, layouts map[string][]string) []adapters.Adapter {
	var out []adapters.Adapter
	base := layouts
	if base == nil {
		base = discovery.KnownLayouts
	}
	merged := make(map[string][]string, len(base)+len(extraRoots))
	for ag, roots := range base {
		merged[ag] = append(merged[ag], roots...)
	}
	for ag, roots := range extraRoots {
		norm := strings.ToLower(strings.TrimSpace(ag))
		if norm != "" {
			merged[norm] = append(merged[norm], roots...)
		}
	}

	agents := make([]string, 0, len(merged))
	for ag := range merged {
		agents = append(agents, ag)
	}
	sort.Strings(agents)

	agyReader := func(dbPath string, hasWAL bool) (map[string]model.AgyConversationMeta, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return storage.ReadAgyConversationSummaries(ctx, dbPath, hasWAL)
	}

	for _, ag := range agents {
		for _, root := range merged[ag] {
			switch ag {
			case "codex":
				out = append(out, adapters.NewCodexAdapter(root))
			case "claude":
				out = append(out, adapters.NewClaudeAdapter(root))
			case "gemini":
				out = append(out, adapters.NewGeminiAdapter(root))
			case "agy":
				out = append(out, adapters.NewAgyAdapter(root, agyReader))
			default:
				out = append(out, adapters.NewGenericAdapter(root, ag))
			}
		}
	}
	return out
}

// Snapshot returns a thread-safe copy of the latest indexer status.
func (idx *Indexer) Snapshot() model.IndexerStatus {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	out := idx.status
	out.DetectedAgents = append([]string(nil), idx.status.DetectedAgents...)
	return out
}

// RunLoop polls continuously until ctx is cancelled, alternating fast and full validation cycles.
func (idx *Indexer) RunLoop(ctx context.Context) {
	idx.mu.Lock()
	idx.status.Running = true
	idx.status.Status = "running"
	idx.mu.Unlock()

	lastFull := time.Time{}
	runCycle := func() {
		full := lastFull.IsZero() || time.Since(lastFull) >= fullCycleInterval
		if _, err := idx.RunOnce(ctx, full); err == nil && full {
			lastFull = time.Now()
		}
	}

	runCycle()

	ticker := time.NewTicker(idx.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			idx.mu.Lock()
			idx.status.Running = false
			idx.status.Status = "stopped"
			idx.mu.Unlock()
			return
		case <-ticker.C:
			runCycle()
		}
	}
}

type discoveredSource struct {
	adapter adapters.Adapter
	desc    adapters.SourceDescriptor
}

type plannedSource struct {
	adapter adapters.Adapter
	plan    adapters.SourcePlan
}

// RunOnce executes one isolated discovery, plan, and ingest cycle across all adapters.
func (idx *Indexer) RunOnce(ctx context.Context, full bool) (model.CycleStats, error) {
	cycleStart := time.Now()
	var stats model.CycleStats

	pairs, dupes := idx.collectSources(full)
	stats.Discovered = len(pairs)
	stats.Duplicates = dupes
	stats.DiscoverMS = roundedMS(time.Since(cycleStart))

	planStart := time.Now()
	toIndex, planFailures, unchanged := idx.planSources(ctx, pairs, full)
	stats.Changed = len(toIndex)
	stats.Unchanged = unchanged
	stats.PlanMS = roundedMS(time.Since(planStart))

	ingestStart := time.Now()
	var errMsgs []string
	for _, item := range toIndex {
		if ctx.Err() != nil {
			break
		}
		if err := idx.ingestOne(ctx, item.adapter, item.plan); err != nil {
			stats.Failures++
			errMsgs = append(errMsgs, item.plan.Source.Identity+": "+err.Error())
			idx.recordFailure(ctx, item.plan.Source.Identity, err)
			continue
		}
		stats.Indexed++
	}
	for _, pf := range planFailures {
		stats.Failures++
		errMsgs = append(errMsgs, pf.identity+": "+pf.err.Error())
		idx.recordFailure(ctx, pf.identity, pf.err)
	}
	lastErrMsg := strings.Join(errMsgs, "; ")
	stats.IngestMS = roundedMS(time.Since(ingestStart))
	stats.CycleMS = roundedMS(time.Since(cycleStart))
	stats.Finished = true

	detected := discovery.DetectedAgents(idx.extraRoots, idx.layouts)
	nowStr := model.FormatUTC(time.Now())

	var notifyErr string
	idx.mu.Lock()
	idx.status.LastSeenAt = nowStr
	idx.status.ActiveSessions = stats.Discovered
	idx.status.DetectedAgents = detected
	idx.status.LastCycle = stats
	if stats.Failures == 0 {
		idx.status.LastSuccessAt = nowStr
		idx.status.LastError = ""
		idx.lastLoggedErr = ""
		if idx.status.Running {
			idx.status.Status = "running"
		}
	} else {
		idx.status.LastError = lastErrMsg
		if idx.status.Running {
			idx.status.Status = "degraded"
		}
		if lastErrMsg != idx.lastLoggedErr {
			idx.lastLoggedErr = lastErrMsg
			notifyErr = lastErrMsg
		}
	}
	onErr := idx.onCycleError
	idx.mu.Unlock()

	if notifyErr != "" && onErr != nil {
		onErr(stats, notifyErr)
	}

	return stats, nil
}

func (idx *Indexer) collectSources(full bool) ([]discoveredSource, int) {
	var pairs []discoveredSource
	seenIDs := make(map[string]bool)
	seenFiles := make(map[string]bool)
	dupes := 0

	for _, ad := range idx.adapters {
		sources, err := ad.Discover(full)
		if err != nil {
			continue
		}
		for _, src := range sources {
			canonical := strings.TrimSuffix(src.Path, ".zst")
			fileKey := src.Kind + "\x00" + discovery.CachedResolve(canonical)
			if seenIDs[src.Identity] || (src.Format == "jsonl" && seenFiles[fileKey]) {
				dupes++
				continue
			}
			seenIDs[src.Identity] = true
			if src.Format == "jsonl" {
				seenFiles[fileKey] = true
			}
			pairs = append(pairs, discoveredSource{adapter: ad, desc: src})
		}
	}
	return pairs, dupes
}

type planFailure struct {
	identity string
	err      error
}

func (idx *Indexer) planSources(ctx context.Context, pairs []discoveredSource, full bool) ([]plannedSource, []planFailure, int) {
	var toIndex []plannedSource
	var failures []planFailure
	unchanged := 0
	now := time.Now()

	if full {
		live := make(map[string]bool, len(pairs))
		for _, p := range pairs {
			live[p.desc.Identity] = true
		}
		idx.mu.Lock()
		for id := range idx.settled {
			if !live[id] {
				delete(idx.settled, id)
			}
		}
		idx.mu.Unlock()
	}

	for _, pair := range pairs {
		if ctx.Err() != nil {
			break
		}
		sig, racy := sourceSignature(pair.adapter, pair.desc, now)
		if !full && sig != "" {
			idx.mu.RLock()
			hit, ok := idx.settled[pair.desc.Identity]
			idx.mu.RUnlock()
			if ok && hit.sig == sig && now.Sub(hit.settledAt) < settledTTL {
				unchanged++
				continue
			}
		}

		var cpPtr *model.IndexCheckpoint
		cp, err := idx.store.GetCheckpointByIdentity(ctx, pair.desc.Identity)
		if err == nil {
			cpPtr = &cp
		} else if !errors.Is(err, storage.ErrNotFound) {
			failures = append(failures, planFailure{identity: pair.desc.Identity, err: err})
			continue
		}

		cpKey := checkpointKey(cpPtr)
		if sig != "" {
			idx.mu.RLock()
			hit, ok := idx.settled[pair.desc.Identity]
			idx.mu.RUnlock()
			if ok && hit.sig == sig && hit.cpKey == cpKey && now.Sub(hit.settledAt) < settledTTL {
				unchanged++
				continue
			}
		}

		plan, planErr := pair.adapter.Plan(pair.desc, cpPtr)
		if planErr != nil {
			idx.dropSettled(pair.desc.Identity)
			failures = append(failures, planFailure{identity: pair.desc.Identity, err: planErr})
			continue
		}
		if plan.Action == adapters.PlanUnchanged {
			unchanged++
			if sig != "" && !racy {
				idx.mu.Lock()
				idx.settled[pair.desc.Identity] = settledEntry{sig: sig, cpKey: cpKey, settledAt: now}
				idx.mu.Unlock()
			} else {
				idx.dropSettled(pair.desc.Identity)
			}
			continue
		}

		idx.dropSettled(pair.desc.Identity)
		toIndex = append(toIndex, plannedSource{adapter: pair.adapter, plan: plan})
	}
	return toIndex, failures, unchanged
}

func (idx *Indexer) dropSettled(identity string) {
	idx.mu.Lock()
	delete(idx.settled, identity)
	idx.mu.Unlock()
}

func (idx *Indexer) ingestOne(ctx context.Context, ad adapters.Adapter, plan adapters.SourcePlan) error {
	var existingPtr *model.AgentSession
	var existingEvents []model.AgentEvent
	if plan.Action == adapters.PlanAppend {
		existing, err := idx.store.GetSessionByIdentity(ctx, plan.Source.Identity)
		if err == nil {
			existingPtr = &existing
			if evs, evErr := idx.store.ListSessionEvents(ctx, existing.ID); evErr == nil {
				existingEvents = evs
			}
		}
	}

	parsed, err := ad.Parse(plan, existingPtr)
	if err != nil {
		return err
	}

	nowStr := model.FormatUTC(time.Now())
	return idx.store.WithWriterTx(ctx, func(tx *sql.Tx) error {
		sess := parsed.Session
		if linkErr := linkWorkspaceTx(ctx, tx, idx.store, &sess, nowStr); linkErr != nil {
			return linkErr
		}
		if err := idx.store.UpsertSessionTx(ctx, tx, sess); err != nil {
			return err
		}
		if parsed.ResetEvents {
			if err := idx.store.DeleteSessionEventsTx(ctx, tx, sess.ID); err != nil {
				return err
			}
		}
		insertedIDs, err := idx.store.InsertEventsTx(ctx, tx, sess.ID, parsed.NewEvents)
		if err != nil {
			return err
		}
		for i := range parsed.NewEvents {
			parsed.NewEvents[i].ID = insertedIDs[i]
		}

		var allEvents []model.AgentEvent
		if parsed.ResetEvents {
			allEvents = parsed.NewEvents
		} else {
			allEvents = make([]model.AgentEvent, 0, len(existingEvents)+len(parsed.NewEvents))
			allEvents = append(allEvents, existingEvents...)
			allEvents = append(allEvents, parsed.NewEvents...)
		}

		projectedSess, proj, err := ad.Project(sess, allEvents)
		if err != nil {
			return err
		}
		if linkErr := linkWorkspaceTx(ctx, tx, idx.store, &projectedSess, nowStr); linkErr != nil {
			return linkErr
		}
		if err := idx.store.UpsertSessionTx(ctx, tx, projectedSess); err != nil {
			return err
		}
		if err := idx.store.ReplaceSessionProjectionTx(ctx, tx, projectedSess.ID, proj); err != nil {
			return err
		}

		cp := model.IndexCheckpoint{
			ID:                "cp-" + projectedSess.ID,
			SourceKind:        plan.Source.Kind,
			SourceIdentity:    plan.Source.Identity,
			SourcePath:        plan.Source.Path,
			RealPath:          discovery.CachedResolve(plan.Source.Path),
			AgentSessionID:    projectedSess.ID,
			ParserVersion:     ad.ParserVersion(),
			Status:            "indexed",
			FileSize:          projectedSess.FileSize,
			FileMtimeNs:       projectedSess.FileMtimeNs,
			LastIndexedOffset: projectedSess.LastIndexedOffset,
			LastIndexedLine:   projectedSess.LastIndexedLine,
			CursorJSON:        parsed.CursorJSON,
			FingerprintJSON:   parsed.FingerprintJSON,
			LastAttemptAt:     &nowStr,
			LastSuccessAt:     &nowStr,
			CreatedAt:         nowStr,
			UpdatedAt:         nowStr,
		}
		return idx.store.UpsertCheckpointTx(ctx, tx, cp)
	})
}

func (idx *Indexer) recordFailure(ctx context.Context, identity string, cause error) {
	nowStr := model.FormatUTC(time.Now())
	msg := cause.Error()
	if len(msg) > 1000 {
		msg = msg[:1000]
	}
	_ = idx.store.WithWriterTx(ctx, func(tx *sql.Tx) error {
		return idx.store.MarkCheckpointErrorTx(ctx, tx, identity, "ingest_error", msg, nowStr)
	})
}

func sourceSignature(ad adapters.Adapter, src adapters.SourceDescriptor, now time.Time) (string, bool) {
	paths := ad.WatchPaths(src)
	var b strings.Builder
	racy := false
	for i, p := range paths {
		info, err := os.Stat(p)
		if err != nil {
			if i == 0 {
				return "", false
			}
			b.WriteString("|missing")
			continue
		}
		mtimeNs := info.ModTime().UnixNano()
		fmt.Fprintf(&b, "|%d:%d", info.Size(), mtimeNs)
		diff := now.Sub(time.Unix(0, mtimeNs))
		if diff < 0 {
			diff = -diff
		}
		if diff < racyWindow {
			racy = true
		}
	}
	return b.String(), racy
}

func checkpointKey(cp *model.IndexCheckpoint) string {
	if cp == nil {
		return ""
	}
	return fmt.Sprintf("%s|%s|%d|%d|%s|%s",
		cp.ParserVersion, cp.Status, cp.FileSize, cp.FileMtimeNs, cp.CursorJSON, cp.FingerprintJSON)
}

func roundedMS(d time.Duration) float64 {
	return float64(int64(d.Seconds()*10000)) / 10.0
}
