package adapters

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/agent-observatory/agent-observatory-go/internal/discovery"
	"github.com/agent-observatory/agent-observatory-go/internal/model"
)

// GenericAdapter indexes arbitrary JSONL files configured via AGENT_OBS_EXTRA_ROOTS.
type GenericAdapter struct {
	root          string
	agent         string
	parserVersion string
	tree          *discovery.TreeCache
}

// NewGenericAdapter creates a fallback JSONL adapter for custom agent roots.
func NewGenericAdapter(root, agent string) *GenericAdapter {
	if strings.TrimSpace(agent) == "" {
		agent = "generic"
	}
	expanded := discovery.ExpandPath(root)
	return &GenericAdapter{
		root:          expanded,
		agent:         strings.ToLower(strings.TrimSpace(agent)),
		parserVersion: "1",
		tree:          discovery.NewTreeCache(expanded, "*.jsonl", nil),
	}
}

func (a *GenericAdapter) Kind() string          { return "generic_jsonl" }
func (a *GenericAdapter) Agent() string         { return a.agent }
func (a *GenericAdapter) ParserVersion() string { return a.parserVersion }

func (a *GenericAdapter) WatchPaths(src SourceDescriptor) []string {
	return []string{src.Path}
}

func (a *GenericAdapter) Identity(path string) string {
	return "generic:" + sha256Hex([]byte(discovery.CachedResolve(path)))
}

func (a *GenericAdapter) Discover(full bool) ([]SourceDescriptor, error) {
	if _, err := os.Stat(a.root); err != nil {
		return nil, nil
	}
	matches := a.tree.Glob(full)
	sort.Strings(matches)
	out := make([]SourceDescriptor, 0, len(matches))
	for _, p := range matches {
		info, err := os.Stat(p)
		if err != nil || info.IsDir() {
			continue
		}
		out = append(out, SourceDescriptor{
			Kind:     a.Kind(),
			Identity: a.Identity(p),
			Path:     p,
			Format:   "jsonl",
			Agent:    a.Agent(),
			ThreadID: fileStem(p),
		})
	}
	return out, nil
}

func (a *GenericAdapter) Plan(src SourceDescriptor, cp *model.IndexCheckpoint) (SourcePlan, error) {
	return planStatRebuild(src, cp, a.parserVersion, nil)
}

func (a *GenericAdapter) Parse(plan SourcePlan, existing *model.AgentSession) (*ParsedSource, error) {
	src := plan.Source
	realPath := discovery.CachedResolve(src.Path)
	nowStr := model.FormatUTC(time.Now())
	info, err := os.Stat(src.Path)
	if err != nil {
		return nil, err
	}

	sess := model.AgentSession{
		ID:                  stableID("session", src.Identity),
		Agent:               a.Agent(),
		SourceKind:          a.Kind(),
		SourceIdentity:      src.Identity,
		RolloutPath:         strPtr(src.Path),
		RolloutRealPath:     strPtr(realPath),
		SourcePath:          strPtr(realPath),
		RolloutFormat:       "jsonl",
		SourceFormat:        "jsonl",
		ThreadID:            fileStem(src.Path),
		ParserVersion:       a.parserVersion,
		ProjectedEventIndex: -1,
		IndexStatus:         "indexed",
		CreatedAt:           nowStr,
		UpdatedAt:           nowStr,
	}
	if existing != nil {
		sess.CreatedAt = existing.CreatedAt
	}

	lines, err := readJSONLLines(src.Path, 0, MaxLineBytes, PrefixBytes, true)
	if err != nil {
		return nil, err
	}

	events := make([]model.AgentEvent, 0, len(lines))
	for idx, ln := range lines {
		lineNo := int64(idx + 1)
		if ln.Oversized {
			events = append(events, model.AgentEvent{
				AgentSessionID: sess.ID,
				EventIndex:     idx,
				LineNumber:     lineNo,
				ByteOffset:     ln.ByteOffset,
				TopLevelType:   strPtr("parse_error"),
				PayloadType:    strPtr("parse_error"),
				NormalizedType: "parse_error",
				RawJSON:        unparsedEventJSON("line_too_large", ln.Prefix, ln.ByteLength, nil),
				RawHash:        ln.OversizedHash,
				IsKnown:        false,
				ParserVersion:  a.parserVersion,
				CreatedAt:      nowStr,
			})
			continue
		}

		cleaned := sanitizeUTF8AndNUL(ln.Raw)
		var obj map[string]any
		if err := json.Unmarshal(cleaned, &obj); err != nil || obj == nil {
			events = append(events, model.AgentEvent{
				AgentSessionID: sess.ID,
				EventIndex:     idx,
				LineNumber:     lineNo,
				ByteOffset:     ln.ByteOffset,
				TopLevelType:   strPtr("parse_error"),
				PayloadType:    strPtr("parse_error"),
				NormalizedType: "parse_error",
				RawJSON:        unparsedEventJSON("invalid_json", ln.Raw, int64(len(ln.Raw)), nil),
				RawHash:        sha256Hex(ln.Raw),
				IsKnown:        false,
				ParserVersion:  a.parserVersion,
				CreatedAt:      nowStr,
			})
			continue
		}

		safe := sanitizeMap(obj)
		typ := strField(safe, "type", "event")
		if typ == "" {
			typ = "unknown"
		}
		ts := parseTimestamp(firstNonNil(safe["timestamp"], safe["time"], safe["created_at"]))
		events = append(events, model.AgentEvent{
			AgentSessionID: sess.ID,
			EventIndex:     idx,
			LineNumber:     lineNo,
			ByteOffset:     ln.ByteOffset,
			EventTimestamp: ts,
			TopLevelType:   strPtr(truncateRunes(typ, 64)),
			PayloadType:    strPtr(truncateRunes(typ, 128)),
			NormalizedType: typ,
			RawJSON:        marshalSafeJSON(safe),
			RawHash:        sha256Hex(ln.Raw),
			IsKnown:        true,
			ParserVersion:  a.parserVersion,
			CreatedAt:      nowStr,
		})
	}

	sess.FileSize = info.Size()
	sess.FileMtimeNs = info.ModTime().UnixNano()
	sess.LastIndexedLine = int64(len(events))
	sess.LastIndexedOffset = info.Size()
	return &ParsedSource{
		Session:         sess,
		ResetEvents:     true,
		NewEvents:       events,
		CursorJSON:      encodeCursorJSON(sess.LastIndexedOffset, sess.LastIndexedLine),
		FingerprintJSON: encodeFingerprintJSON(plan.Fingerprint),
	}, nil
}

func (a *GenericAdapter) Project(sess model.AgentSession, allEvents []model.AgentEvent) (model.AgentSession, model.SessionProjection, error) {
	nowStr := model.FormatUTC(time.Now())
	var (
		messages  []model.Message
		firstUser string
		startTS   *string
		endTS     *string
		maxIdx    = -1
	)

	for _, ev := range allEvents {
		if ev.EventIndex > maxIdx {
			maxIdx = ev.EventIndex
		}
		if ev.EventTimestamp != nil {
			if startTS == nil {
				startTS = ev.EventTimestamp
			}
			endTS = ev.EventTimestamp
		}
		raw := decodeObjectMap(ev.RawJSON)
		role := strField(raw, "role")
		text := extractAnyText(firstNonNil(raw["text"], raw["content"], raw["message"]))
		if (role == "user" || role == "assistant") && text != "" {
			if role == "user" && firstUser == "" {
				firstUser = truncateRunes(collapseSpaces(text), 255)
			}
			evID := ev.ID
			seq := len(messages)
			messages = append(messages, model.Message{
				ID:             stableID("msg", fmt.Sprintf("%s:%d", sess.ID, ev.EventIndex)),
				AgentSessionID: sess.ID,
				EventID:        &evID,
				Role:           role,
				Channel:        "message",
				Sequence:       seq,
				Text:           text,
				MarkdownText:   strPtr(text),
				CopyText:       text,
				Timestamp:      ev.EventTimestamp,
				IsVisible:      true,
				CreatedAt:      nowStr,
			})
		}
	}

	if firstUser != "" {
		sess.DisplayTitle = strPtr(firstUser)
	} else if sess.DisplayTitle == nil {
		sess.DisplayTitle = strPtr(sess.ThreadID)
	}
	if sess.AITitle == nil {
		sess.AITitle = sess.DisplayTitle
	}
	sess.StartTime = startTS
	sess.EndTime = endTS
	if endTS != nil {
		sess.LastActivityAt = endTS
	} else {
		sess.LastActivityAt = startTS
	}
	sess.ProjectedEventIndex = maxIdx
	sess.UpdatedAt = nowStr

	return sess, model.SessionProjection{Messages: messages}, nil
}
