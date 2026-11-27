package adapters

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/agent-observatory/agent-observatory-go/internal/discovery"
	"github.com/agent-observatory/agent-observatory-go/internal/model"
)

// ClaudeAdapter indexes Claude Code JSONL transcripts under ~/.claude/projects.
type ClaudeAdapter struct {
	root          string
	parserVersion string
	tree          *discovery.TreeCache
}

// NewClaudeAdapter creates a Claude Code adapter rooted at projectsDir.
func NewClaudeAdapter(projectsDir string) *ClaudeAdapter {
	expanded := discovery.ExpandPath(projectsDir)
	return &ClaudeAdapter{
		root:          expanded,
		parserVersion: "1",
		tree:          discovery.NewTreeCache(expanded, "*.jsonl", nil),
	}
}

func (a *ClaudeAdapter) Kind() string          { return "claude_jsonl" }
func (a *ClaudeAdapter) Agent() string         { return "claude" }
func (a *ClaudeAdapter) ParserVersion() string { return a.parserVersion }

func (a *ClaudeAdapter) WatchPaths(src SourceDescriptor) []string {
	return []string{src.Path}
}

func (a *ClaudeAdapter) Identity(path string) string {
	canonical := discovery.CachedResolve(path)
	return "claude:claude:" + sha256Hex([]byte("claude\x00"+canonical))
}

func (a *ClaudeAdapter) Discover(full bool) ([]SourceDescriptor, error) {
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

func (a *ClaudeAdapter) Plan(src SourceDescriptor, cp *model.IndexCheckpoint) (SourcePlan, error) {
	return planAppendJSONL(src, cp, a.parserVersion, appendPlanOpts{
		extraFP:           map[string]any{"agent": a.Agent()},
		checkPath:         true,
		checkPrefix:       false,
		rebuildOnSameSize: true,
	})
}

func (a *ClaudeAdapter) Parse(plan SourcePlan, existing *model.AgentSession) (*ParsedSource, error) {
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
		RolloutRealPath:     strPtr("claude:" + realPath),
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

	resetEvents := plan.Action == PlanRebuild || existing == nil || existing.ParserVersion != a.parserVersion
	var startOffset, startLine int64
	var eventIndex int
	if !resetEvents && existing != nil {
		sess = *existing
		sess.RolloutPath = strPtr(src.Path)
		sess.RolloutRealPath = strPtr("claude:" + realPath)
		sess.SourcePath = strPtr(realPath)
		sess.UpdatedAt = nowStr
		startOffset = existing.LastIndexedOffset
		startLine = existing.LastIndexedLine
		eventIndex = int(existing.LastIndexedLine)
	}

	if parentID := claudeParentSession(realPath); parentID != "" && parentID != sess.ThreadID {
		sess.ParentSessionID = strPtr(parentID)
		sess.IsSubagent = true
	}

	lines, err := readJSONLLines(src.Path, startOffset, MaxLineBytes, PrefixBytes, false)
	if err != nil {
		return nil, err
	}

	newEvents := make([]model.AgentEvent, 0, len(lines))
	curLine := startLine
	for _, ln := range lines {
		curLine++
		if ln.Oversized {
			newEvents = append(newEvents, model.AgentEvent{
				AgentSessionID: sess.ID,
				EventIndex:     eventIndex,
				LineNumber:     curLine,
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
			eventIndex++
			sess.LastIndexedLine = curLine
			continue
		}

		cleaned := sanitizeUTF8AndNUL(ln.Raw)
		var obj map[string]any
		if err := json.Unmarshal(cleaned, &obj); err != nil || obj == nil {
			newEvents = append(newEvents, model.AgentEvent{
				AgentSessionID: sess.ID,
				EventIndex:     eventIndex,
				LineNumber:     curLine,
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
			eventIndex++
			sess.LastIndexedLine = curLine
			sess.LastIndexedOffset = ln.ByteOffset + ln.ByteLength
			continue
		}

		safe := sanitizeMap(obj)
		recType := strField(safe, "type")
		if recType == "" {
			recType = "unknown"
		}
		known := recType == "user" || recType == "assistant" || recType == "summary" || recType == "attachment"
		norm := "unknown"
		if known {
			norm = recType
		}
		payloadType := recType
		if msg, ok := safe["message"].(map[string]any); ok {
			if role := strField(msg, "role"); role != "" {
				payloadType = role
			}
			if sess.Model == nil {
				if m := strField(msg, "model"); m != "" {
					sess.Model = strPtr(m)
				}
			}
		}
		ts := parseTimestamp(safe["timestamp"])
		if sess.StartTime == nil && ts != nil {
			sess.StartTime = ts
		}
		if sess.CWD == nil {
			if cwd := strField(safe, "cwd"); cwd != "" {
				sess.CWD = strPtr(cwd)
			}
		}
		if sess.Model == nil {
			if m := strField(safe, "model"); m != "" {
				sess.Model = strPtr(m)
			}
		}
		if sess.CLIVersion == nil {
			if v := strField(safe, "version"); v != "" {
				sess.CLIVersion = strPtr(v)
			}
		}
		if sess.GitBranch == nil {
			if b := strField(safe, "gitBranch", "git_branch"); b != "" {
				sess.GitBranch = strPtr(b)
			}
		}
		if sess.ThreadName == nil {
			if slug := strField(safe, "slug"); slug != "" {
				sess.ThreadName = strPtr(slug)
			}
		}
		if sid := strField(safe, "sessionId"); sid != "" {
			sess.ThreadID = sid
		}

		newEvents = append(newEvents, model.AgentEvent{
			AgentSessionID: sess.ID,
			EventIndex:     eventIndex,
			LineNumber:     curLine,
			ByteOffset:     ln.ByteOffset,
			EventTimestamp: ts,
			TopLevelType:   strPtr(recType),
			PayloadType:    strPtr(payloadType),
			NormalizedType: norm,
			RawJSON:        marshalSafeJSON(safe),
			RawHash:        sha256Hex(ln.Raw),
			IsKnown:        known,
			ParserVersion:  a.parserVersion,
			CreatedAt:      nowStr,
		})
		eventIndex++
		sess.LastIndexedLine = curLine
		sess.LastIndexedOffset = ln.ByteOffset + ln.ByteLength
	}

	sess.FileSize = info.Size()
	sess.FileMtimeNs = info.ModTime().UnixNano()
	return &ParsedSource{
		Session:         sess,
		ResetEvents:     resetEvents,
		NewEvents:       newEvents,
		CursorJSON:      encodeCursorJSON(sess.LastIndexedOffset, sess.LastIndexedLine),
		FingerprintJSON: encodeFingerprintJSON(plan.Fingerprint),
	}, nil
}

func (a *ClaudeAdapter) Project(sess model.AgentSession, allEvents []model.AgentEvent) (model.AgentSession, model.SessionProjection, error) {
	nowStr := model.FormatUTC(time.Now())
	var (
		turns        []model.AgentTurn
		messages     []model.Message
		toolsByCall  = make(map[string]*model.ToolCall)
		toolOrder    []string
		currentTurn  *model.AgentTurn
		sequence     int
		maxIdx       = -1
		latestTS     *string
	)

	for _, ev := range allEvents {
		if ev.EventIndex > maxIdx {
			maxIdx = ev.EventIndex
		}
		if ev.EventTimestamp != nil {
			if latestTS == nil || *ev.EventTimestamp > *latestTS {
				latestTS = ev.EventTimestamp
			}
		}
		raw := decodeObjectMap(ev.RawJSON)
		typ := ""
		if ev.TopLevelType != nil {
			typ = *ev.TopLevelType
		}
		blocks := claudeContentBlocks(raw)

		if typ == "user" && !hasClaudeToolResult(blocks) {
			turn := model.AgentTurn{
				ID:               stableID("turn", fmt.Sprintf("%s:%d", sess.ID, len(turns))),
				Agent:            sess.Agent,
				AgentSessionID:   sess.ID,
				TurnIndex:        len(turns),
				TurnID:           strPtr(strField(raw, "uuid")),
				Status:           "running",
				StartedAt:        ev.EventTimestamp,
				ValidationStatus: "unknown",
				Outcome:          "running",
				MetricConfidence: "derived",
				CreatedAt:        nowStr,
				UpdatedAt:        nowStr,
			}
			turns = append(turns, turn)
			currentTurn = &turns[len(turns)-1]
		}

		if typ == "summary" {
			text := extractAnyText(firstNonNil(raw["summary"], raw["content"]))
			if text != "" {
				evID := ev.ID
				messages = append(messages, model.Message{
					ID:             stableID("msg", fmt.Sprintf("%s:%d", sess.ID, ev.ID)),
					AgentSessionID: sess.ID,
					TurnID:         turnPtrID(currentTurn),
					EventID:        &evID,
					Role:           "system",
					Channel:        "commentary",
					Sequence:       sequence,
					Text:           text,
					MarkdownText:   strPtr(text),
					CopyText:       text,
					Timestamp:      ev.EventTimestamp,
					IsVisible:      true,
					CreatedAt:      nowStr,
				})
				sequence++
			}
		}

		for _, block := range blocks {
			kind := strField(block, "type")
			switch kind {
			case "text":
				text := extractAnyText(block["text"])
				if text != "" && currentTurn != nil {
					role := "assistant"
					if typ == "user" {
						role = "user"
						if sess.DisplayTitle == nil || *sess.DisplayTitle == "" {
							sess.DisplayTitle = strPtr(truncateRunes(collapseSpaces(text), 255))
						}
					}
					evID := ev.ID
					tid := currentTurn.ID
					messages = append(messages, model.Message{
						ID:             stableID("msg", fmt.Sprintf("%d:%d", ev.ID, sequence)),
						AgentSessionID: sess.ID,
						TurnID:         &tid,
						EventID:        &evID,
						Role:           role,
						Channel:        "final",
						Sequence:       sequence,
						Text:           text,
						MarkdownText:   strPtr(text),
						CopyText:       text,
						Timestamp:      ev.EventTimestamp,
						IsVisible:      true,
						CreatedAt:      nowStr,
					})
					sequence++
				}
			case "thinking":
				text := extractAnyText(firstNonNil(block["thinking"], block["text"]))
				if text != "" && currentTurn != nil {
					evID := ev.ID
					tid := currentTurn.ID
					messages = append(messages, model.Message{
						ID:             stableID("msg", fmt.Sprintf("%d:%d", ev.ID, sequence)),
						AgentSessionID: sess.ID,
						TurnID:         &tid,
						EventID:        &evID,
						Role:           "assistant",
						Channel:        "reasoning",
						Sequence:       sequence,
						Text:           text,
						MarkdownText:   strPtr(text),
						CopyText:       text,
						Timestamp:      ev.EventTimestamp,
						IsVisible:      false,
						CreatedAt:      nowStr,
					})
					sequence++
				}
			case "tool_use":
				callID := strField(block, "id")
				if callID == "" {
					callID = fmt.Sprintf("%d:%d", ev.ID, sequence)
				}
				if _, exists := toolsByCall[callID]; exists {
					continue
				}
				name := truncateRunes(strField(block, "name"), 255)
				if name == "" {
					name = "unknown"
				}
				var argsRaw json.RawMessage
				if inp, ok := block["input"].(map[string]any); ok {
					argsRaw = marshalSafeJSON(inp)
				}
				evID := ev.ID
				tc := &model.ToolCall{
					ID:             stableID("tool", fmt.Sprintf("%s:%s", sess.ID, callID)),
					Agent:          sess.Agent,
					AgentSessionID: sess.ID,
					TurnID:         turnPtrID(currentTurn),
					EventID:        &evID,
					CallID:         callID,
					ToolName:       name,
					ToolKind:       "tool",
					ArgumentsJSON:  argsRaw,
					CommandText:    strPtr(extractAnyText(block["input"])),
					Status:         "pending",
					StartedAt:      ev.EventTimestamp,
					CreatedAt:      nowStr,
				}
				toolsByCall[callID] = tc
				toolOrder = append(toolOrder, callID)
			case "tool_result":
				callID := strField(block, "tool_use_id")
				if tc, ok := toolsByCall[callID]; ok {
					outText := extractAnyText(firstNonNil(block["content"], block["output"]))
					tc.OutputText = strPtr(outText)
					tc.CompletedAt = ev.EventTimestamp
					if isErr, _ := block["is_error"].(bool); isErr {
						tc.Status = "failed"
					} else {
						tc.Status = "ok"
					}
				}
			}
		}

		if typ == "assistant" && currentTurn != nil {
			if msgMap, ok := raw["message"].(map[string]any); ok {
				in, cached, cacheCreate, out, reason, total := claudeUsage(msgMap)
				currentTurn.InputTokens += in
				currentTurn.CachedInputTokens += cached
				currentTurn.CacheCreationInputTokens += cacheCreate
				currentTurn.OutputTokens += out
				currentTurn.ReasoningOutputTokens += reason
				currentTurn.TotalTokens += total
			}
		}
	}

	if latestTS != nil {
		sess.EndTime = latestTS
		sess.LastActivityAt = latestTS
	} else {
		sess.LastActivityAt = sess.StartTime
	}

	turnByID := make(map[string]*model.AgentTurn, len(turns))
	var spans []model.ActivitySpan
	var inSum, cachedSum, cacheCreateSum, outSum, reasonSum, totalSum int64
	for i := range turns {
		tr := &turns[i]
		tr.Status = "completed"
		tr.Outcome = "completed"
		if tr.CompletedAt == nil {
			tr.CompletedAt = sess.EndTime
		}
		tr.DurationMS = elapsedMS(tr.StartedAt, tr.CompletedAt)
		turnByID[tr.ID] = tr
		inSum += tr.InputTokens
		cachedSum += tr.CachedInputTokens
		cacheCreateSum += tr.CacheCreationInputTokens
		outSum += tr.OutputTokens
		reasonSum += tr.ReasoningOutputTokens
		totalSum += tr.TotalTokens
		if sp := buildTurnSpan(sess.Agent, sess.SourceKind, sess, *tr); sp != nil {
			spans = append(spans, *sp)
		}
	}

	finalTools := make([]model.ToolCall, 0, len(toolOrder))
	for _, cid := range toolOrder {
		tc := toolsByCall[cid]
		finalTools = append(finalTools, *tc)
		var turnNativeID *string
		if tc.TurnID != nil {
			if tr := turnByID[*tc.TurnID]; tr != nil {
				turnNativeID = tr.TurnID
			}
		}
		if sp := buildToolSpan(sess.Agent, sess.SourceKind, sess, turnNativeID, *tc); sp != nil {
			spans = append(spans, *sp)
		}
	}

	sess.TurnCount = len(turns)
	sess.InputTokens = inSum
	sess.CachedInputTokens = cachedSum
	sess.CacheCreationInputTokens = cacheCreateSum
	sess.OutputTokens = outSum
	sess.ReasoningOutputTokens = reasonSum
	sess.TotalTokens = totalSum
	sess.IsOngoing = false
	sess.ProjectedEventIndex = maxIdx
	sess.UpdatedAt = nowStr

	return sess, model.SessionProjection{
		Turns:     turns,
		Messages:  messages,
		ToolCalls: finalTools,
		Spans:     spans,
	}, nil
}

func claudeParentSession(path string) string {
	parts := strings.Split(filepath.Clean(path), string(filepath.Separator))
	for i, part := range parts {
		if part == "subagents" && i > 0 {
			return parts[i-1]
		}
	}
	return ""
}

func claudeContentBlocks(raw map[string]any) []map[string]any {
	msg, ok := raw["message"].(map[string]any)
	if !ok {
		return nil
	}
	switch c := msg["content"].(type) {
	case []any:
		out := make([]map[string]any, 0, len(c))
		for _, item := range c {
			if m, ok := item.(map[string]any); ok {
				out = append(out, m)
			}
		}
		return out
	case string:
		return []map[string]any{{"type": "text", "text": c}}
	default:
		return nil
	}
}

func hasClaudeToolResult(blocks []map[string]any) bool {
	for _, b := range blocks {
		if strField(b, "type") == "tool_result" {
			return true
		}
	}
	return false
}

func claudeUsage(msg map[string]any) (inTok, cachedTok, cacheCreateTok, outTok, reasonTok, totalTok int64) {
	usage, ok := msg["usage"].(map[string]any)
	if !ok {
		return 0, 0, 0, 0, 0, 0
	}
	inTok = asNonNegInt64(usage["input_tokens"])
	cachedTok = asNonNegInt64(firstNonNil(usage["cache_read_input_tokens"], usage["cached_input_tokens"]))
	cacheCreateTok = asNonNegInt64(usage["cache_creation_input_tokens"])
	outTok = asNonNegInt64(usage["output_tokens"])
	reasonTok = asNonNegInt64(usage["reasoning_output_tokens"])
	if details, ok := usage["output_tokens_details"].(map[string]any); ok {
		if _, hasThinking := details["thinking_tokens"]; hasThinking {
			reasonTok = asNonNegInt64(details["thinking_tokens"])
		}
	}
	totalTok = inTok + cachedTok + cacheCreateTok + outTok
	return inTok, cachedTok, cacheCreateTok, outTok, reasonTok, totalTok
}
