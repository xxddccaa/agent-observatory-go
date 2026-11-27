package adapters

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/agent-observatory/agent-observatory-go/internal/discovery"
	"github.com/agent-observatory/agent-observatory-go/internal/model"
)

var (
	piKnownTypes = map[string]bool{
		"session":               true,
		"session_end":           true,
		"session_shutdown":      true,
		"message":               true,
		"model_change":          true,
		"thinking_level_change": true,
		"compaction":            true,
		"compaction_summary":    true,
		"branch_summary":        true,
		"session_name":          true,
		"session_info":          true,
		"custom":                true,
	}
	piToolKinds = map[string]string{
		"bash": "exec", "shell": "exec", "exec": "exec", "exec_command": "exec",
		"read": "read", "write": "write", "edit": "patch", "apply_patch": "patch",
		"web_search": "web_search", "fetch": "fetch", "browser": "browser",
	}
	piMutatingTools = map[string]bool{"write": true, "edit": true, "apply_patch": true}
)

// PiAdapter indexes Pi session JSONL transcripts under ~/.pi/agent/sessions.
type PiAdapter struct {
	root          string
	parserVersion string
	tree          *discovery.TreeCache
}

// NewPiAdapter creates a read-only Pi adapter rooted at sessionsDir.
func NewPiAdapter(sessionsDir string) *PiAdapter {
	expanded := discovery.ExpandPath(sessionsDir)
	return &PiAdapter{
		root:          expanded,
		parserVersion: "2",
		tree:          discovery.NewTreeCache(expanded, "*.jsonl", nil),
	}
}

func (a *PiAdapter) Kind() string          { return "pi_jsonl" }
func (a *PiAdapter) Agent() string         { return "pi" }
func (a *PiAdapter) ParserVersion() string { return a.parserVersion }

func (a *PiAdapter) WatchPaths(src SourceDescriptor) []string {
	return []string{src.Path}
}

func (a *PiAdapter) Identity(path string) string {
	return "pi:" + sha256Hex([]byte(discovery.CachedResolve(path)))
}

func (a *PiAdapter) Discover(full bool) ([]SourceDescriptor, error) {
	if _, err := os.Stat(a.root); err != nil {
		return nil, nil
	}
	matches := a.tree.Glob(full)
	type cand struct {
		desc  SourceDescriptor
		mtime int64
	}
	var cands []cand
	for _, p := range matches {
		info, err := os.Stat(p)
		if err != nil || info.IsDir() {
			continue
		}
		cands = append(cands, cand{
			desc: SourceDescriptor{
				Kind:     a.Kind(),
				Identity: a.Identity(p),
				Path:     p,
				Format:   "jsonl",
				Agent:    a.Agent(),
				ThreadID: fileStem(p),
			},
			mtime: info.ModTime().UnixNano(),
		})
	}
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].mtime != cands[j].mtime {
			return cands[i].mtime > cands[j].mtime
		}
		return cands[i].desc.Path < cands[j].desc.Path
	})
	out := make([]SourceDescriptor, len(cands))
	for i, c := range cands {
		out[i] = c.desc
	}
	return out, nil
}

func (a *PiAdapter) Plan(src SourceDescriptor, cp *model.IndexCheckpoint) (SourcePlan, error) {
	return planAppendJSONL(src, cp, a.parserVersion, appendPlanOpts{
		checkPath:         false,
		checkPrefix:       true,
		rebuildOnSameSize: true,
	})
}

func (a *PiAdapter) Parse(plan SourcePlan, existing *model.AgentSession) (*ParsedSource, error) {
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
		RolloutRealPath:     strPtr("pi:" + realPath),
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
		sess.RolloutRealPath = strPtr("pi:" + realPath)
		sess.SourcePath = strPtr(realPath)
		sess.UpdatedAt = nowStr
		startOffset = existing.LastIndexedOffset
		startLine = existing.LastIndexedLine
		eventIndex = int(existing.LastIndexedLine)
	}

	// Only consume newline-terminated lines so half-written tails are retried cleanly next poll.
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
			sess.LastIndexedOffset = ln.ByteOffset + ln.ByteLength
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
		payloadType := recType
		role := ""
		if msg, ok := safe["message"].(map[string]any); ok {
			if r := strField(msg, "role"); r != "" {
				role = r
				payloadType = r
			}
		}
		known := piKnownTypes[recType] || role == "user" || role == "assistant" || role == "system" || role == "toolResult"
		normType := "unknown"
		if known {
			normType = recType
		}

		ts := parseTimestamp(safe["timestamp"])
		if ts == nil {
			ts = parseTimestamp(safe["ts"])
		}
		if ts == nil {
			if msg, ok := safe["message"].(map[string]any); ok {
				ts = parseTimestamp(msg["timestamp"])
			}
		}

		if recType == "session" {
			if id := strField(safe, "id"); id != "" {
				sess.ThreadID = id
			}
			if cwd := strField(safe, "cwd"); cwd != "" {
				sess.CWD = strPtr(cwd)
			}
			if v, ok := safe["version"]; ok && v != nil {
				sess.CLIVersion = strPtr(truncateRunes(fmt.Sprint(v), 64))
			}
			if ts != nil && sess.StartTime == nil {
				sess.StartTime = ts
			}
		}

		newEvents = append(newEvents, model.AgentEvent{
			AgentSessionID: sess.ID,
			EventIndex:     eventIndex,
			LineNumber:     curLine,
			ByteOffset:     ln.ByteOffset,
			EventTimestamp: ts,
			TopLevelType:   strPtr(recType),
			PayloadType:    strPtr(payloadType),
			NormalizedType: normType,
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

func (a *PiAdapter) Project(sess model.AgentSession, allEvents []model.AgentEvent) (model.AgentSession, model.SessionProjection, error) {
	nowStr := model.FormatUTC(time.Now())
	byID := make(map[string]piGraphNode, len(allEvents))
	allLines := make(map[int64]bool, len(allEvents))
	decoded := make([]map[string]any, len(allEvents))
	maxEventIndex := -1

	for i, ev := range allEvents {
		if ev.EventIndex > maxEventIndex {
			maxEventIndex = ev.EventIndex
		}
		allLines[ev.LineNumber] = true
		obj := decodeObjectMap(ev.RawJSON)
		decoded[i] = obj
		if id := strField(obj, "id"); id != "" {
			byID[id] = piGraphNode{
				lineNumber: ev.LineNumber,
				parentID:   strField(obj, "parentId"),
				recType:    strField(obj, "type"),
			}
		}
	}

	activeLines := piActiveBranchLines(byID, allLines)

	// Reset projection-derived metadata before replaying active branch records.
	sess.Model = nil
	sess.ThreadName = nil
	sess.DisplayTitle = nil

	var (
		turns                  []model.AgentTurn
		messages               []model.Message
		toolList               []*model.ToolCall
		toolsByCall            = make(map[string]*model.ToolCall)
		currentTurn            *model.AgentTurn
		sequence               int
		lastTimestamp          = sess.StartTime
		lastAssistantStop      string
		hasAssistantStop       bool
		lastRecordWasToolRes   bool
		explicitEnd            bool
	)

	addMessage := func(ev model.AgentEvent, role, channel, text string, ts *string, system bool) {
		if text == "" {
			return
		}
		var turnID *string
		if currentTurn != nil {
			id := currentTurn.ID
			turnID = &id
		}
		evID := ev.ID
		messages = append(messages, model.Message{
			ID:                stableID("msg", fmt.Sprintf("%s:%d:%d", sess.ID, ev.EventIndex, sequence)),
			AgentSessionID:    sess.ID,
			TurnID:            turnID,
			EventID:           &evID,
			Role:              role,
			Channel:           channel,
			Text:              text,
			MarkdownText:      strPtr(text),
			CopyText:          text,
			Sequence:          sequence,
			Timestamp:         ts,
			IsVisible:         channel != "reasoning",
			IsSystemGenerated: system,
			CreatedAt:         nowStr,
		})
		sequence++
	}

	finishTurn := func(ts *string, status string) {
		if currentTurn == nil {
			return
		}
		if currentTurn.CompletedAt == nil {
			currentTurn.CompletedAt = ts
		}
		currentTurn.Status = status
		currentTurn.Outcome = status
		currentTurn.DurationMS = elapsedMS(currentTurn.StartedAt, currentTurn.CompletedAt)
		currentTurn = nil
	}

	for i, ev := range allEvents {
		if !activeLines[ev.LineNumber] {
			continue
		}
		raw := decoded[i]
		recType := strField(raw, "type")
		ts := ev.EventTimestamp
		if ts == nil {
			ts = lastTimestamp
		} else {
			lastTimestamp = ts
		}

		piApplyMetadata(&sess, recType, raw, ts)

		switch recType {
		case "session_end", "session_shutdown":
			explicitEnd = true
			continue
		case "compaction", "compaction_summary", "branch_summary":
			if text := piSummaryText(raw); text != "" {
				addMessage(ev, "system", "commentary", text, ts, true)
			}
			continue
		case "model_change", "thinking_level_change", "session_name", "session_info":
			continue
		}
		if recType != "message" {
			continue
		}

		msg, ok := raw["message"].(map[string]any)
		if !ok {
			continue
		}
		role := strField(msg, "role")
		switch role {
		case "user":
			if currentTurn != nil {
				finishTurn(ts, "completed")
			}
			turn := model.AgentTurn{
				ID:               stableID("turn", fmt.Sprintf("%s:%d", sess.ID, len(turns))),
				Agent:            sess.Agent,
				AgentSessionID:   sess.ID,
				TurnIndex:        len(turns),
				TurnID:           strPtr(strField(raw, "id")),
				Status:           "running",
				Outcome:          "running",
				ValidationStatus: "unknown",
				MetricConfidence: "derived",
				StartedAt:        ts,
				CreatedAt:        nowStr,
				UpdatedAt:        nowStr,
			}
			turns = append(turns, turn)
			currentTurn = &turns[len(turns)-1]
			for _, block := range piContentBlocks(msg) {
				if strField(block, "type") == "text" {
					text := piExtractText(block["text"])
					if strings.TrimSpace(text) != "" {
						addMessage(ev, "user", "final", text, ts, false)
					}
				}
			}
			lastRecordWasToolRes = false
			lastAssistantStop = ""
			hasAssistantStop = false

		case "system":
			for _, block := range piContentBlocks(msg) {
				if strField(block, "type") == "text" {
					addMessage(ev, "system", "commentary", piExtractText(block["text"]), ts, true)
				}
			}

		case "assistant":
			if currentTurn == nil {
				turn := model.AgentTurn{
					ID:               stableID("turn", fmt.Sprintf("%s:%d", sess.ID, len(turns))),
					Agent:            sess.Agent,
					AgentSessionID:   sess.ID,
					TurnIndex:        len(turns),
					TurnID:           strPtr(strField(raw, "id")),
					Status:           "running",
					Outcome:          "running",
					ValidationStatus: "unknown",
					MetricConfidence: "derived",
					StartedAt:        ts,
					CreatedAt:        nowStr,
					UpdatedAt:        nowStr,
				}
				turns = append(turns, turn)
				currentTurn = &turns[len(turns)-1]
			}
			inTok, cachedTok, cacheCreateTok, outTok, reasonTok, totTok := piMessageUsage(msg)
			currentTurn.InputTokens += inTok
			currentTurn.CachedInputTokens += cachedTok
			currentTurn.CacheCreationInputTokens += cacheCreateTok
			currentTurn.OutputTokens += outTok
			currentTurn.ReasoningOutputTokens += reasonTok
			currentTurn.TotalTokens += totTok

			for _, block := range piContentBlocks(msg) {
				bType := strField(block, "type")
				switch bType {
				case "thinking":
					text := piExtractText(block["thinking"])
					if text == "" {
						text = piExtractText(block["text"])
					}
					addMessage(ev, "assistant", "reasoning", text, ts, false)
				case "text":
					text := piExtractText(block["text"])
					if strings.TrimSpace(text) != "" {
						addMessage(ev, "assistant", "final", text, ts, false)
					}
				case "toolCall":
					callID := strField(block, "id")
					if callID == "" {
						callID = fmt.Sprintf("%d:%d", ev.EventIndex, sequence)
					}
					name := strField(block, "name")
					if name == "" {
						name = "unknown"
					}
					name = truncateRunes(name, 255)
					argsMap, _ := block["arguments"].(map[string]any)
					cmdText := piCommandText(argsMap)
					var turnID *string
					if currentTurn != nil {
						tid := currentTurn.ID
						turnID = &tid
					}
					evID := ev.ID
					mutating := piIsMutatingTool(name, cmdText)
					var argsRaw json.RawMessage
					if argsMap != nil {
						argsRaw = marshalSafeJSON(argsMap)
					}
					tc := &model.ToolCall{
						ID:             stableID("tool", fmt.Sprintf("%s:%s", sess.ID, callID)),
						Agent:          sess.Agent,
						AgentSessionID: sess.ID,
						TurnID:         turnID,
						EventID:        &evID,
						CallID:         callID,
						ToolName:       name,
						ToolKind:       piToolKind(name),
						ArgumentsJSON:  argsRaw,
						CommandText:    strPtr(cmdText),
						StartedAt:      ts,
						Status:         "pending",
						IsMutating:     mutating,
						CreatedAt:      nowStr,
					}
					toolList = append(toolList, tc)
					toolsByCall[callID] = tc
				}
			}
			if stop := strField(msg, "stopReason"); stop != "" {
				lastAssistantStop = stop
				hasAssistantStop = true
			}
			lastRecordWasToolRes = false

		case "toolResult":
			callID := strField(msg, "toolCallId")
			tc := toolsByCall[callID]
			if tc == nil {
				continue
			}
			out := piExtractText(msg["content"])
			if out == "" {
				out = piExtractText(msg["details"])
			}
			tc.OutputText = strPtr(out)
			tc.CompletedAt = ts
			tc.ExitCode = piExitCode(msg["details"])
			isErr, _ := msg["isError"].(bool)
			if isErr || (tc.ExitCode != nil && *tc.ExitCode != 0) {
				tc.Status = "failed"
				if tc.ExitCode == nil {
					code := 1
					tc.ExitCode = &code
				}
			} else {
				tc.Status = "ok"
			}
			lastRecordWasToolRes = true
			delete(toolsByCall, callID)
		}
	}

	recent := isRecentFileMtime(sess.FileMtimeNs, OngoingSeconds*time.Second)
	ongoing := !explicitEnd && recent && (lastRecordWasToolRes || !hasAssistantStop || lastAssistantStop == "toolUse" || lastAssistantStop == "tool_use" || lastAssistantStop == "error")
	failed := lastAssistantStop == "error" || lastAssistantStop == "aborted" || lastAssistantStop == "length"
	if currentTurn != nil && !ongoing {
		st := "completed"
		if failed {
			st = "failed"
		}
		finishTurn(lastTimestamp, st)
	} else if currentTurn != nil && failed {
		currentTurn.Status = "failed"
		currentTurn.Outcome = "failed"
	}

	var spans []model.ActivitySpan
	turnByID := make(map[string]*model.AgentTurn, len(turns))
	for i := range turns {
		turnByID[turns[i].ID] = &turns[i]
		if sp := buildTurnSpan(sess.Agent, sess.SourceKind, sess, turns[i]); sp != nil {
			spans = append(spans, *sp)
		}
	}

	finalTools := make([]model.ToolCall, 0, len(toolList))
	for _, tc := range toolList {
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
	sess.InputTokens = 0
	sess.CachedInputTokens = 0
	sess.CacheCreationInputTokens = 0
	sess.OutputTokens = 0
	sess.ReasoningOutputTokens = 0
	sess.TotalTokens = 0
	for _, tr := range turns {
		sess.InputTokens += tr.InputTokens
		sess.CachedInputTokens += tr.CachedInputTokens
		sess.CacheCreationInputTokens += tr.CacheCreationInputTokens
		sess.OutputTokens += tr.OutputTokens
		sess.ReasoningOutputTokens += tr.ReasoningOutputTokens
		sess.TotalTokens += tr.TotalTokens
	}
	sess.IsOngoing = ongoing
	if ongoing {
		sess.EndTime = nil
	} else if lastTimestamp != nil {
		sess.EndTime = lastTimestamp
	}
	if lastTimestamp != nil {
		sess.LastActivityAt = lastTimestamp
	} else {
		sess.LastActivityAt = sess.StartTime
	}
	sess.ProjectedEventIndex = maxEventIndex

	return sess, model.SessionProjection{
		Turns:     turns,
		Messages:  messages,
		ToolCalls: finalTools,
		Spans:     spans,
	}, nil
}

type piGraphNode struct {
	lineNumber int64
	parentID   string
	recType    string
}

func piActiveBranchLines(byID map[string]piGraphNode, allLines map[int64]bool) map[int64]bool {
	parents := make(map[string]bool, len(byID))
	for _, node := range byID {
		if node.parentID != "" {
			parents[node.parentID] = true
		}
	}
	bestID := ""
	var bestLine int64 = -1
	for id, node := range byID {
		if !parents[id] && node.recType != "session" && node.lineNumber > bestLine {
			bestLine = node.lineNumber
			bestID = id
		}
	}
	if bestID == "" {
		return allLines
	}
	selected := make(map[int64]bool)
	seen := make(map[string]bool)
	curID := bestID
	for curID != "" {
		if seen[curID] {
			break
		}
		node, ok := byID[curID]
		if !ok {
			break
		}
		seen[curID] = true
		selected[node.lineNumber] = true
		curID = node.parentID
	}
	if len(selected) == 0 {
		return allLines
	}
	return selected
}

func piApplyMetadata(sess *model.AgentSession, recType string, raw map[string]any, ts *string) {
	switch recType {
	case "session":
		if id := strField(raw, "id"); id != "" {
			sess.ThreadID = id
		}
		if cwd := strField(raw, "cwd"); cwd != "" {
			sess.CWD = strPtr(cwd)
		}
		if v, ok := raw["version"]; ok && v != nil {
			sess.CLIVersion = strPtr(truncateRunes(fmt.Sprint(v), 64))
		}
		if ts != nil {
			sess.StartTime = ts
		}
	case "model_change":
		if m := strField(raw, "modelId", "model"); m != "" {
			sess.Model = strPtr(truncateRunes(m, 255))
		}
	case "message":
		if msg, ok := raw["message"].(map[string]any); ok {
			if m := strField(msg, "model"); m != "" {
				sess.Model = strPtr(truncateRunes(m, 255))
			}
			if sess.StartTime == nil && ts != nil {
				sess.StartTime = ts
			}
			if sess.DisplayTitle == nil && strField(msg, "role") == "user" {
				for _, block := range piContentBlocks(msg) {
					if strField(block, "type") == "text" {
						trimmed := strings.TrimSpace(piExtractText(block["text"]))
						if trimmed != "" {
							sess.DisplayTitle = strPtr(truncateRunes(trimmed, 255))
							break
						}
					}
				}
			}
		}
	case "session_end", "session_shutdown":
		if ts != nil {
			sess.EndTime = ts
		}
	}
	if recType == "session" || recType == "session_name" || recType == "session_info" || recType == "custom" {
		data := raw
		if sub, ok := raw["data"].(map[string]any); ok {
			data = sub
		}
		if name := strField(data, "name", "title", "sessionName", "session_name"); strings.TrimSpace(name) != "" {
			sess.ThreadName = strPtr(truncateRunes(strings.TrimSpace(name), 255))
		}
	}
}

func piContentBlocks(msg map[string]any) []map[string]any {
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

func piExtractText(v any) string {
	switch val := v.(type) {
	case string:
		return sanitizeString(val)
	case []any:
		parts := make([]string, 0, len(val))
		for _, item := range val {
			if part := piExtractText(item); part != "" {
				parts = append(parts, part)
			}
		}
		return strings.Join(parts, "\n")
	case map[string]any:
		if t, ok := val["text"].(string); ok {
			return sanitizeString(t)
		}
		if _, hasContent := val["content"]; hasContent {
			return piExtractText(val["content"])
		}
		if th, ok := val["thinking"].(string); ok {
			return sanitizeString(th)
		}
		return ""
	case nil:
		return ""
	default:
		return sanitizeString(fmt.Sprint(val))
	}
}

func piSummaryText(raw map[string]any) string {
	data := raw
	if sub, ok := raw["data"].(map[string]any); ok {
		data = sub
	}
	for _, key := range []string{"summary", "text", "message", "content"} {
		if val, ok := data[key]; ok {
			if text := strings.TrimSpace(piExtractText(val)); text != "" {
				return text
			}
		}
	}
	return ""
}

func piMessageUsage(msg map[string]any) (inTok, cachedTok, cacheCreateTok, outTok, reasonTok, totTok int64) {
	usage, ok := msg["usage"].(map[string]any)
	if !ok {
		return 0, 0, 0, 0, 0, 0
	}
	inTok = asInt64(usage["input"])
	cachedTok = asInt64(usage["cacheRead"])
	cacheCreateTok = asInt64(usage["cacheWrite"])
	outTok = asInt64(usage["output"])
	// Pi includes reasoning output in `output`, so track it separately without double-counting into total.
	reasonTok = asInt64(usage["reasoning"])
	totTok = asInt64(usage["totalTokens"])
	if totTok == 0 {
		totTok = inTok + cachedTok + cacheCreateTok + outTok
	}
	return inTok, cachedTok, cacheCreateTok, outTok, reasonTok, totTok
}

func piToolKind(name string) string {
	norm := strings.ToLower(strings.TrimSpace(name))
	if k, ok := piToolKinds[norm]; ok {
		return k
	}
	if strings.HasPrefix(norm, "shell") || strings.HasPrefix(norm, "exec") {
		return "exec"
	}
	if norm == "" {
		return "tool"
	}
	return truncateRunes(norm, 64)
}

func piCommandText(args map[string]any) string {
	if len(args) == 0 {
		return ""
	}
	for _, k := range []string{"command", "cmd", "path", "file_path"} {
		if s := strField(args, k); s != "" {
			return s
		}
	}
	raw, err := json.Marshal(sanitizeMap(args))
	if err != nil {
		return ""
	}
	return string(raw)
}

func piIsMutatingTool(name, cmd string) bool {
	if piMutatingTools[strings.ToLower(name)] {
		return true
	}
	if cmd == "" {
		return false
	}
	for _, marker := range []string{" > ", " >> ", "rm ", "mv ", "git apply"} {
		if strings.Contains(cmd, marker) {
			return true
		}
	}
	return false
}

func piExitCode(details any) *int {
	d, ok := details.(map[string]any)
	if !ok {
		return nil
	}
	for _, key := range []string{"exitCode", "exit_code", "code"} {
		val, exists := d[key]
		if !exists || val == nil {
			continue
		}
		switch v := val.(type) {
		case float64:
			n := int(v)
			return &n
		case int64:
			n := int(v)
			return &n
		case int:
			n := v
			return &n
		case json.Number:
			if n, err := v.Int64(); err == nil {
				c := int(n)
				return &c
			}
		case string:
			if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
				return &n
			}
		}
		return nil
	}
	return nil
}
