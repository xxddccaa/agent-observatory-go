package adapters

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/agent-observatory/agent-observatory-go/internal/discovery"
	"github.com/agent-observatory/agent-observatory-go/internal/model"
)

const geminiMaxLegacyJSONBytes = 64 * 1024 * 1024

var (
	geminiKnownMsgTypes = map[string]bool{
		"user": true, "gemini": true, "info": true, "error": true, "warning": true,
	}
	geminiNormalizedTypes = map[string]string{
		"session_meta": "session_meta",
		"meta_set":     "session_update",
		"patch":        "message_patch",
		"rewind":       "history_rewind",
		"user":         "user_message",
		"gemini":       "assistant_response",
		"info":         "system_message",
		"warning":      "system_message",
		"error":        "error_message",
		"parse_error":  "parse_error",
	}
	geminiToolKinds = map[string]string{
		"read_file": "read", "read_many_files": "read", "list_directory": "read",
		"get_internal_docs": "read", "read_mcp_resource": "read", "list_mcp_resources": "read",
		"glob": "search", "grep_search": "search",
		"write_file": "write", "replace": "patch",
		"run_shell_command": "exec", "shell": "exec",
		"google_web_search": "web_search", "web_fetch": "fetch",
		"write_todos": "plan", "enter_plan_mode": "plan", "exit_plan_mode": "plan",
		"tracker_create_task": "plan", "tracker_update_task": "plan", "tracker_get_task": "plan", "tracker_list_tasks": "plan",
		"invoke_agent": "agent", "activate_skill": "agent", "complete_task": "agent", "update_topic": "agent",
		"ask_user": "user_input", "take_snapshot": "snapshot",
	}
	geminiMutatingShellRe = regexp.MustCompile(`(?i)(?:\brm\b|\bmv\b|\bcp\b|\bmkdir\b|\btouch\b|\bchmod\b|\bchown\b|\bgit\s+(?:add|commit|push|reset|checkout|switch|merge|rebase)\b|>>?|sed\s+-i|\btee\b)`)
	geminiExitCodeRe      = regexp.MustCompile(`(?:Exit Code|Command exited with code):\s*(-?\d+)|"exitCode"\s*:\s*(-?\d+)`)
	geminiSessionCtxRe    = regexp.MustCompile(`(?s)^\s*(<session_context>.*?</session_context>)\s*`)
)

// GeminiAdapter indexes Gemini CLI JSONL and legacy JSON conversations under ~/.gemini/tmp.
// root is the Gemini home directory (e.g. ~/.gemini, or its parent if tmpDir is passed as ~/.gemini/tmp).
type GeminiAdapter struct {
	home          string
	parserVersion string
	mu            sync.Mutex
	projIndex     *geminiProjectIndex
}

// NewGeminiAdapter creates a Gemini CLI adapter. Accepts either ~/.gemini or ~/.gemini/tmp.
func NewGeminiAdapter(root string) *GeminiAdapter {
	expanded := filepath.Clean(discovery.ExpandPath(root))
	if filepath.Base(expanded) == "tmp" {
		expanded = filepath.Dir(expanded)
	}
	return &GeminiAdapter{
		home:          expanded,
		parserVersion: "1",
		projIndex:     newGeminiProjectIndex(expanded),
	}
}

func (a *GeminiAdapter) Kind() string          { return "gemini_jsonl" }
func (a *GeminiAdapter) Agent() string         { return "gemini" }
func (a *GeminiAdapter) ParserVersion() string { return a.parserVersion }

func (a *GeminiAdapter) tmpRoot() string {
	return filepath.Join(a.home, "tmp")
}

func (a *GeminiAdapter) Identity(path string) string {
	return "gemini:" + sha256Hex([]byte(discovery.CachedResolve(path)))
}

func geminiLayoutFor(path string) (slugDir, parentSessionID, fallbackThreadID string) {
	clean := filepath.Clean(path)
	stem := fileStem(clean)
	parent := filepath.Dir(clean)
	grandParent := filepath.Dir(parent)
	if filepath.Base(parent) == "chats" {
		return grandParent, "", stem
	}
	if filepath.Base(grandParent) == "chats" {
		return filepath.Dir(grandParent), filepath.Base(parent), stem
	}
	return parent, "", stem
}

func (a *GeminiAdapter) WatchPaths(src SourceDescriptor) []string {
	slugDir, _, _ := geminiLayoutFor(src.Path)
	return []string{src.Path, filepath.Join(a.home, "projects.json"), filepath.Join(slugDir, ".project_root")}
}

func (a *GeminiAdapter) Discover(_ bool) ([]SourceDescriptor, error) {
	tmp := a.tmpRoot()
	slugEntries, err := os.ReadDir(tmp)
	if err != nil {
		return nil, nil
	}

	var out []SourceDescriptor
	for _, entry := range slugEntries {
		if strings.HasPrefix(entry.Name(), ".") || entry.Name() == "background-processes" {
			continue
		}
		if entry.Type()&os.ModeSymlink != 0 || !entry.IsDir() {
			continue
		}
		chatsDir := filepath.Join(tmp, entry.Name(), "chats")
		chatEntries, err := os.ReadDir(chatsDir)
		if err != nil {
			continue
		}

		jsonlStems := make(map[string]bool)
		var jsonCands []string
		var found []string

		for _, ce := range chatEntries {
			name := ce.Name()
			if strings.HasPrefix(name, ".") {
				continue
			}
			full := filepath.Join(chatsDir, name)
			if ce.Type()&os.ModeSymlink == 0 && ce.IsDir() {
				subEntries, subErr := os.ReadDir(full)
				if subErr == nil {
					for _, se := range subEntries {
						if !strings.HasPrefix(se.Name(), ".") && strings.HasSuffix(se.Name(), ".jsonl") && se.Type()&os.ModeSymlink == 0 && !se.IsDir() {
							found = append(found, filepath.Join(full, se.Name()))
						}
					}
				}
				continue
			}
			if ce.Type()&os.ModeSymlink != 0 || ce.IsDir() {
				continue
			}
			if strings.HasSuffix(name, ".jsonl") {
				jsonlStems[fileStem(full)] = true
				found = append(found, full)
			} else if strings.HasSuffix(name, ".json") {
				jsonCands = append(jsonCands, full)
			}
		}

		for _, leg := range jsonCands {
			if !jsonlStems[fileStem(leg)] {
				found = append(found, leg)
			}
		}
		sort.Strings(found)
		for _, p := range found {
			fmtName := "json"
			if strings.HasSuffix(p, ".jsonl") {
				fmtName = "jsonl"
			}
			out = append(out, SourceDescriptor{
				Kind:     a.Kind(),
				Identity: a.Identity(p),
				Path:     p,
				Format:   fmtName,
				Agent:    a.Agent(),
				ThreadID: fileStem(p),
			})
		}
	}
	return out, nil
}

func (a *GeminiAdapter) Plan(src SourceDescriptor, cp *model.IndexCheckpoint) (SourcePlan, error) {
	slugDir, _, _ := geminiLayoutFor(src.Path)
	a.mu.Lock()
	metaDigest := a.projIndex.digest(slugDir)
	a.mu.Unlock()
	extra := map[string]any{"meta": metaDigest}

	if src.Format == "json" {
		return planStatRebuild(src, cp, a.parserVersion, extra)
	}
	return planAppendJSONL(src, cp, a.parserVersion, appendPlanOpts{
		extraFP:           extra,
		checkPath:         true,
		checkPrefix:       true,
		rebuildOnSameSize: false,
	})
}

func (a *GeminiAdapter) Parse(plan SourcePlan, existing *model.AgentSession) (*ParsedSource, error) {
	src := plan.Source
	realPath := discovery.CachedResolve(src.Path)
	_, parentSessionID, fallbackTID := geminiLayoutFor(src.Path)
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
		RolloutRealPath:     strPtr("gemini:" + realPath),
		SourcePath:          strPtr(realPath),
		RolloutFormat:       src.Format,
		SourceFormat:        src.Format,
		ThreadID:            fallbackTID,
		ParentSessionID:     strPtr(parentSessionID),
		IsSubagent:          parentSessionID != "",
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
		sess.RolloutRealPath = strPtr("gemini:" + realPath)
		sess.SourcePath = strPtr(realPath)
		if parentSessionID != "" {
			sess.ParentSessionID = strPtr(parentSessionID)
			sess.IsSubagent = true
		}
		sess.UpdatedAt = nowStr
		startOffset = existing.LastIndexedOffset
		startLine = existing.LastIndexedLine
		eventIndex = int(existing.LastIndexedLine)
	}

	var newEvents []model.AgentEvent
	if src.Format == "json" {
		newEvents, sess.LastIndexedOffset, sess.LastIndexedLine, err = a.parseLegacyJSON(src.Path, sess.ID, eventIndex, nowStr)
		if err != nil {
			return nil, err
		}
	} else {
		lines, readErr := readJSONLLines(src.Path, startOffset, MaxLineBytes, PrefixBytes, true)
		if readErr != nil {
			return nil, readErr
		}
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
			kind, ts := geminiClassifyRecord(safe)
			norm := geminiNormalizedTypes[kind]
			if norm == "" {
				norm = kind
			}
			payType := strField(safe, "type")
			if payType == "" {
				payType = kind
			}
			newEvents = append(newEvents, model.AgentEvent{
				AgentSessionID: sess.ID,
				EventIndex:     eventIndex,
				LineNumber:     curLine,
				ByteOffset:     ln.ByteOffset,
				EventTimestamp: ts,
				TopLevelType:   strPtr(kind),
				PayloadType:    strPtr(payType),
				NormalizedType: norm,
				RawJSON:        marshalSafeJSON(safe),
				RawHash:        sha256Hex(ln.Raw),
				IsKnown:        kind != "unknown",
				ParserVersion:  a.parserVersion,
				CreatedAt:      nowStr,
			})
			eventIndex++
			sess.LastIndexedLine = curLine
			sess.LastIndexedOffset = ln.ByteOffset + ln.ByteLength
		}
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

func (a *GeminiAdapter) parseLegacyJSON(path, sessionID string, eventIndex int, nowStr string) ([]model.AgentEvent, int64, int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, 0, 0, err
	}
	if info.Size() > geminiMaxLegacyJSONBytes {
		return []model.AgentEvent{{
			AgentSessionID: sessionID,
			EventIndex:     eventIndex,
			LineNumber:     1,
			TopLevelType:   strPtr("parse_error"),
			PayloadType:    strPtr("parse_error"),
			NormalizedType: "parse_error",
			RawJSON:        unparsedEventJSON("file_too_large", nil, info.Size(), nil),
			RawHash:        sha256Hex([]byte(fmt.Sprintf("%s:%d", path, info.Size()))),
			IsKnown:        false,
			ParserVersion:  a.parserVersion,
			CreatedAt:      nowStr,
		}}, info.Size(), 1, nil
	}

	rawBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, 0, err
	}
	rawHash := sha256Hex(rawBytes)
	var decoded map[string]any
	if err := json.Unmarshal(sanitizeUTF8AndNUL(rawBytes), &decoded); err != nil || decoded == nil {
		return []model.AgentEvent{{
			AgentSessionID: sessionID,
			EventIndex:     eventIndex,
			LineNumber:     1,
			TopLevelType:   strPtr("parse_error"),
			PayloadType:    strPtr("parse_error"),
			NormalizedType: "parse_error",
			RawJSON:        unparsedEventJSON("invalid_json", rawBytes, int64(len(rawBytes)), nil),
			RawHash:        rawHash,
			IsKnown:        false,
			ParserVersion:  a.parserVersion,
			CreatedAt:      nowStr,
		}}, int64(len(rawBytes)), 1, nil
	}

	safe := sanitizeMap(decoded)
	header := make(map[string]any, len(safe))
	for k, v := range safe {
		if k != "messages" {
			header[k] = v
		}
	}
	headerTS := parseTimestamp(firstNonNil(header["startTime"], header["lastUpdated"]))
	events := []model.AgentEvent{{
		AgentSessionID: sessionID,
		EventIndex:     eventIndex,
		LineNumber:     1,
		EventTimestamp: headerTS,
		TopLevelType:   strPtr("session_meta"),
		PayloadType:    strPtr("session_meta"),
		NormalizedType: "session_meta",
		RawJSON:        marshalSafeJSON(header),
		RawHash:        rawHash,
		IsKnown:        true,
		ParserVersion:  a.parserVersion,
		CreatedAt:      nowStr,
	}}
	eventIndex++
	lineNo := int64(1)
	if msgs, ok := safe["messages"].([]any); ok {
		for _, item := range msgs {
			msg, ok := item.(map[string]any)
			if !ok {
				continue
			}
			lineNo++
			kind, ts := geminiClassifyRecord(msg)
			norm := geminiNormalizedTypes[kind]
			if norm == "" {
				norm = kind
			}
			payType := strField(msg, "type")
			if payType == "" {
				payType = kind
			}
			rawMsg := marshalSafeJSON(msg)
			events = append(events, model.AgentEvent{
				AgentSessionID: sessionID,
				EventIndex:     eventIndex,
				LineNumber:     lineNo,
				EventTimestamp: ts,
				TopLevelType:   strPtr(kind),
				PayloadType:    strPtr(payType),
				NormalizedType: norm,
				RawJSON:        rawMsg,
				RawHash:        sha256Hex(rawMsg),
				IsKnown:        kind != "unknown",
				ParserVersion:  a.parserVersion,
				CreatedAt:      nowStr,
			})
			eventIndex++
		}
	}
	return events, int64(len(rawBytes)), lineNo, nil
}

func geminiClassifyRecord(decoded map[string]any) (string, *string) {
	if _, ok := decoded["$rewindTo"].(string); ok {
		return "rewind", nil
	}
	if _, ok := decoded["$patch"].(map[string]any); ok {
		return "patch", nil
	}
	if _, hasPatch := decoded["$patch"]; !hasPatch && strField(decoded, "id") != "" {
		msgType := strField(decoded, "type")
		if msgType == "" {
			msgType = "unknown"
		}
		ts := parseTimestamp(decoded["timestamp"])
		if geminiKnownMsgTypes[msgType] {
			return msgType, ts
		}
		return "unknown", ts
	}
	if setObj, ok := decoded["$set"].(map[string]any); ok {
		return "meta_set", parseTimestamp(setObj["lastUpdated"])
	}
	if strField(decoded, "sessionId") != "" && (decoded["projectHash"] != nil || decoded["startTime"] != nil || decoded["messages"] != nil) {
		return "session_meta", parseTimestamp(firstNonNil(decoded["startTime"], decoded["lastUpdated"]))
	}
	return "unknown", parseTimestamp(firstNonNil(decoded["timestamp"], decoded["lastUpdated"]))
}

type geminiReplayEntry struct {
	eventID int64
	raw     map[string]any
}

type geminiReplayState struct {
	meta       map[string]any
	orderedIDs []string
	byID       map[string]geminiReplayEntry
}

func (a *GeminiAdapter) Project(sess model.AgentSession, allEvents []model.AgentEvent) (model.AgentSession, model.SessionProjection, error) {
	nowStr := model.FormatUTC(time.Now())
	slugDir, _, _ := geminiLayoutFor(stringOrEmpty(sess.RolloutPath))
	state := geminiReplayState{
		meta: make(map[string]any),
		byID: make(map[string]geminiReplayEntry),
	}

	maxIdx := -1
	for _, ev := range allEvents {
		if ev.EventIndex > maxIdx {
			maxIdx = ev.EventIndex
		}
		top := ""
		if ev.TopLevelType != nil {
			top = *ev.TopLevelType
		}
		applyGeminiReplay(&state, ev.ID, top, decodeObjectMap(ev.RawJSON))
	}

	if sid := strField(state.meta, "sessionId"); sid != "" {
		sess.ThreadID = strings.TrimSpace(sid)
	}

	var dirs []string
	if rawDirs, ok := state.meta["directories"].([]string); ok {
		dirs = rawDirs
	}
	a.mu.Lock()
	cwd := a.projIndex.resolveWorkspace(slugDir, strField(state.meta, "projectHash"), dirs)
	a.mu.Unlock()
	if cwd != "" {
		sess.CWD = strPtr(cwd)
	}
	if summary := strField(state.meta, "summary"); summary != "" {
		sess.AITitle = strPtr(truncateRunes(collapseSpaces(summary), 255))
	}
	sess.StartTime = parseTimestamp(state.meta["startTime"])
	endTime := parseTimestamp(state.meta["lastUpdated"])

	var (
		turns         []model.AgentTurn
		messages      []model.Message
		toolsByCall   = make(map[string]*model.ToolCall)
		orderedTools  []string
		currentTurn   *model.AgentTurn
		sequence      int
		firstUser     string
		lastModel     string
		inputTotal    int64
		cachedTotal   int64
		outputTotal   int64
		reasonTotal   int64
		tokensTotal   int64
		pendingError  bool
	)

	ongoing := isRecentFileMtime(sess.FileMtimeNs, OngoingSeconds*time.Second)

	finishTurn := func(ts *string, status string) {
		if currentTurn == nil {
			return
		}
		if ts != nil && (currentTurn.CompletedAt == nil || *ts > *currentTurn.CompletedAt) {
			currentTurn.CompletedAt = ts
		}
		currentTurn.Status = status
		currentTurn.Outcome = status
		currentTurn.DurationMS = elapsedMS(currentTurn.StartedAt, currentTurn.CompletedAt)
		currentTurn = nil
	}

	openTurn := func(ts *string) *model.AgentTurn {
		idx := len(turns)
		tr := model.AgentTurn{
			ID:               stableID("turn", fmt.Sprintf("%s:%d", sess.ID, idx)),
			Agent:            sess.Agent,
			AgentSessionID:   sess.ID,
			TurnIndex:        idx,
			TurnID:           strPtr(fmt.Sprintf("turn-%d", idx)),
			Status:           "running",
			Outcome:          "running",
			StartedAt:        ts,
			ValidationStatus: "unknown",
			MetricConfidence: "derived",
			CreatedAt:        nowStr,
			UpdatedAt:        nowStr,
		}
		turns = append(turns, tr)
		currentTurn = &turns[len(turns)-1]
		pendingError = false
		return currentTurn
	}

	addMsg := func(eventID int64, msgKey, role, channel, text string, ts *string, system bool) {
		if text == "" {
			return
		}
		messages = append(messages, model.Message{
			ID:                stableID("msg", fmt.Sprintf("%s:%s:%d", sess.ID, msgKey, sequence)),
			AgentSessionID:    sess.ID,
			TurnID:            turnPtrID(currentTurn),
			EventID:           &eventID,
			Role:              role,
			Channel:           channel,
			Sequence:          sequence,
			Text:              text,
			MarkdownText:      strPtr(text),
			CopyText:          text,
			Timestamp:         ts,
			IsVisible:         channel != "reasoning",
			IsSystemGenerated: system,
			CreatedAt:         nowStr,
		})
		sequence++
	}

	for _, msgID := range state.orderedIDs {
		entry := state.byID[msgID]
		rec := entry.raw
		msgType := strField(rec, "type")
		ts := parseTimestamp(rec["timestamp"])
		if ts != nil {
			if sess.StartTime == nil || *ts < *sess.StartTime {
				sess.StartTime = ts
			}
			if endTime == nil || *ts > *endTime {
				endTime = ts
			}
		}
		if m := strings.TrimSpace(strField(rec, "model")); m != "" {
			lastModel = truncateRunes(m, 128)
		}

		switch msgType {
		case "user":
			visible, _, syntheticOnly := geminiExtractPartText(rec["content"])
			if visible == "" && rec["displayContent"] != nil {
				dispVis, _, _ := geminiExtractPartText(rec["displayContent"])
				if dispVis != "" {
					visible = dispVis
					syntheticOnly = false
				}
			}
			if contentList, ok := rec["content"].([]any); ok && ts != nil {
				for _, part := range contentList {
					pm, ok := part.(map[string]any)
					if !ok {
						continue
					}
					if fnResp, ok := pm["functionResponse"].(map[string]any); ok {
						if cid := strField(fnResp, "id"); cid != "" {
							if tc, exists := toolsByCall[cid]; exists && tc.CompletedAt == nil {
								tc.CompletedAt = ts
							}
						}
					}
				}
			}
			if syntheticOnly || visible == "" {
				continue
			}
			if loc := geminiSessionCtxRe.FindStringIndex(visible); loc != nil {
				ctxBlock := strings.TrimSpace(visible[loc[0]:loc[1]])
				addMsg(entry.eventID, msgID+":ctx", "system", "commentary", ctxBlock, ts, true)
				visible = strings.TrimSpace(visible[loc[1]:])
				if visible == "" {
					continue
				}
			}
			if currentTurn != nil {
				st := "completed"
				if pendingError {
					st = "failed"
				}
				finishTurn(ts, st)
			}
			openTurn(ts)
			if firstUser == "" {
				firstUser = truncateRunes(collapseSpaces(visible), 255)
			}
			addMsg(entry.eventID, msgID, "user", "final", visible, ts, false)

		case "gemini":
			if currentTurn == nil {
				openTurn(ts)
			}
			pendingError = false
			if tokens, ok := rec["tokens"].(map[string]any); ok {
				tIn := asNonNegInt64(tokens["input"])
				tCached := asNonNegInt64(tokens["cached"])
				tOut := asNonNegInt64(tokens["output"])
				tThoughts := asNonNegInt64(tokens["thoughts"])
				tTool := asNonNegInt64(tokens["tool"])
				tTotal := asNonNegInt64(tokens["total"])
				if tTotal == 0 {
					tTotal = tIn + tCached + tOut + tThoughts + tTool
				}
				inputTotal += tIn
				cachedTotal += tCached
				outputTotal += tOut
				reasonTotal += tThoughts
				tokensTotal += tTotal
				currentTurn.InputTokens += tIn
				currentTurn.CachedInputTokens += tCached
				currentTurn.OutputTokens += tOut
				currentTurn.ReasoningOutputTokens += tThoughts
				currentTurn.TotalTokens += tTotal
			}

			visible, inlineThoughts, _ := geminiExtractPartText(rec["content"])
			thoughtText := geminiFormatThoughts(rec["thoughts"])
			if inlineThoughts != "" {
				if thoughtText != "" {
					thoughtText = thoughtText + "\n\n" + inlineThoughts
				} else {
					thoughtText = inlineThoughts
				}
			}
			if thoughtText != "" {
				addMsg(entry.eventID, msgID, "assistant", "reasoning", thoughtText, ts, false)
			}
			if visible != "" {
				addMsg(entry.eventID, msgID, "assistant", "final", visible, ts, false)
			}

			if tcalls, ok := rec["toolCalls"].([]any); ok {
				for idx, rawTC := range tcalls {
					tcMap, ok := rawTC.(map[string]any)
					if !ok {
						continue
					}
					callID := truncateRunes(strField(tcMap, "id"), 128)
					if callID == "" {
						callID = fmt.Sprintf("%s:call:%d", msgID, idx)
					}
					toolName := truncateRunes(strField(tcMap, "name", "displayName"), 128)
					if toolName == "" {
						toolName = "tool"
					}
					args := tcMap["args"]
					cmdText := geminiCommandText(toolName, args)
					outText, exitCode := geminiToolOutput(tcMap)
					startedAt := parseTimestamp(tcMap["timestamp"])
					if startedAt == nil {
						startedAt = ts
					}
					status := geminiToolStatus(tcMap["status"], exitCode, ongoing)
					var completedAt *string
					if status == "completed" || status == "failed" || status == "cancelled" {
						completedAt = ts
					}
					argsRaw := geminiArgsJSON(args)
					kind := geminiToolKind(toolName)
					mutating := geminiIsMutating(toolName, cmdText)

					if existingTool, ok := toolsByCall[callID]; ok {
						evID := entry.eventID
						existingTool.EventID = &evID
						tid := currentTurn.ID
						existingTool.TurnID = &tid
						existingTool.ToolName = toolName
						existingTool.ToolKind = kind
						existingTool.ArgumentsJSON = argsRaw
						existingTool.OutputText = strPtr(outText)
						existingTool.CommandText = strPtr(cmdText)
						existingTool.Status = status
						existingTool.ExitCode = exitCode
						existingTool.IsMutating = mutating
						if completedAt != nil {
							existingTool.CompletedAt = completedAt
						}
					} else {
						evID := entry.eventID
						tid := currentTurn.ID
						nt := &model.ToolCall{
							ID:             stableID("tool", fmt.Sprintf("%s:%s", sess.ID, callID)),
							Agent:          sess.Agent,
							AgentSessionID: sess.ID,
							TurnID:         &tid,
							EventID:        &evID,
							CallID:         callID,
							ToolName:       toolName,
							ToolKind:       kind,
							ArgumentsJSON:  argsRaw,
							CommandText:    strPtr(cmdText),
							OutputText:     strPtr(outText),
							ExitCode:       exitCode,
							Status:         status,
							IsMutating:     mutating,
							StartedAt:      startedAt,
							CompletedAt:    completedAt,
							CreatedAt:      nowStr,
						}
						toolsByCall[callID] = nt
						orderedTools = append(orderedTools, callID)
					}
				}
			}
			if ts != nil {
				currentTurn.CompletedAt = ts
			}

		case "info", "warning", "error":
			visible, _, _ := geminiExtractPartText(rec["content"])
			if msgType == "error" {
				pendingError = true
				if currentTurn != nil {
					currentTurn.Status = "failed"
					currentTurn.Outcome = "failed"
				}
			}
			if visible != "" {
				prefix := ""
				if msgType != "info" {
					prefix = fmt.Sprintf("[%s] ", msgType)
				}
				addMsg(entry.eventID, msgID, "system", "commentary", prefix+visible, ts, true)
			}
		}
	}

	if currentTurn != nil {
		if !ongoing {
			st := "completed"
			if pendingError {
				st = "failed"
			}
			finishTurn(endTime, st)
		} else {
			for _, cid := range orderedTools {
				tc := toolsByCall[cid]
				if tc.TurnID != nil && *tc.TurnID == currentTurn.ID && tc.Status == "running" {
					currentTurn.Status = "running"
					currentTurn.Outcome = "running"
					break
				}
			}
		}
	}

	turnByID := make(map[string]*model.AgentTurn, len(turns))
	var spans []model.ActivitySpan
	for i := range turns {
		turnByID[turns[i].ID] = &turns[i]
		if sp := buildTurnSpan(sess.Agent, sess.SourceKind, sess, turns[i]); sp != nil {
			spans = append(spans, *sp)
		}
	}
	finalTools := make([]model.ToolCall, 0, len(orderedTools))
	for _, cid := range orderedTools {
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

	sess.Model = strPtr(lastModel)
	sess.TurnCount = len(turns)
	sess.InputTokens = inputTotal
	sess.CachedInputTokens = cachedTotal
	sess.OutputTokens = outputTotal
	sess.ReasoningOutputTokens = reasonTotal
	sess.TotalTokens = tokensTotal
	if endTime != nil {
		sess.EndTime = endTime
		sess.LastActivityAt = endTime
	} else {
		sess.EndTime = sess.StartTime
		sess.LastActivityAt = sess.StartTime
	}
	sess.IsOngoing = ongoing
	if firstUser != "" {
		sess.DisplayTitle = strPtr(firstUser)
	} else if sess.AITitle != nil {
		sess.DisplayTitle = sess.AITitle
	}
	if sess.AITitle != nil && sess.ThreadName == nil {
		sess.ThreadName = sess.AITitle
	} else if sess.ThreadName == nil && sess.ParentSessionID != nil {
		sess.ThreadName = strPtr(sess.ThreadID)
	}
	sess.ProjectedEventIndex = maxIdx
	sess.UpdatedAt = nowStr

	return sess, model.SessionProjection{
		Turns:     turns,
		Messages:  messages,
		ToolCalls: finalTools,
		Spans:     spans,
	}, nil
}

func applyGeminiReplay(state *geminiReplayState, eventID int64, kind string, raw map[string]any) {
	setMessage := func(id string, msg map[string]any) {
		if _, exists := state.byID[id]; !exists {
			state.orderedIDs = append(state.orderedIDs, id)
		}
		state.byID[id] = geminiReplayEntry{eventID: eventID, raw: msg}
	}
	resetMessages := func(list []any) {
		state.orderedIDs = nil
		state.byID = make(map[string]geminiReplayEntry)
		for _, item := range list {
			if m, ok := item.(map[string]any); ok {
				if mid := strField(m, "id"); mid != "" {
					setMessage(mid, m)
				}
			}
		}
	}

	switch kind {
	case "session_meta":
		for _, k := range []string{"sessionId", "projectHash", "startTime", "lastUpdated", "kind", "summary"} {
			if v, ok := raw[k]; ok && v != nil {
				state.meta[k] = v
			}
		}
		if dirs, ok := raw["directories"].([]any); ok {
			state.meta["directories"] = toNonEmptyStrings(dirs)
		}
		if msgs, ok := raw["messages"].([]any); ok {
			resetMessages(msgs)
		}
	case "meta_set":
		setObj, ok := raw["$set"].(map[string]any)
		if !ok {
			return
		}
		for _, k := range []string{"sessionId", "projectHash", "startTime", "lastUpdated", "kind", "summary"} {
			if v, ok := setObj[k]; ok && v != nil {
				state.meta[k] = v
			}
		}
		if dirs, ok := setObj["directories"].([]any); ok {
			state.meta["directories"] = toNonEmptyStrings(dirs)
		}
		if msgs, ok := setObj["messages"].([]any); ok {
			resetMessages(msgs)
		}
	case "rewind":
		rewindID, _ := raw["$rewindTo"].(string)
		if rewindID == "" {
			return
		}
		cut := -1
		for i, id := range state.orderedIDs {
			if id == rewindID {
				cut = i
				break
			}
		}
		if cut < 0 {
			state.orderedIDs = nil
			state.byID = make(map[string]geminiReplayEntry)
			return
		}
		for _, dropID := range state.orderedIDs[cut:] {
			delete(state.byID, dropID)
		}
		state.orderedIDs = state.orderedIDs[:cut]
	case "patch":
		patchObj, ok := raw["$patch"].(map[string]any)
		if !ok {
			return
		}
		if strField(patchObj, "id") != "" {
			applyGeminiSinglePatch(state, patchObj)
		}
		if updates, ok := patchObj["updates"].([]any); ok {
			for _, u := range updates {
				if um, ok := u.(map[string]any); ok && strField(um, "id") != "" {
					applyGeminiSinglePatch(state, um)
				}
			}
		}
		if remIDs, ok := patchObj["removeIds"].([]any); ok {
			remSet := make(map[string]bool)
			for _, rid := range remIDs {
				if s, ok := rid.(string); ok {
					remSet[s] = true
					delete(state.byID, s)
				}
			}
			kept := state.orderedIDs[:0]
			for _, id := range state.orderedIDs {
				if !remSet[id] {
					kept = append(kept, id)
				}
			}
			state.orderedIDs = kept
		}
		if orderIDs, ok := patchObj["orderIds"].([]any); ok {
			seen := make(map[string]bool)
			var tail []string
			for _, oid := range orderIDs {
				if s, ok := oid.(string); ok && !seen[s] {
					if _, exists := state.byID[s]; exists {
						seen[s] = true
						tail = append(tail, s)
					}
				}
			}
			var prefix []string
			for _, id := range state.orderedIDs {
				if !seen[id] {
					prefix = append(prefix, id)
				}
			}
			state.orderedIDs = append(prefix, tail...)
		}
	default:
		if _, hasPatch := raw["$patch"]; !hasPatch {
			if id := strField(raw, "id"); id != "" {
				setMessage(id, raw)
			}
		}
	}
}

func applyGeminiSinglePatch(state *geminiReplayState, patch map[string]any) {
	msgID := strField(patch, "id")
	entry, ok := state.byID[msgID]
	if !ok {
		return
	}
	// Clone via JSON roundtrip to avoid mutating earlier entries.
	rawBytes, _ := json.Marshal(entry.raw)
	var updated map[string]any
	_ = json.Unmarshal(rawBytes, &updated)
	if updated == nil {
		return
	}
	if v, hasContent := patch["content"]; hasContent && v != nil {
		updated["content"] = v
	}
	if patchCalls, ok := patch["toolCalls"].([]any); ok && strField(updated, "type") == "gemini" {
		if existingCalls, ok := updated["toolCalls"].([]any); ok {
			byCallID := make(map[string]map[string]any)
			for _, item := range existingCalls {
				if cm, ok := item.(map[string]any); ok {
					if cid := strField(cm, "id"); cid != "" {
						byCallID[cid] = cm
					}
				}
			}
			for _, pc := range patchCalls {
				if pcm, ok := pc.(map[string]any); ok {
					if target := byCallID[strField(pcm, "id")]; target != nil {
						if res, hasRes := pcm["result"]; hasRes {
							target["result"] = res
						}
					}
				}
			}
		}
	}
	state.byID[msgID] = geminiReplayEntry{eventID: entry.eventID, raw: updated}
}

func toNonEmptyStrings(items []any) []string {
	var out []string
	for _, it := range items {
		if s, ok := it.(string); ok && strings.TrimSpace(s) != "" {
			out = append(out, strings.TrimSpace(s))
		}
	}
	return out
}

func geminiExtractPartText(content any) (visible string, thoughts string, syntheticOnly bool) {
	switch v := content.(type) {
	case string:
		return sanitizeString(v), "", false
	case []any:
		var visParts, thoughtParts []string
		hasText := false
		sawFnResp := false
		for _, part := range v {
			switch p := part.(type) {
			case string:
				if p != "" {
					visParts = append(visParts, sanitizeString(p))
					if strings.TrimSpace(p) != "" {
						hasText = true
					}
				}
			case map[string]any:
				if _, ok := p["functionResponse"]; ok {
					sawFnResp = true
					continue
				}
				if txt, ok := p["text"].(string); ok {
					if isThought, _ := p["thought"].(bool); isThought {
						if strings.TrimSpace(txt) != "" {
							thoughtParts = append(thoughtParts, sanitizeString(txt))
						}
					} else {
						visParts = append(visParts, sanitizeString(txt))
						if strings.TrimSpace(txt) != "" {
							hasText = true
						}
					}
				}
			}
		}
		visible = strings.TrimSpace(strings.Join(visParts, "\n"))
		thoughts = strings.TrimSpace(strings.Join(thoughtParts, "\n\n"))
		syntheticOnly = sawFnResp && !hasText && thoughts == ""
		return visible, thoughts, syntheticOnly
	default:
		return "", "", false
	}
}

func geminiFormatThoughts(raw any) string {
	list, ok := raw.([]any)
	if !ok {
		return ""
	}
	var blocks []string
	for _, item := range list {
		switch v := item.(type) {
		case string:
			if t := strings.TrimSpace(v); t != "" {
				blocks = append(blocks, sanitizeString(t))
			}
		case map[string]any:
			subj := strings.TrimSpace(strField(v, "subject"))
			desc := strings.TrimSpace(strField(v, "description"))
			switch {
			case subj != "" && desc != "":
				blocks = append(blocks, fmt.Sprintf("**%s**\n%s", subj, desc))
			case subj != "":
				blocks = append(blocks, fmt.Sprintf("**%s**", subj))
			case desc != "":
				blocks = append(blocks, desc)
			}
		}
	}
	return strings.Join(blocks, "\n\n")
}

func geminiToolKind(name string) string {
	norm := strings.ToLower(strings.TrimSpace(name))
	if k := geminiToolKinds[norm]; k != "" {
		return k
	}
	if strings.HasPrefix(norm, "mcp_") || strings.Contains(norm, "__") {
		return "mcp"
	}
	if norm == "" {
		return "tool"
	}
	return truncateRunes(norm, 64)
}

func geminiCommandText(name string, args any) string {
	m, ok := args.(map[string]any)
	if !ok {
		return ""
	}
	if name == "run_shell_command" || name == "shell" {
		return strings.TrimSpace(strField(m, "command", "cmd", "commandLine"))
	}
	return ""
}

func geminiIsMutating(name, cmd string) bool {
	if name == "write_file" || name == "replace" {
		return true
	}
	if (name == "run_shell_command" || name == "shell") && cmd != "" {
		return geminiMutatingShellRe.MatchString(cmd)
	}
	return false
}

func geminiArgsJSON(args any) json.RawMessage {
	switch v := args.(type) {
	case nil:
		return nil
	case map[string]any:
		if len(v) == 0 {
			return nil
		}
		return marshalSafeJSON(v)
	case []any:
		if len(v) == 0 {
			return nil
		}
		return marshalSafeJSON(map[string]any{"items": v})
	case string:
		if v == "" {
			return nil
		}
		return marshalSafeJSON(map[string]any{"raw": v})
	default:
		return marshalSafeJSON(map[string]any{"value": v})
	}
}

func geminiToolOutput(call map[string]any) (string, *int) {
	var exitCode *int
	var chunks []string

	switch res := call["result"].(type) {
	case string:
		if strings.TrimSpace(res) != "" {
			chunks = append(chunks, sanitizeString(res))
		}
	case map[string]any:
		if _, ok := res["exitCode"]; ok {
			c := int(asInt64(res["exitCode"]))
			exitCode = &c
		}
		data, _ := json.Marshal(sanitizeValue(res))
		chunks = append(chunks, string(data))
	case []any:
		for _, item := range res {
			im, ok := item.(map[string]any)
			if !ok {
				continue
			}
			if fnResp, ok := im["functionResponse"].(map[string]any); ok {
				switch resp := fnResp["response"].(type) {
				case map[string]any:
					if _, ok := resp["exitCode"]; ok && exitCode == nil {
						c := int(asInt64(resp["exitCode"]))
						exitCode = &c
					}
					for _, k := range []string{"output", "error", "result", "content"} {
						if val, ok := resp[k]; ok && val != nil {
							if s, ok := val.(string); ok && strings.TrimSpace(s) != "" {
								chunks = append(chunks, sanitizeString(s))
							} else if k != "output" {
								b, _ := json.Marshal(sanitizeValue(val))
								chunks = append(chunks, string(b))
							}
						}
					}
					if len(chunks) == 0 && len(resp) > 0 {
						b, _ := json.Marshal(sanitizeValue(resp))
						chunks = append(chunks, string(b))
					}
				case string:
					if strings.TrimSpace(resp) != "" {
						chunks = append(chunks, sanitizeString(resp))
					}
				}
			} else if inline, ok := im["inlineData"].(map[string]any); ok {
				mime := strField(inline, "mimeType")
				if mime == "" {
					mime = "binary"
				}
				chunks = append(chunks, fmt.Sprintf("[inlineData: %s]", mime))
			}
		}
	}

	if len(chunks) == 0 && call["resultDisplay"] != nil {
		switch d := call["resultDisplay"].(type) {
		case string:
			if strings.TrimSpace(d) != "" {
				chunks = append(chunks, sanitizeString(d))
			}
		case map[string]any:
			if fd := strField(d, "fileDiff"); strings.TrimSpace(fd) != "" {
				chunks = append(chunks, fd)
			} else {
				b, _ := json.Marshal(sanitizeValue(d))
				chunks = append(chunks, string(b))
			}
		default:
			chunks = append(chunks, fmt.Sprint(d))
		}
	}

	out := strings.Join(chunks, "\n\n")
	if exitCode == nil && out != "" {
		if m := geminiExitCodeRe.FindStringSubmatch(out); len(m) >= 3 {
			rawCode := m[1]
			if rawCode == "" {
				rawCode = m[2]
			}
			if rawCode != "" {
				if n, err := strconv.Atoi(rawCode); err == nil {
					exitCode = &n
				}
			}
		}
	}
	return out, exitCode
}

func geminiToolStatus(raw any, exitCode *int, ongoing bool) string {
	status := strings.ToLower(strings.TrimSpace(fmt.Sprint(raw)))
	if raw == nil {
		status = ""
	}
	switch status {
	case "success":
		if exitCode != nil && *exitCode != 0 {
			return "failed"
		}
		return "completed"
	case "error":
		return "failed"
	case "cancelled":
		return "cancelled"
	case "validating", "scheduled", "executing", "awaiting_approval":
		if ongoing {
			return "running"
		}
		return "interrupted"
	}
	if exitCode != nil && *exitCode != 0 {
		return "failed"
	}
	if status == "done" || status == "ok" {
		return "completed"
	}
	if status == "" {
		return "pending"
	}
	return truncateRunes(status, 32)
}

type statPair struct {
	valid   bool
	mtimeNs int64
	size    int64
}

type geminiProjectIndex struct {
	projectsFile string
	projectsSig  statPair
	bySlug       map[string][]string
	byHash       map[string]string
	rootCache    map[string]struct {
		sig   statPair
		value string
	}
}

func newGeminiProjectIndex(home string) *geminiProjectIndex {
	return &geminiProjectIndex{
		projectsFile: filepath.Join(home, "projects.json"),
		bySlug:       make(map[string][]string),
		byHash:       make(map[string]string),
		rootCache: make(map[string]struct {
			sig   statPair
			value string
		}),
	}
}

func readStatPair(path string) statPair {
	info, err := os.Stat(path)
	if err != nil {
		return statPair{}
	}
	return statPair{valid: true, mtimeNs: info.ModTime().UnixNano(), size: info.Size()}
}

func (idx *geminiProjectIndex) loadProjects() {
	sig := readStatPair(idx.projectsFile)
	if sig == idx.projectsSig {
		return
	}
	bySlug := make(map[string][]string)
	byHash := make(map[string]string)
	if sig.valid {
		if raw, err := os.ReadFile(idx.projectsFile); err == nil {
			var parsed struct {
				Projects map[string]string `json:"projects"`
			}
			if json.Unmarshal(raw, &parsed) == nil {
				for rawPath, slug := range parsed.Projects {
					cp := strings.TrimSpace(rawPath)
					cs := strings.TrimSpace(slug)
					if cp == "" || cs == "" {
						continue
					}
					bySlug[cs] = append(bySlug[cs], cp)
					sum := sha256.Sum256([]byte(cp))
					byHash[hex.EncodeToString(sum[:])] = cp
				}
			}
		}
	}
	idx.projectsSig = sig
	idx.bySlug = bySlug
	idx.byHash = byHash
}

func (idx *geminiProjectIndex) readProjectRoot(slugDir string) string {
	rootFile := filepath.Join(slugDir, ".project_root")
	sig := readStatPair(rootFile)
	if cached, ok := idx.rootCache[slugDir]; ok && cached.sig == sig {
		return cached.value
	}
	val := ""
	if sig.valid {
		if raw, err := os.ReadFile(rootFile); err == nil {
			lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
			if len(lines) > 0 {
				val = strings.TrimSpace(lines[0])
			}
		}
	}
	idx.rootCache[slugDir] = struct {
		sig   statPair
		value string
	}{sig: sig, value: val}
	return val
}

func (idx *geminiProjectIndex) digest(slugDir string) string {
	pSig := readStatPair(idx.projectsFile)
	rSig := readStatPair(filepath.Join(slugDir, ".project_root"))
	raw := fmt.Sprintf("%v:%d:%d|%v:%d:%d", pSig.valid, pSig.mtimeNs, pSig.size, rSig.valid, rSig.mtimeNs, rSig.size)
	return sha256Hex([]byte(raw))[:16]
}

func (idx *geminiProjectIndex) resolveWorkspace(slugDir, projectHash string, directories []string) string {
	dotRoot := idx.readProjectRoot(slugDir)
	idx.loadProjects()
	if projectHash != "" {
		if match, ok := idx.byHash[projectHash]; ok {
			if dotRoot != "" {
				sum := sha256.Sum256([]byte(dotRoot))
				if hex.EncodeToString(sum[:]) == projectHash {
					return dotRoot
				}
			}
			return match
		}
	}
	if dotRoot != "" {
		return dotRoot
	}
	if cands := idx.bySlug[filepath.Base(slugDir)]; len(cands) > 0 {
		return cands[0]
	}
	for _, d := range directories {
		if strings.TrimSpace(d) != "" {
			return strings.TrimSpace(d)
		}
	}
	return ""
}

func stringOrEmpty(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
