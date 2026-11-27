package adapters

import (
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

const agySummariesDBName = "conversation_summaries.db"

// AgySummaryReader reads conversation metadata from conversation_summaries.db without creating -shm.
type AgySummaryReader func(dbPath string, hasWAL bool) (map[string]model.AgyConversationMeta, error)

var (
	agyTranscriptNames = []string{"transcript_full.jsonl", "transcript.jsonl"}
	agyNormalized      = map[string]string{
		"USER_INPUT":       "user_message",
		"PLANNER_RESPONSE": "assistant_response",
		"GENERIC":          "tool_result",
		"SYSTEM_MESSAGE":   "system_message",
	}
	agyToolKinds = map[string]string{
		"run_command":     "exec",
		"read_file":       "read",
		"write_file":      "write",
		"edit_file":       "patch",
		"search_web":      "web_search",
		"update_steps":    "plan",
		"manage_task":     "task",
		"invoke_subagent": "agent",
		"define_subagent": "agent",
	}
	agyMutatingTools = map[string]bool{"write_file": true, "edit_file": true}
	agyUserReqRe     = regexp.MustCompile(`(?s)<USER_REQUEST>\s*(.*?)\s*</USER_REQUEST>`)
	agyExitCodeRe    = regexp.MustCompile(`Command exited with code (-?\d+)`)
	agyCompletedAtRe = regexp.MustCompile(`(?m)^Completed At: (\S+)$`)
	agyDetectTypeRe  = regexp.MustCompile(`"type"\s*:\s*"([A-Z_]+)"`)
	agyDetectStatRe  = regexp.MustCompile(`"status"\s*:\s*"([A-Z_]+)"`)
	agyDetectStepRe  = regexp.MustCompile(`"step_index"\s*:\s*(\d+)`)
)

// AgyAdapter indexes Antigravity CLI conversations under ~/.gemini/antigravity-cli/brain.
type AgyAdapter struct {
	home          string
	parserVersion string
	mu            sync.Mutex
	reader        AgySummaryReader
	hasSig        bool
	lastSig       discovery.SQLiteSignature
	lastRacy      bool
	loadedAt      time.Time
	rows          map[string]model.AgyConversationMeta
}

// NewAgyAdapter creates an agy adapter. Accepts either ~/.gemini/antigravity-cli or its brain subdir.
func NewAgyAdapter(root string, reader AgySummaryReader) *AgyAdapter {
	expanded := filepath.Clean(discovery.ExpandPath(root))
	if filepath.Base(expanded) == "brain" {
		expanded = filepath.Dir(expanded)
	}
	return &AgyAdapter{
		home:          expanded,
		parserVersion: "1",
		reader:        reader,
		rows:          make(map[string]model.AgyConversationMeta),
	}
}

func (a *AgyAdapter) Kind() string          { return "agy_jsonl" }
func (a *AgyAdapter) Agent() string         { return "agy" }
func (a *AgyAdapter) ParserVersion() string { return a.parserVersion }

func (a *AgyAdapter) brainRoot() string {
	return filepath.Join(a.home, "brain")
}

func (a *AgyAdapter) Identity(convDir string) string {
	return "agy:" + sha256Hex([]byte(discovery.CachedResolve(convDir)))
}

func (a *AgyAdapter) WatchPaths(src SourceDescriptor) []string {
	store := filepath.Join(a.home, agySummariesDBName)
	return []string{src.Path, store, store + "-wal"}
}

func (a *AgyAdapter) Discover(_ bool) ([]SourceDescriptor, error) {
	brain := a.brainRoot()
	entries, err := os.ReadDir(brain)
	if err != nil {
		return nil, nil
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })

	var out []SourceDescriptor
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".") || entry.Type()&os.ModeSymlink != 0 || !entry.IsDir() {
			continue
		}
		convDir := filepath.Join(brain, entry.Name())
		logsDir := filepath.Join(convDir, ".system_generated", "logs")
		var transcript string
		for _, cand := range agyTranscriptNames {
			p := filepath.Join(logsDir, cand)
			if info, err := os.Stat(p); err == nil && !info.IsDir() {
				transcript = p
				break
			}
		}
		if transcript == "" {
			continue
		}
		out = append(out, SourceDescriptor{
			Kind:     a.Kind(),
			Identity: a.Identity(convDir),
			Path:     transcript,
			Format:   "jsonl",
			Agent:    a.Agent(),
			ThreadID: entry.Name(),
		})
	}
	return out, nil
}

func (a *AgyAdapter) metaFor(conversationID string) model.AgyConversationMeta {
	if a.reader == nil || conversationID == "" {
		return model.AgyConversationMeta{}
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	dbPath := filepath.Join(a.home, agySummariesDBName)
	sig, err := discovery.ReadSQLiteSignature(dbPath)
	if err != nil {
		a.hasSig = false
		a.rows = make(map[string]model.AgyConversationMeta)
		return model.AgyConversationMeta{}
	}

	now := time.Now()
	age := now.Sub(a.loadedAt)
	if a.hasSig && sig.Equal(a.lastSig) && age < time.Hour && (!a.lastRacy || age < time.Second) {
		return a.rows[conversationID]
	}

	rows, readErr := a.reader(dbPath, sig.HasWAL)
	after, afterErr := discovery.ReadSQLiteSignature(dbPath)
	if readErr == nil && afterErr == nil && sig.Equal(after) && rows != nil {
		a.rows = rows
		a.lastSig = sig
		a.hasSig = true
		a.lastRacy = sig.IsRacy(now, 2*time.Second)
		a.loadedAt = now
	}
	return a.rows[conversationID]
}

func agyMetaDigest(meta model.AgyConversationMeta) string {
	raw, _ := json.Marshal([]string{meta.Title, meta.Workspace, meta.ParentID})
	return sha256Hex(raw)[:16]
}

func (a *AgyAdapter) Plan(src SourceDescriptor, cp *model.IndexCheckpoint) (SourcePlan, error) {
	meta := a.metaFor(src.ThreadID)
	return planAppendJSONL(src, cp, a.parserVersion, appendPlanOpts{
		extraFP:           map[string]any{"meta": agyMetaDigest(meta)},
		checkPath:         true,
		checkPrefix:       true,
		rebuildOnSameSize: false,
	})
}

func (a *AgyAdapter) Parse(plan SourcePlan, existing *model.AgentSession) (*ParsedSource, error) {
	src := plan.Source
	realPath := discovery.CachedResolve(src.Path)
	convID := src.ThreadID
	if convID == "" {
		convID = filepath.Base(filepath.Dir(filepath.Dir(filepath.Dir(src.Path))))
	}
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
		RolloutRealPath:     strPtr("agy:" + realPath),
		SourcePath:          strPtr(realPath),
		RolloutFormat:       "jsonl",
		SourceFormat:        "jsonl",
		ThreadID:            convID,
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
		sess.ThreadID = convID
		sess.RolloutPath = strPtr(src.Path)
		sess.RolloutRealPath = strPtr("agy:" + realPath)
		sess.SourcePath = strPtr(realPath)
		sess.UpdatedAt = nowStr
		startOffset = existing.LastIndexedOffset
		startLine = existing.LastIndexedLine
		eventIndex = int(existing.LastIndexedLine)
	}

	lines, err := readJSONLLines(src.Path, startOffset, MaxLineBytes, PrefixBytes, true)
	if err != nil {
		return nil, err
	}

	newEvents := make([]model.AgentEvent, 0, len(lines))
	curLine := startLine
	for _, ln := range lines {
		curLine++
		if ln.Oversized {
			detected := agyDetectPrefix(ln.Prefix)
			stepType := strField(detected, "type")
			if stepType == "" {
				stepType = "unknown"
			}
			newEvents = append(newEvents, model.AgentEvent{
				AgentSessionID: sess.ID,
				EventIndex:     eventIndex,
				LineNumber:     curLine,
				ByteOffset:     ln.ByteOffset,
				TopLevelType:   strPtr(truncateRunes(stepType, 64)),
				PayloadType:    strPtr("line_too_large"),
				NormalizedType: "parse_error",
				RawJSON:        unparsedEventJSON("line_too_large", ln.Prefix, ln.ByteLength, map[string]any{"detected": detected}),
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
			detected := agyDetectPrefix(ln.Raw)
			stepType := strField(detected, "type")
			if stepType == "" {
				stepType = "unknown"
			}
			newEvents = append(newEvents, model.AgentEvent{
				AgentSessionID: sess.ID,
				EventIndex:     eventIndex,
				LineNumber:     curLine,
				ByteOffset:     ln.ByteOffset,
				TopLevelType:   strPtr(truncateRunes(stepType, 64)),
				PayloadType:    strPtr("invalid_json"),
				NormalizedType: "parse_error",
				RawJSON:        unparsedEventJSON("invalid_json", ln.Raw, int64(len(ln.Raw)), map[string]any{"detected": detected}),
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
		stepType := strField(safe, "type")
		if stepType == "" {
			stepType = "unknown"
		}
		norm := agyNormalized[stepType]
		isKnown := norm != ""
		if norm == "" {
			norm = "unknown"
		}
		payType := strField(safe, "source")
		if payType == "" {
			payType = stepType
		}
		newEvents = append(newEvents, model.AgentEvent{
			AgentSessionID: sess.ID,
			EventIndex:     eventIndex,
			LineNumber:     curLine,
			ByteOffset:     ln.ByteOffset,
			EventTimestamp: parseTimestamp(safe["created_at"]),
			TopLevelType:   strPtr(truncateRunes(stepType, 64)),
			PayloadType:    strPtr(truncateRunes(payType, 128)),
			NormalizedType: norm,
			RawJSON:        marshalSafeJSON(safe),
			RawHash:        sha256Hex(ln.Raw),
			IsKnown:        isKnown,
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

func (a *AgyAdapter) Project(sess model.AgentSession, allEvents []model.AgentEvent) (model.AgentSession, model.SessionProjection, error) {
	nowStr := model.FormatUTC(time.Now())
	var (
		turns       []model.AgentTurn
		messages    []model.Message
		tools       []*model.ToolCall
		pending     []*model.ToolCall
		currentTurn *model.AgentTurn
		sequence    int
		lastTS      *string
		firstUser   string
		maxIdx      = -1
		inSum       int64
		cachedSum   int64
		outSum      int64
		totalSum    int64
	)

	cancelPending := func() {
		for _, tc := range pending {
			tc.Status = "cancelled"
		}
		pending = nil
	}
	closeTurn := func() {
		if currentTurn != nil && currentTurn.Status == "running" {
			currentTurn.Status = "interrupted"
			currentTurn.Outcome = "interrupted"
			currentTurn.CompletedAt = lastTS
			currentTurn.DurationMS = elapsedMS(currentTurn.StartedAt, lastTS)
		}
	}
	ensureTurn := func(ts *string) *model.AgentTurn {
		if currentTurn != nil {
			return currentTurn
		}
		tr := model.AgentTurn{
			ID:               stableID("turn", fmt.Sprintf("%s:%d", sess.ID, len(turns))),
			Agent:            sess.Agent,
			AgentSessionID:   sess.ID,
			TurnIndex:        len(turns),
			TurnID:           strPtr(truncateRunes(fmt.Sprintf("%s:implicit:%d", sess.ThreadID, len(turns)), 255)),
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
		return currentTurn
	}
	addMessage := func(ev model.AgentEvent, role, channel, text string, part int, visible, system bool) {
		evID := ev.ID
		messages = append(messages, model.Message{
			ID:                stableID("msg", fmt.Sprintf("%s:%d:%d", sess.ID, ev.EventIndex, part)),
			AgentSessionID:    sess.ID,
			TurnID:            turnPtrID(currentTurn),
			EventID:           &evID,
			Role:              role,
			Channel:           channel,
			Sequence:          sequence,
			Text:              text,
			MarkdownText:      strPtr(text),
			CopyText:          text,
			Timestamp:         ev.EventTimestamp,
			IsVisible:         visible,
			IsSystemGenerated: system,
			CreatedAt:         nowStr,
		})
		sequence++
	}

	for _, ev := range allEvents {
		if ev.EventIndex > maxIdx {
			maxIdx = ev.EventIndex
		}
		raw := decodeObjectMap(ev.RawJSON)
		stepType := ""
		if ev.TopLevelType != nil {
			stepType = *ev.TopLevelType
		}
		ts := ev.EventTimestamp
		if ts != nil {
			if sess.StartTime == nil {
				sess.StartTime = ts
			}
			if lastTS == nil || *ts >= *lastTS {
				lastTS = ts
			}
		}
		content := strField(raw, "content")

		switch stepType {
		case "USER_INPUT":
			if !ev.IsKnown {
				continue
			}
			cancelPending()
			closeTurn()
			stepIdx := fmt.Sprint(firstNonNil(raw["step_index"], ev.EventIndex))
			tr := model.AgentTurn{
				ID:               stableID("turn", fmt.Sprintf("%s:%d", sess.ID, len(turns))),
				Agent:            sess.Agent,
				AgentSessionID:   sess.ID,
				TurnIndex:        len(turns),
				TurnID:           strPtr(truncateRunes(fmt.Sprintf("%s:%s", sess.ThreadID, stepIdx), 255)),
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
			text := agyUnwrapUserRequest(content)
			if text != "" {
				if firstUser == "" {
					firstUser = text
				}
				addMessage(ev, "user", "final", text, 0, true, false)
			}

		case "PLANNER_RESPONSE":
			if !ev.IsKnown {
				continue
			}
			cancelPending()
			tr := ensureTurn(ts)
			if m := strField(raw, "model"); m != "" {
				sess.Model = strPtr(truncateRunes(m, 255))
			}
			inTok := asNonNegInt64(raw["input_tokens"])
			cachedTok := asNonNegInt64(raw["cache_read_tokens"])
			outTok := asNonNegInt64(raw["output_tokens"])
			stepTotal := inTok + cachedTok + outTok
			tr.InputTokens += inTok
			tr.CachedInputTokens += cachedTok
			tr.OutputTokens += outTok
			tr.TotalTokens += stepTotal
			inSum += inTok
			cachedSum += cachedTok
			outSum += outTok
			totalSum += stepTotal

			if thinking := strField(raw, "thinking"); strings.TrimSpace(thinking) != "" {
				addMessage(ev, "assistant", "reasoning", thinking, 0, false, false)
			}
			calls, _ := raw["tool_calls"].([]any)
			if strings.TrimSpace(content) != "" {
				ch := "final"
				if len(calls) > 0 {
					ch = "commentary"
				}
				addMessage(ev, "assistant", ch, content, 1, true, false)
			}

			stepIdx := fmt.Sprint(firstNonNil(raw["step_index"], ev.EventIndex))
			for pos, rawCall := range calls {
				callMap, ok := rawCall.(map[string]any)
				if !ok {
					callMap = map[string]any{"value": rawCall}
				}
				name := strField(callMap, "name")
				if name == "" {
					name = "unknown"
				}
				var argsMap map[string]any
				if am, ok := callMap["args"].(map[string]any); ok {
					argsMap = am
				} else if callMap["args"] != nil {
					argsMap = map[string]any{"value": callMap["args"]}
				}
				kind := agyToolKinds[name]
				if kind == "" {
					kind = "tool"
				}
				var argsRaw json.RawMessage
				if argsMap != nil {
					argsRaw = marshalSafeJSON(argsMap)
				}
				evID := ev.ID
				tid := tr.ID
				tc := &model.ToolCall{
					ID:             stableID("tool", fmt.Sprintf("%s:%d:%d", sess.ID, ev.EventIndex, pos)),
					Agent:          sess.Agent,
					AgentSessionID: sess.ID,
					TurnID:         &tid,
					EventID:        &evID,
					CallID:         fmt.Sprintf("%s:%d", stepIdx, pos),
					ToolName:       truncateRunes(name, 255),
					ToolKind:       kind,
					ArgumentsJSON:  argsRaw,
					CommandText:    strPtr(agyCommandText(name, argsMap)),
					Status:         "pending",
					IsMutating:     agyMutatingTools[name],
					StartedAt:      ts,
					CreatedAt:      nowStr,
				}
				tools = append(tools, tc)
				pending = append(pending, tc)
			}
			if len(calls) > 0 {
				tr.Status = "running"
				tr.Outcome = "running"
				tr.CompletedAt = nil
				tr.DurationMS = nil
			} else {
				tr.Status = "completed"
				tr.Outcome = "completed"
				tr.CompletedAt = ts
				tr.DurationMS = elapsedMS(tr.StartedAt, ts)
			}

		case "GENERIC":
			var tc *model.ToolCall
			if len(pending) > 0 {
				tc = pending[0]
				pending = pending[1:]
			} else {
				evID := ev.ID
				stepIdx := fmt.Sprint(firstNonNil(raw["step_index"], ev.EventIndex))
				tc = &model.ToolCall{
					ID:             stableID("tool", fmt.Sprintf("%s:%d:result", sess.ID, ev.EventIndex)),
					Agent:          sess.Agent,
					AgentSessionID: sess.ID,
					TurnID:         turnPtrID(currentTurn),
					EventID:        &evID,
					CallID:         stepIdx + ":result",
					ToolName:       "unknown",
					ToolKind:       "tool",
					Status:         "pending",
					StartedAt:      ts,
					CreatedAt:      nowStr,
				}
				tools = append(tools, tc)
			}
			detected, _ := raw["detected"].(map[string]any)
			status := strField(raw, "status")
			if status == "" && detected != nil {
				status = strField(detected, "status")
			}
			if ev.NormalizedType != "parse_error" {
				tc.OutputText = strPtr(content)
			} else {
				tc.OutputText = strPtr(fmt.Sprintf("[agy transcript line could not be parsed: %s, %d bytes; see the raw event]", strField(raw, "parse_error"), asInt64(raw["byte_length"])))
			}
			if m := agyExitCodeRe.FindStringSubmatch(content); len(m) >= 2 {
				if n, err := strconv.Atoi(m[1]); err == nil {
					tc.ExitCode = &n
				}
			}
			errStr := strField(raw, "error")
			if errStr != "" && content == "" {
				tc.OutputText = strPtr(errStr)
			}
			if status == "RUNNING" {
				tc.Status = "running"
				tc.CompletedAt = nil
				continue
			}
			compMatches := agyCompletedAtRe.FindAllStringSubmatch(content, -1)
			var compTS *string
			if len(compMatches) > 0 {
				compTS = parseTimestamp(compMatches[len(compMatches)-1][1])
			}
			if compTS == nil {
				compTS = ts
			}
			tc.CompletedAt = compTS
			failed := status == "ERROR" || errStr != "" || (tc.ExitCode != nil && *tc.ExitCode != 0)
			if failed {
				tc.Status = "failed"
			} else if status == "DONE" || status == "" {
				tc.Status = "ok"
			} else {
				tc.Status = truncateRunes(strings.ToLower(status), 32)
			}

		case "SYSTEM_MESSAGE":
			if ev.IsKnown && strings.TrimSpace(content) != "" {
				addMessage(ev, "system", "commentary", content, 0, true, true)
			}
		}
	}

	meta := a.metaFor(sess.ThreadID)
	if meta.Title != "" {
		sess.DisplayTitle = strPtr(truncateRunes(meta.Title, 255))
		sess.AITitle = strPtr(truncateRunes(meta.Title, 255))
	} else if firstUser != "" && (sess.DisplayTitle == nil || *sess.DisplayTitle == "" || *sess.DisplayTitle == sess.ThreadID) {
		sess.DisplayTitle = strPtr(truncateRunes(collapseSpaces(firstUser), 255))
	}
	if sess.DisplayTitle == nil || *sess.DisplayTitle == "" {
		sess.DisplayTitle = strPtr(truncateRunes(sess.ThreadID, 255))
	}
	if meta.Workspace != "" {
		sess.CWD = strPtr(meta.Workspace)
	}
	if meta.ParentID != "" && meta.ParentID != sess.ThreadID {
		sess.ParentSessionID = strPtr(meta.ParentID)
		sess.IsSubagent = true
	}

	turnByID := make(map[string]*model.AgentTurn, len(turns))
	var spans []model.ActivitySpan
	for i := range turns {
		turnByID[turns[i].ID] = &turns[i]
		if sp := buildTurnSpan(sess.Agent, sess.SourceKind, sess, turns[i]); sp != nil {
			spans = append(spans, *sp)
		}
	}
	finalTools := make([]model.ToolCall, 0, len(tools))
	for _, tc := range tools {
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
	sess.OutputTokens = outSum
	sess.TotalTokens = totalSum
	if lastTS != nil {
		sess.LastActivityAt = lastTS
		sess.EndTime = lastTS
	} else {
		sess.LastActivityAt = sess.StartTime
	}
	var lastTurn *model.AgentTurn
	if len(turns) > 0 {
		lastTurn = &turns[len(turns)-1]
	}
	sess.IsOngoing = lastTurn != nil && lastTurn.Status == "running" && lastTS != nil && isRecentFileMtime(sess.FileMtimeNs, OngoingSeconds*time.Second)
	sess.ProjectedEventIndex = maxIdx
	sess.UpdatedAt = nowStr

	return sess, model.SessionProjection{
		Turns:     turns,
		Messages:  messages,
		ToolCalls: finalTools,
		Spans:     spans,
	}, nil
}

func agyUnwrapUserRequest(content string) string {
	if m := agyUserReqRe.FindStringSubmatch(content); len(m) >= 2 {
		return m[1]
	}
	return strings.TrimSpace(content)
}

func agyCommandText(name string, args map[string]any) string {
	if args == nil {
		return ""
	}
	if name == "run_command" {
		if cmd := strField(args, "CommandLine"); cmd != "" {
			return cmd
		}
	}
	b, _ := json.Marshal(sanitizeValue(args))
	return string(b)
}

func agyDetectPrefix(prefix []byte) map[string]any {
	found := make(map[string]any)
	if m := agyDetectTypeRe.FindSubmatch(prefix); len(m) >= 2 {
		found["type"] = string(m[1])
	}
	if m := agyDetectStatRe.FindSubmatch(prefix); len(m) >= 2 {
		found["status"] = string(m[1])
	}
	if m := agyDetectStepRe.FindSubmatch(prefix); len(m) >= 2 {
		if n, err := strconv.Atoi(string(m[1])); err == nil {
			found["step_index"] = n
		}
	}
	return found
}
