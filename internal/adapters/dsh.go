package adapters

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/agent-observatory/agent-observatory-go/internal/discovery"
	"github.com/agent-observatory/agent-observatory-go/internal/model"
	"github.com/klauspost/compress/zstd"
)

const dshMaxLineBytes = 8 * 1024 * 1024

var dshKnownTypes = map[string]bool{
	"session":           true,
	"turn/start":        true,
	"turn/end":          true,
	"user/message":      true,
	"assistant/message": true,
	"tool/call":         true,
	"tool/result":       true,
	"session/title":     true,
}

// DshAdapter indexes zstd-compressed DSH session transcripts under ~/.dsh/sessions.
type DshAdapter struct {
	root          string
	parserVersion string
	tree          *discovery.TreeCache
}

// NewDshAdapter creates a read-only DSH adapter rooted at sessionsDir.
func NewDshAdapter(sessionsDir string) *DshAdapter {
	expanded := discovery.ExpandPath(sessionsDir)
	return &DshAdapter{
		root:          expanded,
		parserVersion: "1",
		tree:          discovery.NewTreeCache(expanded, "session.jsonl.zstd", nil),
	}
}

func (a *DshAdapter) Kind() string          { return "dsh_jsonl" }
func (a *DshAdapter) Agent() string         { return "dsh" }
func (a *DshAdapter) ParserVersion() string { return a.parserVersion }

func (a *DshAdapter) WatchPaths(src SourceDescriptor) []string {
	return []string{src.Path}
}

func (a *DshAdapter) Identity(path string) string {
	return "dsh:" + sha256Hex([]byte(discovery.CachedResolve(path)))
}

func (a *DshAdapter) Discover(full bool) ([]SourceDescriptor, error) {
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
			Kind:        a.Kind(),
			Identity:    a.Identity(p),
			Path:        p,
			Format:      "jsonl",
			Agent:       a.Agent(),
			Compression: "zstd",
			ThreadID:    fileStem(p),
		})
	}
	return out, nil
}

func (a *DshAdapter) Plan(src SourceDescriptor, cp *model.IndexCheckpoint) (SourcePlan, error) {
	// Zstandard streams cannot be reliably seek-resumed by byte offset, so changed files rebuild cleanly.
	return planStatRebuild(src, cp, a.parserVersion, map[string]any{"compression": "zstd"})
}

func (a *DshAdapter) Parse(plan SourcePlan, _ *model.AgentSession) (*ParsedSource, error) {
	src := plan.Source
	realPath := discovery.CachedResolve(src.Path)
	nowStr := model.FormatUTC(time.Now())
	info, err := os.Stat(src.Path)
	if err != nil {
		return nil, err
	}

	rawLines, err := readDshZstdLines(src.Path)
	if err != nil {
		return nil, err
	}

	sess := model.AgentSession{
		ID:                  stableID("session", src.Identity),
		Agent:               a.Agent(),
		SourceKind:          a.Kind(),
		SourceIdentity:      src.Identity,
		RolloutPath:         strPtr(src.Path),
		RolloutRealPath:     strPtr("dsh:" + realPath),
		SourcePath:          strPtr(realPath),
		RolloutFormat:       "jsonl",
		SourceFormat:        "jsonl",
		RolloutCompression:  strPtr("zstd"),
		SourceCompression:   strPtr("zstd"),
		ThreadID:            src.Identity,
		ParserVersion:       a.parserVersion,
		ProjectedEventIndex: -1,
		IndexStatus:         "indexed",
		FileSize:            info.Size(),
		FileMtimeNs:         info.ModTime().UnixNano(),
		CreatedAt:           nowStr,
		UpdatedAt:           nowStr,
	}

	events := make([]model.AgentEvent, 0, len(rawLines))
	for lineNo, raw := range rawLines {
		if int64(len(raw)) > dshMaxLineBytes {
			events = append(events, model.AgentEvent{
				AgentSessionID: sess.ID,
				EventIndex:     len(events),
				LineNumber:     int64(lineNo + 1),
				TopLevelType:   strPtr("parse_error"),
				PayloadType:    strPtr("parse_error"),
				NormalizedType: "parse_error",
				RawJSON:        unparsedEventJSON("line_too_large", raw, int64(len(raw)), nil),
				RawHash:        sha256Hex(raw),
				IsKnown:        false,
				ParserVersion:  a.parserVersion,
				CreatedAt:      nowStr,
			})
			continue
		}

		var decoded any
		if err := json.Unmarshal(sanitizeUTF8AndNUL(raw), &decoded); err != nil {
			events = append(events, model.AgentEvent{
				AgentSessionID: sess.ID,
				EventIndex:     len(events),
				LineNumber:     int64(lineNo + 1),
				TopLevelType:   strPtr("parse_error"),
				PayloadType:    strPtr("parse_error"),
				NormalizedType: "parse_error",
				RawJSON:        unparsedEventJSON("invalid_json", raw, int64(len(raw)), nil),
				RawHash:        sha256Hex(raw),
				IsKnown:        false,
				ParserVersion:  a.parserVersion,
				CreatedAt:      nowStr,
			})
			continue
		}

		obj, ok := decoded.(map[string]any)
		if !ok || obj == nil {
			obj = map[string]any{"type": "raw", "value": decoded}
		}
		safe := sanitizeMap(obj)
		typ := strField(safe, "type")
		if typ == "" {
			typ = "unknown"
		}
		data := safe
		if sub, ok := safe["data"].(map[string]any); ok {
			data = sub
		}

		switch typ {
		case "turn", "step":
			phase := strField(data, "phase", "status", "event")
			if phase == "" {
				phase = "start"
			}
			if phase == "end" || phase == "complete" || phase == "completed" || phase == "finish" {
				typ = "turn/end"
			} else {
				typ = "turn/start"
			}
		case "message":
			role := strField(data, "role")
			if role == "" {
				role = strField(safe, "role")
			}
			if role == "" {
				role = "assistant"
			}
			typ = role + "/message"
		case "call", "tool":
			typ = "tool/call"
		case "result", "tool_result":
			typ = "tool/result"
		}

		if lineNo == 0 && typ == "session" {
			if id := strField(safe, "id"); id != "" {
				sess.ThreadID = id
			}
			if cwd := strField(safe, "cwd"); cwd != "" {
				sess.CWD = strPtr(cwd)
			}
			if parentID := strField(safe, "parentSession"); parentID != "" {
				sess.ParentSessionID = strPtr(parentID)
				sess.IsSubagent = true
			}
			if ts := parseTimestamp(safe["createdAt"]); ts != nil {
				sess.StartTime = ts
			}
		}

		ts := parseTimestamp(safe["time"])
		if ts == nil {
			ts = parseTimestamp(safe["timestamp"])
		}

		events = append(events, model.AgentEvent{
			AgentSessionID: sess.ID,
			EventIndex:     len(events),
			LineNumber:     int64(lineNo + 1),
			EventTimestamp: ts,
			TopLevelType:   strPtr(typ),
			PayloadType:    strPtr(typ),
			NormalizedType: typ,
			RawJSON:        marshalSafeJSON(safe),
			RawHash:        sha256Hex(raw),
			IsKnown:        dshKnownTypes[typ],
			ParserVersion:  a.parserVersion,
			CreatedAt:      nowStr,
		})
	}

	sess.LastIndexedLine = int64(len(events))
	return &ParsedSource{
		Session:         sess,
		ResetEvents:     true,
		NewEvents:       events,
		CursorJSON:      encodeCursorJSON(0, sess.LastIndexedLine),
		FingerprintJSON: encodeFingerprintJSON(plan.Fingerprint),
	}, nil
}

func (a *DshAdapter) Project(sess model.AgentSession, allEvents []model.AgentEvent) (model.AgentSession, model.SessionProjection, error) {
	nowStr := model.FormatUTC(time.Now())
	var (
		turns         []model.AgentTurn
		turnByKey     = make(map[string]*model.AgentTurn)
		turnOrder     []string
		toolList      []*model.ToolCall
		toolsByCall   = make(map[string]*model.ToolCall)
		messages      []model.Message
		firstUser     string
		start         = sess.StartTime
		end           *string
		totalTokens   int64
		sequence      int
		maxEventIndex = -1
	)

	sess.InputTokens = 0
	sess.CachedInputTokens = 0
	sess.CacheCreationInputTokens = 0
	sess.OutputTokens = 0
	sess.ReasoningOutputTokens = 0

	for idx, ev := range allEvents {
		if ev.EventIndex > maxEventIndex {
			maxEventIndex = ev.EventIndex
		}
		typ := ""
		if ev.TopLevelType != nil {
			typ = *ev.TopLevelType
		}
		safe := decodeObjectMap(ev.RawJSON)
		data := safe
		if sub, ok := safe["data"].(map[string]any); ok {
			data = sub
		}
		ts := ev.EventTimestamp
		if start == nil && ts != nil {
			start = ts
		}
		if ts != nil {
			end = ts
		}

		switch typ {
		case "session/title":
			if title := strings.TrimSpace(strField(data, "title")); title != "" {
				trimmed := truncateRunes(title, 255)
				sess.ThreadName = strPtr(trimmed)
				sess.DisplayTitle = strPtr(trimmed)
			}
		case "request/context":
			if m := strField(data, "model"); m != "" && sess.Model == nil {
				sess.Model = strPtr(m)
			}
		case "user/message":
			text := dshExtractText(data["text"])
			if text == "" {
				text = dshExtractText(data["message"])
			}
			if text == "" {
				text = dshExtractText(data["content"])
			}
			srcKind := "user"
			if srcInfo, ok := data["source"].(map[string]any); ok {
				if k := strField(srcInfo, "kind"); k != "" {
					srcKind = k
				}
			}
			if srcKind == "user" && firstUser == "" && text != "" {
				firstUser = text
			}
			evID := ev.ID
			messages = append(messages, model.Message{
				ID:             stableID("msg", fmt.Sprintf("%s:%d", sess.ID, idx+1)),
				AgentSessionID: sess.ID,
				EventID:        &evID,
				Role:           "user",
				Channel:        "final",
				Text:           text,
				MarkdownText:   strPtr(text),
				CopyText:       text,
				Sequence:       sequence,
				Timestamp:      ts,
				IsVisible:      true,
				CreatedAt:      nowStr,
			})
			sequence++

		case "assistant/message":
			msgObj, _ := data["message"].(map[string]any)
			msgData := data
			if msgObj != nil {
				msgData = msgObj
			}
			for _, block := range dshAssistantBlocks(msgData["content"]) {
				blockType := "text"
				if m, ok := block.(map[string]any); ok {
					if bt := strField(m, "type"); bt != "" {
						blockType = bt
					}
				}
				if blockType != "text" && blockType != "reasoning" && blockType != "thinking" && blockType != "redacted_thinking" {
					continue
				}
				text := dshExtractText(block)
				if text == "" {
					continue
				}
				channel := "final"
				if blockType == "reasoning" || blockType == "thinking" || blockType == "redacted_thinking" {
					channel = "reasoning"
				}
				evID := ev.ID
				messages = append(messages, model.Message{
					ID:             stableID("msg", fmt.Sprintf("%s:%d:%d", sess.ID, idx+1, sequence)),
					AgentSessionID: sess.ID,
					EventID:        &evID,
					Role:           "assistant",
					Channel:        channel,
					Text:           text,
					MarkdownText:   strPtr(text),
					CopyText:       text,
					Sequence:       sequence,
					Timestamp:      ts,
					IsVisible:      channel != "reasoning",
					CreatedAt:      nowStr,
				})
				sequence++
			}
			if sess.Model == nil {
				if m := strField(msgData, "model"); m != "" {
					sess.Model = strPtr(m)
				}
			}

			usage := dshSelectUsage(data, msgObj)
			inTok := asInt64FromKeys(usage, "input_tokens", "inputTokens", "input")
			cachedTok := asInt64FromKeys(usage, "cached_input_tokens", "cacheReadTokens")
			cacheWriteTok := asInt64FromKeys(usage, "cache_creation_input_tokens", "cacheWriteTokens")
			outTok := asInt64FromKeys(usage, "output_tokens", "outputTokens", "output")
			reasonTok := asInt64FromKeys(usage, "reasoning_output_tokens", "reasoningTokens", "reasoning")
			tot := asInt64FromKeys(usage, "total_tokens", "totalTokens", "total")
			if tot == 0 {
				tot = inTok + cachedTok + cacheWriteTok + outTok
			}
			totalTokens += tot
			sess.InputTokens += inTok
			sess.CachedInputTokens += cachedTok
			sess.CacheCreationInputTokens += cacheWriteTok
			sess.OutputTokens += outTok
			sess.ReasoningOutputTokens += reasonTok

			turnKey := dshTurnKey(data, "")
			tr := turnByKey[turnKey]
			if tr == nil && len(turnOrder) > 0 {
				// Some DSH message envelopes omit `turn`; attribute tokens to the active turn.
				tr = turnByKey[turnOrder[len(turnOrder)-1]]
			}
			if tr != nil {
				tr.InputTokens += inTok
				tr.CachedInputTokens += cachedTok
				tr.CacheCreationInputTokens += cacheWriteTok
				tr.OutputTokens += outTok
				tr.ReasoningOutputTokens += reasonTok
				tr.TotalTokens += inTok + cachedTok + cacheWriteTok + outTok
			}

		case "turn/start":
			tid := dshTurnKey(data, fmt.Sprint(idx+1))
			turn := model.AgentTurn{
				ID:               stableID("turn", fmt.Sprintf("%s:%s", sess.ID, tid)),
				Agent:            sess.Agent,
				AgentSessionID:   sess.ID,
				TurnIndex:        len(turns),
				TurnID:           &tid,
				Status:           "running",
				Outcome:          "running",
				ValidationStatus: "unknown",
				MetricConfidence: "exact",
				StartedAt:        ts,
				CreatedAt:        nowStr,
				UpdatedAt:        nowStr,
			}
			turns = append(turns, turn)
			turnByKey[tid] = &turns[len(turns)-1]
			turnOrder = append(turnOrder, tid)

		case "turn/end":
			if tr := turnByKey[dshTurnKey(data, "")]; tr != nil {
				reason, _ := data["reason"].(map[string]any)
				outcome := strField(reason, "kind")
				if outcome == "" {
					outcome = strField(data, "status")
				}
				if outcome == "" {
					outcome = "completed"
				}
				tr.CompletedAt = ts
				tr.Status = outcome
				switch outcome {
				case "completed":
					tr.Outcome = "completed"
				case "aborted", "interrupted":
					tr.Outcome = "interrupted"
				case "error", "blocked":
					tr.Outcome = "failed"
				default:
					tr.Outcome = outcome
				}
				if errObj, ok := reason["error"].(map[string]any); ok {
					if code := strField(errObj, "code"); code != "" {
						tr.ErrorKind = strPtr(truncateRunes(code, 64))
					}
					if msg := strField(errObj, "message"); msg != "" {
						tr.ErrorMessage = strPtr(msg)
					}
				}
				tr.DurationMS = elapsedMS(tr.StartedAt, tr.CompletedAt)
			}

		case "tool/call":
			cid := strField(data, "call_id", "callId", "id")
			if cid == "" {
				cid = fmt.Sprint(idx + 1)
			}
			tid := dshTurnKey(data, "")
			var turnID *string
			if tr := turnByKey[tid]; tr != nil {
				id := tr.ID
				turnID = &id
			}
			name := strField(data, "name", "tool", "toolName")
			if name == "" {
				name = "unknown"
			}
			var argsRaw json.RawMessage
			switch arg := data["arguments"].(type) {
			case string:
				var parsed map[string]any
				if json.Unmarshal([]byte(arg), &parsed) == nil && parsed != nil {
					argsRaw = marshalSafeJSON(parsed)
				}
			case map[string]any:
				argsRaw = marshalSafeJSON(arg)
			}
			if argsRaw == nil {
				if inp, ok := data["input"].(map[string]any); ok {
					argsRaw = marshalSafeJSON(inp)
				}
			}
			evID := ev.ID
			tc := &model.ToolCall{
				ID:             stableID("tool", fmt.Sprintf("%s:%s", sess.ID, cid)),
				Agent:          sess.Agent,
				AgentSessionID: sess.ID,
				TurnID:         turnID,
				EventID:        &evID,
				CallID:         cid,
				ToolName:       truncateRunes(name, 255),
				ToolKind:       "tool",
				ArgumentsJSON:  argsRaw,
				CommandText:    strPtr(strField(data, "command")),
				StartedAt:      ts,
				Status:         "pending",
				CreatedAt:      nowStr,
			}
			toolList = append(toolList, tc)
			toolsByCall[cid] = tc

		case "tool/result":
			msgMap, _ := data["message"].(map[string]any)
			resultBlock := dshResultBlock(msgMap)
			cid := strField(data, "call_id", "callId", "id")
			if cid == "" && resultBlock != nil {
				cid = strField(resultBlock, "toolCallId", "tool_use_id")
			}
			tc := toolsByCall[cid]
			if tc != nil {
				out := dshExtractText(data["output"])
				if out == "" {
					out = dshExtractText(data["result"])
				}
				if out == "" && resultBlock != nil {
					out = dshExtractText(resultBlock["content"])
				}
				if out == "" && msgMap != nil {
					out = dshExtractText(msgMap)
				}
				tc.OutputText = strPtr(out)
				tc.CompletedAt = ts
				isErr := data["error"] != nil && data["error"] != false && data["error"] != ""
				if !isErr && resultBlock != nil {
					bErr, _ := resultBlock["isError"].(bool)
					isErr = bErr
				}
				if isErr {
					tc.Status = "failed"
				} else {
					tc.Status = "completed"
				}
			}
		}
	}

	var spans []model.ActivitySpan
	turnByID := make(map[string]*model.AgentTurn, len(turns))
	ongoing := false
	for i := range turns {
		if turns[i].CompletedAt == nil {
			ongoing = true
		} else if turns[i].DurationMS == nil {
			turns[i].DurationMS = elapsedMS(turns[i].StartedAt, turns[i].CompletedAt)
		}
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

	if sess.DisplayTitle == nil || *sess.DisplayTitle == "" {
		if firstUser != "" {
			sess.DisplayTitle = strPtr(truncateRunes(collapseSpaces(firstUser), 255))
		} else {
			sess.DisplayTitle = strPtr(truncateRunes(sess.ThreadID, 255))
		}
	}
	if sess.AITitle == nil {
		sess.AITitle = sess.DisplayTitle
	}
	if sess.StartTime == nil {
		sess.StartTime = start
	}
	sess.EndTime = end
	if end != nil {
		sess.LastActivityAt = end
	} else {
		sess.LastActivityAt = sess.StartTime
	}
	sess.TurnCount = len(turns)
	sess.TotalTokens = totalTokens
	sess.IsOngoing = ongoing
	sess.ProjectedEventIndex = maxEventIndex

	return sess, model.SessionProjection{
		Turns:     turns,
		Messages:  messages,
		ToolCalls: finalTools,
		Spans:     spans,
	}, nil
}

func readDshZstdLines(path string) ([][]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	dec, err := zstd.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("open dsh zstd reader %s: %w", path, err)
	}
	defer dec.Close()

	reader := bufio.NewReaderSize(dec, 64*1024)
	var lines [][]byte
	for {
		var buf bytes.Buffer
		oversized := false
		for {
			frag, isPrefix, readErr := reader.ReadLine()
			if len(frag) > 0 {
				if !oversized && buf.Len()+len(frag) <= dshMaxLineBytes {
					buf.Write(frag)
				} else {
					oversized = true
					if buf.Len() < PrefixBytes {
						need := PrefixBytes - buf.Len()
						if need > len(frag) {
							need = len(frag)
						}
						buf.Write(frag[:need])
					}
				}
			}
			if !isPrefix {
				if readErr == nil {
					if oversized {
						lines = append(lines, []byte(fmt.Sprintf(`{"type":"parse_error","error":"line_too_large","prefix":%q}`, string(buf.Bytes()))))
					} else {
						lines = append(lines, append([]byte(nil), buf.Bytes()...))
					}
				} else if readErr == io.EOF {
					if buf.Len() > 0 && len(bytes.TrimSpace(buf.Bytes())) > 0 && !oversized {
						lines = append(lines, append([]byte(nil), buf.Bytes()...))
					}
					return lines, nil
				} else {
					return nil, readErr
				}
				break
			}
			if readErr != nil && readErr != io.EOF {
				return nil, readErr
			}
		}
	}
}

func dshExtractText(v any) string {
	switch val := v.(type) {
	case nil:
		return ""
	case string:
		return sanitizeString(val)
	case []any:
		parts := make([]string, 0, len(val))
		for _, item := range val {
			if p := dshExtractText(item); p != "" {
				parts = append(parts, p)
			}
		}
		return strings.Join(parts, "\n")
	case map[string]any:
		for _, k := range []string{"text", "content", "output"} {
			if sub, ok := val[k]; ok {
				return dshExtractText(sub)
			}
		}
		raw, err := json.Marshal(sanitizeMap(val))
		if err != nil {
			return ""
		}
		return string(raw)
	default:
		return sanitizeString(fmt.Sprint(val))
	}
}

func dshTurnKey(data map[string]any, fallback string) string {
	if turn, ok := data["turn"]; ok && turn != nil {
		if m, ok := turn.(map[string]any); ok {
			if k := strField(m, "id", "turn_id"); k != "" {
				return k
			}
		} else {
			return sanitizeString(fmt.Sprint(turn))
		}
	}
	if k := strField(data, "turn_id", "id"); k != "" {
		return k
	}
	return fallback
}

func dshAssistantBlocks(content any) []any {
	if list, ok := content.([]any); ok {
		return list
	}
	if content == nil {
		return nil
	}
	return []any{content}
}

func dshSelectUsage(data, msg map[string]any) map[string]any {
	if u, ok := data["usage"].(map[string]any); ok {
		return u
	}
	if u, ok := data["tokenUsage"].(map[string]any); ok {
		return u
	}
	if msg != nil {
		if u, ok := msg["usage"].(map[string]any); ok {
			return u
		}
	}
	return nil
}

func asInt64FromKeys(m map[string]any, keys ...string) int64 {
	if len(m) == 0 {
		return 0
	}
	for _, k := range keys {
		if val, ok := m[k]; ok && val != nil {
			return asInt64(val)
		}
	}
	return 0
}

func dshResultBlock(msg map[string]any) map[string]any {
	if msg == nil {
		return nil
	}
	items, ok := msg["content"].([]any)
	if !ok {
		return nil
	}
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		t := strField(m, "type")
		if t == "tool-result" || t == "tool_result" {
			return m
		}
	}
	return nil
}
