package adapters

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/agent-observatory/agent-observatory-go/internal/discovery"
	"github.com/agent-observatory/agent-observatory-go/internal/model"
	"github.com/klauspost/compress/zstd"
)

var (
	codexKnownTop = map[string]bool{
		"session_meta": true, "response_item": true, "event_msg": true, "turn_context": true, "compacted": true,
		"session_meta_root": true, "state_placeholder": true, "partial_view": true, "world_state": true,
		"message": true, "function_call": true, "function_call_output": true,
		"custom_tool_call": true, "custom_tool_call_output": true, "reasoning": true, "web_search_call": true,
		"structured_output": true, "token_count": true, "token_usage_record": true, "additional_tools": true,
	}
	codexKnownEventMsg = map[string]bool{
		"user_message": true, "agent_message": true, "agent_reasoning": true, "token_count": true,
		"context_compacted": true, "task_started": true, "task_complete": true, "turn_aborted": true,
		"turn_started": true, "turn_completed": true, "turn_failed": true, "patch_apply_begin": true,
		"patch_apply_end": true, "web_search_begin": true, "web_search_end": true,
		"exec_command_begin": true, "exec_command_end": true, "thread_settings_applied": true,
		"thread_rolled_back": true, "error": true, "warning": true, "item_completed": true,
	}
	codexToolKinds = map[string]string{
		"exec_command": "exec", "shell": "exec", "local_shell": "exec", "container.exec": "exec",
		"write_stdin": "stdin", "send_input": "stdin",
		"apply_patch": "patch", "update_plan": "plan", "view_image": "image",
		"web_search_call": "web_search", "spawn_agent": "agent", "wait_agent": "agent", "close_agent": "agent",
		"get_goal": "goal", "create_goal": "goal", "update_goal": "goal",
		"request_user_input": "input", "wait": "wait",
	}
	codexMutatingRe   = regexp.MustCompile(`(?:^|[|&;]\s*|\bsudo\s+)(?:rm|mv|cp|dd|tee|truncate|mkdir|rmdir|touch|chmod|chown|ln|install|patch)\b|>>?|\bsed\s+-i|\bgit\s+(?:add|commit|push|reset|checkout|rm|mv|apply|stash|clean)\b|\b(?:npm|pnpm|yarn)\s+(?:install|i|ci|add)\b|\bpip\s+install\b|\bcargo\s+(?:build|install)\b|\bmake\b`)
	codexValidationRe = regexp.MustCompile(`(?i)(?:^|\s)(?:pytest|python(?:3)?\s+-m\s+(?:pytest|py_compile)|ruff\s+check|mypy|npm\s+(?:run\s+)?(?:test|check|build)|npx\s+(?:playwright|vitest)|(?:vue-)?tsc|cargo\s+(?:test|check)|go\s+test|go\s+vet|make\s+(?:test|check))\b`)
	codexExitCodeRe   = regexp.MustCompile(`exited with code (-?\d+)`)
)

// CodexAdapter indexes Codex-compatible rollout JSONL and .jsonl.zst transcripts.
type CodexAdapter struct {
	root           string
	agent          string
	sourceKind     string
	identityPrefix string
	parserVersion  string
	tree           *discovery.TreeCache
}

// NewCodexAdapter creates a Codex rollout adapter rooted at sessionsDir.
func NewCodexAdapter(sessionsDir string) *CodexAdapter {
	return newCodexVariantAdapter(sessionsDir, "codex", "codex_jsonl", "codex")
}

// NewXDCodeAdapter creates an XDCode rollout adapter reusing Codex JSONL parsing.
func NewXDCodeAdapter(sessionsDir string) *CodexAdapter {
	return newCodexVariantAdapter(sessionsDir, "xdcode", "xdcode_jsonl", "xdcode")
}

func newCodexVariantAdapter(sessionsDir, agent, sourceKind, identityPrefix string) *CodexAdapter {
	expanded := discovery.ExpandPath(sessionsDir)
	return &CodexAdapter{
		root:           expanded,
		agent:          agent,
		sourceKind:     sourceKind,
		identityPrefix: identityPrefix,
		parserVersion:  "4",
		tree:           discovery.NewTreeCache(expanded, "rollout-*.jsonl*", nil),
	}
}

func (a *CodexAdapter) Kind() string          { return a.sourceKind }
func (a *CodexAdapter) Agent() string         { return a.agent }
func (a *CodexAdapter) ParserVersion() string { return a.parserVersion }

func (a *CodexAdapter) WatchPaths(src SourceDescriptor) []string {
	return []string{src.Path}
}

// Identity returns one stable identity across plain .jsonl and compressed .jsonl.zst siblings.
func (a *CodexAdapter) Identity(path string) string {
	canonical := strings.TrimSuffix(path, ".zst")
	return a.identityPrefix + ":" + sha256Hex([]byte(discovery.CachedResolve(canonical)))
}

func (a *CodexAdapter) Discover(full bool) ([]SourceDescriptor, error) {
	if _, err := os.Stat(a.root); err != nil {
		return nil, nil
	}
	matches := a.tree.Glob(full)
	existsSet := make(map[string]bool, len(matches))
	for _, p := range matches {
		existsSet[p] = true
	}

	type cand struct {
		desc  SourceDescriptor
		mtime int64
	}
	var cands []cand
	for _, p := range matches {
		if !strings.HasSuffix(p, ".jsonl") && !strings.HasSuffix(p, ".jsonl.zst") {
			continue
		}
		info, err := os.Stat(p)
		if err != nil || info.IsDir() {
			continue
		}
		isZst := strings.HasSuffix(p, ".jsonl.zst")
		if isZst && existsSet[strings.TrimSuffix(p, ".zst")] {
			continue
		}
		comp := ""
		if isZst {
			comp = "zstd"
		}
		cands = append(cands, cand{
			desc: SourceDescriptor{
				Kind:        a.Kind(),
				Identity:    a.Identity(p),
				Path:        p,
				Format:      "jsonl",
				Agent:       a.Agent(),
				Compression: comp,
				ThreadID:    fileStem(p),
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

func (a *CodexAdapter) Plan(src SourceDescriptor, cp *model.IndexCheckpoint) (SourcePlan, error) {
	if src.Compression == "zstd" {
		return planStatRebuild(src, cp, a.parserVersion, map[string]any{"compression": "zstd"})
	}
	return planAppendJSONL(src, cp, a.parserVersion, appendPlanOpts{
		extraFP:           map[string]any{"compression": ""},
		checkPath:         true,
		checkPrefix:       true,
		rebuildOnSameSize: true,
	})
}

func (a *CodexAdapter) Parse(plan SourcePlan, existing *model.AgentSession) (*ParsedSource, error) {
	src := plan.Source
	realPath := discovery.CachedResolve(src.Path)
	rolloutRealPath := realPath
	if a.identityPrefix != "codex" {
		rolloutRealPath = a.identityPrefix + ":" + realPath
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
		RolloutRealPath:     strPtr(rolloutRealPath),
		SourcePath:          strPtr(realPath),
		RolloutFormat:       "jsonl",
		SourceFormat:        "jsonl",
		RolloutCompression:  strPtr(src.Compression),
		SourceCompression:   strPtr(src.Compression),
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
		sess.RolloutRealPath = strPtr(rolloutRealPath)
		sess.SourcePath = strPtr(realPath)
		sess.UpdatedAt = nowStr
		startOffset = existing.LastIndexedOffset
		startLine = existing.LastIndexedLine
		eventIndex = int(existing.LastIndexedLine)
	}

	var lines []RawLine
	if src.Compression == "zstd" {
		lines, err = readZstdLines(src.Path, startLine, MaxLineBytes, PrefixBytes)
	} else {
		lines, err = readJSONLLines(src.Path, startOffset, MaxLineBytes, PrefixBytes, false)
	}
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

		ev, ok := parseCodexLine(ln.Raw, curLine, ln.ByteOffset, eventIndex, sess.ID, a.parserVersion, nowStr)
		if !ok {
			ev = model.AgentEvent{
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
			}
			sess.IndexStatus = "parse_error"
		} else {
			applyCodexMeta(&sess, ev)
		}
		newEvents = append(newEvents, ev)
		eventIndex++
		sess.LastIndexedLine = curLine
		if src.Compression == "" {
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

func readZstdLines(path string, skipLines int64, maxLineBytes, prefixBytes int) ([]RawLine, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	zr, err := zstd.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer zr.Close()

	br := bufio.NewReaderSize(zr, 64*1024)
	var out []RawLine
	var lineNo int64
	for {
		var buf bytes.Buffer
		var oversized bool
		var prefix []byte
		var hash = sha256.New()
		var lineLen int64
		var eof bool

		for {
			frag, readErr := br.ReadSlice('\n')
			if len(frag) > 0 {
				lineLen += int64(len(frag))
				if oversized {
					_, _ = hash.Write(frag)
				} else if lineLen > int64(maxLineBytes) {
					oversized = true
					combined := append(append([]byte(nil), buf.Bytes()...), frag...)
					if len(combined) > prefixBytes {
						prefix = combined[:prefixBytes]
					} else {
						prefix = combined
					}
					_, _ = hash.Write(combined)
					buf.Reset()
				} else {
					buf.Write(frag)
				}
			}
			if readErr == nil {
				break
			}
			if readErr == bufio.ErrBufferFull {
				continue
			}
			if readErr == io.EOF {
				eof = true
				break
			}
			return nil, readErr
		}
		if lineLen == 0 && eof {
			break
		}
		lineNo++
		if lineNo <= skipLines {
			if eof {
				break
			}
			continue
		}
		if oversized {
			out = append(out, RawLine{
				ByteLength:    lineLen,
				Terminated:    true,
				Prefix:        prefix,
				Oversized:     true,
				OversizedHash: hex.EncodeToString(hash.Sum(nil)),
			})
		} else {
			trimmed := bytes.TrimRight(buf.Bytes(), "\r\n")
			if len(bytes.TrimSpace(trimmed)) > 0 {
				out = append(out, RawLine{
					ByteLength: lineLen,
					Terminated: true,
					Raw:        append([]byte(nil), trimmed...),
					Prefix:     append([]byte(nil), trimmed...),
				})
			}
		}
		if eof {
			break
		}
	}
	return out, nil
}

func parseCodexLine(rawBytes []byte, lineNumber, byteOffset int64, eventIndex int, sessionID, parserVersion, nowStr string) (model.AgentEvent, bool) {
	cleaned := sanitizeUTF8AndNUL(rawBytes)
	var obj map[string]any
	if err := json.Unmarshal(cleaned, &obj); err != nil || obj == nil {
		return model.AgentEvent{}, false
	}
	safe := sanitizeMap(obj)
	top := strField(safe, "type")
	if strField(safe, "record_type") == "state" {
		top = "state_placeholder"
	}
	if top == "" {
		switch {
		case safe["payload"] != nil:
			top = "session_meta"
		case safe["id"] != nil && safe["timestamp"] != nil:
			top = "session_meta_root"
		case safe["call_id"] != nil && safe["arguments"] != nil && safe["name"] != nil:
			top = "function_call"
		case safe["call_id"] != nil && safe["output"] != nil:
			top = "function_call_output"
		case safe["role"] != nil && safe["content"] != nil:
			top = "message"
		case safe["encrypted_content"] != nil:
			top = "reasoning"
		}
	}

	payload, ok := safe["payload"].(map[string]any)
	if !ok {
		payload = safe
	}
	payloadType := strField(payload, "type")
	switch top {
	case "message", "function_call", "function_call_output", "reasoning", "session_meta_root":
		payloadType = top
	}
	if vm := strField(safe, "view_mode"); vm != "" && vm != "full" {
		payloadType = "partial_view"
	}

	normalized := "unknown"
	candidate := payloadType
	if candidate == "" {
		candidate = top
	}
	if codexKnownTop[candidate] {
		normalized = candidate
	}
	if top == "response_item" && codexKnownTop[payloadType] {
		normalized = payloadType
	}
	if top == "event_msg" && codexKnownEventMsg[payloadType] {
		normalized = payloadType
	}

	return model.AgentEvent{
		AgentSessionID: sessionID,
		EventIndex:     eventIndex,
		LineNumber:     lineNumber,
		ByteOffset:     byteOffset,
		EventTimestamp: parseTimestamp(safe["timestamp"]),
		TopLevelType:   strPtr(top),
		PayloadType:    strPtr(payloadType),
		NormalizedType: normalized,
		RawJSON:        marshalSafeJSON(safe),
		RawHash:        sha256Hex(rawBytes),
		IsKnown:        normalized != "unknown",
		ParserVersion:  parserVersion,
		CreatedAt:      nowStr,
	}, true
}

func applyCodexMeta(sess *model.AgentSession, ev model.AgentEvent) {
	raw := decodeObjectMap(ev.RawJSON)
	payload, ok := raw["payload"].(map[string]any)
	if !ok {
		payload = raw
	}
	top := ""
	if ev.TopLevelType != nil {
		top = *ev.TopLevelType
	}
	if top == "session_meta" || top == "session_meta_root" {
		threadMap, _ := payload["thread"].(map[string]any)
		currentID := strField(payload, "id", "thread_id")
		if currentID == "" && threadMap != nil {
			currentID = strField(threadMap, "sessionId")
		}
		parentID := strField(payload, "session_id")
		if currentID != "" {
			sess.ThreadID = currentID
		} else if parentID != "" {
			sess.ThreadID = parentID
		}
		if parentID != "" && parentID != currentID {
			sess.ParentSessionID = strPtr(parentID)
			sess.IsSubagent = true
		}
		if cwd := strField(payload, "cwd"); cwd != "" {
			sess.CWD = strPtr(cwd)
		}
		if orig := strField(payload, "originator"); orig != "" {
			sess.Originator = strPtr(orig)
		}
		if ver := strField(payload, "cli_version"); ver != "" {
			sess.CLIVersion = strPtr(ver)
		}
		if m := strField(payload, "model", "model_provider"); m != "" {
			sess.Model = strPtr(m)
		}
		if git, ok := payload["git"].(map[string]any); ok {
			if branch := strField(git, "branch"); branch != "" {
				sess.GitBranch = strPtr(branch)
			}
		}
		if title := strField(payload, "ai-title"); title != "" {
			sess.AITitle = strPtr(truncateRunes(title, 255))
		}
		if ap := strField(payload, "ask_for_approval"); ap != "" {
			sess.ApprovalMode = strPtr(ap)
		}
		if hb, ok := payload["history_base"].(map[string]any); ok {
			if tid := strField(hb, "thread_id"); tid != "" {
				sess.HistoryBaseThreadID = strPtr(tid)
			}
		}
		if ts := parseTimestamp(payload["timestamp"]); ts != nil {
			sess.StartTime = ts
		} else if sess.StartTime == nil && ev.EventTimestamp != nil {
			sess.StartTime = ev.EventTimestamp
		}
	} else if strField(raw, "type") == "turn_context" {
		if m := strField(payload, "model"); m != "" {
			sess.Model = strPtr(m)
		}
	}
}

func (a *CodexAdapter) Project(sess model.AgentSession, allEvents []model.AgentEvent) (model.AgentSession, model.SessionProjection, error) {
	nowStr := model.FormatUTC(time.Now())
	var (
		turns              []model.AgentTurn
		messages           []model.Message
		tools              []*model.ToolCall
		toolOrder          []string
		toolsByCall        = make(map[string]*model.ToolCall)
		pendingOutputs     = make(map[string]codexPendingOut)
		seenMessages       = make(map[string]bool)
		validationResults  = make(map[string][]bool)
		mutatingTurns      = make(map[string]bool)
		toolIntervals      = make(map[string][][2]time.Time)
		currentTurn        *model.AgentTurn
		lastEventTS        *string
		sessionTotals      map[string]any
		sequence           int
		maxEventIndex      = -1
	)

	for _, ev := range allEvents {
		if ev.EventIndex > maxEventIndex {
			maxEventIndex = ev.EventIndex
		}
		if ev.EventTimestamp != nil {
			lastEventTS = ev.EventTimestamp
		}
		raw := decodeObjectMap(ev.RawJSON)
		payload, ok := raw["payload"].(map[string]any)
		if !ok {
			payload = raw
		}
		top := ""
		if ev.TopLevelType != nil {
			top = *ev.TopLevelType
		}
		ptype := ""
		if ev.PayloadType != nil {
			ptype = *ev.PayloadType
		}
		ts := ev.EventTimestamp

		// Turn boundaries.
		switch ptype {
		case "task_started", "turn_started":
			turn := model.AgentTurn{
				ID:               stableID("turn", fmt.Sprintf("%s:%d", sess.ID, len(turns))),
				Agent:            sess.Agent,
				AgentSessionID:   sess.ID,
				TurnIndex:        len(turns),
				TurnID:           strPtr(strField(payload, "turn_id")),
				Status:           "running",
				StartedAt:        ts,
				ValidationStatus: "unknown",
				Outcome:          "running",
				MetricConfidence: "derived",
				CreatedAt:        nowStr,
				UpdatedAt:        nowStr,
			}
			turns = append(turns, turn)
			currentTurn = &turns[len(turns)-1]
			continue
		case "task_complete", "turn_completed":
			if currentTurn != nil {
				currentTurn.Status = "completed"
				currentTurn.Outcome = "completed"
				if ts != nil {
					currentTurn.CompletedAt = ts
				}
				currentTurn.DurationMS = elapsedMS(currentTurn.StartedAt, currentTurn.CompletedAt)
			}
			continue
		case "turn_aborted", "turn_failed":
			if currentTurn != nil {
				st := "failed"
				if ptype == "turn_aborted" {
					st = "aborted"
				}
				currentTurn.Status = st
				currentTurn.Outcome = st
				if ts != nil {
					currentTurn.CompletedAt = ts
				}
				currentTurn.DurationMS = elapsedMS(currentTurn.StartedAt, currentTurn.CompletedAt)
				currentTurn.ErrorKind = strPtr(strField(payload, "reason", "error_kind"))
				if currentTurn.ErrorKind == nil {
					currentTurn.ErrorKind = strPtr(ptype)
				}
				currentTurn.ErrorMessage = strPtr(strField(payload, "message", "error"))
			}
			continue
		case "error":
			if currentTurn != nil {
				currentTurn.Status = "failed"
				currentTurn.Outcome = "failed"
				ek := strField(payload, "error_kind", "kind")
				if ek == "" {
					ek = "error"
				}
				currentTurn.ErrorKind = strPtr(ek)
				em := strField(payload, "message", "error")
				if em == "" {
					em = "unknown error"
				}
				currentTurn.ErrorMessage = strPtr(em)
			}
			continue
		case "warning", "context_compacted":
			text := strField(payload, "message", "summary")
			if text == "" {
				if ptype == "context_compacted" {
					text = "Context was compacted"
				} else {
					text = "Warning"
				}
			}
			turnID := turnPtrID(currentTurn)
			key := msgDedupeKey(turnID, "system", "commentary", text)
			if !seenMessages[key] {
				seenMessages[key] = true
				evID := ev.ID
				messages = append(messages, model.Message{
					ID:                stableID("msg", fmt.Sprintf("%s:%d:%d", sess.ID, ev.EventIndex, sequence)),
					AgentSessionID:    sess.ID,
					TurnID:            turnID,
					EventID:           &evID,
					Role:              "system",
					Channel:           "commentary",
					Sequence:          sequence,
					Text:              text,
					MarkdownText:      strPtr(text),
					CopyText:          text,
					Timestamp:         ts,
					IsVisible:         true,
					IsSystemGenerated: true,
					CreatedAt:         nowStr,
				})
				sequence++
			}
			continue
		case "token_count":
			info, ok := payload["info"].(map[string]any)
			if !ok {
				info = payload
			}
			if total, ok := info["total_token_usage"].(map[string]any); ok {
				sessionTotals = total
			}
			if currentTurn != nil {
				if last, ok := info["last_token_usage"].(map[string]any); ok {
					currentTurn.InputTokens = asNonNegInt64(last["input_tokens"])
					currentTurn.CachedInputTokens = asNonNegInt64(last["cached_input_tokens"])
					currentTurn.OutputTokens = asNonNegInt64(last["output_tokens"])
					currentTurn.ReasoningOutputTokens = asNonNegInt64(last["reasoning_output_tokens"])
					currentTurn.TotalTokens = asNonNegInt64(last["total_tokens"])
				}
			}
			continue
		}

		// First agent latency check.
		isAgentEv := ptype == "agent_message" || ptype == "agent_reasoning" || ptype == "function_call" || ptype == "custom_tool_call" ||
			(ptype == "message" && strField(payload, "role") == "assistant")
		if isAgentEv && currentTurn != nil && currentTurn.FirstAgentLatencyMS == nil && currentTurn.StartedAt != nil && ts != nil {
			currentTurn.FirstAgentLatencyMS = elapsedMS(currentTurn.StartedAt, ts)
		}

		if role, channel, text, ok := codexMessageProjection(top, ptype, payload); ok {
			stripped := strings.TrimSpace(text)
			turnID := turnPtrID(currentTurn)
			key := msgDedupeKey(turnID, role, channel, stripped)
			if stripped != "" && !seenMessages[key] {
				seenMessages[key] = true
				if role == "user" && (sess.DisplayTitle == nil || *sess.DisplayTitle == "") {
					sess.DisplayTitle = strPtr(truncateRunes(collapseSpaces(stripped), 255))
				}
				md := text
				if channel == "reasoning" {
					md = "*Reasoning*\n\n" + text
				}
				evID := ev.ID
				messages = append(messages, model.Message{
					ID:             stableID("msg", fmt.Sprintf("%s:%d:%d", sess.ID, ev.EventIndex, sequence)),
					AgentSessionID: sess.ID,
					TurnID:         turnID,
					EventID:        &evID,
					Role:           role,
					Channel:        channel,
					Sequence:       sequence,
					Text:           text,
					MarkdownText:   strPtr(md),
					CopyText:       text,
					Timestamp:      ts,
					IsVisible:      true,
					CreatedAt:      nowStr,
				})
				sequence++
			}
			continue
		}

		switch ptype {
		case "function_call", "custom_tool_call":
			callID := strField(payload, "call_id")
			if callID == "" {
				callID = fmt.Sprintf("idx-%d", ev.EventIndex)
			}
			name := truncateRunes(strField(payload, "name"), 255)
			if name == "" {
				name = "unknown"
			}
			var argsMap map[string]any
			if ptype == "custom_tool_call" {
				switch v := payload["input"].(type) {
				case map[string]any:
					argsMap = v
				case string:
					argsMap = map[string]any{"input": v}
				}
			} else {
				switch v := payload["arguments"].(type) {
				case map[string]any:
					argsMap = v
				case string:
					_ = json.Unmarshal([]byte(v), &argsMap)
				}
			}
			cmdText := codexCommandText(name, argsMap)
			kind := codexToolKinds[name]
			if kind == "" {
				kind = "function"
			}
			var argsRaw json.RawMessage
			if argsMap != nil {
				argsRaw = marshalSafeJSON(argsMap)
			}
			mutating := codexIsMutating(name, cmdText)
			evID := ev.ID
			tc := &model.ToolCall{
				ID:             stableID("tool", fmt.Sprintf("%s:%s", sess.ID, callID)),
				Agent:          sess.Agent,
				AgentSessionID: sess.ID,
				TurnID:         turnPtrID(currentTurn),
				EventID:        &evID,
				CallID:         callID,
				ToolName:       name,
				ToolKind:       kind,
				ArgumentsJSON:  argsRaw,
				CommandText:    strPtr(cmdText),
				Status:         "running",
				IsMutating:     mutating,
				StartedAt:      ts,
				CreatedAt:      nowStr,
			}
			if currentTurn != nil && mutating {
				mutatingTurns[currentTurn.ID] = true
			}
			if pending, ok := pendingOutputs[callID]; ok {
				delete(pendingOutputs, callID)
				applyCodexToolResult(tc, pending.output, pending.code, pending.status, pending.completedAt)
			}
			if _, exists := toolsByCall[callID]; !exists {
				toolOrder = append(toolOrder, callID)
			}
			toolsByCall[callID] = tc
			tools = append(tools, tc)

		case "function_call_output", "custom_tool_call_output", "exec_command_end", "mcp_tool_call_end", "web_search_end":
			callID := strField(payload, "call_id")
			outText := codexExtractOutput(payload["aggregated_output"], payload["output"], payload["result"])
			code := parseCodexExitCode(outText)
			if v, ok := payload["exit_code"]; ok {
				c := int(asInt64(v))
				code = &c
			}
			statusHint := strField(payload, "status")
			if tc, ok := toolsByCall[callID]; ok {
				applyCodexToolResult(tc, outText, code, statusHint, ts)
			} else if callID != "" {
				pendingOutputs[callID] = codexPendingOut{output: outText, code: code, status: statusHint, completedAt: ts}
			}

		case "patch_apply_end":
			callID := strField(payload, "call_id")
			if tc, ok := toolsByCall[callID]; ok {
				success, _ := payload["success"].(bool)
				combined := strings.TrimSpace(strings.TrimSpace(strField(payload, "stdout")) + "\n" + strings.TrimSpace(strField(payload, "stderr")))
				if combined != "" {
					tc.OutputText = strPtr(combined)
				}
				code := 1
				tc.Status = "failed"
				if success {
					code = 0
					tc.Status = "ok"
				}
				tc.ExitCode = &code
				if ts != nil {
					tc.CompletedAt = ts
				}
				tc.IsMutating = true
				if tc.TurnID != nil {
					mutatingTurns[*tc.TurnID] = true
				}
			}
		}
	}

	turnByID := make(map[string]*model.AgentTurn, len(turns))
	for i := range turns {
		turnByID[turns[i].ID] = &turns[i]
	}

	finalTools := make([]model.ToolCall, 0, len(toolOrder))
	var spans []model.ActivitySpan
	for _, cid := range toolOrder {
		tc := toolsByCall[cid]
		if tc.Status == "running" {
			tc.Status = "pending_or_missing_output"
		}
		if tc.TurnID != nil && tc.CommandText != nil && codexValidationRe.MatchString(*tc.CommandText) {
			passed := tc.Status == "ok" && (tc.ExitCode == nil || *tc.ExitCode == 0)
			validationResults[*tc.TurnID] = append(validationResults[*tc.TurnID], passed)
		}
		var turnNativeID *string
		if tc.TurnID != nil {
			if tr := turnByID[*tc.TurnID]; tr != nil {
				turnNativeID = tr.TurnID
				if tc.StartedAt != nil && tc.CompletedAt != nil {
					if t1, e1 := model.ParseUTC(*tc.StartedAt); e1 == nil {
						if t2, e2 := model.ParseUTC(*tc.CompletedAt); e2 == nil {
							toolIntervals[tr.ID] = append(toolIntervals[tr.ID], [2]time.Time{t1, t2})
						}
					}
				}
			}
		}
		finalTools = append(finalTools, *tc)
		if sp := buildToolSpan(sess.Agent, sess.SourceKind, sess, turnNativeID, *tc); sp != nil {
			spans = append(spans, *sp)
		}
	}

	for i := range turns {
		tr := &turns[i]
		if tr.DurationMS == nil {
			tr.DurationMS = elapsedMS(tr.StartedAt, tr.CompletedAt)
		}
		ms := intervalUnionMS(toolIntervals[tr.ID])
		tr.ActiveToolMS = &ms
		if tr.Outcome == "unknown" {
			tr.Outcome = tr.Status
		}
		res := validationResults[tr.ID]
		if len(res) > 0 {
			if res[len(res)-1] {
				tr.ValidationStatus = "passed"
			} else {
				tr.ValidationStatus = "failed"
			}
		} else if mutatingTurns[tr.ID] {
			tr.ValidationStatus = "unverified"
		} else {
			tr.ValidationStatus = "not_applicable"
		}
		if sp := buildTurnSpan(sess.Agent, sess.SourceKind, sess, *tr); sp != nil {
			spans = append(spans, *sp)
		}
	}

	// Session aggregates.
	sess.TurnCount = len(turns)
	if sessionTotals != nil {
		sess.InputTokens = asNonNegInt64(sessionTotals["input_tokens"])
		sess.CachedInputTokens = asNonNegInt64(sessionTotals["cached_input_tokens"])
		sess.CacheCreationInputTokens = asNonNegInt64(sessionTotals["cache_creation_input_tokens"])
		sess.OutputTokens = asNonNegInt64(sessionTotals["output_tokens"])
		sess.ReasoningOutputTokens = asNonNegInt64(sessionTotals["reasoning_output_tokens"])
		sess.TotalTokens = asNonNegInt64(sessionTotals["total_tokens"])
	} else if len(turns) > 0 {
		var inTok, cachedTok, cacheCreateTok, outTok, reasonTok, totTok int64
		for _, tr := range turns {
			inTok += tr.InputTokens
			cachedTok += tr.CachedInputTokens
			cacheCreateTok += tr.CacheCreationInputTokens
			outTok += tr.OutputTokens
			reasonTok += tr.ReasoningOutputTokens
			totTok += tr.TotalTokens
		}
		sess.InputTokens = inTok
		sess.CachedInputTokens = cachedTok
		sess.CacheCreationInputTokens = cacheCreateTok
		sess.OutputTokens = outTok
		sess.ReasoningOutputTokens = reasonTok
		sess.TotalTokens = totTok
	}

	var lastTurn *model.AgentTurn
	if len(turns) > 0 {
		lastTurn = &turns[len(turns)-1]
	}
	openTurn := lastTurn != nil && lastTurn.CompletedAt == nil && (lastTurn.Status == "running" || lastTurn.Status == "unknown")
	sess.IsOngoing = openTurn && isRecentFileMtime(sess.FileMtimeNs, OngoingSeconds*time.Second)
	if lastEventTS != nil {
		sess.LastActivityAt = lastEventTS
	} else if lastTurn != nil && lastTurn.CompletedAt != nil {
		sess.LastActivityAt = lastTurn.CompletedAt
	} else {
		sess.LastActivityAt = sess.StartTime
	}
	if sess.IsOngoing {
		sess.EndTime = nil
	} else if lastTurn != nil && lastTurn.CompletedAt != nil {
		sess.EndTime = lastTurn.CompletedAt
	} else {
		sess.EndTime = lastEventTS
	}
	sess.ProjectedEventIndex = maxEventIndex
	sess.UpdatedAt = nowStr

	_ = tools
	return sess, model.SessionProjection{
		Turns:     turns,
		Messages:  messages,
		ToolCalls: finalTools,
		Spans:     spans,
	}, nil
}

type codexPendingOut struct {
	output      string
	code        *int
	status      string
	completedAt *string
}

func codexMessageProjection(top, ptype string, payload map[string]any) (string, string, string, bool) {
	lowTop := strings.ToLower(top)
	switch lowTop {
	case "usermessage", "user_message":
		return "user", "final", extractContentText(payload["content"]), true
	case "assistantmessage", "agentmessage", "assistant_message", "agent_message":
		return "assistant", "final", extractContentText(payload["content"]), true
	case "reasoning", "reasoningitem":
		return "assistant", "reasoning", extractSummaryOrText(payload), true
	}
	if top == "event_msg" {
		switch ptype {
		case "user_message":
			return "user", "final", extractAnyText(payload["message"]), true
		case "agent_message":
			return "assistant", "final", extractAnyText(payload["message"]), true
		case "agent_reasoning":
			return "assistant", "reasoning", extractAnyText(payload["text"]), true
		case "item_completed":
			if item, ok := payload["item"].(map[string]any); ok {
				switch strings.ToLower(strField(item, "type")) {
				case "usermessage":
					return "user", "final", extractContentText(item["content"]), true
				case "agentmessage":
					return "assistant", "final", extractContentText(item["content"]), true
				case "reasoning":
					return "assistant", "reasoning", extractSummaryOrText(item), true
				}
			}
		}
		return "", "", "", false
	}
	if top == "response_item" {
		if ptype == "message" {
			role := strField(payload, "role")
			if role == "" {
				role = "assistant"
			}
			if role != "assistant" {
				return "", "", "", false
			}
			return "assistant", "final", extractContentText(payload["content"]), true
		}
		if ptype == "reasoning" {
			return "assistant", "reasoning", extractContentText(payload["summary"]), true
		}
	}
	if top == "message" {
		role := strField(payload, "role")
		if role == "user" || role == "assistant" {
			return role, "final", extractContentText(payload["content"]), true
		}
	}
	if top == "reasoning" {
		return "assistant", "reasoning", extractContentText(payload["summary"]), true
	}
	return "", "", "", false
}

func extractSummaryOrText(m map[string]any) string {
	for _, k := range []string{"summary_text", "summary", "content"} {
		if v, ok := m[k]; ok && v != nil {
			if txt := extractContentText(v); txt != "" {
				return txt
			}
		}
	}
	return ""
}

func extractContentText(v any) string {
	switch val := v.(type) {
	case string:
		return sanitizeString(val)
	case []any:
		var parts []string
		for _, item := range val {
			if s, ok := item.(string); ok {
				if s != "" {
					parts = append(parts, sanitizeString(s))
				}
			} else if m, ok := item.(map[string]any); ok {
				if txt := strField(m, "text"); txt != "" {
					parts = append(parts, txt)
				}
			}
		}
		return strings.Join(parts, "\n")
	default:
		return extractAnyText(v)
	}
}

func extractAnyText(v any) string {
	switch val := v.(type) {
	case nil:
		return ""
	case string:
		return sanitizeString(val)
	case []any:
		var parts []string
		for _, item := range val {
			if txt := extractAnyText(item); txt != "" {
				parts = append(parts, txt)
			}
		}
		return strings.Join(parts, "\n")
	case map[string]any:
		if txt := strField(val, "text"); txt != "" {
			return txt
		}
		data, err := json.Marshal(sanitizeValue(val))
		if err != nil {
			return ""
		}
		return string(data)
	default:
		return fmt.Sprint(val)
	}
}

func codexCommandText(name string, args map[string]any) string {
	if args == nil {
		return ""
	}
	switch name {
	case "exec_command", "shell", "local_shell", "container.exec":
		return extractAnyText(firstNonNil(args["cmd"], args["command"]))
	case "write_stdin", "send_input":
		return extractAnyText(firstNonNil(args["chars"], args["message"]))
	case "apply_patch":
		return extractAnyText(firstNonNil(args["input"], args["patch"]))
	default:
		return ""
	}
}

func codexIsMutating(name, cmd string) bool {
	if name == "apply_patch" {
		return true
	}
	if (name == "exec_command" || name == "shell" || name == "local_shell" || name == "container.exec") && cmd != "" {
		return codexMutatingRe.MatchString(cmd)
	}
	return false
}

func codexExtractOutput(candidates ...any) string {
	for _, c := range candidates {
		if c == nil {
			continue
		}
		switch v := c.(type) {
		case string:
			return sanitizeString(v)
		case []any:
			var parts []string
			for _, item := range v {
				if m, ok := item.(map[string]any); ok {
					if txt := strField(m, "text"); txt != "" {
						parts = append(parts, txt)
					} else if t := strField(m, "type"); t != "" {
						parts = append(parts, "["+t+"]")
					}
				}
			}
			return strings.Join(parts, "\n")
		default:
			data, _ := json.Marshal(sanitizeValue(v))
			return string(data)
		}
	}
	return ""
}

func parseCodexExitCode(output string) *int {
	if output == "" {
		return nil
	}
	m := codexExitCodeRe.FindStringSubmatch(output)
	if len(m) < 2 {
		return nil
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		return nil
	}
	return &n
}

func applyCodexToolResult(tc *model.ToolCall, output string, code *int, statusHint string, completedAt *string) {
	if output != "" {
		tc.OutputText = strPtr(output)
	}
	tc.ExitCode = code
	low := strings.ToLower(strings.TrimSpace(output))
	looksFailed := strings.Contains(low, "<tool_use_error>") || strings.HasPrefix(low, "error:")
	failed := statusHint == "failed" || statusHint == "error" || (code != nil && *code != 0) || looksFailed
	if failed {
		tc.Status = "failed"
	} else {
		tc.Status = "ok"
	}
	if completedAt != nil {
		tc.CompletedAt = completedAt
	}
}

func turnPtrID(t *model.AgentTurn) *string {
	if t == nil {
		return nil
	}
	id := t.ID
	return &id
}

func msgDedupeKey(turnID *string, role, channel, text string) string {
	tid := ""
	if turnID != nil {
		tid = *turnID
	}
	return tid + "\x00" + role + "\x00" + channel + "\x00" + text
}

func firstNonNil(vals ...any) any {
	for _, v := range vals {
		if v != nil {
			return v
		}
	}
	return nil
}

func intervalUnionMS(intervals [][2]time.Time) int64 {
	if len(intervals) == 0 {
		return 0
	}
	sort.Slice(intervals, func(i, j int) bool {
		return intervals[i][0].Before(intervals[j][0])
	})
	start, end := intervals[0][0], intervals[0][1]
	var total int64
	for _, iv := range intervals[1:] {
		if !iv[0].After(end) {
			if iv[1].After(end) {
				end = iv[1]
			}
			continue
		}
		if ms := end.Sub(start).Milliseconds(); ms > 0 {
			total += ms
		}
		start, end = iv[0], iv[1]
	}
	if ms := end.Sub(start).Milliseconds(); ms > 0 {
		total += ms
	}
	return total
}
