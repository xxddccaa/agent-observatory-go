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
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/agent-observatory/agent-observatory-go/internal/discovery"
	"github.com/agent-observatory/agent-observatory-go/internal/model"
)

const (
	MaxLineBytes   = 16 * 1024 * 1024
	PrefixBytes    = 4096
	OngoingSeconds = 60
)

// RawLine represents one bounded line slice scanned from a JSONL stream.
type RawLine struct {
	ByteOffset    int64
	ByteLength    int64
	Terminated    bool
	Raw           []byte
	Prefix        []byte
	Oversized     bool
	OversizedHash string
}

func readJSONLLines(path string, offset int64, maxLineBytes, prefixBytes int, allowUnterminatedValidJSON bool) ([]RawLine, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	if offset > 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			return nil, err
		}
	}

	reader := bufio.NewReaderSize(f, 64*1024)
	curOffset := offset
	var out []RawLine

	for {
		lineStart := curOffset
		var buf bytes.Buffer
		var oversized bool
		var prefix []byte
		var hash = sha256.New()
		var lineLen int64
		var terminated bool

		for {
			frag, readErr := reader.ReadSlice('\n')
			n := len(frag)
			if n > 0 {
				lineLen += int64(n)
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
				terminated = true
				break
			}
			if readErr == bufio.ErrBufferFull {
				continue
			}
			if readErr == io.EOF {
				break
			}
			return nil, readErr
		}

		if lineLen == 0 {
			break
		}
		curOffset += lineLen

		if oversized {
			if !terminated {
				break
			}
			out = append(out, RawLine{
				ByteOffset:    lineStart,
				ByteLength:    lineLen,
				Terminated:    true,
				Prefix:        prefix,
				Oversized:     true,
				OversizedHash: hex.EncodeToString(hash.Sum(nil)),
			})
			continue
		}

		raw := buf.Bytes()
		trimmed := bytes.TrimRight(raw, "\r\n")
		if len(bytes.TrimSpace(trimmed)) == 0 {
			if !terminated {
				break
			}
			continue
		}

		if !terminated {
			if !allowUnterminatedValidJSON {
				break
			}
			var obj map[string]any
			if err := json.Unmarshal(sanitizeUTF8AndNUL(trimmed), &obj); err != nil || obj == nil {
				break
			}
		}

		p := trimmed
		if len(p) > prefixBytes {
			p = p[:prefixBytes]
		}
		out = append(out, RawLine{
			ByteOffset: lineStart,
			ByteLength: lineLen,
			Terminated: terminated,
			Raw:        append([]byte(nil), trimmed...),
			Prefix:     append([]byte(nil), p...),
		})
		if !terminated {
			break
		}
	}
	return out, nil
}

func sanitizeUTF8AndNUL(raw []byte) []byte {
	s := strings.ToValidUTF8(string(raw), "\ufffd")
	if strings.IndexByte(s, 0) >= 0 {
		s = strings.ReplaceAll(s, "\x00", "")
	}
	if strings.Contains(s, `\u0000`) {
		s = strings.ReplaceAll(s, `\u0000`, "")
	}
	return []byte(s)
}

func sanitizeString(s string) string {
	s = strings.ToValidUTF8(s, "\ufffd")
	if strings.IndexByte(s, 0) >= 0 {
		s = strings.ReplaceAll(s, "\x00", "")
	}
	return s
}

func sanitizeMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[sanitizeString(k)] = sanitizeValue(v)
	}
	return out
}

func sanitizeValue(v any) any {
	switch val := v.(type) {
	case string:
		return sanitizeString(val)
	case map[string]any:
		return sanitizeMap(val)
	case []any:
		out := make([]any, len(val))
		for i, item := range val {
			out[i] = sanitizeValue(item)
		}
		return out
	default:
		return v
	}
}

func marshalSafeJSON(v any) json.RawMessage {
	data, err := json.Marshal(sanitizeValue(v))
	if err != nil {
		fallback, _ := json.Marshal(map[string]any{"parse_error": "marshal_failed"})
		return fallback
	}
	return data
}

func unparsedEventJSON(reason string, prefix []byte, byteLength int64, extra map[string]any) json.RawMessage {
	if len(prefix) > PrefixBytes {
		prefix = prefix[:PrefixBytes]
	}
	payload := map[string]any{
		"parse_error": reason,
		"byte_length": byteLength,
		"raw":         sanitizeString(string(prefix)),
		"truncated":   byteLength > int64(len(prefix)),
	}
	for k, v := range extra {
		payload[k] = v
	}
	return marshalSafeJSON(payload)
}

func stableID(prefix, value string) string {
	sum := sha256.Sum256([]byte(prefix + ":" + value))
	return hex.EncodeToString(sum[:])[:48]
}

func sha256Hex(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func parseTimestamp(raw any) *string {
	switch v := raw.(type) {
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return nil
		}
		if t, err := model.ParseUTC(trimmed); err == nil {
			formatted := model.FormatUTC(t)
			return &formatted
		}
		if t, err := time.Parse(time.RFC3339, trimmed); err == nil {
			formatted := model.FormatUTC(t)
			return &formatted
		}
		return nil
	case float64:
		if v <= 0 {
			return nil
		}
		sec := v
		if sec > 100_000_000_000 {
			sec = sec / 1000.0
		}
		secInt := int64(sec)
		nsec := int64((sec - float64(secInt)) * 1e9)
		formatted := model.FormatUTC(time.Unix(secInt, nsec))
		return &formatted
	case int64:
		if v <= 0 {
			return nil
		}
		if v > 100_000_000_000 {
			formatted := model.FormatUTC(time.UnixMilli(v))
			return &formatted
		}
		formatted := model.FormatUTC(time.Unix(v, 0))
		return &formatted
	default:
		return nil
	}
}

func asInt64(v any) int64 {
	switch val := v.(type) {
	case float64:
		return int64(val)
	case int64:
		return val
	case int:
		return int64(val)
	case json.Number:
		if n, err := val.Int64(); err == nil {
			return n
		}
	case string:
		if n, err := strconv.ParseInt(strings.TrimSpace(val), 10, 64); err == nil {
			return n
		}
	}
	return 0
}

func asNonNegInt64(v any) int64 {
	n := asInt64(v)
	if n < 0 {
		return 0
	}
	return n
}

func strField(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if raw, ok := m[k]; ok {
			if s, ok := raw.(string); ok && s != "" {
				return sanitizeString(s)
			}
		}
	}
	return ""
}

func strPtr(v string) *string {
	if v == "" {
		return nil
	}
	c := v
	return &c
}

func elapsedMS(startedAt, completedAt *string) *int64 {
	if startedAt == nil || completedAt == nil {
		return nil
	}
	t1, err1 := model.ParseUTC(*startedAt)
	t2, err2 := model.ParseUTC(*completedAt)
	if err1 != nil || err2 != nil {
		return nil
	}
	ms := t2.Sub(t1).Milliseconds()
	if ms < 0 {
		ms = 0
	}
	return &ms
}

func collapseSpaces(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func truncateRunes(s string, maxRunes int) string {
	r := []rune(s)
	if len(r) <= maxRunes {
		return s
	}
	return string(r[:maxRunes])
}

func fileStem(path string) string {
	base := filepath.Base(path)
	if strings.HasSuffix(base, ".jsonl.zst") {
		return strings.TrimSuffix(base, ".jsonl.zst")
	}
	ext := filepath.Ext(base)
	return strings.TrimSuffix(base, ext)
}

func decodeObjectMap(raw json.RawMessage) map[string]any {
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil || obj == nil {
		return map[string]any{}
	}
	return obj
}

func encodeCursorJSON(offset, line int64) string {
	data, _ := json.Marshal(map[string]int64{
		"byte_offset": offset,
		"line_number": line,
	})
	return string(data)
}

func encodeFingerprintJSON(fp map[string]any) string {
	data, _ := json.Marshal(fp)
	return string(data)
}

func parseJSONMap(raw string) map[string]any {
	if strings.TrimSpace(raw) == "" {
		return map[string]any{}
	}
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	var out map[string]any
	if err := dec.Decode(&out); err != nil || out == nil {
		return map[string]any{}
	}
	return out
}

type appendPlanOpts struct {
	extraFP             map[string]any
	checkPath           bool
	checkPrefix         bool
	rebuildOnSameSize   bool
}

func planAppendJSONL(src SourceDescriptor, cp *model.IndexCheckpoint, parserVersion string, opts appendPlanOpts) (SourcePlan, error) {
	info, err := os.Stat(src.Path)
	if err != nil {
		return SourcePlan{}, err
	}
	resolved := discovery.CachedResolve(src.Path)
	fp := map[string]any{
		"size":     info.Size(),
		"mtime_ns": info.ModTime().UnixNano(),
		"path":     resolved,
	}
	for k, v := range opts.extraFP {
		fp[k] = v
	}

	fullFP := func() map[string]any {
		if !opts.checkPrefix {
			return fp
		}
		if fileFP, fpErr := discovery.ComputeFileFingerprint(src.Path); fpErr == nil {
			mergeFileFingerprint(fp, fileFP)
		}
		return fp
	}

	if cp == nil || cp.ParserVersion != parserVersion {
		return SourcePlan{Source: src, Action: PlanRebuild, Fingerprint: fullFP()}, nil
	}

	prev := parseJSONMap(cp.FingerprintJSON)
	cursor := parseJSONMap(cp.CursorJSON)

	if opts.checkPath && fmt.Sprint(prev["path"]) != resolved {
		return SourcePlan{Source: src, Action: PlanRebuild, Fingerprint: fullFP()}, nil
	}
	if info.Size() < asInt64(cursor["byte_offset"]) {
		return SourcePlan{Source: src, Action: PlanRebuild, Fingerprint: fullFP()}, nil
	}
	for k, v := range opts.extraFP {
		if k != "meta" && fmt.Sprint(prev[k]) != fmt.Sprint(v) {
			return SourcePlan{Source: src, Action: PlanRebuild, Fingerprint: fullFP()}, nil
		}
	}

	sameStat := cp.Status == "indexed" &&
		asInt64(prev["size"]) == info.Size() &&
		asInt64(prev["mtime_ns"]) == info.ModTime().UnixNano()
	if sameStat {
		extraMatch := true
		for k, v := range opts.extraFP {
			if fmt.Sprint(prev[k]) != fmt.Sprint(v) {
				extraMatch = false
				break
			}
		}
		if extraMatch {
			return SourcePlan{Source: src, Action: PlanUnchanged, Fingerprint: fp}, nil
		}
	}

	if opts.checkPrefix {
		fileFP, fpErr := discovery.ComputeFileFingerprint(src.Path)
		if fpErr == nil {
			mergeFileFingerprint(fp, fileFP)
			prevFP := fileFingerprintFromMap(prev)
			if !discovery.IndexedContentMatches(src.Path, prevFP) {
				return SourcePlan{Source: src, Action: PlanRebuild, Fingerprint: fp}, nil
			}
		}
	}

	if opts.rebuildOnSameSize && info.Size() <= asInt64(prev["size"]) {
		return SourcePlan{Source: src, Action: PlanRebuild, Fingerprint: fp}, nil
	}
	return SourcePlan{Source: src, Action: PlanAppend, Fingerprint: fp}, nil
}

func planStatRebuild(src SourceDescriptor, cp *model.IndexCheckpoint, parserVersion string, extraFP map[string]any) (SourcePlan, error) {
	info, err := os.Stat(src.Path)
	if err != nil {
		return SourcePlan{}, err
	}
	fp := map[string]any{
		"size":     info.Size(),
		"mtime_ns": info.ModTime().UnixNano(),
		"path":     discovery.CachedResolve(src.Path),
	}
	for k, v := range extraFP {
		fp[k] = v
	}
	if cp == nil || cp.ParserVersion != parserVersion || cp.Status != "indexed" {
		return SourcePlan{Source: src, Action: PlanRebuild, Fingerprint: fp}, nil
	}
	prev := parseJSONMap(cp.FingerprintJSON)
	if asInt64(prev["size"]) != info.Size() || asInt64(prev["mtime_ns"]) != info.ModTime().UnixNano() {
		return SourcePlan{Source: src, Action: PlanRebuild, Fingerprint: fp}, nil
	}
	for k, v := range extraFP {
		if fmt.Sprint(prev[k]) != fmt.Sprint(v) {
			return SourcePlan{Source: src, Action: PlanRebuild, Fingerprint: fp}, nil
		}
	}
	return SourcePlan{Source: src, Action: PlanUnchanged, Fingerprint: fp}, nil
}

func mergeFileFingerprint(dst map[string]any, fp discovery.FileFingerprint) {
	dst["head_length"] = fp.HeadLength
	dst["head_hash"] = fp.HeadHash
	dst["tail_offset"] = fp.TailOffset
	dst["tail_length"] = fp.TailLength
	dst["tail_hash"] = fp.TailHash
	if fp.HasDevIno {
		dst["device"] = fp.Device
		dst["inode"] = fp.Inode
		dst["has_dev_ino"] = true
	}
}

func fileFingerprintFromMap(m map[string]any) discovery.FileFingerprint {
	return discovery.FileFingerprint{
		Size:       asInt64(m["size"]),
		MtimeNs:    asInt64(m["mtime_ns"]),
		Path:       fmt.Sprint(m["path"]),
		Device:     uint64(asInt64(m["device"])),
		Inode:      uint64(asInt64(m["inode"])),
		HasDevIno:  m["has_dev_ino"] == true,
		HeadLength: int(asInt64(m["head_length"])),
		HeadHash:   strField(m, "head_hash"),
		TailOffset: asInt64(m["tail_offset"]),
		TailLength: int(asInt64(m["tail_length"])),
		TailHash:   strField(m, "tail_hash"),
	}
}

func buildTurnSpan(agent, _ string, session model.AgentSession, turn model.AgentTurn) *model.ActivitySpan {
	if turn.StartedAt == nil {
		return nil
	}
	return &model.ActivitySpan{
		ID:             stableID("span", "turn:"+turn.ID),
		Agent:          agent,
		AgentSessionID: session.ID,
		SourceKind:     "rollout",
		SourceRecordID: strPtr(fmt.Sprintf("turn:%d", turn.TurnIndex)),
		ThreadID:       strPtr(session.ThreadID),
		TurnID:         turn.TurnID,
		SpanType:       "turn",
		Name:           strPtr(fmt.Sprintf("Turn %d", turn.TurnIndex+1)),
		Status:         turn.Status,
		StartedAt:      *turn.StartedAt,
		CompletedAt:    turn.CompletedAt,
		DurationMS:     elapsedMS(turn.StartedAt, turn.CompletedAt),
		Confidence:     turn.MetricConfidence,
		MetadataJSON:   marshalSafeJSON(map[string]any{"agent": agent, "turn_index": turn.TurnIndex}),
		CreatedAt:      turn.CreatedAt,
	}
}

func buildToolSpan(agent, _ string, session model.AgentSession, turnNativeID *string, tool model.ToolCall) *model.ActivitySpan {
	if tool.StartedAt == nil {
		return nil
	}
	return &model.ActivitySpan{
		ID:             stableID("span", "tool:"+tool.ID),
		Agent:          agent,
		AgentSessionID: session.ID,
		SourceKind:     "rollout",
		SourceRecordID: strPtr("tool:" + tool.CallID),
		ThreadID:       strPtr(session.ThreadID),
		TurnID:         turnNativeID,
		SpanType:       tool.ToolKind,
		Name:           strPtr(tool.ToolName),
		Status:         tool.Status,
		StartedAt:      *tool.StartedAt,
		CompletedAt:    tool.CompletedAt,
		DurationMS:     elapsedMS(tool.StartedAt, tool.CompletedAt),
		Confidence:     "derived",
		MetadataJSON:   marshalSafeJSON(map[string]any{"agent": agent, "tool_call_id": tool.CallID}),
		CreatedAt:      tool.CreatedAt,
	}
}

func isRecentFileMtime(mtimeNs int64, window time.Duration) bool {
	if mtimeNs <= 0 {
		return false
	}
	return time.Since(time.Unix(0, mtimeNs)) <= window
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
