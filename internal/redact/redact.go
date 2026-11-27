package redact

import (
	"encoding/json"
	"regexp"
)

var (
	reOpenAI     = regexp.MustCompile(`sk-[A-Za-z0-9_-]{20,}`)
	reAWS        = regexp.MustCompile(`\bAKIA[0-9A-Z]{16}\b`)
	reBearer     = regexp.MustCompile(`(?i)\bBearer[ \t]+[A-Za-z0-9._~+/=-]{16,}`)
	reURLInfo    = regexp.MustCompile(`(?i)\b((?:postgres(?:ql)?|redis|https?)://[^:/\s]+:)[^@\s]+(@)`)
	reCookie     = regexp.MustCompile(`(?i)\b(cookie|set-cookie)(\s*[:=]\s*)[^\r\n]+`)
	reAssignment = regexp.MustCompile(`(?i)\b(api[_-]?key|token|secret|password)\b(\s*[:=]\s*)(["']?)[^\s"']{8,}`)
)

// Strings masks common secret patterns in display and export text without altering stored DB rows.
func Strings(value string) string {
	if value == "" {
		return value
	}
	redacted := reBearer.ReplaceAllString(value, "Bearer [REDACTED_SECRET]")
	redacted = reOpenAI.ReplaceAllString(redacted, "[REDACTED_SECRET]")
	redacted = reAWS.ReplaceAllString(redacted, "[REDACTED_SECRET]")
	redacted = reURLInfo.ReplaceAllString(redacted, "${1}[REDACTED_SECRET]${2}")
	redacted = reCookie.ReplaceAllString(redacted, "${1}${2}[REDACTED_SECRET]")
	return reAssignment.ReplaceAllString(redacted, "${1}${2}${3}[REDACTED_SECRET]")
}

// Ptr applies Strings redaction to an optional string pointer, returning a fresh copy.
func Ptr(value *string) *string {
	if value == nil {
		return nil
	}
	cleaned := Strings(*value)
	return &cleaned
}

// JSON recursively redacts string values inside a JSON payload.
func JSON(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return raw
	}
	var parsed any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		b, _ := json.Marshal(Strings(string(raw)))
		return b
	}
	redacted := redactValue(parsed)
	out, err := json.Marshal(redacted)
	if err != nil {
		return raw
	}
	return out
}

func redactValue(v any) any {
	switch val := v.(type) {
	case string:
		return Strings(val)
	case map[string]any:
		out := make(map[string]any, len(val))
		for k, item := range val {
			out[k] = redactValue(item)
		}
		return out
	case []any:
		out := make([]any, len(val))
		for i, item := range val {
			out[i] = redactValue(item)
		}
		return out
	default:
		return v
	}
}
