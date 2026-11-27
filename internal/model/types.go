package model

import "time"

// TimestampLayout uses fixed-width 6-digit fractional UTC seconds so SQLite
// string ordering and strftime comparisons stay consistent across all rows.
const TimestampLayout = "2006-01-02T15:04:05.000000Z"

func FormatUTC(t time.Time) string {
	return t.UTC().Format(TimestampLayout)
}

func ParseUTC(raw string) (time.Time, error) {
	if t, err := time.Parse(TimestampLayout, raw); err == nil {
		return t.UTC(), nil
	}
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, err
	}
	return t.UTC(), nil
}

type MetaResponse struct {
	App         string `json:"app"`
	Version     string `json:"version"`
	DataDirHash string `json:"data_dir_hash"`
}

type LiveHealth struct {
	Status  string `json:"status"`
	Version string `json:"version"`
}

type ComponentHealth struct {
	Status        string `json:"status"`
	Healthy       bool   `json:"healthy"`
	Message       string `json:"message,omitempty"`
	Detail        string `json:"detail,omitempty"`
	LastSeenAt    string `json:"last_seen_at,omitempty"`
	LastSuccessAt string `json:"last_success_at,omitempty"`
}

type ReadyHealth struct {
	Status          string                     `json:"status"`
	Version         string                     `json:"version"`
	Environment     string                     `json:"environment"`
	Components      map[string]ComponentHealth `json:"components"`
	RecoveryActions []string                   `json:"recovery_actions"`
}

type ErrorDetail struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id,omitempty"`
	Retryable bool   `json:"retryable"`
}

type ErrorEnvelope struct {
	Error ErrorDetail `json:"error"`
}
