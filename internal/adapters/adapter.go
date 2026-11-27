package adapters

import (
	"github.com/agent-observatory/agent-observatory-go/internal/model"
)

// PlanAction specifies whether an indexing pass should skip, append to, or rebuild a source.
type PlanAction string

const (
	PlanUnchanged PlanAction = "unchanged"
	PlanAppend    PlanAction = "append"
	PlanRebuild   PlanAction = "rebuild"
)

// SourceDescriptor uniquely identifies a discovered local session source file.
type SourceDescriptor struct {
	Kind        string
	Identity    string
	Path        string
	Format      string
	Agent       string
	Compression string
	ThreadID    string
}

// SourcePlan records the planned ingestion action and current file fingerprint.
type SourcePlan struct {
	Source      SourceDescriptor
	Action      PlanAction
	Fingerprint map[string]any
}

// ParsedSource carries session metadata and newly read raw events before DB insertion.
type ParsedSource struct {
	Session         model.AgentSession
	ResetEvents     bool
	NewEvents       []model.AgentEvent
	CursorJSON      string
	FingerprintJSON string
}

// Adapter discovers, plans, parses, and projects one coding agent's local session logs.
type Adapter interface {
	Kind() string
	Agent() string
	ParserVersion() string
	Discover(full bool) ([]SourceDescriptor, error)
	WatchPaths(src SourceDescriptor) []string
	Plan(src SourceDescriptor, cp *model.IndexCheckpoint) (SourcePlan, error)
	Parse(plan SourcePlan, existing *model.AgentSession) (*ParsedSource, error)
	Project(session model.AgentSession, allEvents []model.AgentEvent) (model.AgentSession, model.SessionProjection, error)
}
