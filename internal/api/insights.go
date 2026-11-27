package api

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/agent-observatory/agent-observatory-go/internal/model"
	"github.com/agent-observatory/agent-observatory-go/internal/redact"
)

func parseInsightFilters(r *http.Request) (model.InsightFilters, error) {
	q := r.URL.Query()
	until := time.Now().UTC()
	if rawTo := strings.TrimSpace(q.Get("to")); rawTo != "" {
		parsed, err := model.ParseUTC(rawTo)
		if err != nil {
			return model.InsightFilters{}, fmt.Errorf("invalid to timestamp")
		}
		until = parsed
	}
	since := until.AddDate(0, 0, -30)
	if rawFrom := strings.TrimSpace(q.Get("from")); rawFrom != "" {
		parsed, err := model.ParseUTC(rawFrom)
		if err != nil {
			return model.InsightFilters{}, fmt.Errorf("invalid from timestamp")
		}
		since = parsed
	}
	if !since.Before(until) {
		return model.InsightFilters{}, fmt.Errorf("from must be earlier than to")
	}
	return model.InsightFilters{
		Since:       since,
		Until:       until,
		WorkspaceID: strings.TrimSpace(q.Get("workspace_id")),
		Model:       strings.TrimSpace(q.Get("model")),
		Agent:       strings.TrimSpace(q.Get("agent")),
	}, nil
}

func (s *Server) handleInsightsOverview(w http.ResponseWriter, r *http.Request) {
	filters, err := parseInsightFilters(r)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "bad_request", err.Error(), false)
		return
	}
	res, err := s.store.GetInsightsOverview(r.Context(), filters)
	if err != nil {
		writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleInsightsTimeseries(w http.ResponseWriter, r *http.Request) {
	filters, err := parseInsightFilters(r)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "bad_request", err.Error(), false)
		return
	}
	res, err := s.store.GetInsightsTimeseries(r.Context(), filters)
	if err != nil {
		writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleInsightsLatency(w http.ResponseWriter, r *http.Request) {
	filters, err := parseInsightFilters(r)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "bad_request", err.Error(), false)
		return
	}
	res, err := s.store.GetInsightsLatency(r.Context(), filters)
	if err != nil {
		writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleInsightsTools(w http.ResponseWriter, r *http.Request) {
	filters, err := parseInsightFilters(r)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "bad_request", err.Error(), false)
		return
	}
	q := r.URL.Query()
	res, err := s.store.GetInsightsTools(r.Context(), filters, strings.TrimSpace(q.Get("tool")), strings.TrimSpace(q.Get("status")))
	if err != nil {
		writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleInsightsSkills(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"items": []any{}})
}

func (s *Server) handleInsightsAgents(w http.ResponseWriter, r *http.Request) {
	filters, err := parseInsightFilters(r)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "bad_request", err.Error(), false)
		return
	}
	res, err := s.store.GetInsightsAgents(r.Context(), filters)
	if err != nil {
		writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleInsightsSources(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	res, err := s.store.GetInsightsSources(
		r.Context(),
		strings.TrimSpace(q.Get("agent")),
		strings.TrimSpace(q.Get("source")),
		strings.TrimSpace(q.Get("status")),
	)
	if err != nil {
		writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleSessionTimeline(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	limit, err := parseQueryInt(r, "limit", 500, 1, 2000)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "bad_request", "invalid limit", false)
		return
	}
	res, err := s.store.GetSessionTimeline(r.Context(), id, limit)
	if err != nil {
		writeStoreError(w, r, err)
		return
	}
	for i := range res.Spans {
		res.Spans[i].Name = redact.Ptr(res.Spans[i].Name)
		res.Spans[i].MetadataJSON = redact.JSON(res.Spans[i].MetadataJSON)
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleListEvidence(w http.ResponseWriter, r *http.Request) {
	filters, err := parseInsightFilters(r)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "bad_request", err.Error(), false)
		return
	}
	limit, err := parseQueryInt(r, "limit", 100, 1, 200)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "bad_request", "invalid limit", false)
		return
	}
	q := r.URL.Query()
	res, err := s.store.ListEvidence(
		r.Context(),
		filters,
		strings.TrimSpace(q.Get("source_id")),
		strings.TrimSpace(q.Get("category")),
		strings.TrimSpace(q.Get("level")),
		strings.TrimSpace(q.Get("thread_id")),
		limit,
	)
	if err != nil {
		writeStoreError(w, r, err)
		return
	}
	for i := range res.Items {
		res.Items[i].Target = redact.Ptr(res.Items[i].Target)
		res.Items[i].Summary = redact.Ptr(res.Items[i].Summary)
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleGetEvidence(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rec, err := s.store.GetEvidence(r.Context(), id)
	if err != nil {
		writeStoreError(w, r, err)
		return
	}
	rec.Target = redact.Ptr(rec.Target)
	rec.Summary = redact.Ptr(rec.Summary)
	rec.RawText = redact.Ptr(rec.RawText)
	rec.RawJSON = redact.JSON(rec.RawJSON)
	rec.SourceLocatorJSON = redact.JSON(rec.SourceLocatorJSON)
	rec.MetadataJSON = redact.JSON(rec.MetadataJSON)
	writeJSON(w, http.StatusOK, rec)
}

func (s *Server) handleInsightsExport(w http.ResponseWriter, r *http.Request) {
	format := strings.TrimSpace(r.URL.Query().Get("format"))
	if format == "" {
		format = "json"
	}
	if format != "csv" && format != "json" {
		writeError(w, r, http.StatusBadRequest, "bad_request", "format must be csv or json", false)
		return
	}
	filters, err := parseInsightFilters(r)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "bad_request", err.Error(), false)
		return
	}
	overview, err := s.store.GetInsightsOverview(r.Context(), filters)
	if err != nil {
		writeStoreError(w, r, err)
		return
	}
	timeseries, err := s.store.GetInsightsTimeseries(r.Context(), filters)
	if err != nil {
		writeStoreError(w, r, err)
		return
	}
	tools, err := s.store.GetInsightsTools(r.Context(), filters, "", "")
	if err != nil {
		writeStoreError(w, r, err)
		return
	}

	payload := map[string]any{
		"overview":   overview,
		"timeseries": timeseries,
		"tools":      tools,
	}
	stamp := filters.Until.UTC().Format("20060102T150405Z")
	if format == "csv" {
		csvText := renderInsightsCSV(payload)
		writeJSON(w, http.StatusOK, map[string]any{
			"format":     "csv",
			"filename":   fmt.Sprintf("agent-observatory-insights-%s.csv", stamp),
			"media_type": "text/csv",
			"content":    redact.Strings(csvText),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"format":     "json",
		"filename":   fmt.Sprintf("agent-observatory-insights-%s.json", stamp),
		"media_type": "application/json",
		"content":    payload,
	})
}

func renderInsightsCSV(payload map[string]any) string {
	rawBytes, _ := json.Marshal(payload)
	var normalized map[string]any
	_ = json.Unmarshal(rawBytes, &normalized)

	var buf bytes.Buffer
	cw := csv.NewWriter(&buf)
	_ = cw.Write([]string{"dataset", "key", "value"})

	if ov, ok := normalized["overview"]; ok {
		for _, pair := range flattenKV("", ov) {
			_ = cw.Write([]string{"overview", pair[0], pair[1]})
		}
	}
	if ts, ok := normalized["timeseries"].(map[string]any); ok {
		if pts, ok := ts["points"].([]any); ok {
			for i, pt := range pts {
				for _, pair := range flattenKV("", pt) {
					_ = cw.Write([]string{"timeseries", fmt.Sprintf("%d.%s", i, pair[0]), pair[1]})
				}
			}
		}
	}
	if tools, ok := normalized["tools"].(map[string]any); ok {
		if items, ok := tools["items"].([]any); ok {
			for i, it := range items {
				for _, pair := range flattenKV("", it) {
					_ = cw.Write([]string{"tools", fmt.Sprintf("%d.%s", i, pair[0]), pair[1]})
				}
			}
		}
	}
	cw.Flush()
	return buf.String()
}

func flattenKV(prefix string, v any) [][2]string {
	switch val := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var out [][2]string
		for _, k := range keys {
			next := k
			if prefix != "" {
				next = prefix + "." + k
			}
			out = append(out, flattenKV(next, val[k])...)
		}
		return out
	default:
		return [][2]string{{prefix, fmt.Sprint(val)}}
	}
}
