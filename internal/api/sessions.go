package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/agent-observatory/agent-observatory-go/internal/model"
	"github.com/agent-observatory/agent-observatory-go/internal/redact"
)

func (s *Server) handleListSessions(w http.ResponseWriter, r *http.Request) {
	limit, err := parseQueryInt(r, "limit", 100, 1, 200)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "bad_request", "invalid limit", false)
		return
	}
	isOngoing, err := parseQueryBool(r, "is_ongoing")
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "bad_request", "invalid is_ongoing", false)
		return
	}
	includeSubPtr, err := parseQueryBool(r, "include_subagents")
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "bad_request", "invalid include_subagents", false)
		return
	}
	includeSub := includeSubPtr != nil && *includeSubPtr

	q := r.URL.Query()
	res, err := s.store.ListSessions(r.Context(), model.SessionListFilter{
		Limit:            limit,
		Cursor:           strings.TrimSpace(q.Get("cursor")),
		Query:            q.Get("q"),
		Agent:            strings.TrimSpace(q.Get("agent")),
		WorkspaceID:      strings.TrimSpace(q.Get("workspace_id")),
		IsOngoing:        isOngoing,
		IncludeSubagents: includeSub,
	})
	if err != nil {
		writeStoreError(w, r, err)
		return
	}
	for i := range res.Sessions {
		res.Sessions[i].AITitle = redact.Ptr(res.Sessions[i].AITitle)
		res.Sessions[i].DisplayTitle = redact.Ptr(res.Sessions[i].DisplayTitle)
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleGetSession(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	detail, err := s.store.GetSession(r.Context(), id)
	if err != nil {
		writeStoreError(w, r, err)
		return
	}
	detail.AITitle = redact.Ptr(detail.AITitle)
	detail.DisplayTitle = redact.Ptr(detail.DisplayTitle)
	writeJSON(w, http.StatusOK, detail)
}

func (s *Server) handleSessionMessages(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	msgs, err := s.store.ListSessionMessages(r.Context(), id)
	if err != nil {
		writeStoreError(w, r, err)
		return
	}
	for i := range msgs {
		msgs[i].Text = redact.Strings(msgs[i].Text)
		msgs[i].MarkdownText = redact.Ptr(msgs[i].MarkdownText)
		msgs[i].CopyText = redact.Strings(msgs[i].CopyText)
	}
	writeJSON(w, http.StatusOK, map[string]any{"messages": msgs})
}

func (s *Server) handleSessionEvents(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	evs, err := s.store.ListSessionEvents(r.Context(), id)
	if err != nil {
		writeStoreError(w, r, err)
		return
	}
	for i := range evs {
		evs[i].RawJSON = redact.JSON(evs[i].RawJSON)
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": evs})
}

func (s *Server) handleSessionTurns(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	turns, err := s.store.ListSessionTurns(r.Context(), id)
	if err != nil {
		writeStoreError(w, r, err)
		return
	}
	for i := range turns {
		turns[i].ErrorMessage = redact.Ptr(turns[i].ErrorMessage)
	}
	writeJSON(w, http.StatusOK, map[string]any{"turns": turns})
}

func (s *Server) handleSessionTools(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	tools, err := s.store.ListSessionTools(r.Context(), id)
	if err != nil {
		writeStoreError(w, r, err)
		return
	}
	for i := range tools {
		tools[i].ArgumentsJSON = redact.JSON(tools[i].ArgumentsJSON)
		tools[i].CommandText = redact.Ptr(tools[i].CommandText)
		tools[i].OutputText = redact.Ptr(tools[i].OutputText)
	}
	writeJSON(w, http.StatusOK, map[string]any{"tools": tools})
}

func (s *Server) handleSessionSearch(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	limit, err := parseQueryInt(r, "limit", 100, 1, 500)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "bad_request", "invalid limit", false)
		return
	}
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	if q == "" {
		if _, err := s.store.GetSession(r.Context(), id); err != nil {
			writeStoreError(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"query": "", "matches": []model.SearchMatch{}})
		return
	}
	matches, err := s.store.SearchSession(r.Context(), id, q, limit)
	if err != nil {
		writeStoreError(w, r, err)
		return
	}
	for i := range matches {
		matches[i].Snippet = redact.Strings(matches[i].Snippet)
	}
	writeJSON(w, http.StatusOK, map[string]any{"query": q, "matches": matches})
}

func (s *Server) handleSessionExport(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	format := strings.TrimSpace(r.URL.Query().Get("format"))
	if format == "" {
		format = "text"
	}
	var ext string
	switch format {
	case "text":
		ext = "txt"
	case "markdown":
		ext = "md"
	case "json":
		ext = "json"
	default:
		writeError(w, r, http.StatusBadRequest, "bad_request", "format must be one of text|markdown|json", false)
		return
	}

	sess, err := s.store.GetSession(r.Context(), id)
	if err != nil {
		writeStoreError(w, r, err)
		return
	}
	events, err := s.store.ListSessionEvents(r.Context(), id)
	if err != nil {
		writeStoreError(w, r, err)
		return
	}

	var content string
	if format == "json" {
		rawList := make([]json.RawMessage, 0, len(events))
		for _, ev := range events {
			rawList = append(rawList, redact.JSON(ev.RawJSON))
		}
		encoded, _ := json.MarshalIndent(rawList, "", "  ")
		content = string(encoded)
	} else {
		msgs, err := s.store.ListSessionMessages(r.Context(), id)
		if err != nil {
			writeStoreError(w, r, err)
			return
		}
		tools, err := s.store.ListSessionTools(r.Context(), id)
		if err != nil {
			writeStoreError(w, r, err)
			return
		}
		items := interleaveTranscript(events, msgs, tools)
		if format == "text" {
			content = renderTranscriptText(sess, items)
		} else {
			content = renderTranscriptMarkdown(sess, items)
		}
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"format":   format,
		"filename": fmt.Sprintf("%s-%s.%s", sess.Agent, sess.ThreadID, ext),
		"content":  content,
	})
}

type transcriptItem struct {
	msg  *model.Message
	tool *model.ToolCall
}

func interleaveTranscript(events []model.AgentEvent, msgs []model.Message, tools []model.ToolCall) []transcriptItem {
	msgByEvent := make(map[int64][]*model.Message)
	var orphanMsgs []*model.Message
	for i := range msgs {
		m := &msgs[i]
		if m.EventID != nil {
			msgByEvent[*m.EventID] = append(msgByEvent[*m.EventID], m)
		} else {
			orphanMsgs = append(orphanMsgs, m)
		}
	}
	toolByEvent := make(map[int64][]*model.ToolCall)
	var orphanTools []*model.ToolCall
	for i := range tools {
		tc := &tools[i]
		if tc.EventID != nil {
			toolByEvent[*tc.EventID] = append(toolByEvent[*tc.EventID], tc)
		} else {
			orphanTools = append(orphanTools, tc)
		}
	}

	items := make([]transcriptItem, 0, len(msgs)+len(tools))
	for _, ev := range events {
		for _, m := range msgByEvent[ev.ID] {
			items = append(items, transcriptItem{msg: m})
		}
		for _, tc := range toolByEvent[ev.ID] {
			items = append(items, transcriptItem{tool: tc})
		}
	}
	for _, m := range orphanMsgs {
		items = append(items, transcriptItem{msg: m})
	}
	for _, tc := range orphanTools {
		items = append(items, transcriptItem{tool: tc})
	}
	return items
}

func roleLabel(m *model.Message) string {
	base := strings.ToUpper(m.Role[:1]) + m.Role[1:]
	switch m.Role {
	case "user":
		base = "User"
	case "assistant":
		base = "Assistant"
	case "system":
		base = "System"
	case "developer":
		base = "Developer"
	}
	if m.Channel == "reasoning" {
		return base + " (reasoning)"
	}
	return base
}

func renderTranscriptText(sess model.SessionDetail, items []transcriptItem) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s session %s\n", strings.ToUpper(sess.Agent[:1])+sess.Agent[1:], sess.ThreadID)
	if sess.Model != nil {
		fmt.Fprintf(&b, "Model: %s\n", *sess.Model)
	}
	if sess.CWD != nil {
		fmt.Fprintf(&b, "cwd: %s\n", *sess.CWD)
	}
	b.WriteByte('\n')
	for _, item := range items {
		if item.msg != nil {
			fmt.Fprintf(&b, "%s:\n%s\n\n", strings.ToUpper(roleLabel(item.msg)), redact.Strings(item.msg.Text))
			continue
		}
		tc := item.tool
		status := ""
		if tc.Status != "" {
			status = " (" + tc.Status + ")"
		}
		fmt.Fprintf(&b, "TOOL %s%s:\n", tc.ToolName, status)
		if tc.CommandText != nil && *tc.CommandText != "" {
			fmt.Fprintf(&b, "$ %s\n", redact.Strings(*tc.CommandText))
		}
		if tc.OutputText != nil && *tc.OutputText != "" {
			fmt.Fprintf(&b, "%s\n", redact.Strings(*tc.OutputText))
		}
		b.WriteByte('\n')
	}
	return strings.TrimSpace(b.String()) + "\n"
}

func renderTranscriptMarkdown(sess model.SessionDetail, items []transcriptItem) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# %s session `%s`\n\n", strings.ToUpper(sess.Agent[:1])+sess.Agent[1:], sess.ThreadID)
	if sess.Model != nil {
		fmt.Fprintf(&b, "- **Model:** %s\n", *sess.Model)
	}
	if sess.CWD != nil {
		fmt.Fprintf(&b, "- **cwd:** `%s`\n", *sess.CWD)
	}
	fmt.Fprintf(&b, "- **Turns:** %d\n", sess.TurnCount)
	fmt.Fprintf(&b, "- **Total tokens:** %d\n", sess.TotalTokens)
	ongoing := "no"
	if sess.IsOngoing {
		ongoing = "yes"
	}
	fmt.Fprintf(&b, "- **Ongoing:** %s\n\n", ongoing)
	for _, item := range items {
		if item.msg != nil {
			heading := "## "
			if item.msg.Channel == "reasoning" {
				heading = "### "
			}
			fmt.Fprintf(&b, "%s%s\n\n%s\n\n", heading, roleLabel(item.msg), redact.Strings(item.msg.Text))
			continue
		}
		tc := item.tool
		status := ""
		if tc.Status != "" {
			status = " — " + tc.Status
		}
		fmt.Fprintf(&b, "#### Tool: `%s`%s\n\n", tc.ToolName, status)
		if tc.CommandText != nil && *tc.CommandText != "" {
			fmt.Fprintf(&b, "```\n%s\n```\n", redact.Strings(*tc.CommandText))
		}
		if tc.OutputText != nil && *tc.OutputText != "" {
			fmt.Fprintf(&b, "```\n%s\n```\n", redact.Strings(*tc.OutputText))
		}
		b.WriteByte('\n')
	}
	return strings.TrimSpace(b.String()) + "\n"
}
