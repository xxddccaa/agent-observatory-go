// Session *history*: read-only, indexed rollouts from agent session stores.
// Every helper here is GET-only: history is never mutated from the UI.
import { get } from "./client";

export interface HistorySession {
  id: string;
  agent?: string | null;
  is_subagent?: boolean;
  parent_session_id?: string | null;
  thread_id: string;
  rollout_path: string;
  cwd: string | null;
  /** Durable SQLite workspace relation populated by the indexer. */
  workspace_id: string | null;
  ai_title: string | null;
  /** Optional projections supplied by newer indexers. */
  display_title?: string | null;
  title?: string | null;
  source_kind?: string | null;
  first_user_message?: string | null;
  first_message?: string | null;
  model: string | null;
  turn_count: number;
  total_tokens: number;
  is_ongoing: boolean;
  index_status: string;
  /** ISO timestamp of the last activity; used for list ordering + display. */
  activity_at?: string | null;
  /** Legacy index/update timestamp retained for backwards compatibility. */
  updated_at: string | null;
  /** Workspace semantic classification supplied by the indexer. */
  workspace_kind?: WorkspaceKind | null;
  workspace_confidence?: string | null;
}

export type WorkspaceKind =
  | "persistent_project"
  | "ephemeral_task"
  | "test_fixture"
  | "system_directory"
  | "deleted_path"
  | "unknown";

export interface HistoryDetail {
  id: string;
  agent?: string | null;
  source_kind?: string | null;
  source_path?: string | null;
  thread_id: string;
  rollout_path: string;
  cwd: string | null;
  model: string | null;
  turn_count: number;
  total_tokens: number;
  is_ongoing: boolean;
  updated_at?: string | null;
}

/** Normalized session message author. */
export type MessageRole = "user" | "assistant" | "system" | "developer";
/** "final" = a reply shown to the user; "reasoning" = a private reasoning summary. */
export type MessageChannel = "final" | "reasoning";

export interface HistoryMessage {
  id: string;
  role: MessageRole;
  channel: MessageChannel;
  text: string;
  markdown_text: string;
  copy_text: string;
  sequence: number;
  timestamp: string | null;
  turn_id: string | null;
}

export interface HistoryEvent {
  event_index: number;
  line_number: number;
  byte_offset: number;
  normalized_type: string;
  top_level_type: string | null;
  payload_type: string | null;
  is_known: boolean;
  raw_json: unknown;
  event_timestamp: string | null;
}

export interface HistoryTool {
  id: string;
  call_id: string;
  tool_name: string;
  tool_kind: string;
  command_text: string | null;
  arguments_json: unknown;
  output_text: string | null;
  exit_code: number | null;
  status: string;
  is_mutating: boolean;
  started_at: string | null;
}

export type ExportFormat = "text" | "markdown" | "json";

export interface SessionExport {
  format: ExportFormat;
  filename: string;
  content: string;
}

export type SearchMatchKind = "message" | "tool";

export interface SearchMatch {
  kind: SearchMatchKind;
  id: string;
  ref: string;
  snippet: string;
}

export interface SessionSearch {
  query: string;
  matches: SearchMatch[];
}

export interface HistoryList {
  sessions: HistorySession[];
  next_cursor: string | null;
}

/**
 * Resolve the timestamp shown in history.
 *
 * New API responses always include ``activity_at`` (possibly null).  In that
 * case a null value is intentional: the source did not provide a trustworthy
 * event timestamp, so we must not substitute ``updated_at`` (which is the
 * database/index update time and is what caused old sessions to all appear as
 * "just now").  Only responses from an older API that omit ``activity_at``
 * entirely are allowed to fall back to ``updated_at`` for compatibility.
 */
export function sessionActivityAt(
  session: Pick<HistorySession, "activity_at" | "updated_at">,
): string | null {
  if (session.activity_at !== undefined) return session.activity_at || null;
  return session.updated_at || null;
}

export function listHistory(options: {
  limit?: number;
  cursor?: string | null;
  q?: string;
  isOngoing?: boolean;
  agent?: string;
  includeSubagents?: boolean;
} = {}): Promise<HistoryList> {
  const params = new URLSearchParams();
  if (options.limit) params.set("limit", String(options.limit));
  if (options.cursor) params.set("cursor", options.cursor);
  if (options.q) params.set("q", options.q);
  if (options.agent) params.set("agent", options.agent);
  if (options.includeSubagents) params.set("include_subagents", "true");
  if (options.isOngoing !== undefined) {
    params.set("is_ongoing", String(options.isOngoing));
  }
  const suffix = params.size ? `?${params.toString()}` : "";
  return get<HistoryList>(`/api/sessions${suffix}`);
}

export function getHistory(id: string): Promise<HistoryDetail> {
  return get<HistoryDetail>(`/api/sessions/${encodeURIComponent(id)}`);
}

export function getMessages(id: string): Promise<{ messages: HistoryMessage[] }> {
  return get(`/api/sessions/${encodeURIComponent(id)}/messages`);
}

export function getEvents(id: string): Promise<{ events: HistoryEvent[] }> {
  return get(`/api/sessions/${encodeURIComponent(id)}/events`);
}

export function getTools(id: string): Promise<{ tools: HistoryTool[] }> {
  return get(`/api/sessions/${encodeURIComponent(id)}/tools`);
}

/** Whole-session export rendered by the backend as plain text, Markdown, or JSON. */
export function exportSession(id: string, format: ExportFormat): Promise<SessionExport> {
  return get<SessionExport>(
    `/api/sessions/${encodeURIComponent(id)}/export?format=${encodeURIComponent(format)}`
  );
}

/** Full-text search across a session's indexed messages and tool calls. */
export function searchSession(id: string, q: string): Promise<SessionSearch> {
  return get<SessionSearch>(
    `/api/sessions/${encodeURIComponent(id)}/search?q=${encodeURIComponent(q)}`
  );
}

export type SessionStatusTone = "ok" | "warn" | "danger" | "muted";

export interface SessionStatus {
  label: string;
  tone: SessionStatusTone;
}

const FAILED_INDEX = new Set(["failed", "error"]);

/**
 * Session lifecycle status, distinct from the indexing `index_status`:
 * "ongoing" while the rollout is still live, "failed" when indexing errored,
 * otherwise "ended".
 */
export function sessionStatus(
  session: Pick<HistorySession, "is_ongoing" | "index_status">
): SessionStatus {
  if (session.is_ongoing) return { label: "ongoing", tone: "ok" };
  if (FAILED_INDEX.has(session.index_status)) return { label: "failed", tone: "danger" };
  return { label: "ended", tone: "muted" };
}

/** Resolve a stable, human-readable title while keeping compatibility with old responses. */
export function sessionTitle(session: Pick<HistorySession, "thread_id" | "ai_title"> & {
  display_title?: string | null;
  title?: string | null;
  first_user_message?: string | null;
  first_message?: string | null;
}): string {
  const candidate =
    session.display_title ||
    session.title ||
    session.first_user_message ||
    session.first_message ||
    session.ai_title;
  const normalized = candidate?.replace(/\s+/g, " ").trim();
  return normalized || "Untitled session";
}
