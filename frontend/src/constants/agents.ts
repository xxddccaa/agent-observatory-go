export const AGENT_OPTIONS = [
  { value: "", label: "All agents" },
  { value: "codex", label: "Codex" },
  { value: "claude", label: "Claude" },
  { value: "agy", label: "AGY" },
  { value: "gemini", label: "Gemini CLI" },
  { value: "generic", label: "Generic" },
] as const;

export type AgentFilter = (typeof AGENT_OPTIONS)[number]["value"];

export function agentLabel(agent: string | null | undefined): string {
  const normalized = agent?.trim().toLowerCase();
  if (!normalized) return "Unknown";
  return AGENT_OPTIONS.find((option) => option.value === normalized)?.label ?? agent!.trim();
}
