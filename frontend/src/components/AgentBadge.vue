<template>
  <span class="agent-badge" :data-agent="normalized">{{ agentLabel(agent) }}</span>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { agentLabel } from "../constants/agents";

const props = defineProps<{ agent: string }>();
const normalized = computed(() => props.agent.trim().toLowerCase() || "generic");
</script>

<style scoped>
.agent-badge {
  display: inline-flex;
  align-items: center;
  max-width: 100%;
  padding: 1px 8px;
  border: 1px solid color-mix(in srgb, var(--accent) 42%, var(--border));
  border-radius: 999px;
  background: var(--accent-weak);
  color: var(--accent-strong);
  font-size: 11px;
  font-weight: 600;
  line-height: 1.5;
  text-transform: none;
  white-space: nowrap;
}

.agent-badge[data-agent="claude"] {
  border-color: color-mix(in srgb, #d99a6c 50%, var(--border));
  background: color-mix(in srgb, #d99a6c 13%, transparent);
  color: #e9ad80;
}

.agent-badge[data-agent="agy"] {
  border-color: color-mix(in srgb, #8fc97a 48%, var(--border));
  background: color-mix(in srgb, #8fc97a 12%, transparent);
  color: #a6d993;
}

.agent-badge[data-agent="gemini"] {
  border-color: color-mix(in srgb, #4da3ff 48%, var(--border));
  background: color-mix(in srgb, #4da3ff 12%, transparent);
  color: #8cc2ff;
}

.agent-badge[data-agent="generic"] {
  border-color: var(--border);
  background: var(--panel-2);
  color: var(--fg-muted);
}
</style>
