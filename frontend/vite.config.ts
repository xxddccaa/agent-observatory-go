import { defineConfig } from "vite";
import vue from "@vitejs/plugin-vue";

// One proxy definition shared by `vite dev` (server) and `vite preview` so a
// production-style build served by preview reaches the same API as dev.
// `ws: true` forwards the terminal WebSocket at
// /api/managed-sessions/:id/terminal/ws.
const apiProxy = {
  "/api": {
    target: "http://127.0.0.1:8125",
    changeOrigin: true,
    ws: true,
  },
};

export default defineConfig({
  plugins: [vue()],
  build: {
    outDir: "../internal/webui/dist",
    emptyOutDir: true,
  },
  server: {
    host: "127.0.0.1",
    port: 5173,
    proxy: apiProxy,
  },
  preview: {
    host: "127.0.0.1",
    port: 5174,
    proxy: apiProxy,
  },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: ["./src/__tests__/setup.ts"],
    exclude: ["node_modules", "dist", "tests"],
  },
});
