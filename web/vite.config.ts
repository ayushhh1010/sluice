import { defineConfig } from "vite";
import preact from "@preact/preset-vite";

// In development the API is proxied to a local agent (sluice-dev listens on
// 8080); in production Caddy serves both from one origin.
export default defineConfig({
  plugins: [preact()],
  server: { proxy: { "/api": { target: "http://127.0.0.1:8080", changeOrigin: true } } },
  build: { target: "es2022", sourcemap: true },
});
