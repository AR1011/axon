import { createHash } from "node:crypto";
import { readdirSync, readFileSync } from "node:fs";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig, type Plugin } from "vitest/config";

// A digest of the sources in the page, so every source change changes dist, even one that builds to the same bundle
// (a type), and check.sh's dist step, which wants dist committed no earlier than src, holds for it too.
const sources: Plugin = {
  name: "sources",
  transformIndexHtml: {
    order: "pre",
    handler(html) {
      const files = readdirSync("src", { recursive: true, withFileTypes: true }).filter((f) => f.isFile() && !f.name.startsWith("."));
      const digest = createHash("sha256").update(html);
      for (const path of files.map((f) => `${f.parentPath}/${f.name}`).sort()) {
        digest.update(path).update(readFileSync(path));
      }
      return html.replace("</head>", `  <meta name="sources" content="${digest.digest("hex").slice(0, 16)}" />\n  </head>`);
    },
  },
};

// Every page draws charts, but only once its chunk runs and a chart mounts: fetched alongside the index chunk instead.
const preloadCharts: Plugin = {
  name: "preload-charts",
  transformIndexHtml: {
    order: "post",
    handler: (_, { bundle }) =>
      Object.keys(bundle ?? {})
        .filter((file) => /^assets\/charts-.*\.js$/.test(file))
        .map((file) => ({ tag: "link", attrs: { rel: "modulepreload", crossorigin: true, href: `./${file}` }, injectTo: "head" as const })),
  },
};

export default defineConfig({
  base: "./",
  plugins: [react(), tailwindcss(), sources, preloadCharts],
  server: { proxy: { "/api": process.env.AXON_DASHBOARD_API ?? "http://127.0.0.1:8080" } },
  build: {
    sourcemap: false,
    assetsInlineLimit: 0,
    rollupOptions: { output: { manualChunks: (id) => (/node_modules\/(echarts|zrender)\//.test(id) ? "charts" : undefined) } },
  },
  test: { include: ["src/**/*.test.ts"] },
});
