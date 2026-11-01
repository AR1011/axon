import { defineConfig, devices } from "@playwright/test";

const playground = (port: number, base: string, churn = "", env: Record<string, string> = {}) => ({
  command: `cargo run -q -p axon-obs --features dashboard,system-metrics --example playground -- 6 ${port} ${base} ${churn}`,
  // A 401 counts as up.
  url: `http://127.0.0.1:${port}${base}api/overview`,
  env,
  cwd: "../../..",
  timeout: 600_000,
  reuseExistingServer: false,
});

/** Runs against the real dashboard: build the UI first, so the playground embeds the current bundle. */
export default defineConfig({
  testDir: "./tests",
  fullyParallel: true,
  retries: process.env.CI ? 1 : 0,
  use: { ...devices["Desktop Chrome"], baseURL: "http://127.0.0.1:8097", viewport: { width: 1280, height: 720 }, trace: "retain-on-failure" },
  webServer: [
    playground(8097, "/"),
    playground(8098, "/ops/axon/", "churn"),
    // login.spec.ts: admin signs in, and a session idles out after 5 s.
    playground(8099, "/", "", { AXON_DASHBOARD_PASSWORD: "correct horse", AXON_DASHBOARD_IDLE: "5" }),
  ],
});
