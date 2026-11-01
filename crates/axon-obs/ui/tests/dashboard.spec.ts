import { expect, test, type Page } from "@playwright/test";

/** The second playground: mounted under a base path, with a node that keeps joining and leaving. */
const MOUNTED = "http://127.0.0.1:8098/ops/axon/";
const ROUTES = ["overview", "cluster", "placement", "overview/topology", "nodes", "nodes/relay-0", "actors", "workload", "connections", "connections/console-0/publisher-1/out/control", "streams", "traces", "events"];

/** The top bar's status chip: its name starts with the connection's state and says when the view last moved. */
const status = (page: Page, state = "Connected") => page.getByRole("button", { name: new RegExp(`^${state}(, updated|$)`) });

async function live(page: Page, path: string) {
  const errors: string[] = [];
  page.on("pageerror", (e) => errors.push(e.message));
  page.on("console", (m) => m.type() === "error" && errors.push(m.text()));
  await page.goto(path);
  await expect(status(page)).toBeVisible();
  return errors;
}

/** Polls only, the live stream refused, each overview answer passed through `rewrite`. */
async function polled(page: Page, rewrite: (body: { nodes: { node: { id: string }; error?: string }[] }) => unknown) {
  await page.route(/\/api\/events(\?|$)/, (route) => route.abort());
  await page.route(/\/api\/overview(\?|$)/, async (route) => {
    const response = await route.fetch();
    await route.fulfill({ response, json: rewrite(await response.json()) });
  });
}

const transform = (page: Page) => page.locator("[data-scene]").getAttribute("transform");
const edges = (page: Page, attribute: "data-edge" | "data-value") => page.locator("[data-edge]").evaluateAll((all, a) => all.map((e) => e.getAttribute(a)).sort().join(), attribute);

test("every route fits the desktop viewport, names itself, and has no native select", async ({ page }) => {
  for (const route of ROUTES) {
    const errors = await live(page, `/${route}`);
    const size = await page.evaluate(() => [document.documentElement.scrollWidth, document.documentElement.scrollHeight]);
    expect(size, route).toEqual([1280, 720]);
    await expect(page.locator("h1"), route).toHaveCount(1);
    expect(await page.locator("select").count(), route).toBe(0);
    await expect(page.getByLabel("Cluster summary"), route).toHaveCount(["overview", "nodes", "actors"].includes(route) ? 1 : 0);
    expect(errors, route).toEqual([]);
  }
  await page.goto("/");
  await expect(page).toHaveURL(/\/overview$/);
  await page.goto("/nowhere");
  await expect(page.getByText("Page not found")).toBeVisible();
  await page.goto("/actors/only-a-node");
  await expect(page.getByText("Actor link is incomplete")).toBeVisible();
  await page.goto("/topology?node=relay-1&layout=grid");
  await expect(page, "an old topology link opens the overview's topology full size, its selection kept").toHaveURL(/\/overview\/topology\?node=relay-1&layout=grid$/);
});

test("the sidebar groups its pages, and the signed-in user's way out sits at its foot", async ({ page }) => {
  await live(page, "/overview");
  const nav = page.getByRole("navigation", { name: "Main navigation" });
  const groups = { Cluster: ["cluster", "nodes", "connections", "placement"], Actors: ["actors", "workload", "dead letters"], Investigate: ["events", "traces", "explore"] };
  for (const [group, links] of Object.entries(groups)) {
    const box = nav.getByRole("group", { name: group });
    await expect(box.getByRole("heading", { name: group })).toBeVisible();
    expect((await box.getByRole("link").allInnerTexts()).map((t) => t.replace(/\s*\d+$/, "").toLowerCase()), group).toEqual(links);
  }
  for (const top of ["overview", "streams"]) await expect(nav.getByRole("link", { name: top, exact: true })).toBeVisible();
  await expect(nav.getByRole("link", { name: "topology" }), "topology is the overview's panel now").toHaveCount(0);
  expect(await nav.getByRole("link").count()).toBe(12);
  // The console under test has no users: no account, and nothing left over in the top bar.
  await expect(page.getByRole("button", { name: "Sign out" })).toHaveCount(0);
  await page.getByRole("button", { name: "Collapse sidebar" }).click();
  await expect(nav.getByRole("group", { name: "Cluster" }), "collapsed, a group keeps its name").toBeAttached();
  await page.getByRole("button", { name: "Expand sidebar" }).click();
});

test("the status chip opens on the connection, the history range and the store, from the keyboard too", async ({ page }) => {
  await live(page, "/nodes");
  const chip = status(page);
  await expect(chip).toHaveAttribute("aria-expanded", "false");
  expect((await chip.boundingBox())!.height, "one compact chip, not a row").toBeLessThanOrEqual(26);
  await expect(page.getByRole("region", { name: "Status bar" }), "the status row is gone").toHaveCount(0);
  await chip.focus();
  await page.keyboard.press("Enter");
  const dialog = page.getByRole("dialog", { name: "Console status" });
  await expect(dialog).toBeFocused();
  await expect(chip).toHaveAttribute("aria-expanded", "true");
  await expect(dialog).toContainText(/Connection\s*Live stream/);
  await expect(dialog).toContainText(/Updated\s*\d+s ago/);
  await expect(dialog).toContainText(/History from\s*\d+[smh] ago/, { timeout: 15_000 });
  await expect(dialog.getByRole("radiogroup", { name: "History source" })).toContainText("In-memory ring");
  await page.keyboard.press("Escape");
  await expect(dialog).toHaveCount(0);
  await expect(chip).toBeFocused();
  // A view that stops moving says so on the chip itself.
  await page.route("**/api/events", (route) => route.abort());
  await page.route("**/api/overview", (route) => route.abort("connectionrefused"));
  await page.reload();
  await expect(page.getByRole("button", { name: /^(Connecting|Stale view)/ })).toBeVisible();
});

test("populated narrow views never overflow sideways", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  for (const route of ROUTES) {
    await live(page, `/${route}`);
    expect(await page.evaluate(() => document.documentElement.scrollWidth), route).toBeLessThanOrEqual(390);
    await expect(page.getByRole("navigation", { name: "Main navigation" })).toBeVisible();
  }
  await live(page, "/cluster");
  const table = page.getByLabel("Nodes by subsystem");
  await expect(table.locator("tbody tr").first()).toBeVisible();
  const box = (await table.boundingBox())!;
  expect(box.x + box.width, "the cluster table stays inside the page").toBeLessThanOrEqual(390);
  expect(await table.evaluate((e) => e.querySelector("table")!.scrollWidth - e.clientWidth), "and its columns fit inside it").toBeLessThanOrEqual(1);
});

type Shifted = { shift: number; moved: string[] };
for (const route of ROUTES)
  test(`${route} does not move its layout as it loads and updates, or carry prose outside its help`, async ({ page }) => {
    await page.addInitScript(() => {
      const w = window as unknown as Shifted;
      w.shift = 0;
      w.moved = [];
      new PerformanceObserver((list) => {
        for (const e of list.getEntries() as unknown as { value: number; hadRecentInput: boolean; sources: { node?: Node }[] }[]) {
          if (e.hadRecentInput) continue;
          w.shift += e.value;
          for (const s of e.sources) w.moved.push(s.node instanceof Element ? s.node.outerHTML.slice(0, 120) : String(s.node?.nodeName));
        }
      }).observe({ type: "layout-shift", buffered: true });
    });
    await live(page, `/${route}`);
    // A few live updates, and the late answers a page fills in.
    await page.waitForTimeout(3000);
    const { shift, moved } = await page.evaluate(() => {
      const w = window as unknown as Shifted;
      return { shift: w.shift, moved: w.moved };
    });
    expect.soft(shift, `moved: ${moved.join("\n")}`).toBeLessThan(0.02);
    const prose = await page.evaluate(() =>
      [...document.querySelectorAll("body *")]
        .filter((e) => (e as HTMLElement).offsetParent !== null && !e.closest('[role="dialog"], [role="tooltip"], title, table, pre, code'))
        .map((e) => [...e.childNodes].filter((n) => n.nodeType === Node.TEXT_NODE).map((n) => n.textContent).join(" ").trim())
        .filter((text) => text.split(/\s+/).length > 20),
    );
    expect.soft(prose, "explanation goes in the help dialog or a tooltip").toEqual([]);
  });

test("selection lives in the URL and survives a reload", async ({ page }) => {
  await live(page, "/overview");
  await page.getByLabel("Node inspector").getByText("Selected node").waitFor();
  await page.getByRole("button", { name: /relay-1/ }).first().click();
  await expect(page).toHaveURL(/node=relay-1/);
  await page.getByLabel("Search nodes").fill("publisher");
  await expect(page.getByLabel("Node inspector").getByRole("heading", { name: "relay-1", exact: true })).toBeVisible();
  await page.reload();
  await expect(page.getByLabel("Node inspector").getByRole("heading", { name: "relay-1", exact: true })).toBeVisible();
  await expect(page.getByLabel("Search nodes")).toHaveValue("publisher");
  await page.getByRole("tab", { name: /actors/ }).click();
  await expect(page).toHaveURL(/tab=actors/);
  await expect(page.getByLabel("Actor inventory")).toBeVisible();
  await page.getByRole("link", { name: /^nodes/ }).click();
  await expect(page).toHaveURL(/\/nodes$/);
  await expect(page.getByLabel("Node inspector")).toHaveCount(0);
});

test("the topology camera moves only when asked", async ({ page }) => {
  await live(page, "/overview/topology");
  const workspace = (await page.locator("main").boundingBox())!;
  const graph = (await page.getByLabel("Topology", { exact: true }).boundingBox())!;
  const rail = (await page.getByLabel("Node inspector").boundingBox())!;
  expect([graph.x, graph.y, graph.height, graph.width + rail.width + 1]).toEqual([workspace.x, workspace.y, workspace.height, workspace.width]);
  await expect(page.getByText("No session selected")).toBeVisible();
  await expect(page.locator("[data-node]")).toHaveCount(9);
  // Sessions keep forming after the nodes appear, and each new one refits the camera, as it should.
  await expect.poll(async () => { const before = await edges(page, "data-edge"); await page.waitForTimeout(2500); return !!before && before === (await edges(page, "data-edge")); }, { timeout: 20_000 }).toBe(true);
  // A session can still come or go while we wait; that attempt proves nothing, so it is made again.
  let fitted: string | null, zoomed: string | null;
  for (let attempt = 1; ; attempt++) {
    await page.getByRole("button", { name: "Fit" }).click();
    const sessions = await edges(page, "data-edge");
    fitted = await transform(page);
    await page.getByRole("button", { name: "Zoom in" }).click();
    zoomed = await transform(page);
    expect(zoomed).not.toBe(fitted);
    const readings = await edges(page, "data-value");
    await page.waitForTimeout(2500);
    const camera = await transform(page);
    const after = await edges(page, "data-edge");
    if (after !== sessions && attempt < 5) continue;
    expect(await edges(page, "data-value"), "telemetry refreshed").not.toBe(readings);
    expect(after, "no session came or went").toBe(sessions);
    expect(camera, "a telemetry refresh leaves the camera alone").toBe(zoomed);
    break;
  }

  const box = (await page.locator(".dotgrid").boundingBox())!;
  await page.mouse.move(box.x + 30, box.y + 30);
  await page.mouse.down();
  await page.mouse.move(box.x + 130, box.y + 90);
  await page.mouse.up();
  expect(await transform(page)).not.toBe(zoomed);
  await page.getByRole("button", { name: "Fit" }).click();
  expect(await transform(page)).toBe(fitted);

  const node = page.locator('[data-node="relay-1"]');
  await node.focus();
  await page.keyboard.press("Enter");
  await expect(node).toHaveAttribute("aria-pressed", "true");
  await page.getByRole("button", { name: "Focus" }).click();
  expect(await transform(page)).toContain("scale(1.25)");

  const edge = page.locator("[data-edge]").first();
  await edge.focus();
  await page.keyboard.press("Space");
  await expect(page).toHaveURL(/conn=/);
  await expect(page.getByLabel("Node inspector").getByRole("link", { name: /Open connection/ })).toBeVisible();

  for (const layout of ["Grouped by role", "Circular", "Grid", "Force directed"]) {
    await page.getByRole("button", { name: layout }).click();
    await expect(page.locator("[data-node]")).toHaveCount(9);
  }
  await expect(page).toHaveURL(/layout=force/);
  await page.getByRole("button", { name: "Linear" }).focus();
  await page.keyboard.press("ArrowRight");
  await expect(page.getByRole("button", { name: "Log", exact: true })).toHaveAttribute("aria-pressed", "true");
  const traffic = await edge.getAttribute("data-value");
  await page.getByRole("button", { name: "Round trip", exact: true }).click();
  await expect(edge).not.toHaveAttribute("data-value", traffic!);
  await page.getByRole("button", { name: "Selected + peers" }).click();
  expect(await page.locator("[data-node]").count()).toBeLessThanOrEqual(9);
});

test("the overview topology toggles labels and opens the full page", async ({ page }) => {
  await live(page, "/overview");
  await expect(page.locator("[data-edge]").first()).toBeAttached();
  await expect(page.locator("text.edge-label"), "labels start off: at overview zoom they pile up").toHaveCount(0);
  await page.getByRole("button", { name: "Edge labels" }).click();
  await expect(page.locator("text.edge-label").first()).toBeVisible();
  await page.getByRole("link", { name: "Open full topology" }).click();
  await expect(page).toHaveURL(/\/overview\/topology\?.*node=/);
  await expect(page.locator("main").getByRole("group", { name: "Layout" })).toBeVisible();
  await page.getByRole("link", { name: "Back to the overview" }).click();
  await expect(page).toHaveURL(/\/overview\?.*node=/);
  await expect(page.getByRole("tab", { name: /^attention/ })).toBeVisible();
});

test("node tables sort, rows navigate, and history pauses off the chart tabs", async ({ page }) => {
  await live(page, "/nodes");
  await page.getByRole("button", { name: /^Name/ }).click();
  await expect(page.locator("th").first()).toHaveAttribute("aria-sort", "descending");
  await expect(page.locator("tbody tr").first()).toContainText("relay-");
  const relay = page.locator("tbody tr", { hasText: "relay-0" }).locator("td");
  await expect(relay.nth(3), "its readyz checks").toHaveText("ready", { timeout: 30_000 });
  await expect(relay.nth(8), "the wire versions it speaks").toHaveText("7");
  await expect(page.getByRole("heading", { name: /^Nodes \d+ ready$/ })).toBeVisible();
  await page.locator("tbody tr", { hasText: "publisher-1" }).locator("td").nth(4).click();
  await expect(page).toHaveURL(/\/nodes\/publisher-1/);

  const history: string[] = [];
  page.on("request", (r) => r.url().includes("/api/history") && history.push(r.url()));
  await page.reload();
  await expect(page.getByRole("img", { name: /Process CPU/ })).toBeVisible();
  await expect(page.getByRole("img", { name: /^worker 0 \d+%/ }), "the shared pool keeps its strip").toBeVisible();
  await expect(page.getByRole("img", { name: /^relays-0 \d+%, relays-1 \d+%$/ }), "a named pool gets its own").toBeVisible();
  expect(new Set(history.map((url) => new URL(url).searchParams.get("metrics"))).size, "one request carries the whole page of charts").toBe(1);
  expect(history[0]).toContain("node_id=publisher-1");
  expect(history[0]).toContain("process.cpu_percent");
  const query = new URL(history[0]).searchParams;
  expect(history[0], "the distributions tab asks for its own").not.toContain("cluster.request_rtt");
  expect([query.get("sum"), query.get("by")], "added up per node boot, a named pool apart").toEqual(["node", "pool"]);
  const step = Number(query.get("step_ms"));
  expect(Number(query.get("to_ms")) + 1 - Number(query.get("from_ms"))).toBe(Math.ceil(900_000 / step) * step);
  expect((Number(query.get("to_ms")) + 1) % Number(query.get("step_ms")), "the newest step asked for is whole, not still filling").toBe(0);

  await page.getByRole("tab", { name: "metadata" }).click();
  await expect(page.getByRole("term").filter({ hasText: /^axon\.quota$/ }), "a node serving quotas advertises it, beyond its role and place").toBeVisible();
  await page.getByRole("tab", { name: /^events/ }).click();
  await expect(page.getByLabel("Search events")).toBeVisible();
  const before = history.length;
  await page.clock.install();
  await page.clock.fastForward(31_000);
  expect(history.length).toBe(before);
  await page.getByRole("tab", { name: "metrics" }).click();
  await expect(page.getByText("Actor types and messages")).toBeVisible();
  await page.getByRole("tab", { name: "connections" }).click();
  await expect(page.getByRole("region", { name: "Transport diagnostics" })).toBeVisible();
  await page.getByRole("tab", { name: "distributions" }).click();
  await expect(page.getByText("Request round trip over time")).toBeVisible();
  await page.goto("/nodes/not-a-node");
  await expect(page.getByText("Node not in the current membership")).toBeVisible();
});

test("a chart whose store holds less than its range says how much it covers", async ({ page }) => {
  test.setTimeout(120_000);
  await live(page, "/nodes/relay-0?range=1h");
  // A playground just up has stored nothing yet: ask again until the store has a first point.
  await expect(async () => {
    await page.getByRole("button", { name: "Refresh history" }).click();
    await expect(page.getByText(/^History covers the last (\d+m )?\d+s$/), "the playground is younger than an hour").toBeVisible({ timeout: 2_000 });
  }).toPass({ timeout: 60_000 });
  // Each node's first batch reaches the store on its own schedule: ask again until this one's has.
  await expect(async () => {
    await page.goto("/connections/console-0/publisher-1/out/control?range=1h");
    await expect(page.getByText(/^History covers the last (\d+m )?\d+s$/), "a connection's charts say it too").toBeVisible({ timeout: 5_000 });
  }).toPass({ timeout: 45_000 });
});

test("a node's country label draws its flag, fetched on its own, and its region", async ({ page }) => {
  const flags: string[] = [];
  page.on("request", (r) => /\/assets\/[a-z]{2}-[\w-]+\.svg$/.test(r.url()) && flags.push(r.url()));
  await live(page, "/nodes");
  const publisher = page.locator("tbody tr", { hasText: "publisher-1" }).locator('img[title="Tokyo, JP"]');
  await expect(publisher).toBeVisible();
  await expect.poll(() => publisher.evaluate((img: HTMLImageElement) => img.naturalWidth), "the file loads").toBeGreaterThan(0);
  await expect(page.locator("tbody tr", { hasText: "console-0" }).locator("img"), "a node without the label has none").toHaveCount(0);
  expect(new Set(flags.map((url) => url.split("/assets/")[1].slice(0, 2))), "only the countries named are fetched").toEqual(new Set(["jp", "is"]));
  expect((await page.request.get(flags[0])).headers()["cache-control"], "the browser keeps a flag for good").toContain("immutable");
  await page.goto("/nodes/publisher-1");
  await expect(page.getByText("Tokyo, JP")).toBeVisible();
  for (const [path, where] of [["/cluster", "[aria-label='Nodes by subsystem'] tbody tr"], ["/connections", "tbody tr"], ["/streams", "[data-stream-node='publisher-1']"]]) {
    await page.goto(path);
    await expect(page.locator(where, { hasText: "publisher-1" }).locator('[title="Tokyo, JP"], image').first(), `${path} flags the node`).toBeAttached({ timeout: 15_000 });
  }
});

test("sessions show what each has sent and received, sorted and filtered on it", async ({ page }) => {
  const size = (text: string) => { const [n, unit] = text.trim().split(" "); return Number(n) * 1024 ** ["B", "KiB", "MiB", "GiB", "TiB"].indexOf(unit); };
  const column = (i: number) => page.locator("table").first().locator(`tbody tr td:nth-child(${i})`).allTextContents();
  await live(page, "/connections");
  await expect(page.locator("tbody tr").first()).toBeVisible();
  await page.locator('th button[title="Sent"]').click();
  await expect(page.locator("th", { has: page.locator('button[title="Sent"]') })).toHaveAttribute("aria-sort", "ascending");
  const sent = (await column(7)).map(size);
  expect(sent.every((n) => n >= 0), "every session has a total").toBe(true);
  expect(sent, "smallest first").toEqual([...sent].sort((a, b) => a - b));
  const all = sent.length;
  await live(page, `/connections?cmin=${2 ** 20}`);
  await expect(page.getByRole("combobox", { name: "Session traffic" })).toContainText("≥ 1 MiB");
  const [tx, rx] = [(await column(7)).map(size), (await column(8)).map(size)];
  expect(tx.every((n, i) => n + rx[i] >= 2 ** 20), "only sessions past the threshold").toBe(true);
  expect(tx.length).toBeLessThanOrEqual(all);
});

test("the inventory filters, sorts and pages on the server", async ({ page }) => {
  const requests: URL[] = [];
  page.on("request", (r) => r.url().includes("/api/inventory") && requests.push(new URL(r.url())));
  await live(page, "/actors");
  const inventory = page.getByLabel("Actor inventory");
  await expect(inventory.getByText(/1–25 of \d+/)).toBeVisible();
  const height = (await inventory.boundingBox())!.height;
  await inventory.getByRole("button", { name: "Next page" }).click();
  await expect(inventory.getByText(/26–\d+ of \d+/)).toBeVisible();

  await inventory.getByLabel("Search actors").fill("relay-3");
  await expect.poll(() => requests.some((u) => u.searchParams.get("q") === "relay-3")).toBe(true);
  await expect(inventory.getByText(/1–\d of \d+/)).toBeVisible();
  expect(Math.abs((await inventory.boundingBox())!.height - height)).toBeLessThanOrEqual(2);

  await expect(inventory.getByRole("button", { name: "Group by node" })).toHaveAttribute("aria-pressed", "true");
  await inventory.getByRole("button", { name: /^Processed/ }).click();
  await expect(inventory.getByRole("button", { name: "Group by node" })).toHaveAttribute("aria-pressed", "false");
  await expect.poll(() => requests.at(-1)?.searchParams.get("sort")).toBe("processed");
  await expect(page).toHaveURL(/asort=processed/);

  await inventory.getByRole("combobox", { name: "Node" }).click();
  await page.getByRole("option", { name: "relay-0" }).click();
  await expect.poll(() => requests.at(-1)?.searchParams.get("node_id")).toBe("relay-0");

  await inventory.locator("tbody tr[data-href]").first().locator("td").nth(4).click();
  await expect(page).toHaveURL(/\/actors\/relay-0\/\d+:\d+:\d+\?incarnation=[0-9a-f]{32}/);
  await expect(page.getByText("Sampled timings")).toBeVisible();
  for (const panel of ["Mailbox wait", "Handler duration", "Execution turn"]) await expect(page.getByRole("heading", { name: panel })).toBeVisible();
  await expect(page.getByRole("img", { name: /Processed, admitted and rejected/ }).or(page.getByText("Flat at zero").first())).toBeVisible({ timeout: 15_000 });

  await page.goto(page.url().replace(/incarnation=[0-9a-f]+/, "incarnation=00000000000000000000000000000000"));
  await expect(page.getByText("Actor belongs to a previous engine incarnation")).toBeVisible();
  await expect(page.locator("canvas")).toHaveCount(0);
});

test("connections show rates from consecutive observations", async ({ page }) => {
  await live(page, "/connections");
  await expect(page.getByRole("img", { name: /Sent and received bytes per second/ })).toBeVisible({ timeout: 15_000 });
  await page.locator("tbody tr").nth(3).locator("td").nth(2).click();
  await expect(page.locator("tbody tr").nth(3)).toHaveAttribute("aria-selected", "true");
  await expect(page).toHaveURL(/conn=/);
  await page.getByRole("button", { name: "Keepalive round trip" }).click();
  await expect(page.getByRole("img", { name: /Keepalive round trip/ })).toBeVisible();
  for (const width of [1440, 1100]) {
    await page.setViewportSize({ width, height: 900 });
    const pane = page.getByRole("region", { name: "Transport diagnostics" });
    expect(await pane.evaluate((e) => e.scrollWidth <= e.clientWidth)).toBe(true);
  }
});

test("events come from retained history", async ({ page }) => {
  await live(page, `${MOUNTED}events`);
  await expect(page.getByRole("combobox", { name: "Event range" })).toBeVisible();
  await expect(page.locator("tbody tr").first()).toBeVisible({ timeout: 40_000 });
  await expect(page.locator("tbody time").first()).toHaveAttribute("datetime", /T/);
});

test("controls work from the keyboard", async ({ page }) => {
  await live(page, "/overview");
  const role = page.getByRole("combobox", { name: "Role" });
  await role.focus();
  await page.keyboard.press("ArrowDown");
  const list = page.getByRole("listbox", { name: "Role" });
  await expect(list).toBeVisible();
  expect((await list.boundingBox())!.y).toBeGreaterThan((await role.boundingBox())!.y);
  await page.keyboard.press("ArrowDown");
  await page.keyboard.press("Enter");
  await expect(list).toHaveCount(0);
  await expect(role).toBeFocused();
  await expect(page).toHaveURL(/role=/);
  await role.press("Enter");
  await page.keyboard.press("Escape");
  await expect(list).toHaveCount(0);
  await expect(role).toBeFocused();

  const chip = page.getByLabel("Node inspector").getByRole("button", { name: /Process incarnation/ });
  expect((await chip.innerText()).trim()).toHaveLength(14);
  await chip.click();
  const dialog = page.getByRole("dialog", { name: "Process incarnation" });
  await expect(dialog.getByRole("button", { name: "Copy original ID" })).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(dialog).toHaveCount(0);

  const info = page.getByRole("button", { name: "About the graph" });
  await info.focus();
  await expect(page.getByRole("tooltip")).toContainText("One edge per pair of nodes");
});

test("theme and sidebar persist without a first-frame flash", async ({ page }) => {
  await live(page, "/nodes");
  const before = await page.evaluate(() => document.documentElement.dataset.theme);
  await page.getByRole("link", { name: "Settings" }).click();
  await page.getByRole("radio", { name: before === "light" ? "Dark" : "Light", exact: true }).click();
  await page.goBack();
  const after = await page.evaluate(() => document.documentElement.dataset.theme);
  expect(after).not.toBe(before);

  const handle = page.getByRole("separator", { name: "Resize sidebar" });
  await handle.focus();
  await page.keyboard.press("Shift+ArrowRight");
  await expect(handle).toHaveAttribute("aria-valuenow", "208");
  await page.keyboard.press("End");
  await expect(handle).toHaveAttribute("aria-valuenow", "320");

  await page.addInitScript(() => {
    const widths: number[] = [];
    (window as unknown as { widths: number[] }).widths = widths;
    const sample = () => {
      const sidebar = document.getElementById("sidebar");
      if (sidebar) widths.push(sidebar.getBoundingClientRect().width);
      if (widths.length < 30) requestAnimationFrame(sample);
    };
    requestAnimationFrame(sample);
  });
  await page.reload();
  await expect(status(page)).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.dataset.theme)).toBe(after);
  await expect.poll(() => page.evaluate(() => (window as unknown as { widths: number[] }).widths.length)).toBeGreaterThanOrEqual(30);
  expect(new Set(await page.evaluate(() => (window as unknown as { widths: number[] }).widths))).toEqual(new Set([320]));

  await page.getByRole("button", { name: "Collapse sidebar" }).click();
  expect((await page.locator("#sidebar").boundingBox())!.width).toBe(48);
  await page.getByRole("button", { name: "Expand sidebar" }).click();
  expect((await page.locator("#sidebar").boundingBox())!.width).toBe(320);

  await page.evaluate(() => localStorage.setItem("axon-sidebar:v1", '{"width":999999}'));
  await page.reload();
  expect((await page.locator("#sidebar").boundingBox())!.width).toBe(320);
  await page.evaluate(() => localStorage.setItem("axon-sidebar:v1", "not json"));
  await page.reload();
  expect((await page.locator("#sidebar").boundingBox())!.width).toBe(176);
});

test("a base path scopes every request and link", async ({ page }) => {
  const api: string[] = [];
  page.on("request", (r) => r.url().includes("/api/") && api.push(new URL(r.url()).pathname));
  await page.goto(`${MOUNTED}nodes`);
  await expect(status(page)).toBeVisible();
  expect(api.length).toBeGreaterThan(0);
  expect(api.every((path) => path.startsWith("/ops/axon/api/"))).toBe(true);
  await page.getByRole("navigation", { name: "Main navigation" }).getByRole("link", { name: "overview" }).click();
  await page.getByRole("link", { name: "Open full topology" }).click();
  await expect(page).toHaveURL(new RegExp(`^${MOUNTED}overview/topology\\?node=`));
  await page.reload();
  await expect(page.locator("[data-node]").first()).toBeVisible();
  await page.goto(`${MOUNTED}topology?node=relay-0`);
  await expect(page).toHaveURL(`${MOUNTED}overview/topology?node=relay-0`);
});

test("a connection has its own page, with the kernel's view of the link", async ({ page }) => {
  await live(page, "/connections");
  await page.locator("tbody tr").first().locator("td").nth(2).click();
  await page.getByRole("link", { name: /Open connection/ }).click();
  await expect(page).toHaveURL(/\/connections\/[^/]+\/[^/]+\/(in|out)\/(control|live|bulk)/);
  await expect(page.locator("h1")).toContainText(/→|←/);
  for (const title of ["Framed traffic", "Round trip", "TCP retransmissions", "TCP congestion window", "Frames", "Queued bytes", "Write + flush"]) {
    await expect(page.getByRole("heading", { name: title, exact: true })).toBeVisible();
  }
  const diagnostics = page.getByRole("region", { name: "Transport diagnostics" });
  await expect(diagnostics.getByText("Congestion window")).toBeVisible({ timeout: 15_000 });
  await expect(diagnostics.getByText("Frames refused at the outbox")).toBeVisible();
  await expect(page.getByRole("img", { name: /Framed traffic/ })).toBeVisible({ timeout: 15_000 });
  await page.reload();
  await expect(page.locator("h1")).toContainText(/→|←/);
  await page.goto("/connections/relay-0/nobody/out/control");
  await expect(page.getByText("Connection not in the current view")).toBeVisible();
});

test("each lane of a peer is a row and a page of its own", async ({ page }) => {
  await live(page, "/connections");
  await expect(page.getByRole("button", { name: /^Lane/ })).toHaveAttribute("title", "The traffic class of this connection.");
  // The playground's first node sends large transfers on Bulk to each other node, calls on Live, and cluster messages on Control.
  for (const lane of ["Control", "Live", "Bulk"]) await expect(page.locator("tbody tr").filter({ hasText: lane }).first()).toBeVisible({ timeout: 15_000 });
  await page.goto("/connections/console-0/publisher-1/out/bulk");
  await expect(page.locator("h1")).toContainText("Bulk");
  // The playground caps Bulk under what it sends in one burst, so the writer waits.
  const wait = page.locator("dl div", { has: page.locator("dt", { hasText: "Rate wait" }) }).locator("dd");
  await expect(wait).toHaveText(/^\d+(\.\d+)? (ms|s)$/, { timeout: 15_000 });
  await page.goto("/connections/console-0/publisher-1/out/live");
  await expect(page.locator("h1")).toContainText("Live");
  await expect(wait).toHaveText("0 ns");
  await page.goto("/connections/console-0/publisher-1/out/fast");
  await expect(page.getByText("Connection not in the current view")).toBeVisible();
});

test("a down lane is a row that says how long, and nothing else", async ({ page }) => {
  type Peer = { node: string; outgoing: boolean; lane: string; down_ms: number | null };
  await polled(page, (body) => {
    const peers = (body.nodes.find((n) => n.node.id === "relay-0") as unknown as { snapshot: { cluster: { engine: { peers: Peer[] } } } }).snapshot.cluster.engine.peers;
    peers.push({ ...peers.find((p) => p.node === "publisher-1" && p.lane === "control")!, lane: "bulk", outgoing: true, down_ms: 65_000 });
    return body;
  });
  // relay-0 sends nothing on Bulk, so this is its only Bulk row to publisher-1.
  await page.goto("/connections?cnode=relay-0");
  await expect(status(page, "Polling")).toBeVisible();
  const down = page.locator("tbody tr td span[title]", { hasText: "Down · 1m 5s" });
  await expect(down).toHaveCount(1);
  await expect(down).toHaveAttribute("title", "This connection is down. Calls on it fail and try again later. The peer stays in the cluster.");
  await page.goto("/connections/relay-0/publisher-1/out/bulk");
  await expect(page.getByLabel("Transport diagnostics")).toContainText("This connection is down.");
});

test("a child's edge shows the parent's window to it", async ({ page }) => {
  await live(page, "/streams");
  const edge = page.locator('[data-flow^="publisher-1>"][data-joining="false"]').first();
  await expect(edge.locator("title")).toHaveText(/In flight .+ · Window .+ · Round trip .+ · Rate .+ · Resent \d+/, { timeout: 15_000 });
  await page.locator('[data-stream-node="publisher-1"]').click();
  await expect(page.getByRole("main")).toContainText(/In flight .+ · Window/);
  await expect(page.getByRole("main")).toContainText("Pushes resent");
});

test("the lane, seed and host events are kinds to filter on, and a seed on other timings says so", async ({ page }) => {
  await live(page, "/events");
  await page.getByRole("combobox", { name: "Event kind" }).click();
  for (const kind of ["lane down", "lane up", "host tcp limits", "seed timings"]) await expect(page.getByRole("option", { name: kind, exact: true })).toBeVisible();
  await page.keyboard.press("Escape");
  type Links = { snapshot: { cluster: { discovery: { links: { target: string; identity: string | null; state: string; since_ns: number; error: string | null }[] } } } };
  await polled(page, (body) => {
    const relay = body.nodes.find((n) => n.node.id === "relay-0") as unknown as Links;
    relay.snapshot.cluster.discovery.links.push({ target: "seed-9", identity: null, state: "timings", since_ns: 5e9, error: null });
    return body;
  });
  await page.goto("/cluster");
  await expect(page.getByRole("button", { name: /relay-0 discovery: seed-9: The seed uses different timings\. This node does not count it\./ })).toBeVisible();
});

test("icon toggles explain themselves", async ({ page }) => {
  await live(page, "/overview/topology");
  await page.getByRole("button", { name: "Circular" }).hover();
  await expect(page.getByText("Layout · Circular")).toBeVisible();
  await page.getByRole("button", { name: "Log", exact: true }).focus();
  await expect(page.getByText("Scale · Log")).toBeVisible();
});

test("the topology colour map is a choice, kept in the URL", async ({ page }) => {
  await live(page, "/overview/topology");
  const edge = page.locator("[data-edge] path.edge").first();
  await expect(edge).toHaveAttribute("stroke", /rgb/);
  await page.getByRole("combobox", { name: "Colour map" }).click();
  await page.getByRole("option", { name: "Viridis" }).click();
  await expect(page).toHaveURL(/ramp=viridis/);
  const ports = await page.locator("[data-edge] path.edge").evaluateAll((paths) => paths.map((p) => p.getAttribute("d")!.split(" C")[0]));
  expect(new Set(ports).size, "every edge leaves its card from its own port").toBe(ports.length);
});

test("settings choose the theme and the accent, and both persist", async ({ page }) => {
  await live(page, "/overview");
  await page.getByRole("link", { name: "Settings" }).click();
  await expect(page.locator("h1")).toHaveText("Settings");
  const root = (name: string) => page.evaluate((n) => getComputedStyle(document.documentElement).getPropertyValue(n).trim(), name);
  await page.getByRole("radio", { name: "Light", exact: true }).click();
  expect(await page.evaluate(() => document.documentElement.dataset.theme)).toBe("light");
  await page.getByRole("radio", { name: "Sky" }).click();
  const accent = await root("--accent");
  expect(accent).not.toBe("#218c61");
  await page.getByRole("radio", { name: "Dark", exact: true }).click();
  expect(await root("--accent")).toBe("#6aa9f0");
  await page.reload();
  expect(await root("--accent")).toBe("#6aa9f0");
  await expect(page.getByRole("radio", { name: "Sky" })).toBeChecked();
  await page.getByRole("button", { name: "Reset" }).click();
  expect(await root("--accent")).toBe("#e0b252");
  const store = page.locator("div.rounded-md.border", { has: page.getByText("In use", { exact: true }) }).last();
  await expect(store).toBeVisible();
  expect(await store.evaluate((card) => [...card.childNodes].filter((n) => n.nodeType === Node.TEXT_NODE).map((n) => n.textContent)), "no stray text under a store card").toEqual([]);
});

test("when history was last collected is read on the collector's clock, so a skewed browser shows it right", async ({ page }) => {
  // A browser a day ahead: its clock minus the collector's wall stamp would say 24h ago.
  await page.clock.install({ time: Date.now() + 86_400_000 });
  await live(page, "/overview");
  const recently = /· collected \d{1,2}s ago/;
  await status(page).click();
  await expect(page.getByRole("dialog", { name: "Console status" }).getByText(recently).first()).toBeVisible({ timeout: 15_000 });
  await page.keyboard.press("Escape");
  await page.getByRole("link", { name: "Settings" }).click();
  await expect(page.getByText(recently).first()).toBeVisible();
});

test("a store's card says what it holds, per table, and the room it has left", async ({ page }) => {
  await live(page, "/settings");
  // The playground keeps history in a 256 MiB MemoryMetrics.
  await expect(page.getByText(/ of 256 MiB$/)).toBeVisible({ timeout: 15_000 });
  const usage = page.getByRole("group", { name: "Usage of 0:memory" });
  await expect(usage.getByRole("row", { name: /^samples [\d.]+k? / })).toBeVisible();
  await expect(usage.getByRole("cell", { name: /^\d\d:\d\d → \d\d:\d\d$/ })).toBeVisible();
});

test("a node that leaves stays findable, and events can be filtered", async ({ page }) => {
  test.setTimeout(90_000);
  await live(page, `${MOUNTED}nodes`);
  const visitor = page.locator("tbody tr", { hasText: "visitor-0" });
  await expect(visitor).toBeVisible({ timeout: 40_000 });
  await expect(visitor).toContainText(/Left \d+s ago/, { timeout: 40_000 });
  await expect(page.locator("footer", { hasText: "members" }), "a departed node is counted apart from the members").toHaveText(/^\d+ members · \d+ left/);
  await visitor.locator("td").nth(3).click();
  await expect(page.getByRole("status").filter({ hasText: "left the membership" })).toBeVisible();

  await page.goto(`${MOUNTED}events`);
  await page.getByLabel("Search events").fill("visitor-0");
  await expect(page.locator("tbody tr").first()).toContainText("visitor-0");
  await page.getByRole("combobox", { name: "Event kind" }).click();
  await page.getByRole("option", { name: "joined" }).click();
  await expect(page).toHaveURL(/ek=joined/);
  await expect(page.locator("tbody tr").first()).toContainText(/incarnation/);
  await expect(page.getByTestId("event-lanes"), "a swimlane per node sits above the table").toBeVisible();
  await page.getByRole("combobox", { name: "Event node" }).click();
  await page.getByRole("option", { name: "visitor-0" }).click();
  await expect(page).toHaveURL(/en=visitor-0/);
  await expect(page.locator("tbody tr").first()).toContainText("visitor-0");
  expect(await page.locator("tbody tr").evaluateAll((rows) => rows.every((r) => r.textContent!.includes("visitor-0")))).toBe(true);
});

test("the content is one Tab away, and a session opens from the keyboard", async ({ page }) => {
  await live(page, "/connections");
  const url = page.url();
  await page.keyboard.press("Tab");
  await expect(page.getByRole("link", { name: "Skip to content" })).toBeFocused();
  await page.keyboard.press("Enter");
  await expect(page.locator("main#content"), "the skip link moves focus and stays on the page").toBeFocused();
  expect(page.url()).toBe(url);
  await page.getByLabel("Search sessions").fill("relay-1");
  await expect(page.locator("tbody tr").first()).toContainText("relay-1");
  const cells = await page.locator("table").first().locator("tbody tr td:nth-child(2)").evaluateAll((tds) => tds.map((td) => Math.round(td.getBoundingClientRect().left)));
  expect(new Set(cells).size, "the direction tags share one column").toBe(1);
  const focused = () => page.evaluate(() => (document.activeElement instanceof HTMLTableRowElement ? document.activeElement.sectionRowIndex : null));
  await page.locator("th button").last().focus();
  await page.keyboard.press("Tab");
  const first = await focused();
  expect(first, "one Tab from the header lands on a row").not.toBeNull();
  await page.keyboard.press("j");
  expect(await focused()).not.toBe(first);
  await page.keyboard.press("Enter");
  await expect(page).toHaveURL(/\/connections\/[^/]+\/[^/]+\/(in|out)\/(control|live|bulk)/);
});

test("the stream graph draws flows from parents to children with rates, and drills into streams and events", async ({ page }) => {
  await live(page, "/streams");
  const card = (node: string) => page.locator(`[data-stream-node="${node}"]`);
  await expect(card("publisher-1")).toHaveAttribute("data-kind", "origin");
  await expect(card("console-0")).toHaveAttribute("data-kind", "leaf");
  const intoLeaf = page.locator('[data-flow$=">console-0"][data-joining="false"]');
  await expect(intoLeaf).toHaveCount(1);
  const relay = (await intoLeaf.getAttribute("data-flow"))!.split(">")[0];
  expect(relay).toMatch(/^relay-\d$/);
  await expect(page.locator(`[data-flow="publisher-1>${relay}"]`)).toHaveCount(1);
  await expect(page.locator('[data-flow^="publisher-"][data-flow$=">console-0"]')).toHaveCount(0);
  // Three consumers share two relays, so one relay feeds several; the direct node skips them.
  const fedByRelay = await page.locator('[data-flow^="relay-"][data-joining="false"]').evaluateAll((edges) => edges.map((e) => e.getAttribute("data-flow")!.split(">")[0]));
  expect(fedByRelay.length).toBe(3);
  expect(new Set(fedByRelay).size).toBeLessThan(3);
  await expect(page.locator('[data-flow="publisher-1>direct-0"]')).toHaveCount(1);
  await expect(page.locator('[data-flow^="relay-"][data-flow$=">direct-0"]')).toHaveCount(0);
  await expect(page.locator("[data-saving]")).toContainText(/saves \d+%/);
  // A card's rate says what it counts: what an origin publishes, what any other node delivers.
  await expect(card("publisher-1")).toContainText(/published/);
  await expect(card(relay)).toContainText(/delivered/);
  await expect.poll(async () => Number(await intoLeaf.getAttribute("data-rate")), { timeout: 15_000 }).toBeGreaterThan(0);
  const box = async (node: string) => (await card(node).boundingBox())!;
  expect((await box("publisher-1")).y).toBeLessThan((await box(relay)).y);
  expect((await box(relay)).y).toBeLessThan((await box("console-0")).y);

  await card("console-0").click();
  await expect(page).toHaveURL(/snode=console-0/);
  await page.getByRole("group", { name: "Detail" }).getByRole("button", { name: "Streams" }).click();
  const stream = page.locator('[data-stream="playground/publisher-1"]');
  await expect(stream.getByRole("cell").nth(1)).toHaveText(`publisher-1 → ${relay}`);
  expect(await stream.getByRole("cell").nth(1).evaluate((td) => td.scrollWidth <= td.clientWidth), "the route is never cut").toBe(true);
  // Pointing at the leaf keeps its whole path to the origin lit.
  await card("console-0").hover();
  await expect(page.locator(`[data-flow="publisher-1>${relay}"] path.edge`)).toHaveAttribute("opacity", "0.85");
  await expect(stream.getByRole("cell").nth(2)).toHaveText(/\d/);
  await expect(stream.getByRole("cell").nth(3)).toHaveText(/^\d+$/);
  await page.getByRole("group", { name: "Detail" }).getByRole("button", { name: "Events" }).click();
  await expect(page.locator('[data-stream-event="parent_chosen"]').first()).toContainText("pulling from");
});

test("a node can be dragged out of the layout, its flows follow, and Reset layout puts it back", async ({ page }) => {
  await live(page, "/streams");
  const card = page.locator('[data-stream-node="console-0"]');
  const edge = page.locator('[data-flow$=">console-0"] path.edge');
  const reset = page.getByRole("button", { name: "Reset layout" });
  await expect(reset).toBeDisabled();
  await expect(reset, "a disabled reset says why").toHaveAttribute("title", /drag/i);
  const [placed, routed] = [await card.getAttribute("transform"), await edge.getAttribute("d")];
  const box = (await card.boundingBox())!;
  await page.mouse.move(box.x + 20, box.y + 20);
  await page.mouse.down();
  await page.mouse.move(box.x + 140, box.y - 40, { steps: 6 });
  await page.mouse.up();
  await expect(card).not.toHaveAttribute("transform", placed!);
  await expect(edge).not.toHaveAttribute("d", routed!);
  await expect(page).not.toHaveURL(/snode=console-0/);
  await reset.click();
  await expect(card).toHaveAttribute("transform", placed!);
  await expect(reset).toBeDisabled();
  await card.click();
  await expect(page).toHaveURL(/snode=console-0/);
});

test("streams show who pulls from whom, and the origin feeds relays only", async ({ page }) => {
  await live(page, "/streams?sview=table");
  const row = (node: string) => page.locator("tbody tr", { has: page.locator("td:first-child", { hasText: new RegExp(`^${node}$`) }) });
  await expect(row("publisher-1").getByRole("cell").nth(1)).toHaveText("origin");
  await expect(row("console-0").getByRole("cell").nth(1)).toHaveText("leaf");
  await expect(row("console-0").getByRole("cell").nth(2)).toHaveText(/^relay-\d$/);
  const parent = await row("console-0").getByRole("cell").nth(2).innerText();
  await expect(row(parent).getByRole("cell").nth(1)).toHaveText("relay");
  await expect(row(parent).getByRole("cell").nth(2)).toHaveText(/^publisher-\d(, publisher-\d)*$/);
  await expect(row("publisher-1").getByRole("cell").nth(4)).toHaveText(/\d\/s/);
  for (const name of [/^Admitted\/s/, /^Delivered\/s/]) await expect(page.getByRole("columnheader", { name })).toBeVisible();
  await row("publisher-1").click();
  await expect(page).toHaveURL(/snode=publisher-1/);
  const downstream = page.locator("section", { hasText: "Downstream" }).last();
  await expect(downstream.getByText(parent, { exact: true })).toBeVisible();
  await expect(downstream.getByText("console-0")).toHaveCount(0);
  await page.reload();
  await expect(row("publisher-1")).toHaveAttribute("aria-selected", "true");
});

test("a trace opens as a waterfall whose spans cross nodes, and the filters live in the URL", async ({ page }) => {
  await live(page, `${MOUNTED}traces?q=request`);
  const first = page.locator("tbody tr").first();
  await expect(first).toBeVisible({ timeout: 40_000 });
  await expect(first.locator("td").nth(1)).toHaveText("request");
  await first.click();
  await expect(page).toHaveURL(/\/ops\/axon\/traces\/[0-9a-f]{16}/);
  const spans = page.getByRole("list", { name: "Spans" }).getByRole("listitem");
  await expect(spans.first()).toContainText(/\S+request → \S+/);
  await expect(spans.filter({ hasText: "work · relay-0" })).toHaveCount(1);
  await expect(spans.filter({ hasText: "work · relay-3" })).toHaveCount(1);
  expect(await page.getByLabel("Trace summary").locator("a[href*='/nodes/']").count()).toBeGreaterThan(1);

  await spans.nth(1).locator("span.opacity-35").hover({ force: true });
  await expect(page.getByRole("tooltip")).toContainText(/In mailbox.*Queued for relay-0\./);
  await page.mouse.move(0, 0);
  await page.getByRole("button", { name: "How to read this" }).click();
  await expect(page.getByRole("dialog", { name: "How to read a trace" })).toContainText("in mailbox");
  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog")).toBeHidden();

  await spans.nth(1).getByRole("button").click();
  await expect(page).toHaveURL(/span=[0-9a-f]{16}/);
  await expect(spans.nth(1)).toContainText("In mailbox");
  await page.reload();
  await expect(spans.nth(1).getByRole("button")).toHaveAttribute("aria-expanded", "true");
  await spans.nth(1).getByRole("link", { name: /relay-0 \d+:\d+:\d+/ }).click();
  await expect(page).toHaveURL(/\/actors\/[^/]+\/\d+%3A\d+%3A\d+/);

  await page.goto(`${MOUNTED}traces?q=playground`);
  // A dropped "work" message is a trace of the playground too (its failed handle span): pick a publish.
  const publish = page.locator("tbody tr", { hasText: /publish.*playground\/publisher-\d/ }).first();
  await expect(publish).toBeVisible();
  await publish.click();
  await expect(spans.first()).toContainText(/^publisher-\dpublish playground\/publisher-\d/);
  await expect(spans.nth(1)).toContainText(/batch from publisher-\d/);

  await page.goto(`${MOUNTED}traces`);
  await page.getByRole("combobox", { name: "Outcome" }).click();
  await page.getByRole("option", { name: "Failed" }).click();
  await expect(page).toHaveURL(/outcome=failed/);
  // The playground drops traced "work" every 15 s: a drop is a failed trace, and the only one it makes.
  await expect(page.locator("tbody tr").first()).toContainText(/work/, { timeout: 20_000 });
  for (const row of await page.locator("tbody tr").allInnerTexts()) expect(row).toContain("work");
  await page.goto(`${MOUNTED}traces/00000000000000aa`);
  await expect(page.getByText("Trace not found")).toBeVisible();
});

test("dead letters list what was dropped, why, and link to the trace", async ({ page }) => {
  await live(page, `${MOUNTED}dead-letters?q=work`);
  const first = page.locator("tbody tr").first();
  await expect(first).toBeVisible({ timeout: 40_000 });
  await expect(first).toContainText(/scratch.*work.*actor stopped.*local/);
  await first.getByRole("link", { name: "open" }).click();
  await expect(page).toHaveURL(/\/ops\/axon\/traces\/[0-9a-f]{16}/);
  // The message was dropped before any handler ran: the drop itself is the trace's span.
  await expect(page.getByRole("list", { name: "Spans" }).getByRole("listitem").filter({ hasText: "work" }).first()).toBeVisible({ timeout: 15_000 });
});

test("a list that failed to load says so, never that nothing is there", async ({ page }) => {
  await page.route(/\/api\/(dead-letters|traces)(\?|$)/, (route) => route.fulfill({ status: 503, json: { error: "the store is down" } }));
  await live(page, "/dead-letters");
  await expect(page.getByText("Could not load dead letters")).toBeVisible();
  await expect(page.getByText("the store is down").first()).toBeVisible();
  await expect(page.getByText("No dead letters")).toHaveCount(0);
  await live(page, "/traces");
  await expect(page.getByText("Could not load traces")).toBeVisible();
  await expect(page.getByText(/^No traces in the last/)).toHaveCount(0);

  // An answer in which every node failed is no answer either.
  await page.unrouteAll();
  await page.route(/\/api\/dead-letters(\?|$)/, async (route) => {
    const response = await route.fetch();
    const body = await response.json();
    const nodes = (await (await page.request.get("/api/overview")).json()).nodes.map((n: { node: { id: string } }) => n.node.id);
    await route.fulfill({ response, json: { ...body, letters: [], errors: Object.fromEntries(nodes.map((id: string) => [id, "timed out"])) } });
  });
  await live(page, "/dead-letters");
  await expect(page.getByText("Could not load dead letters")).toBeVisible();
  await expect(page.getByText(/^No answer from /).first()).toBeVisible();
});

test("a node that refuses the console says what to set, and is not taken for one that left", async ({ page }) => {
  // The sentence `a_node_is_inspectable_from_an_allowed_peer` pins, for relay-1. The playground has no way to set Deny.
  const refused = "relay-1 refuses to answer: its TelemetryConfig::access (default Deny) or ClusterConfig::authorizer does not admit this node";
  await polled(page, (body) => {
    for (const n of body.nodes) if (n.node.id === "relay-1") n.error = refused;
    return body;
  });
  await page.goto("/overview?node=relay-1");
  await expect(page.getByRole("alert").filter({ hasText: refused })).toBeVisible();
  const inspector = page.getByRole("complementary", { name: "Node inspector" });
  await expect(inspector).toContainText(refused);
  await expect(inspector).not.toContainText("Departure reason unknown");
});

test("a traces list at its cap says so, and that a duration sort ranks only what came back", async ({ page }) => {
  test.setTimeout(60_000);
  await page.route(/\/api\/traces\?/, async (route) => {
    const response = await route.fetch();
    const body = await response.json();
    const one = body.traces?.[0];
    if (one) body.traces = Array.from({ length: 500 }, (_, i) => ({ ...one, trace: i.toString(16).padStart(16, "0") }));
    await route.fulfill({ response, json: body });
  });
  await live(page, "/traces");
  await expect(page.getByText(/^Newest 500 · use a shorter range or a filter$/)).toBeVisible({ timeout: 40_000 });
  await page.getByRole("button", { name: /^Duration/ }).click();
  await expect(page.getByText(/^Slowest of the newest 500 · set a minimum duration to search the whole range$/)).toBeVisible();
});

test("an actor that lost messages is marked, and the mark opens its dead letters", async ({ page }) => {
  await live(page, `${MOUNTED}actors?aq=flaky`);
  const mark = page.getByRole("link", { name: /^\d+ dead letters$/ });
  await expect(mark).toBeVisible({ timeout: 25_000 });
  await expect(page.getByRole("link", { name: /dead letters/i }).first()).toContainText(/\d/);
  await mark.click();
  await expect(page).toHaveURL(/dead-letters\?q=console-0/);
  await expect(page.locator("tbody tr").first()).toContainText(/flaky.*poke.*actor failed/);
  // Only its letters: one per failure, and the playground's flaky fails every 30 s.
  for (const row of await page.locator("tbody tr").all()) await expect(row).toContainText(/flaky.*poke.*actor failed/);
  // And back: the letter names its actor, and the actor counts its letters.
  await page.locator("tbody tr").first().getByRole("link", { name: "flaky" }).click();
  await expect(page).toHaveURL(/actors\?.*aq=flaky/);
  await expect(page).toHaveURL(/anode=console-0/);
  await page.getByLabel("Actor inventory").locator("tbody tr[data-href]").first().click();
  await expect(page.locator("dt", { hasText: /^Dead letters$/ })).toBeVisible();
  await expect(page.locator("dd").getByRole("link", { name: /^\d+$/ })).toBeVisible();
});

test("an actor shows how long it has been up and, after a restart, since then; both sort on the server", async ({ page }) => {
  const requests: URL[] = [];
  page.on("request", (r) => r.url().includes("/api/inventory") && requests.push(new URL(r.url())));
  await live(page, `${MOUNTED}actors?aq=flaky`);
  const row = page.getByLabel("Actor inventory").locator("tbody tr[data-href]").first();
  await expect(row.locator("[title^='Restarted']")).toHaveText(/^\d+(s|m \d+s)$/, { timeout: 25_000 });
  await expect(row.locator("[title^='Started']")).toHaveText(/^\d+(s|m \d+s)$/);
  for (const [label, sort] of [[/^Up/, "up"], [/^Since restart/, "since_restart"]] as const) {
    await page.getByRole("button", { name: label }).click();
    await expect.poll(() => requests.at(-1)?.searchParams.get("sort")).toBe(sort);
  }
  await row.click();
  await expect(page.locator("dt", { hasText: /^Up$/ })).toBeVisible();
  await expect(page.locator("dt", { hasText: /^Since restart$/ })).toBeVisible();

  // An actor that never restarted shows Up alone.
  await live(page, `${MOUNTED}actors?aq=relay`);
  const relay = page.getByLabel("Actor inventory").locator("tbody tr[data-href]").first();
  await expect(relay.locator("[title^='Started']")).toHaveText(/^\d+(s|m \d+s)$/);
  await expect(relay.locator("[title^='Restarted']")).toHaveCount(0);
  await relay.click();
  await expect(page.locator("dt", { hasText: /^Up$/ })).toBeVisible();
  await expect(page.locator("dt", { hasText: /^Since restart$/ })).toHaveCount(0);
});

test("the overview opens on its verdict, and the topology says what it counts", async ({ page }) => {
  await live(page, "/overview");
  await expect(page.getByRole("tab", { name: /^attention/ })).toHaveAttribute("aria-selected", "true");
  const footer = page.getByLabel("Topology", { exact: true }).locator("footer");
  await expect(footer).toContainText(/\d+ links · \d+ ends/);
  // The ends are the sessions reported, one per node that reports it, which is what the Connections tile counts.
  await expect
    .poll(async () => {
      const ends = (await footer.innerText()).match(/(\d+) ends/)?.[1];
      const tile = (await page.getByLabel("Cluster summary").innerText()).match(/Connections\s+([\d,]+)/)?.[1]?.replaceAll(",", "");
      return ends !== undefined && ends === tile;
    })
    .toBe(true);
});

test("a console server that went away is named, and the tiles stop claiming health", async ({ page }) => {
  // An open stream outlives a route; without one the console polls, and the polls are what find the server gone.
  await page.route("**/api/events", (route) => route.abort());
  await page.goto("/overview");
  await expect(status(page, "Polling")).toBeVisible();
  await page.route("**/api/overview", (route) => route.abort("connectionrefused"));
  await expect(page.getByRole("alert").filter({ hasText: "Console server unreachable" })).toBeVisible({ timeout: 15_000 });
  const summary = page.getByLabel("Cluster summary");
  await expect(summary).toContainText(/last seen \d+s ago/);
  await expect(summary).not.toContainText("all report");
  await expect(status(page, "Stale view")).toBeVisible();
});

test("cluster statistics count membership as one node sees it, and a missing authority has one name", async ({ page }) => {
  await live(page, "/cluster");
  const overview = await (await page.request.get("/api/overview")).json();
  const own = overview.nodes.find((n: { node: { id: string } }) => n.node.id === overview.observer).snapshot.cluster;
  const table = page.getByLabel("Nodes by subsystem");
  await expect(table.getByRole("button", { name: /^Cluster membership:/ }), "the observer's own count, not a sum over nodes").toContainText(new RegExp(`^${own.members} members`));
  await expect(page.getByLabel("ownership: Not configured")).toContainText("Not configured");
  await expect(table.getByRole("button", { name: /^Cluster leases:/ })).toContainText("not configured");
  await expect(table.locator("tbody")).toContainText("not configured");
  // No card scrolls inside: each is as tall as what it holds.
  const scrolling = await page.locator("main section[aria-label]").evaluateAll((cards) => cards.flatMap((c) => [c, ...c.querySelectorAll("*")]).filter((e) => e.scrollHeight > e.clientHeight + 1 && /auto|scroll/.test(getComputedStyle(e).overflowY)).map((e) => e.closest("section")!.getAttribute("aria-label")));
  expect(scrolling).toEqual([]);
  await table.getByRole("columnheader", { name: "Membership" }).getByRole("button").click();
  const drawer = page.getByLabel("Details", { exact: true });
  await expect(drawer).toContainText(`The view of ${overview.observer}`);
  await expect(drawer.locator("dl > div", { has: page.locator("dt > span", { hasText: /^Joined$/ }) }).locator("dd").first(), "the observer's own count, not a sum over nodes").toHaveText(String(own.joined));
  await live(page, "/overview");
  await expect(page.getByLabel("Cluster summary")).toContainText(/Ownership\s*not configured/i);
});

test("status reads as status whatever the accent, and colour is never red for busy", async ({ page }) => {
  await page.addInitScript(() => localStorage.setItem("axon-accent", "#ec86a8"));
  await live(page, "/overview/topology");
  const dot = status(page).locator("[data-dot]");
  const colour = () => dot.evaluate((e) => getComputedStyle(e).backgroundColor);
  expect(await colour(), "Connected is not rose").not.toBe("rgb(236, 134, 168)");
  expect(await colour()).toBe(await page.evaluate(() => { const probe = document.createElement("span"); probe.style.color = "var(--success)"; document.body.append(probe); const c = getComputedStyle(probe).color; probe.remove(); return c; }));
  await expect(page.getByRole("combobox", { name: "Colour map" })).toContainText("Ocean");
  const legend = page.locator("[data-ramp-legend]");
  expect(await legend.evaluate((e) => getComputedStyle(e).backgroundImage), "the legend wedge is painted with the ramp").toContain("linear-gradient");
  await page.goto("/nodes");
  await expect(page.locator("tbody tr img").first()).toBeVisible();
  expect(await page.locator("tbody tr img").first().evaluate((img) => getComputedStyle(img).boxShadow), "a white flag keeps its edge on a white panel").not.toBe("none");
});

test("muted text keeps AA contrast on every light surface", async ({ page }) => {
  await page.addInitScript(() => localStorage.setItem("axon-theme", "light"));
  await live(page, "/overview");
  const ratios = await page.evaluate(() => {
    const rgb = (name: string) => { const probe = document.createElement("span"); probe.style.color = `var(${name})`; document.body.append(probe); const c = getComputedStyle(probe).color.match(/\d+/g)!.map(Number); probe.remove(); return c; };
    const lum = (c: number[]) => { const [r, g, b] = c.map((v) => { v /= 255; return v <= 0.03928 ? v / 12.92 : ((v + 0.055) / 1.055) ** 2.4; }); return 0.2126 * r + 0.7152 * g + 0.0722 * b; };
    const muted = lum(rgb("--muted"));
    return ["--bg", "--header", "--panel", "--panel-raised"].map((s) => { const l = lum(rgb(s)); return [s, (Math.max(l, muted) + 0.05) / (Math.min(l, muted) + 0.05)] as const; });
  });
  for (const [surface, ratio] of ratios) expect(ratio, surface).toBeGreaterThanOrEqual(4.5);
});

test("session tables show whole values, and the endpoint stays in view", async ({ page }) => {
  await live(page, "/connections");
  await expect(page.getByRole("columnheader", { name: /Peer incarnation/ })).toBeVisible();
  await expect(page.locator("tbody tr").first()).toBeVisible();
  const cut = () => page.locator("table").first().locator("tbody td.text-right, tbody td:has(button)").evaluateAll((tds) => tds.filter((td) => td.scrollWidth > td.clientWidth + 1).map((td) => td.textContent));
  await expect.poll(cut, { message: "no number or incarnation is cut beside the detail pane" }).toEqual([]);
  const names = page.locator("table").first().locator("tbody tr").first().locator("td");
  for (const at of [0, 2]) await expect(names.nth(at).locator("xpath=descendant-or-self::*[@title]").first(), "endpoint and peer name themselves in full").toHaveAttribute("title", /\w/);
  const scroller = page.locator("table").first().locator("xpath=..");
  await scroller.evaluate((e) => (e.scrollLeft = e.scrollWidth));
  const [box, first] = [(await scroller.boundingBox())!, (await page.locator("tbody tr").first().locator("td").first().boundingBox())!];
  expect(Math.round(first.x), "scrolled right, the endpoint is still there").toBe(Math.round(box.x));
  await page.goto("/streams?sview=table");
  await expect.poll(cut).toEqual([]);
});

test("the cluster per-node table cuts no cell, under a head per subsystem", async ({ page }) => {
  await live(page, "/cluster");
  const table = page.getByLabel("Nodes by subsystem");
  const cells = table.locator("tbody tr td");
  await expect(cells.first()).toBeVisible();
  // A cell's warning line truncates on purpose: its title holds the line whole.
  const cut = () => cells.evaluateAll((tds) => tds.filter((td) => td.scrollWidth > td.clientWidth + 1 || [...td.querySelectorAll("*")].some((e) => e.scrollWidth > e.clientWidth + 1 && !(e.classList.contains("truncate") && e.closest<HTMLElement>("[title]")?.title.includes(e.textContent ?? "")))).map((td) => td.textContent));
  expect(await cut()).toEqual([]);
  await expect(table.locator("tbody").getByText("not configured").first(), "whole, not \"not conf…\"").toBeVisible();
  for (const sub of ["Membership", "Discovery", "Leases", "Quotas", "Calls"]) await expect(table.getByRole("columnheader", { name: sub, exact: true })).toHaveAttribute("scope", "col");
  await page.setViewportSize({ width: 860, height: 720 });
  expect(await cut(), "narrow, still no cell is cut").toEqual([]);
});

test("the inventory's rate survives a click inside the server's cache", async ({ page }) => {
  // Every answer after the first repeats its gathering time with a million more messages processed: one gathering,
  // so no rate, where the time of the answer would claim millions per second.
  let first: { timestamp_ms: number; actors: { diagnostics: { processed: number } }[] } | undefined;
  let answers = 0;
  await page.route(/\/api\/inventory\?/, async (route) => {
    first ??= await (await route.fetch()).json();
    const bump = 1_000_000 * answers++;
    await route.fulfill({ json: { ...first, actors: first!.actors.map((a) => ({ ...a, diagnostics: { ...a.diagnostics, processed: a.diagnostics.processed + bump } })) } });
  });
  await live(page, "/actors");
  const inventory = page.getByLabel("Actor inventory");
  const rate = inventory.locator("tbody tr[tabindex]").first().locator("td:nth-child(8) small");
  await expect(rate).toHaveText("—");
  const processed = rate.locator("..");
  const before = await processed.innerText();
  await inventory.getByRole("button", { name: "Refresh inventory" }).click();
  await expect(processed, "the second answer is shown").not.toHaveText(before);
  await expect(rate).toHaveText("—");
});

test("the history source stays picked across pages", async ({ page }) => {
  await live(page, "/overview?history=console-0");
  await page.getByRole("navigation", { name: "Main navigation" }).getByRole("link", { name: "nodes" }).click();
  await expect(page).toHaveURL(/\/nodes\?history=console-0$/);
  await page.getByRole("navigation", { name: "Main navigation" }).getByRole("link", { name: "overview" }).click();
  await expect(page).toHaveURL(/\/overview\?history=console-0$/);
});

test("times and names fit their cells at tablet and phone widths", async ({ page }) => {
  for (const width of [768, 390]) {
    await page.setViewportSize({ width, height: 844 });
    for (const route of [`${MOUNTED}dead-letters`, `${MOUNTED}events`, `${MOUNTED}traces`]) {
      await live(page, route);
      await expect(page.locator("tbody tr").first()).toBeVisible({ timeout: 40_000 });
      const cut = await page.locator("tbody tr td:first-child").evaluateAll((tds) => tds.filter((td) => td.scrollWidth > td.clientWidth + 1).map((td) => td.textContent));
      expect(cut, `${route} at ${width}`).toEqual([]);
      const names = await page.locator("tbody a[href*='/nodes/'], tbody span.inline-flex").evaluateAll((all) => all.filter((e) => !e.getAttribute("title")).length);
      expect(names, `${route}: every node name has its title`).toBe(0);
    }
  }
});

test("on a phone the trace waterfall scrolls sideways to its durations", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await live(page, `${MOUNTED}traces?q=request`);
  await page.locator("tbody tr").first().click({ timeout: 40_000 });
  const spans = page.getByRole("list", { name: "Spans" });
  await expect(spans.getByRole("listitem").first()).toBeVisible();
  const scroller = spans.locator("xpath=..");
  const seen = await scroller.evaluate((e) => {
    e.scrollLeft = e.scrollWidth;
    return { right: e.getBoundingClientRect().right, scrolled: e.scrollLeft > 0 };
  });
  expect(seen.right, "the waterfall stays inside the phone").toBeLessThanOrEqual(390);
  expect(seen.scrolled, "and scrolls sideways to its durations").toBe(true);
});

test("graphs stay legible on narrow screens", async ({ page }) => {
  await page.setViewportSize({ width: 1000, height: 800 });
  await live(page, "/streams");
  expect((await page.locator(".dotgrid").boundingBox())!.height, "stacked, the stream graph keeps its height").toBeGreaterThanOrEqual(360);
  await page.setViewportSize({ width: 390, height: 844 });
  await live(page, "/overview/topology");
  await page.getByRole("button", { name: "Fit" }).click();
  const k = Number((await transform(page))!.match(/scale\(([\d.]+)\)/)![1]);
  expect(k, "fitted to a phone's width, cards stay readable").toBeGreaterThanOrEqual(0.6);
});

test("a graph is one tab stop, and arrows move within it", async ({ page }) => {
  for (const [path, item] of [["/overview/topology", "[data-node]"], ["/streams", "[data-stream-node]"]]) {
    await live(page, path);
    await expect(page.locator(item).nth(1)).toBeAttached();
    expect(await page.locator("svg.graph [tabindex='0']").count(), `${path}: no item is a tab stop of its own`).toBe(0);
    await page.locator("svg.graph[tabindex='0']").focus();
    const first = await page.evaluate(() => document.activeElement?.getAttribute("aria-label"));
    expect(first, `${path}: focus lands on an item`).toBeTruthy();
    await page.keyboard.press("ArrowRight");
    expect(await page.evaluate(() => document.activeElement?.getAttribute("aria-label")), `${path}: an arrow moves`).not.toBe(first);
    await page.keyboard.press("Tab");
    expect(await page.evaluate(() => !!document.activeElement?.closest("svg.graph")), `${path}: Tab leaves the graph`).toBe(false);
    // A press on the background pans; it does not pick an item.
    const box = (await page.locator("svg.graph").boundingBox())!;
    await page.mouse.click(box.x + 3, box.y + 3);
    expect(await page.evaluate(() => document.activeElement?.getAttribute("role")), `${path}: the background takes no focus`).not.toBe("button");
  }
});

test("the live stream is gzip", async ({ page }) => {
  const encodings: string[] = [];
  page.on("response", (r) => r.url().includes("/api/events") && encodings.push(r.headers()["content-encoding"] ?? "none"));
  await live(page, "/overview");
  expect(encodings).toContain("gzip");
});

test("KPI sparklines keep clear of every word in their tile, and the trends bar says once what they cover (C45)", async ({ page }) => {
  await live(page, "/overview");
  const summary = page.getByLabel("Cluster summary");
  await expect(summary.locator("svg[role=img]").first()).toBeVisible();
  const struck = await summary.locator(":scope > *").evaluateAll((tiles) =>
    tiles.flatMap((tile) => {
      const spark = tile.querySelector("svg[role=img]")?.getBoundingClientRect();
      if (!spark) return [];
      const words = [...tile.querySelectorAll("span, dt, dd")].filter((e) => !e.children.length && e.textContent!.trim());
      return words.filter((e) => { const r = e.getBoundingClientRect(); return r.left < spark.right && spark.left < r.right && r.top < spark.bottom && spark.top < r.bottom; }).map((e) => e.textContent);
    }),
  );
  expect(struck).toEqual([]);
  expect(await summary.innerText(), "the caption is said once, in the trends bar").not.toContain("observed totals");
  await expect(page.getByRole("banner")).toContainText(/observed totals|partial|live session/i);
});

test("every column head reads alike, and at phone width titles keep their counts (C46, C47)", async ({ page }) => {
  await live(page, "/dead-letters");
  await expect(page.locator("tbody tr").first()).toBeVisible({ timeout: 40_000 });
  const heads = await page.locator("thead th").evaluateAll((ths) => ths.map((th) => { const s = getComputedStyle(th.querySelector("button, div") ?? th); return `${s.fontSize} ${s.fontWeight}`; }));
  expect(new Set(heads).size, heads.join(" | ")).toBe(1);
  await page.setViewportSize({ width: 390, height: 844 });
  for (const [route, name] of [["/dead-letters", /Dead letters\s*\d+/], ["/connections", /Peer sessions\s*\d+/]] as const) {
    await live(page, route);
    const title = page.locator("main").getByRole("heading", { level: 2, name });
    await expect(title).toBeVisible();
    expect(await title.evaluate((h) => h.scrollWidth <= h.clientWidth + 1 && h.getBoundingClientRect().right <= innerWidth), `${route}: the title and its count are whole`).toBe(true);
  }
  await live(page, "/overview");
  const tile = page.getByLabel("Cluster summary").getByRole("link", { name: /Cluster/ });
  await expect(tile).toContainText("Ownership");
  const cut = await tile.locator("dt, dd").evaluateAll((all) => { const edge = all[0].closest("a")!.getBoundingClientRect().right; return all.filter((e) => e.getBoundingClientRect().right > edge + 1 || e.scrollWidth > e.clientWidth + 1).map((e) => e.textContent); });
  expect(cut, "the Cluster tile names its providers in full").toEqual([]);
});

test("toggle buttons are big enough to hit (C48)", async ({ page }) => {
  for (const [route, groups] of [["/overview/topology", "main [role=group] button[aria-pressed]"], ["/overview", "[aria-label='Trend period'] button"]]) {
    await live(page, route);
    // The topology draws its toolbar once the first overview has nodes, which can come after "Connected".
    await expect(page.locator(groups).first()).toBeVisible();
    const sizes = await page.locator(groups).evaluateAll((all) => all.map((b) => [b.getBoundingClientRect().width, b.getBoundingClientRect().height, b.getAttribute("aria-label") ?? b.textContent] as const));
    expect(sizes.length, route).toBeGreaterThan(1);
    for (const [w, h, name] of sizes) expect(Math.min(w, h), `${route}: ${name}`).toBeGreaterThanOrEqual(24);
  }
});

test("a trace starts at a 24-hour time, and a search that matches nothing still counts the events (C49, C50)", async ({ page }) => {
  await live(page, `${MOUNTED}traces?q=request`);
  await page.locator("tbody tr").first().click({ timeout: 40_000 });
  await expect(page.getByLabel("Trace summary").locator("time")).toHaveText(/^[A-Z][a-z]{2} \d{1,2} \d\d:\d\d:\d\d$/);
  await live(page, `${MOUNTED}events`);
  await expect(page.locator("tbody tr").first()).toBeVisible({ timeout: 40_000 });
  await page.getByLabel("Search events").fill("zzz-nothing");
  await expect(page.getByText(/^0 of [1-9]\d* events/)).toBeVisible();
});

test("a node page names what it hides as flat, inside the page's margin (C52)", async ({ page }) => {
  await live(page, "/nodes/relay-0");
  const flat = page.getByText(/^Hidden, flat at zero over 15m: [a-z]/);
  await expect(flat).toBeVisible({ timeout: 20_000 });
  const indent = await flat.evaluate((p) => { const words = document.createRange(); words.selectNodeContents(p); return words.getBoundingClientRect().left - p.closest("main")!.getBoundingClientRect().left; });
  expect(indent).toBeGreaterThanOrEqual(8);
});

test("the store has one name, and the custom accent reads like the others (C53)", async ({ page }) => {
  await live(page, "/settings");
  const stores = page.locator("section", { has: page.getByRole("heading", { name: "Metrics stores" }) });
  await expect(stores).toContainText("In-memory ring");
  await status(page).click();
  await expect(page.getByRole("radiogroup", { name: "History source" })).toContainText("In-memory ring");
  await page.keyboard.press("Escape");
  expect(await page.getByText("Custom", { exact: true }).evaluate((e) => getComputedStyle(e).fontSize)).toBe(await page.getByText("Amber", { exact: true }).evaluate((e) => getComputedStyle(e).fontSize));
});

test("the streams table keeps its title clear, idle nodes are named, and a node's events start with its own (C54)", async ({ page }) => {
  await live(page, "/streams");
  await expect(page.locator("[data-saving]")).toBeVisible({ timeout: 20_000 });
  const idle = page.getByText(/idle nodes? not drawn$/);
  await expect(idle).toHaveText(/^(1 idle node|([2-9]|\d\d+) idle nodes) not drawn$/);
  await expect(idle).toHaveAttribute("title", /\w+-\d/);
  await page.getByRole("group", { name: "View" }).getByRole("button", { name: "Table" }).click();
  await expect(page.locator("tbody tr").first()).toBeVisible();
  await expect(page.locator("[data-saving]"), "the savings line belongs to the graph").toHaveCount(0);
  await live(page, "/streams?snode=consumer-0&stab=events");
  await expect(page.getByRole("group", { name: "Events of" }).getByRole("button", { name: "This node" })).toHaveAttribute("aria-pressed", "true");
});

test("a card's rate holds when an older observation arrives late (C55)", async ({ page }) => {
  test.setTimeout(60_000);
  // Polls only, every other one answered with the observation before the last: a poll overtaken by the stream.
  const answers: unknown[] = [];
  await polled(page, (fresh) => {
    const n = answers.push(fresh) - 1;
    return n >= 2 && n % 2 === 0 ? answers[n - 2] : fresh;
  });
  await page.goto("/streams");
  const cards = page.locator("[data-stream-node]");
  await expect(cards.first()).toBeVisible();
  const shown = new Set<string>();
  const blinks: string[] = [];
  while (answers.length < 7) {
    for (const [id, text] of await cards.evaluateAll((all) => all.map((e) => [e.getAttribute("data-stream-node")!, e.textContent!]))) {
      if (!text.includes("—")) shown.add(id);
      else if (shown.has(id)) blinks.push(`${id} at answer ${answers.length}`);
    }
    await page.waitForTimeout(200);
  }
  expect(shown.size, "rates were shown").toBeGreaterThan(0);
  expect(blinks).toEqual([]);
});

test("the connection page keeps its title off the sidebar and its diagnostics labels whole (C56)", async ({ page }) => {
  await live(page, "/connections/console-0/publisher-1/out/control");
  const title = page.locator("main h1");
  expect((await title.boundingBox())!.x - (await page.locator("main").boundingBox())!.x).toBeGreaterThanOrEqual(8);
  const cells = page.getByLabel("Transport diagnostics").locator("td");
  await expect(cells.first()).toBeVisible();
  expect(await cells.evaluateAll((tds) => tds.filter((td) => td.scrollWidth > td.clientWidth + 1).map((td) => td.textContent))).toEqual([]);
});

test("node pickers sort names as numbers, and typeahead reads a word (C57)", async ({ page }) => {
  await polled(page, (body) => {
    const one = body.nodes.find((n) => n.node.id === "publisher-1")!;
    // First, where an unsorted list would keep them.
    body.nodes.unshift(...["publisher-11", "publisher-10"].map((id) => ({ ...one, node: { ...one.node, id } })));
    return body;
  });
  await page.goto("/overview");
  await expect(status(page, "Polling")).toBeVisible();
  const order = async () => (await page.locator("main ul li button span.truncate.mono, main ul li button .mono .truncate").allInnerTexts()).filter((t) => t.startsWith("publisher"));
  await expect.poll(order).toEqual(["publisher-1", "publisher-2", "publisher-10", "publisher-11"]);
  await page.goto("/actors");
  const picker = page.getByRole("combobox", { name: "Node" });
  await picker.click();
  await expect.poll(async () => (await page.getByRole("option").allInnerTexts()).filter((t) => t.startsWith("publisher"))).toEqual(["publisher-1", "publisher-2", "publisher-10", "publisher-11"]);
  await page.keyboard.press("Escape");
  await picker.focus();
  const active = async () => page.locator(`[id="${await picker.getAttribute("aria-activedescendant")}"]`).innerText();
  await page.keyboard.type("consu");
  await expect.poll(active).toBe("consumer-0");
  await page.waitForTimeout(600);
  await page.keyboard.type("r");
  await expect.poll(active, "a pause starts a new word").toBe("relay-0");
});

test("an unknown node or actor says so and offers a way back (C58)", async ({ page }) => {
  await live(page, "/topology?node=relay-99");
  await expect(page.getByLabel("Node inspector")).toContainText("relay-99 is not in this cluster");
  await live(page, "/actors/console-0/0:999999:0");
  await expect(page.getByText("Actor no longer present on this engine")).toBeVisible();
  await expect(page.getByRole("link", { name: "Back to actors" })).toBeVisible();
  await live(page, "/actors/console-0/nope");
  await expect(page.getByRole("link", { name: "Back to actors" })).toBeVisible();
  await page.getByRole("link", { name: "Events for console-0" }).click();
  await expect(page).toHaveURL(/\/nodes\/console-0\?view=events/);
});

test("placement lists the keys each node runs, and workload shows backlog rows and handler time by message", async ({ page }) => {
  await live(page, "/placement");
  const kinds = page.getByRole("group", { name: "Kind" });
  await expect(kinds.getByRole("button", { name: /^account/ })).toContainText(/account\s*13/, { timeout: 30_000 });
  await expect(kinds.getByRole("button", { name: /^session/ })).toContainText(/session\s*8/);
  // The node with the most account keys, and its keys of each kind, as the nodes answer them.
  const on = async (kind: string) => ((await (await page.request.get(`/api/keys?kind=${kind}&limit=100`)).json()).keys as { key: string; node: string }[]);
  const accounts = await on("account");
  const node = [...new Set(accounts.map((k) => k.node))].sort((a, b) => accounts.filter((k) => k.node === b).length - accounts.filter((k) => k.node === a).length)[0];
  const own = accounts.filter((k) => k.node === node).map((k) => k.key);
  expect(own.length, "a node with keys to narrow").toBeGreaterThan(1);
  await page.locator(`[data-node="${node}"]`).click();
  await expect(page).toHaveURL(new RegExp(`node=${node}`));
  const keys = page.getByLabel("Node", { exact: true }).getByLabel("Keys", { exact: true });
  await expect(keys.locator("tbody tr")).toHaveCount(own.length);
  await expect(keys.locator("tbody")).toContainText(own[0]);
  // The key that starts the fewest others: with two keys or more, fewer rows than all.
  const narrow = (p: string) => own.filter((k) => k.startsWith(p)).length;
  const prefix = [...own].sort((a, b) => narrow(a) - narrow(b))[0];
  await keys.getByLabel("Key starts with").fill(prefix);
  await expect(keys.locator("tbody tr")).toHaveCount(narrow(prefix));
  await keys.getByLabel("Key starts with").fill("");
  await keys.getByRole("combobox", { name: "Kind" }).click();
  await page.getByRole("option", { name: "session" }).click();
  await expect(page).toHaveURL(/kind=session/);
  const sessions = (await on("session")).filter((k) => k.node === node).length;
  if (sessions) await expect(keys.locator("tbody tr")).toHaveCount(sessions);
  else await expect(keys).toContainText("No session key runs here");
  await live(page, "/workload");
  await expect(page.getByRole("img", { name: /Mailbox depth heatmap/ })).toBeVisible({ timeout: 30_000 });
  const handler = page.getByLabel("Handler time by message", { exact: true });
  await expect(handler).toContainText("Sampled on_message time");
  await expect(handler.locator("tbody")).toContainText(/work|tick/);
});

test("headings, table heads and landmarks are whole (C59)", async ({ page }) => {
  await live(page, "/overview?tab=connections");
  await expect(page.locator("tbody tr").first()).toBeVisible();
  expect(await page.locator("h1, h2, h3").evaluateAll((hs) => hs.filter((h) => !h.textContent!.trim()).length), "no empty heading").toBe(0);
  await expect(status(page)).toHaveAccessibleName(/^Connected, updated \d+s ago$/);
  await live(page, "/connections/console-0/publisher-1/out/control");
  await expect(page.getByLabel("Transport diagnostics").getByRole("columnheader", { name: "Metric" })).toBeAttached();
  await expect(page.getByRole("heading", { level: 2, name: "Transport diagnostics" })).toBeVisible();
});

test("the console shows what nodes already send: quotas, calls by outcome, wire versions, handshake failures and history on disk", async ({ page }) => {
  await live(page, "/cluster");
  const table = page.getByLabel("Nodes by subsystem");
  await expect(table.getByRole("button", { name: /^Cluster quotas:/ })).toContainText(/[\d.]+k?\/s denied/, { timeout: 20_000 });
  await expect(table.getByRole("button", { name: /^console-0 quotas:/ })).toContainText(/\d+ rows/);
  await expect(table.getByRole("button", { name: /^console-0 calls:/ })).toContainText(/↑[\d.]+k?\/s/);
  const drawer = page.getByLabel("Details", { exact: true });
  const row = (label: string) => drawer.locator("dl > div", { has: page.locator("dt > span", { hasText: new RegExp(`^${label}$`) }) }).locator("dd").last();
  await table.getByRole("columnheader", { name: "Quotas" }).getByRole("button").click();
  await expect(row("Denied by share")).toHaveText(/[\d.]+k?\/s/, { timeout: 20_000 });
  // The drawer covers the columns on the right.
  await page.keyboard.press("Escape");
  await table.getByRole("columnheader", { name: "Calls" }).getByRole("button").click();
  await expect(drawer.getByText("Calls in", { exact: true })).toBeVisible();
  await expect(row("Received")).toHaveText(/[\d.]+k?\/s/);
  await page.keyboard.press("Escape");
  await table.getByRole("button", { name: /^console-0 quotas:/ }).click();
  await expect(page).toHaveURL(/node=console-0&sub=quotas|sub=quotas&node=console-0/);
  await expect(row("Denied by share")).toHaveText(/[\d.]+k?\/s/);
  await live(page, "/connections");
  await expect(page.getByRole("columnheader", { name: "Wire" })).toBeVisible();
  await expect(page.locator('tbody td[title="The wire version that the session uses"]').first()).toHaveText(/^\d+$/);
  await expect(page.getByText(/handshake failures/)).toBeVisible();
  await live(page, "/streams?snode=publisher-1");
  await expect(page.locator("[data-disk]")).toContainText(/History on disk\s*writes OK/);
});
