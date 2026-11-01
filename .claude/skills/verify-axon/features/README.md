# Console feature map

What the playground runs, and where each console page is. One file per area. Each says what the area shows, how a user
reaches it, how to drive it with `scripts/ctl.sh`, and what trips agents up.

## The playground cluster

With `up N`, node 0 is `console-0` (it serves the dashboard), odd nodes are `relay-<i/2>`, and even nodes are
`publisher-<i/2>`. Three stream listeners always join: `consumer-0`, `consumer-1` and `direct-0`. `up` (3) gives
`console-0, relay-0, publisher-1` plus the listeners. `up 6` adds `relay-1, publisher-2, relay-2`, which is what the
e2e suite uses. `--churn` adds `visitor-0`, which keeps joining and leaving.

Traffic: every node calls `relay` on its peers, and every 40th call is traced. The relay chain runs on a named pool,
`relays`, of two workers. Every 2 s `console-0` sends 1 MiB to each other node on the Bulk lane, which every node
caps at 256 KiB/s, so Bulk rows show a rate wait. Every publisher feeds the stream
`playground/<name>`, kept on disk under `$TMPDIR/axon-playground-<port>/<name>`, and every 100th publish is traced. `console-0` hosts `flaky`, which fails on purpose, and a
`scratch` actor that stops with work queued. Both produce dead letters. `fragile`, also on `console-0`, fails four
times 5 s apart, past its policy, so about 20 s after start the timeline shows its `gave_up`.
`console-0` and the relays register the keyed kinds `account` and `session` (eager under `AXON_PAXOS`); `console-0` starts
`acct-0`..`acct-11`, spread over them, `one` on itself and `sess-0`..`sess-7`, so the placement page counts 13 account
and 8 session keys (epochs only with `AXON_PAXOS` or `AXON_CONSUL`). `AXON_KEYS=n` places n keys in their place:
`account` 60 %, `session` 30 % and `device` 10 %, the last registered on the publishers and relays and placed by `region`;
the log prints `keys: <done> of <n>` every 5 s and the rate at the end.

Every node runs `Quotas` in the region its labels give (`Tokyo`, `Reykjavík`, or `""`). Each call takes one of
`calls/s` (40 a second per tenant, three tenants) and is skipped when refused. Each request holds one of `requests`
(2 per peer). Demand is above the limit, so `denied_share` climbs on every node, while `fallback` stays at 5, one per
entry's first use. The cluster page shows them in the Quotas column of `[aria-label="Nodes by subsystem"]`, and its drawer. Read them with
`scripts/ctl.sh api overview | jq -c '.nodes[] | {id: .node.id, quotas: .snapshot.cluster.quotas}'`, and the counters
over time with `api/history?metrics=quota.denied,quota.fallback&from_ms=..&to_ms=..&step_ms=10000&node_id=console-0`.

Every node serves Prometheus's `/metrics` on a `Health` port of its own, with no sign-in: `scripts/ctl.sh logs` lists
each as `<node> metrics: http://127.0.0.1:<port>/metrics`, and `curl` reads it.

## Areas

| Area | Routes | File |
|---|---|---|
| Cluster health: overview, cluster, topology, nodes | `overview`, `cluster`, `overview/topology`, `nodes`, `nodes/<node>` | [cluster.md](cluster.md) |
| Actors: the inventory and one actor | `actors`, `actors/<node>/<id>` | [actors.md](actors.md) |
| Placement and workload: keyed actors per node, finding a key, backlog heatmap, handler time by message, any stored metric | `placement` (a card per node, `[data-node="relay-0"]`, on `[aria-label^="Placement board"]`; a click on a card or `placement?node=relay-0` opens its drawer, whose keys are `[aria-label="Node"] [aria-label="Keys"] tbody tr`; the search `getByRole("searchbox", { name: /^Find a/ })` shows the first running key that starts with the text, or asks `api/key` for that exact key and shows `would start on <node>` and opens that node, or `has no node`; nothing in the URL keeps it), `explore` (`explore?metric=cluster.calls&by=outcome`, its chart in `[aria-label^="cluster.calls over"]` once history holds two samples), `workload` (`[aria-label="Mailbox depth heatmap, node and actor type by time"]` once history holds two samples, about 10 s after `up`; `[aria-label="Handler time by message"] tbody`) | none yet |
| Streams: who pulls from whom | `streams` | [streams.md](streams.md) |
| Traces and dead letters | `traces`, `traces/<id>`, `dead-letters` | [traces.md](traces.md) |
| Connections and events | `connections`, `connections/<node>/<peer>/<in or out>/<control, live or bulk>`, `events` | [connections.md](connections.md) |
| Settings | `settings` (theme and accent, kept in local storage; then "Metrics stores": each collector's counters with what each sink holds, the room left and when it fills, and per sink `[aria-label="Usage of 0:memory"]` with its tables, the selector to `--wait` on, since the `#stores` anchor above it is empty and never shows) | none yet |
| Signing in | `login` (`?next=` is where it returns to); the foot of the sidebar shows the user and `Sign out` | none yet |

Every ctl run asks for a sign-in: `up` prints the user (`admin`) and a password of its own, and `api`, `doctor` and
`shot` sign in by themselves. To see the login page itself, shoot it before signing in: a steps file that calls
`page.context().clearCookies()` and then `page.goto(new URL("login", page.url()).href)`. The fields are labelled
`User` and `Password`, the button `Sign in`, and a refusal is the page's `alert`. Signed out, the page's own 401s
reach the console, so that shot reports `ok: false`; look at the picture.

Routes come from `crates/axon-obs/ui/src/App.tsx`. The HTTP API behind them is in `crates/axon/guide/dashboard.md#http`.
