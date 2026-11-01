# Dashboard

`axon-obs --features dashboard`: a read-only HTTP console served from inside one node. The UI is embedded in the binary
(`crates/axon-obs/ui/dist`, checked in because `include_dir!` reads it at compile time).

```rust,no_run
# use axon::ActorSystem;
# use axon_cluster::Cluster;
# use axon_obs::{Dashboard, DashboardConfig, User};
# async fn serve(system: &ActorSystem, cluster: &Cluster, hash: String) -> Result<(), Box<dyn std::error::Error>> {
let cfg = DashboardConfig {
    bind: "0.0.0.0:8080".parse()?,
    base_path: "/ops/axon".into(),                       // mount point behind a proxy; default "/"
    users: vec![User { name: "ana".into(), hash }],      // argon2 hashes; required unless the bind is loopback
    trusted_proxies: vec!["10.0.0.2".parse()?],          // whose X-Forwarded-For and -Proto are believed
    history_node: None,                                  // pin history to one MetricsService node
    ..Default::default()
};
let dashboard = Dashboard::start(system, cluster, cfg).await?;
# Ok(())
# }
```

The dashboard node needs read access on every node's `Telemetry` and on the `MetricsService` nodes (`AccessPolicy`).
It stops with the system; open event streams end, and once `shutdown` returns the port is closed and free to bind again,
unless its report's `failures` names the dashboard because the shutdown budget ran out.

`cargo run -p axon-obs --features dashboard,system-metrics --example playground -- [nodes=6] [port=8080] [base_path=/] [churn]`
runs a small cluster in one process with traffic, an in-memory metrics store and the dashboard. On top of
the `nodes` asked for it starts three stream listeners: each publisher feeds a stream, `relay-0` and `relay-1` carry
them, `console-0`, `consumer-0` and `consumer-1` share those two relays, and `direct-0` pulls from the publishers itself.
Each publisher keeps its stream's history on disk, under `axon-playground-<port>` in the system's temporary directory,
emptied at each start.

`docker compose up -d` (repo root, project `axon-playground`) starts a ClickHouse on `127.0.0.1:18123` and a Consul dev
agent on `127.0.0.1:18500`. With `AXON_CONSUL=http://127.0.0.1:18500` the playground discovers its nodes through Consul;
with `AXON_CLICKHOUSE=http://127.0.0.1:18123 AXON_CLICKHOUSE_USER=axon AXON_CLICKHOUSE_PASSWORD=axon-playground` and
`--features clickhouse` its history lives in ClickHouse and survives a restart. Unset, both stay in memory.
`AXON_DASHBOARD_PASSWORD` makes its console ask `admin` to sign in with it, and `AXON_DASHBOARD_IDLE` sets the
session's idle time in seconds.

## HTTP

Every response carries a strict CSP (with `frame-ancestors 'none'`), `nosniff`, `Referrer-Policy: no-referrer`,
outside the hashed `assets/` `no-store`, and over TLS or a trusted https proxy `Strict-Transport-Security`. With users,
every `api/` route needs a session ([Signing in](#signing-in)); the page, its assets and the probes do not. Errors are
`{"available": false, "error": "..."}`, 401 without a session; 429 when 16 requests or 32 event streams are already in
flight.

| Path | Answer |
|---|---|
| `POST login` · `POST logout` | a form with `user` and `password`: 200 and the session cookie, 401, 429 with `Retry-After`, 403 without the console's own `Origin`; `logout` clears the cookie ([Signing in](#signing-in)) |
| `api/session` | `{"user": ..}`, `null` on a console without users |
| `api/overview` | members, each with its latest snapshot (kept, with an `error`, when the node stops answering; without each session's timings and request round trips), history sources, recent events: membership changes and each reachable node's own timeline ([telemetry](telemetry.md#collection-and-history)), pulled on every refresh |
| `api/events` | the same document as SSE `overview` events, serialized once per refresh and shared by every stream; brotli or gzip when the client asks, flushed per event |
| `api/actors?node_id=` | one node's actors: `incarnation` (409 if the node restarted), `id=runtime:slot:generation`, `after`, `q`, `actor_type`, `phase`, `limit`, `timings`; each actor's `diagnostics.dead_letters` counts a supervised family's messages that became dead letters as its generations ended (0 for any other actor, gone by the time it has any) |
| `api/connection?node_id=&peer=&direction=in\|out&lane=control\|live\|bulk` | one session's stage timings and request round trips, `{"timings", "request_rtt"}`, asked of that node; 404 once the node or the session is gone, or the lane is down. The connection on screen polls it |
| `api/inventory` | the cluster's actors, filtered on the nodes, sorted and paged here: the same filters, `sort` (15 keys, `up` and `since_restart` among them, shortest first, and actors that never restarted last under `since_restart`), `order`, `offset`, `limit` ≤ 1000; rows carry `diagnostics` as `api/actors` does; reports `truncated`, per-node `errors`, and `timestamp_ms`, when it was gathered: a cached answer keeps its gathering's time, so rates over two answers stay true |
| `api/streams` | per-stream positions, parents, paths, refused parents and the recent stream events of every reachable node, fetched from the nodes when asked for (`limit` streams per node, 200 by default, at most 1000); per-node `errors` |
| `api/key?kind=&key=` | where one keyed actor runs, `{"owner", "running", "epoch"}`: the node the dashboard node's view places the key on (`Cluster::owner`; while it runs, the node its key watch names; `label.<k>=` the placement's required labels, `null` when no member matches), and whether its actor runs there and under which lease epoch, as the owner answers a key watch within 3 s (502 when it does not; not running also when it cannot be asked). It starts nothing. 403 under `redact`, since the answer would tell of a key no reply shows |
| `api/keys?kind=` | a page of the keys of one kind that run now, `{"keys": [{"key", "node", "epoch"}], "more", "errors"}`: those that start with `prefix`, asked of `node_id` alone or of every reachable node (`Request::Keys` in [telemetry](telemetry.md#live-only)) and merged in key and node order, `limit` ≤ 1000 (100); the next page is `after=` and `after_node=` of the last row. A node that cannot answer, or is from before the request, is in `errors` and the page holds the others. 403 under `redact` |
| `api/history?metrics=a,b` | up to 32 metrics sharing `from_ms`, `to_ms`, `step_ms`, `aggregation`, `node_id`, `label.<k>=`, `max_points`, `max_series`, `sum`, `by`; one round trip to the store, which answers each in columns, summed where asked: the `HistoryResult` of `Query` in [telemetry](telemetry.md), as JSON. `from` is `from_ms` down to a whole step, `count` the steps through `to_ms`; per series its header once and `values`, one per step and `null` where unknown: a gauge's value, a counter's gain over the step, a histogram's gain as sparse `{first, counts, count, sum_ns}`. `sum=node` adds up each node boot's series per step, `sum=cluster` all of them, keeping apart the values of the labels in `by=a,b` (at most 8); a sum has `summed`, how many series it added. `settled`: a refresh asks again only from there |
| `api/series?metric=` | the headers of one metric's series, without values, listed by the store for label suggestions: `node_id`, `from_ms` and `to_ms` (the last hour by default); `truncated` past 128 |
| `api/timeline` | retained cluster events: `from_ms`, `to_ms`, `limit`, and `node` and `kind` to keep one node's or one kind ([event kinds](telemetry.md#collection-and-history)) |
| `api/dead-letters` | the newest dead letters the nodes still hold, and those the metrics store kept ([telemetry](telemetry.md#dead-letters)) |
| `api/traces` · `api/trace?id=` | trace summaries and one trace's spans, from the store where it keeps traces, or the nodes' span rings ([tracing](tracing.md)) |
| `/healthz` · `/readyz` | at the root, whatever `base_path` is, and without credentials: 200 or 503 with the checks ([observability § Health probes](observability.md#health-probes)); under `base_path` they are 404 |

Snapshots are fetched only while someone is watching (a stream is open or the overview or a connection was read in the
last 15 s, on the monotonic clock, so a wall clock stepped back does not keep an unwatched console asking). A console tab
closes its stream and stops polling while it is hidden; shown again, it reopens the stream, whose first event comes at
once. A console from before snapshots dropped their `actors` refuses the overview once any node sends one without them,
so in a rolling upgrade restart the dashboard's node first.

History: every node running a `MetricsService` is probed every 5 s; the healthy one that collected last, by its own
monotonic clock, serves `api/history` and `api/timeline`, and a failed query fails over. `?history=<node>` picks one,
`DashboardConfig::history_node` pins it.

## Signing in

Every user signs in to the same read-only console. A user is a name and an argon2 PHC string, never a password:
`axon_obs::hash_password(password)` makes one (argon2id, 19 MiB, 2 passes, a random salt), and so does any argon2
tool. With no users the console is open, which only a loopback `bind` allows.

The login page posts to `{base}login`, which checks the hash off the async workers and sets a cookie signed with
HMAC-SHA256 over the user, the sign-in time and the last use: `HttpOnly`, `SameSite=Strict`, `Path={base}`, and
`Secure` when the request came over TLS or a trusted proxy says it did. Nothing is kept on the server.
`SessionConfig::idle` (30 min) signs a user out after that long without a request, and each request starts it again;
`SessionConfig::max` (12 h) ends a session however busy it is. A page that refreshes itself (traces, streams, one
actor) is use; the overview's live event stream is not: it ends when its session would, and reconnects only with a
valid one. `POST {base}logout` clears the cookie in that browser; a copied cookie stays valid
until it idles out. `dashboard.sign_out_everyone(key)` is the revocation: every cookie signed before it is refused, and
every live stream ends. Taking a user off `users` refuses their cookies too.

`SessionConfig::key` is the signing key, 32 bytes or more. Left `None` it is random per start, so a restart signs
everyone out. Dashboards behind one balancer need the same key to take each other's cookies, and a rotation gives
each the same new one: `sign_out_everyone(Some(key))` on every node. Each judges a cookie by its own clock, so they must
agree on the time to well within `idle`: a node running ahead signs users out early, one running behind lets them stay
that much longer.

An API call without a valid session gets 401, its `error` saying why: `sign in first`, `signed out: the console
restarted or signed everyone out` (a cookie signed under another key), or `the session expired`. The page then goes
to the login page, which says which of the last two ended the session, and back to the URL it was on after the
sign-in. The page and its hashed assets load without a session: they hold no cluster data. Sign-in attempts
are limited per client address and per user name, on this node: five, then one more every 20 s, and a success gives
its attempt back. Names that are not users share 64 limits, picked by a keyed hash. Past that the answer is 429 with `Retry-After`, even for
the right password. A `POST` to `login` or `logout` must carry an `Origin` (or, lacking one, a `Referer`) naming the
console's own host. Each sign-in, failed sign-in, lockout and sign-out is an event in the console node's timeline
(`login`, `login_failed`, `login_limited`, `logout`), so in the overview and stored like the rest, with the user and
the client address; a name that is not a user shows as
`unknown user`, since it may be a password typed in the wrong field.

## Hosting

Behind a reverse proxy, set `base_path` to the path the proxy forwards and list the proxy in `trusted_proxies`. From
those addresses, and only those, `X-Forwarded-For` names the client (the nearest address that is not a trusted proxy),
`X-Forwarded-Proto: https` marks the request secure, and `X-Forwarded-Host` is the host an `Origin` must name. From
anyone else these headers are ignored. The proxy passes the path unchanged and must not buffer `api/events`:

```text
location /ops/axon/ {
    proxy_pass http://10.0.0.5:8080;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_buffering off;
}
```

Without a proxy the dashboard serves HTTPS itself from your certificate and key, as a rustls `ServerConfig` like the
cluster's. `dashboard.set_tls(config)` swaps it while it runs: handshakes after the call get the new certificate,
connections already open keep theirs. It is refused on a dashboard started without TLS. The dashboard speaks
HTTP/1.1, so leave ALPN unset. Handshakes have 10 s to finish, and one address has at most 16 under way (a trusted proxy
has no share, so the port's 64 under way are its only bound), so a client that opens connections and never handshakes
holds up no one else.

Every port, plain, TLS or a `Health` one, holds at most 256 connections, and one address at most 64 of them; a
trusted proxy has no share, so the port's 256 are its only bound. One past either is closed as it is accepted, and its
place frees when a connection ends. A request head must arrive within 10 s and fit in 16 KiB (a longer one gets 431),
a kept-alive connection has the same 10 s to start its next one, and an answer the client stops reading is cut after
10 s without progress, so a client that connects and sends nothing, trickles its head a byte at a time, or asks and
never reads, holds a place for 10 s at most. The one body read is a sign-in: 4 KiB at most (413 past it), within 10 s
(408 past it). None of these is set in `DashboardConfig`: behind a proxy, the proxy's own limits come first.

```rust,no_run
# use std::sync::Arc;
# use axon_net::rustls::{ServerConfig, crypto::ring, pki_types::{CertificateDer, PrivateKeyDer}};
# use axon_obs::DashboardConfig;
# fn https(certs: Vec<CertificateDer<'static>>, key: PrivateKeyDer<'static>) -> Result<DashboardConfig, Box<dyn std::error::Error>> {
let tls = ServerConfig::builder_with_provider(Arc::new(ring::default_provider()))
    .with_safe_default_protocol_versions()?
    .with_no_client_auth()
    .with_single_cert(certs, key)?;
let cfg = DashboardConfig { tls: Some(Arc::new(tls)), ..Default::default() };
# Ok(cfg)
# }
```

`DashboardConfig::redact` hides keys in the API's replies: an actor name `kind/key` shows as `kind/…`, and so do a
span's name, a dead letter's target and an event's subject. A key a snapshot lists (`TelemetryConfig::snapshot_keys`) shows as `…` beside its kind, and `api/keys` and `api/key` refuse, so the placement page shows the kinds and the counts on each node only: a node's drawer says it could not list the keys, and the key search finds none. A stream's name is its key, so it shows as `…` and eight hex digits of an HMAC
under the session key beside its namespace, the same on every node until that key changes, and a stream span, named
`namespace/name`, as `namespace/…`. Searches see only what the replies show, and names sort by it; a trace search then
covers the newest 1000 traces in its range, since the store cannot search what it would hide.

## UI

`crates/axon-obs/ui` uses React, React Router, Zustand for the live stream, TanStack Query for history and
inventory, Tailwind v4, ECharts in its own chunk, fetched beside the first one, and self-hosted fonts. Pages other than the overview load when first opened. Every answer but a font or a raster image is compressed, brotli or gzip, as the browser offers. All view state (selection, filters, sort,
ranges, topology options) is in the query string; only the theme, accent and sidebar width are in `localStorage`. A page has one time range (`range`);
panels inside it follow it. A table is one tab stop: arrows or `j`/`k` move between rows, Enter opens one. A graph is one tab stop too: arrows move between its nodes and edges. The sidebar keeps `history`, so a picked history source follows every page. Status colours (connected, running, healthy) are their own green whatever the accent, and the topology's measurements default to the Ocean ramp, so a busy edge never reads as an error.
The sidebar groups the pages: Overview; Cluster (cluster, nodes, connections, placement); Actors (actors, workload, dead letters); Streams; Investigate (events, traces, explore); Settings and, with users, the one signed in and Sign out at its foot. Every page explains itself from the ⓘ button in the top bar. Every table exports what it shows: the download icon among its controls opens a dialog naming the filters in force, the row count, and a JSON or CSV choice, one dataset per file. The actor inventory export re-runs the table's query page by page up to the 20,000 rows the server gathers and says when the cluster holds more. Nodes, connections, cluster counters, events, traces, dead letters and stream nodes export the same way. The top bar's download is the raw overview document for support, behind the same confirmation. The top bar's status chip is a dot and the age of the last overview (amber and named when the view is stale, named while polling or connecting, and "live only" in amber while no metrics store answers); it opens on the connection, how far back the chosen metrics store's history reaches, and the history source menu. The overview carries the served bundle's hash; a tab on another build shows a toast offering a refresh. The history source menu flags a collector that is losing data; its counters (cycles, stored, dropped, write errors, gaps, failing sinks) are under Metrics stores in Settings, with what each sink holds per table, the room left and when it fills ([telemetry § Collection and history](telemetry.md#collection-and-history)).

Routes: `overview` (it opens on the attention tab; its topology panel's expand button opens `overview/topology`, the graph full size with its layouts, measurements and colour maps, and the old `topology` route goes there with its query; the six tiles, the node list, the topology, the inspector and the activity tabs; the attention tab runs fixed rules over the live overview: unreachable, stale or draining nodes, nodes lost (gone from the membership without the console seeing them drain, danger; a leave keeps its draining record up for a `poll_interval`, so only one whose shutdown budget ran out sooner is taken for lost) or left (with their reason, a note) in the last 15 minutes, isolation, a supervisor that gave up, mailboxes with a message older than five seconds, streams without a parent with the keys and the last parent lost, a failing stream disk, a collector losing data; and each counter that grew in the last minute, with since when it has kept growing (`or earlier` when that is where this tab's samples begin): leases lost, failed handoffs, restarts, mailbox refusals, dead letters, quota limit denials, quota share denials (only above 1/s over the minute: a node's share running out before the replicas rebalance is normal now and then), stream namespace limits, handshakes refused for the wire version, and listener refusals (naming the handshake failures by reason). Departures are rebuilt from the `left` events on a fresh load, and the nodes tile reads `N lost <ago>`), `cluster` (the members on top: members and unreachable as the console sees them, nodes not ready, and nodes lost or left in the last 15 minutes, then a square per node coloured by its state and the nodes that need attention; then the discovery provider and the ownership authority, `Not configured` without one, as the reporting node reports them: their settings, live figures and seed links, the leases summed over the nodes, and with Paxos leases a row per acceptor from its own snapshot: active or warming, the leases it promised, its refusals, the ballots ahead of its clock and its floor; then Nodes by subsystem, a row per node and a cell for each of membership, discovery, leases, quotas and calls, green when normal, amber to examine and red for a fault now, the nodes with a problem first, under a Cluster row of totals; a cell opens a drawer with that node's counters and the reason for its colour, `node=` and `sub=`, and a column title opens the same counters over all nodes: membership as the reporting node saw it, the rest summed, readiness from each node's `/readyz` checks, and for quotas the denials by limit and by share over the last hour), `placement` (a card per node in a frame per region, on a board to pan and zoom, where a card dragged stays where this browser put it; each card counts the keys its node runs, from its snapshot, with a bar by kind, the keys that came in and went out in the window, `win=` (2m, 15m or 1h), and a red ring where keys failed, lost their lease or stopped in isolation; a line joins two nodes that keys moved between, its dots moving the way the keys went, from each node's start and end counters since the tab opened, so the source of a move is an estimate, and Keys that moved lists them; a chip per kind counts its keys, eager or not, and keeps the board to that kind, `kind=`; a card opens the node's drawer, `node=`: its keys by kind, its moves, and its keys of one kind from `api/keys`, a page of 100 at a time in key order with the lease epoch each started under, narrowed by the start of a key, each linked to its actor; the key search shows the first running key that starts with what is typed and opens its node, else asks `api/key` for that exact key: one not running shows the node that would start it and opens that node's drawer, or that it has no node), `explore` (any stored metric of [telemetry § Persisted samples](telemetry.md#persisted-samples) over the page's range, found by a search over each metric's name and a line on what it counts: a counter or a histogram as its rate, a gauge as its value, added over the series (the highest of them for a ratio, a percentage or a duration) or split by node or by a label, kept to one node and to `k=v` labels, picked from every pair the metric's series carry; below it a chart for each other split, by node or by a label, and for a histogram its quantiles per step, its distribution over the range and a heatmap of it; every choice is in the query string, so a chart is a link), `nodes` (with a Ready column, the `/readyz` checks from each reachable node's snapshot: shutting down, isolated, warming, quotas starting; a badge over the table counts the nodes not ready; and the wire versions each node speaks, lowest..highest), `nodes/:node` (its charts draw the boot before a restart faded behind a dashed restart marker; a node that left keeps its last report and live samples for 15 minutes), `actors`, `actors/:node/:actor`, `workload` (a backlog heatmap from stored history, one row per node and actor type, at most 128, each cell the deepest mailbox in its step over the last 15 minutes; and for one actor type the handler time sampled per message label, added over the nodes: which messages cost time, not a stack profile), `connections` (each session, a row per peer, direction and lane, with its wire version and the bytes it has sent and received since it opened, a lane that is down a row that says for how long, sortable and filtered by a minimum; the listener's refusals below, handshake failures by reason),
`connections/:node/:peer/:in|out/:lane` (its lane, wire version, traffic, round trips, kernel TCP health, refused frames, stage timings), `streams`
(the flow graph from origins to leaves with per-edge rates and, on each edge's title, the parent's window to that child: in flight, window, round trip, rate and pushes resent; a table view, and per node its trends and counters (record bytes its rings and compacted state retain, interests sent, failed and resent whole, pushes resent, waits for the node cap and pushes not applied by reason, refusals at a namespace's limit by limit, and at an origin with history on disk its streams, bytes, unwritten bytes, writes, refused publishes, truncated segments and a failing write's error), streams with lag
and recent stream events; nodes appear once they call
`Telemetry::watch_streams`), `dead-letters` (dead letters per second per node over the last hour, then what was dropped, for whom, why, from which node, and its trace; `group=target` folds the rows by target), `traces` (the newest 500 in the range, asked again every 5 s up to a 15-minute range and less often over a longer one, each minute from 6 hours), `traces/:trace` (the waterfall; `span=` is the open span), `events` (a swimlane per node over the range, each event a
mark coloured by its kind, above the table; clicking a mark filters the table to its node and kind, `en=` and `ek=`), `settings`,
`login` (outside the shell; `?next=` is the console path it returns to, and a signed-in visitor goes straight there).
A node with `country` and `region` labels ([cluster § Spawn actors on other nodes](cluster.md#spawn-actors-on-other-nodes-one-per-key)) shows its flag and region wherever it is named.

`npm run build` (then rebuild the crate) · `npm run lint` · `npm test` (model) · `npm run e2e` (Playwright against three
real playgrounds: one mounted under `/ops/axon/` with a node joining and leaving, one that asks for a sign-in) · `npm run dev` proxies `api/` to
`AXON_DASHBOARD_API` (default `http://127.0.0.1:8080`).
