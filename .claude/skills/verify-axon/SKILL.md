---
name: verify-axon
description: Prove a change in the running system, not only in tests. Starts your own playground cluster with scripts/ctl.sh, checks it, queries the dashboard HTTP API, and screenshots or clicks through the console. Use after changing anything the dashboard, telemetry, metrics, streams or cluster show, when reproducing a report against a live cluster, or when asked to show proof.
---

# Verify axon live

`scripts/ctl.sh` drives a playground: one process holding a small cluster with traffic, streams, traces, dead letters
and the dashboard. Every command prints JSON and exits non-zero on failure, with the next step in `error`.
`scripts/ctl.sh help` lists the commands.

## Launch

```bash
scripts/ctl.sh up                        # 3 nodes on the first free port from 8100; prints {port, pid, url, user, password, log}
scripts/ctl.sh up 6 --base /ops/axon/ --churn   # the e2e shape: more relays, mounted under a path, visitor-0 joins and leaves
```

`up` builds first, so it always runs the checkout as it is. After a UI change run `npm run build` in
`crates/axon-obs/ui` before `up`. The build picks up the new `dist` by itself.

Runs belong to the checkout (`target/ctl/runs/<port>`), and two can run side by side. Pass `-p <port>` when more than
one is up. Never drive a port you did not start. The e2e suite's 8097, 8098 and 8099 are not yours.

The console asks for a sign-in. `api`, `doctor` and `shot` do it with the run's password; a browser you drive yourself
signs in as `admin` with the password `up` printed (also in `target/ctl/runs/<port>/password`).

## Doctor

```bash
scripts/ctl.sh doctor    # {ok, process, owns_port, overview, nodes, reachable}
```

Run it before the first drive and after anything surprising. If `ok` is false, read `logs`. If the cause is not your
change, `down` and `up` again. Do not debug a run that was already broken.

## Drive

```bash
scripts/ctl.sh api overview                          # any route in crates/axon/guide/dashboard.md#http, JSON on stdout
scripts/ctl.sh api 'inventory?q=relay&limit=5'
scripts/ctl.sh shot streams --wait '[data-flow]'     # waits for the status chip to read Connected, then the selector, then shoots
scripts/ctl.sh shot overview --steps /path/click.mjs # the file: export default async (page, expect) => { ... }
```

A steps file holds only the interaction. It gets a Playwright `page` and `expect` and needs no imports. A failed
`expect` or a timeout fails the shot and still saves the screenshot of the page as it was. `shot` also fails on any
console error the page logs.

Use the selectors and names in [features/](features/README.md). They come from `crates/axon-obs/ui/tests/dashboard.spec.ts`,
so they are known to work. Streams, traces and dead letters need 5 to 40 seconds after `up`, so always `--wait` for them.

## Evidence

- Screenshots land in `target/ctl/evidence/` and survive `down`. Name the ones you report with `--out`.
- Prove the user's path. Show the action and the state it produced (a `shot` after `--steps`, or `api` before and after),
  not just a page that loads.
- For a data change, show the number in `api` output and on the page it feeds.
- A behaviour that must keep holding becomes a test in `dashboard.spec.ts`, or `login.spec.ts` for signing in (run with
  `npm run e2e` in `crates/axon-obs/ui`; it starts its own playgrounds on 8097, 8098 and 8099 and fails if something
  else holds them).
- Report what you ran, the JSON that came back, and the screenshot paths. Look at each screenshot before you cite it.

## Cleanup

```bash
scripts/ctl.sh down        # the one run, or -p <port>
scripts/ctl.sh down all    # every run from this checkout
```

`down` stops only the PIDs ctl started, and only if they are playgrounds. A failed `up` cleans up after itself and
keeps its log in `target/ctl/evidence/up-<port>.log`. End every session with `ls` showing `[]`.

## Beyond the console

The playground exercises the runtime, the cluster and streams together, but it is not the only proof. Library behaviour
is proven by the tests and simulations in the AGENTS.md feature map, and performance by the harnesses named in
`crates/axon/guide/benchmarks.md`. With `docker compose up -d`, `AXON_CONSUL=http://127.0.0.1:18500 scripts/ctl.sh up` runs the
playground on Consul discovery, and `AXON_CLICKHOUSE=...` (see crates/axon/guide/dashboard.md) runs it on ClickHouse history.
