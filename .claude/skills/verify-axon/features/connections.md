# Connections and events

## Sub-features

- connections: every transport link, a row per peer, direction and lane (Control, Live, Bulk), and the wire version it runs, with rates computed from consecutive observations
  and a transport diagnostics pane; the footer sums the listeners, handshake failures by reason.
- connection page: its lane and rate wait (the writer's wait for the lane's cap), framed traffic, round trip, TCP retransmissions, congestion window, frames, queued bytes and
  write + flush for one link.
- events: retained cluster events (joins, leaves, restarts, and each node's own: keyed actors acquired, lost, handed
  off; supervised restarts; stream parent losses and resets) with a per-node swimlane, a range picker, search, and node
  and kind filters.

## How to get to it (user POV)

Sidebar, then Connections. Clicking a row selects it (`?conn=`), and "Open connection" goes to its page. Sidebar, then
Events. Pick a range, search, then filter by node (`?en=`) and kind (`?ek=joined`); clicking a swimlane mark sets both.

## Driving it with ctl

```bash
scripts/ctl.sh shot connections --wait 'role=img[name=/Sent and received bytes per second/]'
scripts/ctl.sh shot connections/console-0/publisher-1/out/control --wait 'text=Congestion window'
scripts/ctl.sh shot connections/console-0/publisher-1/out/bulk --wait 'text=Rate wait'   # the playground caps Bulk at 256 KiB/s
scripts/ctl.sh api 'timeline?limit=20'                # a JSON array; empty without --churn
scripts/ctl.sh api 'timeline?node=visitor-0&kind=joined'
scripts/ctl.sh shot events --wait 'tbody tr'          # needs a run started with --churn
```

- The diagnostics pane is `getByRole("region", { name: "Transport diagnostics" })`.
- The event search is `getByLabel("Search events")`. The pickers are `getByRole("combobox", { name: "Event range" })`
  , `getByRole("combobox", { name: "Event node" })` and `getByRole("combobox", { name: "Event kind" })`. The swimlane is
  `getByTestId("event-lanes")`.

## Gotchas

- Events are retained only after the metrics service starts, so the joins at startup are not there. A plain `up` has an
  empty events page. Start with `--churn` so `visitor-0` keeps producing join and leave events.
- Rates need two observations, so the charts appear up to 15 s after the page opens.
- The TCP figures come from the kernel (macOS and Linux). A missing field on another OS is expected.
- A link that is not in the current view shows "Connection not in the current view".
- Only console-0 sends on Bulk, so each Bulk row has console-0 at one end. A lane that is down has no
  session: its row says `Down` and for how long, and its page has no diagnostics.
