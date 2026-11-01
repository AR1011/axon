# Actors

## Sub-features

- inventory: every actor in the cluster, filtered, sorted and paged on the server (`api/inventory`).
- actor detail: mailbox, sampled timings (mailbox wait, handler duration, execution turn) and processed/admitted/rejected
  charts.
- dead-letter mark: an actor that lost messages carries a "N dead letters" link to its dead letters.
- uptime: Up (since the actor or its supervised family was spawned) and Since restart (shown after a restart), sortable
  on the server (`up`, `since_restart`), and both on the actor page.

## How to get to it (user POV)

Sidebar, then Actors. Search, filter by node, sort by a column header, page with "Next page". Clicking a row opens the
actor. Its URL carries the node's `incarnation`. An old incarnation shows "Actor belongs to a previous engine
incarnation".

## Driving it with ctl

```bash
scripts/ctl.sh api 'inventory?q=relay&sort=processed&limit=5' | jq '{total, truncated, first: .actors[0]}'
scripts/ctl.sh api 'actors?node_id=console-0&limit=5'
scripts/ctl.sh shot 'actors?aq=flaky' --wait 'role=link[name=/^\d+ dead letters$/]'
```

- The inventory is `getByLabel("Actor inventory")`, and its search box is `getByLabel("Search actors")`.
- Rows that open an actor are `tbody tr[data-href]`.
- The inventory's URL keys are `aq` (search) and `asort` (sort).

## Gotchas

- `api/actors` needs `node_id`, and answers 409 once that node has restarted.
- `flaky` lives on `console-0`, and its mark takes up to 25 s to appear.
