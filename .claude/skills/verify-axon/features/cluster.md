# Cluster health

## Sub-features

- overview: the summary tiles, an attention list, a small topology and the node inspector. The topology's expand
  button opens `overview/topology` (the old `topology` route redirects there).
- cluster: the members on top (`getByLabel("Members", { exact: true })`: five figures, a square per node, the nodes that
  need attention), then the discovery and ownership cards as the reporting node reports them (`[aria-label^="ownership"]`;
  `ownership: Not configured` without an authority; under Paxos a table `[aria-label="Acceptors"]`), then
  `[aria-label="Nodes by subsystem"]`: a row per node with a button per subsystem, named `<node> <subsystem>: <reasons or
  normal>` (`Cluster quotas: …` on the totals row). A cell opens the drawer `[aria-label="Details"]` (`?node=&sub=`), a
  column title the same counters over all nodes (`?sub=`). The drawer covers the columns on its right: Escape closes it.
- overview/topology: the full graph of nodes and links, with a colour map and layouts.
- nodes: a sortable table of members, including nodes that left. `nodes/<node>` has tabs for charts, metrics,
  connections, distributions, events and metadata. Above the charts is a strip of worker utilisation per pool: the
  shared one (`worker 0 …`) and each named pool (`relays-0 …`, named as its threads are).

## How to get to it (user POV)

The app opens on Overview. The sidebar's "Main navigation" groups its links (`getByRole("group", { name: "Cluster" })` holds
cluster, nodes, connections and placement). The top bar's status chip is `getByRole("button", { name: /^Connected, updated/ })`
and opens the "Console status" dialog with the history source menu. Clicking a node row
opens its page. Selection and search live in the URL (`?node=relay-1`, `?tab=actors`), so a link reproduces a view.

## Driving it with ctl

```bash
scripts/ctl.sh api overview | jq '[.nodes[] | {id: .node.id, reachable}]'
scripts/ctl.sh shot 'overview?node=relay-0&labels=on' --wait 'text.edge-label'
scripts/ctl.sh shot nodes/publisher-1 --wait 'canvas'
scripts/ctl.sh shot overview/topology --wait '[data-node]'
```

- The inspector is `getByLabel("Node inspector")`. The node search box is `getByLabel("Search nodes")`.
- The summary tiles are `getByLabel("Cluster summary")`. They show only on overview, nodes and actors.
- Node tabs are `getByRole("tab", { name: "metrics" })`, and likewise for the others.

## Gotchas

- `nodes/<node>` fetches all its charts in one `api/history` request. A change that adds a second request per chart is a
  regression, and the e2e test checks it.
- A node that left stays in the table as "Left Ns ago". Only `--churn` shows this, and it takes up to 40 s.
