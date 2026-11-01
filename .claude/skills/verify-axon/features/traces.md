# Traces and dead letters

## Sub-features

- traces: a list of trace summaries with search and an Outcome filter, all kept in the URL.
- trace detail: a span waterfall across nodes. Each span has a mailbox/handler tooltip, an expandable row (`?span=`) and a
  link to its actor. "How to read this" opens a help dialog.
- dead letters: what was dropped, why, and where. Each row links to its trace.

## How to get to it (user POV)

Sidebar, then Traces, then click a row. Sidebar, then Dead Letters. An actor's "N dead letters" mark also opens that
page.

## Driving it with ctl

```bash
scripts/ctl.sh api traces | jq '.traces[:3]'                    # each has .trace (the id), .root, .nodes, .spans
scripts/ctl.sh api "trace?id=$(scripts/ctl.sh api traces | jq -r '.traces[0].trace')" | jq '.spans | length'
scripts/ctl.sh api dead-letters | jq '.letters[:3]'              # .target, .label, .reason, .node_id, .trace
scripts/ctl.sh shot 'traces?q=request' --wait 'tbody tr'
scripts/ctl.sh shot 'dead-letters?q=work' --wait 'tbody tr'
```

- Spans are `getByRole("list", { name: "Spans" }).getByRole("listitem")`.
- `traces?q=request` finds cluster calls. `traces?q=playground` finds stream publishes.

## Gotchas

- The first traces and dead letters take up to 40 s after `up`.
- `traces/00000000000000aa` shows "Trace not found". That page is the not-found state, not a failure.
