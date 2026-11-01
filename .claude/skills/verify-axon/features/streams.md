# Streams

## Sub-features

- graph: stream nodes as cards (origin, relay, leaf), with flows from parent to child that carry rates.
- table: the same data as rows (`?sview=table`), showing each node's kind, parents and rate.
- side panel: one node's counters (retained bytes, interests), history on disk where the node is an origin with
  `disk` set (`[data-disk]`, publishers in the playground), parents, downstream, resets and refusals (by a parent, at a
  namespace limit), plus its streams and events.

## How to get to it (user POV)

Sidebar, then Streams. Click a card or a row to select it (`?snode=<node>`). Use the Graph and Table toggle at the top.
You can drag a card out of the layout, and Reset layout puts it back.

## Driving it with ctl

```bash
scripts/ctl.sh api streams | jq '.nodes | keys'   # an object keyed by node, plus per-node errors
scripts/ctl.sh shot streams --wait '[data-flow]'
scripts/ctl.sh shot 'streams?sview=table&snode=publisher-1' --wait 'tbody tr'
```

- Cards are `[data-stream-node="<node>"]`, with `data-kind` set to `origin`, `relay` or `leaf`.
- Flows are `[data-flow="<parent>><child>"]`, with `data-rate` and `data-joining`.
- The savings figure is `[data-saving]` ("saves N%").

## Gotchas

- Right after `up`, the graph says "No stream flows" for a few seconds. Always `--wait '[data-flow]'`.
- Expected shape: publishers feed the relays only, `console-0`, `consumer-0` and `consumer-1` pull from a relay, and
  `direct-0` pulls from the publishers itself. A flow that breaks this is a bug, not noise.
- A rate reads 0 until two observations have arrived. Poll `data-rate` rather than screenshotting at once.
