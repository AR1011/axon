import type { ReactNode } from "react";
import { HelpDialog, Part } from "../components/HelpDialog";

export const Box = ({ children, tone = "" }: { children: ReactNode; tone?: string }) => <span className={`rounded-sm border border-border px-1.5 py-0.5 ${tone}`}>{children}</span>;
const Arrow = ({ label = "" }: { label?: string }) => <span className="mono text-[10px] text-muted">{label} →</span>;
const Flow = ({ children }: { children: ReactNode }) => <span className="flex flex-wrap items-center gap-1.5">{children}</span>;
const Dotted = ({ tone, children }: { tone: string; children: ReactNode }) => (
  <span className="flex items-center gap-1.5">
    <span className={`size-[5px] rounded-full ${tone}`} />
    {children}
  </span>
);
const HelpMeter = ({ value, tone = "bg-accent" }: { value: number; tone?: string }) => (
  <span className="block h-1 rounded-full bg-raised">
    <span className={`block h-1 rounded-full ${tone}`} style={{ width: `${value}%` }} />
  </span>
);

export function EventsHelp() {
  return (
    <HelpDialog button="What is this" title="What the events are">
      <Part
        title="A change in who is in the cluster"
        picture={
          <span className="flex flex-col gap-1">
            <Dotted tone="bg-accent">joined · reachable</Dotted>
            <Dotted tone="bg-muted">leaving · left</Dotted>
            <Dotted tone="bg-warning">restarted · unreachable · discovery unavailable · resync</Dotted>
          </span>
        }
      >
        Green shows an arrival or a recovery. Grey shows a departure. Amber shows a problem. Each row is one event that a collector saw: discovery reported a node, or the session from this node to a peer stopped or recovered.
      </Part>
      <Part
        title="Leaving is an intention, left is a fact"
        picture={
          <Flow>
            <Box>leaving</Box>
            <Arrow label="drain" />
            <Box>left</Box>
          </Flow>
        }
      >
        A node that stops correctly announces it first, with its reason in the detail. A node that crashes shows unreachable, then left when discovery removes it. Restarted means that a new process started with the same name.
      </Part>
      <Part
        title="What happened on a node"
        picture={
          <span className="flex flex-col gap-1">
            <Dotted tone="bg-accent">acquired</Dotted>
            <Dotted tone="bg-muted">handed off</Dotted>
            <Dotted tone="bg-warning">lease lost · eager restart · actor restarted · parent lost</Dotted>
          </span>
        }
      >
        Each node also sends its own events. A keyed actor starts under its lease, loses the lease, goes to another node, or restarts on a node that is still in the cluster. A supervised family restarts, and the node tells it at most one time each minute, with the count. A stream loses its parent. The lanes above the table show the events of each node. Click a mark to keep only that node and kind.
      </Part>
      <Part
        title="Live session or retained history"
        picture={
          <span className="flex flex-col gap-1 text-[10px] text-muted">
            <span className="mono text-text">Live session · 15m · 1h · 24h</span>
            <span>ranges need a metrics store</span>
          </span>
        }
      >
        Live shows the events that this browser tab received after it opened. With a metrics store, the range menu shows stored events. These events stay after a reload and after a restart of the console.
      </Part>
    </HelpDialog>
  );
}

export function TracesHelp() {
  return (
    <HelpDialog button="What is this" title="What a trace is">
      <Part
        title="One message and all that it caused"
        picture={
          <Flow>
            <Box>orders</Box>
            <Arrow label="request" />
            <Box>billing</Box>
            <Arrow label="tell" />
            <Box>mailer</Box>
          </Flow>
        }
      >
        A trace starts where the application asks for one. When sampling is on, a trace also starts on one message in N. All that the message causes, on any node, has the same id: handlers, calls to other nodes, stream batches, timers and jobs.
      </Part>
      <Part
        title="A row is a trace, named by how it started"
        picture={
          <span className="flex flex-col gap-1">
            <span className="flex gap-3"><span className="w-14">request</span><span className="text-muted">orders → billing</span></span>
            <span className="flex gap-3"><span className="w-14">publish</span><span className="text-muted">orders → prices/eurusd</span></span>
            <span className="flex gap-3"><span className="w-14">work</span><span className="text-muted">orders → relay-0</span></span>
          </span>
        }
      >
        Root is what the first span did. From is the node where it ran. To is the peer, stream or actor that it was for. "Not held" means that the first span is gone: a ring removed it, or that node left.
      </Part>
      <Part
        title="Duration is first arrival to last end"
        picture={
          <span className="flex flex-col gap-2">
            <HelpMeter value={90} />
            <HelpMeter value={35} />
            <HelpMeter value={12} tone="bg-danger" />
          </span>
        }
      >
        The slowest trace in the list sets the scale of the bars. Red means that a handler or a call in the trace failed. Spans and Nodes count what the trace touched. Open a row to see the waterfall.
      </Part>
      <Part
        title="Where the list comes from"
        picture={
          <Flow>
            <Box>node rings</Box>
            <Arrow label="collector" />
            <Box>metrics store</Box>
          </Flow>
        }
      >
        Each node keeps its newest spans in memory. The collector copies them to the metrics store every few seconds, and the list reads the store. Without a store, the list asks the nodes for the spans that they still keep.
      </Part>
    </HelpDialog>
  );
}

export function StreamsHelp() {
  return (
    <HelpDialog button="What is this" title="How streams flow">
      <Part
        title="Origin, relay, leaf"
        picture={
          <Flow>
            <Box>origin</Box>
            <Arrow />
            <Box>relay</Box>
            <Arrow />
            <span className="flex flex-col gap-1">
              <Box>leaf</Box>
              <Box>leaf</Box>
            </span>
          </Flow>
        }
      >
        An origin publishes a stream. Each other node pulls each stream one time, from a parent that it prefers, and sends it to its own subscribers. A node that also sends to other nodes is a relay. A node that only receives is a leaf.
      </Part>
      <Part
        title="Why a tree"
        picture={
          <span className="flex flex-col gap-1.5 text-[10px] text-muted">
            <span>origin sends</span>
            <HelpMeter value={30} />
            <span>if every leaf pulled from it</span>
            <HelpMeter value={95} tone="bg-warning" />
          </span>
        }
      >
        The origin sends each batch to a few children, not to each listener. With more than one leaf, the header compares what the origins send with what they must send if each leaf pulls directly.
      </Part>
      <Part
        title="What the numbers are"
        picture={
          <span className="mono flex flex-col gap-1 text-[10px]">
            <span>Records/s <span className="text-muted">published or delivered here</span></span>
            <span>In · Out <span className="text-muted">payload between nodes</span></span>
            <span>Resets <span className="text-muted">subscriptions restarted</span></span>
          </span>
        }
      >
        The counters are for each node, never for each stream. In and Out are the bytes of stream payload on the links between nodes. They do not include what local subscribers read.
      </Part>
      <Part
        title="The window to a child"
        picture={
          <span className="flex flex-col gap-1.5 text-[10px] text-muted">
            <span>in flight, of the window</span>
            <HelpMeter value={60} />
          </span>
        }
      >
        A parent sends pushes to a child and does not wait for each answer. The window is the number of bytes that the parent can send before the child answers. It is two times the rate multiplied by the round trip, and it is never smaller than two pushes. In flight is the bytes that the child did not answer yet. Rate is the highest recent rate at which the child answered. Resent is the pushes that the parent sent again after a failure or a gap. Hover over a child's edge to see these values.
      </Part>
      <Part
        title="How a problem shows"
        picture={
          <span className="flex flex-col gap-1">
            <Dotted tone="bg-warning">without a parent</Dotted>
            <Dotted tone="bg-warning">resets · refused by a parent</Dotted>
          </span>
        }
      >
        A node without a parent needs a stream, but it has no node to pull it from. A reset restarts a subscription. The cause can be a slow consumer, a gap or a new generation. Select a node to see its reasons, its streams with their lag, and its recent stream events.
      </Part>
    </HelpDialog>
  );
}

export function ConnectionsHelp() {
  return (
    <HelpDialog button="What is this" title="What a peer connection is">
      <Part
        title="Up to three connections between two nodes"
        picture={
          <Flow>
            <Box>node a</Box>
            <span className="mono flex flex-col text-[10px] text-muted">
              <span>⇄ control</span>
              <span>⇄ live</span>
              <span>⇄ bulk</span>
            </span>
            <Box>node b</Box>
          </Flow>
        }
      >
        Each peer has up to three connections, one for each lane. Control sends cluster messages. Live sends calls. Bulk sends large transfers. A slow Bulk connection does not stop Live or Control. A lane can have a rate cap. Rate wait on its page is the time that its writes waited for the cap. Each connection is a TCP session. It is authenticated, and its compression is optional. The node that needs a connection first dials it. When the connection stops, that node dials it again in the background.
      </Part>
      <Part
        title="Both ends report it"
        picture={
          <span className="mono flex flex-col gap-1 text-[10px]">
            <span>a <span className="text-muted">outgoing →</span> b</span>
            <span>b <span className="text-muted">← incoming</span> a</span>
          </span>
        }
      >
        Thus one connection can show two times, one time from each endpoint. Rates are what that endpoint counted: framed bytes on the wire, after compression.
      </Part>
      <Part
        title="Queue and RTT"
        picture={
          <span className="flex flex-col gap-1.5 text-[10px] text-muted">
            <span>queue</span>
            <HelpMeter value={20} />
            <span>queue, the peer is too slow</span>
            <HelpMeter value={85} tone="bg-warning" />
          </span>
        }
      >
        Queue is the frames that wait for their write to the peer. A queue that stays high means that the link or the peer is the bottleneck. RTT is the round trip of the last keepalive ping on that session.
      </Part>
      <Part
        title="Open one for the detail"
        picture={
          <span className="flex flex-col gap-1 text-[10px] text-muted">
            <span className="text-text">traffic · round trips · TCP health</span>
            <span>refused frames · stage timings</span>
          </span>
        }
      >
        Select a row to show its telemetry next to the table. Enter or ↗ opens the full page. That page shows the kernel TCP health where the platform reports it, and the reason for each refused frame.
      </Part>
    </HelpDialog>
  );
}

const Tile = ({ label, value }: { label: string; value: string }) => (
  <span className="flex flex-col rounded-sm border border-border px-2 py-1">
    <span className="text-[9px] text-muted">{label}</span>
    <span className="mono text-[11px]">{value}</span>
  </span>
);

export function OverviewHelp() {
  return (
    <HelpDialog button="What is this" title="What the overview shows">
      <Part
        title="Six tiles, one cluster"
        picture={
          <span className="grid grid-cols-3 gap-1">
            <Tile label="Nodes" value="9" />
            <Tile label="Actors" value="73" />
            <Tile label="Connections" value="76" />
          </span>
        }
      >
        Each tile adds the values of the nodes that answered. Its sparkline shows the last hour, from retained history or from the data that this tab received. The cluster tile shows the discovery and ownership providers and opens the cluster page.
      </Part>
      <Part
        title="Select a node, the inspector shows it"
        picture={
          <Flow>
            <Box>node list</Box>
            <Arrow label="select" />
            <Box tone="border-accent">inspector</Box>
          </Flow>
        }
      >
        The list and the topology select the same node, and the URL keeps it. The inspector shows the resources of that node and one of its sessions. The activity tabs below show connections, actors and events for all nodes.
      </Part>
      <Part
        title="Attention shows what to examine first"
        picture={
          <span className="flex flex-col gap-1">
            <Dotted tone="bg-danger">unreachable · isolated</Dotted>
            <Dotted tone="bg-warning">stale · draining · backlog · refused peers</Dotted>
            <Dotted tone="bg-muted">left · replay · collection gap</Dotted>
          </span>
        }
      >
        The attention tab applies fixed rules to the live overview. It shows nodes that do not answer, nodes that the cluster lost or that left in the last 15 minutes, and a supervisor that stopped its restarts. It shows mailboxes with a message older than five seconds, streams without a parent, a stream disk that fails and a collector that loses data. It also shows each failure counter that increased in the last minute: leases lost, restarts, refusals, dead letters and denials. Normally the tab is empty.
      </Part>
    </HelpDialog>
  );
}

export function TopologyHelp() {
  return (
    <HelpDialog button="What is this" title="How to read the topology">
      <Part
        title="A node is a circle, a session is a line"
        picture={
          <Flow>
            <Box tone="rounded-full">relay-0</Box>
            <Arrow label="dialled" />
            <Box tone="rounded-full">console-0</Box>
          </Flow>
        }
      >
        Each peer session that a node reports is an edge. The edge starts at the node that dialled. Two nodes usually share one session. A double line means that each node dialled before the other accepted.
      </Part>
      <Part
        title="Width is traffic, colour is health"
        picture={
          <span className="flex flex-col gap-1.5">
            <HelpMeter value={80} />
            <HelpMeter value={25} tone="bg-warning" />
          </span>
        }
      >
        An edge becomes wider with more bytes per second in the last few samples. Amber shows a session with retransmissions or with an outbound queue that increases. Grey shows a session that the node reported as closed.
      </Part>
      <Part
        title="Layouts and focus"
        picture={
          <Flow>
            <Box>force</Box>
            <Box>circular</Box>
            <Box>grid</Box>
            <Arrow label="focus" />
            <Box tone="border-accent">one node</Box>
          </Flow>
        }
      >
        Force puts busy pairs near each other. Circular keeps each name easy to read. Grid sorts the nodes by role. Focus hides all that is not adjacent to the selected node. On large clusters, do not show the labels.
      </Part>
    </HelpDialog>
  );
}

export function NodesHelp() {
  return (
    <HelpDialog button="What is this" title="What a node is">
      <Part
        title="One process, one incarnation"
        picture={
          <Flow>
            <Box>relay-0</Box>
            <span className="mono text-[10px] text-muted">c8be·bd6d·09a1</span>
          </Flow>
        }
      >
        A node is a process that runs with a stable name. Its incarnation changes at each restart. Thus a name with a new incarnation is a replacement, and its history starts again.
      </Part>
      <Part
        title="Reachable, stale, unreachable"
        picture={
          <span className="flex flex-col gap-1">
            <Dotted tone="bg-accent">running · reported within 7 s</Dotted>
            <Dotted tone="bg-warning">stale · draining</Dotted>
            <Dotted tone="bg-danger">unreachable · session lost</Dotted>
          </span>
        }
      >
        Green means that the dashboard has a recent snapshot. Stale means that the node is in the membership, but its last snapshot is older than seven seconds. Unreachable means that the session from the dashboard to the node stopped.
      </Part>
      <Part
        title="Filter by role, sort by any column"
        picture={
          <Flow>
            <Box>role=relay</Box>
            <Arrow label="sort" />
            <Box>CPU ↓</Box>
          </Flow>
        }
      >
        The role comes from the node metadata. You can sort each column. The search finds names, addresses and metadata values. The URL keeps the selection, so you can share a filtered view.
      </Part>
    </HelpDialog>
  );
}

export function NodeHelp() {
  return (
    <HelpDialog button="What is this" title="What the node page shows">
      <Part
        title="Resources are history plus live"
        picture={
          <span className="flex flex-col gap-1 text-[10px] text-muted">
            <HelpMeter value={70} />
            <span>retained history · then live samples</span>
          </span>
        }
      >
        Each chart joins the retained history for the selected range with the samples that this tab received after it. A chart shows only the current incarnation. When a chart stays at zero for all of the range, the page gives only its name.
      </Part>
      <Part
        title="Workers are the runtime's threads"
        picture={
          <span className="flex h-6 items-end gap-[3px]">
            {[0.2, 0.9, 0.3, 0.25].map((u, i) => (
              <span key={i} className={`w-3 rounded-[2px] ${u > 0.85 ? "bg-warning" : "bg-accent"}`} style={{ height: `${u * 100}%` }} />
            ))}
          </span>
        }
      >
        There is one bar for each worker. Its height is the busy fraction in the last interval. An even row shows a healthy pool. One tall bar shows one worker with all of the load, usually from an actor that does not yield.
      </Part>
      <Part
        title="The other tabs"
        picture={
          <Flow>
            <Box>actors</Box>
            <Box>distributions</Box>
            <Box>connections</Box>
            <Box>metrics</Box>
            <Box>events</Box>
          </Flow>
        }
      >
        Actors shows the actors of this node. Distributions shows latency histograms. Connections and events are the cluster pages for this node only. Metrics shows each counter that the node reports, as a raw value.
      </Part>
    </HelpDialog>
  );
}

export function ActorsHelp() {
  return (
    <HelpDialog button="What is this" title="What the inventory shows">
      <Part
        title="Each actor, up to a limit"
        picture={
          <Flow>
            <Box tone="border-accent">depth 412</Box>
            <Box>depth 40</Box>
            <Box tone="text-muted">depth 0 · …</Box>
          </Flow>
        }
      >
        Each node reports its actors, up to a limit for each node. The dashboard sorts them and shows them in pages. The total counts all actors on the node. Past the limit, the list says that it is truncated. Search gets a new page from the nodes.
      </Part>
      <Part
        title="Rejected has a reason"
        picture={
          <span className="flex flex-col gap-1">
            <Dotted tone="bg-warning">full · too large</Dotted>
            <Dotted tone="bg-muted">not ready · closed · waiter limit · deadline</Dotted>
          </span>
        }
      >
        A mailbox rejects a message when the mailbox is full or when the message is larger than its size limit. It also rejects when the actor is closed or did not start yet. It rejects when too many senders are in the queue, or when the deadline passed first. The column adds all the reasons.
      </Part>
      <Part
        title="Open one for its timings"
        picture={
          <Flow>
            <Box>row</Box>
            <Arrow label="Enter" />
            <Box tone="border-accent">actor page</Box>
          </Flow>
        }
      >
        The actor page adds mailbox wait, handler latency and turn latency for each message type. It also shows a live chart of depth and throughput while the tab stays open.
      </Part>
    </HelpDialog>
  );
}

export function ActorHelp() {
  return (
    <HelpDialog button="What is this" title="How to read an actor">
      <Part
        title="Wait, then handle, then turn"
        picture={
          <Flow>
            <Box>mailbox wait</Box>
            <Arrow />
            <Box tone="border-accent">handler</Box>
            <Arrow />
            <Box>turn</Box>
          </Flow>
        }
      >
        Mailbox wait is the time that a message stays in the mailbox before the actor takes it. Handler is the code of the actor. Turn is the full scheduling slice. Thus turn minus handler is the runtime overhead and the yield time.
      </Part>
      <Part
        title="Depth and age show different things"
        picture={
          <span className="flex flex-col gap-1 text-[10px] text-muted">
            <span className="mono text-text">depth 412 · oldest 3.2 s</span>
            <span>deep but new is a burst; shallow but old is stuck</span>
          </span>
        }
      >
        Depth is the number of messages in the queue now. Oldest message age is the time that the first message in the queue waited. A deep mailbox with a new oldest message is fast enough. A shallow mailbox with an old message is not fast enough.
      </Part>
      <Part
        title="Live only"
        picture={
          <span className="flex flex-col gap-1 text-[10px] text-muted">
            <HelpMeter value={45} />
            <span>samples since this tab opened</span>
          </span>
        }
      >
        The charts for each actor come from the data that this tab received. History keeps data for each actor type, not for each actor. When you reload the page, the charts start again.
      </Part>
    </HelpDialog>
  );
}
