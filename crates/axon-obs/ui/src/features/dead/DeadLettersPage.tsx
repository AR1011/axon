import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { useMemo } from "react";
import { Link } from "react-router";
import type { DeadLetterList, DeadLetterRecord } from "../../api";
import { getJson } from "../../api";
import { Count, Dot, Empty, Stamp, STAMP } from "../../components/atoms";
import { ExportButton } from "../../components/Export";
import { HelpDialog, Part } from "../../components/HelpDialog";
import { Panel } from "../../components/Panel";
import { Box } from "../PageHelp";
import { Search } from "../../components/Search";
import { Segmented } from "../../components/Segmented";
import { Select } from "../../components/Select";
import { LinePlot, PlotFrame } from "../../charts/Plots";
import type { Line } from "../../charts/options";
import { Head, SortHead, useSort, type Accessors } from "../../components/sort";
import { useDomain, useHistory, useNow, useUrlState } from "../../hooks";
import { ago, count, shortType } from "../../model/format";
import { seriesLine } from "../../model/history";
import { GAP_MS } from "../../model/live";
import { joinPoints, ratePoints, seamGap } from "../../model/series";
import { tracePath } from "../traces/TracesPage";
import { NodeName } from "../../components/Flag";
import { deadLetters, nodeBuffer, useLive } from "../../store";

const REASONS: Record<string, [label: string, detail: string]> = {
  actor_stopped: ["actor stopped", "The actor stopped while these messages were still in its mailbox."],
  actor_failed: ["actor failed", "The actor failed while these messages were still in its mailbox."],
  actor_panicked: ["actor panicked", "The actor panicked while these messages were still in its mailbox."],
  drain_deadline: ["drain deadline", "The drain of the actor did not finish before its deadline."],
  drained: ["drained", "The actor drained while these messages were still in its stash."],
  shutdown: ["shutdown", "The runtime stopped while these messages were still in a mailbox."],
  fenced: ["fenced", "The lease of the actor expired, so the actor stopped before it handled these messages."],
  unknown_actor: ["no such actor", "A tell from another node named an endpoint that this node does not have."],
  schema_mismatch: ["schema mismatch", "A tell from another node used a different schema version."],
  full: ["mailbox full", "A tell from another node, or a publish to a topic, found the mailbox full."],
  too_large: ["too large", "A publish to a topic was larger than the mailbox limit of a subscriber."],
  not_ready: ["between restarts", "A publish to a topic reached a supervised subscriber between two generations."],
  closed: ["subscriber stopping", "A publish to a topic reached a subscriber during its stop."],
  forbidden: ["forbidden", "The authorizer refused a tell from another node."],
  invalid_message: ["invalid message", "This node could not decode a tell from another node."],
  stopped: ["recipient stopped", "A tell from another node reached an actor that was already stopped."],
};

const reasonOf = (l: DeadLetterRecord) => REASONS[l.reason] ?? [l.reason.replaceAll("_", " "), ""];
const targetOf = (l: DeadLetterRecord) => l.target || shortType(l.actor_type) || l.actor;

const FIELDS: Accessors<DeadLetterRecord> = {
  time: (l) => l.at_us,
  node: (l) => l.node_id,
  target: targetOf,
  message: (l) => l.label,
  reason: (l) => reasonOf(l)[0],
  from: (l) => l.peer,
  count: (l) => l.count,
};

const DAY = 86_400_000;

const Msg = ({ lost = false }: { lost?: boolean }) => <span className={`h-3 w-4 rounded-[2px] ${lost ? "bg-warning" : "bg-accent"}`} />;

export function DeadLetterHelp() {
  return (
    <HelpDialog button="What is this" title="What a dead letter is">
      <Part
        title="A message that the actor accepted but never handled"
        picture={
          <span className="flex items-center gap-1.5">
            <Box>sender</Box>
            <span className="text-muted">→</span>
            <span className="flex items-center gap-1 rounded-sm border border-border p-1">
              <Msg />
              <Msg lost />
              <Msg lost />
            </span>
            <span className="text-warning">✕</span>
            <Box tone="text-muted">actor</Box>
          </span>
        }
      >
        The sender received "accepted" and continued. Nothing tells the sender what occurred after that, so the loss shows only on this page. A send that the mailbox refuses immediately is not on this page: the sender gets that refusal and can try again.
      </Part>
      <Part
        title="An actor stopped while its mailbox was not empty"
        picture={
          <span className="flex flex-col gap-1.5">
            <span className="flex items-center gap-1">
              <Msg lost />
              <Msg lost />
              <Msg lost />
              <span className="ml-1 text-muted">work × 3</span>
            </span>
            <span className="text-[10px] text-muted">actor stopped · failed · panicked · drain deadline · shutdown</span>
          </span>
        }
      >
        The actor drops all messages that were still in its queue. This page shows them by message kind with a count, under the reason why the actor stopped.
      </Part>
      <Part
        title="A tell from another node was refused"
        picture={
          <span className="flex items-center gap-1.5">
            <Box>node b</Box>
            <span className="mono text-[10px] text-muted">tell →</span>
            <span className="text-warning">✕</span>
            <Box>node a</Box>
          </span>
        }
      >
        A tell does not wait for an answer, so the sender does not get a refusal on arrival. The reasons are no such actor, mailbox full, recipient stopped, forbidden, or a message that the node could not decode. "From" is the node that sent the tell.
      </Part>
      <Part
        title="A subscriber of a topic missed a publish"
        picture={
          <span className="flex items-center gap-1.5">
            <span className="mono text-[10px] text-muted">publish →</span>
            <span className="text-warning">✕</span>
            <Box>subscriber</Box>
          </span>
        }
      >
        A publish never waits. Thus a subscriber misses the message when its mailbox is full or too small for it, when it stops, or when it is between restarts. The target is the topic, and the message is "publish".
      </Part>
      <Part
        title="What is kept"
        picture={
          <span className="flex flex-col gap-1 text-[10px] text-muted">
            <span className="mono text-text">1024 per node</span>
            <span>repeats within a second share a row</span>
          </span>
        }
      >
        Each node keeps its newest letters in memory. A metrics store that keeps them, such as ClickHouse, still has them after the node restarts. Without such a store, the letters stop with the node, and only the dead letter counts in its metrics stay. The time of a row is when a node last saw it. "open" shows the trace of the first traced message in the row.
      </Part>
    </HelpDialog>
  );
}

const RATE_SPAN = 3_600_000;

/** Per node, the last hour: retained history where a store keeps it, then the live session. */
function DeadLetterRate() {
  const nodes = useLive((s) => s.overview?.nodes);
  const history = useHistory({ metrics: ["runtime.dead_letters"], span: RATE_SPAN, points: 144, sum: "node" });
  const lines = useMemo(() => {
    const data = history.data;
    const gap = seamGap(data?.step);
    return (nodes ?? [])
      .flatMap((n): Line[] => {
        if (!n.snapshot) return [];
        const incarnation = n.snapshot.incarnation;
        const retained = seriesLine(data, "runtime.dead_letters", (x) => x.node_id === n.node.id && x.incarnation === incarnation, true);
        const live = ratePoints(nodeBuffer.rows(n.node.id, incarnation).map((r) => ({ t: r.t, v: r.deadLetters })), GAP_MS);
        return [{ name: n.node.id, points: joinPoints(retained, live, gap) }];
      })
      .filter((l) => l.points.some((p) => (p[1] ?? 0) > 0))
      .slice(0, 8);
  }, [nodes, history.data]);
  const domain = useDomain(RATE_SPAN);
  return (
    <PlotFrame title="Dead letters per second" info="For each node over the last hour: retained history where a metrics store keeps it, then the live samples of this session. The chart does not show a node that dropped no messages, and it shows at most eight nodes.">
      <LinePlot lines={lines} unit="/s" domain={domain} label="Dead letters per second by node" empty={{ title: "None in the last hour", detail: "No node dropped a message in the samples that this console keeps." }} />
    </PlotFrame>
  );
}

interface Group {
  target: string;
  letters: number;
  rows: number;
  reasons: Set<string>;
  nodes: Set<string>;
  lastUs: number;
}

function byTarget(letters: DeadLetterRecord[]): Group[] {
  const groups = new Map<string, Group>();
  for (const l of letters) {
    const target = targetOf(l);
    const g = groups.get(target) ?? { target, letters: 0, rows: 0, reasons: new Set(), nodes: new Set(), lastUs: 0 };
    g.letters += l.count;
    g.rows += 1;
    g.reasons.add(reasonOf(l)[0]);
    g.nodes.add(l.node_id);
    g.lastUs = Math.max(g.lastUs, l.at_us);
    groups.set(target, g);
  }
  return [...groups.values()].sort((a, b) => b.letters - a.letters);
}

function Groups({ letters, onPick }: { letters: DeadLetterRecord[]; onPick: (target: string) => void }) {
  const now = useNow();
  return (
    <div className="scroll-y flex-1 overflow-x-auto">
      <table className="min-w-[640px]">
        <thead>
          <tr>
            <Head label="For" width="30%" />
            <Head label="Letters" width="10%" align="right" />
            <Head label="Rows" width="8%" align="right" />
            <Head label="Reasons" width="27%" />
            <Head label="Nodes" width="15%" />
            <Head label="Last" width="10%" />
          </tr>
        </thead>
        <tbody>
          {byTarget(letters).map((g) => (
            <tr key={g.target} className="cursor-pointer hover:bg-raised" onClick={() => onPick(g.target)} title="Show these rows">
              <td className="truncate">{g.target}</td>
              <td className="text-right">{count(g.letters)}</td>
              <td className="text-right">{count(g.rows)}</td>
              <td className="truncate font-sans">{[...g.reasons].join(", ")}</td>
              <td className="truncate">{[...g.nodes].sort().join(", ")}</td>
              <td className="text-muted">{ago(now - g.lastUs / 1000)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function DeadLettersPage() {
  const [group, setGroup] = useUrlState<"" | "target">("group", "");
  const [q, setQ] = useUrlState("q", "");
  const [reason, setReason] = useUrlState("reason", "");
  const list = useQuery({
    queryKey: ["dead-letters"],
    refetchInterval: 5_000,
    placeholderData: keepPreviousData,
    queryFn: ({ signal }) => getJson<DeadLetterList>("dead-letters", {}, signal),
  });
  const all = list.data?.letters ?? [];
  const terms = q.toLowerCase().split(/\s+/).filter(Boolean);
  const letters = all.filter(
    (l) => (!reason || l.reason === reason) && terms.every((term) => [l.node_id, l.target, l.actor, l.actor_type, l.label, l.peer, reasonOf(l)[0]].some((v) => (v === l.actor ? v.startsWith(term) : v.toLowerCase().includes(term)))),
  );
  const sort = useSort(letters, FIELDS, "d", "time", "desc");
  const unreachable = Object.keys(list.data?.errors ?? {});
  const nodes = useLive((s) => s.overview?.nodes.length ?? 0);
  // The sidebar's count: what each node dropped since it started, which the rows, at most the newest each still holds, need not add up to.
  const total = useLive(deadLetters);
  // Only when nothing loaded: a refetch that failed leaves the letters it had, and a filter may match none of them.
  const failed = all.length ? null : (list.error?.message ?? (unreachable.length >= Math.max(1, nodes) ? `No answer from ${unreachable.join(", ")}.` : null));
  const reasons = [...new Set(all.map((l) => l.reason))].sort();
  return (
    <>
      <h1 className="sr-only-h1">Dead letters</h1>
      <div className="grid h-[220px] shrink-0">
        <DeadLetterRate />
      </div>
      <Panel
        className="flex-1"
        title={
          <>
            Dead letters <Count>{count(total)}</Count>
          </>
        }
        controls={
          <>
            <Search label="Search dead letters" value={q} onChange={setQ} className="w-[180px] max-[700px]:w-[110px]" />
            <Select label="Reason" value={reason} onChange={setReason} options={[{ value: "", label: "Any reason" }, ...reasons.map((r) => ({ value: r, label: REASONS[r]?.[0] ?? r }))]} />
            <Segmented label="Group" value={group} onChange={setGroup} options={[{ value: "", label: "Rows" }, { value: "target", label: "Targets" }]} />
            <ExportButton dataset={{ name: "Dead letters", count: letters.length, filters: [q && ["search", q], reason && ["reason", reason]], rows: () => sort.sorted }} />
          </>
        }
        footer={
          <span>
            {sort.sorted.length} of {all.length} rows · {count(letters.reduce((sum, l) => sum + l.count, 0))} letters
            {unreachable.length ? ` · no answer from ${unreachable.join(", ")}` : ""}
            {list.error ? ` · ${list.error.message}` : ""}
          </span>
        }
      >
        {!letters.length && failed ? (
          <Empty title="Could not load dead letters" detail={failed} />
        ) : !letters.length ? (
          <Empty
            title={list.isPending ? "Wait for the dead letters" : all.length ? "No dead letters match the filters" : "No dead letters"}
            detail={all.length ? "Use fewer filters." : "No node dropped a message: no actor stopped with messages in its queue, and no node refused a tell."}
          />
        ) : group === "target" ? (
          <Groups letters={letters} onPick={(target) => { setQ(target); setGroup(""); }} />
        ) : (
          <div className="scroll-y flex-1 overflow-x-auto">
            <table className="min-w-[760px]">
              <thead>
                <tr>
                  <SortHead sort={sort} field="time" label="Time" width={STAMP} />
                  <SortHead sort={sort} field="node" label="Node" width="12%" />
                  <SortHead sort={sort} field="target" label="For" width="20%" />
                  <SortHead sort={sort} field="message" label="Message" width="11%" />
                  <SortHead sort={sort} field="reason" label="Reason" width="15%" />
                  <SortHead sort={sort} field="from" label="From" width="11%" />
                  <SortHead sort={sort} field="count" label="Count" width="7%" align="right" />
                  <Head label="Trace" width="7%" />
                </tr>
              </thead>
              <tbody>
                {sort.sorted.map((l) => {
                  const [label, detail] = reasonOf(l);
                  return (
                    <tr key={`${l.node_id}:${l.seq}`}>
                      <td>
                        <Stamp ms={l.at_us / 1000} span={DAY} />
                      </td>
                      <td>
                        <NodeName id={l.node_id} />
                      </td>
                      <td className="truncate" title={[l.target, l.actor_type, l.actor].filter(Boolean).join(" · ")}>
                        {l.actor ? <Link to={`/actors?${new URLSearchParams({ aq: l.target || shortType(l.actor_type), anode: l.node_id })}`} className="underline decoration-border underline-offset-2 hover:decoration-current">{targetOf(l)}</Link> : targetOf(l)}
                      </td>
                      <td>{l.label || <span className="text-muted">unlabelled</span>}</td>
                      <td className="font-sans" title={detail}>
                        <Dot tone="warning" />
                        {label}
                      </td>
                      <td>{l.peer ? <NodeName id={l.peer} /> : <span className="text-muted">local</span>}</td>
                      <td className="text-right">{count(l.count)}</td>
                      <td>{l.trace ? <Link to={tracePath(l.trace)}>open</Link> : ""}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </Panel>
    </>
  );
}
