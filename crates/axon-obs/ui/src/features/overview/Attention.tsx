import { Link } from "react-router";
import { Badge, Empty } from "../../components/atoms";
import { useNow, useStaleSince } from "../../hooks";
import { attention, type Concern } from "../../model/attention";
import { nodeBuffer, useLive } from "../../store";

export function useAttention() {
  const now = useNow();
  const overview = useLive((s) => s.overview);
  const departed = useLive((s) => s.departed);
  const since = useStaleSince();
  // Stale also covers an overview this console cannot read and one that went back, with the server answering.
  const why = useLive((s) => s.error) ?? "Stale view";
  return attention(overview, departed, now, since == null ? null : { since, why }, nodeBuffer);
}

export function Attention({ concerns }: { concerns: Concern[] }) {
  if (!concerns.length) return <Empty title="Nothing needs attention" detail="All nodes are reachable, current and running. There is no backlog, no isolation and no collection loss." />;
  return (
    <div className="scroll-y flex-1">
      <table className="w-full text-[10px]">
        <thead className="sticky top-0 bg-panel text-left text-muted">
          <tr>{["Severity", "Node", "What", "Detail"].map((h) => <th key={h} className="border-b border-border px-3 py-1.5 font-normal">{h}</th>)}</tr>
        </thead>
        <tbody>
          {concerns.map((c, i) => (
            <tr key={i} className="hover:bg-raised">
              <td className="px-3 py-[5px]"><Badge tone={c.tone}>{c.tone === "muted" ? "note" : c.tone}</Badge></td>
              <td className="mono px-3 py-[5px]">{c.node}</td>
              <td className="px-3 py-[5px]">{c.to ? <Link to={c.to} className="hover:text-accent hover:underline">{c.title}</Link> : c.title}</td>
              <td className="max-w-[520px] truncate px-3 py-[5px] text-muted" title={c.detail}>{c.detail}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
