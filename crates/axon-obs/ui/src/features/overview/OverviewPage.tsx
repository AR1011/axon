import { Tabs } from "../../components/Tabs";
import { useUrlState } from "../../hooks";
import { totals, useLive } from "../../store";
import { Inventory } from "../actors/Inventory";
import { ConnectionTelemetry } from "../connections/ConnectionTelemetry";
import { PeerTable } from "../connections/PeerTable";
import { Timeline } from "../events/Timeline";
import { Inspector } from "../nodes/Inspector";
import { NodeList } from "../nodes/NodeList";
import { listConnections, useSelectedConnection, useSelectedNode } from "../select";
import { Topology } from "../topology/Topology";
import { Attention, useAttention } from "./Attention";

type Activity = "attention" | "connections" | "actors" | "events";

export function OverviewPage() {
  const [node] = useSelectedNode();
  const [connection] = useSelectedConnection();
  const [tab, setTab] = useUrlState<Activity>("tab", "attention");
  const overview = useLive((s) => s.overview);
  const concerns = useAttention();
  const counts = {
    attention: concerns.length,
    connections: listConnections(overview).length,
    actors: overview ? (totals.actors ?? 0) : 0,
    events: overview?.events.length ?? 0,
  };
  // The inspector follows the selected node; its connection pane shows that node's selected session.
  const inspected = connection && node && connection.view.node.id === node.node.id ? connection : listConnections(overview, node?.node.id)[0];
  return (
    <>
      <h1 className="sr-only-h1">Cluster</h1>
      <div className="grid min-h-0 flex-1 grid-cols-[208px_minmax(0,1fr)_304px] grid-rows-[minmax(0,1.25fr)_minmax(0,1fr)] gap-2.5 min-[1600px]:grid-cols-[240px_minmax(0,1fr)_380px] max-[1250px]:grid-cols-[182px_minmax(0,1fr)_280px] max-[900px]:flex max-[900px]:flex-col max-[900px]:overflow-y-auto">
        <div className="flex min-h-0 flex-col max-[900px]:h-[240px] max-[900px]:shrink-0 [&>*]:flex-1"><NodeList /></div>
        <div className="flex min-h-0 min-w-0 flex-col max-[900px]:h-[340px] max-[900px]:shrink-0 [&>*]:flex-1"><Topology /></div>
        <div className="row-span-2 flex min-h-0 flex-col">
          <Inspector view={node}>
            <ConnectionTelemetry connection={inspected} compact />
          </Inspector>
        </div>
        <section className="col-span-2 flex min-h-0 min-w-0 flex-col overflow-hidden rounded-md border border-border bg-panel max-[900px]:min-h-[320px]">
          <Tabs label="Activity" value={tab} onChange={setTab} tabs={(["attention", "connections", "actors", "events"] as const).map((value) => ({ value, label: value, count: counts[value] }))} />
          {tab === "attention" && <Attention concerns={concerns} />}
          {tab === "connections" && <PeerTable flat />}
          {tab === "actors" && <Inventory flat />}
          {tab === "events" && <Timeline compact />}
        </section>
      </div>
    </>
  );
}
