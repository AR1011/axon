import { Panel } from "../../components/Panel";
import type { Range } from "../../hooks";
import { useSelectedConnection } from "../select";
import { ConnectionTelemetry } from "./ConnectionTelemetry";
import { PeerTable } from "./PeerTable";

export function ConnectionsLayout({ scope, pageRange }: { scope?: string; pageRange?: Range }) {
  const [selected] = useSelectedConnection(scope);
  return (
    <div className="grid min-h-0 flex-1 grid-cols-[minmax(0,1fr)_minmax(320px,420px)] tiles max-[1100px]:grid-cols-1 max-[1100px]:overflow-y-auto">
      <PeerTable scope={scope} />
      <Panel className="scroll-y">
        <ConnectionTelemetry connection={selected} pageRange={pageRange} />
      </Panel>
    </div>
  );
}

export function ConnectionsPage() {
  return (
    <>
      <h1 className="sr-only-h1">Connections</h1>
      <ConnectionsLayout />
    </>
  );
}
