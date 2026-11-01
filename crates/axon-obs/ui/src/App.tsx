import { Navigate, Route, Routes, useLocation } from "react-router";
import { Empty } from "./components/atoms";
import { OverviewPage } from "./features/overview/OverviewPage";
import { LoginPage } from "./features/login/LoginPage";
import { lazyNamed as page } from "./lazy";
import { Shell } from "./shell/Shell";

const ActorDetail = page(() => import("./features/actors/ActorDetail"), "ActorDetail");
const ActorsPage = page(() => import("./features/actors/ActorsPage"), "ActorsPage");
const ClusterPage = page(() => import("./features/cluster/ClusterPage"), "ClusterPage");
const ConnectionDetail = page(() => import("./features/connections/ConnectionDetail"), "ConnectionDetail");
const ConnectionsPage = page(() => import("./features/connections/ConnectionsPage"), "ConnectionsPage");
const EventsPage = page(() => import("./features/events/EventsPage"), "EventsPage");
const NodeDetail = page(() => import("./features/nodes/NodeDetail"), "NodeDetail");
const NodesPage = page(() => import("./features/nodes/NodesPage"), "NodesPage");
const PlacementPage = page(() => import("./features/placement/PlacementPage"), "PlacementPage");
const WorkloadPage = page(() => import("./features/workload/WorkloadPage"), "WorkloadPage");
const ExplorePage = page(() => import("./features/explore/ExplorePage"), "ExplorePage");
const TopologyPage = page(() => import("./features/topology/TopologyPage"), "TopologyPage");
const StreamsPage = page(() => import("./features/streams/StreamsPage"), "StreamsPage");
const DeadLettersPage = page(() => import("./features/dead/DeadLettersPage"), "DeadLettersPage");
const TraceDetail = page(() => import("./features/traces/TraceDetail"), "TraceDetail");
const TracesPage = page(() => import("./features/traces/TracesPage"), "TracesPage");
const SettingsPage = page(() => import("./features/settings/SettingsPage"), "SettingsPage");

/** The topology page became the overview's topology panel at full size; old links keep their selection. */
function OldTopology() {
  return <Navigate to={`/overview/topology${useLocation().search}`} replace />;
}

export function App() {
  return (
    <Routes>
      <Route path="login" element={<LoginPage />} />
      <Route element={<Shell />}>
        <Route index element={<Navigate to="overview" replace />} />
        <Route path="overview" element={<OverviewPage />} />
        <Route path="cluster" element={<ClusterPage />} />
        <Route path="placement" element={<PlacementPage />} />
        <Route path="workload" element={<WorkloadPage />} />
        <Route path="explore" element={<ExplorePage />} />
        <Route path="overview/topology" element={<TopologyPage />} />
        <Route path="topology" element={<OldTopology />} />
        <Route path="nodes" element={<NodesPage />} />
        <Route path="nodes/:nodeId" element={<NodeDetail />} />
        <Route path="actors" element={<ActorsPage />} />
        <Route path="actors/:nodeId/:actorId" element={<ActorDetail />} />
        <Route path="actors/*" element={<Empty title="Actor link is incomplete" detail="An actor link names a node and an actor." />} />
        <Route path="connections" element={<ConnectionsPage />} />
        <Route path="connections/:nodeId/:peerId/:direction/:lane" element={<ConnectionDetail />} />
        <Route path="connections/*" element={<Empty title="Connection link is incomplete" detail="A connection link names a node, a peer, a direction and a lane." />} />
        <Route path="streams" element={<StreamsPage />} />
        <Route path="dead-letters" element={<DeadLettersPage />} />
        <Route path="traces" element={<TracesPage />} />
        <Route path="traces/:traceId" element={<TraceDetail />} />
        <Route path="events" element={<EventsPage />} />
        <Route path="settings" element={<SettingsPage />} />
        <Route path="*" element={<Empty title="Page not found" detail="Use the navigation to return to the cluster." />} />
      </Route>
    </Routes>
  );
}
