import { HelpDialog, Part } from "../../components/HelpDialog";

const A = "hsl(95 52% 56%)";
const B = "hsl(232 52% 60%)";

function Bar({ gap = [0, 0], wait = [0, 0], busy, colour }: { gap?: [number, number]; wait?: [number, number]; busy: [number, number]; colour: string }) {
  const span = ([from, to]: [number, number]) => ({ left: `${from}%`, width: `${to - from}%` });
  return (
    <span className="relative block h-2.5 rounded-sm bg-bg">
      <span className="absolute top-1/2 border-t border-dashed border-muted" style={span(gap)} />
      <span className="absolute inset-y-0 rounded-sm opacity-35" style={{ ...span(wait), background: colour }} />
      <span className="absolute inset-y-0 rounded-sm" style={{ ...span(busy), background: colour }} />
    </span>
  );
}

const Row = ({ indent, colour, node, did }: { indent: number; colour: string; node: string; did: string }) => (
  <span className="flex h-6 items-center gap-1.5">
    {indent > 0 && (
      <span className="relative h-full w-5 shrink-0" style={{ marginLeft: (indent - 1) * 20 }}>
        <span className="absolute top-0 left-1/2 h-1/2 border-l border-muted/60" />
        <span className="absolute top-1/2 left-1/2 w-4 border-t border-muted/60" />
      </span>
    )}
    <span className="size-2 shrink-0 rounded-sm" style={{ background: colour }} />
    <span>{node}</span>
    <span className="truncate text-muted">{did}</span>
  </span>
);

export function TraceHelp() {
  return (
    <HelpDialog button="How to read this" title="How to read a trace">
      <Part
        title="A row is one action of one node"
        picture={
          <>
            <Row indent={0} colour={A} node="orders" did="request → billing" />
            <Row indent={1} colour={B} node="billing" did="charge · card-1" />
            <Row indent={2} colour={B} node="billing" did="receipt · mailer" />
          </>
        }
      >
        A line connects each row to the row above it that caused it. Read down a branch to follow one message through the cluster. Rows at the same indent under one parent occurred at the same time.
      </Part>
      <Part
        title="A bar shows when it occurred"
        picture={
          <>
            <Bar gap={[10, 40]} wait={[40, 60]} busy={[60, 90]} colour={B} />
            <span className="mono relative mt-1 block h-7 text-[10px] text-muted">
              <span className="absolute left-[10%] w-[30%] text-center">between spans</span>
              <span className="absolute left-[40%] w-[20%] text-center">in mailbox</span>
              <span className="absolute left-[60%] w-[30%] text-center text-text">busy</span>
            </span>
          </>
        }
      >
        The solid part is the time of the handler, or of a call that waits for its answer. The number on the right is its length. The pale segment at the start is the time that the message stayed in the mailbox of the actor. The dashed line is time after the cause ended that no span records: on the wire between nodes, or before a timer.
      </Part>
      <Part
        title="The top bar is the whole trace"
        picture={
          <>
            <span className="mono flex justify-between text-[10px] text-muted">
              <span>0</span>
              <span>650 µs</span>
              <span>1.3 ms</span>
            </span>
            <span className="mb-1.5 block h-2.5 rounded-sm bg-muted/50" />
            <Bar busy={[0, 8]} colour={A} />
          </>
        }
      >
        All bars use this scale, from the arrival of the first span to the end of the last span. A short bar far to the right means a small quantity of work that started late.
      </Part>
      <Part
        title="A colour is a node, red is a failure"
        picture={
          <span className="flex flex-col gap-1.5">
            <Bar busy={[5, 45]} colour={A} />
            <Bar busy={[30, 70]} colour={B} />
            <Bar busy={[60, 95]} colour="var(--danger)" />
          </span>
        }
      >
        The summary above the spans shows the colour of each node. Red means that the handler returned an error and the actor stopped, or that the call failed.
      </Part>
      <Part
        title="What can seem incorrect"
        picture={
          <span className="flex items-center gap-1.5">
            <span className="size-2 rounded-sm" style={{ background: B }} />
            billing <span className="text-muted">charge · card-1</span> <span className="text-[10px] text-muted">detached</span>
          </span>
        }
      >
        Each node puts the time of its own clock on its spans. Thus a hop can show early or late by the skew between the clocks. "Detached" shows a span with a cause that no node recorded or still keeps. Open a row to see its exact times.
      </Part>
    </HelpDialog>
  );
}
