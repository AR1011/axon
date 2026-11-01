import { Link } from "react-router";
import type { NodeView } from "../api";
import { nodeById, nodePath, placeOf } from "../features/select";
import { useLive, type Live } from "../store";

// Each flag is its own file in the build, fetched only once a node names that country. Two letters only: the set's
// regional flags (gb-sct, sh-ac, ...) are no ISO code and would only add weight.
const FLAGS = import.meta.glob<string>("/node_modules/flag-icons/flags/4x3/[a-z][a-z].svg", { query: "?url", import: "default", eager: true });

export const flagUrl = (country: string): string | undefined => FLAGS[`/node_modules/flag-icons/flags/4x3/${country.toLowerCase()}.svg`];

/** A graph card's title in SVG: the flag, then the node's name cut to `room` characters (three fewer beside a flag). */
export function CardTitle({ view, room }: { view: NodeView; room: number }) {
  const url = flagUrl(placeOf(view).country);
  const id = view.node.id;
  const fits = url ? room - 3 : room;
  return (
    <>
      {url && <image href={url} x="22" y="12" width="13" height="10" preserveAspectRatio="xMidYMid slice" aria-hidden />}
      {/* A white field would vanish into a light card without its edge. */}
      {url && <rect x="22" y="12" width="13" height="10" rx="1" fill="none" stroke="var(--border)" aria-hidden />}
      <text x={url ? 40 : 22} y="21" fill="var(--text)" fontSize="11" fontWeight="550" fontFamily="var(--font-mono)">{id.length > fits ? `${id.slice(0, fits - 1)}…` : id}</text>
    </>
  );
}

/** The flag of the node's country, or nothing when the node is unknown or names none this build knows. Hidden from
 * screen readers, which read the place from the node's inspector and page; elsewhere it is the flag's title on hover.
 * Selects strings, so a flag renders again only when its node's place changes. */
export function Flag({ node }: { node: string }) {
  const place = (s: Live) => {
    const view = nodeById(s, node);
    return view ? placeOf(view) : undefined;
  };
  const url = useLive((s) => flagUrl(place(s)?.country ?? ""));
  const title = useLive((s) => place(s)?.text);
  return url ? <img src={url} alt="" aria-hidden title={title} className="inline-block h-[10px] w-[13px] shrink-0 rounded-[2px] object-cover align-[-1px] ring-1 ring-border" /> : null;
}

/** A node's flag and name, as a link to its page unless `link` is false. Cut to its cell, the whole name is its title. */
export function NodeName({ id, link = true, className = "", tabIndex }: { id: string; link?: boolean; className?: string; tabIndex?: number }) {
  const classes = `inline-flex max-w-full items-center gap-1.5 ${className}`;
  const name = <><Flag node={id} /><span className="truncate">{id}</span></>;
  return link ? (
    <Link to={nodePath(id)} tabIndex={tabIndex} title={id} className={classes}>{name}</Link>
  ) : (
    <span title={id} className={classes}>{name}</span>
  );
}
