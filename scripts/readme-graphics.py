#!/usr/bin/env python3
"""Draws the README's verification graphics into assets/readme/.

The fault matrix is read from spec/faults.md; the other figures are the ones the spec records (spec/tla,
spec/simulation.md, spec/fault-rig.md, AGENTS.md). Change them here when a run moves them, then rerun.
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "assets" / "readme"

INK, PANEL, LINE = "#0a0b0c", "#141618", "#24282c"
AMBER, PAPER, SLATE, DIM, ROSE, SKY = "#e0b252", "#eceded", "#8a8e94", "#5b5f65", "#e08f96", "#7fb2e5"
SANS = "Inter, -apple-system, 'Segoe UI', Helvetica, Arial, sans-serif"
MONO = "'JetBrains Mono', SFMono-Regular, Menlo, Consolas, monospace"


def esc(s):
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def text(x, y, s, size=13, fill=PAPER, weight=400, family=SANS, anchor="start", extra=""):
    return (f'<text x="{x}" y="{y}" font-family="{family}" font-size="{size}" font-weight="{weight}" '
            f'fill="{fill}" text-anchor="{anchor}" {extra}>{esc(s)}</text>')


def kicker(x, y, s, fill=AMBER, size=11, anchor="start"):
    return text(x, y, s, size + 1.5, fill, 500, SANS, anchor)


def svg(name, w, h, body, title, rule=138):
    up = 30
    h -= up
    doc = (f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" viewBox="0 0 {w} {h}" role="img">'
           f'<title>{esc(title)}</title>'
           f'<rect x="0.5" y="0.5" width="{w - 1}" height="{h - 1}" rx="4" fill="{INK}" stroke="{LINE}"/>'
           f'<g transform="translate(0 -{up})"><line x1="40" y1="{rule}" x2="{w - 40}" y2="{rule}" stroke="{LINE}"/>'
           + "".join(body) + "</g></svg>\n")
    (OUT / name).write_text(doc)


def header(k, title, sub, w=960):
    return [text(40, 86, title, 24, PAPER, 600, extra='letter-spacing="-0.3"'),
            text(40, 112, sub, 13.5, SLATE)]


def layers(feats, faults, gaps, cited):
    rows = [
        ("Models · TLA+", "TLC examines all the states that it can reach",
         [("7", "models: leases, hand-over,", "relays, disk, quotas, seeds"),
          ("3.6 B", "states examined,", "and TLC found no error"),
          ("109", "mutations add an error;", "TLC found all of them")]),
        ("Concurrency · loom", "loom examines each thread order on lock-free paths",
         [("16", "loom models, which run", "each time check.sh runs"),
          ("4", "paths: the mailbox, worker wake,", "remote replies, write order"),
          ("", "", "")]),
        ("Deterministic simulation · the World", "Real clusters run on virtual time. A seed sets the faults.",
         [("87", "World tests: crash, skew,", "pause, partition, full disk"),
          ("64", "seeds per test per check;", "two runs give one digest"),
          ("60 000", "seeds per test, which", "ran on dev boxes")]),
        ("Real faults · the rig", "Five servers get real faults. A checker reads the logs.",
         [("68", "fault classes: kill -9, OOM,", "partitions, disk, clock steps"),
          ("79", "invariants; the checker", "examines them in the logs"),
          ("602", "fault episodes in one", "night of 6.9 hours")]),
        ("Fault matrix · fuzz tests", "Tests apply each fault to each feature that it can reach",
         [(str(gaps), f"gaps in {feats} features", f"× {faults} kinds of fault"),
          (str(cited), "tests in the matrix; each", "applies its fault"),
          ("4", "fuzz targets: the decoders", "and the stream core")]),
    ]
    body = header("", "How we test axon",
                  "Five levels of tests, from the design to real servers. Each level finds errors that the level above it cannot find.")
    top, rh, gap = 128, 88, 0
    x0 = 40
    for i, (k, title, stats) in enumerate(rows):
        y = top + i * (rh + gap) + 12
        if i:
            body.append(f'<line x1="{x0}" y1="{y}" x2="920" y2="{y}" stroke="{LINE}"/>')
        body.append(kicker(x0, y + 34, k, AMBER, 11))
        body.append(text(x0, y + 58, title, 14, PAPER, 400))
        for j, (big, l1, l2) in enumerate(stats):
            if not big:
                continue
            sx = 480 + j * 148
            body.append(f'<line x1="{sx - 14}" y1="{y + 20}" x2="{sx - 14}" y2="{y + rh - 20}" stroke="{LINE}"/>')
            body.append(text(sx, y + 42, big, 23, AMBER, 500, MONO, extra='letter-spacing="-0.6"'))
            body.append(text(sx, y + 60, l1, 10.5, SLATE))
            body.append(text(sx, y + 74, l2, 10.5, SLATE))
    h = top + rh * 5 + 36
    svg("verification.svg", 960, h, body, "How we test axon: five levels")


SHORT = {
    "mailbox admission and backpressure": "mailbox", "supervision and restart": "supervision",
    "jobs (submit_io, submit_future, attach)": "jobs", "stash (cx.stash)": "stash", "watches (cx.watch)": "watches",
    "links (cx.link)": "links", "routers (Router)": "routers", "topics (Topic)": "topics",
    "drain and shutdown": "drain, shutdown", "framing and compression": "framing", "sessions and keepalive": "sessions",
    "keyed actors (get_or_spawn, rebalance, KeyedRef, idle, watch_key)": "keyed actors",
    "rolling upgrades (wire versions, schemas side by side)": "rolling upgrades", "relay core and node": "relays",
    "recovery and cursors": "recovery", "namespace limits": "limits", "metrics collection": "metrics",
    "ClickHouse sink": "ClickHouse", "dashboard host": "console", "health probes": "health probes",
    "Prometheus endpoint": "Prometheus",
}


def fault_matrix():
    feats, faults, cells, feat = [], [], {}, None
    for line in (ROOT / "spec" / "faults.md").read_text().splitlines():
        if line.startswith("### "):
            feat = line[4:].strip()
            feats.append(feat)
            continue
        m = re.match(r"^\| ([A-Za-z][^|]*?) \| (.*) \|\s*$", line)
        if feat and m and m.group(1) != "Fault":
            fault, cell = m.groups()
            if fault not in faults:
                faults.append(fault)
            cells[feat, fault] = None if cell.startswith("n/a") else len(re.findall(r"`[^`]+::[^`]+`", cell))
    tested = [v for v in cells.values() if v is not None]
    gaps = sum(1 for v in tested if v == 0)

    cw, ch, g = 15, 15, 2
    lx, gy = 160, 250
    w = lx + len(feats) * (cw + g) + 40
    body = header("Fault matrix · spec/faults.md",
                  f"Each feature × each fault: {gaps} gaps",
                  f"{len(feats)} features × {len(faults)} kinds of fault. In {len(tested)} cells, the fault can reach the feature, "
                  f"and the cell names the tests for it.", w)
    body.append(text(40, 130, f"These cells name {sum(tested)} tests in total. The other {len(cells) - len(tested)} cells give the "
                     "reason that the fault cannot reach the feature.", 13.5, SLATE))
    groups = []
    for i, f in enumerate(feats):
        grp, name = f.split(": ", 1)
        x = lx + i * (cw + g)
        if not groups or groups[-1][0] != grp:
            groups.append([grp, x, x])
        groups[-1][2] = x + cw
        body.append(text(x + cw / 2 + 4, gy - 10, SHORT.get(name, name), 10, SLATE, family=MONO,
                         extra=f'transform="rotate(-55 {x + cw / 2 + 4} {gy - 10})"'))
    for grp, a, b in groups:
        yb = gy + len(faults) * (ch + g) + 8
        body.append(f'<path d="M{a} {yb - 4}V{yb}H{b}V{yb - 4}" fill="none" stroke="{DIM}"/>')
        body.append(text((a + b) / 2, yb + 18, grp, 11, SLATE, 400, SANS, "middle"))
    for r, fault in enumerate(faults):
        y = gy + r * (ch + g)
        body.append(text(lx - 12, y + 11, fault, 11, PAPER, family=MONO, anchor="end"))
        for c, f in enumerate(feats):
            x = lx + c * (cw + g)
            v = cells[f, fault]
            if v is None:
                body.append(f'<rect x="{x + 0.5}" y="{y + 0.5}" width="{cw - 1}" height="{ch - 1}" fill="none" stroke="{LINE}"/>')
            else:
                op = 0.38 if v == 1 else 0.6 if v == 2 else 0.8 if v <= 4 else 1
                body.append(f'<rect x="{x}" y="{y}" width="{cw}" height="{ch}" fill="{AMBER if v else ROSE}" '
                            f'opacity="{op}"><title>{esc(f)} × {esc(fault)}: {v} tests</title></rect>')
    ly = gy + len(faults) * (ch + g) + 62
    body.append(text(lx - 12, ly + 11.5, "tests for the cell", 11, SLATE, anchor="end"))
    for i, (lbl, op) in enumerate([("1", 0.38), ("2", 0.6), ("3–4", 0.8), ("5+", 1)]):
        x = lx + i * 58
        body.append(f'<rect x="{x}" y="{ly}" width="{cw}" height="{ch}" fill="{AMBER}" opacity="{op}"/>')
        body.append(text(x + cw + 7, ly + 11.5, lbl, 11, PAPER, family=MONO))
    x = lx + 4 * 58 + 20
    body.append(f'<rect x="{x + 0.5}" y="{ly + 0.5}" width="{cw - 1}" height="{ch - 1}" fill="none" stroke="{LINE}"/>')
    body.append(text(x + cw + 7, ly + 11.5, "the fault cannot reach the feature", 11, PAPER))
    svg("fault-matrix.svg", w, ly + 50, body, f"Fault matrix: {len(feats)} features by {len(faults)} faults, {gaps} gaps", 150)
    return len(feats), len(faults), gaps, sum(tested)


def tla():
    # Largest run per model: spec/simulation.md § TLA (deep runs 2026-10-01), spec/paxos-leases.md, AGENTS.md.
    rows = [
        ("PaxosLeases", "one owner per key, fenced leases, clock skew", 1.80e9, "1.80 B", "depth 20", False, 6),
        ("PaxosLeases", "the same, without skew", 1.06e9, "1.06 B", "depth 31", False, None),
        ("SnapshotThenLive", "compacted state: snapshot, replay, live", 511e6, "511 M", "depth 22", False, 16),
        ("Handover", "a key that moves between nodes", 168e6, "168 M", "depth 31", False, 20),
        ("RelayGiveUp", "a relay releases a position only when it must", 36.6e6, "36.6 M", "depth 55", True, 21),
        ("DiskResume", "stream history on disk, resumed after crashes", 15.6e6, "15.6 M", "depth 18", False, 20),
        ("QuotaGrant", "cluster-wide quota grants", 14.8e6, "14.8 M", "depth 10", False, 12),
        ("SeedMembership", "membership through seed servers", 4.9e6, "4.9 M", "depth 74", True, 14),
    ]
    import math
    body = header("Model checking · TLA+",
                  "TLC examined 3.6 billion states and found no error",
                  "The largest run of each model. A mutation adds an error. If TLC does not find it, the check fails.")
    bx, bw, top, rh = 380, 330, 178, 40
    lo, hi = math.log10(1e6), math.log10(2e9)
    sx = lambda v: bx + (math.log10(v) - lo) / (hi - lo) * bw
    body.append('<defs><linearGradient id="open" x1="0" x2="1"><stop offset="0.7" stop-color="' + AMBER +
                '"/><stop offset="1" stop-color="' + AMBER + '" stop-opacity="0.1"/></linearGradient></defs>')
    for v, lbl in [(1e6, "1 M"), (1e7, "10 M"), (1e8, "100 M"), (1e9, "1 B")]:
        x = sx(v)
        body.append(f'<line x1="{x}" y1="{top - 8}" x2="{x}" y2="{top + rh * len(rows)}" stroke="{LINE}" stroke-dasharray="2 3"/>')
        body.append(text(x, top - 16, lbl, 10.5, DIM, family=MONO, anchor="middle"))
    body.append(kicker(920, top - 16, "mutations found", SLATE, 9, "end"))
    for i, (m, what, v, lbl, depth, done, muts) in enumerate(rows):
        y = top + i * rh
        body.append(f'<line x1="40" y1="{y + rh}" x2="920" y2="{y + rh}" stroke="{LINE}"/>')
        body.append(text(40, y + 17, m, 13, PAPER, 500, MONO))
        body.append(text(40, y + 32, what, 11, SLATE))
        body.append(f'<rect x="{bx}" y="{y + 14}" width="{sx(v) - bx}" height="8" '
                    f'fill="{AMBER if done else "url(#open)"}"/>')
        body.append(text(sx(v) + 8, y + 22, lbl, 11.5, PAPER, 500, MONO))
        body.append(text(sx(v) + 8 + len(lbl) * 7.2 + 6, y + 22, depth, 10.5, DIM, family=MONO))
        if muts:
            body.append(text(920, y + 23, muts, 14, AMBER, 500, MONO, "end"))
    y = top + rh * len(rows) + 22
    body.append(f'<rect x="40" y="{y + 1}" width="28" height="8" fill="{AMBER}"/>')
    body.append(text(76, y + 9.5, "TLC examined all the states in the bounds", 11, PAPER))
    body.append(f'<rect x="300" y="{y + 1}" width="28" height="8" fill="url(#open)"/>')
    body.append(text(336, y + 9.5, "TLC stopped at a time limit and found no error", 11, PAPER))
    svg("tla.svg", 960, y + 60, body, "TLA+: TLC examined 3.6 billion states and found all 109 mutations")


CLASSES = """kill pause freeze leave isolate partition oneway bridge netem rate cpu oom memhigh io slowdisk diskerr diskdrop
diskfull step drift drift-far skew-restart authz peer powercut diskhang rofs flaky splitdelay readerr diskdead diskcorrupt
inodes roll cfgdrift scale cold-restart acceptors-restart acceptors-rolling leave-minority leave-slowdisk crashloop
killterm tasksmax nofile threadstarve reclaim hook coredump swap thp takeover-step drift-edge step-huge leap reject
syndrop halfopen portcut graylink flap mtu asym dup corrupt conntrack backlog ports""".split()
NETWORK = set("isolate partition oneway bridge netem rate reject syndrop halfopen portcut graylink flap mtu asym dup corrupt "
              "conntrack backlog ports".split())
DISK = set("io slowdisk diskerr diskdrop diskfull powercut diskhang rofs flaky splitdelay readerr diskdead diskcorrupt inodes "
           "leave-slowdisk".split())
CLOCK = set("step drift drift-far skew-restart takeover-step drift-edge step-huge leap".split())
PROCESS = set("kill leave oom roll scale cold-restart acceptors-restart acceptors-rolling leave-minority crashloop killterm "
              "coredump".split())
INPUT = set("authz peer hook cfgdrift".split())


def rig():
    assert len(CLASSES) == 68 and len(set(CLASSES)) == 68
    body = header("Real faults · spec/fault-rig.md",
                  "Five servers get real faults, and a checker reads the logs",
                  "The rig tests what a simulation cannot: real kernels and TCP, SIGKILL during a write, OOM, cgroups, disks and clocks.")
    # five nodes on a pentagon
    import math
    cx, cy, r = 205, 330, 118
    pos = [(cx + r * math.sin(2 * math.pi * i / 5), cy - r * math.cos(2 * math.pi * i / 5)) for i in range(5)]
    for i in range(5):
        for j in range(i + 1, 5):
            (x1, y1), (x2, y2) = pos[i], pos[j]
            cut = (i, j) in [(0, 3), (2, 3)]
            body.append(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{ROSE if cut else "#33383d"}" '
                        f'{"stroke-dasharray=\"3 3\"" if cut else ""}/>')
    notes = ["store · nemesis · checker", "acceptor", "acceptor · clock step", "SIGKILL, isolated", "disk full"]
    for i, (x, y) in enumerate(pos):
        acc = i < 3
        hit = i >= 2
        body.append(f'<rect x="{x - 26}" y="{y - 15}" width="52" height="30" rx="2" fill="{PANEL}" '
                    f'stroke="{ROSE if i == 3 else AMBER if acc else SLATE}"/>')
        body.append(f'<rect x="{x - 26}" y="{y - 15}" width="3" height="30" fill="{ROSE if i == 3 else AMBER if acc else SLATE}"/>')
        body.append(text(x + 1, y + 4.5, f"b{i + 1}", 12.5, PAPER, 500, MONO, "middle"))
        ty = y - 26 if y < cy - 20 else y + 34
        body.append(text(x, ty, notes[i], 10, ROSE if hit else SLATE, family=MONO, anchor="middle"))
    body.append(f'<rect x="40" y="{cy + r + 48}" width="14" height="10" rx="1" fill="{PANEL}" stroke="{AMBER}"/>')
    body.append(text(62, cy + r + 57, "PaxosLeases acceptor", 10, SLATE, family=MONO))
    body.append(f'<line x1="210" y1="{cy + r + 53}" x2="232" y2="{cy + r + 53}" stroke="{ROSE}" stroke-dasharray="3 3"/>')
    body.append(text(240, cy + r + 57, "link cut", 10, SLATE, family=MONO))

    # stats of the 2026-10-01 night (chaos 1001, pairs 1002, scenarios 1003)
    sx0, sy = 430, 170
    body.append(kicker(sx0, sy, "One night on five bare-metal servers", SLATE, 9.5))
    for k, (big, lbl) in enumerate([("602", "fault episodes: in stacks,"), ("6.9 h", "of faults while the"),
                                    ("61", "fault classes that night;")]):
        x = sx0 + k * 170
        body.append(text(x, sy + 42, big, 26, AMBER, 500, MONO, extra='letter-spacing="-0.6"'))
        body.append(text(x, sy + 64, lbl, 10.5, SLATE))
    for k, lbl in enumerate(["in pairs and in scenarios", "workload runs", "each applied and removed"]):
        body.append(text(sx0 + k * 170, sy + 78, lbl, 10.5, SLATE))
    iy = sy + 116
    body.append(kicker(sx0, iy, "79 invariants, examined from the logs only", SLATE, 9.5))
    inv = [("fence-order", "the epoch of a key's writes never decreases"),
           ("applied-twice", "no operation occurs two times"),
           ("ack-lost", "the store keeps each write that it confirmed"),
           ("stream-gap · stream-dup", "a stream does not skip or repeat a record"),
           ("liveness-after", "each call gets a reply in 5 lease periods after a heal")]
    for k, (n, what) in enumerate(inv):
        y = iy + 24 + k * 24
        body.append(f'<line x1="{sx0}" y1="{y + 8}" x2="920" y2="{y + 8}" stroke="{LINE}"/>')
        body.append(text(sx0, y, n, 11, PAPER, 500, MONO))
        body.append(text(sx0 + 190, y, what, 11, SLATE))

    # the 68 classes as chips
    groups = [("network", NETWORK), ("disk", DISK), ("process", PROCESS), ("clock", CLOCK), ("resources", None),
              ("input and config", INPUT)]
    known = NETWORK | DISK | CLOCK | PROCESS | INPUT
    y = 560
    body.append(f'<line x1="40" y1="{y - 22}" x2="920" y2="{y - 22}" stroke="{LINE}"/>')
    body.append(kicker(40, y, f"All {len(CLASSES)} fault classes", AMBER, 10))
    y += 22
    for i, (name, members) in enumerate(groups):
        items = sorted(c for c in CLASSES if (c in members if members else c not in known))
        if i:
            body.append(f'<line x1="40" y1="{y - 6}" x2="920" y2="{y - 6}" stroke="{LINE}" stroke-opacity="0.6"/>')
        body.append(text(40, y + 13, name[0].upper() + name[1:], 12.5, PAPER, 500))
        body.append(text(40, y + 31, f"{len(items)} classes", 11, SLATE))
        for j, c in enumerate(items):
            body.append(text(190 + j % 5 * 146, y + 13 + j // 5 * 20, c, 12, PAPER, family=MONO))
        y += max(2, (len(items) + 4) // 5) * 20 + 16
    svg("fault-rig.svg", 960, y + 16, body, "The real-fault rig: five servers, 68 fault classes, 79 invariants")


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    layers(*fault_matrix())
    tla()
    rig()
