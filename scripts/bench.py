#!/usr/bin/env python3
"""scripts/bench.sh's parse, compare and merge steps; the rules are in crates/axon/guide/benchmarks.md, The gate.

  bench.py parse <run dir>        <run dir>/logs/<job>.r<n>.log -> CSV on stdout, one row per metric
  bench.py compare <base> <now>   exit 1 on a REGRESSION or a MISSING row, 2 when it cannot compare
  bench.py merge <base> <now>     the baseline with the jobs <now> ran replaced by <now>'s rows, on stdout
"""
import csv, math, os, re, statistics, sys, traceback

FLOOR = 3.0
UNITS = {"ns": 1e-9, "µs": 1e-6, "us": 1e-6, "ms": 1e-3, "s": 1.0}
LOWER = {"ns/msg", "ns/frame", "ns/record", "ns/record/subscriber", "ns/thread", "µs", "µs/batch", "ms", "s", "cores",
         "B", "MB", "ratio", "leases", "records", "resets"}
HIGHER = {"req/s", "frames/s", "msg/s", "M calls/s", "M records/s", "/s"}
FIELDS = ["job", "mode", "metric", "unit", "better", "median", "runs"]


def dur(text, unit="s"):
    """'828.51ms' -> '0.82851' in `unit`, keeping the digits printed ('1.40s' stays '1.40')."""
    m = re.fullmatch(r"([\d.]+)(ns|µs|us|ms|s)", text)
    shift = round(math.log10(UNITS[m[2]] / UNITS[unit]))
    return f"{float(m[1]) * 10**shift:.{max(0, len(m[1].partition('.')[2]) - shift)}f}"


def parse(job, text):
    out = []  # (mode, metric, value, unit, better)

    def add(mode, metric, v, unit, better=None):  # v as printed, or as computed
        out.append((mode, metric, v if isinstance(v, str) else f"{v:.6g}", unit, better if better is not None else
                    "lower" if unit in LOWER else "higher" if unit in HIGHER else ""))

    lines = [l for l in text.splitlines() if not l.startswith("#")]
    t = "\n".join(lines)
    if job.startswith("perf-"):
        seen = {}
        for l in lines:
            if m := re.match(r"(.+?): n=\d+ .*ns/msg=([\d.]+)", l):
                seen[m[1]] = m[2]  # route and topic run twice: the second, with the workers awake, is kept
        for label, v in seen.items():
            add(job[5:], label, v, "ns/msg")
    elif job.startswith("pipeline"):
        kv = dict(re.findall(r"(\S+?)=(\S+)", lines[0]))
        mode = job[9:]
        add(mode, "req/s", kv["req/s"], "req/s")
        for k in ["p50", "p99", "p99.9"]:
            add(mode, k, dur(kv[k], "us"), "µs")
        add(mode, "max", dur(kv["max"], "us"), "µs", "")  # a single sample
        add(mode, "syncs/s", kv["syncs/s"], "1/s")
        add(mode, "entries/sync", kv["entries/sync"], "entries")
    elif job == "loopback":
        for l in lines[1:]:
            p, codec, fps, _, _, p50, p99 = l.split()
            add(f"{p} {codec}", "one-way", fps, "frames/s")
            add(f"{p} {codec}", "echo RTT p50", p50, "µs")
            add(f"{p} {codec}", "echo RTT p99", p99, "µs")
    elif job == "remote_calls":
        for l in lines:
            if m := re.match(r"sequential (\S+), 256 B: p50 (\S+)\s+p99 (\S+)\s+p99.9 (\S+)", l):
                for k, v in zip(["p50", "p99", "p99.9"], m.groups()[1:]):
                    add(f"sequential {m[1]}", k, dur(v, "us"), "µs")
            elif m := re.match(r"\s*(\d+) concurrent requests:\s+(\d+)", l):
                add(f"{m[1]} in flight", "throughput", m[2], "req/s")
            elif m := re.match(r"tell, 256 B:\s+(\d+) msg/s sent, (\d+) delivered, (\d+) dropped", l):
                add("tell", "sent", m[1], "msg/s")
                add("tell", "dropped", m[3], "msgs")
    elif job.startswith("quota-check"):
        first = None
        for l in lines:
            if m := re.match(r"(\w+)\s+(\d+)\s+(\w+)\s+(\d+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)$", l):
                op, keys, dist, th, mps, _, adm = m.groups()
                if first is not None:  # printed before its size's rows
                    add(f"check keys={keys}", "first call, 24 threads", first, "ns/thread")
                    first = None
                add(f"check {op} {dist} keys={keys}", f"{th} threads", mps, "M calls/s")
                if float(adm) < 1 and op != "AtLimit":
                    add(f"check {op} {dist} keys={keys}", f"{th} threads admitted", adm, "fraction")
            elif m := re.match(r"first call for a key: (\d+) ns", l):
                first = m[1]
            elif m := re.match(r"(\d+) entries, (\d+) rows, (\d+) units on the fallback in the last pass, (\S+) after", l):
                add(f"check entries={m[1]}", "grants settled after", dur(m[4]), "s")
                add(f"check entries={m[1]}", "rows", m[2], "rows")
            elif m := re.match(r"idle with (\d+) entries: ([\d.]+) cores", l):
                add(f"check entries={m[1]}", "idle", m[2], "cores")
    elif job == "quota-memory":
        m = re.search(r"lone (\d+), three nodes one using (\d+), all three (\d+)", t)
        for k, v in zip(["lone", "one of three", "all three"], m.groups()):
            add("memory 200 k keys", f"resident per key, {k}", v, "B")
        m = re.search(r"an entry, granted: (\d+) B; a replica's key with one row: (\d+) B; each further row: (-?\d+) B", t)
        for k, v in zip(["entry", "key with one row", "further row"], m.groups()):
            add("memory 200 k keys", k, v, "B")
    elif job == "quota-bytes":  # what the regrant sends and denies is behaviour, not speed
        for l in lines:
            if m := re.match(r"\s*(\d+) takes/s a node:\s+(\d+) B/s sent a node, ([\d.]+) reports/s a node, (\d+) fallback units/s, (\d+) denied/s", l):
                mode = f"bytes, {m[1]} takes/s a node"
                add(mode, "sent a node", m[2], "B/s")
                add(mode, "reports a node", m[3], "1/s")
                add(mode, "fallback units", m[4], "1/s")
                add(mode, "denied a node", m[5], "1/s")
    elif job == "quota-pull":
        m = re.search(r"none pulling (\S+) after, every key held (\S+) after", t)
        add("pull 1 M keys", "none pulling after d left", dur(m[1]), "s")
        add("pull 1 M keys", "every key held after d left", dur(m[2]), "s")
        m = re.search(r"d back: ready (\S+) after its start, holding (\d+) rows", t)
        add("pull 1 M keys", "d ready after its restart", dur(m[1]), "s")
    elif job == "fanout":
        for l in lines[1:]:
            m = re.match(r"(.+?)\s+([\d.]+) ns/record", l)
            add(re.sub(r"\s+", " ", m[1]).strip(), "cost", m[2], "ns/record" + ("/subscriber" if l.startswith("fanout") else ""))
    elif job == "disk":
        for m in re.finditer(r"(\w+): ([\d.]+) M records/s published and durable, ([\d.]+) µs median until durable", t):
            add(m[1], "published and durable", m[2], "M records/s")
            add(m[1], "median wait until durable", m[3], "µs")
    elif job in ("log", "log-roll"):
        for l in lines:
            if m := re.match(r"\| 1 \| (\S+) \| (\d+) \| (\d+) \| \d+ \| (\d+) \| (\d+) \| (\d+) \| (\d+) \|", l):
                mode = f"{m[1]}, {m[2]} a write"
                add(mode, "records/s", m[3], "/s")
                for k, v in zip(["p50", "p99", "p99.9"], m.groups()[3:6]):
                    add(mode, k, v, "µs")
                add(mode, "max", m[7], "µs", "")  # a single sample
    elif job == "overhead":
        section = None
        for l in lines:
            if l.startswith(("embedded:", "hosted:", "transport:")):
                section = l.split(":")[0]
            elif section and (m := re.match(r"(.+?)\s+([\d.]+)\s+([\d.]+)\.\.([\d.]+)\s+[+-][\d.]+%", l)):
                add(section, m[1].strip(), m[2], "ns/frame" if section == "transport" else "ns/msg")
    elif job == "scale-actors":
        for l in lines[1:]:
            if m := re.match(r"(\S.*?)\s{2,}([\d.]+) (/s|B resident|cores|MB)", l):
                add("actors 1 M", m[1], m[2], m[3].replace("B resident", "B"))
    elif job.startswith("scale-keyed"):
        add(job[6:], "keyed spawns", re.search(r": (\d+) /s", t)[1], "/s")
        if m := re.search(r"median of 5: (\S+)", t):
            add(job[6:], "renew 100 k leases, median of 5", dur(m[1], "ms"), "ms")
    elif job.startswith("scale-nodes"):
        mode = job[6:]
        m = re.search(r"started and agreed in (\S+),", t)
        add(mode, "start and agree", dur(m[1]), "s")
        res = re.findall(r"resident per node\s+([\d.]+) MB", t)
        add(mode, "resident per node, no links", res[0], "MB")
        add(mode, "resident per node, 20 links", res[1], "MB")
        add(mode, "idle, no links", re.search(r"idle, no links\s+([\d.]+)", t)[1], "cores")
        add(mode, "idle, 20 links", re.search(r"idle, 20 links per node\s+([\d.]+)", t)[1], "cores")
        add(mode, "links opened in", dur(re.search(r"links opened in (\S+),", t)[1]), "s")
        add(mode, "join seen by every node", dur(re.search(r"a join seen by every node in (\S+),", t)[1]), "s")
        add(mode, "leave seen by every node", dur(re.search(r"a leave seen by every node in (\S+),", t)[1]), "s")
        add(mode, "keyed actors placed", re.search(r"placed at (\d+) /s", t)[1], "/s")
        add(mode, "resident per keyed actor", re.search(r"per keyed actor\s+(\d+) B", t)[1], "B")
        add(mode, "idle, keyed actors placed", re.search(r"idle, keyed actors placed\s+([\d.]+)", t)[1], "cores")
        add(mode, "join moving keys settles", dur(re.search(r"keyed actors settled in (\S+),", t)[1]), "s")
        add(mode, "restart, each", dur(re.search(r"restarted one by one in \S+ \((\S+) each\)", t)[1]), "s")
        add(mode, "leases the other nodes lost", re.search(r"leases the other nodes lost\s+(\d+)", t)[1], "leases")
    elif job == "scale-streams":
        m = re.search(r"subscriptions up in (\S+) at ([\d.]+) cores.*rss (\d+) MB", t)
        add("streams 50 k", "subscriptions up", dur(m[1]), "s")
        add("streams 50 k", "resident after subscriptions", m[3], "MB")
        m = re.search(r"published (\d+) records/s .* at ([\d.]+) cores: (\d+) of (\d+) delivered.*?(\d+) resets", t)
        add("streams 50 k", "200 k records/s published", m[2], "cores")
        add("streams 50 k", "records not delivered", int(m[4]) - int(m[3]), "records")
        m = re.search(r"subscriptions changed in \S+ at ([\d.]+) cores", t)
        add("streams 50 k", "subscription churn", m[1], "cores")
        m = re.search(r"a relay lost: every subscription fed by another parent after (\S+) at .*rss (\d+) MB", t)
        add("streams 50 k", "relay lost, all fed again", dur(m[1]), "s")
        add("streams 50 k", "resident after relay loss", m[2], "MB")
    elif job == "scale-subscribers":
        m = re.search(r"([\d.]+) M records/s delivered, (\d+) ns processor time per record, ([\d.]+) cores", t)
        mode = "subscribers 20 k, 200 batches/s"
        add(mode, "delivered", m[1], "M records/s", "")  # the offered load
        add(mode, "processor time", m[2], "ns/record")
        add(mode, "cores", m[3], "cores")
        add(mode, "resets", re.search(r"(\d+) resets", t)[1], "resets")
    elif job == "scale-metrics":
        m = re.search(r"(\d+) bytes a batch.*write ([\d.]+) us", t)
        add("metrics 256 MiB", "bytes a batch", m[1], "B", "")
        add("metrics 256 MiB", "write", m[2], "µs/batch")
        add("metrics 256 MiB", "RSS growth / held bytes", re.search(r"([\d.]+) of held_bytes", t)[1], "ratio")
    if not out:
        raise ValueError(f"{job}: nothing parsed")
    return out


def parse_run(run):
    values, units = {}, {}
    for line in open(os.path.join(run, "jobs.txt")):
        job = line.split("|")[0]
        n = 1
        while os.path.exists(path := os.path.join(run, "logs", f"{job}.r{n}.log")):
            try:
                for mode, metric, v, unit, better in parse(job, open(path).read()):
                    values.setdefault((job, mode, metric), []).append(v)
                    units[(job, mode, metric)] = (unit, better)
            except (TypeError, IndexError, AttributeError, KeyError, ValueError) as e:
                raise ValueError(f"{path}: cannot parse ({e!r}): did the harness's output change?") from e
            n += 1
    write([*key, *units[key], f"{statistics.median(map(float, vs)):.6g}", " ".join(vs)] for key, vs in values.items())


def write(rows):
    w = csv.writer(sys.stdout, lineterminator="\n")
    w.writerow(FIELDS)
    w.writerows(rows)


def read(path):
    rows = csv.DictReader(l for l in open(path) if not l.startswith("#"))
    return {(r["job"], r["mode"], r["metric"]): r for r in rows}


def tolerance(b):
    """How far a row may move and still be noise: FLOOR of its median, the range of its rounds (with 5 or more, less
    the highest and lowest, as the median leaves out one slow round), one and a half steps of its printed precision
    (so one rounding flip is not a change; integers are counts, exact), and for idle cores at 0 a hundredth of a core."""
    bm, texts = float(b["median"]), b["runs"].split()
    runs = sorted(map(float, texts))
    runs = runs[1:-1] if len(runs) >= 5 else runs
    decimals = max((len(t.split(".")[1]) for t in texts if "." in t and "e" not in t), default=0)
    return max(FLOOR / 100 * abs(bm), runs[-1] - runs[0], 1.5 * 10**-decimals if decimals else 0,
               0.01 if bm == 0 and b["unit"] == "cores" else 0)


def compare(base_path, now_path):
    base, now = read(base_path), read(now_path)
    ran = {job for job, _, _ in now}
    fmt = lambda v: f"{v:.3g}" if abs(v) < 1000 else f"{v:,.0f}".replace(",", " ")
    table = []
    counts = {v: 0 for v in ["REGRESSION", "MISSING", "improved", "within noise", "reported", "new", "not run"]}
    for key in [*base, *(k for k in now if k not in base)]:
        b, n = base.get(key), now.get(key)
        if n is None:  # a job that ran and no longer prints a gated row has changed its output
            verdict = "MISSING" if key[0] in ran and b["better"] else "not run"
            counts[verdict] += 1
            if verdict == "MISSING":
                table.append((*key, fmt(float(b["median"])), "", "", "", verdict))
            continue
        nm = float(n["median"])
        if b is None:
            counts["new"] += 1
            table.append((*key, "", fmt(nm), "", "", "new"))
            continue
        bm = float(b["median"])
        if not b["better"]:
            counts["reported"] += 1
            table.append((*key, fmt(bm), fmt(nm), "", "", "reported"))
            continue
        tol, change = tolerance(b), nm - bm
        worse = change if b["better"] == "lower" else -change
        verdict = "REGRESSION" if worse > tol else "improved" if worse < -tol else "within noise"
        counts[verdict] += 1
        pct = lambda v: f"{v / abs(bm) * 100:.1f} %" if bm else fmt(v)
        table.append((*key, fmt(bm), fmt(nm), ("+" if change >= 0 else "") + pct(change), "±" + pct(tol), verdict))
    head = ("job", "mode", "metric", "baseline", "now", "change", "band", "verdict")
    widths = [max(len(r[i]) for r in [head, *table]) for i in range(len(head))]
    for r in [head, *table]:
        print("  ".join(c.rjust(w) if 3 <= i <= 6 else c.ljust(w) for i, (c, w) in enumerate(zip(r, widths))).rstrip())
    print("\n" + ", ".join(f"{v} {k}" for k, v in counts.items()) + f" (baseline {base_path})")
    return 1 if counts["REGRESSION"] or counts["MISSING"] else 0


def merge(base_path, now_path):
    base = read(base_path) if os.path.exists(base_path) else {}
    now = read(now_path)
    ran = {job for job, _, _ in now}
    jobs = dict.fromkeys(job for job, _, _ in [*base, *now])  # the baseline's order, new jobs last
    write(list(r.values()) for job in jobs for k, r in (now if job in ran else base).items() if k[0] == job)


if __name__ == "__main__":
    try:
        match sys.argv[1:]:
            case ["parse", run]:
                parse_run(run)
            case ["compare", base, now]:
                sys.exit(compare(base, now))
            case ["merge", base, now]:
                merge(base, now)
            case _:
                print(__doc__, file=sys.stderr)
                sys.exit(2)
    except Exception:
        traceback.print_exc()
        sys.exit(2)
