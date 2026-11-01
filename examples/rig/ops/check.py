#!/usr/bin/env python3
"""Checks a rig run from its logs alone (spec/fault-rig.md § Invariants).

    check.py <rundir>          writes check.txt, violations.jsonl and recovery.json there, prints check.txt; exits 1
                               on a violation, 2 on a run it cannot read or judged too little of, 3 on a store log it
                               cannot trust
    check.py --selftest [n]    runs every plant of PLANTS on a synthetic run, then times a clean run of n ops
                               (default 1000000; 0 skips it)
    check.py --gate <rundir|recovery.json> <baseline.json> [ratio]
                               each class's recovery against a baseline's (default ratio 2); exits 1 on a REGRESSION,
                               2 on runs of another kind

Python 3.9, stdlib only. Times are wall microseconds; the store's clock (b1) is the timeline.

To extend it (spec/fault-rig.md § Extending the rig): an invariant is a name in INVARIANTS, a check (a Run method
in CHECKS, or lines in one) that calls self.flag and counts self.judged, and a @plant that fails without it; a fault
class gets an EFFECT entry; a workload's clean lines join the synthetic run through SYNTH_EXTRAS.
"""
import bisect
import collections
import functools
import itertools
import json
import math
import os
import pickle
import random
import re
import shutil
import sys
import tempfile
import time
import types

S = 1_000_000
MS = 1_000
INVARIANTS = [
    "fence-order", "epoch-two-nodes", "epoch-two-writers", "turn-after-takeover", "ack-lost", "ack-mismatch",
    "applied-twice", "phantom", "chain", "journal-missing", "liveness-after", "liveness-during", "io-lease-lost",
    "mass-drop", "lease-lost-quiet", "latency-quiet", "clock-behind", "growth",
    # Each follower checks its own records (src/streams.rs) and logs what breaks; these rest on no clock.
    "stream-dup", "stream-reorder", "stream-gap", "stream-liveness",
    "stream-publish-err", "stream-reset-kind", "stream-quiet", "stream-generation",
    # The process and its logs.
    "give-up", "log-complete", "heartbeat", "client-task", "unexpected-restart", "shutdown-bounded",
    # A malformed or confused peer at the node's own ports (src/peer.rs).
    "peer-slots", "peer-impostor",
    # The harness: the faults happened as planned and did what they do; the store's log holds together.
    "nemesis", "fault-effect", "store-void",
    # The disk workload's streams, on a disk of their own (src/disk.rs).
    "disk-durable", "disk-resume", "disk-error", "disk-retention",
    # The process lane: a hook's panic stays in its actor; a node's restarts leave its peers as they were.
    "panic-contained", "peer-leak",
    # Beyond the store's lines: actors and turns against the leases, latency around a fault, sockets, the kernel, the
    # clocks the ops bracket, and each journal line against its record.
    "zombie", "turn-after-acquire", "blast-radius", "sockets", "kernel", "causality", "journal-divergent",
]
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import nemesis  # noqa: E402  the planner's tags, so the two never name a class's kind apart

PROBES, CLOCK, IO, NETWORK, RESTARTS = nemesis.PROBES, nemesis.CLOCK, nemesis.DISK, nemesis.NETWORK, nemesis.RESTARTS
# A process that resumes runs the turn it was in. A killed one runs nothing more: bytes it wrote before the SIGKILL
# may still arrive after, from the kernel's send buffer, which DELTA covers.
RESUMES = {"pause", "freeze", "skew-restart"}
# The node's process is killed and restarted (RESTARTS, tagged restart: diskerr and diskdrop by the reset at their
# off), or stops without ending (nemesis.STOPS).
HALTS = nemesis.STOPS | RESTARTS
DURING = {"kill": lambda T: 3 * T + 2, "isolate": lambda T: 3 * T + 5, "leave": lambda T: 3,
          "oom": lambda T: 3 * T + 2}
DELTA, SLACK, RESUME_WINDOW = 5 * MS, 10 * MS, 50 * MS
WINDOW = 10 * S
STORE_ONLY = {"fence-order", "chain", "applied-twice", "ack-lost", "ack-mismatch", "phantom", "journal-missing"}
# A step of the nemesis takes a few ssh round trips, but a stop waits for the node to leave (TimeoutStopSec=30) and a
# reset for e2fsck or mkfs.
STEP_BOUND = 2 * S
SLOW_STEP = {("leave", "on"): 35 * S}
# The disk classes whose off resets their disk (fault.sh), and those whose disk lost or changed what it had synced.
RESET_AT_OFF = ("diskerr", "diskdrop", "powercut", "rofs", "flaky", "readerr", "diskdead", "diskcorrupt")
LYING = {"diskdrop", "diskcorrupt"}
SLOW_STEP.update({(c, "off"): 60 * S for c in RESET_AT_OFF})
SLOW_STEP.update({("powercut", "on"): 30 * S, ("readerr", "on"): 10 * S, ("inodes", "on"): 30 * S,
                  ("inodes", "off"): 30 * S})
# The suspend of a table swap waits for the device's in-flight IO, which the box's other loop files slow: seen 14 s.
SLOW_STEP.update({("slowdisk", "on"): 20 * S, ("splitdelay", "on"): 20 * S})
# A restart waits for the node to leave; an upgrade then until it listens again (fault.sh, 30 s at most).
SLOW_STEP[("upgrade", "on")] = 65 * S
SLOW_STEP[("cfgdrift", "on")] = SLOW_STEP[("cfgdrift", "off")] = SLOW_STEP[("scale", "on")] = 35 * S
HEARTBEAT = 15 * S  # the stats and streams lines come every 10 s
LINES_SLACK = 16  # client lines a kill cuts between counting an ack and writing its line, one per worker thread
DOWN_BOUND = 25 * S  # under systemd's TimeoutStopSec=30
HEAL_LAG = 45 * S  # a heal or reset logs after it restarted the unit: systemd's 30 s stop, then fsck or mkfs
GIVE_UP = 120 * S  # src/load.rs
STREAM_BAD_RESET = {"Gap", "InvalidRecovery", "Revoked"}  # no fault explains these: the relay broke its guarantee
STREAM_SLOW_RESET = {"SlowConsumer", "GlobalCapacity"}  # only while a node on the path is starved
STREAM_STARVED = {"rate", "cpu", "memhigh", "pause", "freeze"}
STARVES = {"cpu", "memhigh", "reclaim", "swap", "threadstarve"}  # a thread runs late: its line comes after the work
STREAM_BAD_SUBSCRIBE = ("Forbidden", "InvalidOptions", "LimitExceeded", "Recovery(Gap", "Recovery(ConflictingDuplicate",
                        "Recovery(InvalidBatch", "Recovery(PositionOverflow", "Recovery(InvalidSnapshot",
                        "Recovery(Adapter")
# Anti-vacuity: below these judged counts, or with less steady time before the first episode, a run reads UNJUDGED.
FLOORS = {
    "smoke": {"ack-lost": 2000, "journal-missing": 1000, "liveness-after": 500, "heartbeat": 30, "fault-effect": 5,
              "stream-gap": 10000, "sockets": 5},
    "night": {"ack-lost": 20000, "journal-missing": 10000, "liveness-after": 5000, "heartbeat": 500,
              "fault-effect": 20, "stream-gap": 100000, "sockets": 50},
    "selftest": {"ack-lost": 1000, "journal-missing": 1000, "liveness-after": 100, "heartbeat": 30, "fault-effect": 3,
                 "stream-gap": 1000, "sockets": 5},
}
FLOOR_WORKLOAD = {"stream-gap": {"streams"}}  # a floor holds only where the workloads it judges ran
STEADY_FLOOR = {"smoke": 20 * S, "night": 120 * S, "selftest": 20 * S}
for _mode in ("chaos", "pairs", "scenarios"):  # the schedules: judged at least as a smoke is, at either timing
    FLOORS[_mode], STEADY_FLOOR[_mode] = FLOORS["smoke"], STEADY_FLOOR["smoke"]
# `growth` (spec/fault-rig.md § Invariants): the slack per judged resource, in its unit. mem_kb holds the page
# cache of the logs and journal, which grows with the run, actors follow the node's share of keys, and sessions has a
# link each way per peer (sockets bounds it): reported only. Threads are held to a ceiling instead: tokio's blocking
# pool keeps its high-water mark while its jobs keep coming, up to BLOCKING_CAP, over the node's fewest threads.
GROWTH_WARMUP, GROWTH_SPAN, GROWTH_RATIO = 6 * 60 * S, 15 * 60 * S, 0.25
GROWTH_SLACK = {"rss_kb": 64 * 1024, "anon_kb": 64 * 1024, "fds": 32}
BLOCKING_CAP, THREADS_SLACK = 512, 16
GROWTH_SHOWN = ("mem_kb", "actors", "sessions")
# liveness-during: the classes that take the node's process away (any other fault on a node leaves it impaired, and
# its ops unjudged), those that cut links, those that cut some of a link's packets or connections (the node impaired
# as well), and those under which an acceptor grants no lease of T in time; a class not in DURING is bound by 3T + 5 s.
GONE = HALTS - IO | {"leave-slowdisk"}
CUTS = {"isolate", "partition", "oneway", "bridge"}
FRAYS = {"reject", "syndrop", "graylink", "flap"}
SLOW = {"netem", "mtu", "asym"}  # rate: a starvation under the blob workload, else harmless (during-rate)
DRIFTS = {"drift", "drift-far", "drift-edge"}
# zombie: a lease, the store call a stop waits out (10 s), and 7 s of the engine's ticks. A node memory.high throttles
# runs its stops late, as a stopped one does: chaos-20260930-2145-s44 stopped b1's actors 19 to 40 s late.
ZOMBIE = lambda T: (T + 17) * S
ZOMBIE_LATE = HALTS | {"memhigh"}
# blast-radius: the ops a fault on one node must leave alone, p99 and max. Classes that stop the node or cut it off
# are judged from 3T + 5 s after their on, once its keys moved; the rest from the on. b1's starvation classes are not
# judged until measured: the nemesis and the store run beside it.
BLAST_LATER = {"kill", "pause", "freeze", "isolate", "leave", "oom"}
BLAST_NOW = {"io", "slowdisk", "diskerr", "diskfull", "diskdrop", "cpu", "memhigh", "netem", "rate", "step", "drift"}
BLAST_NOT_B1 = {"cpu", "memhigh", "oom", "pause", "freeze"}
BLAST_P99, BLAST_MAX, QUIET_P99 = S, 5 * S, S
# sockets: established connections to one peer on the cluster port (nemesis.SOCKETS), over a session each way per
# lane (spec/lanes.md § 2.1) and a redial.
LANES = 3
PER_PEER = 2 * LANES + 2
# kernel: (a message's text, the fault classes whose window explains it, None for none).
# The kernel writes "Possible SYN flooding"; backlog's listen queue of one overflows by design.
KERNEL = [("nf_conntrack: table full", None), ("TCP: out of memory", None), ("Possible SYN flooding", {"backlog"}),
          ("blocked for more than", None), ("EXT4-fs error", IO), ("I/O error", IO), ("Killed process", {"oom"})]
# The clock map moves a line by its second's bracket only where the bracket is this tight and the nemesis's sample
# falls out of it.
TIGHT = 100 * MS
JOURNAL_FIELDS = ("k", "e", "n", "s", "d", "b", "a", "o")
DELTA_LOOSE = 20 * MS  # a refusal whose δ is past this is judged loosely, and counted
CAUSAL_SLACK = 2 * MS  # a second's bracket may cross by this much: chrony's slew, the stamps' grain
# The recovery each class should need beyond the wait for a caller (Run.idle), reported beside what it took: a lease
# and the engine's tick for a node gone, a keepalive more for a cut, a resume more for a stop, a restart for a disk.
B_CLASS = {**dict.fromkeys(("kill", "oom", "leave"), lambda T: T + 3),
           **dict.fromkeys(CUTS, lambda T: T + 4),
           **dict.fromkeys(("pause", "freeze"), lambda T: T + 5),
           **dict.fromkeys(("io", "slowdisk", "diskerr", "diskfull", "diskdrop", "cpu"), lambda T: 5)}
GATE_MIN, GATE_SLACK = 5, 1.0  # --gate: the episodes a class needs in both runs, and seconds of noise beside the ratio


def pct(xs, q):
    if not xs:
        return None
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))]


def far_side(f, nodes):
    """The nodes a cut separates f's nodes from: the rest (isolate), `other` (partition, bridge) or `to`."""
    if f["class"] == "isolate":
        return set(nodes) - set(f["nodes"])
    return set(f["args"].get("other") or f["args"].get("to") or ())


def starves(f):
    """memhigh, or a cpu quota under 10 %: the node runs its turns too late to hold its leases."""
    return f["class"] == "memhigh" or f["class"] == "cpu" and float(f["args"].get("quota_pct", 100)) < 10


def fault_key(x):
    """A nemesis on or off line's fault: its id, <episode>.<index>, or in a log from before ids its class and nodes."""
    return x.get("id") or (x["i"], x["class"], tuple(x["nodes"]))


disk_of = nemesis.disk_of


def origin_of(s):
    """A stream's origin: b1 of b1/s0 and of disk-b1/s0."""
    return s.split("/")[0].rpartition("-")[2]


def evidence(f):
    """The EFFECT entry of a fault: its class's, or for a disk fault on another disk than the journal (class, disk)'s."""
    disk = disk_of(f) if f["class"] in IO else "journal"
    return EFFECT.get(f["class"] if disk == "journal" else (f["class"], disk), ())


def effect_floor(plan, n):
    """The fault-effect floor n, or the plan's faults with evidence where fewer: a smoke of ONLY a class or two."""
    return min(n, sum(1 for e in plan for f in e["faults"] if evidence(f)))


class Run:
    def __init__(self, d):
        self.dir, self.bad = d, 0
        p = os.path.join(d, "meta.json")
        self.meta = json.load(open(p)) if os.path.exists(p) else {}
        self.T = float(self.meta.get("lease_secs", 6))
        self.B = 5 * self.T * S
        params = self.meta.get("params") or {}
        self.every = int(params.get("every_ms") or 0) * MS
        self.keys = int(params.get("keys") or 0)
        # Quiet windows: B after an episode, and from a phase's start until every key was first called and leased.
        self.quiet_after, self.warmup = self.B, 2.5 * self.every + 2 * self.T * S + 10 * S
        self.acceptors = list(self.meta.get("acceptors") or [])
        self.plan = list(self.read(os.path.join(d, "plan.jsonl")))
        self.names = {e["i"]: e.get("name") for e in self.plan}  # a scenario's name, of a scenarios run
        self.wan = self.meta.get("timing") == "wan"
        self.nem = list(self.read(os.path.join(d, "nemesis.jsonl")))
        self.store = list(self.read(os.path.join(d, "store.jsonl")))
        self.v = collections.defaultdict(list)
        self.judged = collections.Counter()
        self.explained = collections.Counter()
        self.journal_exempt = collections.Counter()
        self.ts = collections.defaultdict(list)  # node -> the store time of each line it logged, for fault-effect
        self.rtts, self.loose, self.lanes = {}, 0, {}
        self.find_nodes()
        self.clock_samples()
        self.clock_map()
        self.episodes()

    def read(self, path):
        if not os.path.exists(path):
            return
        with open(path, "rb") as f:
            for raw in f:
                if raw.strip():
                    try:
                        yield json.loads(raw)
                    except ValueError:
                        self.bad += 1  # a line cut by a kill, or stale bytes a diskdrop left, with a line after them
                        x = self.salvage(raw)
                        if x is not None:
                            yield x

    @staticmethod
    def salvage(raw):
        """The whole line at the end of a bad one: a journal appends to the bytes a lying disk left before it."""
        i = raw.find(b'{"', 1)
        while i > 0:
            try:
                return json.loads(raw[i:])
            except ValueError:
                i = raw.find(b'{"', i + 1)
        return None

    def find_nodes(self):
        """A node's files are in the directory holding its client.jsonl or node.jsonl, named by its `up` line."""
        self.nodes = {}
        for dp, _, fs in os.walk(self.dir):
            if "client.jsonl" in fs or "node.jsonl" in fs:
                lines = list(self.read(os.path.join(dp, "node.jsonl")))
                name = next((x["node"] for x in lines if x.get("r") == "up" and "node" in x), os.path.basename(dp))
                journals = [os.path.join(jp, f) for jp, _, jf in os.walk(dp) for f in jf
                            if f.startswith("journal") and f.endswith(".jsonl")]
                unit = os.path.join(dp, "unit.jsonl")
                self.nodes[name] = {"dir": dp, "node": lines, "journals": journals,
                                    "unit": [self.unit_line(x) for x in self.read(unit)] if os.path.exists(unit) else None}

    @staticmethod
    def unit_line(x):
        """(node wall µs, message) of a line of `journalctl -o json`, whose MESSAGE is bytes where not UTF-8."""
        m = x.get("MESSAGE", "")
        return int(x.get("__REALTIME_TIMESTAMP", 0)), bytes(m).decode(errors="replace") if isinstance(m, list) else str(m)

    def clock_samples(self):
        self.offs = collections.defaultdict(list)
        for x in self.nem:
            if x.get("ev") == "clock" and x.get("offset_ms") is not None:  # None: ssh to the node failed
                self.offs[x["node"]].append((x["t"], x["offset_ms"] * MS, (x.get("rtt_ms") or 0) * MS))
        for v in self.offs.values():
            v.sort()
        self.off_t = {n: [s[0] for s in v] for n, v in self.offs.items()}
        self.slop = {n: max(s[2] for s in v) for n, v in self.offs.items()}

    def to_store(self, node, t):
        """A node's wall time on the store's clock, through the nearest clock sample (offset = node - b1)."""
        v = self.offs.get(node)
        off = 0
        if v:
            i = bisect.bisect_left(self.off_t[node], t)
            if i == len(v) or (i > 0 and t - v[i - 1][0] < v[i][0] - t):
                i -= 1
            off = v[i][1]
        br, s = self.brackets.get(node, {}).get(t // S), self.slop.get(node, 0) + MS
        if br and not br[0] - s <= off <= br[1] + s:
            off = (br[0] + br[1]) // 2
        return t - off

    def clock_map(self):
        """Per node and second of its clock, the offsets (node - b1) its ops allow: an op's send came before the store's
        record of it, and its ack after. to_store takes a tight bracket's middle where the nemesis's sample, taken only
        at each settle, falls out of it: a clock stepped or drifting in between. A second the clock stepped back over
        came twice, at two offsets, and has none. On one box the bracket holds 0."""
        rt = {}
        for x in self.store:
            if x["r"] == "A" and x["v"] == "ok":
                rt.setdefault(x["op"], x["t"])
        self.brackets, self.tight, self.inverted = {}, {}, {}
        bad = self.bad  # check_clients reads these files again and counts their cut lines
        for node, nd in self.nodes.items():
            lo, hi, twice, top = {}, {}, set(), -math.inf
            for x in self.read(os.path.join(nd["dir"], "client.jsonl")):
                if "t" not in x:
                    continue
                if x["t"] < top - TIGHT:  # in write order: the clock went back from top
                    twice.update(range(x["t"] // S, top // S + 1))
                    top = x["t"]
                top = max(top, x["t"])
                t0 = rt.get(x.get("op"))
                if t0 is None:
                    continue
                b, d = x["t"] // S, x["t"] - t0
                if x.get("r") == "send":  # one per op, before its first try (src/load.rs)
                    lo[b] = max(lo.get(b, -math.inf), d)
                elif x.get("r") == "ack":
                    hi[b] = min(hi.get(b, math.inf), d)
            br = {b: (lo[b], hi[b]) for b in lo.keys() & hi.keys() - twice}
            self.judged["causality"] += len(br)
            self.inverted[node] = sorted((b, l, h) for b, (l, h) in br.items() if l > h + CAUSAL_SLACK)
            tight = self.brackets[node] = {b: lh for b, lh in br.items() if 0 <= lh[1] - lh[0] <= TIGHT}
            # The store's seconds whose lines a bracket placed: liveness-after judges an op there under a clock fault.
            self.tight[node] = {(b * S + dt - (lh[0] + lh[1]) // 2) // S for b, lh in tight.items() for dt in (0, S - 1)}
        self.bad = bad

    def episodes(self):
        eps = {}
        self.resets = collections.defaultdict(list)
        self.disk_resets = collections.defaultdict(list)  # (node, disk) -> when each reset of it ended
        self.kills = collections.defaultdict(list)  # node -> (sent, done) of each kill the nemesis sent it, store time
        ts = [x["t"] for x in self.store] + [x["t"] for x in self.nem if "t" in x]
        self.end = max(ts) if ts else 0
        for x in self.nem:
            ev = x.get("ev")
            if ev in ("on", "off"):
                e = eps.setdefault(x["i"], {"i": x["i"], "faults": {}})
                f = e["faults"].setdefault(fault_key(x), {"class": x["class"], "nodes": list(x["nodes"]),
                                                          "args": x.get("args") or {}, "ep": x["i"]})
                if ev == "on" and (x["class"] == "kill" or x.get("sub") == "kill"):  # a step kills its targets only
                    for n in set(x.get("targets", x["nodes"])) - set(x.get("noop", ())):
                        self.kills[n].append((x["sent"], x["done"]))
                if ev == "on" and x["class"] in ("powercut", "readerr"):  # their kill; powercut's freeze at its start
                    for n in set(x["nodes"]) - set(x.get("noop", ())):
                        self.kills[n].append((x["sent"], x["sent"] + 500 * MS if x["class"] == "powercut" else x["done"]))
                if ev == "on":  # skew-restart logs an on and an off per step (`sub`): its first on, its last off
                    f.setdefault("on", (x["sent"], x["done"]))
                    if x.get("noop"):  # the nodes where fault.sh found the unit not running
                        f["noop"] = sorted(set(f.get("noop", ())) | set(x["noop"]))
                else:
                    f["off"] = (x["sent"], x["done"])
            elif ev == "journal-reset":
                self.resets[x["node"]].append((x["t"], x.get("how", "fsck"), x.get("i"), x.get("detail", "")))
                self.disk_resets[(x["node"], "journal")].append(x["t"])
            elif ev == "disk-reset":  # another disk than the journal, `-d streams`: no journal line waived by it
                self.disk_resets[(x["node"], x.get("disk"))].append(x["t"])
        # TIMING=wan's backdrop is no episode: it stands apart in self.ambient, where only check_wan reads it, unless
        # its scenario is a probe.
        self.ambient, self.bulks = [], []
        for e in eps.values():
            e["name"] = self.names.get(e["i"])
            keep, kept = e["name"] in nemesis.GEO_PROBES, []
            for f in e["faults"].values():
                f.setdefault("on", (self.end, self.end))
                f.setdefault("off", (self.end, self.end))
                (kept if keep or not ambient(f) else self.ambient).append(f)
                if f["class"] == "bulk":
                    self.bulks.append(f)
            e["faults"] = kept
        # run.sh's backdrop (nemesis.py backdrop): geo on every node from before the nodes start until they stop.
        for x in self.nem:
            if x.get("ev") == "backdrop" and x.get("rc") == 0 and x["on"]:
                self.ambient.append({"class": "geo", "nodes": list(x["nodes"]), "args": {}, "ep": None,
                                     "on": (x["sent"], x["done"]), "off": (self.end, self.end)})
            elif x.get("ev") == "backdrop" and not x["on"] and self.ambient and self.ambient[-1]["ep"] is None:
                self.ambient[-1]["off"] = (x["sent"], x["done"])
        self.eps = sorted((e for e in eps.values() if e["faults"]), key=lambda e: min(f["on"][0] for f in e["faults"]))
        for e in self.eps:
            e["start"] = min(f["on"][0] for f in e["faults"])
            e["end"] = max(f["off"][1] for f in e["faults"])
            e["classes"] = sorted({f["class"] for f in e["faults"]})
            e["probe"] = bool(PROBES & set(e["classes"])) or e["name"] in nemesis.GEO_PROBES
        by_i = {e["i"]: e for e in self.eps}
        # A reset is a kill for liveness, and a forced heal ends a fault its off did not: the episode lasts until them.
        self.healed = collections.defaultdict(list)  # node -> when a heal or a reset ended on it
        for x in self.nem:
            if x.get("ev") in ("heal", "journal-reset", "disk-reset"):
                self.healed[x["node"]].append(x["t"])
                if x.get("i") in by_i:
                    by_i[x["i"]]["end"] = max(by_i[x["i"]]["end"], x["t"])
        for h in self.healed.values():
            h.sort()
        # Episodes that overlap are judged as one, from the first start to the last end, blamed on all of them.
        self.clusters = []
        for e in self.eps:
            c = self.clusters[-1] if self.clusters else None
            if c is not None and e["start"] <= c["end"]:
                c["end"], c["probe"] = max(c["end"], e["end"]), c["probe"] or e["probe"]
                c["is"].append(e["i"])
            else:
                self.clusters.append({"i": e["i"], "is": [e["i"]], "start": e["start"], "end": e["end"],
                                      "probe": e["probe"]})
            e["cluster"] = len(self.clusters) - 1
        self.starts = [c["start"] for c in self.clusters]
        self.ends = [c["end"] for c in self.clusters]
        self.windows = collections.defaultdict(list)  # node -> its faults, each on from on[0] until off[1]
        for e in self.eps:
            for f in e["faults"]:
                for n in sorted(set(f["nodes"]) - set(f.get("noop", ()))):
                    self.windows[n].append(f)

    def alone(self, e):
        """The episode overlaps no other, and the one before ended B before it began: what it alone causes."""
        c = e["cluster"]
        return len(self.clusters[c]["is"]) == 1 and (c == 0 or self.ends[c - 1] + self.B <= e["start"])

    def exempt(self, node, a, b, clock=True):
        """The node's process stopped, starved, restarted or ran a hook, or its clock moved, somewhere in [a, b]; with
        clock False a clock that moved excuses nothing, for what runs on the monotonic clock."""
        if self.fault_on(node, HALTS | (CLOCK if clock else set()) | {"memhigh", "threadstarve", "hook"}, a, b) or any(
                starves(f) for f in self.fault_on(node, {"cpu"}, a, b)):
            return True
        h = self.healed.get(node, [])
        i = bisect.bisect_left(h, a)
        return i < len(h) and h[i] - HEAL_LAG <= b

    def fault_on(self, node, classes, a, b):
        return [f for f in self.windows.get(node, ()) if f["class"] in classes and f["on"][0] <= b and a <= f["off"][1]]

    def lane(self, node, name):
        """A node's lines of <name>, read once, each with `_t` on the store's clock and `_inc` its incarnation."""
        if (node, name) not in self.lanes:
            nd, ups = self.nodes.get(node), self.ups.get(node, [])
            xs = self.lanes[(node, name)] = [  # read counted a line salvage cut down to a nested object, with no "t"
                x for x in self.read(os.path.join(nd["dir"], name)) if "t" in x] if nd else []
            for x in xs:
                x["_t"] = self.to_store(node, x["t"])
                x["_inc"] = bisect.bisect_right(ups, x["_t"])
        return self.lanes[(node, name)]

    def sent_in(self, lo, hi):
        """(start, op) of each op sent from lo to hi, both included."""
        b = self.by_start
        return b[bisect.bisect_left(b, (lo, "")):bisect.bisect_right(b, (hi, "￿"))]

    def flag(self, inv, t, probe=None, **kw):
        if "episode" not in kw:  # the last episodes begun by t, while t is within B of their end
            i = bisect.bisect_right(self.starts, t) - 1
            c = self.clusters[i] if i >= 0 and t <= self.ends[i] + self.B else None
            kw["episode"] = c and c["i"]
            if c and len(c["is"]) > 1:
                kw["episodes"] = c["is"]
        if probe is None:  # the store fences whatever the clocks do, so its invariants never belong to a probe
            probe = inv not in STORE_ONLY and any(e["probe"] and e["start"] <= t <= e["end"] + self.B for e in self.eps)
        active = [[f["class"], f["nodes"]] for e in self.eps if e["start"] <= t and t - self.B <= e["end"]
                  for f in e["faults"] if f["on"][0] <= t and t - self.B <= f["off"][1]]
        if active:
            kw.setdefault("faults", active)
        self.v[inv].append(dict(inv=inv, t=t, probe=probe, **kw))

    # ---- the store

    def check_store(self):
        self.acc = collections.defaultdict(list)
        self.owns = collections.defaultdict(list)
        self.rec = {}
        self.rec_ts = collections.defaultdict(list)  # writer -> when the store accepted each of its records
        refused = []
        owner, own_e, owned = {}, {}, set()
        for x in self.store:
            k, v, r = x["k"], x["v"], x["r"]
            if v == "ok":
                n0 = owner.setdefault((k, x["e"]), x["n"])
                if n0 != x["n"]:
                    self.flag("epoch-two-nodes", x["t"], k=k, e=x["e"], nodes=[n0, x["n"]])
                if r == "O":
                    self.owns[k].append(x)
                    own_e[k] = max(own_e.get(k, -1), x["e"])
                    owned.add((k, x["e"]))
                else:
                    if (k, x["e"]) not in owned:  # the store takes appends of an epoch it granted only
                        self.flag("store-void", x["t"], probe=False, what="append with no own before it", k=k,
                                  e=x["e"], op=x["op"])
                    self.acc[k].append(x)
                    self.rec_ts[x["n"]].append(x["t"])
                    if x["e"] < own_e.get(k, -1) or x["f"] != x["e"]:  # accepted behind a newer owner's fence
                        self.flag("fence-order", x["t"], k=k, s=x["s"], e=x["e"], f=x["f"], own_e=own_e.get(k))
                    if x["op"] in self.rec:
                        self.flag("applied-twice", x["t"], k=k, op=x["op"], s=[self.rec[x["op"]]["s"], x["s"]])
                    else:
                        self.rec[x["op"]] = x
            elif v == "stale":
                refused.append(x)
            elif v == "conflict":
                self.flag("epoch-two-nodes" if r == "O" else "epoch-two-writers", x["t"], k=k, e=x["e"], n=x["n"],
                          op=x.get("op"))
        self.total_balance = self.total_delta = 0
        for k, recs in self.acc.items():
            s_next, e_max = 0, -1
            for x in recs:
                if x["s"] != s_next or x["e"] < e_max:
                    self.flag("fence-order", x["t"], k=k, s=x["s"], want_s=s_next, e=x["e"], max_e=e_max)
                s_next, e_max = x["s"] + 1, max(e_max, x["e"])
            prev = 0
            for x in sorted(recs, key=lambda x: x["s"]):
                b, d, a, o = x["b"], x["d"], x["a"], x["o"]
                ok = b == prev and a >= 0 and (
                    (o == "ok" and a == b + d and b + d >= 0) or (o == "overdraft" and a == b and b + d < 0))
                if not ok:
                    self.flag("chain", x["t"], k=k, s=x["s"], op=x["op"], prev=prev, b=b, d=d, a=a, o=o)
                if o == "ok":
                    self.total_delta += d
                prev = a
            self.total_balance += prev
        n = sum(len(r) for r in self.acc.values())
        for inv in ("fence-order", "chain", "applied-twice", "phantom"):
            self.judged[inv] = n
        self.judged["epoch-two-nodes"] = self.judged["epoch-two-writers"] = n + sum(map(len, self.owns.values()))
        for x in refused:
            self.refusal(x)

    def refusal(self, x):
        k, e, n, S_, L = x["k"], x["e"], x["n"], x["t"], x.get("lag", 0)
        newer = next((o for o in self.owns[k] if o["e"] > e and o["t"] <= S_), None)
        if newer is None:  # the store refuses only behind a newer owner
            self.flag("store-void", S_, probe=False, what="refused with no newer own", k=k, e=e, n=n, r=x["r"])
            return
        self.judged["turn-after-takeover"] += 1
        bound, slack = self.turn_began(n, S_, L)
        if bound <= newer["t"] + slack:
            return
        w = self.resumed(n, bound, S_)
        if w:
            self.explained[w] += 1
            return
        self.flag("turn-after-takeover", S_, k=k, e=e, n=n, op=x.get("op"), r=x["r"], lag_us=L,
                  turn_after_new_owner_ms=round((bound - newer["t"]) / MS, 3), new_owner=newer["n"], new_e=newer["e"],
                  node_faults=[(w["class"], w["on"], w["off"]) for w in self.fault_on(
                      n, set(DURING) | RESUMES | {"cpu", "memhigh"} | DRIFTS, S_ - 60 * S, S_)])

    def turn_began(self, n, S_, L):
        """The earliest the turn of n's store line at S_, lag L, can have begun, and the slack n's faults give it."""
        drifts = self.fault_on(n, DRIFTS, S_, S_)
        eps = max([0.01] + [abs(float(w["args"].get("k", 0))) / 10000 for w in drifts])
        bound = S_ - L * (1 + eps) - self.delta(n, S_)
        slack = SLACK + (100 * MS if self.fault_on(n, {"cpu"}, bound, S_) else 0) + (
            S if self.fault_on(n, {"memhigh"}, bound, S_) else 0)
        return bound, slack

    def resumed(self, n, bound, S_):
        """The class of a fault on n that resumed it within [bound - 50 ms, S_], or None."""
        for w in self.fault_on(n, RESUMES, bound - RESUME_WINDOW, S_):
            if w["off"][0] <= S_ and bound - RESUME_WINDOW <= w["off"][1]:
                return w["class"]
        return None

    def delta(self, n, t):
        """δ, the most a store call from n at t spent on the way: DELTA, or the slowest round trip of n's stats line
        covering t."""
        if n not in self.rtts:
            xs = sorted((self.to_store(n, x["t"]), (x.get("rtt_us") or {}).get("max", 0))
                        for x in self.nodes.get(n, {}).get("node", ()) if x.get("r") == "stats")
            self.rtts[n] = ([a for a, _ in xs], [b for _, b in xs])
        ts, rs = self.rtts[n]
        i = bisect.bisect_left(ts, t)
        d = max(DELTA, rs[i] if i < len(rs) else 0)
        self.loose += d > DELTA_LOOSE
        return d

    # ---- clients and journals

    def stepped(self, node, xs):
        """xs' times on the store's clock. A second the clock stepped back over came twice and has no bracket: in
        write order, a line in it before the step is on the offset the clock has once past the top it stepped back
        from, plus the step, and one after the step on that offset (pairs-20261001-2342-s1002, episodes 239 and 262).
        Only under a clock fault: a line is stamped before the log's lock, so a starved thread's comes late without
        one."""
        steps, top = [], -math.inf
        for j, x in enumerate(xs):
            if x["t"] < top - TIGHT and self.fault_on(node, CLOCK, self.to_store(node, x["t"]) - S,
                                                       self.to_store(node, top) + S):
                br = self.brackets.get(node, {}).get(top // S + 1)
                steps.append((j, top, x["t"], br and (br[0] + br[1]) // 2))
                top = x["t"]
            top = max(top, x["t"])
        out, k = [], 0
        for j, x in enumerate(xs):
            while k < len(steps) and steps[k][0] <= j:
                k += 1
            off = None
            if k and x["t"] <= steps[k - 1][1]:
                off = steps[k - 1][3]
            elif k < len(steps) and x["t"] >= steps[k][2] and steps[k][3] is not None:
                off = steps[k][3] + steps[k][1] - steps[k][2]
            out.append(self.to_store(node, x["t"]) if off is None else x["t"] - off)
        return out

    def check_clients(self):
        self.sends, self.acks, self.gives = {}, {}, {}
        self.inc_last = {}
        for node, nd in self.nodes.items():
            xs = [x for x in self.read(os.path.join(nd["dir"], "client.jsonl")) if x.get("op") is not None]
            for x, t in zip(xs, self.stepped(node, xs)):
                ev, op = x.get("r"), x["op"]
                self.ts[node].append(t)
                inc = op.rpartition(".")[0]
                if self.inc_last.get(inc, -1) < t:
                    self.inc_last[inc] = t
                if ev == "send":
                    self.sends.setdefault(op, (node, t, x["k"], x.get("d")))
                elif ev == "ack":
                    self.acks.setdefault(op, (t, x["o"], x["a"], x.get("n"), x.get("ms"), x.get("e")))
                elif ev == "give_up":
                    self.gives.setdefault(op, (t, str(x.get("err"))))
        for op, (t, err) in self.gives.items():  # an op unanswered for 120 s: no fault in a plan lasts that long
            self.flag("give-up", t, op=op, err=err)
        self.judged["give-up"] = self.judged["ack-lost"] = self.judged["ack-mismatch"] = len(self.acks)
        for op, (t, o, a, n, _, e) in self.acks.items():
            r = self.rec.get(op)
            if r is None:
                self.flag("ack-lost", t, op=op, n=n)
            elif [r["o"], r["a"], r["n"], r["e"]] != [o, a, n, e]:  # n and e: the writer, also for a retry from `done`
                self.flag("ack-mismatch", r["t"], k=r["k"], op=op, ack=[o, a, n, e],
                          record=[r["o"], r["a"], r["n"], r["e"]])
        for op, r in self.rec.items():
            s = self.sends.get(op)
            if s is None or s[2:] != (r["k"], r["d"]):  # a record no client sent, or not what it sent
                self.flag("phantom", r["t"], k=r["k"], op=op, n=r["n"], sent=s and list(s[2:]))
        journaled, jt, ups, jerr = {}, {}, {}, set()
        for node, nd in self.nodes.items():
            ops, ts = set(), []
            for p in nd["journals"]:
                for x in self.read(p):
                    if "t" not in x:
                        self.bad += 1  # whole stale lines a diskdrop left, which parse
                        continue
                    ts.append(self.to_store(node, x["t"]))
                    if x.get("v") == "ok" and "op" in x:
                        ops.add(x["op"])
                        self.journal_line(node, x, ts[-1])
            journaled[node], jt[node] = ops, sorted(ts)
            ups[node] = sorted(self.to_store(node, x["t"]) for x in nd["node"] if x.get("r") == "up")
            jerr.update((node, x.get("op")) for x in nd["node"] if x.get("r") == "journal_err")
        self.ups = ups
        for op in self.acks:
            r = self.rec.get(op)
            if r is None or op in journaled.get(r["n"], ()):
                self.judged["journal-missing"] += r is not None
                continue
            n, t = r["n"], r["t"]  # the writer journals the record; a retry may be answered elsewhere, from `done`
            drop = self.dropped(n, t)
            mkfs = self.formatted(n, t)
            if drop is not None:
                self.journal_exempt[("diskdrop", n, drop)] += 1
            elif (n, op) in jerr:
                self.journal_exempt[("journal_err", n, None)] += 1
            elif self.died_after(n, ups.get(n, ()), jt.get(n, ()), t, self.acks[op][0]) or any(
                    t <= done <= t + S and sent < self.acks[op][0] for sent, done in self.kills.get(n, ())):
                # The second: a kill the nemesis logged, on the store's clock, which a skew-restart's step moves the
                # writer's `up` and journal lines off.
                self.journal_exempt[("killed before fsync", n, None)] += 1
            elif mkfs is not None:
                self.journal_exempt[("mkfs", n, mkfs)] += 1
            else:
                self.judged["journal-missing"] += 1
                self.flag("journal-missing", t, k=r["k"], op=op, n=n)

    def journal_line(self, node, x, t):
        """journal-divergent: a journal line says what the store's record of its op says, unless a lying disk left it."""
        r = self.rec.get(x["op"])
        if r is None:
            return
        self.judged["journal-divergent"] += 1
        bad = {f: [x[f], r[f]] for f in JOURNAL_FIELDS if f in x and x[f] != r[f]}
        if bad and self.dropped(node, t) is None:
            self.flag("journal-divergent", r["t"], probe=False, k=r["k"], op=x["op"], node=node, differ=bad)

    def died_after(self, n, ups, jts, t, acked):
        """The writer's process died within 1 s of the store's OK at t, and before the op was acked: it restarted
        within 60 s, with a restart fault, reset or heal on it, and its last journal line before then is before t + 1 s
        and before the ack. An ack is sent after the fsync, so one before that line came from a writer still alive to
        fsync: its line was lost, not unwritten. The ack of an op whose writer died first is a retry, answered from the
        record by the key's next owner. A fault of its journal disk at t (from the on sent: a dm table swap suspends
        the device first) holds the writer back for as long, and so does starving it, whose lines come seconds behind
        and out of order (the lock is not fair; pairs-20261001-2342-s1002, b3 at 3085 s): then only the ack bounds
        that line."""
        i = bisect.bisect_right(ups, t)
        if i == len(ups) or ups[i] > t + 60 * S or not (
                self.fault_on(n, RESTARTS, t - 5 * S, ups[i]) or any(t - 5 * S <= h <= ups[i] + S
                                                                     for h in self.healed.get(n, ()))):
            return False
        j = bisect.bisect_left(jts, ups[i]) - 1
        held = any(disk_of(f) == "journal" for f in self.fault_on(n, IO, t, t + S)) or \
            self.fault_on(n, STARVES, t, t + S)
        return j < 0 or jts[j] < (acked if held else min(t + S, acked))

    def dropped(self, n, t):
        """The start of the diskdrop whose window, from its on until the reset at its off (or its off's end where none
        was logged, which nemesis flags), holds t."""
        for w in self.windows.get(n, ()):
            if w["class"] == "diskdrop" and disk_of(w) == "journal" and w["on"][0] <= t <= min(
                    [rt for rt, *_ in self.resets.get(n, ()) if w["off"][0] <= rt <= w["off"][1] + 5 * S]
                    or [w["off"][1]]):
                return w["on"][0]
        return None

    def formatted(self, n, t):
        """The mkfs after t that lost the journal line of a record at t: one whose archive of the old journal is missing
        or empty, or which formatted the disk of a fault held within 60 s of t (the archive is mounted with noload,
        without the ext4 journal's commits not yet written back, which dirty_expire's 30 s bounds), but not of a
        powercut, which a journaled file system survives. Its time, or None."""
        for rt, how, _, detail in self.resets.get(n, ()):
            if how != "mkfs" or rt < t:
                continue
            a = os.path.join(self.nodes[n]["dir"], "journal-archive", os.path.basename((detail.split() or [""])[-1]))
            if not (os.path.isdir(a) and os.listdir(a)) or any(
                    disk_of(w) == "journal" for w in self.fault_on(n, IO - {"powercut"}, t, t + 60 * S)):
                return rt
        return None

    # ---- liveness

    def check_liveness(self):
        B = self.B
        self.by_start = sorted((t, op) for op, (_, t, _, _) in self.sends.items())
        for op, (node, st, k, _) in self.sends.items():
            a = self.acks.get(op)
            at = a[0] if a else None
            deadline, cause = None, None
            cl = self.clusters  # overlapping episodes as one: liveness from the end of the last of them
            j = bisect.bisect_left(self.ends, st)
            # Caught in flight by the next episode, begun before it was due: due B after that one.
            while j + 1 < len(cl) and self.starts[j + 1] < self.ends[j] + B and (at is None or at > self.starts[j + 1]):
                j += 1
            if j < len(cl) and (at is None or at > self.ends[j]):
                deadline, cause = self.ends[j] + B, cl[j]
            i = bisect.bisect_right(self.ends, st) - 1
            nxt = self.starts[i + 1] if i + 1 < len(cl) else None
            # Started after episode i: due by st + B, unless the next episode began first and caught it in flight.
            if i >= 0 and (nxt is None or st < nxt and (st + B <= nxt or at is not None and at <= nxt)) and (
                    deadline is None or st + B < deadline):
                deadline, cause = st + B, cl[i]
            # A clock fault moves the caller's lines off the timeline, unless the clock map placed both of them.
            if deadline is None or self.fault_on(node, CLOCK, st, deadline) and not (
                    at is not None and {st // S, at // S} <= self.tight.get(node, set())):
                continue
            self.late("liveness-after", op, node, k, st, at, deadline, cause)

    def late(self, inv, op, node, k, st, at, deadline, cause):
        if at is None and self.inc_last.get(op.rpartition(".")[0], 0) < deadline:
            return  # its client died, or the run ended, before the deadline
        self.judged[inv] += 1
        if at is not None and at <= deadline + self.slop.get(node, 0):
            return
        more = {"episodes": cause["is"]} if len(cause.get("is", ())) > 1 else {}
        self.flag(inv, deadline, probe=cause["probe"], op=op, k=k, node=node, sent=st, acked=at, episode=cause["i"],
                  late_ms=None if at is None else round((at - deadline) / MS), **more)

    # ---- node stats and quiet windows

    def check_nodes(self):
        self.stats = collections.defaultdict(list)  # node -> [(t, inc, line)]
        phases = self.meta.get("phases") or []
        ups = []
        for node, nd in self.nodes.items():
            inc = 0
            for x in nd["node"]:
                ev = x.get("r")
                x["_t"] = self.to_store(node, x["t"])
                self.ts[node].append(x["_t"])
                if ev == "up":
                    inc += 1
                    ups.append(x["_t"])
                x["_inc"] = inc
                if ev == "stats":
                    self.stats[node].append(x)
        if not phases:
            first = min(ups) if ups else (self.store[0]["t"] if self.store else 0)
            phases = [{"start": first, "end": self.end}]
        self.phases = phases
        self.t0 = phases[0]["start"]
        busy = []  # the episodes' spans, merged: quiet() runs per probe, quota and stats line, a bisect each
        for s, e in sorted((e["start"], e["end"] + self.quiet_after) for e in self.eps):
            if busy and s <= busy[-1][1]:
                busy[-1][1] = max(busy[-1][1], e)
            else:
                busy.append([s, e])
        busy_s = [s for s, _ in busy]

        def calm(a, b):  # no episode, nor its quiet_after, between a and b
            i = bisect.bisect_right(busy_s, b) - 1
            return not (i >= 0 and a <= busy[i][1])

        def quiet(a, b):
            return any(p["start"] + self.warmup <= a and b <= p["end"] for p in phases) and calm(a, b)
        self.quiet = quiet

        wcache = {}

        # cached per window: a cache per second let a second a window boundary splits take the earlier window
        def qwin(t):
            for p in phases:
                if p["start"] <= t <= p["end"]:
                    w = p["start"] + (t - p["start"]) // WINDOW * WINDOW
                    if w not in wcache:
                        wcache[w] = quiet(w, w + WINDOW)
                    return w if wcache[w] else None
            return None

        for node, nd in self.nodes.items():
            live, stops, live_at = 0, collections.Counter(), {}
            for x in nd["node"]:
                ev = x.get("r")
                if ev == "up":
                    live = 0
                elif ev == "start":
                    live += 1
                elif ev == "stop":
                    w = qwin(x["_t"]) if x.get("why") != "Drained" else None
                    if w is not None:
                        live_at.setdefault(w, live)
                        stops[w] += 1
                    live -= 1
                elif ev == "load":
                    w = qwin(x["_t"])
                    if w is not None and x.get("p50", 0) > 1000:
                        self.flag("latency-quiet", w, node=node, p50_ms=x["p50"], window=w)
            for w, c in stops.items():
                if c > max(10, 0.01 * live_at[w]):
                    self.flag("mass-drop", w, node=node, stops=c, actors=live_at[w], window=w)
            st = self.stats[node]
            for a, b in zip(st, st[1:]):
                if a["_inc"] == b["_inc"] and b.get("leases_lost", 0) > a.get("leases_lost", 0) and quiet(a["_t"], b["_t"]):
                    self.flag("lease-lost-quiet", b["_t"], node=node, leases_lost=[a["leases_lost"], b["leases_lost"]])
        lat = collections.defaultdict(list)
        for op, (t, _, _, _, ms, _) in self.acks.items():
            s = self.sends.get(op)
            # Judged by its send's window, unless it was still in flight when an episode began: under geo an op takes
            # 200 ms, and those sent just before a cut waited it out (scenarios-20261027-0626-s1, 11 of 1050 ops).
            if s is not None and ms is not None:
                w = qwin(s[1])
                if w is not None and calm(s[1], max(t, s[1])):
                    lat[(s[0], w)].append(ms)
        for (node, w), xs in sorted(lat.items()):
            p50 = pct(xs, 0.5)
            if p50 > 1000:
                self.flag("latency-quiet", w, node=node, p50_ms=p50, ops=len(xs), window=w)
            elif len(xs) >= 20 and pct(xs, .99) > QUIET_P99 / MS:
                self.flag("latency-quiet", w, node=node, p99_ms=pct(xs, .99), ops=len(xs), window=w)
        self.quiet_windows = sorted(w for w, q in wcache.items() if q)
        self.judged["mass-drop"] = self.judged["latency-quiet"] = len(self.quiet_windows)
        self.judged["lease-lost-quiet"] = sum(quiet(a["_t"], b["_t"]) for st in self.stats.values()
                                              for a, b in zip(st, st[1:]))
        for e in self.eps:
            if set(e["classes"]) <= IO and self.alone(e):
                self.judged["io-lease-lost"] += 1
                for f in e["faults"]:
                    for n in f["nodes"]:
                        self.io_lease(n, e)
            if "skew-restart" in e["classes"]:
                lo, hi = e["start"], e["end"] + self.B
                stale_o = [x for x in self.store if x["r"] == "O" and x["v"] == "stale" and lo <= x["t"] <= hi]
                behind = any(float(x.get("ownership", {}).get("clock behind", 0)) > 0
                             for n in self.acceptors for x in self.stats.get(n, ()) if lo <= x["_t"] <= hi + WINDOW)
                if stale_o and not behind:
                    self.flag("clock-behind", stale_o[0]["t"], probe=True, k=stale_o[0]["k"], episode=e["i"])

    def io_lease(self, n, e):
        st = self.stats.get(n, [])
        base = None
        c = e["cluster"] + 1  # a line after the next episode began counts what that one did too (pairs s49, a cascade)
        end = min(e["end"] + WINDOW, self.starts[c] if c < len(self.starts) else math.inf)
        for x in st:
            if x["_t"] <= e["start"]:
                base = x
            elif x["_t"] <= end and base is not None and x["_inc"] == base["_inc"]:
                for c in ("leases_lost", "isolations"):
                    if x.get(c, 0) > base.get(c, 0):
                        self.flag("io-lease-lost", x["_t"], node=n, episode=e["i"], counter=c,
                                  moved=[base[c], x[c]])
                        return

    # ---- streams

    def check_streams(self):
        """Each follower judges its own records and logs what breaks (src/streams.rs): those fail the run. Its
        silences, from `resume` lines and the `quiet` of its aggregate lines, fail `stream-liveness` when one outlasts B
        after an episode's end, or the warmup's before the first, and before the next episode."""
        self.stream_totals = collections.Counter()
        silences = collections.defaultdict(list)  # (follower node, stream) -> [(from, until)], store time
        self.stream_lines = collections.defaultdict(list)  # node -> the times of its aggregate lines, for heartbeat
        for node in self.nodes:
            gens = {}
            for x in self.lane(node, "stream.jsonl"):
                ev, t = x.get("r"), x["_t"]
                self.ts[node].append(t)
                if ev in ("dup", "reorder", "gap") and not self.lied(x.get("s", ""), t, t):
                    self.flag("stream-" + ev, t, probe=False, node=node,
                              **{f: x[f] for f in ("s", "g", "seq", "last", "start", "bad") if f in x})
                elif ev == "rewritten" and not self.lied(x["s"], *self.rewound(x, t)):  # per origin restart in check_disk
                    self.flag("disk-durable", t, node=node, **{f: x.get(f) for f in ("s", "g", "seq", "inc", "open",
                                                                                     "max")})
                elif ev == "resume":
                    silences[(node, x["s"])].append((t - x["quiet_ms"] * MS, t))
                elif ev == "streams":
                    self.stream_lines[node].append(t)
                    for f in ("recv", "gaps", "dups", "reorders", "resets", "origin_lost", "afters"):
                        self.stream_totals[f] += x.get(f, 0)
                    self.stream_totals["max_lag_ms"] = max(self.stream_totals["max_lag_ms"], x.get("max_lag_ms", 0))
                    for s, q in x.get("quiet", {}).items():
                        silences[(node, s)].append((t - q * MS, t))
                elif ev == "open":  # a generation only goes up: a follower after a cursor of the last would be wrong
                    self.judged["stream-generation"] += 1
                    if gens.get(x["s"], 0) >= x["g"]:
                        self.flag("stream-generation", t, probe=False, node=node, s=x["s"], g=x["g"], last=gens[x["s"]])
                    gens[x["s"]] = x["g"]
                elif ev == "publish_err":  # none clears by waiting (src/streams.rs)
                    self.flag("stream-publish-err", t, node=node, s=x.get("s"), err=x.get("err"))
                if ev in ("reset", "subscribe_err", "origin_lost"):
                    self.stream_end(node, t, x)
        self.silences = silences
        for inv in ("stream-dup", "stream-reorder", "stream-gap", "stream-publish-err", "stream-reset-kind",
                    "stream-quiet"):
            self.judged[inv] = self.stream_totals["recv"]
        # ponytail: an origin is up when it logged a stats line (every 10 s) in the silence or a window after it.
        up = lambda n, lo, hi: any(lo <= x["_t"] <= hi for x in self.stats.get(n, ()))
        # The steady time before the first episode is judged too, from the warmup's end.
        steady = {"i": None, "end": self.t0 + self.warmup, "probe": False}
        for i, e in enumerate([steady] + self.clusters):
            nxt = self.starts[i] if i < len(self.starts) else self.end
            flagged = set()
            for (node, s), xs in sorted(silences.items()):
                for a, b in xs:
                    due = max(a, e["end"]) + self.B
                    # A follower's clock fault moves its lines off the timeline, as for the keys' liveness. A follower
                    # ahead of an origin whose disk lost synced records waits, silent, until it publishes past them;
                    # one whose disk refuses every publish publishes nothing.
                    if due < min(b, nxt) and (node, s) not in flagged and up(origin_of(s), due - self.B, b + WINDOW) \
                            and not self.fault_on(node, CLOCK, a - WINDOW, b) and not self.lied(s, a, b) \
                            and not self.refusing(s, a, b):
                        flagged.add((node, s))
                        self.flag("stream-liveness", due, probe=e["probe"], node=node, s=s, silent_from=a, heard=b,
                                  episode=e["i"])
        self.judged["stream-liveness"] = sum(map(len, silences.values()))

    def stream_end(self, node, t, x):
        """A subscription that ended, reset or failed: never in a quiet window, never for a kind no fault explains, and
        slow only while the follower, the origin or its relay (the next node) is starved."""
        ids = sorted(self.nodes, key=lambda n: int(n[1:]) if n[1:].isdigit() else 0)
        s, why = x.get("s", ""), str(x.get("why") or x.get("err") or "")
        origin = origin_of(s)
        relay = ids[(ids.index(origin) + 1) % len(ids)] if origin in ids else origin
        kind = why.split("(")[0].split(" ")[0].split("{")[0]
        # A follower ahead of what the disk lost, or of an origin that cannot publish, is refused.
        if self.quiet(t, t) and not self.lied(s, t, t) and not self.refusing(s, t - WINDOW, t + WINDOW):
            self.flag("stream-quiet", t, node=node, s=s, r=x.get("r"), why=why)
        if (x.get("r") == "reset" and kind in STREAM_BAD_RESET or x.get("r") == "subscribe_err" and why.startswith(
                STREAM_BAD_SUBSCRIBE)) and not self.lied(s, t, t):
            self.flag("stream-reset-kind", t, node=node, s=s, r=x.get("r"), why=why)
        # Or the follower itself just started again: it takes back its keys and replays its followers at once.
        elif x.get("r") == "reset" and kind in STREAM_SLOW_RESET and not any(
                self.fault_on(n, STREAM_STARVED, t - self.B, t) for n in {node, origin, relay}) \
                and not self.fault_on(node, RESTARTS, t - self.B, t):
            self.flag("stream-reset-kind", t, node=node, s=s, r="reset", why=why, path=sorted({node, origin, relay}))

    def stream_report(self):
        if not self.stream_totals:
            return []
        out = ["streams: %s" % " ".join("%s %d" % kv for kv in sorted(self.stream_totals.items())),
               "stream recovery after episodes, slowest follower's time from the end to its next record (s): p50 p95 max"]
        rec = collections.defaultdict(list)
        for e in self.eps:
            gaps = [b - e["end"] for xs in self.silences.values() for a, b in xs if a <= e["end"] < b]
            for c in e["classes"]:
                rec[c].append(max(gaps + [0]) / S)
        for c, xs in sorted(rec.items()):
            out.append("  %-14s %6.2f %6.2f %6.2f  (%d)" % (c, pct(xs, .5), pct(xs, .95), max(xs), len(xs)))
        return out

    # ---- the disk workload

    def rewound(self, x, t):
        """Where a follower's `rewritten` line's loss lies: from the origin's open of the incarnation its highest record
        came from (`was`) to the open of the one that went back. A follower ahead of the loss hears nothing until the
        origin publishes past it, minutes later (pairs s49: 162 s after a diskcorrupt)."""
        o = origin_of(x["s"])
        if "was" in x and o in self.nodes:
            opens = {y["inc"]: y["_t"] for y in self.lane(o, "disk.jsonl")
                     if y.get("r") == "open" and y.get("s") == x["s"]}
            if x["was"] in opens and x["inc"] in opens:
                return opens[x["was"]], opens[x["inc"]]
        return t, t

    def lied(self, s, a, b):
        """A disk stream whose origin's streams disk lost or changed what it had synced (LYING) by b: what its
        followers and its next start see in [a, b] is the fault's until the origin, at its rate over the run, publishes
        past where it was at the fault's off, and for 120 s at least."""
        if not s.startswith("disk-"):
            return False
        o = origin_of(s)
        opens = [(y["_t"], y["at"]) for y in self.lane(o, "disk.jsonl") if y.get("r") == "open" and y.get("s") == s]
        rate = (opens[-1][1] - opens[0][1]) / max(opens[-1][0] - opens[0][0], 1) if opens else 0
        for f in self.fault_on(o, LYING, -math.inf, b):
            if disk_of(f) != "streams":
                continue
            reach = f["off"][1] + 120 * S
            i = bisect.bisect_right(opens, (f["on"][0], math.inf))  # the first open after the fault began
            if 0 < i < len(opens) and rate > 0:
                (tp, ap), (tn, an) = opens[i - 1], opens[i]
                reach = max(reach, tn + (ap + rate * (f["off"][1] - tp) - an) / rate)
            if reach >= a:
                return True
        return False

    def refusing(self, s, a, b):
        """A disk stream whose origin's disk refused every publish in [a, b]: each of its disk lines then failing."""
        if not s.startswith("disk-"):
            return False
        xs = [y for y in self.lane(origin_of(s), "disk.jsonl") if y.get("r") == "disk" and a <= y["_t"] <= b]
        return bool(xs) and all(y.get("failing") for y in xs)

    def check_disk(self):
        """The disk workload (src/disk.rs), from each node's disk.jsonl and its followers' `rewritten` lines.
        disk-durable: no follower had a record its origin's restart went back over. disk-resume: an origin opens each
        stream again in the generation its disk holds, after the records it opened after the time before, unless a
        reset formatted the disk between (not within 60 s of a powercut of it: ext4 survives a power cut), and takes
        each publish one past the last. Neither holds for a disk that lied (`lied`). disk-error: a refused publish is
        refused by the disk; Full or Failed only under a fault of the streams disk, Behind under any fault or stop of
        the node, and each cleared by a publish taken or a start within B of the fault's off, or of a stop of the node
        or another fault of its disk that began by then. disk-retention: the disk's bytes stay within what retention
        bounds them to."""
        self.disk = {}
        mkfs = collections.defaultdict(list)  # node -> when a reset formatted its streams disk
        for x in self.nem:
            if x.get("ev") == "disk-reset" and x.get("disk") == "streams" and x.get("how") == "mkfs" and not \
                    any(disk_of(f) == "streams" for f in self.fault_on(x["node"], {"powercut"}, x["t"] - 60 * S, x["t"])):
                mkfs[x["node"]].append(x["t"])
        disk = set(ran_workload(self, "disk"))
        for node in self.nodes:
            lines = self.disk[node] = self.lane(node, "disk.jsonl")
            self.ts[node] += [x["_t"] for x in lines]
            if node in disk and not any(
                    x.get("r") == "open" for x in lines):  # a disk workload that never opened judges nothing
                self.flag("log-complete", self.t0, probe=False, node=node, what="no open in disk.jsonl")
            opens = collections.defaultdict(list)
            for x in lines:
                if x.get("r") == "open":
                    opens[x["s"]].append(x)
                elif x.get("r") == "disk":
                    self.judged["disk-retention"] += 1
                    if x.get("bytes", 0) > x.get("cap", math.inf):
                        self.flag("disk-retention", x["_t"], node=node, bytes=x["bytes"], cap=x["cap"])
                elif x.get("r") == "publish_err":
                    self.disk_error(node, lines, x)
                elif x.get("r") == "publish_seq":  # a publish taken at another position than one past the last
                    self.flag("disk-resume", x["_t"], node=node, s=x.get("s"), want=x.get("want"), got=x.get("got"))
            for s, xs in sorted(opens.items()):
                for a, b in zip(xs, xs[1:]):
                    self.judged["disk-resume"] += 1
                    self.judged["disk-durable"] += 1
                    if (b.get("stored") is None or b["g"] != a["g"] or b["at"] < a["at"]) and not self.lied(
                            s, a["_t"], b["_t"]) and not any(a["_t"] <= t <= b["_t"] for t in mkfs[node]):
                        self.flag("disk-resume", b["_t"], node=node, s=s, before=[a["g"], a["at"]],
                                  stored=b.get("stored"), g=b["g"], at=b["at"])

    def disk_error(self, node, lines, x):
        t, err, s = x["_t"], str(x.get("err")), x.get("s")
        self.judged["disk-error"] += 1
        kind = err[5:].split(")")[0] if err.startswith("Disk(") else None
        if kind in ("Full", "Failed"):
            by = [f for f in self.fault_on(node, IO, t - 5 * S, t) if disk_of(f) == "streams"]
            if kind == "Failed":  # a segment it could not open for want of a descriptor
                by += self.fault_on(node, {"nofile"}, t - 5 * S, t)
        elif kind == "Behind":
            by = self.fault_on(node, nemesis.TAGS, t - self.B, t)
        else:
            by = []
        if not by and not (kind == "Behind" and self.exempt(node, t - self.B, t)):
            self.flag("disk-error", t, node=node, s=s, err=err, what="no fault of its disk explains it")
            return
        due = max([f["off"][1] for f in by] + [t]) + self.B
        for f in sorted(self.fault_on(node, HALTS | IO, t, math.inf), key=lambda f: f["on"][0]):
            if f["on"][0] <= due and (f["class"] in HALTS or disk_of(f) == "streams"):  # held down, or its disk again
                due = max(due, f["off"][1] + self.B)
        cleared = next((y["_t"] for y in lines if y["_t"] > t and y.get("s") == s and y.get("r") in ("publish_ok", "open")),
                       None)
        if (cleared if cleared is not None else self.end) > due:
            self.flag("disk-error", t, node=node, s=s, err=err, what="not cleared", due=due, cleared=cleared)

    def check_growth(self):
        """Per node and incarnation, from its `res` lines after GROWTH_WARMUP: each quarter's p50. A resource grows when
        the last quarter's is above the first's by GROWTH_RATIO of it plus its slack, and still rising: the last quarter
        a sixth of that above the third, where a leak's is a third and a warmup's or a filled cache's is none. Threads
        grow when a quarter's is above the node's fewest by more than tokio's blocking pool can hold."""
        self.growth, self.unjudged = [], []
        for node in sorted(self.nodes):
            incs, judged = collections.defaultdict(list), 0
            for x in self.nodes[node]["node"]:
                if x.get("r") == "res":
                    incs[x["_inc"]].append(x)
            # A value the node could not read is null (src/resources.rs): no sample, never a zero.
            ceiling = min((x["threads"] for xs in incs.values() for x in xs if x.get("threads") is not None),
                          default=0) + BLOCKING_CAP + THREADS_SLACK
            for inc, xs in sorted(incs.items()):
                a, b = xs[0]["_t"] + GROWTH_WARMUP, xs[-1]["_t"]
                if b <= a:
                    continue
                judge = b - a >= GROWTH_SPAN
                judged += judge
                qs = [[], [], [], []]
                for x in xs:
                    if x["_t"] >= a:
                        qs[min(3, int(4 * (x["_t"] - a) / (b - a)))].append(x)
                out = []
                for f in list(GROWTH_SLACK) + ["threads"] + list(GROWTH_SHOWN):
                    p = [pct([x[f] for x in q if x.get(f) is not None], .5) for q in qs]
                    if None in p:
                        continue
                    grow = p[3] - p[0]
                    if f == "threads":
                        limit, bad = ceiling, max(p) > ceiling
                        shown = "threads %d to %d (ceiling %d)" % (p[0], p[3], ceiling)
                    elif f in GROWTH_SLACK:
                        limit = GROWTH_RATIO * p[0] + GROWTH_SLACK[f]
                        bad = judge and grow > limit and p[3] - p[2] > grow / 6
                        shown = "%s %d to %d (%+d, limit %d)" % (f, p[0], p[3], grow, limit)
                    else:
                        out.append("%s %d to %d" % (f, p[0], p[3]))
                        continue
                    if bad:
                        self.flag("growth", b, probe=False, episode=None, node=node, inc=inc, resource=f, p50=p,
                                  limit=limit)
                    out.append(shown + (" GROWING" if bad else ""))
                self.growth.append("  %s inc %d, %.0f min %s: %s" % (
                    node, inc, (b - a) / 60 / S, "judged" if judge else "shown", ", ".join(out)))
            self.judged["growth"] += judged
            if not judged:
                self.unjudged.append(node)

    # ---- the process and its logs

    def restart_near(self, node, t):
        """A restart fault on the node, or a heal or reset of it, explains a start at t."""
        return bool(self.fault_on(node, RESTARTS, t - 60 * S, t + 30 * S)) or any(
            t - 60 * S <= h <= t + HEAL_LAG for h in self.healed.get(node, ()))

    def check_restarts(self):
        """unexpected-restart: an `up` no phase start, restart fault, reset or heal explains, and in the unit's log a
        panic, a crash's exit, or a SIGKILL outside a kill, oom or reset. shutdown-bounded: a `down` with an error, an
        actor still running or slower than systemd waits; a stop that timed out; a nonzero exit."""
        for node, nd in self.nodes.items():
            ups = sorted(x["_t"] for x in nd["node"] if x.get("r") == "up")
            first, lo = set(), -math.inf
            for p in self.phases:  # each phase starts every node once
                first.update(t for t in ups[bisect.bisect_left(ups, lo):][:1])
                lo = p.get("end", self.end)
            for t in ups:
                self.judged["unexpected-restart"] += 1
                if t not in first and not self.restart_near(node, t):
                    self.flag("unexpected-restart", t, node=node, what="up")
            for x in nd["node"]:
                if x.get("r") == "down":
                    self.judged["shutdown-bounded"] += 1
                    if x.get("err") or x.get("still_running") or x.get("ms", 0) > DOWN_BOUND:
                        self.flag("shutdown-bounded", x["_t"], node=node,
                                  **{f: x.get(f) for f in ("ms", "err", "reason", "still_running")})
            timed_out = -math.inf
            for t0, msg in nd["unit"] or ():
                t = self.to_store(node, t0)
                if ups and t < ups[0] - S:  # the unit's lines of an earlier run
                    continue
                if "timed out" in msg or "result 'timeout'" in msg:
                    timed_out = t
                    self.flag("shutdown-bounded", t, node=node, msg=msg)
                elif ("hooks.rs" in msg and self.fault_on(node, {"hook"}, t - 5 * S, t + 5 * S)) or (
                        "code=dumped" in msg and self.fault_on(node, {"coredump"}, t - 5 * S, t + 5 * S)):
                    continue  # a panic a hook asked for (panic-contained judges it), a core a coredump asked for
                elif "panicked at" in msg or "status=101" in msg or "code=dumped" in msg:
                    self.flag("unexpected-restart", t, node=node, msg=msg)
                elif "code=killed" in msg and t > timed_out + 60 * S and not (
                        self.fault_on(node, RESTARTS, t - 5 * S, t + S) or any(  # a drifting clock maps it early
                            t - S <= h <= t + HEAL_LAG for h in self.healed.get(node, ()))):
                    self.flag("unexpected-restart", t, node=node, msg=msg)
                elif "code=exited" in msg and "status=0/" not in msg and not self.fault_on(node, IO, t - 5 * S, t):
                    self.flag("shutdown-bounded", t, node=node, msg=msg)

    def check_logs(self):
        """log-complete: each node's files, and per incarnation as many ack and give_up lines as its counters say.
        heartbeat: stats and streams lines at most HEARTBEAT apart, and each workload's tick moving between two stats
        lines, unless the node was stopped, starved or restarted then. client-task: each key's client calls it at least
        every 1.5 EVERY_MS + GIVE_UP."""
        for n in self.meta.get("nodes") or ():
            if n not in self.nodes:
                self.flag("log-complete", self.t0, probe=False, node=n, what="no logs")
        acked = collections.Counter(op.rpartition(".")[0] for op in self.acks)
        gave = collections.Counter(op.rpartition(".")[0] for op in self.gives)
        incs_all = {}  # (node, boot) -> (start, end, whether its clients call keys)
        for node, nd in self.nodes.items():
            self.judged["log-complete"] += 1
            if nd["unit"] is None:
                self.flag("log-complete", self.t0, probe=False, node=node, what="no unit.jsonl")
            incs = []
            for x in nd["node"]:
                if x.get("r") == "up":
                    incs.append((x, []))
                elif incs:
                    incs[-1][1].append(x)
            for j, (up, xs) in enumerate(incs):
                b, st = up.get("inc"), [x for x in xs if x.get("r") == "stats"]
                down = [x for x in xs if x.get("r") == "down"][:1]
                end = incs[j + 1][0]["_t"] if j + 1 < len(incs) else next(
                    (p["end"] for p in self.phases if p["start"] - 90 * S <= up["_t"] <= p.get("end", self.end)
                     and "end" in p), self.end)
                serve = any(x.get("r") == "load" for x in xs)
                incs_all[(node, b)] = (up["_t"], down[0]["_t"] if down else end,
                                       not serve and "accounts" in up.get("workloads", ["accounts"]))
                if serve:  # no client lines: its counter alone says an op gave up
                    g = next((x for x in st if x.get("give_ups")), None)
                    if g is not None:
                        self.flag("give-up", g["_t"], node=node, inc=b, give_ups=g["give_ups"])
                elif st:
                    for f, have in (("acks", acked["%s.%s" % (node, b)]), ("give_ups", gave["%s.%s" % (node, b)])):
                        if st[-1].get(f, 0) - have > LINES_SLACK:
                            self.flag("log-complete", st[-1]["_t"], node=node, inc=b, what=f, counted=st[-1][f],
                                      lines=have)
                seq = st + down
                for a, c in zip(seq, seq[1:]):
                    self.judged["heartbeat"] += 1
                    ta, tc = a.get("ticks") or {}, c.get("ticks") or {}
                    # after a throttle the stats loop fires its missed ticks at once, lines a ms apart
                    stalled = sorted(w for w, v in tc.items() if v <= ta.get(w, -1)) if c["_t"] - a["_t"] > 5 * S else []
                    if (c["_t"] - a["_t"] > HEARTBEAT or stalled) and not self.exempt(node, a["_t"], c["_t"]):
                        self.flag("heartbeat", c["_t"], node=node, inc=b, gap_ms=(c["_t"] - a["_t"]) // MS,
                                  stalled=stalled)
            ups = [up["_t"] for up, _ in incs]
            sl = self.stream_lines.get(node, [])
            for a, c in zip(sl, sl[1:]):
                if bisect.bisect_right(ups, a) == bisect.bisect_right(ups, c):
                    self.judged["heartbeat"] += 1
                    if c - a > HEARTBEAT and not self.exempt(node, a, c):
                        self.flag("heartbeat", c, node=node, what="streams", gap_ms=(c - a) // MS)
        if not (self.every and self.keys):
            return
        sends = collections.defaultdict(lambda: collections.defaultdict(list))
        for op, (node, t, k, _) in self.sends.items():
            sends[op.rpartition(".")[0]][k].append(t)
        first, then = 2.5 * self.every + 10 * S, 1.5 * self.every + GIVE_UP + 5 * S
        for (node, b), (a, z, calls) in sorted(incs_all.items()):
            ks = sends.get("%s.%s" % (node, b), {})
            if not calls or not ks and z - a <= first + WINDOW:
                continue
            ext = 60 * S if self.exempt(node, a, z) else 0
            bad = []
            for i in range(self.keys):
                k = "k%d" % i
                self.judged["client-task"] += 1
                ts = [a] + sorted(ks.get(k, ())) + [z]
                for j in range(len(ts) - 1):
                    if ts[j + 1] - ts[j] > (first if j == 0 else then) + ext:
                        bad.append((ts[j] + (first if j == 0 else then) + ext, k, ts[j], ts[j + 1]))
                        break
            if bad:
                t, k, since, until = min(bad)
                self.flag("client-task", t, node=node, inc=b, k=k, called=since, next=until, keys=len(bad))

    # ---- the harness

    def check_nemesis(self):
        """The nemesis did what the plan says: each planned fault on and off with exit 0 and its arguments, each step
        within its bound, under the fault's id (<episode>.<index in it>) where the log has ids; every check clean
        without a forced heal, every clock within 50 ms of b1's; every planned class in the run's plan version, and on
        one box none it cannot run."""
        seen = {}
        ids = any("id" in x for x in self.nem if x.get("ev") in ("on", "off"))  # a run from before ids keys by class
        key_of = lambda e, j, f: "%s.%d" % (e["i"], j) if ids else (e["i"], f["class"], tuple(f["nodes"]))
        planned = {key_of(e, j, f): f for e in self.plan for j, f in enumerate(e["faults"])}
        t0 = min((x["t"] for x in self.nem if "t" in x), default=self.end)
        for x in self.nem:
            ev, t = x.get("ev"), x.get("t", self.end)
            if ev in ("on", "off"):
                key = fault_key(x)
                s = seen.setdefault(key, {"on": 0, "off": 0, "args": x.get("args") or {}})
                s[ev] += 1
                what = dict(i=x["i"], id=x.get("id"), cls=x["class"], nodes=x["nodes"], ev=ev, sub=x.get("sub"))
                took = x["done"] - x["sent"]
                if x.get("rc"):
                    self.flag("nemesis", t, probe=False, what="exit %s" % x["rc"], stderr=x.get("stderr"), **what)
                if took > SLOW_STEP.get((x.get("sub") or x["class"], ev), STEP_BOUND):
                    self.flag("nemesis", t, probe=False, what="slow step", took_ms=took // MS, **what)
                p = planned.get(key)
                role = p is not None and p["nodes"][0].startswith("@")  # it went on a node of `among`
                if p is None or p["class"] != x["class"] or (
                        not set(x["nodes"]) <= set(p.get("among", ())) if role else p["nodes"] != list(x["nodes"])):
                    self.flag("nemesis", t, probe=False, what="not in the plan", **what)
            elif ev in ("check", "clock") and x.get("forced") or ev == "heal" and x.get("rc"):
                self.flag("nemesis", t, probe=False, what="%s forced a heal" % ev if ev != "heal" else "heal failed",
                          i=x.get("i"), node=x.get("node"), out=x.get("out"), offset_ms=x.get("offset_ms"))
        v, one_box = int(self.meta.get("plan_version", 1)), bool(self.meta.get("one_box"))
        classes = dict(nemesis.PLANS.get(v, {}), **(nemesis.WAN if self.wan else {}))
        for e in self.plan:
            for j, f in enumerate(e["faults"]):
                self.judged["nemesis"] += 1
                s = seen.get(key_of(e, j, f))
                t = t0 + int(e.get("at", 0) * S)
                if f["class"] not in classes or one_box and f["class"] in nemesis.ONE_BOX_OUT:
                    self.flag("nemesis", t, probe=False, what="a class plan version %d%s has not" % (
                        v, " on one box" if one_box else ""), i=e["i"], cls=f["class"], nodes=f["nodes"])
                if not s or not s["on"] or not s["off"]:
                    self.flag("nemesis", t, probe=False, what="planned but not applied", i=e["i"], cls=f["class"],
                              nodes=f["nodes"], seen=s)
                elif s["args"] != (f.get("args") or {}):
                    self.flag("nemesis", t, probe=False, what="arguments not the plan's", i=e["i"], cls=f["class"],
                              args=s["args"], planned=f.get("args"))
        for e in self.eps:  # a disk that failed or lied is reset at its off, or the next writes fall on it
            for f in e["faults"]:
                for n in f["nodes"]:
                    if f["class"] in RESET_AT_OFF and not any(
                            f["off"][0] <= rt <= f["off"][1] + 5 * S for rt in self.disk_resets[(n, disk_of(f))]):
                        self.flag("nemesis", f["off"][1], probe=False, what="no reset", i=e["i"], cls=f["class"],
                                  node=n, disk=disk_of(f))

    def check_peer(self):
        """peer-slots: a peer episode alone cost the node no real session (no peer went unreachable while only the
        probe hit its ports). peer-impostor: the node turned away what it must: its auth failures rose across every two
        stats lines holding a wrong-secret attempt its handshake refused as auth, and no stale impostor received a
        frame the node sent."""
        self.peer = {n: self.lane(n, "peer.jsonl") for n in self.nodes}
        for n, xs in self.peer.items():
            for x in xs:
                if x.get("mode") == "stale":
                    self.judged["peer-impostor"] += 1
                    if x.get("outcome") == "connected" and x.get("frames", 0) > 0:
                        self.flag("peer-impostor", x["_t"], node=n, frames=x["frames"],
                                  what="the node sent a stale impostor a frame")
        auth = lambda x: ((x.get("net") or {}).get("handshake_failures") or [0])[0]
        # Only attempts the node's handshake answered with auth: a timeout (the node paused, frozen, starved) or a
        # reset is counted under another reason, or not at all.
        refused = lambda x: x.get("mode") == "secret" and x.get("err") == "auth"
        for e in self.eps:
            for f in e["faults"]:
                if f["class"] != "peer":
                    continue
                for node in set(f["nodes"]) - set(f.get("noop", ())):
                    for a, b in self.probe_pairs(node, f, refused):
                        self.judged["peer-impostor"] += 1
                        if auth(b) <= auth(a):
                            self.flag("peer-impostor", b["_t"], node=node, auth=auth(b),
                                      what="wrong-secret attempts and no auth failure counted")
                    if e["classes"] != ["peer"] or not self.alone(e) or self.exempt(node, f["on"][0], f["off"][1]):
                        continue
                    pairs = self.stat_pairs(node, f)
                    if not pairs:
                        continue
                    self.judged["peer-slots"] += 1
                    jump = next((b for a, b in pairs
                                 if b.get("became_unreachable", 0) > a.get("became_unreachable", 0)), None)
                    if jump is not None:
                        self.flag("peer-slots", jump["_t"], node=node, mode=f["args"].get("mode"),
                                  what="a real peer went unreachable while only the probe hit the node")

    def probe_pairs(self, node, f, which=lambda x: x.get("outcome") != "error"):
        """The node's stats pairs across the fault that hold a whole attempt of its probe (peer.jsonl, `started` to
        `t`, a second clear of the later line) for which `which` holds: by default, one that reached a listener. Only
        the fault's own mode: another peer fault on the node at once is judged by its own attempts."""
        tried = [(self.to_store(node, x.get("started", x["t"])), x["_t"])
                 for x in self.peer.get(node, ()) if x.get("mode") == f["args"].get("mode") and which(x)]
        return [(a, b) for a, b in self.stat_pairs(node, f) if any(a["_t"] < s and t < b["_t"] - S for s, t in tried)]

    def stat_pairs(self, node, f):
        """Consecutive stats lines of one incarnation of the node, from S before the fault's on to WINDOW after its off."""
        st = [x for x in self.stats.get(node, ()) if f["on"][0] - S <= x["_t"] <= f["off"][1] + WINDOW]
        return [(a, b) for a, b in zip(st, st[1:]) if a["_inc"] == b["_inc"]]

    def check_effects(self):
        """fault-effect: each fault left the trace its class leaves (EFFECT), or none is contradicted where its class
        has none to read. It reads only the nodes where the fault acted (not `noop`), and a disk fault only on the
        journal, unless its class names evidence per disk in EFFECT under (class, disk). A fault whose on was done under
        1 s before its off was sent held too briefly to leave one (pairs s49: a slowdisk on done 5.8 s late, 0.2 s
        before its off; s50: a reject of 0.9 s)."""
        for n in self.ts:
            self.ts[n].sort()
        for v in self.rec_ts.values():
            v.sort()
        self.effects = collections.Counter()
        for e in self.eps:
            for f in e["faults"]:
                acted = dict(f, nodes=[n for n in f["nodes"] if n not in f.get("noop", ())])
                held = f["off"][0] - f["on"][1] >= S
                got = [ev(self, acted) for ev in evidence(f)] if acted["nodes"] and held else []
                bad = [d for ok, d in got if ok is False]
                seen = "contradicted" if bad else "witnessed" if got and all(ok for ok, _ in got) else "unwitnessed"
                self.effects[(f["class"], seen)] += 1
                self.judged["fault-effect"] += seen == "witnessed"
                if bad:
                    self.flag("fault-effect", f["on"][0], probe=False, episode=e["i"], cls=f["class"],
                              nodes=f["nodes"], args=f["args"], evidence=bad)

    def wrote(self, n, a, b):
        ts = self.rec_ts.get(n, [])
        return bisect.bisect_left(ts, b) - bisect.bisect_left(ts, a)

    def check_void(self):
        """store-void, beside the store's own lines (check_store): an op acked with no record, sent and acked inside a
        silence of the store's log longer than 1 s (not its end), from a node whose clock no fault moved; anything but
        its first line in the store's output."""
        ts = sorted(x["t"] for x in self.store)
        gaps = set()
        for op, a in self.acks.items():
            s = self.sends.get(op)
            if s is None:
                continue
            self.judged["store-void"] += 1
            if op in self.rec or self.fault_on(s[0], CLOCK, s[1], a[0]):  # its record placed it: the clock map did not
                continue
            i = bisect.bisect_right(ts, s[1])
            prev, nxt = ts[i - 1] if i else -math.inf, ts[i] if i < len(ts) else math.inf
            if a[0] < nxt < math.inf and nxt - prev > S and prev not in gaps:
                gaps.add(prev)
                self.flag("store-void", s[1], probe=False, what="an op sent and acked in a silence of the store's log",
                          op=op, silent_from=prev, until=nxt)
        p = os.path.join(self.dir, "store.out")
        extra = [l.strip() for l in open(p, errors="replace")][1:] if os.path.exists(p) else []
        if [l for l in extra if l]:
            self.flag("store-void", self.end, probe=False, what="store.out", lines=extra[:5])

    # ---- beyond the store's lines

    @functools.cached_property
    def seeds(self):
        """The nodes serving seeds: b1..bSEEDS, or every node."""
        return {"b%d" % i for i in range(1, int(self.meta.get("seeds") or len(self.nodes)) + 1)}

    @functools.cached_property
    def own_ts(self):
        return {k: [o["t"] for o in xs] for k, xs in self.owns.items()}

    def owner(self, k, t):
        """The node the store last granted k to by t, or None."""
        i = bisect.bisect_right(self.own_ts.get(k, []), t)
        return self.owns[k][i - 1]["n"] if i else None

    def healthy(self, active, hol=False):
        """While `active` hold: the callers to judge, the nodes left impaired, the links cut, and the part holding a
        majority of the acceptors, whose nodes keep their leases however the cuts split it. A fault that stops a
        node's process takes it out, one that starves it takes it out and leaves it holding its leases, one that cuts
        links splits the rest. With `hol` (the blob workload's large frames on every session) a `rate` starves the
        node's sessions as memhigh starves its process: the lease messages wait behind the frames. The callers are the
        nodes of the part holding a majority of the acceptors and of the seeds, not impaired, that reach no node of it
        stranded: one that reaches too few acceptors itself to hold a lease, which such a caller may still place its
        keys on (a bridge's far side, seen from its middle)."""
        gone, impaired, cut = set(), set(), set()
        nodes = set(self.nodes) | set(self.acceptors)
        for f in active:
            ns = set(f["nodes"])
            if f["class"] in GONE:
                gone |= ns
            elif starves(f) or hol and f["class"] == "rate":
                gone |= ns
                impaired |= ns
            elif f["class"] in CUTS:
                cut |= {p for x in ns for y in far_side(f, nodes) for p in ((x, y), (y, x))}
            else:
                impaired |= ns
                if f["class"] in FRAYS or f["class"] == "portcut" and f["args"].get("port") == 7400:  # a cut as well
                    cut |= {p for x in ns for y in far_side(f, nodes) for p in ((x, y), (y, x))}
        left = nodes - gone
        while left:
            part, todo = set(), [min(left)]
            while todo:
                x = todo.pop()
                if x not in part:
                    part.add(x)
                    todo += [y for y in left - part if (x, y) not in cut]
            left -= part
            if 2 * len(part & set(self.acceptors)) > len(self.acceptors):
                if 2 * len(part & self.seeds) <= len(self.seeds):  # no majority of the seeds hears it: isolated
                    return set(), impaired, cut, part
                # A netem'd acceptor (hundreds of ms, lost packets) grants no lease of T in time: b5, cut from b3,
                # held none through b1 and b2 (chaos-20261001-1537-s62); so does one behind asym or mtu.
                slow = {x for f in active if f["class"] in SLOW for x in f["nodes"]}
                stranded = {x for x in part if 2 * sum((a == x or (x, a) not in cut) and a not in slow
                                                       for a in part & set(self.acceptors)) <= len(self.acceptors)}
                impaired |= stranded
                callers = {x for x in part - impaired if all((x, y) in cut for y in stranded)}
                return callers, impaired, cut, part
        return set(), impaired, cut, set()

    def check_during(self):
        """liveness-during: an op sent while faults hold, from one of healthy()'s callers, on a key they leave alone or
        whose owner they take away, is acked within the longest DURING of the faults on. Not judged: an op sent while a
        fault goes on or off, within B after one ended or a node was healed, or before one goes on within its bound;
        and one whose key an impaired node owns, or a node of the majority's part cut from the sender, which keeps its
        lease, or that such a node, or one back from a stop cut from the sender, wrote. A key owned outside that part,
        which cannot hold a lease, is judged: it must move."""
        if not self.acceptors:
            return
        fs = [(e, f) for e in self.eps for f in e["faults"]]
        ons = sorted(f["on"][0] for _, f in fs)
        ends = sorted([f["off"][0] for _, f in fs] + [h for hs in self.healed.values() for h in hs])
        pts = sorted({t for _, f in fs for t in f["on"] + f["off"]})
        hol = bool(ran_workload(self, "blob"))
        for a, b in zip(pts, pts[1:]):
            active = [(e, f) for e, f in fs if f["on"][1] <= a and b <= f["off"][0]]
            if not active or any(f["on"][0] < b and a < f["on"][1] or f["off"][0] < b and a < f["off"][1]
                                 for _, f in fs):
                continue
            callers, impaired, cut, part = self.healthy([f for _, f in active], hol)
            gone = {x for _, f in active if f["class"] in GONE for x in f["nodes"]}
            D = max(DURING.get(f["class"], lambda T: 3 * T + 5)(self.T) for _, f in active) * S
            cause = self.clusters[active[0][0]["cluster"]]
            for st, op in self.by_start[bisect.bisect_left(self.by_start, (a, "")):
                                        bisect.bisect_left(self.by_start, (b, ""))]:
                node, _, k, _ = self.sends[op]
                deadline = st + D
                i, j = bisect.bisect_left(ends, st - self.B), bisect.bisect_right(ons, st)
                if node not in callers or i < len(ends) and ends[i] <= st or (
                        j < len(ons) and ons[j] <= deadline):
                    continue
                ts = self.own_ts.get(k, [])
                lo, hi = max(0, bisect.bisect_right(ts, st) - 1), bisect.bisect_right(ts, deadline)
                w = self.rec.get(op, {}).get("n")  # where the caller's placement sent it, the store's owner or not
                if w in impaired or (node, w) in cut and (w in part or w in gone) or any(
                        o["n"] in impaired or (node, o["n"]) in cut and o["n"] in part
                        for o in self.owns.get(k, [])[lo:hi]):
                    continue
                ack = self.acks.get(op)
                self.late("liveness-during", op, node, k, st, ack and ack[0], deadline, cause)

    def check_actors(self):
        """zombie: an actor still running ZOMBIE after the store granted a newer epoch of its key to another node, or
        after its own start if later. turn-after-acquire: an accepted append whose turn began after an actor of a newer
        epoch of its key had started on another node, before the store saw that node's own: two actors of the key
        running turns at once, which the store's fence cannot see."""
        starts = self.actor_starts = collections.defaultdict(list)  # k -> [(store t, e, node)]
        for node, nd in self.nodes.items():
            for x in nd["node"]:
                if x.get("r") == "start" and "k" in x:
                    starts[x["k"]].append((x["_t"], x["e"], node))
        for xs in starts.values():
            xs.sort()
        for node, nd in self.nodes.items():
            live = {}  # k -> (e, t) of the actor of k running on the node
            for x in nd["node"]:
                r = x.get("r")
                if r in ("up", "down"):  # a new process, or the end of this one, ends every actor
                    for k, (e, t) in live.items():
                        self.zombie(node, k, e, t, x["_t"])
                    live = {}
                elif r == "start" and "k" in x:
                    if x["k"] in live:
                        self.zombie(node, x["k"], *live[x["k"]], x["_t"])
                    live[x["k"]] = (x["e"], x["_t"])
                elif r == "stop" and "k" in x and live.get(x["k"], (None,))[0] == x.get("e"):
                    self.zombie(node, x["k"], *live.pop(x["k"]), x["_t"])
            last = nd["node"][-1]["_t"] if nd["node"] else 0
            for k, (e, t) in live.items():  # still running when its log ends
                self.zombie(node, k, e, t, last, ended=False)
        top = {k: list(itertools.accumulate((e for _, e, _ in xs), max)) for k, xs in starts.items()}
        tts = {k: [t for t, _, _ in xs] for k, xs in starts.items()}
        for k, recs in self.acc.items():
            ts, tp = tts.get(k, []), top.get(k, [])
            for x in recs:
                self.judged["turn-after-acquire"] += 1
                j = bisect.bisect_left(ts, x["t"])
                if not j or tp[j - 1] <= x["e"]:  # no newer epoch started before the store took it
                    continue
                t2, e2, n2 = next((s for s in starts[k][:j] if s[1] > x["e"] and s[2] != x["n"]), (None,) * 3)
                if t2 is None:
                    continue
                n = x["n"]
                if self.fault_on(n, CLOCK, t2 - self.B, x["t"] + self.B) or self.fault_on(
                        n2, CLOCK, t2 - self.B, x["t"] + self.B):
                    continue  # a drifting or stepped clock places the start line no better than its mapping
                bound, slack = self.turn_began(n, x["t"], x.get("lag", 0))
                if bound <= t2 + slack + self.slop.get(n, 0) + self.slop.get(n2, 0):
                    continue
                w = self.resumed(n, bound, x["t"])
                if w:
                    self.explained[w] += 1
                    continue
                self.flag("turn-after-acquire", x["t"], k=k, e=x["e"], n=n, op=x["op"], lag_us=x.get("lag", 0),
                          turn_after_start_ms=round((bound - t2) / MS, 3), started=n2, started_e=e2, start_t=t2)

    def zombie(self, node, k, e, t0, end, ended=True):
        """The newer owner is the store's first grant of a newer epoch to another node, or that node's actor's start
        line where earlier, plus both clocks' slop: a start whose own never reached the store is a grant all the same
        (two actors of one key at once, neither of which the store fences)."""
        o = next(((o["t"], o["e"], o["n"]) for o in self.owns.get(k, ()) if o["e"] > e and o["n"] != node), None)
        s = next(((t + self.slop.get(n, 0) + self.slop.get(node, 0), e2, n) for t, e2, n in self.actor_starts.get(k, ())
                  if e2 > e and n != node and not self.fault_on(n, CLOCK, t - self.B, t + self.B)), None)
        o = min(filter(None, (o, s)), default=None)
        if o is None:
            return
        due = max(t0, o[0]) + ZOMBIE(self.T)
        for w in self.fault_on(node, ZOMBIE_LATE, t0, due):  # stopped, it stops its actors when it runs again
            due = max(due, w["off"][1] + ZOMBIE(self.T))
        if not ended and end < due or self.fault_on(node, CLOCK, t0, due):
            return
        self.judged["zombie"] += 1
        if end > due:
            self.flag("zombie", due, node=node, k=k, e=e, started=t0, newer_t=o[0], newer_e=o[1], newer_n=o[2],
                      ended=end if ended else None)

    def check_blast(self):
        """blast-radius: through a fault on one node alone, ops that neither start nor end on it and whose key it did
        not own keep p99 under BLAST_P99 and none past BLAST_MAX, from its on (or 3T + 5 s after, BLAST_LATER) to its
        off."""
        self.blast = collections.defaultdict(list)  # class -> (p99, max) of each episode judged, for the report
        for e in self.eps:
            if len(e["faults"]) != 1 or not self.alone(e):
                continue
            f = e["faults"][0]
            if len(f["nodes"]) != 1:
                continue
            c, x = f["class"], f["nodes"][0]
            if c not in BLAST_NOW | BLAST_LATER or x == "b1" and c in BLAST_NOT_B1:
                continue
            lo = f["on"][1] + ((3 * self.T + 5) * S if c in BLAST_LATER else 0)
            ms = []
            for st, op in self.by_start[bisect.bisect_left(self.by_start, (lo, "")):
                                        bisect.bisect_left(self.by_start, (f["off"][0], ""))]:
                node, _, k, _ = self.sends[op]
                a, r = self.acks.get(op), self.rec.get(op)
                if node != x and a and a[4] is not None and r and r["n"] != x and self.owner(k, f["on"][0]) != x:
                    ms.append(a[4])
            if not ms:
                continue
            self.judged["blast-radius"] += 1
            p99, most = pct(ms, .99), max(ms)
            self.blast[c].append((p99, most))
            if len(ms) >= 20 and p99 > BLAST_P99 / MS or most > BLAST_MAX / MS:
                self.flag("blast-radius", lo, episode=e["i"], cls=c, node=x, ops=len(ms), p99_ms=p99, max_ms=most)

    def check_sockets(self):
        """sockets: two samples of a node in a row, with no restart between them, both holding more than PER_PEER
        established connections to one peer, or both a connection the peer closed and the node did not; two `res`
        lines in a row with more sessions than a link each way per lane to every other node (the engine's peers,
        incoming and dialled)."""
        prev = {}
        for x in self.nem:
            if x.get("ev") != "sockets":
                continue
            n, p = x["node"], prev.get(x["node"])
            prev[n] = x
            ups = self.ups.get(n, [])
            if p is None or bisect.bisect_right(ups, p["t"]) != bisect.bisect_right(ups, x["t"]):
                continue
            self.judged["sockets"] += 1
            many = sorted(q for q, c in x["estab"].items() if c > PER_PEER and p["estab"].get(q, 0) > PER_PEER)
            if many or x["close_wait"] and p["close_wait"]:
                self.flag("sockets", x["t"], probe=False, node=n, peers=many, estab=x["estab"],
                          close_wait=[p["close_wait"], x["close_wait"]], since=p["t"])

        def beside(n, t):  # a session the engine holds beside a member's: a peer probe's that authenticated, or a
            # halfopen's old process, silent until heartbeats give it up while the new one dials in
            return any(f["args"].get("mode") in ("stale", "impostor") for f in self.fault_on(n, {"peer"}, t - self.B, t)) \
                or any(self.fault_on(m, {"halfopen"}, t - self.B, t) for m in self.nodes)
        # Two `res` lines in a row: a peer that gave its session up under a throttle dials in again before this node
        # gives up its side, one sample (chaos-20261001-1306-s52: b2 at 9 for one line as a `rate` went off).
        over = 2 * LANES * (len(self.nodes) - 1)
        for n, nd in self.nodes.items():
            prev = None
            for x in nd["node"]:
                if x.get("r") == "up":
                    prev = None
                if x.get("r") != "res" or x.get("sessions") is None:
                    continue
                if x["sessions"] > over and prev is not None and prev["sessions"] > over and not beside(n, x["_t"]):
                    self.flag("sockets", x["_t"], probe=False, node=n, sessions=x["sessions"], nodes=len(self.nodes))
                prev = x

    def check_kernel(self):
        """kernel: a message of KERNEL outside the windows of the classes that explain it (from 1 s before their on to
        60 s after their off) on its node, named by the unit in it or the file's; on one box the kernel is every node's.
        An OOM kill of anything but the rig (`rig`, `exe`, or `exe-bN` on one box, which names the node)."""
        files = [(n, os.path.join(nd["dir"], "kernel.jsonl")) for n, nd in sorted(self.nodes.items())]
        for node, p in files + [(None, os.path.join(self.dir, "kernel.jsonl"))]:
            for x in self.read(p):
                t0, msg = self.unit_line(x)
                m = re.search(r"rig-node@(b\d+)|\(exe-(b\d+)\)", msg)
                who = m.group(1) or m.group(2) if m else node
                t = self.to_store(who, t0) if who else t0
                if t < self.t0:  # the log starts a minute early: the box before the run, another run's heal
                    continue
                self.judged["kernel"] += 1
                for text, why in KERNEL:
                    if text not in msg:
                        continue
                    if text == "Killed process" and not re.search(r"\((rig|exe(-b\d+)?)\)", msg) or not why or not any(
                            self.fault_on(n, why, t - 60 * S, t + S) for n in ([who] if who else self.windows)):
                        self.flag("kernel", t, probe=False, node=who, msg=msg[:300])
                    break

    def check_clock(self):
        """causality: a second of a node's clock where an op's send came after the store's record of an op, or its ack
        before, beyond a clock fault's window on it: the reply went out before the record, or the clock moved."""
        for node, bs in sorted(self.inverted.items()):
            for b, lo, hi in bs:
                t = self.to_store(node, b * S)
                pad = 60 * S + max([abs(float(w["args"].get("step_s", 0))) * S for w in self.windows.get(node, ())
                                    if w["class"] in CLOCK] or [0])
                if not self.fault_on(node, CLOCK, t - pad, t + pad):
                    self.flag("causality", t, probe=False, node=node, second=b, send_after_ms=round(lo / MS, 3),
                              ack_after_ms=round(hi / MS, 3))

    @functools.cached_property
    def acc_ts(self):
        return [[x["t"] for x in xs] for xs in self.acc.values()]

    def slowest(self, t):
        """The slowest key's time from t to its next accepted write (s), or None."""
        gaps = [ts[i] - t for ts in self.acc_ts for i in [bisect.bisect_left(ts, t)] if i < len(ts)]
        return max(gaps) / S if gaps else None

    @functools.cached_property
    def recovery_json(self):
        """Per class, each episode's slowest key's time from its end to its next accepted write (s), beside its bound:
        B_CLASS plus idle, the p95 of the same wait from the start of a quiet window where it stays quiet, what the
        callers' pace alone puts into any recovery time."""
        rec = collections.defaultdict(list)
        for e in self.eps:
            x = self.slowest(e["end"])
            for c in e["classes"] if x is not None else ():
                rec[c].append(x)
        xs = [x for w in self.quiet_windows for x in [self.slowest(w)] if x is not None and self.quiet(w, w + x * S)]
        idle = pct(xs, .95) if xs else 0
        out = {"run": self.meta.get("run"), "mode": self.meta.get("mode"), "one_box": bool(self.meta.get("one_box")),
               "lease_secs": self.T, "nodes": len(self.nodes), "commit": self.meta.get("commit"), "idle_s": round(idle, 3), "classes": {}}
        for c, xs in sorted(rec.items()):
            b = B_CLASS.get(c)
            out["classes"][c] = {"n": len(xs), "p50": round(pct(xs, .5), 3), "p95": round(pct(xs, .95), 3),
                                 "max": round(max(xs), 3), "bound": b and round(b(self.T) + idle, 3)}
        return out

    # ---- output

    def run(self):
        for check in CHECKS:
            check(self)
        # A run the checker cannot read passes every invariant vacuously: say so instead.
        self.unreadable = ["node %s: lines but no `up`" % n for n, nd in self.nodes.items()
                           if nd["node"] and not any(x.get("r") == "up" for x in nd["node"])]
        if not self.sends and any(x["r"] == "A" and x["v"] == "ok" for x in self.store):
            self.unreadable.append("accepted appends but no `send` read")
        # ... and so does a run that judged too little: too few ops, faults seen, or steady time before the first.
        mode = self.meta.get("mode")
        floors = dict(FLOORS.get(mode, {}))
        ran = {w for nd in self.nodes.values() for x in nd["node"] if x.get("r") == "up"
               for w in x.get("workloads") or ["streams"]}
        if "fault-effect" in floors:
            floors["fault-effect"] = effect_floor(self.plan, floors["fault-effect"])
        if floors:  # the surface: probes and dead letters always, an isolate where the probes could see one, a TLS
            # run's passes, and the blob and local workloads where they ran
            floors.update({"probe-quiet": 1, "metrics-monotonic": 1, "dead-quiet": 1,
                           "readyz-isolated": min(1, self.isolates), "tls-rotated": int(bool(self.meta.get("tls")))})
            floors.update(dict.fromkeys(("blob-corrupt", "blob-quiet") if "blob" in ran else (), 1))
            floors.update(dict.fromkeys(
                ("local-lost", "supervision-bound", "timer-step", "local-quiet") if "local" in ran else (), 1))
            if self.wan:  # each where its scenario was planned
                named = set(self.names.values())
                floors.update({"isolation-quiet": 1, "stream-window": 1, "firehose-gap": 1,
                               "stream-throughput": int("geo-quiet" in named), "live-under-bulk": int(bool(named & set(nemesis.BULK)))})
        self.vacuous = ["%s judged %d, under %d" % (inv, self.judged[inv], n) for inv, n in floors.items()
                        if self.judged[inv] < n and FLOOR_WORKLOAD.get(inv, set()) <= ran]
        self.steady = (self.starts[0] if self.starts else self.end) - self.t0 - self.warmup
        if mode in STEADY_FLOOR and self.steady < STEADY_FLOOR[mode]:
            self.vacuous.append("%.1f s steady before the first episode, under %d" % (
                self.steady / S, STEADY_FLOOR[mode] / S))
        vs = sorted((v for vs in self.v.values() for v in vs), key=lambda v: v["t"])
        main, probe = [v for v in vs if not v["probe"]], [v for v in vs if v["probe"]]
        self.verdict = ("UNREADABLE" if self.unreadable else "VOID" if any(v["inv"] == "store-void" for v in main)
                        else "FAIL" if main else "UNJUDGED" if self.vacuous else "PASS")
        return main, probe

    def since(self, t):
        return "%.3f s" % ((t - self.t0) / S)

    def report(self, main, probe):
        m = self.meta
        out = ["run %s seed %s mode %s commit %s%s%s plan %s version %s%s" % (
            m.get("run", os.path.basename(self.dir.rstrip("/"))), m.get("seed"), m.get("mode"), m.get("commit"),
            " (dirty)" if m.get("dirty") else "", " plant %s" % m["plant"] if m.get("plant") else "",
            m.get("plan_sha256"), m.get("plan_version", 1), " one box" if m.get("one_box") else "")]
        verdicts = collections.Counter((x["r"], x["v"]) for x in self.store)
        out.append("counts: ops sent %d, acked %d, give_ups %d; store %s; episodes %d of %d planned %s; "
                   "unreadable lines %d" % (
            len(self.sends), len(self.acks), len(self.gives),
            " ".join("%s:%s=%d" % (r, v, c) for (r, v), c in sorted(verdicts.items())),
            len(self.eps), len(self.plan), dict(collections.Counter(c for e in self.eps for c in e["classes"])), self.bad))
        out.append("balance: sum of balances %d, sum of accepted deltas %d" % (self.total_balance, self.total_delta))
        merged = [c["is"] for c in self.clusters if len(c["is"]) > 1]
        out.append("episodes merged, overlapping: %d %s; steady before the first episode %.1f s" % (
            len(merged), merged if merged else "", self.steady / S))
        per = collections.Counter(v["inv"] for v in main)
        for inv in INVARIANTS:
            judged = " (%d judged)" % self.judged[inv] if inv in self.judged else ""
            out.append("%-20s %s%s" % (inv, "FAIL %d" % per[inv] if per[inv] else "PASS", judged))
        if probe:
            out.append("assumption violated (probe episodes, not failing): %s" % dict(
                collections.Counter(v["inv"] for v in probe)))
        if self.unjudged:
            out.append("growth not judged on %s: no incarnation of %d min" % (
                ", ".join(self.unjudged), (GROWTH_WARMUP + GROWTH_SPAN) / 60 / S))
        out += ["UNREADABLE: " + u for u in self.unreadable] + ["UNJUDGED: " + u for u in self.vacuous]
        out.append("VERDICT %s" % self.verdict)
        if main:
            out += self.context(main[0])
        out += self.reports() + self.stream_report() + self.quota_report()
        return "\n".join(out) + "\n"

    def context(self, v):
        t = v["t"]
        out = ["", "FIRST VIOLATION %s at %s: %s" % (v["inv"], self.since(t), json.dumps(v))]
        k = v.get("k")
        ops = {v[f] for f in ("op",) if v.get(f)}
        nodes = {v[f] for f in ("n", "node") if isinstance(v.get(f), str)}
        if k is not None:
            kl = [x for x in self.store if x["k"] == k]
            i = bisect.bisect_left([x["t"] for x in kl], t)
            out.append("store records of %s around it:" % k)
            out += ["  " + json.dumps(x) for x in kl[max(0, i - 20):i + 20]]
        for node, nd in self.nodes.items():
            cl = [x for x in self.read(os.path.join(nd["dir"], "client.jsonl")) if x.get("op") in ops] if ops else []
            jl = [x for p in nd["journals"] for x in self.read(p) if x.get("op") in ops] if ops else []
            kl = [x for x in nd["node"] if x.get("r") in ("start", "stop") and x.get("k") == k]
            sl = [x for x in self.stats.get(node, ()) if node in nodes and abs(x["_t"] - t) <= 30 * S]
            for what, xs in (("client", cl), ("journal", jl), ("actors", kl), ("stats", sl)):
                if xs:
                    out.append("%s %s:" % (node, what))
                    out += ["  " + json.dumps({a: b for a, b in x.items() if not a.startswith("_")}) for x in xs]
        if v.get("s") and v.get("node") in self.nodes:
            node = v["node"]
            out.append("%s streams, %s and aggregates within 60 s:" % (node, v["s"]))
            out += ["  " + json.dumps(x) for x in self.read(os.path.join(self.nodes[node]["dir"], "stream.jsonl"))
                    if x.get("s", v["s"]) == v["s"] and abs(self.to_store(node, x["t"]) - t) <= 60 * S]
        for e in self.eps:
            if e["start"] <= t + 5 * S and t - 60 * S <= e["end"]:
                out.append("episode %s %s from %s to %s: %s" % (e["i"], e["classes"], self.since(e["start"]),
                                                               self.since(e["end"]), json.dumps(e["faults"])))
        return out

    def reports(self):
        rj = self.recovery_json
        out = ["", "recovery after episodes, slowest key's time to its next accepted write (s): p50 p95 max (n), and "
               "the bound: B_CLASS plus %.2f s, the p95 of that time from a quiet window's start" % rj["idle_s"]]
        for c, x in rj["classes"].items():
            out.append("  %-14s %6.2f %6.2f %6.2f  (%d)  %s" % (
                c, x["p50"], x["p95"], x["max"], x["n"], "-" if x["bound"] is None else "%.2f%s" % (
                    x["bound"], " OVER" if x["max"] > x["bound"] else "")))
        out.append("latency of ops sent during episodes (ms): p50 p99")
        lat = collections.defaultdict(list)
        for e in self.eps:
            for _, op in self.sent_in(e["start"], e["end"]):
                a = self.acks.get(op)
                if a and a[4] is not None:
                    for c in e["classes"]:
                        lat[c].append(a[4])
        for c, xs in sorted(lat.items()):
            out.append("  %-14s %8.1f %8.1f  (%d)" % (c, pct(xs, .5), pct(xs, .99), len(xs)))
        out.append("blast radius, the others' ops through a fault on one node (ms): worst p99 and max of %d, %d allowed" % (
            BLAST_P99 / MS, BLAST_MAX / MS))
        for c, xs in sorted(self.blast.items()):
            out.append("  %-14s %8.1f %8.1f  (%d)" % (c, max(p for p, _ in xs), max(m for _, m in xs), len(xs)))
        out.append("explained refusals per class: %s" % dict(self.explained))
        out.append("refusals and turns judged with a round trip past %d ms: %d" % (DELTA_LOOSE / MS, self.loose))
        for seen in ("witnessed", "unwitnessed", "contradicted"):
            out.append("fault effects %s: %s" % (seen, {c: n for (c, s), n in sorted(self.effects.items()) if s == seen}))
        if self.journal_exempt:
            out.append("journal lines waived: %s" % {"%s %s%s" % (why, n, "" if t is None else " @" + self.since(t)): c
                                                     for (why, n, t), c in self.journal_exempt.items()})
        out.append("per node: ack p50/p99 ms, give_ups, stops by why, load p50/p99 ms")
        byn = collections.defaultdict(list)
        for op, a in self.acks.items():
            s = self.sends.get(op)
            if s and a[4] is not None:
                byn[s[0]].append(a[4])
        gives = collections.Counter(self.sends[op][0] for op in self.gives if op in self.sends)
        for node in sorted(self.nodes):
            nl = self.nodes[node]["node"]
            stops = collections.Counter(x.get("why") for x in nl if x.get("r") == "stop")
            loads = [x for x in nl if x.get("r") == "load"]
            xs = byn.get(node, [])
            out.append("  %s: ack %s/%s, give_ups %d, stops %s, load %s/%s" % (
                node, pct(xs, .5), pct(xs, .99), gives[node], dict(stops),
                pct([x.get("p50", 0) for x in loads], .5), max([x.get("p99", 0) for x in loads] or [None])))
        out.append("resources per node and incarnation, p50 of the first and last quarter after %d min of warmup:" % (
            GROWTH_WARMUP / 60 / S))
        out += self.growth
        out.append("refusals seen by clients (give_up err): %s" % dict(collections.Counter(e for _, e in self.gives.values())))
        return out

    # ---- the quota workload (src/quota.rs, spec/fault-rig.md § Workloads)

    def quota_lines(self, node, path):
        """The node's quota lines, each with `_st`: its monotonic `m` placed on the store's clock by an anchor per
        incarnation, taken where the wall clock is trusted, so a stepped node stays on the timeline. None before the
        first anchor. The anchor moves when two lines in a row agree on a new one (drift, a clock sample)."""
        clock = [f for f in self.windows.get(node, ()) if f["class"] in CLOCK]

        def doubtful(ts):
            for f in clock:
                lo, hi, step = f["on"][0] - S, f["off"][1] + S, float(f["args"].get("step_s", 0)) * S
                if lo <= ts <= hi or lo <= ts - step <= hi:
                    return True
            return False
        off = cand = None
        out = []
        for x in self.read(path):
            if "m" not in x:
                continue
            if x.get("r") == "qup":
                off = cand = None
            ts = self.to_store(node, x["t"])
            o = ts - x["m"] * MS
            if not doubtful(ts):
                if off is None or cand is not None and abs(o - cand) <= QUOTA_ANCHOR:
                    off, cand = o, None
                elif abs(o - off) > QUOTA_ANCHOR:
                    cand = o
                else:
                    cand = None
            x["_st"] = None if off is None else off + x["m"] * MS
            x["_off"], x["_at"] = off, ts if x["_st"] is None else x["_st"]  # _at: for a flag, by wall clock at worst
            out.append(x)
        return out

    def quota_faults(self, share, n, first, uses=()):
        """Each fault's (from, until, excess in limits) where its class may push a key past its limit, and the key's
        first use: every node on the fallback, n shares at once (spec/quotas.md § 5, a new key), for QUOTA_NEW and as
        long again as what they admitted may last. An incarnation's first use later than that admits its share on the
        fallback beside the grants the others hold (§ 2.5): the limit once, the share per such node."""
        out = [(first, first + QUOTA_NEW + QUOTA_AFTER, n * share - 1, "new")] if n * share > 1 else []
        out += [(t, t + QUOTA_NEW + QUOTA_AFTER, share, "new") for t in uses if t > first + QUOTA_NEW]
        for e in self.eps:
            for f in e["faults"]:
                x = quota_excess(f, share, n, self.seeds)
                if x:
                    out.append((f["on"][0], f["off"][1] + QUOTA_AFTER, x, f["class"]))
        return sorted(out)

    def check_quota(self):
        """Rates admitted across the cluster per window, permits held at once, what a refusal says, and readiness,
        each against spec/quotas.md § 5's bounds for the faults then."""
        self.quota_over = collections.defaultdict(dict)  # k -> class -> most, in limits
        rates = collections.defaultdict(lambda: collections.defaultdict(lambda: [0, 0]))  # k -> bin -> [admitted, offered]
        spans = collections.defaultdict(list)  # k -> [(t, +1 | -1)]
        uses = collections.defaultdict(list)  # k -> each incarnation's first acquire or drain of it
        limits, nodes = {}, 0
        for node, nd in self.nodes.items():
            lines = self.quota_lines(node, os.path.join(nd["dir"], "quota.jsonl"))
            incs = []
            for x in lines:
                if x.get("r") == "qup" or not incs:
                    incs.append([])
                incs[-1].append(x)
            for inc in incs:
                open_, skipped, last, used = collections.Counter(), collections.Counter(), None, set()
                for x in inc:
                    ev, st, at = x.get("r"), x["_st"], x["_at"]
                    k = "lim.open" if ev == "lopen" else x.get("k")
                    if ev in ("pacq", "lopen", "drain") and st is not None and k not in used:
                        used.add(k)
                        uses[k].append(st - x.get("ms", 0) * MS)  # a drain logs when it ends
                    if ev == "qup":
                        nodes = max(nodes, x.get("nodes", 0))
                        limits.update({"q0": x["hot"], "q1": x["hot"], "r0": x["under"], "p0": x["ws"], "p1": x["ws"],
                                       "lim": x["lim"]["records"], "lim.open": x["lim"]["open"],
                                       "lim.subs": x["lim"]["subscriptions"]})
                        self.quota_pulling(node, x, inc)
                    if ev == "qrate":
                        self.quota_rate(node, x, rates)
                    elif ev == "lsub":
                        self.judged["lim-subs"] += 1
                        if x["max"] > limits.get("lim.subs", x["max"]):
                            self.flag("lim-subs", at, node=node, held=x["max"],
                                      limit=limits["lim.subs"])
                        if x["errors"]:
                            self.flag("lim-err", at, node=node, op="subscribe", errors=x["errors"])
                    elif ev == "lerr":
                        self.flag("lim-err", at, node=node, s=x.get("s"), op=x.get("op"),
                                  err=x.get("err"))
                    elif ev == "qheld":
                        self.judged["quota-permit-leak"] += 1
                        extra = x["table"] - x["lim_open"] - sum(x["held"])
                        if extra > QUOTA_TABLE_SLACK:
                            self.flag("quota-permit-leak", at, node=node, table=x["table"],
                                      held=x["held"], lim_open=x["lim_open"])
                    elif ev == "drain" and st is not None and self.quiet(st - QUOTA_DRAIN_QUIET, st):
                        self.judged["quota-permit-leak"] += 1
                        if x["got"] < x["want"]:
                            self.flag("quota-permit-leak", st, node=node, k=x["k"], got=x["got"], want=x["want"],
                                      ms=x["ms"])
                    elif ev in ("pacq", "prel", "lopen", "lclose"):
                        k = x["k"] if ev in ("pacq", "prel") else "lim.open"
                        if ev in ("pacq", "lopen"):
                            if st is None:
                                skipped[k] += 1
                                continue
                            open_[k] += 1
                            spans[k].append((st, 1))
                        elif skipped[k]:
                            skipped[k] -= 1
                        elif open_[k]:
                            open_[k] -= 1
                            spans[k].append((st if st is not None else last, -1))
                    if st is not None:
                        last = st
                for k, c in open_.items():  # held when the incarnation ended: until its last line
                    spans[k] += [(last, -1)] * c
        if not limits:
            return
        nodes = max(nodes, 1)
        for k, bins in rates.items():
            self.quota_window(k, bins, limits[k], nodes)
        for k, xs in spans.items():
            self.quota_held(k, xs, limits[k], nodes, uses[k])

    def quota_pulling(self, node, up, inc):
        """quota-pulling: a replica that started is ready within QUOTA_READY, unless its incarnation ended first or a
        fault began, or held another than the restart that started it."""
        t, most = up["_at"], up["m"] + QUOTA_READY // MS
        if inc[-1]["m"] < most or any(
                f["on"][0] <= t + QUOTA_READY and t <= f["off"][1] and not (f["class"] in RESTARTS and f["on"][0] < t)
                for e in self.eps for f in e["faults"]):
            return
        self.judged["quota-pulling"] += 1
        ready = next((x["m"] for x in inc if x.get("r") == "qready"), None)
        if ready is None or ready > most:
            self.flag("quota-pulling", t + QUOTA_READY, node=node, up=t, ready_after_ms=ready and ready - up["m"])

    def quota_rate(self, node, x, rates):
        """A qrate line's buckets into the store's bins, and what a refusal said to wait, which rests on no clock."""
        k, w = x["k"], x["w"]
        ra = x.get("ra")
        if ra:
            self.judged["quota-retry"] += 1
            hi = (max(w / 8, 100) if w else 1000) + 1  # a rate's bucket or a report; a permit's heartbeat
            if not 0 < ra[0] or ra[1] > hi:
                self.flag("quota-retry", x["_at"], node=node, k=k, ra=ra, most=hi)
        st, off = x["_st"], x["_off"]
        if st is None or not w:
            return
        if k == "r0" and self.quiet(st - S, st):
            self.judged["quota-retry"] += 1
            if sum(x["l"]) or x.get("stuck", 0) > QUOTA_STUCK / MS:
                self.flag("quota-retry", st, node=node, k=k, refused_at_limit=sum(x["l"]), stuck_ms=x.get("stuck"))
        for j, (a, l, s) in enumerate(zip(x["a"], x["l"], x["s"])):
            v = rates[k][int((off + (x["b"] + j + 0.5) * QUOTA_BIN) // QUOTA_BIN)]
            v[0] += a
            v[1] += a + l + s

    def quota_window(self, k, bins, m, n):
        """quota-rate-over, lim-rate: admitted in every 8 bins, a window, at most m (1 + rho) and a bucket each for
        the nodes' grids and ours, plus each fault's excess until QUOTA_AFTER past its end. quota-under: in a quiet
        window offered at least twice m, at least QUOTA_UNDER of it admitted."""
        inv = "lim-rate" if k == "lim" else "quota-rate-over"
        lo, hi = min(bins), max(bins)
        first = min(g for g in bins if bins[g][0] or g == hi) * QUOTA_BIN
        faults = self.quota_faults(math.ceil(m / n) / m, n, first)
        base = m * (1 + QUOTA_RHO + 2 / 8)
        s, i, active, over = 0, 0, [], False
        for g in range(lo, hi + 1):
            s += bins[g][0] if g in bins else 0
            s -= bins[g - 8][0] if g - 8 in bins else 0
            if g not in bins:
                continue
            te = (g + 1) * QUOTA_BIN
            ts = te - 8 * QUOTA_BIN
            while i < len(faults) and faults[i][0] <= te:
                active.append(faults[i])
                i += 1
            active = [f for f in active if f[1] >= ts]
            self.judged[inv] += 1
            most = self.quota_over[k]
            for c in {f[3] for f in active} or ["steady"]:
                most[c] = max(most.get(c, 0), s / m)
            bound = base + m * sum(f[2] for f in active)
            if s > bound and not over:
                self.flag(inv, te, k=k, admitted=s, bound=round(bound, 1), limit=m, faults_excess=[f[3] for f in active])
            over = s > bound
            # a window's lines come up to a second after it: one within that of a phase's end is short (pairs s50)
            if k != "r0" and g % 8 == 7 and self.quiet(ts, te + S):
                o = sum(bins.get(h, (0, 0))[1] for h in range(g - 7, g + 1))
                if o >= 2 * m:
                    self.judged["quota-under"] += 1
                    if s < QUOTA_UNDER * m:
                        self.flag("quota-under", te, k=k, admitted=s, offered=o, limit=m)

    def quota_held(self, k, xs, p, n, uses):
        """quota-permit-over, lim-open: spans shrunk at both ends by the clocks' slop and the anchor's tolerance, never
        more at once than p and each fault's excess until QUOTA_AFTER past its end."""
        inv = "lim-open" if k == "lim.open" else "quota-permit-over"
        d = QUOTA_ANCHOR + max(self.slop.values() or [0])
        faults = self.quota_faults(math.ceil(p / n) / p, n, min([t for t, v in xs if v > 0] + uses), uses)
        evs = sorted((t + d if v > 0 else t - d, v) for t, v in xs)
        self.judged[inv] += sum(v > 0 for _, v in xs)
        c, over = 0, False
        for t, v in evs:
            c += v
            bound = p * (1 + sum(f[2] for f in faults if f[0] <= t <= f[1]))
            if c > bound and not over:
                self.flag(inv, t, k=k, held=c, bound=bound, limit=p)
            over = c > bound
            self.quota_over[k]["held"] = max(self.quota_over[k].get("held", 0), c / p)

    def quota_report(self):
        if not self.quota_over:
            return []
        out = ["quota: the most admitted in a window or held at once, in limits, per key and class active then:"]
        return out + ["  %-8s %s" % (k, " ".join("%s %.2f" % cv for cv in sorted(most.items())))
                      for k, most in sorted(self.quota_over.items())]


def check(rundir, quiet=False):
    r = Run(rundir)
    main, probe = r.run()
    text = r.report(main, probe)
    with open(os.path.join(rundir, "check.txt"), "w") as f:
        f.write(text)
    with open(os.path.join(rundir, "violations.jsonl"), "w") as f:
        for v in main + probe:
            f.write(json.dumps(v) + "\n")
    with open(os.path.join(rundir, "recovery.json"), "w") as f:
        json.dump(r.recovery_json, f, indent=1)
    if not quiet:
        sys.stdout.write(text)
    return {"UNREADABLE": 2, "VOID": 3, "FAIL": 1, "UNJUDGED": 2, "PASS": 0}[r.verdict], main, probe, r


# ---- the registries later lanes add to

CHECKS = [Run.check_store, Run.check_clients, Run.check_liveness, Run.check_nodes, Run.check_peer, Run.check_streams,
          Run.check_growth, Run.check_restarts, Run.check_logs, Run.check_nemesis,
          Run.check_disk,  # before check_effects: its lines are evidence of the disk faults
          Run.check_effects, Run.check_void, Run.check_during, Run.check_actors, Run.check_blast, Run.check_sockets,
          Run.check_kernel, Run.check_clock]


def silent(r, f):
    """The node logged nothing while the fault held it stopped, and something in the 5 s before."""
    a, b = f["on"][1] + 200 * MS, f["off"][0]
    if b - a < S:
        return None, "too short to see"
    for n in f["nodes"]:
        if r.fault_on(n, CLOCK, f["on"][0], b):
            return None, "%s's clock moved" % n
        ts = r.ts.get(n, [])
        i, j = bisect.bisect_left(ts, a), bisect.bisect_left(ts, b)
        if j > i:
            return False, "%s logged %d lines while stopped, the first %.3f s after its on" % (n, j - i, (ts[i] - a) / S)
        if not (i and ts[i - 1] >= f["on"][0] - 5 * S):
            return None, "%s silent before" % n
    return True, "silent"


def restarted(r, f):
    """The node started again (an `up`) after the fault began, within 60 s of its off; a skew-restart's step moves the
    up back by its size."""
    pad = abs(float(f["args"].get("step_s", 0))) * S
    for n in f["nodes"]:
        ups = r.ups.get(n, [])
        i = bisect.bisect_right(ups, f["on"][0] - pad)
        if not (i < len(ups) and ups[i] <= f["off"][1] + 60 * S):
            return False, "%s did not start again" % n
    return True, "restarted"


def cut(r, f):
    """No op crossed the cut: none sent from one side after it began was written on the other (by the store's clock:
    a sender's may have moved) and acked before its off. The sides are the nodes and the rest (isolate), `other`
    (partition, bridge) or `to` (oneway, which drops only what the nodes send: an op from `to` still reaches them)."""
    a, b = set(f["nodes"]), far_side(f, r.nodes)
    lo, hi = f["on"][1] + 100 * MS, f["off"][0]
    tried = 0
    for _, op in r.sent_in(lo, hi):
        node = r.sends[op][0]
        if node not in a | b:
            continue
        tried += 1
        ack, rec = r.acks.get(op), r.rec.get(op)
        if ack and rec and ack[0] <= hi and lo <= rec["t"] <= hi and rec["n"] in a | b and (node in a) != (
                rec["n"] in a) and not (f["class"] == "oneway" and node in b):
            return False, "op %s from %s written by %s and acked during the cut" % (op, node, rec["n"])
    return (True, "%d ops sent across it" % tried) if tried else (None, "no ops sent")


def delayed(r, f):
    """Ops between a delayed node and another (with `to`, one of those) took at least half the delay, at the median."""
    d, ms, to = float(f["args"].get("delay_ms", 0)), [], set(f["args"].get("to") or ())
    lo, hi = f["on"][1], f["off"][0]
    for _, op in r.sent_in(lo, hi):
        node, ack, rec = r.sends[op][0], r.acks.get(op), r.rec.get(op)
        if ack and rec and ack[4] is not None and ack[0] <= hi and node != rec["n"] and (
                node in f["nodes"] or rec["n"] in f["nodes"]) and (not to or {node, rec["n"]} & to):
            ms.append(ack[4])
    if len(ms) < 20:
        return None, "%d ops" % len(ms)
    return pct(ms, .5) >= d / 2, "p50 %.1f ms of %d ops, delay %d ms" % (pct(ms, .5), len(ms), d)


def redialed(*names, strict=True):
    def moved(r, f):
        """A counter of the stats lines (under net when dotted) grew on the node or a peer the fault names, from the
        last line before its on to 2 WINDOW after its off, in one incarnation: sessions broke and were dialled again.
        Without strict, none moving tells nothing."""
        def get(x, k):
            for p in k.split("."):
                x = x.get(p) if isinstance(x, dict) else None
            return x
        read = False
        for n in sorted(set(f["nodes"]) | set(f["args"].get("to") or ())):
            st = r.stats.get(n, [])
            i = bisect.bisect_right([x["_t"] for x in st], f["on"][0])  # bisect's key= is 3.10's
            base = st[i - 1] if i else None
            for x in st[i:] if i else ():
                if x["_t"] > f["off"][1] + 2 * WINDOW or x["_inc"] != base["_inc"]:
                    break
                for k in names:
                    a, b = get(base, k), get(x, k)
                    if a is None or b is None:
                        continue
                    read = True
                    if b > a:
                        return True, "%s's %s %d -> %d" % (n, k, a, b)
        if not read:
            return None, "no %s to read" % "/".join(names)
        return False if strict else None, "no %s moved" % "/".join(names)
    return moved


def given(arg, value, ev):
    """ev, for a fault whose args[arg] is value; nothing to read for the others."""
    def seen(r, f):
        return ev(r, f) if f["args"].get(arg) == value else (None, "%s %s" % (arg, f["args"].get(arg)))
    return seen


def fsync_at_least(need):
    def slow(r, f):
        """The node's slowest fsyncs while the fault held reached what it imposes: a stats line's fsync_us p99 where
        the fault held its whole 10 s, else its max, since a shorter fault leaves under 1 % of them slow; only lines
        the fault held for half of its time or of theirs, whose max is not the noise of a line it barely touched."""
        want, on, off = need(f["args"]), f["on"][1], f["off"][0]
        for n in f["nodes"]:
            p = [x["fsync_us"]["p99" if held >= WINDOW else "max"] for x in r.stats.get(n, ())
                 for held in [min(x["_t"], off) - max(x["_t"] - WINDOW, on)]
                 if 2 * held >= min(off - on, WINDOW) and (x.get("fsync_us") or {}).get("n")]
            if not p:
                return None, "%s fsynced nothing" % n
            if max(p) < want:
                return False, "%s fsync (p99, or max where it held under a line) at most %d us, the fault imposes %d" % (
                    n, max(p), want)
        return True, "slow"
    return slow


def journal_err(need=""):
    def failed(r, f):
        """The node's journal writes failed (journal_err, its message holding `need`), where it wrote records."""
        lo, hi = f["on"][0], f["off"][1] + S
        for n in f["nodes"]:
            if any(x.get("r") == "journal_err" and lo <= x["_t"] <= hi and need in str(x.get("err"))
                   for x in r.nodes.get(n, {}).get("node", ())):
                continue
            if r.wrote(n, f["on"][1] + S, f["off"][0]):
                return False, "%s wrote records and no journal_err%s" % (n, need and " with " + need)
            return None, "%s wrote nothing" % n
        return True, "journal writes failed"
    return failed


def peer_hit(r, f):
    """The node's listener saw the probe: its handshake failures, rejections or accepts moved across two stats lines
    holding an attempt of the probe (Run.probe_pairs)."""
    def total(x):
        net = x.get("net") or {}
        return sum(net.get("handshake_failures") or []) + sum(
            net.get(k, 0) for k in ("accepted", "accept_errors", "rejected_busy", "rejected_per_ip"))
    for n in f["nodes"]:
        pairs = r.probe_pairs(n, f)
        if not pairs:
            return None, "%s: no attempt between two stats lines" % n
        if not any(total(b) > total(a) for a, b in pairs):
            return False, "%s's listener counters flat across the probe's attempts" % n
    return True, "listener counters moved"


def dropped(r, f):
    """Journal lines of records the node wrote while its disk dropped writes are gone after the reset."""
    for n in f["nodes"]:
        if r.journal_exempt[("diskdrop", n, f["on"][0])]:
            continue
        if r.wrote(n, f["on"][1] + S, f["off"][0]):
            return False, "%s's journal holds every record it wrote" % n
        return None, "%s wrote nothing" % n
    return True, "lines dropped"


# class -> what shows it happened: each fn(run, fault) -> (True | False | None, detail), None where the run holds too
# little to tell. A class with none (rate, cpu, memhigh, and the clocks) is counted unwitnessed. A disk class's entry
# reads the journal; its evidence on another disk goes under (class, disk), e.g. ("diskdrop", "streams").
EFFECT = {
    "kill": [silent, restarted], "pause": [silent], "freeze": [silent], "leave": [silent, restarted],
    "oom": [restarted], "skew-restart": [restarted],
    "isolate": [cut], "partition": [cut], "oneway": [cut], "bridge": [cut], "netem": [delayed],
    "slowdisk": [fsync_at_least(lambda a: 0.8 * float(a["delay_ms"]) * MS)],
    "io": [fsync_at_least(lambda a: 0.5 * S / float(a["iops"]))],
    "diskfull": [journal_err("No space")], "diskdrop": [dropped],
    "peer": [peer_hit],
    # The network lane's. graylink, flap, mtu, dup and corrupt leave no trace they must: unwitnessed.
    "reject": [cut, redialed("net.dial_failures", "net.dials", "became_unreachable")],
    "syndrop": [given("reset", 1, cut), given("reset", 1, redialed("net.dial_failures", "net.dials"))],
    "halfopen": [restarted], "portcut": [given("port", 7400, cut)], "asym": [delayed], "backlog": [restarted],
    "conntrack": [redialed("net.dials", "net.dial_failures", strict=False)],
    "ports": [redialed("net.dials", "net.dial_failures", "net.accepted")],
}
STREAM_STARVED |= {"graylink", "mtu"}  # a link that loses most packets, or every large one, starves a relay behind it


def hung(r, f):
    """The node's slowest journal fsync (stats' fsync_us max) after its disk hung reached half the hang, where a stats
    line after the off, in the incarnation the hang began in, could report it."""
    want = 0.5 * (f["off"][0] - f["on"][1])
    for n in f["nodes"]:
        inc = bisect.bisect_right(r.ups.get(n, []), f["on"][1])
        xs = [x for x in r.stats.get(n, ()) if f["on"][1] < x["_t"] <= f["off"][0] + 2 * WINDOW and x["_inc"] == inc]
        m = [x["fsync_us"]["max"] for x in xs if (x.get("fsync_us") or {}).get("n")]
        if not m or not any(x["_t"] > f["off"][0] for x in xs):
            return None, "%s reported no fsync after the hang" % n
        if max(m) < want:
            return False, "%s fsync max %d us, the hang %d" % (n, max(m), want)
    return True, "hung"


def disk_err(kind, must=True, hold=0):
    def refused(r, f):
        """The disk workload had a publish refused with Disk(kind) while the fault held (its first refusal of a run of
        them, or a stats line failing so after it), where it ran then: a line while it held (not a restart's open
        after the off, pairs-20261001-2342-s1002 episode 122), or on both sides in one incarnation. Where the fault
        held under hold, nothing to read."""
        lo, hi = f["on"][0], f["off"][1] + S
        if f["off"][0] - f["on"][1] < hold:
            return None, "held under %d s" % (hold // S)
        for n in f["nodes"]:
            xs = [x for x in r.disk.get(n, ()) if lo - WINDOW <= x["_t"] <= hi + WINDOW]
            ran = any(f["on"][1] <= x["_t"] <= f["off"][0] for x in xs) or \
                bool(xs) and xs[0]["_t"] < lo < hi < xs[-1]["_t"] and xs[0]["_inc"] == xs[-1]["_inc"]
            if not any(x.get("r") == "publish_err" and lo <= x["_t"] <= hi and "Disk(%s)" % kind in str(x.get("err")) or
                       x.get("r") == "disk" and lo <= x["_t"] <= hi + WINDOW and x.get("failing") == kind for x in xs):
                return False if must and ran else None, "%s had no publish refused with Disk(%s)%s" % (
                    n, kind, "" if ran else ", nor ran the disk workload")
        return True, "Disk(%s)" % kind
    return refused


def truncated(r, f):
    """After the reset of a disk that corrupted its writes, the node's next start cut its streams at a corrupt frame."""
    for n in f["nodes"]:
        o = [x for x in r.disk.get(n, ()) if x.get("r") == "open" and f["off"][0] <= x["_t"] <= f["off"][1] + 60 * S]
        if all(x.get("stored") is None for x in o):
            return None, "%s opened nothing its disk held after the reset" % n
        if not any(x.get("truncated") for x in o):
            return False, "%s's start after the reset cut nothing" % n
    return True, "cut at a corrupt frame"


def unit_says(text, disk=False):
    def said(r, f):
        """The node's unit log holds text while the fault held; with disk, only where the disk workload ran, the one
        reader of the streams disk."""
        for n in f["nodes"]:
            unit = r.nodes.get(n, {}).get("unit")
            if unit is None:
                return None, "%s has no unit log" % n
            if disk and not r.disk.get(n):
                return None, "%s ran no disk workload" % n
            if not any(f["on"][0] <= r.to_store(n, t) <= f["off"][1] and text in m for t, m in unit):
                return False, "%s's unit never said %s" % (n, text)
        return True, text
    return said


EFFECT["powercut"] = EFFECT[("powercut", "streams")] = [silent, restarted]
# A hung streams disk refuses with Behind once 256 KiB wait (src/disk.rs), two 2 KB records every 50 ms: 3.2 s.
EFFECT["diskhang"], EFFECT[("diskhang", "streams")] = [hung], [disk_err("Behind", hold=4 * S)]
for c in ("rofs", "diskdead", "diskerr"):
    EFFECT[c] = [journal_err()]
    EFFECT[(c, "streams")] = [disk_err("Failed")]


def down_for(ev, at_least=2 * S):
    """ev, where a flakey table, up first, was down at_least before the off: a second down, then the reset's restart,
    left no error a node saw (pairs-20261001-2342-s1002, episodes 4 and 177)."""
    def judged(r, f):
        up, cycle = float(f["args"]["up_s"]) * S, float(f["args"]["up_s"] + f["args"]["down_s"]) * S
        span = f["off"][0] - f["on"][1]
        down = span // cycle * (cycle - up) + max(0, span % cycle - up)
        return ev(r, f) if down >= at_least else (None, "down %.1f s" % (down / S))
    return judged


EFFECT["flaky"], EFFECT[("flaky", "streams")] = [down_for(journal_err())], [down_for(disk_err("Failed"))]
EFFECT[("diskfull", "streams")] = [disk_err("Full")]
EFFECT["splitdelay"] = [fsync_at_least(lambda a: 0.8 * float(a["write_ms"]) * MS)]
EFFECT[("readerr", "streams")] = [restarted, unit_says("Input/output error", disk=True)]
EFFECT[("diskcorrupt", "streams")] = [truncated]
EFFECT[("inodes", "streams")] = [disk_err("Full", must=False)]


# ---- the surface (spec/fault-rig.md § Workloads): what a node says of itself on its Health port, the CA rotation,
# and the blob, local and dead-letter lines.

INVARIANTS += [
    "readyz-isolated", "probe-quiet", "metrics-monotonic", "tls-rotated", "handshake-quiet",
    "blob-corrupt", "blob-quiet", "local-lost", "supervision-bound", "timer-step", "local-quiet", "dead-quiet",
]
READY_BY = 7 * S  # spec/seed-discovery.md § 2.2: a node cut off isolates by 7 s
# src/main.rs's wan transport and heartbeat: a cut node's sessions close idle after the last frame it had, at most a
# keepalive before the cut; it isolates at the first heartbeat a grace after the last one heard.
WAN_IDLE, WAN_KEEPALIVE, WAN_BEAT, WAN_GRACE = 8 * S, S, 500 * MS, 2 * S
# At wan by idle + heartbeat_interval + 3/2 heartbeat_grace (spec/wan.md § 12), and a probe's second
READY_BY_WAN = WAN_IDLE + WAN_BEAT + 3 * WAN_GRACE // 2 + S
READY_AFTER = 3 * S  # and stays so until its seed sessions are back
TLS_BY = 10 * S
RESET = ("axon_connection_", "axon_persisted_actor_")
TIMER_LATE_MS, TIMER_TICKS = 1000, (80, 120)  # src/local.rs: a timer every 100 ms, a line every 10 s
FLAKY = 4  # src/local.rs: supervised subscribers, each failing on its own
CATCH_UP = 5 * S  # a publisher's missed ticks after a stall: 4160 in 0.7 s after a 50 s memhigh, chaos s47


def check_probes(r):
    """readyz-isolated: a node the isolate class cuts off answers /readyz 503 with checks.cluster "isolated", at least
    once from its on until READY_AFTER after its off, and every time from READY_BY after its on until its off; a probe
    that got no answer is none of that. An isolate shorter than READY_BY may heal first: its miss is an assumption
    violated, not a failure. probe-quiet: in quiet windows every probe is answered 200, and no `health` line holds an
    error. metrics-monotonic: no counter of a node's /metrics goes down between two scrapes of one incarnation (the
    `up` lines), a scrape within 1 s of an `up` not judged, nor the series observability.md says restart with a
    connection's session or a persisted name."""
    probes = collections.defaultdict(list)
    for x in r.read(os.path.join(r.dir, "probes.jsonl")):
        probes[(x["node"], x["path"])].append(x)
    r.isolates = 0

    def isolated(x):
        return x["code"] == 503 and (x.get("checks") or {}).get("cluster") == "isolated"

    for e in r.eps:
        for f in e["faults"]:
            if f["class"] != "isolate":
                continue
            lo, hi = f["on"][1], f["off"][0]
            ready_by = READY_BY_WAN if r.wan else READY_BY
            short = hi - lo < ready_by  # the plan's start at 5 s: healed before the node need have isolated
            for n in sorted(set(f["nodes"]) - set(f.get("noop", ()))):
                # The one seed is live alone (spec/seed-discovery.md § 2.1): cut off, it never isolates.
                if r.exempt(n, f["on"][0], hi + READY_AFTER) or n == "b1" and r.meta.get("seeds") == "1":
                    continue
                r.isolates += 1
                xs = [x for x in probes[(n, "/readyz")] if lo <= x["sent"] and x["t"] <= hi + READY_AFTER]
                got = [x for x in xs if x["code"] is not None]
                if not xs:
                    continue
                r.judged["readyz-isolated"] += 1
                late = [x for x in got if x["sent"] >= lo + ready_by and x["t"] <= hi and not isolated(x)]
                if late or not any(map(isolated, got)):
                    x = (late or got or xs)[0]
                    r.flag("readyz-isolated", x["t"], probe=True if short else None, node=n, episode=e["i"],
                           held_ms=(hi - lo) // MS, code=x["code"], checks=x.get("checks"),
                           err=x.get("err"), probes=len(xs), answered=len(got), isolated=sum(map(isolated, got)))
    seen = set()
    for (n, path), xs in sorted(probes.items()):
        for x in xs:
            if not r.quiet(x["sent"], x["t"]):
                continue
            r.judged["probe-quiet"] += 1
            w = (n, path, x["sent"] // WINDOW)
            if x["code"] != 200 and w not in seen:
                seen.add(w)
                r.flag("probe-quiet", x["t"], node=n, path=path, code=x["code"], checks=x.get("checks"),
                       err=x.get("err"))
    for n, nd in r.nodes.items():
        for x in nd["node"]:
            if x.get("r") == "health" and x.get("err"):
                r.flag("probe-quiet", x["_t"], probe=False, node=n, what="health", err=x["err"])
        ups, prev = r.ups.get(n, []), None
        for x in probes[(n, "/metrics")]:
            j = bisect.bisect_left(ups, x["sent"] - S)
            if x["code"] != 200 or "counters" not in x or j < len(ups) and ups[j] <= x["t"] + S:
                continue
            inc = bisect.bisect_right(ups, x["t"])
            if prev and prev[0] == inc:
                r.judged["metrics-monotonic"] += 1
                down = sorted(k for k, v in x["counters"].items() if v < prev[1].get(k, v) and not k.startswith(RESET))
                if down:
                    r.flag("metrics-monotonic", x["t"], node=n, inc=inc, series=down[:5],
                           was=[prev[1][k] for k in down[:5]], now=[x["counters"][k] for k in down[:5]])
            prev = (inc, x["counters"])


def check_tls(r):
    """tls-rotated: within TLS_BY of each pass of the CA rotation (tls.jsonl), every node set that pass or a later one
    (a `tls` line, hup or boot), unless stopped, starved or restarted then; a node boots with every pass done before
    it; no `tls` line holds an error. handshake-quiet: no stats line counts a handshake failure more than the one
    before it in a quiet window."""
    rot = list(r.read(os.path.join(r.dir, "tls.jsonl")))
    tls = {n: [x for x in nd["node"] if x.get("r") == "tls"] for n, nd in r.nodes.items()}
    for n, xs in sorted(tls.items()):
        for x in xs:
            # A second's slack: a pass relinked between a booting node's read of `current` and its line, and its
            # SIGHUP waiting for the loop, is taken once the node is up.
            need = max((k["pass"] for k in rot if k["done"] + S <= x["_t"]), default=0)
            if x.get("err"):
                r.flag("tls-rotated", x["_t"], probe=False, node=n, what="error", err=x["err"], at=x.get("at"))
            elif x.get("at") == "boot" and int(x["pass"][1:]) < need:
                r.flag("tls-rotated", x["_t"], probe=False, node=n, what="booted on an old pass", got=x["pass"],
                       need=need)
        for k in rot:
            a, b = k["t"], k["done"] + TLS_BY
            if r.exempt(n, a, b):
                continue
            r.judged["tls-rotated"] += 1
            if not any(a - S <= x["_t"] <= b and not x.get("err") and int(x["pass"][1:]) >= k["pass"] for x in xs):
                r.flag("tls-rotated", b, probe=False, node=n, what="pass not set", need=k["pass"])

    def failures(x):
        v = (x.get("net") or {}).get("handshake_failures")  # one count per reason (axon_net::ListenerStats)
        return None if v is None else sum(v)

    for n, st in sorted(r.stats.items()):
        for a, c in zip(st, st[1:]):
            fa, fc = failures(a), failures(c)
            if fa is None or fc is None or a["_inc"] != c["_inc"] or not r.quiet(a["_t"], c["_t"]):
                continue
            r.judged["handshake-quiet"] += 1
            if fc > fa:
                r.flag("handshake-quiet", c["_t"], node=n, handshake_failures=[a["net"]["handshake_failures"],
                                                                               c["net"]["handshake_failures"]])


def check_surface_workloads(r):
    """blob-corrupt: no echo came back other than sent. blob-quiet: a blobs line whose window (and the 5 s a call may
    take before it) is quiet has calls answered and none failed. local-lost: no topic subscriber's gaps or dups grow
    unless the node was stopped, starved or restarted. supervision-bound: every flaky family alive, each planned
    failure restarted, at most one per family pending. timer-step: a window the node was not stalled in holds 80 to
    120 ticks, none over 1 s late, whatever its wall clock did. local-quiet: in quiet windows no ask fails and the
    publisher, the router, the flaky subscribers and the timer all move. dead-quiet: in quiet windows no dead letter
    but the flaky subscribers' planned ones."""
    for n in sorted(r.nodes):
        for x in r.lane(n, "bulk.jsonl"):  # src/bulk.rs: an upload whose crc the far side computed otherwise
            if x["r"] == "bulk_bad":
                r.flag("blob-corrupt", x["_t"], probe=False, node=n, to=x.get("to"), bytes=x.get("bytes"),
                       crc=x.get("crc"), want=x.get("want"))
            elif x["r"] == "bulks":
                r.judged["blob-corrupt"] += x.get("ok", 0)
        for x in r.lane(n, "blob.jsonl"):
            if x["r"] == "blob_bad":
                r.flag("blob-corrupt", x["_t"], probe=False, node=n, to=x.get("to"), bytes=x.get("bytes"),
                       back=x.get("back"), first_diff=x.get("first_diff"))
            elif x["r"] == "blobs":
                r.judged["blob-corrupt"] += x.get("ok", 0)
                if r.quiet(x["_t"] - WINDOW - 5 * S, x["_t"]):
                    r.judged["blob-quiet"] += 1
                    if x.get("err") or not x.get("ok"):
                        r.flag("blob-quiet", x["_t"], node=n, ok=x.get("ok"), err=x.get("err"), errs=x.get("errs"))
    for n in sorted(r.nodes):
        xs = [x for x in r.lane(n, "local.jsonl") if x["r"] == "local"]
        # A line written while the node shut down counts the families it already stopped: before its down, or from a
        # killterm's TERM until it is up again.
        stopping = [(d["_t"] - d.get("ms", 0) * MS, d["_t"]) for d in r.nodes[n]["node"] if d.get("r") == "down"]
        stopping += [(f["on"][0], next((u for u in r.ups.get(n, ()) if u > f["on"][0]), math.inf))
                     for f in r.fault_on(n, {"killterm"}, -math.inf, math.inf)]
        for x in xs:
            r.judged["supervision-bound"] += 1
            pending = x["planned_fails"] - x["restarts"]
            if (x["families"] != FLAKY or not 0 <= pending <= FLAKY) and not any(a <= x["_t"] <= b for a, b in stopping):
                r.flag("supervision-bound", x["_t"], node=n, families=x["families"], planned=x["planned_fails"],
                       restarts=x["restarts"])
            if not r.exempt(n, x["_t"] - WINDOW, x["_t"], clock=False):
                r.judged["timer-step"] += 1
                if x["late_max_ms"] > TIMER_LATE_MS or not TIMER_TICKS[0] <= x["timer_ticks"] <= TIMER_TICKS[1]:
                    r.flag("timer-step", x["_t"], probe=False, node=n, ticks=x["timer_ticks"],
                           late_max_ms=x["late_max_ms"])
        for p, x in zip(xs, xs[1:]):
            if p["_inc"] != x["_inc"]:
                continue
            # the publisher's interval fires the ticks a stall missed at once, past the subscribers' mailboxes
            if not r.exempt(n, p["_t"] - CATCH_UP, x["_t"]):
                r.judged["local-lost"] += 1
                if x["gaps"] > p["gaps"] or x["dups"] > p["dups"]:
                    r.flag("local-lost", x["_t"], node=n, gaps=[p["gaps"], x["gaps"]], dups=[p["dups"], x["dups"]])
            if r.quiet(p["_t"], x["_t"]):
                r.judged["local-quiet"] += 1
                still = [k for k in ("published", "asks_ok", "flaky_got") if x[k] <= p[k]] + [
                    "got[%d]" % i for i, (g, h) in enumerate(zip(p["got"], x["got"])) if h <= g]
                if x["asks_err"] > p["asks_err"] or still or not x["timer_ticks"]:
                    r.flag("local-quiet", x["_t"], node=n, asks_err=[p["asks_err"], x["asks_err"]], still=still,
                           ticks=x["timer_ticks"])
    for n, nd in sorted(r.nodes.items()):
        for x in nd["node"]:
            if x.get("r") != "dead" or not r.quiet(x["_t"] - 2 * WINDOW, x["_t"]):
                continue  # a letter settles a few seconds after its last repeat
            r.judged["dead-quiet"] += 1
            # The flaky families' topic, and what one held when it failed (its target the family, rig.local.flaky-k).
            bad = {k: c for k, c in (x.get("by") or {}).items()
                   if not k.rpartition(" ")[2].startswith("rig.local.flaky")}
            if bad:
                r.flag("dead-quiet", x["_t"], node=n, by=bad)


CHECKS += [check_probes, check_tls, check_surface_workloads]

# ---- across regions, TIMING=wan (spec/wan.md § 12; lanes.md and push-window.md § The rig (D3))

INVARIANTS += ["isolation-quiet", "live-under-bulk", "stream-throughput", "stream-window", "firehose-gap"]
FLOOR_WORKLOAD.update({"stream-window": {"firehose"}, "stream-throughput": {"firehose"}, "live-under-bulk": {"bulk"},
                       "firehose-gap": {"firehose"}})
ISOLATES_AFTER = WAN_IDLE - WAN_KEEPALIVE - WAN_BEAT + WAN_GRACE  # the soonest an unplanted node isolates: 8.5 s
LIVE_SLACK = 250 * MS
THROUGHPUT, LAG_MS = 0.9, 2000
PUSH_BYTES, PUSH_WINDOW, PUSH_INFLIGHT, PUSHES = (1 << 20) - 64, 64 << 20, 256 << 20, 64  # axon-stream's core.rs


def ambient(f):
    """TIMING=wan's backdrop: geo without loss, and the bulk uploads."""
    return f["class"] == "bulk" or f["class"] == "geo" and "loss" not in f["args"]


def reach(r, faults, n):
    """The nodes n still reaches through the links faults cut (healthy()'s), itself included."""
    cut, side, todo = r.healthy(faults)[2], set(), [n]
    while todo:
        x = todo.pop()
        if x not in side:
            side.add(x)
            todo += [y for y in r.nodes if (x, y) not in cut]
    return side


def check_wan(r):
    """isolation-quiet: in a quiet window, and through a geo scenario's faults, no node's isolations moves unless one cut
    it from every peer for longer than ISOLATES_AFTER; through faults shorter than lease_grace no leases_lost moves on a
    node that kept a majority of the acceptors. live-under-bulk: while bulk uploads run, each caller's echo p99 to each
    peer, over the episode's minutes away from others (10 calls or more), stays under the round trip between them and
    LIVE_SLACK, and no lease, isolation or peer is lost.
    stream-throughput: in a quiet 10 s under geo and no bulk, each firehose follower receives THROUGHPUT of what b1
    published, under LAG_MS behind. stream-window: each push window and what is in flight within core.rs's limits, and
    pushes resent only near a fault. firehose-gap: no follower's batch skips records of its generation within one
    subscription, fault or none: a `Live` follower skips nothing short of a reset (src/firehose.rs)."""
    if not r.wan:
        return
    isolation_quiet(r)
    live_under_bulk(r)
    if ran_workload(r, "firehose"):
        stream_throughput(r)
        stream_window(r)
        firehose_gap(r)


def firehose_gap(r):
    for n in sorted(r.nodes):
        for x in r.lane(n, "firehose.jsonl"):
            if x["r"] == "recv" and x.get("records"):
                r.judged["firehose-gap"] += 1
            elif x["r"] == "gap":
                r.flag("firehose-gap", x["_t"], node=n, g=x["g"], after=x["after"], first=x["first"],
                       missed=x["first"] - x["after"] - 1, since=x.get("since"))


def isolation_quiet(r):
    G = nemesis.grace(r.T) * S
    for n, st in sorted(r.stats.items()):
        for a, b in zip(st, st[1:]):
            lo, hi = a["_t"], b["_t"]
            if a["_inc"] != b["_inc"]:
                continue
            geo = [e for e in r.eps if e["start"] <= hi and lo <= e["end"] + r.B]
            mine = {e["i"] for e in geo}
            if not r.quiet(lo, hi) and not (geo and not r.exempt(n, lo - r.B, hi) and all(
                    f["ep"] in mine for f in r.fault_on(n, nemesis.TAGS, lo, hi))):
                continue
            r.judged["isolation-quiet"] += 1
            faults = [f for e in geo for f in e["faults"]]
            cut = max((f["off"][1] - f["on"][0] for f in faults if reach(r, [f], n) == {n}), default=0)
            moved = lambda c: b.get(c, 0) > a.get(c, 0)
            if moved("isolations") and cut <= ISOLATES_AFTER:
                r.flag("isolation-quiet", hi, node=n, isolations=[a.get("isolations"), b.get("isolations")],
                       cut_ms=cut // MS)
            if moved("leases_lost") and faults and all(f["off"][0] - f["on"][1] < G for f in faults) and 2 * len(
                    reach(r, faults, n) & set(r.acceptors)) > len(r.acceptors):
                r.flag("isolation-quiet", hi, node=n, leases_lost=[a.get("leases_lost"), b.get("leases_lost")],
                       what="a lease lost through a fault shorter than lease_grace, a majority of acceptors kept")


def live_under_bulk(r):
    calls = {n: [x for x in r.lane(n, "echo.jsonl") if x.get("r") in ("ok", "err")] for n in sorted(r.nodes)}
    at = {n: [x["_t"] for x in xs] for n, xs in calls.items()}
    stats_at = {n: [x["_t"] for x in st] for n, st in r.stats.items()}
    for f in r.bulks:
        probe = True if r.names.get(f["ep"]) in nemesis.GEO_PROBES else None
        # A pair's calls over the episode's judged minutes, not a minute's: 30 calls, whose p99 is the slowest
        pooled = collections.defaultdict(list)
        for lo in range(f["on"][1], f["off"][0], 60 * S):
            hi = min(lo + 60 * S, f["off"][0])
            if any(e["i"] != f["ep"] and e["start"] <= hi and lo - r.B <= e["end"] for e in r.eps):
                continue
            for n, xs in calls.items():
                if r.exempt(n, lo, hi):
                    continue
                for x in xs[bisect.bisect_left(at[n], lo):bisect.bisect_left(at[n], hi)]:
                    if not r.exempt(x["to"], lo, hi):
                        pooled[(n, x["to"])].append((x["_t"], x["ms"]))
                st = r.stats.get(n, [])[bisect.bisect_left(stats_at.get(n, []), lo):bisect.bisect_right(
                    stats_at.get(n, []), hi)]
                for a, b in zip(st, st[1:]):
                    if a["_inc"] != b["_inc"]:
                        continue
                    r.judged["live-under-bulk"] += 1
                    moved = [c for c in ("leases_lost", "isolations", "became_unreachable") if b.get(c, 0) > a.get(c, 0)]
                    if moved:
                        r.flag("live-under-bulk", b["_t"], probe=probe, node=n, moved=moved)
        for (n, to), xs in sorted(pooled.items()):
            if len(xs) < 10:
                continue
            r.judged["live-under-bulk"] += 1
            v, bound = [ms for _, ms in xs], 2 * nemesis.geo_ms(n, to) + LIVE_SLACK / MS
            if pct(v, .99) > bound:
                r.flag("live-under-bulk", max(t for t, _ in xs), probe=probe, node=n, to=to, p99_ms=pct(v, .99),
                       bound_ms=bound, calls=len(v))


def stream_throughput(r):
    geo = collections.defaultdict(list)
    for f in r.ambient:
        for n in f["nodes"] if f["class"] == "geo" else ():
            geo[n].append((f["on"][1], f["off"][0]))
    bulk = [(f["on"][0], f["off"][1]) for f in r.bulks]
    under = lambda n, a, b: any(s <= a and b <= e for s, e in geo.get(n, ()))
    bins, origins = collections.defaultdict(lambda: [0, 0]), set()  # (node, 10 s) -> [bytes, the largest lag]
    for n in sorted(r.nodes):
        for x in r.lane(n, "firehose.jsonl"):
            if x["r"] in ("pub", "recv"):
                v = bins[(n, x["_t"] // WINDOW)]
                v[0] += x["bytes"]
                v[1] = max(v[1], x.get("lag_ms", 0))
                if x["r"] == "pub":
                    origins.add(n)
    followers = sorted({n for n, _ in bins} - origins)
    for o, w in sorted(k for k in bins if k[0] in origins):
        a, b = w * WINDOW, (w + 1) * WINDOW
        if not r.quiet(a, b) or not under(o, a - 30 * S, b) or any(s <= b and a - 30 * S <= e for s, e in bulk) or \
                r.exempt(o, a, b):
            continue
        for n in followers:  # each that received in the 30 s before
            if not under(n, a - 30 * S, b) or r.exempt(n, a, b) or not any((n, w - k) in bins for k in (1, 2, 3)):
                continue
            r.judged["stream-throughput"] += 1
            (sent, _), (got, lag) = bins[(o, w)], bins.get((n, w), (0, 0))
            if got < THROUGHPUT * sent or lag >= LAG_MS:
                r.flag("stream-throughput", b, node=n, sent=sent, got=got, lag_ms=lag)


def stream_window(r):
    for n in sorted(r.nodes):
        hist, last, seen = collections.defaultdict(collections.deque), {}, set()

        def bad(what, x, peer=None, **kw):
            if (what, peer) not in seen:
                seen.add((what, peer))
                r.flag("stream-window", x["_t"], node=n, peer=peer, what=what, **kw)

        for x in r.lane(n, "firehose.jsonl"):
            # after its phase the nodes stop, and a push to one stopping is resent
            if x["r"] != "window" or not any(p["start"] <= x["_t"] <= p.get("end", r.end) for p in r.phases):
                continue
            r.judged["stream-window"] += 1
            ds = x.get("downstream") or []
            if x["push_inflight_bytes"] > PUSH_INFLIGHT + PUSH_BYTES * len(ds):
                bad("in flight past max_push_inflight", x, bytes=x["push_inflight_bytes"], children=len(ds))
            for d in ds:
                k = (x["_inc"], d["node"], d["lane"], d["channel"])
                h = hist[k]
                h.append((x["_t"], d["window"], d["rate"] or 0, d["rtt_us"] or 0))
                while h[0][0] < x["_t"] - WINDOW:
                    h.popleft()
                # A push works its window out between lines, as twice a rate times a round trip: of either line around
                # it (scenarios-20261026-2059-s1, a 444 ms round trip and the next line's rate).
                mix = max((2 * max(a[2], b[2]) * max(a[3], b[3]) // 1_000_000
                           for a, b in zip(h, itertools.islice(h, 1, None))), default=0)
                if d["inflight"] > max(max(y[1] for y in h), min(mix, PUSH_WINDOW)) + PUSH_BYTES:
                    bad("in flight past the window", x, d["node"], inflight=d["inflight"], window=d["window"])
                if d["window"] > PUSH_WINDOW or d["pushes"] > PUSHES:
                    bad("past max_push_window or the pushes in flight", x, d["node"], window=d["window"],
                        pushes=d["pushes"])
                p, last[k] = last.get(k), (x["_t"], d["resent"])
                if p and d["resent"] > p[1] and not any(
                        r.fault_on(m, nemesis.TAGS, p[0] - r.B, x["_t"]) or r.exempt(m, p[0] - r.B, x["_t"])
                        for m in (n, d["node"])):
                    bad("pushes resent with no fault", x, d["node"], resent=[p[1], d["resent"]])


CHECKS.append(check_wan)


def first_up(r, n, f):
    """The node's last `up` before the fault began, and its first after, within 60 s of the off (None where none)."""
    ups = [x for x in r.nodes.get(n, {}).get("node", ()) if x.get("r") == "up"]
    before = [x for x in ups if x["_t"] < f["on"][0]]
    after = [x for x in ups if f["on"][0] <= x["_t"] <= f["off"][1] + 60 * S]
    return before[-1] if before else None, after[0] if after else None


def upgraded(r, f):
    """Each rolled node came back on the other build: its first `up` after the on names another build than the one
    before it."""
    for n in f["nodes"]:
        before, after = first_up(r, n, f)
        if before is None or after is None or "build" not in before:
            return None, "%s has no up with a build on both sides" % n
        if after.get("build") == before["build"]:
            return False, "%s stayed on %s" % (n, before["build"])
    return True, "upgraded"


UP_CFG = {"LEASE_SECS": ("lease_ms", 1000), "HEARTBEAT_MS": ("heartbeat_ms", 1)}  # cfgdrift's variable in the up line


def drifted(r, f):
    """The node came back with the drifted setting: its first `up` after the on shows the value."""
    key, scale = UP_CFG[f["args"]["var"]]
    for n in f["nodes"]:
        _, after = first_up(r, n, f)
        if after is None or key not in after:
            return None, "%s has no up with %s after the on" % (n, key)
        if after[key] != round(float(f["args"]["value"]) * scale):
            return False, "%s came back with %s %s" % (n, key, after[key])
    return True, "drifted"


def leaver(ev):
    """ev of a bounded leave's leaver alone: the other nodes on its side of the cut never stopped."""
    return lambda r, f: ev(r, dict(f, nodes=[f["args"]["leaver"]]))


EFFECT["roll"] = [restarted, upgraded]
EFFECT["cfgdrift"] = [restarted, drifted]
EFFECT["scale"] = [silent, restarted]
EFFECT["cold-restart"] = [restarted]
EFFECT["acceptors-restart"] = EFFECT["acceptors-rolling"] = [restarted]
EFFECT["leave-minority"] = [cut, leaver(restarted)]
EFFECT["leave-slowdisk"] = [restarted]


# ---- process faults (spec/fault-rig.md § Fault classes)

PANICS = {"panic", "panic_start", "panic_job"}  # src/hooks.rs
# A kill after TERM waits up to 3 s; threadstarve pins threads through python; a core is written before the restart;
# swapoff reads back what the node swapped out.
SLOW_STEP.update({("killterm", "on"): 5 * S, ("threadstarve", "on"): 5 * S, ("threadstarve", "off"): 5 * S,
                  ("coredump", "on"): 10 * S, ("swap", "off"): 30 * S})
# Not threads: a peer's tokio blocking pool doubled after a crashloop on one box; growth holds threads to a ceiling.
PEER_FDS = 8


def hooked(r, f):
    """The node logged the hook fault.sh wrote (its `hook` line) while the fault held."""
    for n in f["nodes"]:
        xs = [x for x in r.nodes.get(n, {}).get("node", ()) if x.get("r") == "hook"]
        if not xs:
            return None, "%s logs no hook lines" % n
        if not any(x.get("hook") == f["args"].get("hook") and f["on"][0] <= x["_t"] <= f["off"][1] + S for x in xs):
            return False, "%s never logged hook %s" % (n, f["args"].get("hook"))
    return True, "hooked"


EFFECT.update({"crashloop": [restarted], "killterm": [restarted], "coredump": [restarted], "hook": [hooked],
               # the kill; the step is a clock's
               "takeover-step": [lambda r, f: restarted(r, dict(f, nodes=f["nodes"][:1]))]})


def panic_contained(r):
    """panic-contained: a hook's panic restarts the actor, not the node. Per hook fault of a panic kind and node that
    logged `panic` lines: no `up` from the first panic until 30 s after its off that a restart fault explains not;
    stats' supervised restarts moving in the same incarnation by two stats lines after the off; and a node that wrote
    records in the 30 s before the fault writing one within B + 30 s after its off."""
    for e in r.eps:
        for f in e["faults"]:
            if f["class"] != "hook" or f["args"].get("hook") not in PANICS:
                continue
            a, z = f["on"][0], f["off"][1]
            for n in sorted(set(f["nodes"]) - set(f.get("noop", ()))):
                nl = r.nodes.get(n, {}).get("node", ())
                p = [x for x in nl if x.get("r") == "panic" and a <= x["_t"] <= z + 2 * S]
                if not p:
                    continue
                r.judged["panic-contained"] += 1
                t0 = p[0]["_t"]
                what = dict(episode=e["i"], node=n, hook=f["args"]["hook"], panics=len(p))
                up = [t for t in r.ups.get(n, ()) if t0 < t <= z + 30 * S and not r.restart_near(n, t)]
                if up:
                    r.flag("panic-contained", up[0], what="the node started again", **what)
                    continue
                st = r.stats.get(n, [])
                before = [x for x in st if x["_t"] < t0 and x["_inc"] == p[0]["_inc"]][-1:]
                after = [x for x in st if t0 < x["_t"] <= z + 2 * WINDOW and x["_inc"] == p[0]["_inc"]]
                if before and "restarts" in before[0] and after and max(
                        x.get("restarts", 0) for x in after) <= before[0]["restarts"]:
                    r.flag("panic-contained", after[-1]["_t"], what="no supervised restart counted",
                           restarts=before[0]["restarts"], **what)
                elif r.wrote(n, a - 30 * S, a) and not r.wrote(n, z, z + r.B + 30 * S):
                    r.flag("panic-contained", z + r.B + 30 * S, what="wrote nothing after the off", **what)


def peer_leak(r):
    """peer-leak: after an episode alone of restart faults only, each peer it did not touch holds at most PEER_FDS
    more fds, and no more sessions than it had or than a link each way per lane to every other node, in its first
    `res` line from B after the episode's end until the next begins, against its last before the episode, in one
    incarnation."""
    res = {n: [x for x in nd["node"] if x.get("r") == "res"] for n, nd in r.nodes.items()}
    for e in r.eps:
        if not (set(e["classes"]) <= RESTARTS and r.alone(e)):
            continue
        c = e["cluster"]
        stop = r.starts[c + 1] if c + 1 < len(r.starts) else r.end
        hit = {n for f in e["faults"] for n in f["nodes"]}
        for n in sorted(set(r.nodes) - hit):
            before = [x for x in res[n] if x["_t"] < e["start"]][-1:]
            after = [x for x in res[n] if e["end"] + r.B <= x["_t"] < stop][:1]
            if not (before and after) or before[0]["_inc"] != after[0]["_inc"] or r.exempt(n, e["start"], after[0]["_t"]):
                continue
            r.judged["peer-leak"] += 1
            b, x = before[0], after[0]
            over = {}
            if None not in (b.get("fds"), x.get("fds")) and x["fds"] > b["fds"] + PEER_FDS:
                over["fds"] = [b["fds"], x["fds"]]
            if None not in (b.get("sessions"), x.get("sessions")) and x["sessions"] > max(b["sessions"], 2 * LANES * (len(r.nodes) - 1)):
                over["sessions"] = [b["sessions"], x["sessions"]]
            if over:
                r.flag("peer-leak", x["_t"], episode=e["i"], node=n, grew=over)


CHECKS += [panic_contained, peer_leak]


# ---- the synthetic run and the selftest


def synth(ops=20000, keys=50, seed=1):
    """A clean run: nodes b1..b3 (all acceptors), lease 2 s, four episodes. Kill b2 (its keys move after T + 1 s; one
    record it stored just before has no journal line), pause b3 for 12 s, longer than B (its keys move; a late write
    refused at the resume, one whose turn began 10 ms after the resume, explained, and an op b3 sent 20 ms before it,
    acked after), diskdrop b1 (its journal lines lost until the reset, and the first after it glued to stale bytes),
    skew-restart b2 b3 (a probe). One b2 record
    has a `journal_err` instead of its journal line. Returns the files as lists of lines, and what the plants need."""
    rnd = random.Random(seed)
    T0, T = 1_700_000_000 * S, 2
    nodes, off = ["b1", "b2", "b3"], {"b1": 0, "b2": 3 * MS, "b3": -2 * MS}
    run = "selftest-s%d" % seed
    files = collections.defaultdict(list)
    ks = ["k%d" % i for i in range(keys)]

    def at(s):
        return T0 + int(s * S)

    def nline(who, name, t, **kw):
        files[who + "/" + name].append(dict(t=t + off[who], **kw))

    def unit(who, t, msg):  # as `journalctl -o json` writes them, the node's clock
        files[who + "/unit.jsonl"].append({"__REALTIME_TIMESTAMP": str(t + off[who]), "MESSAGE": msg})

    E = [dict(i=0, cls="kill", nodes=["b2"], on=at(80), off=at(83), args={"hold_s": 3}),
         dict(i=1, cls="pause", nodes=["b3"], on=at(120), off=at(132), args={}),
         dict(i=2, cls="diskdrop", nodes=["b1"], on=at(160), off=at(170), args={}),
         dict(i=3, cls="skew-restart", nodes=["b2", "b3"], on=at(200), off=at(220), args={"step_s": -4, "hold_s": 2})]
    K, P, D = E[0], E[1], E[2]
    reset = D["off"] + 2 * S
    nem = files["nemesis.jsonl"]
    for n in nodes:
        nem.append(dict(t=at(0), ev="clock", node=n, offset_ms=off[n] / MS, rtt_ms=0.8))
    nem.append(dict(t=at(1), ev="clock", node="b2", ok=False, offset_ms=None, rtt_ms=None))  # ssh to it failed
    for e in E:
        # As nemesis.py logs them: skew-restart as a step on, a kill on, the kill's off, the step's off, by `sub`.
        steps = ([("on", "step", e["on"]), ("on", "kill", e["on"] + 50 * MS), ("off", "kill", e["on"] + 2 * S),
                  ("off", "step", e["off"])] if e["cls"] == "skew-restart" else [("on", None, e["on"]), ("off", None, e["off"])])
        for ev, sub, t in steps:
            nem.append(dict({"class": e["cls"]}, t=t + 40 * MS, ev=ev, i=e["i"], id="%d.0" % e["i"], nodes=e["nodes"],
                            args=e["args"], sent=t, done=t + 40 * MS, rc=0, stderr="", **({"sub": sub} if sub else {})))
        for n in nodes:
            nem.append(dict(t=e["off"] + 3 * S, ev="check", i=e["i"], node=n, rc=0, out="", forced=False))
            nem.append(dict(t=e["off"] + 3 * S, ev="clock", i=e["i"], node=n, ok=True, offset_ms=off[n] / MS,
                            rtt_ms=0.8, forced=False))
    nem.append(dict(t=reset, ev="journal-reset", i=D["i"], node="b1", how="fsck", detail="journal-reset fsck 0"))
    files["plan.jsonl"] = [dict(i=e["i"], at=(e["on"] - T0) // S, dur=(e["off"] - e["on"]) // S,
                                faults=[{"class": e["cls"], "nodes": e["nodes"], "args": e["args"]}]) for e in E]
    # skew-restart kills b2 and b3 50 ms into it and starts them 2 s later.
    down3, up3 = E[3]["on"] + 90 * MS, E[3]["on"] + 3 * S + 40 * MS
    lives = {"b1": [(at(0), at(300))], "b2": [(at(0), K["on"]), (K["off"] + S, down3), (up3, at(300))],
             "b3": [(at(0), down3), (up3, at(300))]}
    boot = {}
    for n, ls in lives.items():
        for i, (a, b) in enumerate(ls):
            boot[(n, i)] = (a + off[n]) // MS
            nline(n, "node.jsonl", a, r="up", node=n, inc=boot[(n, i)], workloads=["accounts", "streams"])
            if b < at(300):
                unit(n, b, "rig-node@%s.service: Main process exited, code=killed, status=9/KILL" % n)
        nline(n, "node.jsonl", at(299), r="down", ms=300, err=None, reason="sigterm", still_running=0)
        unit(n, at(299) + 300 * MS, "rig-node@%s.service: Deactivated successfully." % n)
    store = files["store.jsonl"]
    fence, owner, seq, bal, free, down, pending = {}, {}, {}, {}, {}, {}, {}

    def own(k, n, t):
        e = max(t // MS, fence.get(k, 0) + 1)
        store.append(dict(t=t, r="O", k=k, e=e, n=n, lag=200, v="ok", f=e, len=seq.get(k, 0)))
        nline(n, "node.jsonl", t - MS, r="start", k=k, e=e)
        fence[k], owner[k], down[k] = e, n, t

    for i, k in enumerate(ks):
        seq[k], bal[k], free[k] = 0, 0, 0
        own(k, nodes[i % 3], at(0.5) + i * 10)

    def fail(dead, t0, others):
        for j, k in enumerate(sorted(k for k in ks if owner[k] == dead)):
            pending[k] = (t0 + (T + 1) * S + j * 100, others[j % 2], dead, fence[k])

    stops = []

    def apply(k):
        t, n, dead, old = pending.pop(k)
        own(k, n, t)
        if dead == "b3":
            stops.append((k, old))
            nline("b3", "node.jsonl", P["off"] + 45 * MS + len(stops) * 100, r="stop", k=k, e=old, why="Fenced")

    count = collections.Counter()

    def inc_of(n, t):
        return next(i for i, (a, b) in enumerate(lives[n]) if a <= t < b)

    def process(t, n, k):
        for k2 in [k2 for k2, m in pending.items() if m[0] <= t or k2 == k]:
            apply(k2)
        d = rnd.randint(1, 100) * (1 if rnd.random() < 0.6 else -1)
        i = inc_of(n, t)
        count[(n, i)] += 1
        op = "%s.%d.%d" % (n, boot[(n, i)], count[(n, i)])
        p = max(t + 2 * MS, down[k] + MS, free[k])
        free[k] = p + MS
        w, e, s, b = owner[k], fence[k], seq[k], bal[k]
        o, a = ("ok", b + d) if b + d >= 0 else ("overdraft", b)
        rec = dict(t=p, r="A", k=k, e=e, n=w, s=s, op=op, d=d, b=b, a=a, o=o, lag=300, v="ok", f=e)
        store.append(rec)
        seq[k], bal[k] = s + 1, a
        if not (w == "b1" and D["on"] <= p <= reset):
            j = dict(rec, rtt=900)
            del j["t"]
            nline(w, "journal-%s.jsonl" % run, p + MS, **j)
        nline(n, "client.jsonl", t, r="send", op=op, k=k, d=d)
        nline(n, "client.jsonl", p + 2 * MS, r="ack", op=op, k=k, o=o, a=a, n=w, e=e, tries=1,
              ms=(p + 2 * MS - t) / MS)
        return rec

    sched = [(at(rnd.uniform(2, 285)), rnd.choice(nodes), rnd.choice(ks)) for _ in range(ops * 21 // 20)]
    sched = sorted([(t, n, k) for t, n, k in sched
                    if not (n == "b2" and K["on"] - 100 * MS <= t <= K["off"] + 1500 * MS)
                    and not (n == "b3" and P["on"] - 100 * MS <= t <= P["off"] + 200 * MS)
                    and not (n in ("b2", "b3") and down3 - 150 * MS <= t <= up3 + 500 * MS)][:ops - 3]
                   + [(P["on"] - 20 * MS, "b3", ks[3])])
    # b2's client sent this just before the kill and died with it: no ack, and no liveness violation.
    nline("b2", "client.jsonl", K["on"] - 50 * MS, r="send", op="b2.%d.0" % boot[("b2", 0)], k=ks[0], d=1)
    events = [(K["on"], "b2"), (P["on"], "b3")]
    info = dict(T0=T0, E=E, run=run, keys=ks)
    for t, n, k in sched:
        while events and events[0][0] <= t:
            et, dead = events.pop(0)
            if dead == "b2":
                fail("b2", et, ["b1", "b3"])
                continue
            b3keys = sorted(k2 for k2 in ks if owner[k2] == "b3")
            fail("b3", et, ["b1", "b2"])
            # Two ops b3 took before the pause and wrote after it: refused, retried by b1, acked by the new owner.
            for tag, k2, turn, lag in (("late", b3keys[0], et - 3 * MS, None),
                                       ("resumed", b3keys[1], P["off"] + 50 * MS, 3 * MS)):
                old = fence[k2]
                rec = process(et - 5 * MS, "b1", k2)
                S_ = turn + lag if lag else P["off"] + 60 * MS
                line = dict(rec, t=S_, e=old, n="b3", lag=S_ - turn, v="stale", f=fence[k2])
                store.append(line)
                info[tag] = line
        process(t, n, k)
    for k in list(pending):
        apply(k)
    synth_streams(nline, lives, E)
    # b3's op caught by the pause: acked 500 ms after the resume.
    straddle = next(x for x in files["b3/client.jsonl"] if x["r"] == "send" and x["t"] == P["on"] - 20 * MS + off["b3"])
    ack = next(x for x in files["b3/client.jsonl"] if x["r"] == "ack" and x["op"] == straddle["op"])
    ack["t"], ack["ms"] = P["off"] + 500 * MS + off["b3"], (P["off"] + 500 * MS - P["on"] + 20 * MS) / MS
    # b2 killed between the store's OK and its fsync, so the op another node sent is acked by the key's next owner,
    # from the record; and a failed journal write, logged.
    by_b2 = [x for x in store if x["r"] == "A" and x["n"] == "b2"]
    killed = max((x for x in by_b2 if x["t"] < K["on"] and not x["op"].startswith("b2.")), key=lambda x: x["t"])
    client = files[killed["op"].split(".")[0] + "/client.jsonl"]
    ack = next(x for x in client if x["r"] == "ack" and x["op"] == killed["op"])
    late = K["on"] + (T + 2) * S - killed["t"]
    ack["t"], ack["ms"] = ack["t"] + late, ack["ms"] + late / MS
    failed = next(x for x in by_b2 if x["t"] > at(40))
    jn = "b2/journal-%s.jsonl" % run
    files[jn] = [j for j in files[jn] if j["op"] not in (killed["op"], failed["op"])]
    # After the reset b1's journal holds what the dropped writes left: stale bytes, then its next line glued on.
    glued = min((x for x in files["b1/journal-%s.jsonl" % run] if x["t"] > reset), key=lambda x: x["t"])
    glued["_pre"] = "\0" * 40 + '{"a":3041,"b":2999,"d":42,"e":88911,"v":"ok"}\n' + '{"a":552,"b":558,"k":"k63","rtt"'
    nline("b2", "node.jsonl", failed["t"] + MS, r="journal_err", k=failed["k"], e=failed["e"], op=failed["op"],
          err="EIO")
    # The stats lines last, their counters from the client lines as they now are.
    acked = collections.defaultdict(list)
    for n in nodes:
        for x in files[n + "/client.jsonl"]:
            if x["r"] == "ack":
                acked[x["op"].rpartition(".")[0]].append(x["t"])
    for ts in acked.values():
        ts.sort()
    for n, ls in lives.items():
        for i, (a, b) in enumerate(ls):
            acks = acked["%s.%d" % (n, boot[(n, i)])]
            for j, t in enumerate(range(a + 5 * S, b, 10 * S)):
                if n == "b3" and P["on"] <= t <= P["off"]:
                    continue
                lost = len(stops) if n == "b3" and i == 0 and t > P["off"] else 0
                nline(n, "node.jsonl", t, r="stats", leases_lost=lost, isolations=0,
                      ownership={"held": "10", "clock behind": "0"}, acks=bisect.bisect_right(acks, t + off[n]),
                      give_ups=0, fsync_us={"n": 90, "p50": 700, "p99": 1500, "max": 3000},
                      ticks={"accounts": 100 * (j + 1), "streams": 200 * (j + 1)})
                nline(n, "node.jsonl", t, r="res", **RES)
    files["store.out"] = ["store on 127.0.0.1:7900, log store.jsonl"]
    for extra in SYNTH_EXTRAS:
        extra(files, info, lives, nline)
    info["meta"] = dict(run=run, seed=seed, mode="selftest", commit="synthetic", dirty=False, lease_secs=T,
                        acceptors=nodes, nodes=nodes, plan_sha256="-", plan_version=max(nemesis.PLANS), one_box=False,
                        params={"load": "ledger", "keys": str(keys), "every_ms": "5000"})
    return files, info


def synth_streams(nline, lives, E):
    """Each node originates <node>/s0 and s1 and follows the others'. b2's followers resume 1.5 s after its kill ends;
    b3's streams, and b3's own followers, are silent through its pause, longer than B, which an aggregate line of b1
    sees open."""
    K, P = E[0], E[1]
    for n, ls in lives.items():
        for g, (a, b) in enumerate(ls):
            for s in ("s0", "s1"):
                nline(n, "stream.jsonl", a + 100 * MS, r="open", s="%s/%s" % (n, s), g=g + 1)
            for t in range(a + 5 * S, b, 10 * S):
                if n == "b3" and P["on"] <= t <= P["off"]:
                    continue
                quiet = {"b3/s0": (t - P["on"]) // MS, "b3/s1": (t - P["on"]) // MS} if n != "b3" and P["on"] < t < P[
                    "off"] + S else {}
                nline(n, "stream.jsonl", t, r="streams", recv=800 - 100 * len(quiet), gaps=0, dups=0, reorders=0,
                      resets=0, origin_lost=0, max_lag_ms=3, quiet=quiet)
    for n, origin, back, frm in (("b1", "b2", K["off"] + 1500 * MS, K["on"]), ("b3", "b2", K["off"] + 1500 * MS, K["on"]),
                                 ("b1", "b3", P["off"] + S, P["on"]), ("b2", "b3", P["off"] + S, P["on"]),
                                 ("b3", "b1", P["off"] + 500 * MS, P["on"]), ("b3", "b2", P["off"] + 500 * MS, P["on"])):
        for s in ("s0", "s1"):
            nline(n, "stream.jsonl", back, r="resume", s="%s/%s" % (origin, s), quiet_ms=(back - frm) // MS)


def synth_disk(files, info, lives, nline):
    """Each node originates disk-<node>/s0 and s1, opened at each start in generation 1 after what its disk holds,
    and reports its disk every 10 s, within its cap."""
    P, cap = info["E"][1], 2 * ((8 << 20) + (128 << 10)) + (256 << 10)
    for n, ls in lives.items():
        for j, (a, b) in enumerate(ls):
            for s in ("s0", "s1"):
                nline(n, "disk.jsonl", a + 150 * MS, r="open", s="disk-%s/%s" % (n, s), g=1,
                      stored=[1, 1000 * j] if j else None, at=1000 * j, inc=j + 1, truncated=0)
            for t in range(a + 5 * S, b, 10 * S):
                if not (n == "b3" and P["on"] <= t <= P["off"]):
                    nline(n, "disk.jsonl", t, r="disk", streams=2, bytes=min(cap // 2, (t - info["T0"]) // 20), cap=cap,
                          unwritten=0, writes=(t - a) // (5 * MS), write_failures=0, refused=0, failing=None,
                          error=None, truncated=0)


def synth_surface(files, info, lives, nline):
    """Every node probed each second while it lives, answered 200 but while b3 is paused (timeouts), and scraped every
    10 s, its counter from 0 each incarnation; blob, local and dead lines every 10 s, with errors and an unexplained
    dead letter only near an episode, and no local line across b3's pause; three passes of a CA rotation, each
    node's `tls` line 100 ms after each and at each boot the pass in force; a handshake failure counted across b2's
    kill."""
    E, T0 = info["E"], info["T0"]
    K, P = E[0], E[1]
    busy = [(e["on"], e["off"] + 8 * S) for e in E]  # within B (10 s here) of each, so never in a quiet window

    def near(a, b):
        return any(s < b and a <= e for s, e in busy)  # a window that ends as an episode begins is quiet

    rot = [T0 + s * S for s in (30, 50, 250)]
    files["tls.jsonl"] = [{"t": t, "done": t + 300 * MS, "pass": k} for k, t in enumerate(rot, 1)]
    for n, ls in lives.items():
        for a, b in ls:
            nline(n, "node.jsonl", a - 50 * MS, r="tls", err=None, at="boot",
                  **{"pass": "p%d" % sum(t + 300 * MS <= a for t in rot)})
            for k, t in enumerate(rot, 1):
                if a <= t < b:
                    nline(n, "node.jsonl", t + 100 * MS, r="tls", err=None, at="hup", **{"pass": "p%d" % k})
            for j, s in enumerate(range(a + S, b - S, S)):
                paused = n == "b3" and P["on"] <= s <= P["off"]
                for path in ("/readyz", "/healthz") + (("/metrics",) if j % 10 == 0 else ()):
                    x = dict(t=s + (800 if paused else 3) * MS, sent=s, node=n, path=path, code=None if paused else 200)
                    if paused:
                        x["err"] = "timeout: timed out"
                    elif path == "/metrics":
                        x["counters"] = {'axon_sent_total{peer="b1"}': 7.0 * j, "axon_turn_seconds_count": float(j),
                                        'axon_connection_calls_total{peer="b2"}': float(j % 3)}
                    else:
                        x["checks"] = {"system": "ok", "cluster": "ok", "ownership": "ok", "quotas": "ok"}
                    files["probes.jsonl"].append(x)
            for j, t in enumerate(range(a + 10 * S, b, 10 * S), 1):
                if n == "b3" and P["on"] <= t <= P["off"]:
                    continue
                err = 2 if near(t - 15 * S, t) else 0
                nline(n, "blob.jsonl", t, r="blobs", ok=30, busy=1, err=err, bytes=30 << 20, max_ms=40,
                      errs={"Timeout": err} if err else {}, ok_by={})
                if n == "b3" and P["on"] < t < P["off"] + 10 * S:
                    continue  # the window across the pause, its timer late: left out, so a plant may undo the pause
                nline(n, "local.jsonl", t, r="local", timer_ticks=100, late_max_ms=3,
                      published=1000 * j, told=8000 * j, got=[1000 * j] * 4, gaps=0, dups=0, flaky_got=4000 * j,
                      planned_fails=8 * j, restarts=8 * j - j % 2, families=4, asks_ok=100 * j, asks_err=0)
                by = {"not_ready publish rig.local.flaky": 1, "actor_failed u64 rig.local.flaky-2": 1}
                if near(t - 25 * S, t - 5 * S):
                    by["closed tell accounts/k1"] = 3
                nline(n, "node.jsonl", t - 5 * S, r="dead", by=by, total=4 * j)
    for n in lives:
        for x in files[n + "/node.jsonl"]:
            if x["r"] == "up":
                x["workloads"] = x["workloads"] + ["blob", "local"]
            if x["r"] == "stats":
                x.setdefault("net", {})["handshake_failures"] = [int(n == "b1" and x["t"] > K["on"]), 0, 0, 0, 0]


def peer_synth(files, info, lives, nline):
    """Two clean peer episodes after the others: a wrong secret at b3, refused, and a stale impostor at b2, which
    connects and is sent nothing. The node's listener saw the probe: its auth failures rise through its episode."""
    # Indices 6 and 7, clear of the slot (240-262 s) and the quiet window (265 s) the selftest plants use.
    for e, node, on, off, mode, outcome in ((6, "b3", 270, 282, "secret", "refused"),
                                            (7, "b2", 285, 295, "stale", "connected")):
        add_episode(files, info, e, "peer", [node], on, off, mode=mode)
        files["nemesis.jsonl"] += [dict(t=info["T0"] + (off + 3) * S, ev="check", i=e, node=n, rc=0, out="",
                                        forced=False) for n in lives]
        for s in range(on, off):
            nline(node, "peer.jsonl", info["T0"] + s * S, mode=mode, outcome=outcome, frames=0,
                  err="auth" if outcome == "refused" else None,
                  started=info["T0"] + s * S - 50 * MS)
        for x in files[node + "/node.jsonl"]:  # only in the episode, so handshake-quiet's quiet windows stay flat
            if x.get("r") == "stats":
                into = min(max(0, x["t"] - info["T0"] - on * S), (off - on) * S) // S
                x.setdefault("net", {}).setdefault("handshake_failures", [0] * 5)[0] += into


def synth_sockets(files, info, lives, nline):
    """At each settle, every node's sockets: a connection each way per lane to each peer."""
    for x in [x for x in files["nemesis.jsonl"] if x.get("ev") == "clock" and x["offset_ms"] is not None]:
        files["nemesis.jsonl"].append(dict(t=x["t"], ev="sockets", i=x.get("i"), node=x["node"], close_wait=0,
                                           estab={n: 2 * LANES for n in lives if n != x["node"]}))


def synth_kernel(files, info, lives, nline):
    """The kernel's word for b1's journal failing under its diskdrop."""
    files["b1/kernel.jsonl"].append({"__REALTIME_TIMESTAMP": str(info["E"][2]["on"] + 2 * S), "MESSAGE":
                                     "EXT4-fs error (device dm-0): ext4_journal_check_start:84: Detected aborted journal"})


# fn(files, info, lives, nline): a workload's clean lines, added to every synthetic run. lives maps each node to its
# incarnations' (start, end) on the store's clock; nline(node, file, t, **fields) writes a line on the node's clock.
SYNTH_EXTRAS = [synth_disk, synth_surface, peer_synth, synth_sockets, synth_kernel]


def write(files, meta, d):
    for name, lines in files.items():
        os.makedirs(os.path.dirname(os.path.join(d, name)), exist_ok=True)
        with open(os.path.join(d, name), "w") as f:
            # _at: when a line under a stepped clock was written, which places it in the file
            for x in sorted(lines, key=lambda x: x.get("_at", x.get("t", 0)) if isinstance(x, dict) else 0):
                if isinstance(x, dict) and "_at" in x:
                    x = {a: b for a, b in x.items() if a != "_at"}
                if isinstance(x, dict) and "_pre" in x:
                    x = x["_pre"] + json.dumps({a: b for a, b in x.items() if a != "_pre"})
                f.write((x if isinstance(x, str) else json.dumps(x)) + "\n")
    with open(os.path.join(d, "meta.json"), "w") as f:
        json.dump(meta, f)


def last(files, k, pred=lambda x: True):
    return [x for x in files["store.jsonl"] if x["r"] == "A" and x["v"] == "ok" and x["k"] == k and pred(x)][-1]


def first_line(files, name, pred):
    return next(x for x in sorted(files[name], key=lambda x: x["t"]) if pred(x))


# (name, fn(files, info), invariants it must fail, invariants it flags in probe episodes, exit code or None for 1/0)
PLANTS = []
ANY = None  # a plant whose exit code alone is judged


def plant(name=None, want=(), probe=(), rc=None):
    """Registers a selftest case: fn(files, info) changes a clean synthetic run, which must then fail exactly `want`
    (by default [name], the function's name after `p_`) and flag `probe` in probe episodes, and exit `rc`."""
    def register(fn):
        n = name or fn.__name__[2:].replace("_", "-")
        PLANTS.append((n, fn, [n] if want == () else want, list(probe), rc))
        return fn
    return register


def at(i, t):
    """t s after T0, or (e, "on" | "off", s): s after episode e's on or off."""
    return i["E"][t[0]][t[1]] + round(t[2] * S) if isinstance(t, tuple) else i["T0"] + round(t * S)


def plant_line(name, file, t, want=(), probe=(), /, **line):
    """A plant that adds one line to file, at at(t)."""
    plant(name, want, probe)(lambda f, i: f[file].append(dict(t=at(i, t), **line)))


def plant_log(name, file, t, msg, want=()):
    """A plant that adds one line of `journalctl -o json` to file, at at(t)."""
    plant(name, want)(lambda f, i: log_line(f, file, at(i, t), msg))


def plant_episode(name, want, cls, nodes, on, off, probe=(), **args):
    """A plant that adds episode 4, cls on nodes from on to off s."""
    plant(name, want, probe)(lambda f, i: add_episode(f, i, 4, cls, nodes, on, off, **args))


def log_line(f, file, t, msg):
    f[file].append({"__REALTIME_TIMESTAMP": str(t), "MESSAGE": msg})


def unjournal(f, i, x, acked=None):
    """Record x's line gone from its writer's journal; with acked, every ack of its op a retry answered then."""
    name = "%s/journal-%s.jsonl" % (x["n"], i["run"])
    f[name] = [j for j in f[name] if j["op"] != x["op"]]
    for c in ("b1", "b2", "b3") if acked else ():
        for a in f[c + "/client.jsonl"]:
            if a["r"] == "ack" and a["op"] == x["op"]:
                a["t"] = acked


def slow_acks(f, node, ops, ms):
    for x in f[node + "/client.jsonl"]:
        if x["r"] == "ack" and x["op"] in ops:
            x["ms"] = ms


def slow_fsync(f, i, node, lo, hi):
    """node's stats lines in (lo, hi] s after T0 hold a fsync of 700 ms."""
    for x in f[node + "/node.jsonl"]:
        if x.get("r") == "stats" and i["T0"] + lo * S < x["t"] <= i["T0"] + hi * S:
            x["fsync_us"] = dict(x["fsync_us"], max=700 * MS)


@plant()
def p_fence_order(f, i):
    x = last(f, i["pk"][0])
    x["s"] += 1
    first_line(f, "%s/journal-%s.jsonl" % (x["n"], i["run"]), lambda j: j["op"] == x["op"])["s"] += 1


@plant()
def p_epoch_two_nodes(f, i):
    o = next(x for x in f["store.jsonl"] if x["r"] == "O" and x["k"] == i["pk"][1])
    f["store.jsonl"].append(dict(o, t=i["T0"] + 65 * S, n="b3" if o["n"] != "b3" else "b1", v="conflict", len=0))


@plant()
def p_epoch_two_writers(f, i):
    x = last(f, i["pk"][2])
    f["store.jsonl"].append(dict(x, t=i["T0"] + 66 * S, op="x.0.1", s=x["s"] + 7, v="conflict"))


@plant()
def p_turn_after_takeover(f, i):
    i["resumed"]["t"] += 190 * MS  # its turn began 200 ms after the resume, not 10


@plant()
def p_ack_lost(f, i):
    t = i["T0"] + 62 * S
    f["b1/client.jsonl"] += [dict(t=t, r="send", op="b1.0.999999", k="k0", d=5),
                             dict(t=t + 4 * MS, r="ack", op="b1.0.999999", k="k0", o="ok", a=5, n="b1", e=1, tries=1,
                                  ms=4.0)]


@plant()
def p_ack_mismatch(f, i):
    first_line(f, "b2/client.jsonl", lambda x: x["r"] == "ack" and x["t"] > i["T0"] + 40 * S)["a"] += 1


@plant(want=["ack-mismatch"])
def p_ack_writer(f, i):
    """A retry answered from `done` that names the answering node, not the record's writer."""
    x = first_line(f, "b2/client.jsonl", lambda x: x["r"] == "ack" and x["t"] > i["T0"] + 40 * S and x["n"] != "b2")
    x["n"] = "b2"


@plant()
def p_applied_twice(f, i):
    k = i["pk"][3]
    x, y = last(f, k), next(x for x in f["store.jsonl"] if x["r"] == "A" and x["v"] == "ok" and x["k"] == k)
    for n in f:
        if n.endswith(".jsonl") and "/" in n:
            f[n] = [y for y in f[n] if y.get("op") != x["op"]]
    x["op"] = y["op"]


@plant()
def p_phantom(f, i):
    x = first_line(f, "b2/client.jsonl", lambda x: x["r"] == "send" and x["t"] > i["T0"] + 45 * S)
    f["b2/client.jsonl"].remove(x)


@plant(want=["phantom"])
def p_phantom_d(f, i):
    """A record whose amount is not the one its client sent."""
    x = first_line(f, "b2/client.jsonl", lambda x: x["r"] == "send" and x["t"] > i["T0"] + 45 * S)
    x["d"] += 1


@plant()
def p_chain(f, i, x=None):
    x = x or last(f, i["pk"][4], lambda x: x["o"] == "ok")
    x["a"] += 7
    for c in (c for n, c in f.items() if n.endswith(".jsonl")):
        for y in c:
            if (y.get("r") == "ack" or "rtt" in y) and y["op"] == x["op"]:  # its acks, and its journal line
                y["a"] += 7


@plant()
def p_journal_missing(f, i):
    name = "b2/journal-%s.jsonl" % i["run"]
    f[name].remove(first_line(f, name, lambda x: x["t"] > i["T0"] + 50 * S))


@plant(want=["journal-missing"])
def p_ack_before_fsync(f, i):
    """An op b2 wrote and was acked for about 0.9 s before its kill, with no journal line: the reply went out before
    the fsync, which the waiver for a kill between the store's OK and the fsync must not hide."""
    K = i["E"][0]
    acked = {x["op"]: x["t"] for c in ("b1", "b3") for x in f[c + "/client.jsonl"] if x["r"] == "ack"}
    unjournal(f, i, next(x for x in f["store.jsonl"] if x["r"] == "A" and x["n"] == "b2" and K["on"] - S <= x["t"]
                         and acked.get(x["op"], K["on"]) < K["on"] - 800 * MS))


@plant(want=["journal-missing"])
def p_ack_before_fsync_stepped(f, i):
    """ack_before_fsync with the ack on b1, whose clock ran 30 s ahead for the second around it, then stepped back:
    written in the seconds the step back replays, write order places it before the kill, not 30 s after
    (pairs-20261001-2342-s1002, episode 239)."""
    K = i["E"][0]
    acked = {x["op"]: x["t"] for x in f["b1/client.jsonl"] if x["r"] == "ack"}
    x = next(x for x in f["store.jsonl"] if x["r"] == "A" and x["n"] == "b2" and K["on"] - S <= x["t"]
             and acked.get(x["op"], K["on"]) < K["on"] - 800 * MS)
    unjournal(f, i, x)
    a = acked[x["op"]]
    add_episode(f, i, 4, "step", ["b1"], (a - i["T0"]) / S - 0.5, (a - i["T0"]) / S + 0.5, step_s=30)
    for y in f["b1/client.jsonl"]:
        if "t" in y and a - 500 * MS <= y["t"] < a + 500 * MS:
            y["_at"], y["t"] = y["t"], y["t"] + 30 * S


@plant(want=[])
def p_killed_under_step(f, i):
    """A b2 record the store took just before the skew-restart's kill, with no journal line and its ack a retry 500 ms
    after the kill: b2's `up` and journal lines are on its stepped clock, so only the nemesis's kill explains it."""
    kill = i["E"][3]["on"] + 50 * MS
    unjournal(f, i, [x for x in f["store.jsonl"] if x["r"] == "A" and x["n"] == "b2" and kill - S < x["t"] <= kill][-1],
              kill + 500 * MS)


@plant(want=[])
def p_killed_under_slowdisk(f, i):
    """A slowdisk of b2's journal from 4 s before its kill to 1 s before, a b2 record the store took 3 s before the
    kill with no journal line, and its ack a retry 500 ms after the kill: the suspend of the table swap held the
    writer, whose later lines came before the kill (chaos-20260930-2245-s47, b3 at 370.9 s)."""
    kill = i["E"][0]["on"]  # 80 s
    add_episode(f, i, 4, "slowdisk", ["b2"], 76, 79, dev="journal", delay_ms=245)
    slow_fsync(f, i, "b2", 76, 90)
    unjournal(f, i, next(x for x in f["store.jsonl"] if x["r"] == "A" and x["n"] == "b2" and kill - 3 * S <= x["t"]),
              kill + 500 * MS)


@plant(want=[])
def p_killed_while_starved(f, i, client="b"):
    """A cpu quota on b2 from 4 s before its kill, a b2 record the store took 3 s before the kill with no journal line
    while b2 journals others up to the kill, and its ack a retry 500 ms after: the starved writer ran behind."""
    kill = i["E"][0]["on"]  # 80 s
    add_episode(f, i, 4, "cpu", ["b2"], 76, 81, quota_pct=3)
    x = next(x for x in f["store.jsonl"] if x["r"] == "A" and x["n"] == "b2" and kill - 3 * S <= x["t"]
             and x["op"].startswith(client))
    unjournal(f, i, x, kill + 500 * MS)
    return kill


@plant(want=[])
def p_killed_while_starved_stepped(f, i):
    """killed_while_starved with the retry's ack on b1, whose clock stepped back 2 s from 0.5 s before the kill to 3 s
    after, so that the ack, written after the step, reads 1.5 s before the kill (pairs-20261001-2342-s1002, b2's acks
    of b3's records at 3297 s, the takeover-step's second node)."""
    kill = p_killed_while_starved(f, i, "b1.")
    add_episode(f, i, 5, "step", ["b1"], 79.5, 83, step_s=-2)
    for x in f["b1/client.jsonl"]:
        if "t" in x and kill - 500 * MS <= x["t"] < kill + 3 * S:
            x["_at"], x["t"] = x["t"], x["t"] - 2 * S


def plant_flaky(name, want, off, up_s, down_s):
    """A flakey table on b2's journal from 240 s, reset at its off, through which b2 writes on and logs no journal_err:
    judged only where it was down 2 s."""
    def fn(f, i):
        add_episode(f, i, 4, "flaky", ["b2"], 240, off, dev="journal", up_s=up_s, down_s=down_s)
        f["nemesis.jsonl"].append({"ev": "journal-reset", "node": "b2", "t": i["T0"] + off * S + 100 * MS, "i": 4})
    plant(name, want)(fn)


# b2's streams disk full from just before its kill to just before its restart, whose opens come within 1 s of the off.
plant_episode("diskfull-killed", [], "diskfull", ["b2"], 79.95, 83.5, dev="streams")
plant_flaky("flaky-brief", [], 246, 5, 1)
plant_flaky("flaky-down", ["fault-effect"], 250, 2, 3)


@plant(want=["heartbeat", "turn-after-takeover"])
def p_noop_pause(f, i):
    """fault.sh found b3's unit not running when the pause went on (the nemesis logs it as noop): nothing on b3
    resumed, so the refusal of the turn 10 ms after the resume is no longer explained, nor b3's stats lines' gap."""
    for x in f["nemesis.jsonl"]:
        if x.get("ev") == "on" and x.get("class") == "pause":
            x["noop"] = ["b3"]


def delay_ack(f, node, lo, hi, until):
    s = first_line(f, node + "/client.jsonl", lambda x: x["r"] == "send" and lo <= x["t"] <= hi)
    a = next(x for x in f[node + "/client.jsonl"] if x["r"] == "ack" and x["op"] == s["op"])
    a["t"], a["ms"] = until(s["t"]), (until(s["t"]) - s["t"]) / MS


@plant()
def p_liveness_after(f, i):
    end = i["E"][1]["off"] + 40 * MS
    delay_ack(f, "b1", end + S, end + 5 * S, lambda t: t + 12 * S)


@plant(want=[])
def p_liveness_after_caught(f, i):
    """An op b1 sent in the last second of episode 1, acked 12 s after its end, caught in flight by a dup of b3 from 3
    to 20 s after that end: due B after the dup's."""
    end = i["E"][1]["off"] + 40 * MS
    add_episode(f, i, 4, "dup", ["b3"], (end - i["T0"]) // S + 3, (end - i["T0"]) // S + 20, dup_pct=10)
    delay_ack(f, "b1", end - S, end, lambda t: end + 12 * S)


@plant()
def p_liveness_during(f, i):
    on = i["E"][0]["on"]
    delay_ack(f, "b1", on + S // 2, on + 2 * S, lambda t: on + 40 * MS + 9 * S)


def bump(f, node, frm, counter="leases_lost"):
    for x in f[node + "/node.jsonl"]:
        if x["r"] == "stats" and x["t"] >= frm:
            x[counter] = x.get(counter, 0) + 1


plant("io-lease-lost")(lambda f, i: bump(f, "b1", i["E"][2]["on"] + 5 * S))


@plant(want=[])
def p_io_then_cut(f, i):
    """Leases lost in another episode of b1 that began 1 s after its diskdrop's reset: the second's doing
    (pairs-20260930-2317-s49, a partition 2 s after a diskhang)."""
    add_episode(f, i, 4, "dup", ["b1"], 173, 179)
    bump(f, "b1", i["T0"] + 174 * S)


plant("mass-drop")(lambda f, i: f["b1/node.jsonl"].extend(
    dict(t=i["T0"] + 62 * S + n * 10 * MS, r="stop", k="k%d" % n, e=1, why="Fenced") for n in range(20)))


plant("lease-lost-quiet")(lambda f, i: bump(f, "b2", i["T0"] + 265 * S))


@plant()
def p_latency_quiet(f, i):
    lo, hi = i["T0"] + 60 * S, i["T0"] + 70 * S
    slow_acks(f, "b3", {x["op"] for x in f["b3/client.jsonl"] if x["r"] == "send" and lo <= x["t"] < hi}, 1500.0)


@plant(want=[])
def p_latency_quiet_straddle(f, i):
    """The ops b2 sent in the last 500 ms of a quiet window, in flight when a dup of b2 began 200 ms after it, acked
    1 s after the dup's end: the episode's latency, not the window's."""
    add_episode(f, i, 4, "dup", ["b2"], 250.2, 256, dup_pct=10)
    lo, hi = i["T0"] + 249.5 * S, i["T0"] + 250 * S
    for x in f["b2/client.jsonl"]:
        if x["r"] == "send" and lo <= x["t"] < hi:
            delay_ack(f, "b2", x["t"], x["t"], lambda t: i["T0"] + 257 * S)


@plant(want=[], probe=["clock-behind", "turn-after-takeover"])
def p_clock_behind(f, i):
    k = i["pk"][5]
    f["store.jsonl"].append(dict(t=i["E"][3]["on"] + 5 * S, r="O", k=k, e=1, n="b2", lag=1000, v="stale",
                                 f=max(x["e"] for x in f["store.jsonl"] if x["k"] == k), len=0))


@plant(want=["chain"])
def p_chain_in_probe(f, i):
    """A store invariant broken on b1's key during the b2 b3 skew-restart still fails the run."""
    on = i["E"][3]["on"] + 5 * S
    p_chain(f, i, next(x for x in f["store.jsonl"]
                       if x["r"] == "A" and x["n"] == "b1" and x["o"] == "ok" and x["t"] > on))


@plant(want=["fence-order"])
def p_fence_order_own(f, i):
    """An append accepted in an epoch below a newer owner's accepted own."""
    x = last(f, i["pk"][0])
    f["store.jsonl"].append(dict(t=x["t"] - 1, r="O", k=x["k"], e=x["e"] + 1000, n=x["n"], lag=200, v="ok",
                                 f=x["e"] + 1000, len=x["s"]))


RES = dict(rss_kb=50000, anon_kb=40000, mem_kb=90000, threads=20, fds=100, actors=17, sessions=2)


def res_hours(f, i, rss, hours=3, threads=lambda m: 20):
    """b1's `res` lines for `hours` in one incarnation, rss_kb and anon_kb following rss(minutes since its up)."""
    t0 = i["T0"]
    f["b1/node.jsonl"] = [x for x in f["b1/node.jsonl"] if x["r"] != "res"]
    for t in range(t0 + 5 * S, t0 + hours * 3600 * S, 10 * S):
        m = (t - t0) / 60 / S
        kb = int(rss(m))
        f["b1/node.jsonl"].append(dict(RES, t=t, r="res", rss_kb=kb, anon_kb=kb - 10240, threads=threads(m)))


@plant()
def p_growth(f, i):
    """A leak of 1 MiB a minute for three hours."""
    res_hours(f, i, lambda m: 61440 + 1024 * m)


@plant(want=["growth"])
def p_growth_restarts(f, i):
    """A leak of 5 MiB a minute that starts again with each of three incarnations of an hour: judged apart, not as
    one saw-tooth."""
    for h in (1, 2):
        t = i["T0"] + h * 3600 * S
        f["b1/node.jsonl"].append(dict(t=t, r="up", node="b1", inc=h + 1, workloads=["streams"]))
        f["nemesis.jsonl"].append(dict(t=t + 5 * S, ev="heal", i=3, node="b1", rc=0, out="", stderr=""))
    res_hours(f, i, lambda m: 61440 + 5120 * (m % 60))


@plant(want=[])
def p_warmup(f, i):
    """No leak: a cache that fills 300 MiB over two hours, levelling off (more than the limit, but no longer rising in
    the last quarter), a 30 MiB step at 90 min (keys taken over) that stays, a 40 MiB spike for two samples, noise;
    and 18 threads more from 140 min."""
    res_hours(f, i, lambda m: 30720 + 307200 * (1 - math.exp(-m / 40)) + 30720 * (m >= 90)
              + 40960 * (120 <= m < 120.3) + 512 * (int(m * 6) % 7 - 3), threads=lambda m: 20 + 18 * (m >= 140))


@plant(want=[])
def p_threads_pool(f, i):
    """b5 of the night of 2026-09-27: tokio's blocking pool stepping from 17 threads to 521 and staying, under 512 over
    the 9 of the node's first incarnation."""
    res_hours(f, i, lambda m: 61440, threads=lambda m: 9 if m < 1 else [17, 284, 419, 521][min(3, int(m / 45))])


@plant(want=["growth"])
def p_threads_leak(f, i):
    """Five threads a minute for three hours: 900 more, past what the blocking pool can hold."""
    res_hours(f, i, lambda m: 61440, threads=lambda m: 20 + 5 * int(m))


@plant(want=ANY, rc=2)
def p_unreadable(f, i):
    """Client and node lines naming their event in a field the checker does not read."""
    for name, lines in f.items():
        if name.endswith(("client.jsonl", "node.jsonl")):
            for x in lines:
                x["ev"] = x.pop("r")


for _r, _seq, _last in (("dup", 40, 41), ("reorder", 40, 42), ("gap", 43, 40)):
    plant_line("stream-" + _r, "b1/stream.jsonl", 61, r=_r, s="b2/s0", g=1, start="live", seq=_seq, last=_last)


@plant()
def p_stream_liveness(f, i):
    """b1 hears nothing of b3/s1 from 2 s after the pause ends until 14 s after: past B, before the next episode."""
    f["b1/stream.jsonl"].append(dict(t=i["E"][1]["off"] + 14 * S, r="resume", s="b3/s1", quiet_ms=12000))


@plant(want=["stream-liveness"])
def p_stream_liveness_open(f, i):
    """b2's aggregate lines see b1/s0 silent from 1 s after the pause ends, and it never resumes."""
    frm, until = i["E"][1]["off"] + S, i["E"][2]["on"]
    for x in f["b2/stream.jsonl"]:
        if x["r"] == "streams" and frm < x["t"] < until:
            x["quiet"] = {"b1/s0": (x["t"] - frm) // MS}


# b1 silent on b3/s1 from 25 s to 60 s, past B in the steady time before the first episode.
plant_line("stream-liveness-steady", "b1/stream.jsonl", 60, ["stream-liveness"], r="resume", s="b3/s1", quiet_ms=35000)
# b1 silent on b2/s0 from 5 s before the diskdrop to 8 s into it: the next episode began inside B.
plant_line("stream-next-episode", "b1/stream.jsonl", (2, "on", 8), [], r="resume", s="b2/s0", quiet_ms=13000)
# b3, under skew-restart, silent on b1/s0 past B after it: its lines are off the timeline, as for keys.
plant_line("stream-follower-clock", "b3/stream.jsonl", (3, "off", 20), [], r="resume", s="b1/s0", quiet_ms=19000)
plant_line("stream-gap-in-probe", "b1/stream.jsonl", (3, "on", 5), ["stream-gap"], r="gap", s="b2/s0", g=1, seq=43,
           last=[1, 40], start="after")


@plant("stream-origin-down", want=[])
def p_stream_liveness_origin_down(f, i):
    """p_stream_liveness while b3 logs nothing from the pause's end on: its streams' silence is not a follower's."""
    p_stream_liveness(f, i)
    f["b3/node.jsonl"] = [x for x in f["b3/node.jsonl"] if x["r"] != "stats" or x["t"] < i["E"][1]["off"]]


def nem_line(f, **kw):
    return next(x for x in f["nemesis.jsonl"] if all(x.get(k) == v for k, v in kw.items()))


def add_episode(f, i, e, cls, nodes, on, off, noop=(), **args):
    """An episode the nemesis applied and the plan held, on and off at on and off s; noop, the nodes where fault.sh
    found nothing to act on."""
    nem_fault(f, i, e, 0, cls, nodes, on, off, args, noop)
    on, off = i["T0"] + on * S, i["T0"] + off * S
    f["plan.jsonl"].append(dict(i=e, at=(on - i["T0"]) // S, dur=(off - on) // S,
                                faults=[{"class": cls, "nodes": nodes, "args": args}]))


def nem_fault(f, i, e, j, cls, nodes, on, off, args, noop=()):
    """The nemesis's on and off lines of fault j of episode e, at on and off s."""
    for ev, t in (("on", i["T0"] + on * S), ("off", i["T0"] + off * S)):
        f["nemesis.jsonl"].append({"class": cls, "t": t + 40 * MS, "ev": ev, "i": e, "id": "%d.%d" % (e, j),
                                   "nodes": nodes, "args": args, "sent": t, "done": t + 40 * MS, "rc": 0, "stderr": "",
                                   **({"noop": list(noop)} if noop else {})})


@plant()
def p_nemesis(f, i):
    """The pause's off failed: fault.sh exited 1."""
    x = nem_line(f, ev="off", i=1)
    x["rc"], x["stderr"] = 1, "Failed to kill unit"


# The diskdrop was never turned off, nor its disk reset.
plant("nemesis-unapplied", ["nemesis"])(lambda f, i: f["nemesis.jsonl"].remove(nem_line(f, ev="off", i=2)))


@plant(want=["nemesis"])
def p_nemesis_forced(f, i):
    """b3's check after the pause was not clean, and its heal ended 2 s into the diskdrop: the two episodes overlap
    and are judged as one, which fails nothing else."""
    t = i["E"][1]["off"] + 3 * S
    nem_line(f, ev="check", i=1, node="b3").update(rc=1, forced=True, out="stopped")
    f["nemesis.jsonl"].append(dict(t=i["E"][2]["on"] + 2 * S, ev="heal", i=1, node="b3", rc=0, out="", stderr=""))
    assert t < i["E"][2]["on"]


@plant(want=["nemesis"])
def p_nemesis_slow(f, i):
    """The kill's on took 3 s."""
    x = nem_line(f, ev="on", i=0)
    x["done"] = x["t"] = x["sent"] + 3 * S


# The kill held 3 s where the plan said 4.
plant("nemesis-args", ["nemesis"])(lambda f, i: f["plan.jsonl"][0]["faults"][0].update(args={"hold_s": 4}))


@plant(want=["nemesis"])
def p_nemesis_id(f, i):
    """The kill's on and off name fault 0.1, where the plan's episode 0 holds only 0.0."""
    for x in f["nemesis.jsonl"]:
        if x.get("i") == 0 and x.get("ev") in ("on", "off"):
            x["id"] = "0.1"


@plant(want=["nemesis", "fault-effect"])
def p_nemesis_version(f, i):
    """An episode of a class plan version 1 does not have, in a run that planned version 1; b1 logging through the
    power cut contradicts it too."""
    i["meta"]["plan_version"] = 1
    add_episode(f, i, 4, "powercut", ["b1"], 240, 250)


@plant(want=["nemesis"])
def p_nemesis_one_box(f, i):
    """A step of b2's clock in a run on one box, whose clock is every node's."""
    i["meta"]["one_box"] = True
    add_episode(f, i, 4, "step", ["b2"], 240, 250, step_s=2)


@plant(want=[])
def p_nemesis_role(f, i):
    """p_noop_kill planned on the owner of k1 among b1 and b2: it went on b1, which the role may pick."""
    p_noop_kill(f, i)
    f["plan.jsonl"][-1]["faults"][0].update(nodes=["@owner:k1"], among=["b1", "b2"])


@plant(want=["nemesis"])
def p_nemesis_role_outside(f, i):
    """The same where the role may pick only b2 or b3: the kill went on a node its plan did not allow."""
    p_noop_kill(f, i)
    f["plan.jsonl"][-1]["faults"][0].update(nodes=["@owner:k1"], among=["b2", "b3"])


@plant(want=[])
def p_noop_kill(f, i):
    """A kill of b1 that fault.sh found nothing to act on (noop): b1 logging through it contradicts nothing."""
    add_episode(f, i, 4, "kill", ["b1"], 240, 250, noop=["b1"], hold_s=10)


# A diskdrop of b1's streams disk, reset at its off: b1's journal keeps every line, and that is no evidence against it
# nor a missing reset.
plant("streams-diskdrop", [])(lambda f, i: streams_fault(f, i, "diskdrop", "b1"))


# b3 logged a line 5 s into its pause.
plant_line("fault-effect", "b3/stream.jsonl", (1, "on", 5), r="resume", s="b1/s0", quiet_ms=100)
# b3's clients' ops kept being written on b1 and b2 through its isolate, and its /readyz said ready.
plant_episode("fault-effect-cut", ["fault-effect", "readyz-isolated"], "isolate", ["b3"], 240, 250)
plant_episode("fault-effect-oneway", ["fault-effect"], "oneway", ["b3"], 240, 250, to=["b1", "b2"])
plant_episode("fault-effect-oneway-back", [], "oneway", ["b1"], 240, 250, to=["b3"])
# b1's fsyncs stayed at 1.5 ms through a slowdisk of 200 ms.
plant_episode("fault-effect-fsync", ["fault-effect"], "slowdisk", ["b1"], 240, 262, delay_ms=200)


def as_class(f, e, cls, args, sub=None, nodes=None):
    """Episode e's on and off lines and its plan as a fault of cls with args, on nodes where given, each step by
    `sub`."""
    new = {"class": cls, "args": args, **({"nodes": nodes} if nodes else {})}
    for x in f["nemesis.jsonl"]:
        if x.get("i") == e and x.get("ev") in ("on", "off"):
            x.update(new, **({"sub": sub} if sub else {}))
    f["plan.jsonl"][e]["faults"][0].update(new)


def as_roll(f, builds):
    """The skew-restart as a roll of b2 and b3, which restart 3 s into it, its first upgrade taking 20 s; each node's
    ups name its last builds of `builds`."""
    as_class(f, 3, "roll", {"gap_s": 5}, sub="upgrade")
    on = nem_line(f, ev="on", i=3)
    on["done"] = on["t"] = on["sent"] + 20 * S
    for n in ("b2", "b3"):
        ups = [x for x in f[n + "/node.jsonl"] if x["r"] == "up"]
        for x, b in zip(ups, builds[-len(ups):]):
            x["build"] = b


# A roll whose nodes come back on v2, an upgrade step of 20 s: their restarts explained, the step in its bound; and one
# whose nodes come back on the build they left.
plant("roll", [])(lambda f, i: as_roll(f, ["v1", "v1", "v2"]))
plant("fault-effect-roll", ["fault-effect"])(lambda f, i: as_roll(f, ["v1", "v1", "v1"]))


def as_drift(f, lease_ms):
    """b2's kill as a cfgdrift of its lease to 4 s; its up after it says lease_ms."""
    as_class(f, 0, "cfgdrift", {"var": "LEASE_SECS", "value": 4})
    ups = [x for x in f["b2/node.jsonl"] if x["r"] == "up"]
    for x in ups:
        x["lease_ms"] = 2000
    ups[1]["lease_ms"] = lease_ms


plant("cfgdrift", [])(lambda f, i: as_drift(f, 4000))
plant("fault-effect-cfgdrift", ["fault-effect"])(lambda f, i: as_drift(f, 2000))


@plant(want=[])
def p_leave_minority(f, i):
    """b2's kill as a leave-minority of b2 and b3, b2 the leaver: b3, on its side of the cut, need not restart."""
    as_class(f, 0, "leave-minority", {"leaver": "b2", "other": []}, nodes=["b2", "b3"])


@plant(want=["fault-effect"])
def p_fault_effect_leave_minority(f, i):
    """p_leave_minority with b3 the leaver, which never left."""
    p_leave_minority(f, i)
    as_class(f, 0, "leave-minority", {"leaver": "b3", "other": []})


def net_stats(f, node, redial=None):
    """b-node stats lines with the net counters, one more dial on each from redial on."""
    for x in f[node + "/node.jsonl"]:
        if x["r"] == "stats":
            x.setdefault("net", {}).update(dials=4 + (redial is not None and x["t"] >= redial), dial_failures=0,
                                           accepted=4)


@plant(want=["fault-effect"])
def p_fault_effect_redial(f, i):
    """A ports fault on b2 from 240 s to 250 s, its sessions reset, and no node's dials or accepts moved."""
    add_episode(f, i, 4, "ports", ["b2"], 240, 250, n=4)
    net_stats(f, "b2")


@plant("fault-effect-redialed", want=[])
def p_fault_effect_redialed(f, i):
    """The same, and b2 dialled again 5 s in."""
    add_episode(f, i, 4, "ports", ["b2"], 240, 250, n=4)
    net_stats(f, "b2", i["T0"] + 245 * S)


# b3's packets to b1 delayed 200 ms, while their ops took as long as ever.
plant_episode("fault-effect-asym", ["fault-effect"], "asym", ["b3"], 240, 262, delay_ms=200, to=["b1"])


@plant("fault-effect-asym-peer", want=[])
def p_fault_effect_asym_peer(f, i):
    """b3's packets to b2 delayed 200 ms from 240 s to 262 s, its ops written by b2 taking 250 ms more, and its more
    numerous ops written by b1 as fast as ever: judged on all of b3's ops, the delay would be contradicted."""
    add_episode(f, i, 4, "asym", ["b3"], 240, 262, delay_ms=200, to=["b2"])
    for x in f["b3/client.jsonl"]:
        if x["r"] == "ack" and x["n"] == "b2" and i["T0"] + 240 * S <= x["t"] <= i["T0"] + 262 * S:
            x["ms"] += 250


# b3's seed port cut from b1 and b2, while ops crossed on 7400: no cut to contradict.
plant_episode("fault-effect-portcut-7001", [], "portcut", ["b3"], 240, 250, port=7001, to=["b1", "b2"])


@plant(want=[])
def p_fault_effect_fsync_short(f, i):
    """A slowdisk of 200 ms on b1 for 6 s, under 1 % of the fsyncs of any stats line: only their max shows it."""
    add_episode(f, i, 4, "slowdisk", ["b1"], 240, 246, delay_ms=200)
    slow_fsync(f, i, "b1", 240, 256)


@plant(want=[])
def p_fault_effect_on_late(f, i):
    """A slowdisk on b1 from 240 s to 246 s whose on was done 5.8 s after it was sent, with no fsync slow: it held
    0.2 s (pairs-20260930-2317-s49, a table swap under memhigh)."""
    add_episode(f, i, 4, "slowdisk", ["b1"], 240, 246, delay_ms=200)
    x = nem_line(f, ev="on", i=4)
    x["done"] = x["t"] = x["sent"] + 5800 * MS


@plant(want=["journal-missing"])
def p_killed_elsewhere(f, i):
    """b2's kill as the first step of an acceptors-rolling of b2 and b3, b3's step a noop 1 s later; a b3 record taken
    just before b2's kill, acked after it, has no journal line: a kill of b2 does not explain b3's journal."""
    as_class(f, 0, "acceptors-rolling", {"gap_s": 1}, sub="kill", nodes=["b2", "b3"])
    on = nem_line(f, ev="on", i=0)
    on["targets"] = ["b2"]
    f["nemesis.jsonl"].append(dict(on, targets=["b3"], noop=["b3"], sent=on["sent"] + S, done=on["done"] + S,
                                   t=on["t"] + S))
    x = [x for x in f["store.jsonl"] if x["r"] == "A" and x["n"] == "b3" and on["done"] - S < x["t"] <= i["E"][0]["on"]]
    unjournal(f, i, x[-1], on["done"] + 500 * MS)


@plant(want=["fault-effect"])
def p_fault_effect_fsync_noise(f, i):
    """The same 6 s slowdisk, b1's fsyncs untouched while it held: one slow fsync in the next line, which it held 1 s
    of 10, is that line's noise."""
    add_episode(f, i, 4, "slowdisk", ["b1"], 240, 246, delay_ms=200)
    slow_fsync(f, i, "b1", 250, 256)


# b1 started again at 298 s, with no fault on it.
plant("unexpected-restart")(lambda f, i: f["b1/node.jsonl"].append(dict(t=i["T0"] + 298 * S, r="up", node="b1",
                                                                        inc=(i["T0"] + 298 * S) // MS)))


plant_log("unexpected-restart-panic", "b1/unit.jsonl", 100,
          "thread 'tokio-runtime-worker' panicked at src/streams.rs:1:1:", ["unexpected-restart"])
# A SIGKILL in no kill, oom or reset.
plant_log("unexpected-restart-killed", "b1/unit.jsonl", 100,
          "rig-node@b1.service: Main process exited, code=killed, status=9/KILL", ["unexpected-restart"])


plant("shutdown-bounded")(lambda f, i: next(x for x in f["b2/node.jsonl"] if x["r"] == "down").update(
    err="shutdown: 1 actor did not stop"))


@plant(want=["peer-slots"])
def p_peer_slots(f, i):
    """b3 counted a peer unreachable while only the probe hit it."""
    xs = [x for x in f["b3/node.jsonl"] if x.get("r") == "stats" and i["T0"] + 271 * S < x["t"] < i["T0"] + 290 * S]
    xs[-1]["became_unreachable"] = 1


@plant(want=["fault-effect"])
def p_fault_effect_peer(f, i):
    """b2's listener counters stood still through the stale impostor's attempts."""
    for x in f["b2/node.jsonl"]:
        if x.get("r") == "stats":
            x["net"] = {"handshake_failures": [0]}


# b2 sent the stale impostor a frame.
plant("peer-impostor")(lambda f, i: next(x for x in f["b2/peer.jsonl"] if x.get("mode") == "stale").update(frames=1))


@plant(want=["peer-impostor"])
def p_peer_wrong_secret(f, i):
    """b3 counted the wrong secrets as some other handshake failure, not auth."""
    for x in f["b3/node.jsonl"]:
        if x.get("r") == "stats":
            v = x["net"]["handshake_failures"]
            v[0], v[1] = 0, v[0]


@plant(want=[])
def p_peer_secret_timeout(f, i):
    """b3 timed the wrong secrets out (paused, starved) instead of refusing them as auth: not peer-impostor's to judge."""
    for x in f["b3/peer.jsonl"]:
        x["err"] = "timeout_handshake"
    for x in f["b3/node.jsonl"]:
        if x.get("r") == "stats":
            v = x["net"]["handshake_failures"]
            v[0], v[2] = 0, v[0]


plant_log("shutdown-timeout", "b3/unit.jsonl", 299.2, "rig-node@b3.service: State 'stop-sigterm' timed out. Killing.",
          ["shutdown-bounded"])


# The store's log lost the own of a key's first epoch.
plant("store-void", rc=3)(lambda f, i: f["store.jsonl"].remove(
    next(x for x in f["store.jsonl"] if x["r"] == "O" and x["k"] == i["pk"][6])))


@plant(want=["store-void"], rc=3)
def p_store_void_stale(f, i):
    """An append refused as stale where no newer owner holds the key."""
    x = last(f, i["pk"][6])
    f["store.jsonl"].append(dict(x, t=i["T0"] + 250 * S, op="b1.0.999997", v="stale"))


plant("store-void-out", ["store-void"], rc=3)(lambda f, i: f["store.out"].append(
    "thread 'main' panicked at src/store.rs:1:1:"))


@plant(want=ANY, rc=3)
def p_store_void_gap(f, i):
    """The store's log is missing 60 s to 62 s, while ops were sent and acked: it fails the rest too, and is VOID."""
    f["store.jsonl"] = [x for x in f["store.jsonl"] if not i["T0"] + 60 * S <= x["t"] <= i["T0"] + 62 * S]


plant_line("give-up", "b1/client.jsonl", 100, r="give_up", op="b1.0.999998", k="k0", tries=2400, err="timed out")


@plant()
def p_log_complete(f, i):
    """b1's counters count 100 acks more than it has lines for."""
    for x in f["b1/node.jsonl"]:
        if x["r"] == "stats" and x["t"] >= i["T0"] + 100 * S:
            x["acks"] += 100


@plant(want=["log-complete"])
def p_log_complete_unit(f, i):
    del f["b3/unit.jsonl"]


@plant()
def p_heartbeat(f, i):
    """b1 logged no stats line from 60 s to 100 s."""
    f["b1/node.jsonl"] = [x for x in f["b1/node.jsonl"]
                          if x["r"] != "stats" or not i["T0"] + 60 * S <= x["t"] <= i["T0"] + 100 * S]


@plant(want=["heartbeat"])
def p_heartbeat_tick(f, i):
    """b1's accounts workload stopped trying from 100 s to 150 s, its stats lines still coming."""
    for x in f["b1/node.jsonl"]:
        if x["r"] == "stats" and i["T0"] + 100 * S <= x["t"] <= i["T0"] + 150 * S:
            x["ticks"]["accounts"] = 1000


# A key k50 whose client never called it: the run was started with 51 keys.
plant("client-task")(lambda f, i: i["meta"]["params"].update(keys="51"))


plant_line("stream-publish-err", "b1/stream.jsonl", 61, r="publish_err", s="b1/s0", err="LimitExceeded")
# During the diskdrop: no fault explains a gap.
plant_line("stream-reset-kind", "b3/stream.jsonl", (2, "on", 5), r="reset", s="b1/s0", why="Gap", start="after")
plant_line("stream-subscribe-err", "b3/stream.jsonl", (2, "on", 5), ["stream-reset-kind"], r="subscribe_err",
           s="b1/s0", err="Recovery(ConflictingDuplicate)")
# A slow consumer during b1's diskdrop: nothing starved b3, b2 or b2's relay b3.
plant_line("stream-slow", "b3/stream.jsonl", (2, "on", 5), ["stream-reset-kind"], r="reset", s="b2/s0",
           why="SlowConsumer", start="live")
# With no fault anywhere.
plant_line("stream-quiet", "b1/stream.jsonl", 62, r="reset", s="b2/s0", why="UpstreamLost", start="live")


@plant()
def p_stream_generation(f, i):
    """b2 opened its streams at generation 1 again after its kill."""
    for x in f["b2/stream.jsonl"]:
        if x["r"] == "open" and x["g"] == 2:
            x["g"] = 1


@plant(want=ANY, rc=2)
def p_unjudged(f, i):
    """No stream lines at all from a run whose nodes ran streams: UNJUDGED, not PASS."""
    for n in ("b1", "b2", "b3"):
        del f[n + "/stream.jsonl"]


@plant(want=["journal-missing"])
def p_journal_missing_mkfs(f, i):
    """A b1 journal line of 90 s missing, with a mkfs of b1's disk at the diskdrop's reset whose archive holds the
    old journal: formatting 70 s after the line was synced does not waive it."""
    name = "b1/journal-%s.jsonl" % i["run"]
    f[name].remove(first_line(f, name, lambda x: x["t"] > i["T0"] + 90 * S))
    r = nem_line(f, ev="journal-reset")
    r.update(how="mkfs", detail="journal-reset mkfs /home/ubuntu/rig/runs/x/journal-archive/1700000172")
    f["b1/journal-archive/1700000172/journal-%s.jsonl" % i["run"]] = f[name][:1]


@plant(want=[])
def p_journal_missing_formatted(f, i):
    """The same, with the archive empty: the mkfs lost the old journal, which waives the line."""
    p_journal_missing_mkfs(f, i)
    del f["b1/journal-archive/1700000172/journal-%s.jsonl" % i["run"]]


@plant(want=["journal-missing", "fault-effect"])
def p_journal_missing_powercut(f, i):
    """A powercut of b1 from 240 s to 250 s, its journal formatted at the reset at 252 s with the old journal archived,
    and b1's journal line of a record of 238 s missing: a power cut waives no line. b1 logging through the cut, and
    not starting again, contradicts the powercut too."""
    add_episode(f, i, 4, "powercut", ["b1"], 240, 250)
    f["nemesis.jsonl"].append(dict(t=i["T0"] + 252 * S, ev="journal-reset", i=4, node="b1", how="mkfs",
                                   detail="journal-reset mkfs /home/ubuntu/rig/runs/x/journal-archive/1700000252"))
    name = "b1/journal-%s.jsonl" % i["run"]
    x = first_line(f, name, lambda x: x["t"] > i["T0"] + 238 * S)
    assert x["t"] < i["T0"] + 239 * S
    f[name].remove(x)
    f["b1/journal-archive/1700000252/journal-%s.jsonl" % i["run"]] = f[name][:1]


# b1 had record 1000 of disk-b2/s0 from b2's first incarnation, and b2's second opened it after record 900.
plant_line("disk-durable", "b1/stream.jsonl", (0, "off", 3), r="rewritten", s="disk-b2/s0", g=1, seq=901, inc=2,
           open=900, max=1000)


def streams_fault(f, i, cls, node="b2", on=240, off=250, reset=252):
    """cls on node's streams disk from on to off s, its disk reset at reset s."""
    add_episode(f, i, 4, cls, [node], on, off, dev="streams")
    f["nemesis.jsonl"].append(dict(t=i["T0"] + reset * S, ev="disk-reset", i=4, node=node, disk="streams", how="fsck",
                                   detail="streams-reset fsck 0"))


@plant(want=[])
def p_disk_durable_lied(f, i):
    """The same after a diskdrop of b2's streams disk, with a dup b3 saw and then silence for 20 s while b2 published
    again up to where b3 was: the disk lied, not the stream."""
    streams_fault(f, i, "diskdrop")
    f["b3/stream.jsonl"].append(dict(t=i["T0"] + 276 * S, r="resume", s="disk-b2/s0", quiet_ms=20000))
    for n, r in (("b1", "rewritten"), ("b3", "dup")):
        f[n + "/stream.jsonl"].append(dict(t=i["T0"] + 256 * S, r=r, s="disk-b2/s0", g=1, seq=901, inc=3, open=900,
                                           max=1000, last=1000))


@plant(want=[])
def p_disk_durable_late(f, i):
    """A diskcorrupt of b2's streams disk at 136 s, in its second incarnation, and b1 hearing b2's third only when it
    published past b1's record 1000 from the second, at 280 s: the loss lies between those opens, however long b1
    waited (pairs-20260930-2317-s49, 162 s)."""
    streams_fault(f, i, "diskcorrupt", "b2", 136, 140, 141)
    f["b1/stream.jsonl"].append(dict(t=i["T0"] + 280 * S, r="rewritten", s="disk-b2/s0", g=1, seq=1001, inc=3,
                                     open=900, max=1000, was=2))


@plant(want=[])
def p_stream_quiet_lied(f, i):
    """The same diskcorrupt, and b3, ahead of what b2's disk lost, refused at 155 s in a quiet window (pairs s49)."""
    streams_fault(f, i, "diskcorrupt", "b2", 136, 140, 141)
    f["b3/stream.jsonl"].append(dict(t=i["T0"] + 155 * S, r="subscribe_err", s="disk-b2/s1",
                                     err="Recovery(HistoryUnavailable)"))


@plant()
def p_disk_resume(f, i):
    """b2's disk held nothing of its streams when it started again after the kill."""
    for x in f["b2/disk.jsonl"]:
        if x["r"] == "open" and x["inc"] == 2:
            x.update(stored=None, at=0)


@plant()
def p_disk_error(f, i):
    """b1's disk refused a publish at 60 s, with no fault anywhere."""
    for r, t, kw in (("publish_err", 60, {"err": "Disk(Failed)"}), ("publish_ok", 61, {"after_ms": 1000})):
        f["b1/disk.jsonl"].append(dict(t=i["T0"] + t * S, r=r, s="disk-b1/s0", **kw))


@plant(want=["disk-error"])
def p_disk_error_stuck(f, i):
    """A diskerr of b1's streams disk: its publishes refused from 241 s and never taken again, nor b1 started again."""
    streams_fault(f, i, "diskerr", "b1")
    f["b1/disk.jsonl"].append(dict(t=i["T0"] + 241 * S, r="publish_err", s="disk-b1/s0", err="Disk(Failed)"))


@plant(want=[])
def p_disk_error_cleared(f, i):
    """The same, taken again at 251 s."""
    p_disk_error_stuck(f, i)
    f["b1/disk.jsonl"].append(dict(t=i["T0"] + 251 * S, r="publish_ok", s="disk-b1/s0", after_ms=10000))


@plant(want=[])
def p_disk_error_stacked(f, i):
    """The stuck diskerr, then a diskfull of the same disk from 248 s to 262 s, taken again at 270 s: within B of the
    later fault's off."""
    p_disk_error_stuck(f, i)
    add_episode(f, i, 5, "diskfull", ["b1"], 248, 262, dev="streams")
    for r, t, kw in (("publish_err", 249, {"err": "Disk(Full)"}), ("publish_ok", 270, {"after_ms": 1000})):
        f["b1/disk.jsonl"].append(dict(t=i["T0"] + t * S, r=r, s="disk-b1/s0", **kw))


@plant(want=[])
def p_disk_error_nofile(f, i):
    """A nofile on b1 from 240 s to 250 s: a publish refused as Failed at 241 s, a segment it could not open, taken
    again at 251 s (chaos-20260930-2213-s46)."""
    add_episode(f, i, 4, "nofile", ["b1"], 240, 250, headroom=0)
    for r, t, kw in (("publish_err", 241, {"err": "Disk(Failed)"}), ("publish_ok", 251, {"after_ms": 10000})):
        f["b1/disk.jsonl"].append(dict(t=i["T0"] + t * S, r=r, s="disk-b1/s0", **kw))


@plant(want=[])
def p_heartbeat_burst(f, i):
    """memhigh on b1 from 240 s to 250 s, then b1's stats loop writes its missed lines at 251 s, a ms apart, the ticks
    of the later ones not moved (chaos-20260930-2213-s46)."""
    add_episode(f, i, 4, "memhigh", ["b1"], 240, 250, pct=80)
    st = [x for x in f["b1/node.jsonl"] if x["r"] == "stats" and i["T0"] + 240 * S <= x["t"] < i["T0"] + 260 * S]
    for j, x in enumerate(st):
        x["t"], x["ticks"] = i["T0"] + 251 * S + j * MS, dict(st[0]["ticks"])


plant_line("disk-publish-seq", "b1/disk.jsonl", 60, ["disk-resume"], r="publish_seq", s="disk-b1/s0", want=1001,
           got=1002)


@plant(want=["fault-effect"])
def p_disk_resume_formatted(f, i):
    """A powercut of b2's journal from 240 s to 250 s, and its streams disk formatted at 252 s by another reset: its
    start at 253 s holds nothing, which that mkfs explains. b2 logging through the cut, and not starting again,
    contradicts the powercut."""
    add_episode(f, i, 4, "powercut", ["b2"], 240, 250)
    for t, disk, how in ((251, "journal", "fsck"), (252, "streams", "mkfs")):
        f["nemesis.jsonl"].append(dict(t=i["T0"] + t * S, i=4, node="b2", how=how, detail="%s-reset %s" % (disk, how),
                                       **({"ev": "journal-reset"} if disk == "journal" else {"ev": "disk-reset",
                                                                                            "disk": disk})))
    for s in ("s0", "s1"):
        f["b2/disk.jsonl"].append(dict(t=i["T0"] + 253 * S, r="open", s="disk-b2/%s" % s, g=4, stored=None, at=0, inc=4,
                                       truncated=0))


# b1's streams disk held 20 MiB at 100 s, over the 16.75 MiB retention bounds it to.
plant("disk-retention")(lambda f, i: next(x for x in f["b1/disk.jsonl"] if x["r"] == "disk"
                                         and x["t"] >= i["T0"] + 100 * S).update(bytes=20 << 20))


# ---- the quota workload (src/quota.rs, spec/fault-rig.md § Workloads)

INVARIANTS += [
    # Cluster-wide limits (src/quota.rs), judged on the monotonic clock of each node: rates admitted per window and
    # permits held at once within spec/quotas.md § 5's bounds, and not far under them; what a refusal says to wait;
    # permits given back; readiness. lim-*: the stream node's limits on namespace lim, through the same quotas.
    "quota-rate-over", "quota-permit-over", "quota-permit-leak", "quota-retry", "quota-under", "quota-pulling",
    "lim-rate", "lim-open", "lim-subs", "lim-err",
]
QUOTA_BIN = 125 * MS  # a 1 s rate's bucket, W/8: src/quota.rs counts on this grid
QUOTA_RHO = 0.01  # spec/quotas.md § 5: a steady over-grant of rho times the limit
QUOTA_ANCHOR = 20 * MS  # a line whose wall and monotonic clocks disagree with the anchor by more may move it
QUOTA_AFTER = 10 * S  # a fault's excess outlasts it: a grant's life, 5 s, and a window
QUOTA_READY = 30 * S
QUOTA_NEW = 1500 * MS  # a node on the fallback for a key new to it: the simulation fails one past 1.5 s
QUOTA_UNDER = 0.5  # of the limit, at least, where twice it is offered: the simulation's floor is 0.848
QUOTA_STUCK = 1500 * MS  # an under-limit caller that honours retry_after, from its first try to its admission
QUOTA_TABLE_SLACK = 3  # permits a node's counters and its table may disagree by, taken or given back in between
QUOTA_DRAIN_QUIET = 20 * S
FLOOR_WORKLOAD.update({inv: {"quota"} for inv in ("quota-rate-over", "quota-permit-over", "quota-retry", "quota-under")})
FLOOR_WORKLOAD.update({inv: {"quota", "streams"} for inv in ("lim-rate", "lim-open")})  # lim runs in the stream node
for _mode, _n in (("smoke", 1), ("night", 20), ("selftest", 1)):
    FLOORS[_mode].update({"quota-rate-over": 1000 * _n, "quota-permit-over": 200 * _n, "quota-retry": 100 * _n,
                          "quota-under": 10 * _n, "lim-rate": 500 * _n, "lim-open": 20 * _n})
CHECKS.append(Run.check_quota)


def quota_excess(f, share, n, seeds):
    """How far past a limit a fault may push a key, in limits (spec/quotas.md § 5): a side more per side of a cut, and
    a limit per node isolated, as is each node of a partition's side without a majority of the seeds; a crash an eighth
    of its grant, and its fresh start the fallback share; a stop its whole grant; a drift its rate and a bucket;
    hearing fewer than a majority, every node's fallback share."""
    c, k = f["class"], len(set(f["nodes"]) - set(f.get("noop", ())))
    if c == "partition":
        sides = set(f["nodes"]), set(f["args"]["other"])
        return sum(len(side) for side in sides if 2 * len(side & seeds) <= len(seeds))
    if c in CUTS:
        return {"isolate": k, "bridge": 2}.get(c, 1)
    if c in NETWORK:
        return n * share
    if c == "leave-minority":  # a cut into two sides, then its leaver's restart
        return 1 + 1 / 8 + share
    if c in RESTARTS:
        return k * (1 / 8 + share)
    if c in HALTS or c in ("cpu", "memhigh"):
        return k * (1 + share)
    if c in DRIFTS:
        return k * (1 / 8 + abs(float(f["args"].get("k", 0))) / 10000 * (f["off"][1] - f["on"][0]) / S)
    if c in CLOCK or c in IO:
        return 0  # quotas run on the monotonic clock and read no disk
    return k * (1 + share)


def synth_quota(files, info, lives, nline):
    """Each node's quota lines: q0 and q1 admitted 8 and refused 13 a bucket per node, r0 1, lim 8 and 10, logged
    every 2 s; two permits of p0 and p1 and two lim streams per node held from 50 ms to 900 ms of each odd second, 6
    at once, but in the first 15 s of each minute, when each logs qheld at 8 s and one drains at 9.9 s. A node killed
    holds its last second's until its last line. b2 and b3 log nothing in the second around the skew-restart's kill,
    and 4 s behind through it, as their clocks read."""
    T0, E = info["T0"], info["E"]
    P, SK = E[1], E[3]
    for n in files:
        if n.endswith("node.jsonl"):
            for x in files[n]:
                if x.get("r") == "up":
                    x["workloads"] = x["workloads"] + ["quota"]
    for n, ls in lives.items():
        for a, b in ls:
            def q(t, r, **kw):
                if t >= b or t < a or n == "b3" and P["on"] <= t <= P["off"] or n != "b1" and abs(t - SK["on"]) <= S:
                    return
                wall = t - 4 * S if n != "b1" and SK["on"] <= t <= SK["off"] else t
                nline(n, "quota.jsonl", wall, r=r, m=(t - a) // MS, **kw)
            q(a + 200 * MS, "qup", inc=a // MS, nodes=3, hot=200, under=200, ws=7,
              lim={"subscriptions": 4, "records": 200, "open": 6})
            q(a + 2 * S, "qready")
            for i, t in enumerate(range(a + S, b, S)):
                q(t + 500 * MS, "lsub", held=2, max=3, refused=0, ends=0, errors=0)
                if i % 2:
                    continue
                bk = dict(b=max(0, i - 1) * 8, stuck=0)
                z = 16 if i else 8
                q(t, "qrate", k="q0", max=200, w=1000, a=[8] * z, l=[13] * z, s=[0] * z, ra=[3, 125], **bk)
                q(t, "qrate", k="q1", max=200, w=1000, a=[8] * z, l=[13] * z, s=[0] * z, ra=[3, 125], **bk)
                q(t, "qrate", k="r0", max=200, w=1000, a=[1] * z, l=[0] * z, s=[0] * z, ra=None, **bk)
                q(t, "qrate", k="lim", max=200, w=1000, a=[8] * z, l=[10] * z, s=[0] * z, ra=[2, 125], **bk)
                if i % 10 == 0:
                    q(t, "qrate", k="lim.open", max=6, w=0, a=[0] * z, l=[1] + [0] * (z - 1), s=[0] * z, ra=[100, 1000],
                      **bk)
                    q(t, "qstats", ready=True, pulling=False, entries=5, permits=10, keys=6, rows=18, mismatched=0,
                      denied_limit=100, denied_share=3, fallback=0, reports=100, report_failures=0)
            for s in range((a - T0) // S, (b - T0) // S + 1):
                sec = T0 + s * S
                if (s + 20) % 60 < 15:  # T0 is 20 s into a minute
                    if (s + 20) % 60 == 8:
                        q(sec, "qheld", held=[0, 0], table=1, lim_open=1)
                    if (s + 20) % 60 == 9 and n == "b%d" % ((T0 // S + s) // 60 % 3 + 1):
                        for k in ("p0", "p1"):
                            q(sec + 900 * MS, "drain", k=k, got=6, want=6, ms=400)
                    continue
                acq, rel = sec + 50 * MS, sec + 900 * MS
                if s % 2 == 0 or acq < a + 300 * MS:
                    continue
                for k in ("p0", "p0", "p1", "p1", "lim", "lim"):
                    got, back = ("pacq", "prel") if k != "lim" else ("lopen", "lclose")
                    what = {"k": k} if k != "lim" else {"s": "lim/x", "g": 1}
                    q(acq, got, **what)
                    if rel <= b - 200 * MS:
                        q(rel, back, **what)

SYNTH_EXTRAS.append(synth_quota)


def quota_at(f, i, node, lo, hi, **kw):
    return [x for x in f[node + "/quota.jsonl"]
            if i["T0"] + lo * S <= x["t"] <= i["T0"] + hi * S and all(x.get(a) == v for a, v in kw.items())]


@plant()
def p_quota_rate_over(f, i, k="q0"):
    """b1 admitted q0 40 more a bucket from 60 s to 62 s: 512 in a window of a limit of 200."""
    for x in quota_at(f, i, "b1", 60, 62, r="qrate", k=k):
        x["a"] = [a + 40 for a in x["a"]]


def quota_step(f, i, extra):
    """b2's clock stepped 3 s back from 240 s to 250 s, its quota lines logged 3 s behind, and b2 admitting 16 of q0 a
    bucket from 230 s to 250 s, b1 and b3 4: a checker that placed them by wall clock would count b2's twice."""
    add_episode(f, i, 4, "step", ["b2"], 240, 250, step_s=-3)
    for n in ("b1", "b2", "b3"):
        for x in quota_at(f, i, n, 230, 251, r="qrate", k="q0"):
            x["a"] = [16 if n == "b2" else 4] * 8
            if n == "b2" and i["T0"] + 244 * S <= x["t"] <= i["T0"] + 246 * S:
                x["a"] = [a + extra for a in x["a"]]
    for x in quota_at(f, i, "b2", 240, 250):
        x["t"] -= 3 * S


plant("quota-rate-stepped", ["quota-rate-over"])(lambda f, i: quota_step(f, i, 40))
plant("quota-stepped-clean", [])(lambda f, i: quota_step(f, i, 0))
plant("lim-rate")(lambda f, i: p_quota_rate_over(f, i, k="lim"))


@plant()
def p_quota_permit_over(f, i, at=61, node="b1"):
    """b1 held two p0 permits more from 61.1 s to 61.8 s: 8 of 7."""
    f[node + "/quota.jsonl"] += [dict(t=i["T0"] + (at * 1000 + 100) * MS, r="pacq", k="p0", m=at * 1000 + 100)] * 2 + [
        dict(t=i["T0"] + (at * 1000 + 800) * MS, r="prel", k="p0", m=at * 1000 + 800)] * 2


# The same 8 of 7 at p0's first use, 1.1 s: three nodes on the fallback may hold 3 each.
plant("quota-new-key", [])(lambda f, i: p_quota_permit_over(f, i, at=1))


@plant(want=[])
def p_quota_late_use(f, i):
    """The same 8 of 7 by b3, which first takes p0 then, the others holding their grants: its share on the fallback
    beside them (chaos-20260930-2245-s47: 18 of 12 at 13 s, all but one node's first use, after its drain at 4 s)."""
    f["b3/quota.jsonl"] = [x for x in f["b3/quota.jsonl"] if not (
        x.get("k") == "p0" and x["r"] in ("pacq", "prel", "drain") and x["t"] < i["T0"] + 61 * S)]
    p_quota_permit_over(f, i, node="b3")


# b1 took two p0 permits more at 79.1 s, which it gave back at 79.4 s, while b2, killed at 80 s, still held its two:
# its spans open when it died count until its last line, 79.5 s.
plant("quota-permit-dead", ["quota-permit-over"])(lambda f, i: f["b1/quota.jsonl"].extend(
    [dict(t=i["T0"] + 79_100 * MS, r="pacq", k="p0", m=79_100)] * 2
    + [dict(t=i["T0"] + 79_400 * MS, r="prel", k="p0", m=79_400)] * 2))
plant("lim-open")(lambda f, i: f["b1/quota.jsonl"].extend([
    dict(t=i["T0"] + 61_200 * MS, r="lopen", s="lim/b1-2", g=9, m=61_200),
    dict(t=i["T0"] + 61_700 * MS, r="lclose", s="lim/b1-2", g=9, m=61_700)]))


plant("lim-subs")(lambda f, i: quota_at(f, i, "b3", 61, 62, r="lsub")[0].update(max=5))
plant_line("lim-err", "b2/quota.jsonl", 61.003, r="lerr", s="lim/b2-0", op="publish", err="Closed", m=61_000)
# A subscribe to lim on b3 failed other than at its limit.
plant("lim-err-subscribe", ["lim-err"])(lambda f, i: quota_at(f, i, "b3", 61, 62, r="lsub")[0].update(errors=1))


@plant()
def p_quota_permit_leak(f, i):
    """b2's table held 4 permits more than its holders and streams at 108 s: one was never given back."""
    quota_at(f, i, "b2", 107, 109, r="qheld")[0]["table"] += 4


@plant(want=["quota-permit-leak"])
def p_quota_drain(f, i):
    """The drain at 49.9 s got 4 of p1's 6: another node never gave two back."""
    x = next(x for n in ("b1", "b2", "b3") for x in quota_at(f, i, n, 49, 50, r="drain", k="p1"))
    x["got"] = 4


# b1's under-limit caller waited 3 s at 64 s, honouring retry_after, with the cluster far under r0's limit; a refusal
# of q0 said to wait 2 s, past a bucket; one of lim's records rate said nothing of when to try again.
plant("quota-retry")(lambda f, i: quota_at(f, i, "b1", 64, 65, r="qrate", k="r0")[0].update(stuck=3000))
plant("quota-retry-after", ["quota-retry"])(lambda f, i: quota_at(f, i, "b3", 249, 251, r="qrate", k="q0")[0].update(
    ra=[3, 2000]))
plant("quota-retry-none", ["quota-retry"])(lambda f, i: quota_at(f, i, "b2", 249, 251, r="qrate", k="lim")[0].update(
    ra=[0, 125]))


@plant(want=[])
def p_quota_leave_minority(f, i):
    """p_leave_minority, b3 admitting q0 40 more a bucket while it runs: two sides, then the leaver's restart."""
    p_leave_minority(f, i)
    on, off = ((i["E"][0][e] - i["T0"]) // S for e in ("on", "off"))
    for x in quota_at(f, i, "b3", on + 2, off, r="qrate", k="q0"):
        x["a"] = [a + 40 for a in x["a"]]


@plant()
def p_quota_under(f, i):
    """From 60 s to 70 s, quiet, the cluster admitted 48 of q0 a second while offering 480: a limit that refuses."""
    for n in ("b1", "b2", "b3"):
        for x in quota_at(f, i, n, 60, 70, r="qrate", k="q0"):
            x["a"], x["l"] = [2] * 8, [18] * 8


@plant(want=[])
def p_quota_under_end(f, i):
    """A phase that ended at 255.13 s, quiet, every node's q0 buckets of the second before it refusing all they were
    offered: the nodes stopping, the window short (pairs-20260930-2346-s50, 96 of lim at 588.35 s, the phase's end)."""
    end = i["T0"] + 255130 * MS
    i["meta"]["phases"] = [{"start": min(x["t"] for x in f["b1/node.jsonl"] if x["r"] == "up"), "end": end}]
    for n in ("b1", "b2", "b3"):
        for x in quota_at(f, i, n, 254, 257, r="qrate", k="q0"):
            for j in range(len(x["a"])):  # a line's buckets are the 2 s before it
                if 254 * S < x["t"] - i["T0"] - 2 * S + (j + 0.5) * QUOTA_BIN < 255 * S:
                    x["a"][j], x["l"][j] = 0, 40


@plant()
def p_quota_pulling(f, i):
    f["b1/quota.jsonl"] = [x for x in f["b1/quota.jsonl"] if x["r"] != "qready"]


@plant(want=[])
def p_quota_no_streams(f, i):
    """A run of quota without streams has no lim load, and its floors do not hold."""
    for n in ("b1", "b2", "b3"):
        f[n + "/quota.jsonl"] = [x for x in f[n + "/quota.jsonl"]
                                 if x["r"] not in ("lopen", "lclose", "lsub") and x.get("k") not in ("lim", "lim.open")]
        for x in f[n + "/node.jsonl"]:
            if x.get("r") == "up":
                x["workloads"] = [w for w in x["workloads"] if w != "streams"]


@plant(want=ANY, rc=2)
def p_quota_unjudged(f, i):
    """No quota lines from a run whose nodes ran quota: UNJUDGED, not PASS."""
    for n in ("b1", "b2", "b3"):
        del f[n + "/quota.jsonl"]


# ---- keyed actors beyond accounts, compacted streams, named calls and the authorizer (spec/fault-rig.md §
# Workloads): src/{compacted,eager,watch,echo,idle}.rs, and the authz fault class.

INVARIANTS += ["compact-fold", "compact-origin", "eager-restart", "watch-epoch", "watch-owner", "echo-deadline",
               "authz-leak", "resolve-all", "spawn-idle", "echo-liveness", "idle-race"]
# Floors: a smoke's, ten times that a night's, each applying only to a run whose nodes ran its workload (Run.run).
for inv, w, n in (("compact-fold", "compacted", 50), ("eager-restart", "eager", 100), ("watch-owner", "watch", 50),
                  ("echo-deadline", "echo", 200), ("echo-liveness", "echo", 50), ("resolve-all", "echo", 5),
                  ("spawn-idle", "echo", 50), ("idle-race", "idle", 20)):
    FLOOR_WORKLOAD[inv] = {w}
    for mode, x in (("smoke", 1), ("night", 10), ("selftest", 0)):
        FLOORS[mode][inv] = max(1, n * x)
ECHO_WITHIN, TMP_IDLE, IDLE_RACE = S, 3 * S, 5 * S  # src/echo.rs, and an idle key's call answered within 5 s


def incs(r, node):
    """A node's incarnations, each [its up, its last line] on the store's clock."""
    c = r.__dict__.setdefault("_incs", {})
    if node not in c:
        out = []
        for x in r.nodes.get(node, {}).get("node", ()):
            if x.get("r") == "up":
                out.append([x["_t"], x["_t"]])
            elif out:
                out[-1][1] = max(out[-1][1], x["_t"])
        c[node] = out
    return c[node]


def alive(r, node, a, b=None):
    """One incarnation of the node was up from a through b."""
    return any(u <= a and (a if b is None else b) <= z for u, z in incs(r, node))


def ran_workload(r, w):
    return [n for n, nd in r.nodes.items() if any(w in (x.get("workloads") or ()) for x in nd["node"]
                                                   if x.get("r") == "up")]


def put(st, k, v):
    """A keyed record on a compacted state: an empty value removes the key (src/compacted.rs)."""
    if v:
        st[k] = v
    else:
        st.pop(k, None)


def check_compacted(r):
    """compact-origin: each origin's records of a generation numbered 1, 2, .. with no publish error, never more keys
    than its limit, and refused as full only at the limit with a new key. compact-fold: each state a follower logged
    (its snapshot, and its fold of the records after it) is the fold of the origin's records through its cursor. A
    follower's subscription that ended or failed is judged as the streams workload's are (Run.stream_end)."""
    pubs = collections.defaultdict(dict)  # (s, g) -> seq -> (k, v)
    queries = collections.defaultdict(list)
    for n in r.nodes:
        state, last, limit = {}, {}, {}
        for x in r.lane(n, "compacted.jsonl"):
            ev, s = x.get("r"), x.get("s")
            if ev in ("snap", "fold"):
                queries[(s, x["g"])].append((x["through"], n, x))
            elif ev in ("reset", "subscribe_err"):
                r.stream_end(n, x["_t"], x)
            elif ev == "open":
                state[s], limit[s] = {}, x["max_keys"]
            elif ev == "pub":
                r.judged["compact-origin"] += 1
                g, st = x["g"], state.setdefault(s, {})
                want = last.get((s, g), 0) + 1
                if x["seq"] != want:
                    r.flag("compact-origin", x["_t"], probe=False, node=n, s=s, g=g, seq=x["seq"], want=want)
                last[(s, g)] = x["seq"]
                pubs[(s, g)][x["seq"]] = (x["k"], x["v"])
                put(st, x["k"], x["v"])
                if len(st) > limit.get(s, math.inf):
                    r.flag("compact-origin", x["_t"], probe=False, node=n, s=s, what="past max_keys", keys=len(st))
            elif ev == "full":
                st = state.get(s, {})
                if len(st) < limit.get(s, 0) or x["k"] in st:  # 32 short records are far under max_bytes
                    r.flag("compact-origin", x["_t"], probe=False, node=n, s=s, what="full under its limits",
                           k=x["k"], keys=len(st))
            elif ev == "publish_err":
                r.flag("compact-origin", x["_t"], node=n, s=s, err=x.get("err"))
    for sg, qs in queries.items():
        ps, top = pubs.get(sg, {}), 0
        while top + 1 in ps:  # a kill can cut the origin's last lines: past them nothing is judged
            top += 1
        st, at = {}, 0
        for through, n, x in sorted(qs, key=lambda q: q[0]):
            if through > top:
                break
            while at < through:
                at += 1
                put(st, *ps[at])
            r.judged["compact-fold"] += 1
            if x["state"] != st:
                r.flag("compact-fold", x["_t"], probe=False, node=n, s=sg[0], g=sg[1], through=through, what=x["r"],
                       differ=sorted(set(x["state"].items()) ^ set(st.items()))[:6])


def check_eager(r):
    """eager-restart: every key b1 started (its `spawned` line) runs on a live node B + 2 s after each episode that
    ends before the next begins, and B + 5 s after they were started; running is a `start` line of the key with no
    `stop` after it in an incarnation that logs on. Only in the phase that started them: a phase's end stops them.
    The acceptors log who holds each key, so a key whose node ended while every acceptor restarted is no exception."""
    sp = [x for x in r.lane("b1", "eager.jsonl") if x.get("r") == "spawned"]
    if not sp:
        return
    t0, keys = sp[0]["_t"], ["e%d" % i for i in range(sp[0]["keys"])]
    phase = next((p for p in r.phases if p["start"] <= t0 <= p.get("end", r.end)), {"end": r.end})
    ev = collections.defaultdict(list)
    for n, nd in r.nodes.items():
        for x in nd["node"]:
            if x.get("r") in ("start", "stop") and str(x.get("k", "")).startswith("e"):
                ev[x["k"]].append((x["_t"], n, x["_inc"], x["r"] == "start", x.get("why")))
    qs = []
    if t0 + r.B + 5 * S < min(r.starts[:1] + [phase.get("end", r.end) - 20 * S]):
        qs.append((t0 + r.B + 5 * S, None))
    for i, c in enumerate(r.clusters):
        q = c["end"] + r.B + 2 * S
        nxt = min(r.starts[i + 1] if i + 1 < len(r.starts) else math.inf, phase.get("end", r.end) - 20 * S)
        if c["end"] > t0 and q < nxt:
            qs.append((q, c))
    def running(n, inc, q):  # logged past q, or went quiet under WINDOW before a restart fault sent after q ended it
        ls = incs(r, n)
        nxt = ls[inc][0] if inc < len(ls) else r.end
        return ls[inc - 1][1] >= q or ls[inc - 1][1] + WINDOW >= q and q < nxt and any(
            f["on"][0] >= q for f in r.fault_on(n, RESTARTS, q, nxt))

    for q, c in qs:
        down = []
        for k in keys:
            r.judged["eager-restart"] += 1
            last = {}
            for t, n, inc, up, why in ev[k]:
                if t <= q:
                    last[(n, inc)] = (t, up, why)
            if not any(up and inc and running(n, inc, q) for (n, inc), (_, up, _) in last.items()):
                down.append(k)
        if down:
            r.flag("eager-restart", q, keys=down[:10], down=len(down), **({"episode": c["i"]} if c else {}))


def check_watch(r):
    """watch-owner: an owner a watch shows, whose epoch owned the key at the store, is the node that owned it there.
    watch-epoch: a watch never shows a key's epoch below one it showed before in its node's incarnation, unless the
    node shown or the watcher was stopped, starved or had its clock moved within B before."""
    owner = {(x["k"], x["e"]): x["n"] for x in r.store if x["r"] == "O" and x["v"] == "ok"}
    for n in r.nodes:
        top = {}
        for x in r.lane(n, "watch.jsonl"):
            if x.get("r") != "own" or x.get("e") is None:
                continue
            t, key = x["_t"], (x["_inc"], x["k"])
            r.judged["watch-epoch"] += 1
            if x["e"] < top.get(key, -1) and not (r.exempt(x["n"], t - r.B, t) or r.exempt(n, t - r.B, t)):
                r.flag("watch-epoch", t, node=n, k=x["k"], e=x["e"], shown=x["n"], after=top[key])
            top[key] = max(top.get(key, -1), x["e"])
            o = owner.get((x["k"], x["e"]))
            if o is not None:
                r.judged["watch-owner"] += 1
                if o != x["n"]:
                    r.flag("watch-owner", t, probe=False, node=n, k=x["k"], e=x["e"], shown=x["n"], store=o)


def check_echo(r):
    """Named calls with deadlines, lookups, remote spawns and the authorizer (src/echo.rs); each judged where no fault
    explains it: see spec/fault-rig.md § Workloads."""
    echo = set(ran_workload(r, "echo"))

    def due_after(a):  # B after a, or after the last of the episodes around it
        due = a + r.B
        for c in r.clusters:
            if c["start"] <= due and c["end"] >= a:
                due = max(due, c["end"] + r.B)
        return due

    for n in r.nodes:
        xs = r.lane(n, "echo.jsonl")
        inc, since = None, {}  # a refused peer -> when this incarnation began refusing it, on the node's clock
        tmp = {}  # (inc, id) -> [up, stop]
        for x in xs:
            ev, t, i = x.get("r"), x["_t"], x["_inc"]
            if i != inc:
                inc, since = i, {}
            if ev == "authz":
                new = set(x["refuse"])
                since = {p: since.get(p, x["t"]) for p in new}
            elif ev == "ran":
                fr = x["from"]
                if x.get("tmp") is None:  # a spawned actor's calls are not the named ones the authorizer guards
                    r.judged["authz-leak"] += 1
                    if fr in since and x["t"] > since[fr] + ECHO_WITHIN + 100 * MS:
                        r.flag("authz-leak", t, probe=False, node=n, peer=fr, id=x["id"],
                               refused_ms=(x["t"] - since[fr]) // MS)
                a = r.to_store(fr, x["sent"])
                if r.exempt(n, a - S, t) or r.exempt(fr, a - S, t) or r.fault_on(n, NETWORK, a - S, t) or \
                        r.fault_on(fr, NETWORK, a - S, t) or r.fault_on(n, STARVES - {"cpu"}, a, t):
                    continue  # time in a buffer or a retransmit, or a page-in or a hog between the deadline check and
                    # the stamp, is not charged to the deadline (spec); a CPUQuota's stall ends with its 100 ms period
                r.judged["echo-deadline"] += 1
                slack = 100 * MS + r.slop.get(n, 0) + r.slop.get(fr, 0) + (
                    100 * MS if r.fault_on(n, {"cpu"}, a, t) else 0)
                if t - a > x["within_ms"] * MS + slack:
                    r.flag("echo-deadline", t, node=n, peer=fr, id=x["id"], late_ms=(t - a - x["within_ms"] * MS) // MS)
            elif ev == "resolve" and r.quiet(t - 10 * S, t):
                want = {m for m in echo - {n} if alive(r, m, t - 15 * S, t)}
                r.judged["resolve-all"] += 1
                if want - set(x["got"]) or x["failed"]:
                    r.flag("resolve-all", t, node=n, missing=sorted(want - set(x["got"])), failed=x["failed"])
            elif ev == "tmp_up":
                tmp[(inc, x["id"])] = [t, None]
            elif ev == "tmp_stop" and (inc, x["id"]) in tmp:
                tmp[(inc, x["id"])][1] = t
        for (i, _), (up, stop) in tmp.items():
            due = up + TMP_IDLE + 10 * S
            if 0 < i <= len(incs(r, n)) and incs(r, n)[i - 1][1] < due or r.exempt(n, up, due):
                continue
            r.judged["spawn-idle"] += 1
            if stop is None or stop > due:
                r.flag("spawn-idle", due, node=n, up=up, stop=stop)
        # echo-liveness: each target answers this caller again within B of the last episode around a silence.
        targets = {x["to"] for x in xs if x.get("r") in ("ok", "err")}
        oks = collections.defaultdict(list)
        for x in xs:
            if x.get("r") == "ok":
                oks[(x["to"], x["_inc"])].append(x["_t"])
        for i, (u, z) in enumerate(incs(r, n), 1):
            for to in sorted(targets):
                ts = [u] + oks.get((to, i), []) + [z]
                for a, b in zip(ts, ts[1:]):
                    if b <= a + r.B + r.slop.get(n, 0):  # due is never earlier: calls a second apart skip its search
                        r.judged["echo-liveness"] += 1
                        continue
                    due = due_after(a)
                    # A target that came up during the silence (nodes boot one after another) has B from its up.
                    up = next((v for v, w in incs(r, to) if v <= due <= w), None)
                    if up is not None and up > a:
                        due = due_after(up)
                    if b <= due + r.slop.get(n, 0) or not alive(r, to, due) or r.fault_on(n, CLOCK, a, b) or \
                            r.fault_on(to, CLOCK, a, b):
                        r.judged["echo-liveness"] += b <= due + r.slop.get(n, 0)
                        continue
                    r.judged["echo-liveness"] += 1
                    r.flag("echo-liveness", due, node=n, to=to, silent_from=a, heard=b)
                    break


def check_idle(r):
    """idle-race: an op sent within 1 s of an idle stop (`Drained`) of its key, in a quiet window, is answered within
    IDLE_RACE: a call racing the drain is neither lost nor held until a retry gives up."""
    drains = collections.defaultdict(list)
    for nd in r.nodes.values():
        for x in nd["node"]:
            if x.get("r") == "stop" and x.get("why") == "Drained":
                drains[x["k"]].append(x["_t"])
    for v in drains.values():
        v.sort()
    for op, (node, st, k, _) in r.sends.items():
        ds = drains.get(k)
        if not ds:
            continue
        j = bisect.bisect_left(ds, st - S)
        if j == len(ds) or ds[j] > st + S or not r.quiet(st, st + IDLE_RACE):
            continue
        a = r.acks.get(op)
        if a is None and r.inc_last.get(op.rpartition(".")[0], 0) < st + IDLE_RACE:
            continue  # its client died first
        r.judged["idle-race"] += 1
        if a is None or a[0] - st > IDLE_RACE:
            r.flag("idle-race", st, op=op, k=k, node=node, drained=ds[j], acked=a and a[0])


CHECKS += [check_compacted, check_eager, check_watch, check_echo, check_idle]


def refused(r, f):
    """The node's echo workload refused the fault's peers (an `authz` line naming them) within 3 s of its on, and
    none within 3 s of its off."""
    want = sorted(f["args"].get("to") or ())
    for n in f["nodes"]:
        if not r.lane(n, "echo.jsonl"):
            return None, "%s runs no echo" % n
        if r.fault_on(n, HALTS | CLOCK | {"nofile"}, f["on"][0], f["off"][1] + 3 * S):
            return None, "%s stopped, restarted, ran out of descriptors or had its clock moved" % n
        xs = [x for x in r.lane(n, "echo.jsonl") if x.get("r") == "authz"]
        on = any(f["on"][0] <= x["_t"] <= f["on"][1] + 3 * S and sorted(x["refuse"]) == want for x in xs)
        off = any(f["off"][0] <= x["_t"] <= f["off"][1] + 3 * S and not x["refuse"] for x in xs)
        if not (on and off):
            return False, "%s refused %s: set %s, cleared %s" % (n, want, on, off)
    return True, "refused"


EFFECT["authz"] = [refused]


def synth_keyed(files, info, lives, nline):
    """b1 originates b1/c0, which b2 snapshots and b3 folds every 20 s; b1 hosts eager keys e0..e4 all run and watches
    k0..k4, shown each owner the store granted 20 ms after; every node calls another each second and resolves every
    10 s; b1 hosts a tmp actor every 10 s; and one op on k-key races a Drained stop of its key, answered at once."""
    T0, P = info["T0"], info["E"][1]
    off = {x["node"]: x["offset_ms"] * MS for x in files["nemesis.jsonl"] if x.get("ev") == "clock"
           and x.get("offset_ms") is not None}
    at = lambda s: T0 + int(s * S)
    live = lambda n, t: any(a <= t < b for a, b in lives[n]) and not (n == "b3" and P["on"] <= t <= P["off"])
    for n in lives:
        for x in files[n + "/node.jsonl"]:
            if x["r"] == "up":
                x["workloads"] = x["workloads"] + ["compacted", "eager", "watch", "echo", "idle"]
    rnd, st, seq = random.Random(7), {}, 0
    nline("b1", "compacted.jsonl", at(1), r="open", s="b1/c0", g=1, max_keys=32)
    for s in range(2, 290):
        seq += 1
        k, v = "K%d" % rnd.randrange(6), "" if rnd.random() < 0.2 else "v%d" % seq
        nline("b1", "compacted.jsonl", at(s), r="pub", s="b1/c0", g=1, seq=seq, k=k, v=v)
        put(st, k, v)
        for who, ev in (("b2", "snap"), ("b3", "fold")):
            if s % 20 == 0 and live(who, at(s) + 5 * MS):
                nline(who, "compacted.jsonl", at(s) + 5 * MS, r=ev, s="b1/c0", g=1, through=seq, state=dict(st))
    nline("b1", "eager.jsonl", at(9), r="spawned", keys=5)
    for k in range(5):
        nline("b1", "node.jsonl", at(8), r="start", k="e%d" % k, e=1)
    for x in sorted((x for x in files["store.jsonl"] if x["r"] == "O" and x["k"] in info["keys"][:5]),
                    key=lambda x: x["t"]):
        nline("b1", "watch.jsonl", x["t"] + 20 * MS, r="own", k=x["k"], n=x["n"], e=x["e"])
    nodes, ids = sorted(lives), collections.Counter()
    for s in range(1, 299):
        for j, c in enumerate(nodes):
            to, t = nodes[(j + 1 + s % 2) % 3], at(s) + j * 100 * MS
            if live(c, t) and live(to, t + 2 * MS) and live(c, t + 4 * MS):
                ids[c] += 1
                nline(to, "echo.jsonl", t + 2 * MS, r="ran", id=ids[c], sent=t + off[c], within_ms=1000, tmp=None,
                      **{"from": c})
                nline(c, "echo.jsonl", t + 4 * MS, r="ok", to=to, id=ids[c], sent=t + off[c], ms=4)
            if s % 10 == 0 and live(c, t):
                nline(c, "echo.jsonl", t, r="resolve", got=[m for m in nodes if m != c and live(m, t)], failed=[])
        if s % 10 == 0 and 20 <= s <= 280:
            nline("b1", "echo.jsonl", at(s), r="tmp_up", id=s)
            nline("b1", "echo.jsonl", at(s) + 3100 * MS, r="tmp_stop", id=s, why="Drained")
    x = next(x for n in nodes for x in files[n + "/client.jsonl"] if x["r"] == "send"
             and at(45) <= x["t"] - off[n])
    node = x["op"].split(".")[0]
    nline("b1", "node.jsonl", x["t"] - off[node] + 300 * MS, r="stop", k=x["k"], e=0, why="Drained")
    info["idle_op"] = x["op"]


SYNTH_EXTRAS.append(synth_keyed)

def add_geo(f, i, name, on, off, faults=()):
    """Episode 4 of a TIMING=wan run: the scenario name, geo on b1..b3 from on to off s, and faults, each (class, nodes,
    on, off, args)."""
    i["meta"]["timing"] = "wan"
    fs = [("geo", ["b1", "b2", "b3"], on, off, {})] + list(faults)
    for j, (cls, nodes, a, b, args) in enumerate(fs):
        nem_fault(f, i, 4, j, cls, nodes, a, b, args)
    f["plan.jsonl"].append(dict(i=4, at=on, dur=off - on, name=name,
                                faults=[{"class": c, "nodes": n, "args": a} for c, n, _, _, a in fs]))


@plant(want=["isolation-quiet"])
def p_isolation_quiet(f, i):
    """A wan run, b2 isolated in a quiet window at 45 s."""
    i["meta"]["timing"] = "wan"
    bump(f, "b2", i["T0"] + 45 * S, "isolations")


@plant(want=["isolation-quiet"], probe=["readyz-isolated"])
def p_isolation_quiet_cut(f, i):
    """Under a geo-partition-short, b3 isolated by a cut of 7 s: sooner than an unplanted node can (isolate-early.sh).
    Its /readyz said ready through it, under READY_BY_WAN: a probe."""
    add_geo(f, i, "geo-partition-short", 284, 296, [("isolate", ["b3"], 286, 293, {})])
    bump(f, "b3", i["T0"] + 290 * S, "isolations")


@plant(want=[])
def p_isolation_quiet_long(f, i):
    """The same, cut for 10 s, longer than ISOLATES_AFTER, and isolated on /readyz."""
    add_geo(f, i, "geo-partition-short", 284, 296, [("isolate", ["b3"], 286, 296, {})])
    bump(f, "b3", i["T0"] + 290 * S, "isolations")
    readyz_b3(f, i, isolated_503)


def wan_bulk(f, i):
    add_geo(f, i, "wan-bulk", 231, 268, [("bulk", ["b1"], 232, 267, {})])
    for x in f["b1/node.jsonl"]:
        if x["r"] == "up":
            x["workloads"] = x["workloads"] + ["bulk"]


@plant(want=["live-under-bulk"])
def p_live_under_bulk(f, i):
    """Under bulk uploads, b1's echo calls to b2 take 590 ms, over the 190 ms round trip and LIVE_SLACK
    (bulk-on-live.sh)."""
    wan_bulk(f, i)
    for x in f["b1/echo.jsonl"]:
        if x["r"] == "ok" and x["to"] == "b2" and i["T0"] + 240 * S <= x["t"] < i["T0"] + 260 * S:
            x["ms"] = 590


plant("live-under-bulk-clean", [])(wan_bulk)


def firehose(f, i, recv=lambda s: 1.0):
    """b1 publishes 13 MB a second and b2 and b3 receive recv(s) of it, 150 ms behind, under a geo-quiet from 200 s;
    b1's push windows to them every second."""
    add_geo(f, i, "geo-quiet", 200, 269)
    for s in range(226, 299):
        t = i["T0"] + s * S + 300 * MS
        f["b1/firehose.jsonl"].append(dict(t=t, r="pub", s="firehose/b1", bytes=13_000_000, records=13))
        for n in ("b2", "b3"):
            f[n + "/firehose.jsonl"].append(dict(t=t + 150 * MS, r="recv", s="firehose/b1", records=13, lag_ms=150,
                                                 bytes=int(13_000_000 * recv(s))))
        down = [dict(node=n, lane="Bulk", channel=0, window=4 << 20, inflight=2 << 20, pushes=3, resent=0,
                     rtt_us=190_000, rate=13_000_000) for n in ("b2", "b3")]
        f["b1/firehose.jsonl"].append(dict(t=t, r="window", downstream=down, push_inflight_bytes=4 << 20,
                                           push_resent=0))
    for n in ("b1", "b2", "b3"):
        for x in f[n + "/node.jsonl"]:
            if x["r"] == "up":
                x["workloads"] = x["workloads"] + ["firehose"]


plant("stream-throughput-clean", [])(firehose)
# b2's Live follower skips 300 records of generation 1 at 250 s, after a Ready (window-floor's run, or a relay's skip)
plant("firehose-gap")(lambda f, i: (firehose(f, i), f["b2/firehose.jsonl"].append(
    dict(t=i["T0"] + 250 * S, r="gap", s="firehose/b1", g=1, after=1200, first=1501, since=["ready"]))))
# b2 receives 60 % of what b1 publishes from 240 s to 250 s (window-floor.sh)
plant("stream-throughput")(lambda f, i: firehose(f, i, lambda s: 0.6 if 240 <= s < 250 else 1.0))


@plant()
def p_stream_window(f, i):
    """At 250 s 9 MiB in flight to b2, past its 4 MiB window (no-window.sh)."""
    firehose(f, i)
    x = next(x for x in f["b1/firehose.jsonl"] if x["r"] == "window" and x["t"] >= i["T0"] + 250 * S)
    x["downstream"][0] = dict(x["downstream"][0], inflight=9 << 20)


@plant()
def p_compact_fold(f, i):
    """b2's snapshot of b1/c0 at 40 s lacks one of its keys."""
    x = first_line(f, "b2/compacted.jsonl", lambda x: x["r"] == "snap" and x["t"] > i["T0"] + 30 * S and x["state"])
    x["state"].pop(sorted(x["state"])[0])


# b1 refused a publish as full, holding six keys of its 32.
plant_line("compact-origin", "b1/compacted.jsonl", 100.5, r="full", s="b1/c0", k="K1")
# A snapshot follower reset for a gap during the diskdrop, which no fault explains.
plant_line("compact-reset", "b2/compacted.jsonl", (2, "on", 5), ["stream-reset-kind"], r="reset", s="b1/c0", why="Gap")
# b1's e3 stopped and no node started it again.
plant_line("eager-restart", "b1/node.jsonl", 100, r="stop", k="e3", e=1, why="Lost")
# Fenced while b1 stayed up, which b1 starts again itself.
plant_line("eager-fenced", "b1/node.jsonl", 100, ["eager-restart"], r="stop", k="e3", e=1, why="Fenced")


@plant(want=[])
def p_eager_last_line(f, i):
    """b2 took e3 over at 150 s and ran it until the skew-restart at 200 s, but logged nothing after 181 s: running
    at 184 s, when its last line was already 3 s old (pairs-20260930-2346-s50, b3 before an acceptors-restart)."""
    f["b1/node.jsonl"].append(dict(t=i["T0"] + 150 * S, r="stop", k="e3", e=1, why="Handoff"))
    f["b2/node.jsonl"].append(dict(t=i["T0"] + 150 * S + 3 * MS + 50 * MS, r="start", k="e3", e=2))
    up = min(x["t"] for x in f["b2/node.jsonl"] if x["r"] == "up" and x["t"] > i["T0"] + 181 * S)
    f["b2/node.jsonl"] = [x for x in f["b2/node.jsonl"] if not i["T0"] + 181 * S < x["t"] - 3 * MS < up - 3 * MS]
    f["b1/node.jsonl"].append(dict(t=i["T0"] + 206 * S, r="start", k="e3", e=3))


@plant()
def p_watch_epoch(f, i):
    """b1's watch showed k1's first owner again at 250 s, long after newer ones (b2's kill moved it)."""
    x = first_line(f, "b1/watch.jsonl", lambda x: x["k"] == i["keys"][1])
    f["b1/watch.jsonl"].append(dict(x, t=i["T0"] + 250 * S))


@plant()
def p_watch_owner(f, i):
    """b1's watch showed k1's latest epoch on a node that never owned it."""
    x = [x for x in f["b1/watch.jsonl"] if x["k"] == i["keys"][1]][-1]
    f["b1/watch.jsonl"].append(dict(x, t=x["t"] + 1, n=next(n for n in ("b1", "b2", "b3") if n != x["n"])))


# b2 ran a call from b1 at 100 s that b1 sent 1.5 s before with a deadline of 1 s.
plant("echo-deadline")(lambda f, i: f["b2/echo.jsonl"].append({
    "t": i["T0"] + 100 * S + 3 * MS, "r": "ran", "from": "b1", "id": 999999, "sent": i["T0"] + 98 * S + 500 * MS,
    "within_ms": 1000, "tmp": None}))


@plant("echo-deadline-swapped", [])
def p_echo_deadline_swapped(f, i):
    """A call 220 ms late as b2's swap went off, paging back in for 1.3 s (pairs-20261001-2342-s1002, episode 245)."""
    add_episode(f, i, 4, "swap", ["b2"], 94, 100.1)
    f["b2/echo.jsonl"].append({"t": i["T0"] + 100 * S, "r": "ran", "from": "b1", "id": 999999,
                               "sent": i["T0"] + 98 * S + 780 * MS, "within_ms": 1000, "tmp": None})


def authz_episode(f, i, lines=True):
    """b1 refuses b2 from 240 s to 250 s; b2's calls to b1 then fail."""
    add_episode(f, i, 4, "authz", ["b1"], 240, 250, to=["b2"])
    lo, hi = i["T0"] + 240 * S, i["T0"] + 251 * S + 500 * MS
    f["b1/echo.jsonl"] = [x for x in f["b1/echo.jsonl"] if not (x["r"] == "ran" and x["from"] == "b2"
                                                                 and lo <= x["t"] <= hi)]
    f["b2/echo.jsonl"] = [x for x in f["b2/echo.jsonl"] if not (x["r"] == "ok" and x["to"] == "b1"
                                                                 and lo <= x["t"] <= hi + 10 * MS)]
    if lines:
        f["b1/echo.jsonl"] += [dict(t=lo + 100 * MS, r="authz", refuse=["b2"]), dict(t=lo + 10 * S + 100 * MS,
                                                                                     r="authz", refuse=[])]


# The refusal alone, as fault.sh and src/echo.rs make it: witnessed, and nothing else fails.
plant("authz-clean", [])(authz_episode)


@plant()
def p_authz_leak(f, i):
    """b1 ran a call of b2's 5 s into refusing it."""
    authz_episode(f, i)
    f["b1/echo.jsonl"].append({"t": i["T0"] + 245 * S, "r": "ran", "from": "b2", "id": 999999,
                               "sent": i["T0"] + 245 * S - 3 * MS, "within_ms": 1000, "tmp": None})


# An authz fault on b1 that its echo workload never took up: no `authz` line.
plant("fault-effect-authz", ["fault-effect"])(lambda f, i: authz_episode(f, i, lines=False))
# b1's lookup at 60 s, in a quiet window, did not find b3.
plant("resolve-all")(lambda f, i: first_line(f, "b1/echo.jsonl", lambda x: x["r"] == "resolve" and x["t"] >= i["T0"]
                                             + 60 * S)["got"].remove("b3"))
# b1's tmp actor of 60 s never stopped.
plant("spawn-idle")(lambda f, i: f["b1/echo.jsonl"].remove(first_line(f, "b1/echo.jsonl", lambda x: x["r"] == "tmp_stop"
                                                                     and x["id"] == 60)))


@plant()
def p_echo_liveness(f, i):
    """b1's calls to b2 stopped being answered at 250 s."""
    f["b1/echo.jsonl"] = [x for x in f["b1/echo.jsonl"] if not (x["r"] == "ok" and x["to"] == "b2"
                                                                 and x["t"] > i["T0"] + 250 * S)]


@plant()
def p_idle_race(f, i):
    """The op sent 300 ms before its key drained was answered 6 s later."""
    node = i["idle_op"].split(".")[0]
    send = next(x for x in f[node + "/client.jsonl"] if x["r"] == "send" and x["op"] == i["idle_op"])
    ack = next(x for x in f[node + "/client.jsonl"] if x["r"] == "ack" and x["op"] == i["idle_op"])
    ack["t"], ack["ms"] = send["t"] + 6 * S, 6000.0


def probe_at(f, i, node, path, s):
    return next(x for x in f["probes.jsonl"] if x["node"] == node and x["path"] == path and x["sent"] >= i["T0"] + s * S)


def isolate_b3(f, i, answer=None):
    """b3 isolated from 286 s to 296 s; answer(x) sets each of its /readyz answers from 288 s on."""
    add_episode(f, i, 4, "isolate", ["b3"], 286, 296)
    if answer:
        readyz_b3(f, i, answer)


def readyz_b3(f, i, answer):
    for x in f["probes.jsonl"]:
        if x["node"] == "b3" and x["path"] == "/readyz" and x["sent"] >= i["T0"] + 288 * S:
            answer(x)


def isolated_503(x):
    x.update(code=503, checks=dict(x["checks"], cluster="isolated"))


# b3's /readyz answers 200 through its isolate; or 503, cluster isolated, from 2 s into it until the run ends.
plant("readyz-isolated")(isolate_b3)
plant("readyz-isolated-seen", [])(lambda f, i: isolate_b3(f, i, isolated_503))


@plant(want=["readyz-isolated"])
def p_readyz_isolated_back(f, i):
    """b3 isolated, and says so, until 292 s: from READY_BY into the isolate it says ready again."""
    isolate_b3(f, i, lambda x: isolated_503(x) if x["sent"] < i["T0"] + 292 * S else None)


@plant(want=["readyz-isolated"])
def p_readyz_isolated_timeout(f, i):
    """No /readyz of b3 answered through its isolate: a timeout is not a 503."""
    isolate_b3(f, i, lambda x: (x.pop("checks"), x.update(code=None, err="timeout: timed out")))


# For 5 s, the plan's shortest, and ready throughout: healed before it need have seen it.
plant_episode("readyz-isolated-short", [], "isolate", ["b3"], 286, 291, probe=["readyz-isolated"])


@plant(want=[])
def p_readyz_isolated_lone_seed(f, i):
    """SEEDS=1 and b1, the one seed, isolated and ready throughout: it reaches its own seed, all there is."""
    i["meta"]["seeds"] = "1"
    add_episode(f, i, 4, "isolate", ["b1"], 286, 296)


@plant(want=ANY, rc=2)
def p_tls_unjudged(f, i):
    """A TLS run with no tls.jsonl: its CA never rotated."""
    i["meta"]["tls"] = "1"
    del f["tls.jsonl"]


@plant(want=ANY, rc=2)
def p_local_unjudged(f, i):
    """The local workload ran, and no local.jsonl: its invariants judged nothing."""
    for k in [k for k in f if k.endswith("/local.jsonl")]:
        del f[k]


@plant(want=ANY, rc=2)
def p_readyz_unjudged(f, i):
    """An isolate, and no probes.jsonl: the run has not shown what /readyz says while isolated."""
    isolate_b3(f, i)
    del f["probes.jsonl"]


# b1's /readyz says it is isolated at 50 s, in a quiet window.
plant("probe-quiet")(lambda f, i: isolated_503(probe_at(f, i, "b1", "/readyz", 50)))


@plant(want=["probe-quiet"])
def p_probe_quiet_timeout(f, i):
    """b2's /healthz timed out at 100 s, in a quiet window."""
    x = probe_at(f, i, "b2", "/healthz", 100)
    x.update(code=None, err="timeout: timed out", t=x["sent"] + 800 * MS)
    del x["checks"]


plant_line("probe-quiet-health", "b3/node.jsonl", 2, ["probe-quiet"], r="health", addr="10.77.0.3:7401",
           err="Address in use")


@plant()
def p_metrics_monotonic(f, i):
    """A counter of b1's /metrics goes back at 61 s."""
    x = probe_at(f, i, "b1", "/metrics", 60)
    x["counters"] = {k: 0.0 for k in x["counters"]}


# b2 never set the CA rotation's second pass; b1 refused the third's certificates; b2 restarted after its kill with
# the first pass, two passes after it.
plant("tls-rotated")(lambda f, i: f["b2/node.jsonl"].remove(first_line(f, "b2/node.jsonl", lambda x: x["r"] == "tls"
                                                                       and x["pass"] == "p2")))
plant("tls-rotated-err", ["tls-rotated"])(lambda f, i: first_line(
    f, "b1/node.jsonl", lambda x: x["r"] == "tls" and x["pass"] == "p3").update(err="invalid certificate"))
plant("tls-rotated-boot", ["tls-rotated"])(lambda f, i: first_line(
    f, "b2/node.jsonl", lambda x: x["r"] == "tls" and x["at"] == "boot" and x["t"] > i["T0"] + 80 * S).update(
    {"pass": "p0"}))


@plant()
def p_handshake_quiet(f, i):
    """b1 counts a handshake failure between 95 s and 105 s, in a quiet window."""
    for x in f["b1/node.jsonl"]:
        if x["r"] == "stats" and x["t"] > i["T0"] + 100 * S:
            x["net"]["handshake_failures"][2] += 1


plant_line("blob-corrupt", "b2/blob.jsonl", 150, r="blob_bad", to="b1", bytes=3 << 20, back=3 << 20, first_diff=2097152)
# Three of b3's echoes failed in the window to 60 s, a quiet one.
plant("blob-quiet")(lambda f, i: first_line(f, "b3/blob.jsonl", lambda x: x["t"] >= i["T0"] + 60 * S).update(
    err=3, errs={"NoReply": 3}))


@plant()
def p_local_lost(f, i):
    """A subscriber on b1 missed 5 messages before 60 s."""
    for x in f["b1/local.jsonl"]:
        if x["t"] >= i["T0"] + 60 * S:
            x["gaps"] += 5


@plant(want=[])
def p_local_catch_up(f, i):
    """memhigh on b1 until 1 s before a local line, and 5 gaps by the next: the publisher's interval fired the ticks
    the throttle held back at once, past the subscribers' mailboxes (chaos-20260930-2245-s47)."""
    x = first_line(f, "b1/local.jsonl", lambda x: x["t"] >= i["T0"] + 245 * S)
    at = (x["t"] - i["T0"]) // S
    add_episode(f, i, 4, "memhigh", ["b1"], at - 30, at - 1, pct=80)
    for y in f["b1/local.jsonl"]:
        if y["t"] > x["t"]:
            y["gaps"] += 5


# A flaky family on b2 gave up at 110 s; b1's timer ran 3 s late before 60 s, nothing holding it.
plant("supervision-bound")(lambda f, i: first_line(f, "b2/local.jsonl", lambda x: x["t"] >= i["T0"] + 110 * S).update(
    families=3))
plant("timer-step")(lambda f, i: first_line(f, "b1/local.jsonl", lambda x: x["t"] >= i["T0"] + 60 * S).update(
    late_max_ms=3000))


@plant(want=["fault-effect"])
def p_local_held(f, i):
    """b1's timer ran 3 s late in a window a hook on it held, and b2's flaky families were 2 after a killterm's TERM
    (the synthetic b2 does not start again, hence fault-effect)."""
    add_episode(f, i, 4, "hook", ["b1"], 240, 250, hook="block", ms=1000)
    first_line(f, "b1/local.jsonl", lambda x: x["t"] >= i["T0"] + 250 * S).update(late_max_ms=3000)
    add_episode(f, i, 5, "killterm", ["b2"], 240, 250, kill_ms=1000, terms=1)
    first_line(f, "b2/local.jsonl", lambda x: x["t"] >= i["T0"] + 241 * S).update(families=2)


@plant()
def p_local_quiet(f, i):
    """An ask of b3's router failed before 60 s."""
    for x in f["b3/local.jsonl"]:
        if x["t"] >= i["T0"] + 60 * S:
            x["asks_err"] += 1


# b1 dead-lettered tells to an account at 65 s.
plant("dead-quiet")(lambda f, i: first_line(f, "b1/node.jsonl", lambda x: x["r"] == "dead" and x["t"] >= i["T0"]
                                            + 60 * S)["by"].update({"full tell accounts/k3": 2}))


def hook_panics(f, i, on, restarted):
    """A panic hook on b1 from on to on + 10 s: its hook lines, three panics from 1 s in, each in the unit's log as
    hooks.rs does, and its stats' supervised restarts moving from 0 to `restarted` after the first."""
    add_episode(f, i, 4, "hook", ["b1"], on, on + 10, hook="panic", ms=0)
    t0, nl = i["T0"] + on * S, f["b1/node.jsonl"]
    nl += [dict(t=t0 + 500 * MS, r="hook", hook="panic", ms=0), dict(t=t0 + 10500 * MS, r="hook", hook="off", ms=0)]
    for j in range(3):
        nl.append(dict(t=t0 + S + j * MS, r="panic", k="k%d" % j, hook="panic"))
        f["b1/unit.jsonl"].insert(0, {"__REALTIME_TIMESTAMP": str(t0 + S + j * MS),
                                      "MESSAGE": "thread 'axon-worker-1' panicked at examples/rig/src/hooks.rs:58:13:"})
    for x in nl:
        if x["r"] == "stats":
            x["restarts"] = restarted if x["t"] > t0 + S else 0


# A panic hook on b1 whose panics each restarted a key's actor: witnessed, and the panics in the unit's log explained;
# and the same with no supervised restart counted: the panics went uncaught somewhere.
plant("panic-hook", [])(lambda f, i: hook_panics(f, i, 240, 3))
plant("panic-contained")(lambda f, i: hook_panics(f, i, 240, 0))


@plant(want=["panic-contained", "unexpected-restart"])
def p_panic_escaped(f, i):
    """A panic hook on b1 from 270 s whose panic took the process down: b1 up again at 296 s."""
    hook_panics(f, i, 270, 3)
    f["b1/node.jsonl"].append(dict(t=i["T0"] + 296 * S, r="up", node="b1", inc=(i["T0"] + 296 * S) // MS))


@plant()
def p_peer_leak(f, i):
    """b1 holds 20 more fds after b2's kill."""
    next(x for x in f["b1/node.jsonl"] if x["r"] == "res" and x["t"] == i["T0"] + 95 * S)["fds"] += 20


def late_at_256(f, i, sender, owner):
    """The first op sender sent from 242 s on a key owner owns, acked at 256 s."""
    owns = {x["k"]: x["n"] for x in f["store.jsonl"] if x["r"] == "O"}
    s = first_line(f, sender + "/client.jsonl", lambda x: x["r"] == "send" and x["t"] >= i["T0"] + 242 * S and owns[
        x["k"]] == owner)
    a = first_line(f, sender + "/client.jsonl", lambda x: x["r"] == "ack" and x["op"] == s["op"])
    a["t"] = i["T0"] + 256 * S
    a["ms"] = (a["t"] - s["t"]) / MS


def plant_during(name, want, sender, owner, *eps, acceptors=None):
    """Episodes 4, 5, .. of eps, each (class, node, args), from 240 s to 250 s, and late_at_256(sender, owner)."""
    def fn(f, i):
        if acceptors:
            i["meta"]["acceptors"] = acceptors
        for e, (cls, node, args) in enumerate(eps, 4):
            add_episode(f, i, e, cls, [node], 240, 250, **args)
        late_at_256(f, i, sender, owner)
    plant(name, want)(fn)


# cpu on b1 and memhigh on b3, the op on a key of b2's: late by 3T + 5 s where b1 still runs (quota 20 %), unjudged
# where it is starved too (5 %), the acceptors left then a minority.
plant_during("during-combo", ["liveness-during"], "b2", "b2", ("cpu", "b1", {"quota_pct": 20}),
             ("memhigh", "b3", {"pct": 80}))
plant_during("during-starved", [], "b2", "b2", ("cpu", "b1", {"quota_pct": 5}), ("memhigh", "b3", {"pct": 80}))


def during_rate(blob):
    def fn(f, i):
        """rate on b1 and memhigh on b3 from 240 s to 250 s, and an op b2 sent at 242 s, on a key b2 owns, acked at
        256 s: late where b1's link carries only the leases, unjudged under the blob workload, whose frames the lease
        messages wait behind (chaos-20260930-2213-s46)."""
        add_episode(f, i, 4, "rate", ["b1"], 240, 250, mbit=4.8)
        add_episode(f, i, 5, "memhigh", ["b3"], 240, 250, pct=80)
        late_at_256(f, i, "b2", "b2")
        if not blob:  # the synthetic run has it on every node (synth_surface)
            for x in [x for name in f if name.endswith("/node.jsonl") for x in f[name]]:
                if x["r"] == "up":
                    x["workloads"] = [w for w in x["workloads"] if w != "blob"]
    return fn


plant("during-rate", want=["liveness-during"])(during_rate(False))
plant("during-rate-blob", want=[])(during_rate(True))


# memhigh on the owner, b2: unjudged, the owner starved while it keeps its lease.
plant_during("during-starved-owner", [], "b1", "b2", ("memhigh", "b2", {"pct": 80}))
# Acceptors b1 and b2 alone, b3 cut from b1 through b2, the op on a key of b1's: unjudged, b2 reaching b3, which
# reaches one acceptor of two and may still be where b2 places the key.
plant_during("during-stranded", ["fault-effect"], "b2", "b1", ("bridge", "b3", {"other": ["b1"], "via": "b2"}),
             acceptors=["b1", "b2"])
# b2 cut from b1 alone, both still in the majority through b3, the op on a key of b1's: unjudged, the owner out of the
# sender's reach while it keeps its lease. The synthetic run's other ops cross the cut, which fault-effect names.
plant_during("during-cut", ["fault-effect"], "b2", "b1", ("partition", "b2", {"other": ["b1"]}))
# Acceptors b1 and b2 alone, b3's port to b1 cut, the op on a key of b1's: unjudged where the port is the cluster's,
# 7400, a cut that leaves b3 one acceptor of two, where b2 may still place the key (chaos-20261001-1537-s62); late
# where it is the seed port, 7001.
for _name, _want, _port in (("during-portcut", ["fault-effect"], 7400),
                            ("during-portcut-seed", ["liveness-during"], 7001)):
    plant_during(_name, _want, "b2", "b1", ("portcut", "b3", {"port": _port, "to": ["b1"]}), acceptors=["b1", "b2"])


@plant(want=["fault-effect"])
def p_during_netem_acceptor(f, i):
    """netem on b1 and b3 cut from b2 from 240 s to 250 s, and an op b2 sent at 242 s on a key b2 owns, acked at
    256 s: unjudged, b2's acceptors then itself and the netem'd b1, as b5's in chaos-20261001-1537-s62."""
    add_episode(f, i, 4, "netem", ["b1"], 240, 250, delay_ms=267, jitter_ms=83, loss_pct=9.0)
    add_episode(f, i, 5, "oneway", ["b3"], 240, 250, to=["b2"])
    late_at_256(f, i, "b2", "b2")


# The owner, b1, isolated: late by 3T + 5 s, the owner cut off from the acceptors and so its lease, whose keys must
# move. The synthetic probes never saw b1's /readyz say isolated, which readyz-isolated names.
plant_during("during-isolated-owner", ["liveness-during", "fault-effect", "readyz-isolated"], "b2", "b1",
             ("isolate", "b1", {}))


@plant()
def p_zombie(f, i):
    """b3 starts an actor of an epoch older than b1's of a b1 key at 30 s, and never stops it."""
    o = next(x for x in f["store.jsonl"] if x["r"] == "O" and x["n"] == "b1" and x["k"] in i["pk"])
    f["b3/node.jsonl"].append(dict(t=i["T0"] + 30 * S, r="start", k=o["k"], e=o["e"] - 1))


@plant(want=[])
def p_zombie_memhigh(f, i):
    """As p_zombie, at 240 s, while memory.high throttles b3 from 240 s to 262 s, and stopped at 275 s: late, as a
    stopped node's actors are, and within ZOMBIE of the throttle's end."""
    o = next(x for x in f["store.jsonl"] if x["r"] == "O" and x["n"] == "b1" and x["k"] in i["pk"])
    add_episode(f, i, 4, "memhigh", ["b3"], 240, 262, pct=90)
    f["b3/node.jsonl"] += [dict(t=i["T0"] + 240 * S, r="start", k=o["k"], e=o["e"] - 1),
                           dict(t=i["T0"] + 275 * S, r="stop", k=o["k"], e=o["e"] - 1, why="Fenced")]


@plant(want=["zombie", "turn-after-acquire"])
def p_zombie_unowned(f, i):
    """b3 starts an actor of a newer epoch of a key b1 always owns at 30 s, whose own never reaches the store, and
    stops it at 31 s: b1's actor runs on, and writes, with no newer grant in the store's log."""
    o = next(x for x in f["store.jsonl"] if x["r"] == "O" and x["n"] == "b1" and x["k"] not in i["pk"])
    f["b3/node.jsonl"] += [dict(t=i["T0"] + 30 * S, r="start", k=o["k"], e=o["e"] + 1),
                           dict(t=i["T0"] + 31 * S, r="stop", k=o["k"], e=o["e"] + 1, why="Drained")]


@plant()
def p_turn_after_acquire(f, i):
    """The new owner of a key moved off b3 at the pause started its actor 100 ms before b3's last write of it."""
    P = i["E"][1]
    o = next(x for x in f["store.jsonl"] if x["r"] == "O" and P["on"] < x["t"] < P["off"] and x["k"] in i["pk"])
    w = last(f, o["k"], lambda x: x["n"] == "b3" and x["t"] < P["on"])
    first_line(f, o["n"] + "/node.jsonl", lambda x: x["r"] == "start" and x["k"] == o["k"] and x["e"] == o["e"])[
        "t"] = w["t"] - 100 * MS
    return o["n"]


@plant(want=[])
def p_turn_after_acquire_drift(f, i):
    """As turn-after-acquire, the new owner's clock drifting meanwhile: its start line's time is not trusted."""
    add_episode(f, i, 4, "drift", [p_turn_after_acquire(f, i)], 110, 140, k=-23)


@plant()
def p_blast_radius(f, i):
    """30 of b3's ops written by b2 during b1's diskdrop took 3 s."""
    D = i["E"][2]
    ops = sorted(x["op"] for x in f["store.jsonl"] if x["r"] == "A" and x["n"] == "b2" and x["op"].startswith("b3.")
                 and D["on"] + S < x["t"] < D["off"])[:30]
    slow_acks(f, "b3", ops, 3000.0)


@plant(want=["latency-quiet"])
def p_quiet_p99(f, i):
    """One in 20 of b1's ops sent from 40 s to 50 s took 4 s, the median still fast."""
    lo, hi = i["T0"] + 40 * S, i["T0"] + 50 * S
    slow_acks(f, "b1", sorted(x["op"] for x in f["b1/client.jsonl"] if x["r"] == "send" and lo <= x["t"] < hi)[::20],
              4000.0)


def settles(f, node, lo, hi):
    return [x for x in f["nemesis.jsonl"] if x.get("ev") == "sockets" and x["node"] == node and lo < x["t"] < hi]


@plant()
def p_sockets(f, i):
    """b2 kept three connections its peers had closed through the settles after the pause and the diskdrop."""
    for x in settles(f, "b2", i["E"][1]["off"], i["E"][2]["off"] + 10 * S):
        x["close_wait"] = 3


@plant(want=["sockets"])
def p_sockets_peer(f, i):
    """b3 held one connection to b1 past PER_PEER through the settles after the kill and the pause."""
    for x in settles(f, "b3", i["E"][0]["off"], i["E"][1]["off"] + 10 * S):
        x["estab"]["b1"] = PER_PEER + 1


@plant(want=[], rc=2)
def p_sockets_unsampled(f, i):
    """No settle's `ss` answered: UNJUDGED, not a pass with no socket seen."""
    f["nemesis.jsonl"] = [x for x in f["nemesis.jsonl"] if x.get("ev") != "sockets"]


@plant(want=["sockets"])
def p_sessions(f, i):
    for x in [x for x in f["b1/node.jsonl"] if x["r"] == "res"][:2]:
        x["sessions"] = 40


@plant(want=[])
def p_sessions_redial(f, i):
    """b2 counted one session too many for one line, a peer's redial beside its old session
    (chaos-20261001-1306-s52)."""
    [x for x in f["b2/node.jsonl"] if x["r"] == "res"][5]["sessions"] = 2 * LANES * 4 + 1


@plant(want=[])
def p_sessions_beside(f, i):
    """b1 counted one session too many while a stale-incarnation probe of b2 held one open (pairs-20260930-2155-s45)."""
    x = next(x for x in f["b1/node.jsonl"] if x["r"] == "res" and x["t"] > i["T0"] + 240 * S)
    at = (x["t"] - i["T0"]) // S
    add_episode(f, i, 4, "peer", ["b1"], at - 2, at + 4, mode="stale")
    x["sessions"] = 2 * LANES * 4 + 1


plant_log("kernel", "b2/kernel.jsonl", 100, "nf_conntrack: nf_conntrack: table full, dropping packet")
# Capitalised as the kernel writes it, with no backlog on b2.
plant_log("kernel-syn", "b2/kernel.jsonl", 100,
          "TCP: request_sock_TCP: Possible SYN flooding on port [10.77.0.2]:7400. Sending cookies.", ["kernel"])
# Before the run's first node started: the log's minute before the run, not this run's.
plant_log("kernel-before", "b2/kernel.jsonl", -30, "nf_conntrack: nf_conntrack: table full, dropping packet", [])
plant_log("kernel-late", "b1/kernel.jsonl", (2, "off", 80), "EXT4-fs error (device dm-0): ext4_find_entry:1: x",
          ["kernel"])
# The box's OOM killer took another process than the rig: on one box the kernel's log is the box's.
plant_log("kernel-oom", "kernel.jsonl", 100, "Out of memory: Killed process 4242 (python3) total-vm:1kB", ["kernel"])


@plant(want=["fault-effect"])
def p_kernel_oom_rig(f, i):
    """On one box the OOM killer names b2's rig by its unit's binary, exe-b2, inside b2's oom window, and on its own box
    as exe (the synthetic b2 does not start again, hence fault-effect)."""
    add_episode(f, i, 4, "oom", ["b2"], 240, 250)
    log_line(f, "kernel.jsonl", i["T0"] + 245 * S, "Memory cgroup out of memory: Killed process 42 (exe-b2) x")
    log_line(f, "b2/kernel.jsonl", i["T0"] + 246 * S, "Out of memory: Killed process 44 (exe) x")


@plant(want=["fault-effect", "kernel"])
def p_kernel_oom_other(f, i):
    """The OOM killer took b3's rig inside b2's oom window only."""
    add_episode(f, i, 4, "oom", ["b2"], 240, 250)
    log_line(f, "kernel.jsonl", i["T0"] + 245 * S, "Memory cgroup out of memory: Killed process 43 (exe-b3) x")


@plant(want=["causality"])
def p_ack_before_commit(f, i):
    """b1 logged an ack 50 ms before the store's record of its op: the reply went out before the record."""
    r = next(x for x in f["store.jsonl"] if x["r"] == "A" and x["op"].startswith("b1.") and x["t"] > i["T0"] + 50 * S)
    first_line(f, "b1/client.jsonl", lambda x: x["r"] == "ack" and x["op"] == r["op"])["t"] = r["t"] - 50 * MS


def step_b2(f, i):
    """b2's clock 5 s ahead from 95 s to 105 s, which the nemesis's samples, at each settle, never see."""
    add_episode(f, i, 4, "step", ["b2"], 95, 105, step_s=5)
    for name, xs in f.items():
        if name.startswith("b2/") and name.endswith(".jsonl") and "unit" not in name:
            for x in xs:
                if "t" in x and i["T0"] + 95 * S <= x["t"] - 3 * MS < i["T0"] + 105 * S:
                    x["t"] += 5 * S


# The seconds where b2's clock ran twice cross their brackets, under the step, so no causality.
plant("step-mapped", [])(step_b2)


@plant(want=["liveness-after"])
def p_step_mapped_late(f, i):
    """step_b2, and an op b2 sent at about 97 s acked at 117 s: past its deadline, 115 s, judged though b2's clock was
    stepped, the clock map placing both lines."""
    step_b2(f, i)
    delay_ack(f, "b2", i["T0"] + 101500 * MS, i["T0"] + 103 * S, lambda t: i["T0"] + 117 * S + 3 * MS)


@plant()
def p_journal_divergent(f, i):
    first_line(f, "b2/journal-%s.jsonl" % i["run"], lambda x: x["t"] > i["T0"] + 50 * S)["a"] += 1


@plant(want=["turn-after-takeover"])
def p_refusal_after_kill(f, i):
    """b2's write refused 70 ms after its kill ended, of a turn begun 25 ms before: no process resumes a SIGKILL."""
    K = i["E"][0]
    o = next(x for x in f["store.jsonl"] if x["r"] == "O" and x["t"] > K["on"] and x["k"] in i["pk"])
    old = [x for x in f["store.jsonl"] if x["r"] == "O" and x["k"] == o["k"] and x["t"] < o["t"]][-1]
    x = last(f, o["k"], lambda x: x["t"] < K["on"])
    f["store.jsonl"].append(dict(x, t=K["off"] + 70 * MS, e=old["e"], n="b2", op="b2.0.999996", lag=20 * MS,
                                 v="stale", f=o["e"]))


def selftest(big):
    clean = pickle.dumps(synth(), pickle.HIGHEST_PROTOCOL)  # plants change their copy: synth once, copy each
    tmp = tempfile.mkdtemp(prefix="rig-check-")
    ok = True
    try:
        def case(name, plant, want, want_probe=(), want_rc=None):
            files, info = pickle.loads(clean)
            info["pk"] = [k for k in info["keys"] if k not in (info["late"]["k"], info["resumed"]["k"])]
            if plant:
                plant(files, info)
            d = os.path.join(tmp, name)
            write(files, info["meta"], d)
            rc, main, probe, r = check(d, quiet=True)
            got, gotp = sorted({v["inv"] for v in main}), sorted({v["inv"] for v in probe})
            good = rc == (want_rc if want_rc is not None else 1 if want else 0) and (
                want is None or got == sorted(want) and gotp == sorted(want_probe))
            print("%-4s %-24s exit %d  named %s%s" % ("ok" if good else "FAIL", name, rc, ", ".join(got) or "-",
                                                       "  probe " + ", ".join(gotp) if gotp else ""))
            if not good:
                print(open(os.path.join(d, "check.txt")).read())
            return good, r, info

        f = lambda c, **a: {"faults": [{"class": c, "nodes": ["b2"], "args": a}]}
        assert effect_floor([f("pause")], 5) == 1 and effect_floor([f("cpu"), f("diskdrop", dev="streams")], 5) == 0
        assert effect_floor([f("kill"), f("diskdrop")] * 3, 5) == 5
        base = {"mode": "night", "one_box": False, "lease_secs": 6.0, "nodes": 5, "classes": {"kill": {"n": 9, "p95": 4, "max": 5}}}
        now = dict(base, classes={"kill": {"n": 7, "p95": 6.0, "max": 7.0}})
        assert gate(now, base)[0] == 0 and gate(now, base, 1.2)[0] == 1 and gate(dict(now, mode="smoke"), base)[0] == 2
        assert gate(dict(now, nodes=2), base)[0] == 2
        nodisk = types.SimpleNamespace(disk={}, nodes={"b2": {"unit": []}})
        assert EFFECT[("readerr", "streams")][1](nodisk, {"nodes": ["b2"], "on": (0, 0), "off": (1, 1)})[0] is None
        good, r, info = case("clean", None, [])
        waived = collections.Counter(why for why, _, _ in r.journal_exempt.elements())
        skew = next(e for e in r.eps if e["classes"] == ["skew-restart"])
        E3 = info["E"][3]
        effects = {c: s for (c, s) in r.effects}
        good = good and dict(r.explained) == {"pause": 1} and set(waived) == {"diskdrop", "journal_err",
                                                                             "killed before fsync"} and (
            skew["start"], skew["end"]) == (E3["on"], E3["off"] + 40 * MS) and set(effects.values()) == {"witnessed"}
        print("%-4s clean: %d ops, refusals explained %s (the turn 10 ms after a resume), journal lines waived %s, "
              "an op in flight through a pause longer than B, a journal line glued to stale bytes, skew-restart's "
              "window from its first on to its last off, fault effects %s" % (
                  "ok" if good else "FAIL", len(r.sends), dict(r.explained), dict(waived), effects))
        ok &= good
        for name, fn, want, probe, rc in PLANTS:
            ok &= case(name, fn, want, probe, rc)[0]
        planted = {inv for _, _, want, probe, _ in PLANTS for inv in (want or []) + probe}
        missing = [inv for inv in INVARIANTS if inv not in planted]
        print("%-4s every invariant has a plant%s" % ("FAIL" if missing else "ok", ": not " + ", ".join(missing)
                                                      if missing else ""))
        ok &= not missing
        if big:
            files, info = synth(ops=big, keys=2000, seed=2)
            d = os.path.join(tmp, "big")
            write(files, info["meta"], d)
            del files
            t = time.time()
            rc, main, probe, r = check(d, quiet=True)
            took = time.time() - t
            good = rc == 0 and took < 60
            print("%-4s %d ops (%d store lines) checked in %.1f s, exit %d" % (
                "ok" if good else "FAIL", len(r.sends), len(r.store), took, rc))
            ok &= good
    finally:
        shutil.rmtree(tmp)
    print("selftest %s" % ("PASS" if ok else "FAIL"))
    return 0 if ok else 1


def gate(now, base, ratio=2.0):
    """A run's recovery.json against a baseline's: (exit code, lines). REGRESSION where a class with GATE_MIN episodes
    in both took more than ratio times the baseline's p95 or max, and GATE_SLACK; 2 where the runs differ in kind."""
    kind = ("mode", "one_box", "lease_secs", "nodes")
    if [now.get(f) for f in kind] != [base.get(f) for f in kind]:
        return 2, ["not comparable: run %s, baseline %s" % ({f: now.get(f) for f in kind}, {f: base.get(f) for f in kind})]
    rc, out = 0, []
    for c, x in sorted(now["classes"].items()):
        b = base["classes"].get(c)
        if b is None or min(x["n"], b["n"]) < GATE_MIN:
            out.append("%-14s not judged: %d episodes, baseline %s" % (c, x["n"], b and b["n"]))
            continue
        bad = [f for f in ("p95", "max") if x[f] > ratio * b[f] + GATE_SLACK]
        rc |= bool(bad)
        out.append("%-14s p95 %.2f (baseline %.2f) max %.2f (baseline %.2f)%s" % (
            c, x["p95"], b["p95"], x["max"], b["max"], " REGRESSION " + ",".join(bad) if bad else ""))
    return rc, out


def main():
    if len(sys.argv) >= 4 and sys.argv[1] == "--gate":
        load = lambda p: json.load(open(os.path.join(p, "recovery.json") if os.path.isdir(p) else p))
        rc, out = gate(load(sys.argv[2]), load(sys.argv[3]), float(sys.argv[4]) if len(sys.argv) > 4 else 2.0)
        print("\n".join(out + ["gate %s" % ("PASS", "REGRESSION", "NOT COMPARABLE")[rc]]))
        sys.exit(rc)
    if len(sys.argv) >= 2 and sys.argv[1] == "--selftest":
        sys.exit(selftest(int(sys.argv[2]) if len(sys.argv) > 2 else 1_000_000))
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    sys.exit(check(sys.argv[1])[0])


if __name__ == "__main__":
    main()
