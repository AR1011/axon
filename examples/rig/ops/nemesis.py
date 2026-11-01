#!/usr/bin/env python3
"""The rig's fault schedule, and the hands that apply it (spec/fault-rig.md § The nemesis and its schedules).

    nemesis.py plan <seed> <smoke|night> [n] [min]   the plan as JSON lines: same seed and mode, same bytes; with n,
                                                     only the episodes on b1..bn, one after another; a night of min
                                                     minutes (150) is a prefix of a longer one; on one box, without
                                                     the episodes that hold a class tagged five-box-only or global;
                                                     PLAN_VERSION=v plans that version (the newest by default)
    nemesis.py plan <seed> <chaos|pairs|scenarios> [n] [min]
                                                     the schedules (spec/fault-rig.md § The nemesis and its
                                                     schedules): overlapping faults and cascades; SHARD=k/m of
                                                     every class pair in four relations; named sequences,
                                                     SCENARIOS=a,b of them; TIMING=smoke|night their T;
                                                     TIMING=wan the geo scenarios at the WAN profile's lease
    nemesis.py version                               the plan version `plan` uses
    nemesis.py replay <rundir> <i> <n> <out.jsonl>   that run's episode i with the episodes before it whose aftermath
                                                     reached it, the group n times B + 15 s apart, to out; prints the
                                                     run's load, keys, every_ms, lease_secs, plan version, group size
    nemesis.py coverage <rundir>..                   the class pairs those runs put on together, per relation, from
                                                     their logs, against every feasible cell
    nemesis.py run <plan.jsonl> <log.jsonl> [--only i]
    nemesis.py backdrop on|off <log.jsonl>          TIMING=wan's geo delays on every node, for the whole run
                                                     applies it from b1 through `ssh <node> sudo ~/rig/fault.sh`
    nemesis.py offset <ip>..                         per node: ssh from here works, and its clock minus this one's
    nemesis.py probe <ip>                            puts every fault.sh class on that node and sees it with its own probe;
                                                     on one box, every other node's fault.sh check is clean meanwhile
    nemesis.py combos <ip> <other ip>                stacks faults on that node and across it and the other, on and off
                                                     in both orders; then a noop, a refused claim, and heal of a mix
    nemesis.py selftest                              plans deterministic, version 1 byte for byte, legality, schedules
    nemesis.py standin [journal dir]                 what the unit runs for `probe`: listens on the rig ports, burns a
                                                     core, churns memory, appends to the journal

Nodes are b1..b5 by position in ~/rig/nodes; b1 is the control box. ssh options come from b1's ~/.ssh/config, which
`scripts/rig.sh setup` writes. On one box (setup.sh --netns, marked by ~/rig/netns) each node is a network namespace
and ~/rig/in.sh takes ssh's place. Python 3.9, stdlib only.
"""
import collections
import concurrent.futures as cf
import contextlib
import hashlib
import io
import itertools
import json
import os
import random
import re
import shlex
import subprocess
import sys
import tempfile
import threading
import time
from unittest import mock

# Per class, what the planner, the checker and doctor read: network, clock, disk; restart (the node's process ends);
# global (state every node on a box shares); five-box-only (never on one box, whose clock is every node's); probe (an
# assumption probe, reported apart and never in a combination). The disk lane's classes each go on their node's own dm
# device, so all run on one box; the operations (roll .. leave-slowdisk) restart nodes, composed of fault.sh classes
# by `steps`; of the process lane, coredump (core_pattern), swap and thp are the box's.
TAGS = {c: set(t.split()) for c, t in {
    "kill": "restart", "pause": "", "freeze": "", "leave": "restart", "isolate": "network", "partition": "network",
    "oneway": "network", "bridge": "network", "netem": "network", "rate": "network", "cpu": "", "oom": "restart",
    "memhigh": "", "io": "disk", "slowdisk": "disk", "diskfull": "disk", "diskerr": "disk restart",
    "diskdrop": "disk restart", "step": "clock five-box-only", "drift": "clock five-box-only",
    "drift-far": "clock five-box-only probe", "skew-restart": "clock five-box-only probe restart",
    "authz": "",  # the rig's own authorizer, set by its echo workload from fault.sh's file (src/echo.rs)
    "peer": "",  # a malformed/confused-peer probe at the node's own ports: composable, one box, no restart
    "powercut": "disk restart", "diskhang": "disk", "rofs": "disk restart", "flaky": "disk restart",
    "splitdelay": "disk", "readerr": "disk restart", "diskdead": "disk restart", "diskcorrupt": "disk restart",
    "inodes": "disk",
    "roll": "restart", "cfgdrift": "restart probe", "scale": "restart", "cold-restart": "restart",
    "acceptors-restart": "restart", "acceptors-rolling": "restart probe", "leave-minority": "restart",
    "leave-slowdisk": "disk restart",
    "crashloop": "restart", "killterm": "restart", "tasksmax": "probe", "nofile": "", "threadstarve": "",
    "reclaim": "", "hook": "", "coredump": "restart global five-box-only", "swap": "global five-box-only",
    "thp": "global five-box-only", "takeover-step": "clock five-box-only restart",
    "drift-edge": "clock five-box-only", "step-huge": "clock five-box-only probe", "leap": "clock five-box-only probe",
    "reject": "network", "syndrop": "network", "halfopen": "network restart", "portcut": "network",
    "graylink": "network", "flap": "network", "mtu": "network", "asym": "network", "dup": "network",
    "corrupt": "network", "conntrack": "network", "backlog": "network restart", "ports": "network",
    # spec/wan.md § 12: each link delayed as between its nodes' regions, and b1's bulk uploads (src/bulk.rs)
    "geo": "network wan", "bulk": "wan",
}.items()}
VOCAB = {"network", "clock", "disk", "restart", "global", "five-box-only", "probe", "wan"}
PEER_MODES = ["secret", "garbage", "truncated", "oversized", "stall", "fuzz", "impostor", "stale"]
STREAMS_ONLY = {"readerr", "diskcorrupt", "inodes"}  # the journal only appends to one file it never reads back
# What a class holds on its node that fault.sh gives one fault at a time: it refuses a second's on with exit 3.
SOLE = dict(dict.fromkeys(("slowdisk", "diskerr", "diskdrop", "leave-slowdisk", "powercut", "diskhang", "rofs", "flaky",
                           "splitdelay", "readerr", "diskdead", "diskcorrupt"), "table"),
            netem="qdisc", rate="qdisc", asym="qdisc", dup="qdisc", corrupt="qdisc", cpu="cpu", oom="memory.max",
            memhigh="memory.high", swap="memory.high", io="io", diskfull="filler", inodes="filler",
            authz="authz", cfgdrift="override", tasksmax="tasks", nofile="nofile", threadstarve="affinity",
            hook="hook", coredump="core", thp="thp", leap="leap", geo="qdisc", bulk="bulk",
            conntrack="sysctl:net.netfilter.nf_conntrack_tcp_loose", backlog="sysctl:net.core.somaxconn",
            ports="sysctl:net.ipv4.ip_local_port_range", **dict.fromkeys(("drift", "drift-far", "drift-edge"), "tick"))
HOOKS = ("panic", "panic_start", "panic_job", "block", "jobsleep")  # src/hooks.rs
# fault.sh's arguments of a class after its name, from its args in this order.
ARGS = {"kill": ["hold_s"], "netem": ["delay_ms", "jitter_ms", "loss_pct", "reorder_pct", "dup_pct"],
        "rate": ["mbit"], "cpu": ["quota_pct"], "oom": ["pct"], "memhigh": ["pct"], "io": ["iops"],
        "slowdisk": ["delay_ms"], "step": ["step_s"], "step-huge": ["step_s"], "drift": ["k"], "drift-far": ["k"],
        "peer": ["mode"], "flaky": ["up_s", "down_s"], "splitdelay": ["read_ms", "write_ms"],
        "cfgdrift": ["var", "value"], "reject": ["how"], "syndrop": ["reset"], "portcut": ["port"],
        "graylink": ["loss_pct"], "asym": ["delay_ms"], "flap": ["up_ms", "down_ms"], "mtu": ["bytes"],
        "dup": ["dup_pct"], "corrupt": ["corrupt_pct"], "ports": ["n"], "crashloop": ["period_ms"],
        "killterm": ["kill_ms", "terms"], "tasksmax": ["headroom"], "nofile": ["headroom"],
        "threadstarve": ["busy_pct"], "reclaim": ["mb"], "hook": ["hook", "ms"], "swap": ["pct"], "thp": ["mode"]}
STOPS = {"pause", "freeze"}  # they stack with each other, never with a restart on the same node
LEDGER = {"kill", "leave", "oom", "diskerr", "diskdrop", "powercut"}  # restarts fault.sh holds per fault id: two may stack
EDGE = {"pause", "freeze", "isolate", "partition", "oneway", "bridge", "kill", "cpu", "netem", "reject", "syndrop",
        "portcut", "graylink", "threadstarve", "diskhang"}  # timed against a lease
# One node and no other named in its arguments: a role may choose the node as the fault goes on.
ROLE = {"kill", "pause", "freeze", "leave", "isolate", "cpu", "oom", "memhigh", "io", "slowdisk", "diskfull",
        "diskerr", "diskdrop", "rate", "powercut", "diskhang", "rofs", "diskdead", "halfopen", "conntrack", "backlog"}


def tagged(tag):
    return {c for c, t in TAGS.items() if tag in t}


NETWORK, CLOCK, DISK = tagged("network"), tagged("clock"), tagged("disk")
RESTARTS, PROBES = tagged("restart"), tagged("probe")
ONE_BOX_OUT = tagged("five-box-only") | tagged("global")
# Plan versions, each class: (night weight, shortest, longest episode in seconds). Version 1 is frozen so that the
# seeds of nights run before replay byte for byte (the selftest pins them); a new class or draw goes into the newest.
# A night draws in this order.
PLANS = {1: {
    "kill": (10, 0, 30), "pause": (10, 1, 20), "isolate": (10, 5, 40), "partition": (10, 5, 40),
    "netem": (10, 10, 60), "cpu": (10, 5, 60), "freeze": (6, 1, 20), "leave": (6, 5, 30), "oneway": (6, 5, 40),
    "io": (6, 10, 60), "slowdisk": (6, 10, 60), "diskfull": (6, 10, 60), "step": (6, 10, 60), "drift": (6, 30, 120),
    "oom": (6, 10, 40), "bridge": (4, 10, 40), "rate": (4, 10, 60), "memhigh": (4, 10, 60), "diskerr": (4, 5, 30),
    "diskdrop": (2, 5, 30), "drift-far": (2, 30, 120), "skew-restart": (1, 20, 30),
}}
PLANS[2] = {
    **PLANS[1],
    "powercut": (4, 5, 30), "diskhang": (4, 5, 20), "rofs": (3, 5, 30), "flaky": (4, 10, 60),
    "splitdelay": (4, 10, 60), "readerr": (3, 5, 30), "diskdead": (3, 5, 30), "diskcorrupt": (2, 5, 30),
    "inodes": (3, 10, 60), "authz": (4, 5, 40),
    "roll": (2, 30, 60), "cfgdrift": (2, 30, 120), "scale": (2, 60, 300), "cold-restart": (2, 10, 40),
    "acceptors-restart": (3, 5, 20), "acceptors-rolling": (2, 5, 20), "leave-minority": (3, 5, 30),
    "leave-slowdisk": (3, 5, 30),
    "reject": (6, 5, 40), "syndrop": (6, 5, 40), "halfopen": (4, 10, 40), "portcut": (6, 5, 40),
    "graylink": (4, 10, 60), "flap": (4, 10, 60), "mtu": (3, 10, 60), "asym": (4, 10, 60), "dup": (3, 10, 60),
    "corrupt": (3, 10, 60), "conntrack": (2, 5, 30), "backlog": (2, 10, 40), "ports": (2, 10, 40),
    "crashloop": (3, 10, 40), "killterm": (4, 5, 20), "tasksmax": (2, 10, 40), "nofile": (3, 10, 40),
    "threadstarve": (4, 5, 40), "reclaim": (3, 10, 60), "hook": (4, 5, 30), "coredump": (2, 5, 20),
    "swap": (2, 10, 60), "thp": (2, 30, 120), "takeover-step": (2, 10, 30), "drift-edge": (3, 30, 120),
    "step-huge": (1, 10, 30), "leap": (1, 20, 30),
    "peer": (6, 5, 30),
}
# A smoke's episode lasts 6 s, or here its class's own: a scale is minutes, a drifted setting needs leases to pass.
SMOKE_DUR = {"scale": 60, "cfgdrift": 20}
MODES = {"smoke": {"T": 2}, "night": {"T": 6}}
# TIMING=wan's classes, outside the plan versions: only its scenarios draw them, at its lease (run.sh's LEASE_SECS).
WAN, WAN_T = {"geo": (0, 30, 1200), "bulk": (0, 30, 600)}, 10
BULK = ("wan-bulk", "rate-bulk", "rate-bulk-uncapped")  # the scenarios that run the bulk workload
GEO = frozenset(("geo-quiet", "geo-partition-short", "geo-partition-long", "geo-syndrop", "geo-loss") + BULK)
GEO_PROBES = {"rate-bulk-uncapped"}  # links under the Bulk cap: what Live loses without it, reported apart
RIG = os.path.expanduser("~/rig")
NETNS = os.path.exists(os.path.join(RIG, "netns"))  # one box, whose clock is every node's and the store's


def names(n):
    return ["b%d" % (i + 1) for i in range(n)]


def weighted(rng, C, classes):
    return rng.choices(classes, [C[c][0] for c in classes])[0]


# A class's arguments drawn after its nodes, in this order.
DRAWS = {
    "rate": lambda r: {"mbit": round(r.uniform(0.5, 10), 1)},
    "cpu": lambda r: {"quota_pct": r.randint(2, 50)},
    "oom": lambda r: {"pct": r.randint(60, 90)},
    "memhigh": lambda r: {"pct": r.randint(70, 90)},
    "io": lambda r: {"iops": r.randint(5, 50)},
    "slowdisk": lambda r: {"delay_ms": r.randint(20, 500)},
    "step": lambda r: {"step_s": r.choice([-5, -2, -0.5, 0.5, 2, 5, 30, 300])},
    "drift": lambda r: {"k": r.choice([-1, 1]) * r.randint(10, 40)},
    "drift-far": lambda r: {"k": r.choice([-1, 1]) * r.randint(100, 1000)},
    "reject": lambda r: {"how": r.choice(["tcp-reset", "icmp-port-unreachable", "icmp-host-unreachable"])},
    "syndrop": lambda r: {"reset": r.randint(0, 1)},
    "portcut": lambda r: {"port": r.choice([7400, 7001])},
    "graylink": lambda r: {"loss_pct": r.randint(30, 90)},
    "asym": lambda r: {"delay_ms": r.randint(200, 900)},
    # 400 ms a cycle at least: its loop's two iptables calls stay under 5 a second
    "flap": lambda r: {"up_ms": r.randint(200, 2000), "down_ms": r.randint(200, 2000)},
    "mtu": lambda r: {"bytes": r.choice([576, 1200, 1400])},
    "dup": lambda r: {"dup_pct": r.randint(30, 100)},
    "corrupt": lambda r: {"corrupt_pct": round(r.uniform(1, 20), 1)},
    "ports": lambda r: {"n": r.choice([4, 16])},
    "flaky": lambda r: {"up_s": r.randint(1, 5), "down_s": r.randint(1, 3)},
    "splitdelay": lambda r: {"read_ms": r.randint(0, 100), "write_ms": r.randint(50, 500)},
    "leave-slowdisk": lambda r: {"delay_ms": r.randint(50, 500)},
    # 300 kills it while it starts, 2000 once it serves
    "crashloop": lambda r: {"period_ms": r.choice([300, 800, 2000])},
    "killterm": lambda r: {"kill_ms": r.choice([10, 50, 200, 500, 1000, 3000]), "terms": r.choice([1, 1, 5])},
    "tasksmax": lambda r: {"headroom": r.choice([0, 4, 16])},
    "nofile": lambda r: {"headroom": r.choice([0, 4, 16])},
    "threadstarve": lambda r: {"busy_pct": r.choice([50, 90, 100])},
    "reclaim": lambda r: {"mb": r.choice([16, 64, 256])},
    "swap": lambda r: {"pct": r.randint(30, 60)},
    "thp": lambda r: {"mode": r.choice(["always", "never"])},
    "step-huge": lambda r: {"step_s": r.choice([-3600, 3600, 86400])},
    "peer": lambda r: {"mode": r.choice(PEER_MODES)},
}


def draw_fault(rng, C, cls, nodes, T, dur=None, node=None):
    """One fault of cls, lasting dur or a draw from its class's range; node pins a single-node class to that node."""
    if dur is None:
        dur = round(rng.uniform(*C[cls][1:]), 1)
    # Clock classes never touch b1, whose clock is the run's timeline; nor ports: each ssh the nemesis makes from b1
    # leaves a port in TIME_WAIT.
    others = nodes[1:]
    pick = node or rng.choice(others if cls in CLOCK | {"ports"} else nodes)
    rest = [x for x in nodes if x != pick]
    f = {"class": cls, "nodes": [pick], "args": {}, "dur": dur}
    a = f["args"]
    if cls == "kill":
        a["hold_s"] = dur
    elif cls == "partition":
        side = rng.sample(nodes, len(nodes))
        k = rng.randint(1, len(nodes) // 2)
        f["nodes"], a["other"] = sorted(side[:k]), sorted(side[k:])
    elif cls == "bridge":
        a_, b, c, d, e = rng.sample(nodes, 5)
        f["nodes"], a["other"], a["via"] = sorted([a_, b]), sorted([d, e]), c
    elif cls == "oneway":
        a["to"] = [rng.choice(rest)]
    elif cls == "netem":
        every = node is None and rng.random() < 0.5
        if every:
            f["nodes"] = list(nodes)
        # D + 2J <= 2D each way delayed, so at two sigma the round trip stays under 1 s: a dial counts a peer
        # unreachable past one keepalive. Loss still costs a dial whose SYN it drops (a 1 s RTO).
        d = rng.randint(20, 225 if every else 400)
        a.update(delay_ms=d, jitter_ms=rng.randint(1, d // 2), loss_pct=round(rng.uniform(0, 10), 1),
                 reorder_pct=round(rng.uniform(0, 25), 1), dup_pct=round(rng.uniform(0, 5), 1))
    elif cls == "authz":  # the peers whose calls and spawns the node refuses
        a["to"] = sorted(rng.sample(rest, rng.randint(1, min(2, len(rest)))))  # RIG_N=2 has one
    elif cls == "skew-restart":
        f["nodes"] = others[:2]  # acceptors b2 and b3; b1 keeps its promises
        a.update(step_s=-2 * T, hold_s=2, restore_s=T + 5)
    elif cls in ("reject", "syndrop", "portcut", "graylink", "asym", "flap"):  # to one peer, or to every other node
        a["to"] = [rng.choice(rest)] if cls in ("graylink", "asym") or rng.random() < 0.5 else rest
    elif cls == "roll":  # every node in turn onto its other build, each once the one before listens and warmed
        f["nodes"] = rng.sample(nodes, len(nodes))
        a["gap_s"] = 2 * T + 1  # past a restarted acceptor's warming, T and a margin (paxos-leases.md §2.2)
    elif cls == "cold-restart":  # the whole cluster down, then up together or one by one
        f["nodes"] = list(nodes)
        a["how"], a["stagger_s"] = rng.choice(["kill", "leave"]), rng.choice([0, 0, 1, T])
    elif cls in ("acceptors-restart", "acceptors-rolling"):  # run.sh's acceptors, at once or inside warming
        f["nodes"] = nodes[:3]
        a["gap_s"] = 0 if cls == "acceptors-restart" else round(rng.uniform(0.2, 0.8) * T, 1)
    elif cls == "cfgdrift":
        a["var"], a["value"] = rng.choice([("LEASE_SECS", 2 * T), ("LEASE_SECS", T / 2), ("HEARTBEAT_MS", 250),
                                           ("HEARTBEAT_MS", 2000)])
    elif cls == "leave-minority":  # the leaver, alone or with one more, cut from the rest, then it leaves
        f["nodes"] = sorted([pick] + rng.sample(rest, rng.randint(0, 1) if len(rest) > 1 else 0))  # some left out
        a["leaver"], a["other"] = pick, sorted(set(rest) - set(f["nodes"]))
    elif cls == "hook":  # both durations drawn whichever hook it is
        a["hook"] = rng.choice(HOOKS)
        a["ms"] = {"block": rng.choice([100, 1000]), "jobsleep": rng.choice([200, 1000, 3000])}.get(a["hook"], 0)
    elif cls == "takeover-step":  # kill the first, holding it down, and step the second while its keys move
        f["nodes"] = rng.sample(others, 2)
        a.update(hold_s=dur, step_s=rng.choice([-2, 2, 5]))
    elif cls == "drift-edge":  # the first's tick +k, the second's -k: 0.98 % apart, at the 1 % bound
        f["nodes"] = sorted(rng.sample(others, 2))
        a["k"] = 49
    if cls in DRAWS:
        a.update(DRAWS[cls](rng))
    if cls in DISK and C is not PLANS[1]:  # version 1 put every disk fault on the journal
        a["dev"] = "streams" if cls in STREAMS_ONLY else rng.choice(["journal", "streams"])
    return f


def single_node(f):
    return len(f["nodes"]) == 1 and f["class"] not in ("partition", "bridge", "skew-restart")


def combo(rng, C, nodes, T):
    """Two or three faults on distinct nodes, or a network or clock fault with one more on its node, each drawn by
    weight and lasting its own class's duration."""
    pool = [c for c in C if c not in ("partition", "bridge") and c not in PROBES]
    if rng.random() < 0.5:
        faults, used = [], set()
        for _ in range(rng.choice([2, 3])):
            while True:
                f = draw_fault(rng, C, weighted(rng, C, pool), nodes, T)
                if single_node(f) and f["nodes"][0] not in used and not set(f["args"].get("to", ())) & used:
                    break
            used.add(f["nodes"][0])
            faults.append(f)
        return faults
    while True:
        first = draw_fault(rng, C, weighted(rng, C, [c for c in pool if c in NETWORK | CLOCK]), nodes, T)
        if single_node(first):
            break
    n = first["nodes"][0]
    back = first["class"] == "step" and first["args"]["step_s"] < 0  # a clock stepped back is not restarted with it
    while True:  # drawn again only where illegal, which no version 1 draw is, so its plans stay as they were
        cls = weighted(rng, C, [c for c in pool if c not in CLOCK and not (back and c in RESTARTS)
                                and not (c in NETWORK and first["class"] in NETWORK)
                                and not (c in STOPS and first["class"] in RESTARTS)])
        faults = [first, draw_fault(rng, C, cls, nodes, T, node=n)]
        if legal(faults):
            return faults


def disk_of(f):
    return f["args"].get("dev", "journal")  # passed to fault.sh as -d


def legal(faults):
    """Faults that may be on at once: no probe beside another fault, and per node no two holding one SOLE resource,
    no stop beside a restart, no restart of a clock stepped back (skew-restart is that probe), no class of STOPS
    twice (the first off would resume the second), and a restart outside LEDGER alone among restarts; beside a
    `roll`, one fault at most and on another node."""
    if len(faults) > 1 and any(f["class"] in PROBES for f in faults):
        return False
    on = collections.defaultdict(list)
    for f in faults:
        for n in f["nodes"]:
            on[n].append(f)
    for fs in on.values():
        cs = [f["class"] for f in fs]
        sole = [SOLE[f["class"]] + (":" + disk_of(f) if f["class"] in DISK else "")
                for f in fs if f["class"] in SOLE]  # fault.sh claims io, table and filler per disk
        back = any(f["class"] == "step" and f["args"]["step_s"] < 0 for f in fs)
        restarts = [c for c in cs if c in RESTARTS]
        if len(sole) > len(set(sole)) or (set(cs) & STOPS and restarts) or (back and restarts):
            return False
        # A hung disk: a stop or restart waits on a process that cannot die. A power cut unmounts its disk: another
        # fault's filler would land on the root file system.
        disks = [disk_of(f) for f in fs if f["class"] in DISK]
        if "diskhang" in cs and set(cs) & (STOPS | RESTARTS) or any(
                f["class"] in ("diskhang", "powercut") and disks.count(disk_of(f)) > 1 for f in fs):
            return False
        if len([c for c in cs if c in STOPS]) > len(set(cs) & STOPS) or len(restarts) > 1 and set(restarts) - LEDGER:
            return False
    rolls = [f for f in faults if f["class"] == "roll"]
    return not rolls or len(faults) <= 2 and not any(
        set(f["nodes"]) & set(r["nodes"]) for r in rolls for f in faults if f is not r)


# ---- schedules (spec/fault-rig.md § The nemesis and its schedules): faults at their own offset in an episode, on a
# trigger, on a role


def span(f):
    """When a fault may be on, from its episode's start: a triggered one from when it is armed until the latest it
    can fire plus its duration; a restart at least 1 s, while its unit comes back."""
    t = f.get("trigger")
    a = f.get("at", 0)
    return a, a + (t["within"] + t.get("delay", 0) if t else 0) + max(f["dur"], 1 if f["class"] in RESTARTS else 0)


def candidates(f):
    """A fault whose role picks its node as it goes on, on every node the role may pick."""
    return dict(f, nodes=f["among"]) if f["nodes"][0].startswith("@") else f


def timeline_legal(faults):
    """legal() over the faults on at each moment one goes on. An off due with an on goes first (episode())."""
    spans = [span(f) for f in faults]
    return all(legal([candidates(g) for g, (a, b) in zip(faults, spans) if a <= s < b or g is f])
               for f, (s, _) in zip(faults, spans))


def grace(T):
    return (0.99 * T - 0.2) * 7 / 8  # lease_grace unset: 7/8 of T (1 - drift) - margin (spec/paxos-leases.md § 2.4)


def edges(T):
    """Durations at the lease's thresholds: the renew interval, the grace G, T, W = T (1 + drift) + margin, multiples
    of T, and a few seconds, each also 10 % and 2 % short and long."""
    G, W = grace(T), 1.01 * T + 0.2
    return sorted({round(b * k, 2) for b in (0.4 * G, G, T, W, 2 * T, 3 * T, 1, 2, 4, 5, 8, 10)
                   for k in (0.9, 0.98, 1, 1.02, 1.1)})


def lasting(rng, C, cls, T, fixed=None, p=0.3):
    """A duration for cls: with probability p at an edge (EDGE classes), else fixed, else a draw from its range."""
    return min(rng.choice(edges(T)), C[cls][2]) if cls in EDGE and rng.random() < p else fixed


def cast(rng, f, nodes, p=0.2):
    """With probability p, a ROLE fault goes on whichever node owns one of the first 100 keys when it goes on."""
    if f["class"] in ROLE and rng.random() < p:
        f["nodes"], f["among"] = ["@owner:k%d" % rng.randrange(100)], list(nodes)
    return f


def episode_of(i, at, faults, kind, name=None):
    e = {"i": i, "at": round(at, 1), "dur": round(max(span(f)[1] for f in faults), 2), "faults": faults, "kind": kind}
    return dict(e, name=name) if name else e


def classes(C, only):
    """ONLY's classes (a,b,..), or all of C, sorted."""
    pool = sorted(only.split(",")) if only else sorted(C)
    if set(pool) - set(C):
        sys.exit("unknown classes in ONLY: %s" % ",".join(sorted(set(pool) - set(C))))
    return pool


# The fewest nodes a class's draw names: a peer; two besides b1, whose clock is the run's timeline; a bridge's five.
NEEDS = dict.fromkeys(CLOCK | {"partition", "oneway", "authz", "reject", "syndrop", "portcut", "graylink", "asym",
                               "flap", "ports", "leave-minority"}, 2)
NEEDS.update({"takeover-step": 3, "drift-edge": 3, "skew-restart": 3, "bridge": 5})


def pool_of(C, n, one_box, only):
    """The classes a schedule draws from."""
    return [c for c in classes(C, only) if c not in PROBES and not (one_box and c in ONE_BOX_OUT)
            and n >= NEEDS.get(c, 1)]


def chaos(rng, C, pool, nodes, T, minutes):
    """Clusters of faults arriving every 2T on average, overlapping up to MAX_ON at once, some at edge durations
    and some on a key's owner; one in ten a class on every node, or on three or more at once. Three in ten episodes
    begin within B of the last one's end, a cascade; at least every 10 minutes a rest of 2 (B + 20) s."""
    MAX_ON, B, eps, at, rest = 3, 5 * T, [], 0.0, 600
    single = [c for c in pool if c in ROLE | {"step", "drift", "netem", "oneway"}]  # one node each
    while at < minutes * 60:
        faults = []
        if single and rng.random() < 0.1:
            cls = weighted(rng, C, single)
            dur = lasting(rng, C, cls, T) or round(rng.uniform(*C[cls][1:]), 1)
            ok = nodes[1:] if cls in CLOCK else nodes
            for n in sorted(rng.sample(ok, rng.randint(min(3, len(ok)), len(ok)))):
                faults.append(dict(draw_fault(rng, C, cls, nodes, T, dur, node=n), at=0))
            kind = "correlated"
        else:
            H, t, kind = rng.uniform(10 * T, 50 * T), 0.0, "chaos"
            while t < H:
                cls = weighted(rng, C, pool)
                f = dict(cast(rng, draw_fault(rng, C, cls, nodes, T, lasting(rng, C, cls, T)), nodes), at=round(t, 2))
                if sum(1 for g in faults if span(g)[0] <= t < span(g)[1]) < MAX_ON and timeline_legal(faults + [f]):
                    faults.append(f)
                t += rng.expovariate(1 / (2 * T))
        e = episode_of(len(eps), at, faults, kind)
        eps.append(e)
        gap = rng.uniform(0, B) if rng.random() < 0.3 else rng.uniform(B + 10, B + 20)
        if at + e["dur"] + gap > rest:
            gap, rest = max(gap, 2 * (B + 20)), rest + 600
        at += e["dur"] + gap
    return eps


RELATIONS = ("same", "apart", "stagger", "cascade")


def used(f):
    """Every node a fault names, a role's on every node of its `among`."""
    a = f["args"]
    return set(candidates(f)["nodes"]) | set(a.get("other", ())) | set(a.get("to", ())) | (
        {a["via"]} if "via" in a else set())


def place(rng, C, a, b, rel, nodes, T, short, tries=50):
    """One pair cell as (faults, the cascade's second episode's faults or None, the gap before it), or None where
    `tries` draws cannot place it. same: both on at once sharing a node; apart: at once on disjoint nodes; stagger:
    b on halfway through a, sharing a node where legal, else apart; cascade: b within B/2 after a ends, on its node."""
    B = 5 * T
    dur = lambda c: lasting(rng, C, c, T, 6 if short else None)
    for k in range(tries):
        fa = dict(draw_fault(rng, C, a, nodes, T, dur(a)), at=0)
        near = rel in ("same", "cascade") or rel == "stagger" and k < tries // 2
        pin = rng.choice(fa["nodes"]) if near and b not in ("partition", "bridge") else None
        if pin == nodes[0] and b in CLOCK | {"ports"}:
            continue
        fb = dict(draw_fault(rng, C, b, nodes, T, dur(b), node=pin), at=0)
        if near != bool(set(fa["nodes"]) & set(fb["nodes"])):
            continue
        if rel == "cascade":
            return [fa], [fb], round(rng.uniform(0, B / 2), 1)
        if rel == "stagger":
            if fa["dur"] <= 1:
                continue
            fb["at"] = round(max(1, fa["dur"] / 2), 2)
        if timeline_legal([fa, fb]):
            return [fa, fb], None, 0
    return None


_CELLS = {}


def cells(C, pool, nodes, T, short):
    """Every pair cell (a, b, relation) of pool that place() can put on, in one fixed order (Random('pairs')), so a
    shard is the same cells whatever the seed: feasibility is a fixed draw of its own per cell."""
    key = (tuple(sorted(C.items())), tuple(pool), tuple(nodes), T, short)
    if key not in _CELLS:
        out = [(a, b, r) for a, b in itertools.combinations_with_replacement(sorted(pool), 2) for r in RELATIONS
               if place(random.Random("cell/%s/%s/%s" % (a, b, r)), C, a, b, r, nodes, T, short)]
        random.Random("pairs").shuffle(out)
        _CELLS[key] = out
    return _CELLS[key]


def placed(seed, C, cell, nodes, T, short):
    return (place(random.Random("%s/%s" % (seed, "/".join(cell))), C, *cell, nodes, T, short)
            or place(random.Random("cell/%s/%s/%s" % cell), C, *cell, nodes, T, short))


def label(cell):
    return "%s+%s/%s" % cell


def pairs(seed, C, pool, nodes, T, minutes, short):
    """Shard SHARD=k/m (1/1) of every feasible cell, each placed once, two cells to an episode where their nodes are
    disjoint and together legal; with minutes, the prefix that begins before then."""
    B, shard = 5 * T, (os.environ.get("SHARD") or "1/1").split("/")
    k, m = map(int, shard) if len(shard) == 2 and all(x.isdigit() for x in shard) else (0, 0)
    if not 1 <= k <= m:
        sys.exit("SHARD=k/m wants 1 <= k <= m")
    eps, at, held = [], 0.0, None

    def emit(faults, names, gap=B + 5):
        nonlocal at
        eps.append(episode_of(len(eps), at, faults, "pair", ",".join(names)))
        at += eps[-1]["dur"] + gap

    for cell in cells(C, pool, nodes, T, short)[k - 1::m]:
        if minutes and at >= minutes * 60:
            break
        first, second, gap = placed(seed, C, cell, nodes, T, short)
        if second is None and held and not set().union(*map(used, held[0])) & set().union(
                *map(used, first)) and timeline_legal(held[0] + first):
            emit(held[0] + first, held[1] + [label(cell)])
            held = None
            continue
        if held:
            emit(*held)
            held = None
        if second is None:
            held = (first, [label(cell)])
        else:
            emit(first, [label(cell)], gap)
            emit(second, [label(cell)])
    if held and not (minutes and at >= minutes * 60):
        emit(*held)
    return eps


def fault(cls, nodes, at, dur, **a):
    """A fault of a scenario on nodes (a name, a list, or a role given `among`), at `at` for dur, with arguments
    that bite without ending the run by default."""
    nodes = [nodes] if isinstance(nodes, str) else sorted(nodes)
    extra = {k: a.pop(k) for k in ("among", "trigger") if k in a}
    base = {"kill": {"hold_s": round(dur, 2)}, "cpu": {"quota_pct": 10}, "oom": {"pct": 70}, "memhigh": {"pct": 80},
            "io": {"iops": 10}, "slowdisk": {"delay_ms": 300}, "rate": {"mbit": 1},
            "netem": {"delay_ms": 200, "jitter_ms": 50, "loss_pct": 2, "reorder_pct": 5, "dup_pct": 1}}
    return dict({"class": cls, "nodes": nodes, "args": dict(base.get(cls, {}), **a), "dur": round(dur, 2),
                 "at": round(at, 2)}, **extra)


def split(side, nodes):
    return {"other": sorted(set(nodes) - set(side))}


def library(T):
    """The named sequences (spec/fault-rig.md § The nemesis and its schedules), name/parameter -> faults: A1..A3 the
    acceptors, N1 and N2 the other nodes; N2 relays N1's streams (src/streams.rs); G the lease grace, W T (1 + drift)
    + margin."""
    ns = names(5)
    A1, A2, A3, N1, N2 = ns
    G, W = grace(T), 1.01 * T + 0.2
    takes = {"r": "O", "v": "ok", "n": [A3, N1, N2, A1], "prev": A2}  # an own of a key A2 held (Tails)
    s = {}
    for d in (0, 0.25 * T, 0.5 * T, T):  # the node that took one of A2's keys first dies d after it did
        s["owner-kill-after-takeover/%g" % d] = [fault("kill", A2, 0, 3 * T), fault(
            "kill", "@trigger", 0.1, 2 * T, among=[A3, N1, N2, A1],
            trigger={"log": "store", "when": [takes], "within": 3 * T, "delay": d})]
    s["double-hop"] = [fault("kill", A2, 0, 4 * T), fault(
        "kill", "@trigger", 0.1, 2 * T, among=[A3, N1, N2, A1],
        trigger={"log": "store", "when": [takes], "within": 3 * T}), fault(
        "kill", "@trigger", 0.2, 2 * T, among=ns,
        trigger={"log": "store", "when": [dict(takes, k="$k"), {"r": "O", "v": "ok", "k": "$k"}],
                 "within": 4 * T})]
    s["origin-kill-at-open"] = [fault("kill", N1, 0, 0), fault(
        "kill", "@trigger", 0.1, 0, among=[N1], trigger={"log": "stream.jsonl", "when": [{"r": "open", "n": N1}],
                                                          "within": 10 * T})]
    s["relay-kill-at-origin-open"] = [fault("kill", N1, 0, 0), fault(
        "kill", N2, 0.1, T, trigger={"log": "stream.jsonl", "when": [{"r": "open", "n": N1}], "within": 10 * T})]
    for d in (G - 0.3, G, T - 0.1, T, T + 0.1, W + 0.2):
        s["heal-at-expiry/%.2f" % d] = [fault("isolate", N1, 0, d)]
        s["pause-at-expiry/%.2f" % d] = [fault("pause", N1, 0, d)]
        s["freeze-at-expiry/%.2f" % d] = [fault("freeze", N1, 0, d)]
    for d in (G, T + 0.1):
        s["owner-pause-at-expiry/%.2f" % d] = [fault("pause", "@owner:k0", 0, d, among=ns)]
    for d in (T, 3 * T, 10 * T):
        s["acceptor-minority/%g" % d] = [fault("partition", [A2, A3], 0, d, **split([A2, A3], ns))]
    s["majority-down"] = [fault("kill", A2, 0, 3 * T), fault("kill", A3, 0, 3 * T)]
    s["cold-start"] = [fault("kill", a, 0, 0) for a in (A1, A2, A3)]
    s["all-restart"] = [fault("kill", n, 0, 0) for n in ns]
    for g in (0.5 * W, W + 1, 2 * W):
        s["rolling-acceptors/%.2f" % g] = [fault("kill", a, k * g, 0) for k, a in enumerate((A1, A2, A3))]
    s["acceptor-restart-while-partitioned/kill"] = [fault("partition", A1, 0, 4 * T, **split([A1], ns)),
                                                    fault("kill", A1, T, 0)]
    s["acceptor-restart-while-partitioned/leave"] = [fault("partition", A1, 0, 4 * T, **split([A1], ns)),
                                                     fault("leave", A1, T, T)]
    s["partition-switch"] = [fault("partition", [A1, N1], 0, 2 * T, **split([A1, N1], ns)),
                             fault("partition", [A2, N1], 2 * T, 2 * T, **split([A2, N1], ns))]
    s["rolling-restart-during-partition"] = [fault("partition", N2, 0, 6 * T, **split([N2], ns))] + [
        fault("kill", a, (0.5 + 2 * k) * T, 0) for k, a in enumerate((A1, A2, A3))]
    s["leave-in-minority"] = [fault("partition", [A3, N2], 0, 4 * T, **split([A3, N2], ns)), fault("leave", N2, T, T)]
    for p in (1, 2, 4, G):
        s["flap/%.2f" % p] = [fault("isolate", N1, k * 2 * p, p) for k in range(10)]
        s["oneway-flap/%.2f" % p] = [fault("oneway", N1, k * 2 * p, p, to=[A1]) for k in range(10)]
    s["grey-acceptor"] = [fault("netem", A2, 0, 10 * T, delay_ms=400, jitter_ms=100, loss_pct=5, reorder_pct=0,
                                dup_pct=0), fault("cpu", A2, 0, 10 * T)]
    for a, b in (("freeze", "netem"), ("oom", "diskfull"), ("leave", "diskdrop"), ("slowdisk", "cpu"),
                 ("io", "memhigh"), ("isolate", "diskerr")):
        s["stack/%s+%s" % (a, b)] = [fault(a, N1, 0, 4 * T), fault(b, N1, 0, 4 * T)]
    s["whole-cluster/netem"] = [fault("netem", ns, 0, 6 * T), fault("kill", N1, T, T)]
    s["whole-cluster/cpu"] = [fault("cpu", n, 0, 6 * T, quota_pct=20) for n in ns]
    s["whole-cluster/rate"] = [fault("rate", ns, 0, 6 * T), fault("kill", A1, T, T)]
    for c in sorted(ROLE):
        s["correlated/%s" % c] = [fault(c, n, 0, 3 * T) for n in ns]
    s["step-during-partition"] = [fault("partition", N1, 0, 4 * T, **split([N1], ns)),
                                  fault("step", N1, T, 2 * T, step_s=-2)]
    s["drift-while-isolated"] = [fault("drift", N1, 0, 6 * T, k=40), fault("isolate", N1, T, 2 * T)]
    # spec/wan.md § 12, each a geo span from 30 s before its fault to 30 s after, on the run's backdrop (only rate= and
    # loss= change the bands): A2 is the far region's acceptor.
    geo = lambda d, on=ns, **a: fault("geo", on, 0, d + 60, **a)
    s["geo-quiet"] = [geo(1140)]
    s["geo-partition-short"] = [geo(G - 1), fault("partition", A2, 30, G - 1, **split([A2], ns))]
    s["geo-partition-long"] = [geo(3 * T), fault("partition", A2, 30, 3 * T, **split([A2], ns))]
    s["geo-syndrop"] = [geo(30), fault("syndrop", A3, 30, 30, to=[N2], reset=0), fault("isolate", N2, 40, 10)]
    s["geo-loss"] = [geo(240, A2, loss=2), geo(240, [A1, A3, N1, N2], loss=2, to=[A2])]
    for name, link in zip(BULK, ({}, {"rate": 150}, {"rate": 100})):
        s[name] = [geo(540, ns[1:]), geo(540, A1, **link), fault("bulk", A1, 30, 540)]
    return s


def scenarios(rng, C, pool, nodes, T, minutes, want=None):
    """Every variant of the library on pool's classes and on nodes, SCENARIOS=a,b those named a or b, in a seeded
    order, each an episode B + 10 s after the last; with minutes, the prefix that begins before then."""
    B, eps, at = 5 * T, [], 0.0
    unknown = set(want or ()) - {k.split("/")[0] for k in library(T)}
    if unknown:  # a typo would run an empty plan
        sys.exit("unknown scenarios in SCENARIOS: %s" % ",".join(sorted(unknown)))
    lib = [(k, v) for k, v in sorted(library(T).items()) if {f["class"] for f in v} <= set(pool)
           and set().union(*map(used, v)) <= set(nodes) and (not want or k.split("/")[0] in want)]
    rng.shuffle(lib)
    for name, faults in lib:
        if minutes and at >= minutes * 60:
            break
        eps.append(episode_of(len(eps), at, faults, "scenario", name))
        at += eps[-1]["dur"] + B + 10
    return eps


def mix(seed, C, pool, nodes, T, at):
    """What a smoke adds after its singles (plan version 2 on): a pair cell of each relation and two scenarios and a
    flap, each rotating with the seed."""
    k, B, eps = int(hashlib.sha256(str(seed).encode()).hexdigest(), 16), 5 * T, []
    for j, rel in enumerate(RELATIONS):
        cs = [c for c in cells(C, pool, nodes, T, True) if c[2] == rel]
        if cs:
            cell = cs[(k * 4 + j) % len(cs)]
            first, second, gap = placed(seed, C, cell, nodes, T, True)
            eps.append(episode_of(0, at, first, "pair", label(cell)))
            if second:
                at += eps[-1]["dur"] + gap
                eps.append(episode_of(0, at, second, "pair", label(cell)))
            at += eps[-1]["dur"] + B + 5
    lib = [(n, v) for n, v in sorted(library(T).items()) if {f["class"] for f in v} <= set(pool)]
    flaps = [x for x in lib if x[0].startswith("flap/")]
    picks = [lib[(k + j * len(lib) // 2) % len(lib)] for j in range(2) if lib] + flaps[k % max(1, len(flaps)):][:1]
    for name, faults in picks:
        eps.append(episode_of(0, at, faults, "scenario", name))
        at += eps[-1]["dur"] + B + 5
    return eps


def version():
    v = int(os.environ.get("PLAN_VERSION") or max(PLANS))
    if v not in PLANS:
        sys.exit("no plan version %d: %s" % (v, ", ".join(map(str, PLANS))))
    return v


def plan(seed, mode, minutes=None, v=None, only=None, n=None, one_box=False):
    """With only (a,b,..), a smoke of those classes. The schedules draw on b1..bn only, and on one box only the
    classes it can run; smoke and night leave that to plan_lines, so that version 1 stays byte for byte."""
    v = v or version()
    C = PLANS[v]
    rng = random.Random("%s/%s" % (seed, mode))
    nodes = names(5)
    if mode in ("chaos", "pairs", "scenarios"):
        timing, nodes = os.environ.get("TIMING") or "smoke", names(n or 5)
        want = set(filter(None, os.environ.get("SCENARIOS", "").split(",")))
        if timing == "wan":
            if mode != "scenarios" or want - GEO:
                sys.exit("TIMING=wan runs the geo scenarios only: %s" % ",".join(sorted(GEO)))
            C, T, want = dict(C, **WAN), WAN_T, want or set(GEO)
        elif timing not in MODES or want & GEO:
            sys.exit("TIMING is smoke, night or wan, the geo scenarios at wan only")
        else:
            T = MODES[timing]["T"]
        pool = pool_of(C, len(nodes), one_box, only)
        eps = (chaos(rng, C, pool, nodes, T, minutes or 60) if mode == "chaos" else
               pairs(seed, C, pool, nodes, T, minutes, T == MODES["smoke"]["T"]) if mode == "pairs" else
               scenarios(rng, C, pool, nodes, T, minutes, want))
    elif mode not in MODES:
        sys.exit("no mode %s: smoke, night, chaos, pairs, scenarios" % mode)
    elif mode == "smoke":  # every class once; with only, those classes, in rounds until `minutes` when given
        T, eps, pool, at = MODES[mode]["T"], [], classes(C, only), 0.0
        while not eps or (minutes and at < minutes * 60):
            order = list(pool)
            rng.shuffle(order)
            for cls in order:
                d = SMOKE_DUR.get(cls, 6)
                eps.append({"i": len(eps), "at": at, "dur": d, "faults": [draw_fault(rng, C, cls, nodes, T, d)]})
                at += d + 12
        if v >= 2 and os.environ.get("SMOKE_MIX") != "0":
            mixed = mix(seed, C, pool_of(C, 5, one_box, only), nodes, T, at)
            eps += [dict(e, i=len(eps) + j) for j, e in enumerate(mixed)]
    else:  # a shorter plan of the same seed is a prefix of the longer
        T, eps, at = MODES[mode]["T"], [], 0.0
        while at < (minutes or 150) * 60:
            faults = (combo(rng, C, nodes, T) if rng.random() < 0.15 else
                      [draw_fault(rng, C, weighted(rng, C, list(C)), nodes, T)])
            dur = max(f["dur"] for f in faults)
            eps.append({"i": len(eps), "at": round(at, 1), "dur": dur, "faults": faults})
            at += dur + rng.uniform(5 * T + 10, 5 * T + 20)
    for e in eps:
        assert "kind" in e or 1 <= len(e["faults"]) <= 3, e
        assert all(f["class"] in C for f in e["faults"]), e
        assert not any(f["class"] in CLOCK | {"ports"} and "b1" in f["nodes"] for f in e["faults"]), e
        assert mode == "smoke" or "kind" in e or all(C[f["class"]][1] <= f["dur"] <= C[f["class"]][2]
                                                     for f in e["faults"]), e
        assert timeline_legal(e["faults"]) if "kind" in e else legal(e["faults"]), e
        assert all(f["nodes"][0][1:].partition(":")[0] in ROLES and f.get("among") for f in e["faults"]
                   if f["nodes"][0].startswith("@")), e  # resolve() would fail mid-run
    return eps


def plan_lines(seed, mode, n=None, minutes=None, v=None, only=None, one_box=None):
    """What `plan` prints: with n, only the episodes on b1..bn; on one box, none holding a class it cannot run."""
    keepn = set(names(n)) if n else None
    one_box = NETNS if one_box is None else one_box
    return [dumps(e) for e in keep(plan(seed, mode, minutes, v, only, n, one_box), lambda f: (
        keepn is None or used(f) <= keepn) and not (one_box and f["class"] in ONE_BOX_OUT))]


def keep(eps, ok):
    """The episodes whose faults all pass ok, each keeping its gap before the next, as if the others were never
    drawn: a run on fewer boxes, or on one, runs a subsequence of the plan."""
    out, at = [], 0.0
    for e, nxt in zip(eps, eps[1:] + [None]):
        if all(ok(f) for f in e["faults"]):
            out.append(dict(e, i=len(out), at=round(at, 1)))
            at += (nxt["at"] - e["at"]) if nxt else 0
    return out


def dumps(x):
    return json.dumps(x, sort_keys=True, separators=(",", ":"))


def group(eps, i, B):
    """Episode i and every episode before it whose end plus B reaches its start: the overlap or cascade it ran in,
    at their gaps, from 0."""
    e = next(e for e in eps if e["i"] == i)
    g = [x for x in eps if x["i"] < i and x["at"] + x["dur"] + B > e["at"]] + [e]
    return [dict(x, at=round(x["at"] - g[0]["at"], 2)) for x in g]


def replay(src, i, n, out):
    with open(os.path.join(src, "meta.json")) as f:
        m = json.load(f)
    with open(os.path.join(src, "plan.jsonl")) as f:
        g = group(list(map(json.loads, f)), int(i), 5 * float(m["lease_secs"]))
    v = int(m.get("plan_version", 1))  # a run from before versions planned version 1
    cs = {f["class"] for e in g for f in e["faults"]}
    if v not in PLANS:
        sys.exit("the run planned version %d, which this nemesis.py does not have" % v)
    if cs - set(PLANS[v]) - set(WAN):
        sys.exit("episode %s holds %s, not all of plan version %d" % (i, ",".join(sorted(cs)), v))
    if NETNS and cs & ONE_BOX_OUT:
        sys.exit("episode %s holds a class that one box cannot put on one node" % i)
    gap = max(e["at"] + e["dur"] for e in g) + 5 * float(m["lease_secs"]) + 15
    with open(out, "w") as f:
        for k in range(int(n)):
            for j, e in enumerate(g):
                f.write(dumps(dict(e, i=k * len(g) + j, at=round(k * gap + e["at"], 1))) + "\n")
    p = m["params"]
    print(p["load"], p["keys"], p["every_ms"], m["lease_secs"], v, len(g))


def windows(nem):
    """The faults of a nemesis log as (class, nodes it acted on, first on due, last off done), where every command
    of it exited 0 and it acted on some node. Due is sent less slip_ms: a second on of a node waits for the first's
    step, which may take seconds (a dm suspend), and two faults due together are still `same`."""
    fs = {}
    for x in nem:
        if x.get("ev") in ("on", "off"):
            f = fs.setdefault(x.get("id") or (x["i"], x["class"]), {"class": x["class"], "nodes": set(x["nodes"]),
                                                                   "noop": set(), "rc": 0, "on": None, "off": None})
            f["rc"] |= x.get("rc", 0)
            f["noop"] |= set(x.get("noop", ()))
            if x["ev"] == "on" and f["on"] is None:
                f["on"] = x["sent"] - x.get("slip_ms", 0) * 1000
            if x["ev"] == "off":
                f["off"] = x["done"]
    return sorted((f["class"], f["nodes"] - f["noop"], f["on"], f["off"]) for f in fs.values()
                  if not f["rc"] and f["on"] is not None and f["off"] is not None and f["nodes"] - f["noop"])


def related(ws, B):
    """The pair cells those windows put on: on within 0.5 s of each other, same where they share a node, else apart;
    otherwise overlapping, stagger; the second on within B after the first's off on a node of it, cascade. µs."""
    out = set()
    ws = sorted(ws, key=lambda w: w[2])
    for j, (ca, na, a_on, a_off) in enumerate(ws):
        for cb, nb, b_on, b_off in ws[j + 1:]:
            if b_on > a_off + B:
                break
            c = tuple(sorted((ca, cb)))
            if b_on - a_on < 500_000 and b_on < a_off:
                out.add(c + ("same" if na & nb else "apart",))
            elif b_on < a_off:
                out.add(c + ("stagger",))
            elif na & nb:
                out.add(c + ("cascade",))
    return out


def coverage(dirs):
    """The cells the runs covered, against every cell a pairs plan of their version and box can place."""
    got, ms = set(), []
    for d in dirs:
        with open(os.path.join(d, "meta.json")) as f:
            m = json.load(f)
        ms.append((m.get("plan_version", 1), m["lease_secs"], bool(m.get("one_box")), sorted(m.get("nodes") or ())))
        with open(os.path.join(d, "nemesis.jsonl")) as f:
            got |= related(windows(json.loads(l) for l in f if l.strip()), 5 * float(m["lease_secs"]) * 1e6)
    if len(set(map(repr, ms))) > 1:  # the cells feasible differ with each
        sys.exit("coverage wants runs of one plan version, lease, box and nodes: %s" % sorted(set(map(repr, ms))))
    C, T = PLANS[int(m.get("plan_version", 1))], float(m["lease_secs"])  # a mode's T is its lease
    nodes, short = sorted(m.get("nodes") or names(5)), T == MODES["smoke"]["T"]
    pool = pool_of(C, len(nodes), bool(m.get("one_box")), None)
    want = set(cells(C, pool, nodes, T, short))
    dur = 6 if short else sum(C[c][1] + C[c][2] for c in pool) / 2 / len(pool)  # a night draws from each range
    left = sorted(want - got)
    print("covered %d of %d cells (%.0f %%) from %d runs" % (len(want & got), len(want), 100 * len(want & got) / max(
        1, len(want)), len(dirs)))
    for rel in RELATIONS:
        print("  %-8s %d of %d" % (rel, sum(1 for c in want & got if c[2] == rel), sum(1 for c in want if c[2] == rel)))
    print("left: about %.1f h of pairs at %s timing" % (  # few cells pack two to an episode, a cascade takes two
        len(left) * (dur + 5 * T + 5) / 3600, "smoke" if short else "night"))
    for c in left[:40]:
        print("  " + label(c))
    if len(left) > 40:
        print("  and %d more" % (len(left) - 40))


# ---- applying it


def nodes_in_use():
    """Name to ip of the first RIG_N of ~/rig/nodes, or all."""
    with open(os.path.join(RIG, "nodes")) as f:
        xs = [l.strip() for l in f if l.strip()]
    xs = xs[:int(os.environ.get("RIG_N") or len(xs))]
    return dict(zip(names(len(xs)), xs))


def remote(ip, cmd):
    return [os.path.join(RIG, "in.sh"), ip, cmd] if NETNS else ["ssh", ip, cmd]


def ssh(ip, cmd, timeout=120):
    try:
        p = subprocess.run(remote(ip, cmd), capture_output=True, text=True, timeout=timeout)
        return p.returncode, p.stdout.strip(), p.stderr.strip()
    except subprocess.TimeoutExpired:
        return 124, "", "timeout"


def fault_sh(ip, *args):
    return ssh(ip, "sudo ~/rig/fault.sh " + " ".join(str(a) for a in args))


def commands(f, where):
    """(ip, fault.sh args) per target of one fault, the same for on and off."""
    c, a, ns = f["class"], f["args"], f["nodes"]
    at = lambda xs: ",".join(where[x] for x in xs)
    if c == "takeover-step":
        return [(where[ns[0]], ["kill", a["hold_s"]]), (where[ns[1]], ["step", a["step_s"]])]
    if c == "drift-edge":
        return [(where[ns[0]], ["drift", a["k"]]), (where[ns[1]], ["drift", -a["k"]])]
    tail = ([at(a["other"])] if c in ("partition", "bridge") else
            [",".join(a["to"])] if c == "authz" else  # node ids, which the node's authorizer compares
            ["all" if c == "flap" and len(a["to"]) > 1 else at(a["to"])] if "to" in a else [])
    if c == "geo":  # each peer's one-way delay, then the keyword arguments the fault has
        return [(where[n], [c] + geo_delays(n, where) + ["%s=%s" % (k, a[k]) for k in ("rate", "loss") if k in a] + tail)
                for n in ns]
    sub = {"drift-far": "drift", "step-huge": "step"}.get(c, c)
    return [(where[n], [sub] + [a[k] for k in ARGS.get(c, ())] + tail) for n in ns]


def steps(f, where):
    """A composed class's on and off, each a list of steps: (ip, fault.sh args) run at once, then seconds to wait.
    None for a class fault.sh runs itself. The checker reads its first on and its last off as the fault's."""
    c, a, ns = f["class"], f["args"], f["nodes"]
    at = lambda cmd, nodes: [(where[n], cmd) for n in nodes]
    one_by_one = lambda cmd, gap: [(at(cmd, [n]), gap * (n != ns[-1])) for n in ns] if gap else [(at(cmd, ns), 0)]
    if c == "roll":
        return one_by_one(["upgrade"], a["gap_s"]), [(at(["upgrade"], ns), 0)]
    if c == "cold-restart":
        down = ["kill", f["dur"]] if a["how"] == "kill" else ["leave"]  # a kill with a hold holds it down too
        return [(at(down, ns), 0)], one_by_one(down, a["stagger_s"])
    if c in ("acceptors-restart", "acceptors-rolling"):  # kill 0: Restart=always brings each back in 1 s
        return one_by_one(["kill", 0], a["gap_s"]), [(at(["kill", 0], ns), 0)]
    if c in ("leave-minority", "leave-slowdisk"):  # the leave while the fault holds; back in reverse order
        fault = at(["slowdisk", a["delay_ms"]], ns) if c == "leave-slowdisk" else \
            at(["partition", ",".join(where[x] for x in a["other"])], ns)
        leave = at(["leave"], [a.get("leaver", ns[0])])
        return [(fault, 0), (leave, 0)], [(leave, 0), (fault, 0)]
    return None


class Log:
    def __init__(self, path):
        self.f, self.lock = open(path, "a"), threading.Lock()  # an episode's steps log from threads of their own

    def __call__(self, ev, **kw):
        kw.update(t=now_us(), ev=ev)
        with self.lock:
            self.f.write(dumps(kw) + "\n")
            self.f.flush()


def now_us():
    return time.time_ns() // 1000


def apply(pool, log, i, fid, f, ev, where, cmds=None, extra=None):
    """One fault's on or off on every target at once, logged with sent and done. fid names the fault to fault.sh, so
    its off undoes only what its on did. cmds (a composed class's step) are logged as `sub` and `targets`; `noop` the
    nodes where it found nothing to act on (the unit not running); extra: when it was due, its role, its trigger."""
    sent = now_us()
    opts = ["-i", fid] + (["-d", f["args"]["dev"]] if "dev" in f["args"] else [])
    rs = list(pool.map(lambda c: (c[0],) + fault_sh(c[0], *opts, ev, *c[1]), cmds or commands(f, where)))
    noop = sorted(name_of(where, ip) for ip, _, out, _ in rs if any(l.startswith("noop") for l in out.splitlines()))
    log(ev, i=i, id=fid, sent=sent, done=now_us(), rc=max(r[1] for r in rs), stderr="; ".join(r[3] for r in rs if r[3]),
        **{"class": f["class"], "nodes": f["nodes"], "args": f["args"],
           **({"sub": cmds[0][1][0], "targets": sorted(name_of(where, c[0]) for c in cmds)} if cmds else {}),
           **({"noop": noop} if noop else {}), **(extra or {})})
    for ip, _, out, _ in rs:
        resets(log, i, name_of(where, ip), out)


def resets(log, i, node, out):
    """fault.sh prints a line per disk reset; the checker needs the journal's to waive the journal before it."""
    for line in out.splitlines():
        what = line.split()[0] if line else ""
        if what == "journal-reset":
            log("journal-reset", i=i, node=node, how=line.split()[1], detail=line)
        elif what.endswith("-reset"):
            log("disk-reset", i=i, node=node, disk=what[:-len("-reset")], how=line.split()[1], detail=line)


def name_of(where, ip):
    return next(n for n, x in where.items() if x == ip)


def offsets(pool, targets, samples=10):
    """The node's clock minus this one's, from the ping over one ssh session with the shortest round trip: a fresh
    ssh per sample puts the remote process start on one leg only."""
    echo = "import sys, time\nfor _ in sys.stdin: print(time.time_ns(), flush=True)"

    def one(ip):
        try:
            p = subprocess.Popen(remote(ip, "python3 -u -c '%s'" % echo),
                                 stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
            best = None
            for _ in range(samples):
                t0 = time.time_ns()
                p.stdin.write("\n")
                p.stdin.flush()
                line = p.stdout.readline()
                t1 = time.time_ns()
                if not line:
                    return ip, False, None, None
                if best is None or t1 - t0 < best[1]:
                    best = (int(line) - (t0 + t1) // 2, t1 - t0)
            p.stdin.close()
            p.wait(5)
            return ip, True, best[0] / 1e6, best[1] / 1e6
        except (OSError, ValueError, subprocess.TimeoutExpired):
            return ip, False, None, None
    return list(pool.map(one, targets))


SOCKETS = "ss -Htan '( sport = :7400 or dport = :7400 )'"


def sockets(out, where):
    """From SOCKETS' lines, the node's established cluster connections per peer, and those the peer closed."""
    names = {ip: n for n, ip in where.items()}
    estab, close_wait = collections.Counter(), 0
    for line in out.splitlines():
        s = line.split() + ["", "", "", "", ""]
        peer = s[4].rpartition(":")[0].strip("[]").replace("::ffff:", "")
        if s[0] == "ESTAB" and peer in names:
            estab[names[peer]] += 1
        close_wait += s[0] == "CLOSE-WAIT"
    return {"estab": dict(sorted(estab.items())), "close_wait": close_wait}


def settle(pool, log, i, where):
    """fault.sh check on every node, and the part of it only b1 sees: the clock within 50 ms. heal what is not. Then
    each node's cluster connections, for the checker's sockets."""
    def heal(ip):
        rc, out, err = fault_sh(ip, "heal")
        log("heal", i=i, node=name_of(where, ip), rc=rc, out=out, stderr=err)
        resets(log, i, name_of(where, ip), out)

    for ip, (rc, out, err) in zip(where.values(), pool.map(lambda ip: fault_sh(ip, "check"), where.values())):
        log("check", i=i, node=name_of(where, ip), rc=rc, out=out, forced=rc != 0)
        if rc != 0:
            heal(ip)
    for ip, ok, off, rtt in offsets(pool, where.values()):
        far = not ok or abs(off) >= 50
        log("clock", i=i, node=name_of(where, ip), ok=ok, offset_ms=off, rtt_ms=rtt, forced=far)
        if far:
            heal(ip)
    for ip, (rc, out, err) in zip(where.values(), pool.map(lambda ip: ssh(ip, SOCKETS), where.values())):
        if rc == 0:
            log("sockets", i=i, node=name_of(where, ip), **sockets(out, where))


def match(x, want, env):
    """The bindings after line x matches want ({field: value, a list of values, or $name}), or None: a $name seen
    before must be equal, one not seen yet binds."""
    env = dict(env)
    for k, w in want.items():
        v = x.get(k)
        if isinstance(w, str) and w.startswith("$"):
            if env.setdefault(w[1:], v) != v:
                return None
        elif v not in w if isinstance(w, list) else v != w:
            return None
    return env


class Watch:
    """A trigger armed at since (µs): its `when` matched line by line in order; hit is (wall time, last line)."""

    def __init__(self, when, since):
        self.when, self.since, self.env, self.step, self.hit, self.done = when, since, {}, 0, None, False

    def feed(self, x):
        if self.hit or self.done or x.get("t", 0) < self.since:
            return
        env = match(x, self.when[self.step], self.env)
        if env is not None:
            self.env, self.step = env, self.step + 1
            if self.step == len(self.when):
                self.hit = (time.time(), x)


class Tails:
    """The logs the plan's triggers and roles read, as their lines come: the store's (store.jsonl beside the nemesis
    log: the nemesis runs on b1, the store's box), which also says who owns each key, from its first line, and gives
    each accepted own the owner before it as prev; a node log by name through `tail -F` on every node, each line given
    the node's name as n. The store flushes after an own and every 100 ms otherwise; a node log line is one write."""

    def __init__(self, eps, where, d):
        self.lock, self.owner, self.watches, self.procs = threading.Lock(), {}, collections.defaultdict(list), []
        fs = [f for e in eps for f in e["faults"]]
        logs = {f["trigger"]["log"] for f in fs if f.get("trigger")}
        if "store" in logs or any(f["nodes"][0].startswith("@owner") for f in fs):
            threading.Thread(target=self.store, args=(os.path.join(d, "store.jsonl"),), daemon=True).start()
        for name in sorted(logs - {"store"}):
            for n, ip in where.items():
                cmd = "exec tail -n0 -F %s 2>/dev/null" % os.path.join(d, n, name)
                p = subprocess.Popen(remote(ip, cmd), stdout=subprocess.PIPE, stdin=subprocess.DEVNULL, text=True)
                self.procs.append(p)
                threading.Thread(target=self.node, args=(name, n, p), daemon=True).start()

    def feed(self, log, x):
        with self.lock:
            if log == "store" and x.get("r") == "O" and x.get("v") == "ok":
                x["prev"], self.owner[x["k"]] = self.owner.get(x["k"]), x["n"]
            for w in self.watches[log]:
                w.feed(x)

    def store(self, path):
        while not os.path.exists(path):
            time.sleep(0.05)
        with open(path) as f:
            buf = ""
            while True:
                buf += f.readline()
                if not buf.endswith("\n"):
                    time.sleep(0.002)
                    continue
                line, buf = buf, ""
                if '"O"' in line or any(not (w.hit or w.done) for w in self.watches["store"]):
                    try:
                        self.feed("store", json.loads(line))
                    except (ValueError, TypeError):
                        pass

    def node(self, name, n, p):
        for line in p.stdout:
            try:
                self.feed(name, dict(json.loads(line), n=n))
            except (ValueError, TypeError):  # a line that is no JSON object must not end this node's triggers
                pass

    def watch(self, log, when):
        w = Watch(when, now_us())
        with self.lock:
            self.watches[log] = [x for x in self.watches[log] if not (x.hit or x.done)] + [w]
        return w

    def close(self):  # tail exits once its output pipe closes (coreutils polls it)
        for p in self.procs:
            p.kill()


# A role names the node a fault goes on as it goes on: fn(the trigger's line or None, argument, Tails) -> a node or
# None. Where it names none, or one not in the fault's `among`, the fault goes on among[0] and its line says so.
ROLES = {
    "trigger": lambda hit, arg, tails: (hit or {}).get("n"),
    "owner": lambda hit, arg, tails: tails.owner.get(arg),
}


def resolve(f, hit, tails):
    want = f["nodes"][0]
    if not want.startswith("@"):
        return f, {}
    name, _, arg = want[1:].partition(":")
    got = ROLES[name](hit, arg, tails)
    ok = got in f["among"]
    return dict(f, nodes=[got if ok else f["among"][0]]), {"role": {"want": want, "got": got, "hit": ok}}


def episode(pool, log, e, where, tails, planned=None):
    """Each fault on at its offset, or when its trigger fires (on timeout, at `within`), and off its duration later.
    Events due together run in plan order, an off before an on. A step runs on a thread of its own, so a slow one
    (a dm suspend under load takes 10 s and more) holds up only the steps on nodes it touches. Each line says when it
    was due from the episode's start and how late it ran, and how late the episode began after `planned` (the last
    one's settle ran long)."""
    start, seq, todo, armed, busy = time.time(), itertools.count(), [], {}, {}
    late = round((start - planned) * 1000) if planned else 0

    def push(due, off, *ev):
        todo.append((due, off, next(seq)) + ev)

    def launch(fid, g, ev, extra, steps, then=()):  # steps: (fault.sh commands or None, seconds to wait after)
        t = threading.Thread(target=lambda: [(apply(pool, log, e["i"], fid, g, ev, where, c, extra), time.sleep(w))
                                             for c, w in steps], daemon=True)
        t.start()
        busy[t] = (used(g), then)  # then: what is queued once every step is done, the fault's offs

    for j, f in enumerate(e["faults"]):
        push(start + f.get("at", 0), 1, "arm" if f.get("trigger") else "on", "%s.%d" % (e["i"], j), f, None, {})
    while todo or armed or busy:
        for t, (_, then) in list(busy.items()):
            if not t.is_alive():
                del busy[t]
                for ev in then:
                    push(*ev)
        now = time.time()
        for fid, (w, f, until) in list(armed.items()):
            if w.hit or now >= until:
                del armed[fid]
                w.done = True
                t, x = w.hit or (now, None)
                why = {"hit": x is not None}
                if x:  # how long after its line was written the trigger saw it
                    why.update(lag_ms=round((t - x["t"] / 1e6) * 1000, 1), line=x)
                push(t + f["trigger"].get("delay", 0), 1, "on", fid, f, None, {"trigger": why, "hit": x})
        held = set().union(*(n for n, _ in busy.values()))
        for item in sorted(x for x in todo if x[0] <= now):
            due, _, _, kind, fid, f, cmds, ctx = item
            g, role = resolve(f, ctx.get("hit"), tails) if kind == "on" else (f, {})
            if kind != "arm" and used(g) & held:  # steps on a node run in order: none overtakes one held there
                held |= used(g)
                continue
            todo.remove(item)
            if kind == "arm":
                armed[fid] = (tails.watch(f["trigger"]["log"], f["trigger"]["when"]), f, due + f["trigger"]["within"])
                continue
            held |= used(g)
            extra = {"plan_ms": round((due - start) * 1000), "slip_ms": round((time.time() - due) * 1000), "late_ms": late}
            composed = steps(g, where)
            if kind == "off":
                launch(fid, g, "off", extra, composed[1] if composed and cmds is None else [(cmds, 0)])
                continue
            extra.update(role, **({"trigger": ctx["trigger"]} if "trigger" in ctx else {}))
            if g["class"] == "skew-restart":  # step the acceptors back, restart them, and restore the clocks later
                a = g["args"]
                step = [(where[n], ["step", a["step_s"]]) for n in g["nodes"]]
                kill = [(where[n], ["kill", a["hold_s"]]) for n in g["nodes"]]
                launch(fid, g, "on", extra, [(step, 0), (kill, 0)], [
                    (due + a["hold_s"], 0, "off", fid, g, kill, {}),
                    (due + min(a["restore_s"], g["dur"]), 0, "off", fid, g, step, {})])
            else:  # its offs once every on step is done, and not before its duration
                launch(fid, g, "on", extra, composed[0] if composed else [(None, 0)],
                       [(due + g["dur"], 0, "off", fid, g, None, {})])
        time.sleep(0.005)
    time.sleep(max(0, start + e["dur"] - time.time()))


def run(plan_path, log_path, only=None):
    where = nodes_in_use()
    with open(plan_path) as f:
        eps = [json.loads(l) for l in f if l.strip()]
    log = Log(log_path)
    pool = cf.ThreadPoolExecutor(32)
    tails = Tails(eps, where, os.path.dirname(os.path.abspath(log_path)))
    t0 = time.time()
    try:
        settle(pool, log, None, where)
        for e in eps:
            if only is not None and e["i"] != only:
                continue
            if only is None:
                time.sleep(max(0, t0 + e["at"] - time.time()))
            start = time.time()
            episode(pool, log, e, where, tails, t0 + e["at"] if only is None else None)
            t0 += time.time() - start - e["dur"]  # steps that outlast dur (a roll, a slow leave or reset) keep the gap
            settle(pool, log, e["i"], where)
    finally:
        tails.close()


# ---- doctor faults and combos


def geo_ms(x, y):
    """One way between nodes x and y (b1..b5) under geo: spec/wan.md § 8."""
    region = lambda n: {"b1": "eu", "b4": "eu", "b2": "sin"}.get(n, "us")
    pair = {region(x), region(y)}
    return 0.12 if len(pair) == 1 else {frozenset(("eu", "us")): 46, frozenset(("eu", "sin")): 95}.get(
        frozenset(pair), 109.5)


def geo_delays(n, where):
    """fault.sh geo's ip=ms for each of n's peers."""
    return ["%s=%s" % (where[m], geo_ms(n, m)) for m in sorted(where) if m != n]


def backdrop(ev, log_path):
    """TIMING=wan's geo delays on every node in use, before the nodes start and until they stop (run.sh), logged as
    `backdrop` with sent and done: the sessions never see their round trips change under them."""
    where, sent = nodes_in_use(), now_us()
    with cf.ThreadPoolExecutor(len(where)) as pool:
        rs = list(pool.map(lambda n: fault_sh(where[n], "backdrop", ev, *(geo_delays(n, where) if ev == "on" else ())),
                           sorted(where)))
    Log(log_path)("backdrop", on=ev == "on", nodes=sorted(where), sent=sent, done=now_us(), rc=max(r[0] for r in rs),
                  stderr="; ".join(r[2] for r in rs if r[2]), **{"class": "geo"})
    return max(r[0] for r in rs) == 0


# A doctor probe: seen(what before() returned) -> (ok, detail) while the fault is on, after hold seconds; dev the disk
# it goes on (-d; None for the journal); after, if not None, the same once it is off.
Probe = collections.namedtuple("Probe", "cls args seen before hold dev after")


def kit(ip, where, pool):
    """The probes of the node at ip by name, which is also the fault's id, and a shell on the node."""
    peers = [x for x in where.values() if x != ip]
    sh = lambda cmd: ssh(ip, cmd)[1]
    unit, mnt = sh(". ~/rig/node.sh; echo $unit $mnt").split()
    cg = "/sys/fs/cgroup/system.slice/%s.service" % unit
    ports = (7400, 7900) if NETNS else (7400, 22, 7900)  # no sshd in a namespace

    def reach(srcs):
        return {src: {p: ssh(src, "timeout 2 bash -c '</dev/tcp/%s/%d' && echo yes || echo no" % (ip, p))[1] == "yes"
                      for p in ports} for src in srcs}

    def cut(srcs):
        r = reach(srcs)
        return all(not v[7400] and all(v[p] for p in ports[1:]) for v in r.values()), r

    def untouched(srcs):
        r = reach(srcs)
        return all(all(v.values()) for v in r.values()), r

    def both(a, b):
        return a[0] and b[0], {"cut": a[1], "open": b[1]}

    def from_peer(cmd):
        return ssh(where["b1"] if ip != where["b1"] else peers[0], cmd)[1]

    def connect_ms(port):
        return float(from_peer("python3 -c 'import socket,time;t=time.time();socket.create_connection((\"%s\",%d),5)"
                               ".close();print(round((time.time()-t)*1000,1))'" % (ip, port)) or "nan")

    def timed(cmd):
        return sh("t=$(date +%%s%%N); %s 2>&1; echo rc=$?; echo ms=$(( ($(date +%%s%%N) - t) / 1000000 ))" % cmd)

    def events(key):  # the unit's cgroup is gone while it restarts; system.slice counts its children's events too
        return int(sh("cat %s/memory.events /sys/fs/cgroup/system.slice/memory.events 2>/dev/null | "
                      "awk '$1 == \"%s\" {print $2}' | sort -n | tail -1" % (cg, key)) or 0)

    def offset():
        return offsets(pool, [ip])[0][2]

    def pid():
        return sh("systemctl show -p MainPID --value " + unit)

    def shows(cmd, test):  # cmd: a shell command or a function
        def seen(_):
            s = sh(cmd) if isinstance(cmd, str) else cmd()
            return test(s), s
        return seen

    def killed(b):
        a = pid()
        return a not in (b, "0"), {"before": b, "after": a}

    def netem(_):
        ms, other_ms = connect_ms(7400), connect_ms(ports[1])
        return ms >= 297 and other_ms < 50, {"7400_ms": ms, "%d_ms" % ports[1]: other_ms}

    def geo(_):  # the farthest peer's dial waits out the node's delay toward it; another port is not delayed
        me = name_of(where, ip)
        far = max((n for n in where if n != me), key=lambda n: geo_ms(me, n))
        ms = [float(r[1]) for r in dial(where[far], 7400, 3) if r[0] == "ok"]
        other = [float(r[1]) for r in dial(where[far], ports[1], 3) if r[0] == "ok"]
        want = geo_ms(me, far)
        return len(ms) == len(other) == 3 and min(ms) >= 0.9 * want and max(other) < 50, {
            "peer": far, "want_ms": want, "7400_ms": ms, "%d_ms" % ports[1]: other}

    def rate(_):
        from_peer("timeout 3 bash -c 'cat </dev/tcp/%s/7400 >/dev/null'" % ip)
        s = sh("tc -s qdisc show | awk '/qdisc netem 40:/ {getline; print $2}'")
        return int(s or 0) > 0, {"band4_bytes": s}

    def throttled(_):
        stat = "awk '$1 == \"nr_throttled\" {print $2}' %s/cpu.stat" % cg
        m, a = sh("cat %s/cpu.max" % cg), sh(stat)
        time.sleep(1)
        c = sh(stat)
        return m == "20000 100000" and int(c) > int(a), {"cpu.max": m, "nr_throttled": [a, c]}

    def moved(key):
        def seen(b):
            a = events(key)
            return a > b, {key: [b, a]}
        return seen

    def shifted(want):
        def seen(b):
            a = offset()
            return abs(a - b - want) < 20, {"offset_ms": [b, a], "want_ms": want}
        return seen

    def flips(fid):  # its rules come and go while it is on
        def seen(_):
            s = sh("for i in $(seq 15); do sudo iptables -S RIG | grep -c rig:%s; sleep 0.1; done | sort -u" % fid)
            return s.split() == ["0", "1"], {"rules seen": s.split()}
        return seen

    def dial(src, port, n=1, t=2):  # n connects from src: each ok, timeout or its errno's name, and its ms
        py = ("import errno,socket,time\nfor _ in range(%d):\n t=time.time()\n try:\n"
              "  socket.create_connection(('%s',%d),%s).close();r='ok'\n except socket.timeout: r='timeout'\n"
              " except OSError as e: r=errno.errorcode.get(e.errno,'?')\n print(r,round((time.time()-t)*1000,1))"
              % (n, ip, port, t))
        return [l.split() for l in ssh(src, "python3 -c " + shlex.quote(py))[1].splitlines()] or [["none", "nan"]]

    def pull(src):  # the bytes of the 1 MB the node's 7400 sends that arrive within 4 s, none in 1 s ending it
        py = ("import socket,time\nn=0\ntry:\n s=socket.create_connection(('%s',7400),2);s.settimeout(1)\n"
              " end=time.time()+4\n while time.time()<end:\n  b=s.recv(65536)\n  if not b: break\n  n+=len(b)\n"
              "except OSError: pass\nprint(n)" % ip)
        return int(ssh(src, "python3 -c " + shlex.quote(py))[1] or 0)

    def nstat(src, names):
        return sum(int(l.split()[1]) for l in ssh(src, "nstat -saz " + " ".join(names))[1].splitlines()
                   if l.split() and not l.startswith("#"))

    def held(name):  # before(): a session from peers[0] to the node's 7001, a byte echoed every 0.2 s, logged
        log = "/tmp/rig-held-%s-%d" % (name, os.getpid())
        py = ("import os,socket,time\ns=socket.create_connection(('%s',7001),2);s.settimeout(1);end=time.time()+30\n"
              "while time.time()<end and os.path.exists('%s'):\n try:\n  s.sendall(b'x');r='ok' if s.recv(1) else 'eof'\n"
              " except socket.timeout: r='wait'\n except OSError: r='err'\n print(round(time.time(),1),r,flush=True)\n"
              " if r in ('eof','err'): break\n time.sleep(0.2)" % (ip, log))  # ends once session() removes its log
        ssh(peers[0], "setsid nohup python3 -c %s >%s 2>&1 </dev/null & sleep 1" % (shlex.quote(py), log))
        return log

    def session(log):  # what that session saw, its last word, and how long ago on its own clock
        out = ssh(peers[0], "cat %s; rm -f %s; date +%%s.%%N" % (log, log))[1].splitlines()
        rs = [l.split() for l in out[:-1] if len(l.split()) == 2]
        return {"last": rs[-1][1] if rs else None, "seen": sorted({r[1] for r in rs}),
                "age_s": round(float(out[-1]) - float(rs[-1][0]), 1) if rs and out else None}

    def refused(want):
        def seen(_):
            d, o = dial(peers[0], 7400)[0], dial(peers[0], ports[1])[0]
            return d[0] == want and float(d[1]) < 100 and o[0] == "ok", {"7400": d, str(ports[1]): o}
        return seen

    def synless(reset):  # no new session, while the one up lives on, or with reset was aborted
        def seen(log):
            d, s = dial(peers[0], 7400)[0], session(log)
            live = s["last"] == "ok" and s["age_s"] is not None and s["age_s"] < 2
            return d[0] == "timeout" and (s["last"] == "err" if reset else live), {"dial": d, "session": s}
        return seen

    def halfopen(b):  # a new process, and the peer's session to the old one never hears it end
        k, detail = killed(b[0])
        s = session(b[1])
        return k and s["last"] == "wait" and not {"eof", "err"} & set(s["seen"]), dict(detail, session=s)

    def portcut(port, other):
        def seen(_):
            r = {"cut": dial(peers[0], port)[0], "other port": dial(peers[0], other)[0], "other peer": dial(others[0], port)[0]}
            return r["cut"][0] == "timeout" and r["other port"][0] == r["other peer"][0] == "ok", r
        return seen

    def gray(_):
        rs = [r[0] for r in dial(peers[0], 7400, 24, 0.4)]
        return 0 < rs.count("ok") < 24, {"connects": dict(collections.Counter(rs))}

    def mtu(_):
        d, n = dial(peers[0], 7400)[0], pull(peers[0])
        return d[0] == "ok" and n < 1 << 20, {"dial": d, "pulled": n}

    def asym(_):
        a, b = dial(peers[0], 7400)[0], dial(others[0], 7400)[0]
        return a[0] == b[0] == "ok" and float(a[1]) >= 297 and float(b[1]) < 50, {"to": a, "other": b}

    def counted(names):  # counters of peers[0] that move while it pulls from the node
        def seen(b):
            n, a = pull(peers[0]), nstat(peers[0], names)
            return a > b, {"pulled": n, "+".join(names): [b, a]}
        return seen

    def untracked(log):
        d, s = dial(peers[0], 7400)[0], session(log)
        return d[0] == "ok" and s["last"] == "wait", {"dial": d, "session": s}

    def backlog(b):
        k, detail = killed(b)
        q = sh("ss -Hltn 'sport = :7400' | awk '{print $3}'")
        return k and q == "1", dict(detail, backlog=q)

    def exhausted(n):  # connects to itself that hold their ports: n + 1 at most, then EADDRNOTAVAIL
        def seen(_):
            py = ("import errno,socket\nss,rs=[],[]\nfor _ in range(40):\n s=socket.socket();s.settimeout(0.5);ss.append(s)\n"
                  " try:\n  s.connect(('%s',7001));rs.append('ok')\n except socket.timeout: rs.append('timeout')\n"
                  " except OSError as e: rs.append(errno.errorcode.get(e.errno,'?'))\nprint(' '.join(rs))" % ip)
            rs = sh("python3 -c " + shlex.quote(py)).split()
            return 0 < rs.count("ok") <= n + 1 and "EADDRNOTAVAIL" in rs, {"connects": dict(collections.Counter(rs))}
        return seen

    k = {}

    def add(name, args, seen, before=lambda: None, hold=5, dev=None, after=None, cls=None):
        k[name] = Probe(cls or name, args, seen, before, hold, dev, after)

    states = ("for t in $(cat %s/cgroup.threads); do awk '/^State/ {print $2}' /proc/$t/status; done | sort -u" % cg)
    dd = "dd if=/dev/zero of=%s/probe bs=4k count=1 oflag=direct,dsync conv=notrunc status=none" % mnt
    write = "printf %%s | dd of=%s/probe bs=4k count=1 oflag=direct,dsync conv=notrunc,sync status=none" % mnt
    others = [p for p in peers if p != peers[0]]
    down = shows("systemctl is-active " + unit, lambda s: s in ("inactive", "failed"))  # stopped, and not restarting
    add("kill", [0], killed, before=pid)
    add("kill-hold", [3], down, cls="kill")  # held down until off, not restarted after 1 s
    add("pause", [], shows(states, lambda s: s == "T"))
    add("freeze", [], shows("systemctl show -p FreezerState --value " + unit, lambda s: s == "frozen"))
    add("leave", [], down)
    add("isolate", [], lambda b: cut(peers))
    add("partition", [",".join(others)], lambda b: both(cut(others), untouched([peers[0]])))
    add("bridge", [",".join(others[-2:])], lambda b: both(cut(others[-2:]), untouched(peers[:-2])))
    add("oneway", [peers[0]], lambda b: both(cut([peers[0]]), untouched(others)))
    add("netem", [300, 1, 0, 0, 0], netem)
    add("rate", [1], rate)
    delays = geo_delays(name_of(where, ip), where)
    add("geo", delays, geo)
    add("geo-rate", delays + ["rate=50"], shows("tc qdisc show", lambda s: s.count("rate 50Mbit") == len(peers)), cls="geo")
    add("bulk", [], shows(". ~/rig/node.sh; test -e $state/bulk && echo on", lambda s: s == "on"))
    add("cpu", [20], throttled)
    add("oom", [60], moved("oom_kill"), before=lambda: events("oom_kill"))
    add("memhigh", [80], moved("high"), before=lambda: events("high"))
    add("io", [20], shows("cat %s/io.max" % cg, lambda s: "wiops=20" in s))
    add("slowdisk", [300], shows(lambda: timed(dd), lambda s: "rc=0" in s and int(s.split("ms=")[1]) >= 300))
    add("diskerr", [], shows(lambda: timed(dd), lambda s: "Input/output error" in s))
    add("diskdrop", [], shows(write % "B" + "; dd if=%s/probe bs=4k count=1 iflag=direct status=none | head -c1" % mnt,
                              lambda s: s == "A"), before=lambda: sh(write % "A"))
    add("diskfull", [], shows(lambda: timed("dd if=/dev/zero of=%s/probe2 bs=4k count=1 oflag=direct,dsync "
                                            "status=none; stat -c size=%%s %s/probe2" % (mnt, mnt)),
                              # a direct write on the full disk can also come back empty and silent
                              lambda s: "No space left" in s or "size=4096" not in s))
    add("sysctl", ["net.core.somaxconn", 64], shows("sysctl -n net.core.somaxconn", lambda s: s == "64"))
    add("toggle", [300, "oneway", peers[0]], flips("toggle"))
    add("authz", ["b9"], shows(". ~/rig/node.sh; cat $state/authz", lambda s: s == "b9"))
    add("reject", ["tcp-reset", peers[0]], refused("ECONNREFUSED"))
    add("reject-icmp", ["icmp-host-unreachable", peers[0]], refused("EHOSTUNREACH"), cls="reject")
    add("syndrop", [0, peers[0]], synless(False), before=lambda: held("syndrop"))
    add("syndrop-reset", [1, peers[0]], synless(True), before=lambda: held("syndrop-reset"), cls="syndrop")
    add("halfopen", [], halfopen, before=lambda: (pid(), held("halfopen")))
    add("portcut", [7400, peers[0]], portcut(7400, 7001))
    add("portcut-7001", [7001, peers[0]], portcut(7001, 7400), cls="portcut")
    add("graylink", [50, peers[0]], gray)
    add("flap", [200, 200, peers[0]], flips("flap"))
    add("mtu", [1200], mtu)
    add("asym", [300, peers[0]], asym)
    dsack, csum = ["TcpExtTCPDSACKOldSent", "TcpExtTCPDSACKOfoSent"], ["TcpInCsumErrors", "IpExtInCsumErrors", "IpInHdrErrors"]
    add("dup", [100], counted(dsack), before=lambda: nstat(peers[0], dsack))
    add("corrupt", [20], counted(csum), before=lambda: nstat(peers[0], csum))
    add("conntrack", [], untracked, before=lambda: held("conntrack"))
    add("backlog", [1], backlog, before=pid)
    if NETNS or ip != where["b1"]:  # b1's nemesis reaches b1 over ssh, which needs a port for the off
        add("ports", [4], exhausted(4))

    def probed(b):  # the probe's last line, in the log fault.sh named for it (a run's LOG_DIR or the mount), is new
        s = sh(". ~/rig/node.sh; tail -n1 \"$(systemctl show -P Environment rig-fault${n:+-$n}-peer.service"
               " | tr ' ' '\\n' | sed -n 's/^RIG_PEER_LOG=//p')\"")
        x = json.loads(s) if s.startswith("{") else {}
        # A listener answered: doctor's stand-in speaks no axon, so the handshake ends on its magic, not on auth.
        return x.get("t", 0) > b and x.get("outcome") == "refused", s

    add("peer", ["secret"], probed, before=now_us)
    if not NETNS:  # one box has one clock: fault.sh refuses these there, and `shifted` would read b1 against itself
        add("step", [-2], shifted(-2000), before=offset)
        add("drift", [40], shifted(40), before=offset, hold=10)
    # The disk lane's classes, on the streams disk.
    smnt, sdm = sh(". ~/rig/node.sh; device streams; echo $mnt $dm").split()
    sdd = lambda f: "dd if=/dev/zero of=%s/%s bs=4k count=1 oflag=direct,dsync conv=notrunc status=none" % (smnt, f)
    sread = "dd if=%s/probe bs=4k count=1 iflag=direct of=/dev/null status=none" % smnt
    first = lambda f: sh("dd if=%s/%s bs=4k count=1 iflag=direct status=none | od -An -tx1 -N1" % (smnt, f)).strip()

    def cut_off(_):  # powercut: the node down, its disk unmounted under a table that drops writes
        s = sh("systemctl is-active %s; mountpoint -q %s && echo mounted || echo unmounted; sudo dmsetup table %s | "
               "cut -d' ' -f3,6-" % (unit, smnt, sdm)).split("\n")
        return s[0] in ("inactive", "failed") and s[1:] == ["unmounted", "flakey 0 1 1 drop_writes"], s

    def hung(_):  # a direct write, in its own session so the probe does not wait on it, still blocked after 2 s
        s = sh("sudo dmsetup info %s | grep -c SUSPENDED; setsid %s </dev/null >/dev/null 2>&1 & p=$!; sleep 2; "
               "kill -0 $p 2>/dev/null && echo blocked" % (sdm, sdd("probe")))
        return s.split() == ["1", "blocked"], s

    def toggles(_):  # taken while up, then refused once down: ext4 aborts at the first failed journal write
        s = sh("for i in $(seq 30); do %s 2>/dev/null; echo $?; sleep 0.2; done | uniq" % sdd("probe"))
        return s.split()[:2] == ["0", "1"], {"dd exit codes": s.split()}

    def split(_):
        w, r = timed(sdd("probe")), timed(sread)
        wms, rms = int(w.split("ms=")[1]), int(r.split("ms=")[1])
        return "rc=0" in w and "rc=0" in r and wms >= 300 and 50 <= rms < 300, {"write": w, "read": r}

    corrupt = ("head -c 4096 /dev/zero | tr '\\0' A | dd of=%s/probe bs=4k count=1 oflag=direct,dsync status=none; "
               "dd if=%s/probe bs=4k count=1 iflag=direct status=none | od -An -tx1 -j31 -N1" % (smnt, smnt))
    add("powercut", [], cut_off, before=lambda: sh(sdd("probe-pc") + " && printf B | dd of=%s/probe-pc "
                                                   "conv=notrunc status=none" % smnt).strip(),
        dev="streams", after=lambda _: (first("probe-pc") == "00", {"first byte": first("probe-pc")}))
    add("diskhang", [], hung, dev="streams")
    add("rofs", [], shows("findmnt -no OPTIONS %s; touch %s/probe-ro 2>&1" % (smnt, smnt),
                          lambda s: "cannot touch" in s), dev="streams")  # EIO, the mount rw, on 6.8
    add("flaky", [2, 1], toggles, dev="streams", hold=0)
    add("splitdelay", [50, 300], split, dev="streams")
    add("readerr", [], shows(lambda: timed(sread), lambda s: "Input/output error" in s),
        before=lambda: sh(sdd("probe")), dev="streams")
    # EIO, or EROFS once a background write in the hold has made ext4 abort
    add("diskdead", [], shows(lambda: timed(sdd("probe")), lambda s: "Input/output error" in s
                              or "Read-only file system" in s), dev="streams")
    add("diskcorrupt", [], shows(lambda: sh(corrupt).strip(), lambda s: s == "01"), dev="streams")
    add("inodes", [], shows("touch %s/probe-ino 2>&1" % smnt, lambda s: "No space left" in s), dev="streams", hold=1)

    def restarted_with(read, want):  # restarted, and read() shows what the fault set: (read(), pid()) before
        def seen(b):
            a = (read(), pid())
            return want(b[0], a[0]) and a[1] not in (b[1], "0"), {"before": b, "after": a}
        return seen

    link = lambda: sh(". ~/rig/node.sh; readlink $exe")
    over = lambda: sh(". ~/rig/node.sh; cat $override 2>/dev/null")
    add("upgrade", [], restarted_with(link, lambda b, a: a != b), before=lambda: (link(), pid()))
    add("cfgdrift", ["HEARTBEAT_MS", 250], restarted_with(over, lambda b, a: a == "HEARTBEAT_MS=250"),
        before=lambda: (over(), pid()))
    add("scale", [], down)

    # The process lane's probes, against the stand-in: its threads are all `python3`, it starts a thread per
    # connection, and burns a core. tasksmax goes last, since it leaves the stand-in's port 7400 without its thread.
    def cgstat(f, key):
        return lambda: int(sh("awk '$1 == \"%s\" {print $2}' %s/%s" % (key, cg, f)) or 0)

    restarts = lambda: int(sh("systemctl show -p NRestarts --value " + unit) or 0)

    def restarted(b):  # systemd's count, not sampled PIDs: the loop's phase locks onto each start, which then lives 5 ms
        time.sleep(4)
        a = restarts()
        return a - b >= 2, {"restarts": [b, a]}

    def nofile(_):
        p = pid()
        soft, fds = sh("sudo prlimit --pid %s --nofile -o SOFT --noheadings" % p), sh("sudo ls /proc/%s/fd | wc -l" % p)
        return soft.isdigit() and int(soft) <= int(fds or 0) + 2, {"soft": soft, "fds": fds}

    usage = cgstat("cpu.stat", "usage_usec")

    def cpu_rate():  # CPU µs a second, the stand-in's burner most of it
        a = usage()
        time.sleep(1)
        return usage() - a

    def starved(b):
        a = cpu_rate()
        return a < 0.3 * b, {"usage_us_a_s": [b, a]}

    def rose(get):
        def seen(b):
            a = get()
            return a > b, {"before": b, "after": a}
        return seen

    ctl = ". ~/rig/node.sh; d=$(sed -n 's/^LOG_DIR=//p' $env 2>/dev/null); [ -n \"$d\" ] && [ -d \"$d\" ] || d=$state; cat $d/ctl"
    pids_refused = cgstat("pids.events", "max")

    def spawn_refused(b):  # a connection makes the stand-in start a thread, which pids.max refuses
        from_peer("timeout 2 bash -c '</dev/tcp/%s/7400' 2>/dev/null; true" % ip)
        return rose(pids_refused)(b)

    add("crashloop", [500], restarted, before=restarts, hold=1)
    add("killterm", [200, 5], killed, before=pid)
    add("nofile", [0], nofile)
    add("threadstarve", [100, "python3"], starved, before=cpu_rate)
    add("reclaim", [64], rose(cgstat("memory.stat", "pgscan")), before=cgstat("memory.stat", "pgscan"))
    add("hook", ["block", 100], shows(ctl, lambda s: s == "block 100"))
    if not NETNS:  # the box's own state
        cores = "ls /var/tmp/rig-core.* 2>/dev/null | wc -l"
        add("coredump", [], lambda b: (killed(b)[0] and sh(cores) != "0", {"pid": killed(b)[1], "cores": sh(cores)}),
            before=pid, hold=8)
        add("thp", ["never"], shows("cat /sys/kernel/mm/transparent_hugepage/enabled", lambda s: "[never]" in s))
        add("swap", [50], shows("swapon --show=NAME --noheadings", lambda s: "rig-swap" in s))
        status = "adjtimex -p | awk '$1 == \"status:\" {print $2}'"

        def leap(b):  # hours ahead, with STA_INS set
            a, st = offset(), int(sh(status) or 0)
            return abs(a - b) > 1000 and st & 16 > 0, {"offset_ms": [b, a], "status": st}
        add("leap", [], leap, before=offset)
    add("tasksmax", [0], spawn_refused, before=pids_refused)
    sh(write % "A")  # the disk probes write over it in place, which a full disk allows
    return k, sh


CLEAN = ". ~/rig/node.sh; rm -f $mnt/probe $mnt/probe2; device streams; rm -f $mnt/probe*"  # what the probes wrote


def on_off(ip, ev, name, p):
    return fault_sh(ip, "-i", name, *(["-d", p.dev] if p.dev else []), ev, p.cls, *p.args)


def probe(ip):
    """Every fault.sh class on `ip` for about 5 s, seen by its own probe, then off, then check. On one box, every
    other node's fault.sh check is clean while it is on: no fault reaches past its own node."""
    where = nodes_in_use()
    peers = [x for x in where.values() if x != ip]
    pool = cf.ThreadPoolExecutor(16)
    k, sh = kit(ip, where, pool)
    results = []
    for name, c in k.items():
        b = c.before()
        rc, out, err = on_off(ip, "on", name, c)
        time.sleep(c.hold)
        ok, detail = c.seen(b) if rc == 0 else (False, err)
        others = dict(zip(peers, pool.map(lambda p: fault_sh(p, "check")[1], peers))) if NETNS else {}
        orc, _, oerr = on_off(ip, "off", name, c)
        crc, cout, _ = fault_sh(ip, "check")
        r = {"id": name, "class": c.cls, "args": c.args, "seen": ok, "detail": detail, "off": orc == 0, "check": cout}
        if c.after:
            r["after"], r["after_detail"] = c.after(b)
        if NETNS:
            r["others"] = {name_of(where, p): out for p, out in others.items()}
        if orc:
            r["off_err"] = oerr
        results.append(r)
        print(dumps(r), flush=True)
        if crc:
            fault_sh(ip, "heal")
    sh(CLEAN)
    return all(r["seen"] and r["off"] and r["check"] == "clean" and set(r.get("others", {}).values()) <= {"clean"}
               and r.get("after", True) for r in results)


def combos(x, y):
    """Faults stacked on node x, and across x and y, each pair on in turn, then off in both orders: the one still on
    is still seen by its probe after the other's off, every command exits 0 and every node checks clean after both.
    Then a stop on a unit that is not running is a noop, a second fault on a resource refused, and heal clean after a
    mix of faults on both nodes, a suspended journal and an errored streams disk."""
    where = nodes_in_use()
    pool = cf.ThreadPoolExecutor(16)
    (kx, sh), (ky, _) = kit(x, where, pool), kit(y, where, pool)
    k = {x: kx, y: ky}
    pairs = [(x, "isolate", x, "oneway"), (x, "kill-hold", x, "leave"), (x, "pause", x, "freeze"),
             (x, "partition", x, "netem"), (x, "slowdisk", x, "diskfull"), (x, "io", x, "slowdisk"),
             (x, "cpu", x, "memhigh"), (x, "oom", x, "kill-hold"), (x, "toggle", x, "isolate"),
             (x, "sysctl", x, "netem"), (x, "partition", y, "isolate"), (x, "kill-hold", y, "pause"),
             (x, "netem", y, "netem"), (x, "leave", y, "oneway"), (x, "authz", x, "kill-hold"), (x, "flap", x, "isolate"),
             (x, "halfopen", x, "portcut"), (x, "backlog", x, "ports"), (x, "mtu", x, "asym"),
             # the hold ledger across a power cut, and one node's two disks at once
             (x, "powercut", x, "kill-hold"), (x, "slowdisk", x, "diskhang"), (x, "diskfull", x, "inodes")]
    if not NETNS:
        pairs.append((x, "drift", x, "step"))
    pairs += [(x, "nofile", x, "cpu"), (x, "threadstarve", x, "netem"), (x, "reclaim", x, "memhigh"),
              (x, "hook", x, "isolate"), (x, "crashloop", y, "pause")]
    ok = True

    def report(r, good):  # and every node checks clean
        nonlocal ok
        r["check"] = {name_of(where, ip): out for ip, (_, out, _) in zip(where.values(), pool.map(
            lambda ip: fault_sh(ip, "check"), where.values()))}
        r["ok"] = good = good and set(r["check"].values()) == {"clean"}
        ok &= good
        print(dumps(r), flush=True)
        for n, out in r["check"].items():
            if out != "clean":
                fault_sh(where[n], "heal")

    for xa, a, xb, b in [p for p in pairs if p[1] in k[p[0]] and p[3] in k[p[2]]]:
        ca, cb = k[xa][a], k[xb][b]
        fs = [{"class": c.cls, "nodes": [name_of(where, n)],
               "args": {**({"step_s": c.args[0]} if c.cls == "step" else {}), **({"dev": c.dev} if c.dev else {})}}
              for n, c in ((xa, ca), (xb, cb))]
        assert legal(fs), (a, b)
        for first in (a, b):
            ba = ca.before()
            rcs = [on_off(xa, "on", a, ca)[0]]
            bb = cb.before()
            rcs.append(on_off(xb, "on", b, cb)[0])
            time.sleep(3)
            (xo, o, co), (xs, s, cs, bs) = ((xa, a, ca), (xb, b, cb, bb)) if first == a else ((xb, b, cb), (xa, a, ca, ba))
            rcs.append(on_off(xo, "off", o, co)[0])
            time.sleep(2)
            seen, detail = cs.seen(bs)
            rcs.append(on_off(xs, "off", s, cs)[0])
            r = {"on": [[name_of(where, xa), a], [name_of(where, xb), b]], "off first": o, "rc": rcs,
                 "still seen": seen, "detail": detail}
            report(r, not any(rcs) and seen)

    down = on_off(x, "on", "leave", kx["leave"])[0]
    p_on, p_off = on_off(x, "on", "pause", kx["pause"]), on_off(x, "off", "pause", kx["pause"])
    up = on_off(x, "off", "leave", kx["leave"])[0]
    report({"noop": "pause while left", "pause": [p_on[:2], p_off[:2]], "rc": [down, up]},
           not down and not up and all(p[:2] == (0, "noop inactive") for p in (p_on, p_off)))

    n_on = on_off(x, "on", "netem", kx["netem"])[0]
    r_on, r_off = on_off(x, "on", "rate", kx["rate"]), on_off(x, "off", "rate", kx["rate"])
    seen, _ = kx["netem"].seen(None)
    n_off = on_off(x, "off", "netem", kx["netem"])[0]
    report({"refused": "rate over netem", "rate": [r_on[0], r_off[:2]], "netem still seen": seen, "rc": [n_on, n_off]},
           not n_on and not n_off and r_on[0] == 3 and r_off[:2] == (0, "noop qdisc held by fault netem") and seen)

    mix = ["isolate", "netem", "cpu", "slowdisk", "diskfull", "sysctl", "toggle", "leave", "pause"]
    rcs = [on_off(x, "on", m, kx[m])[0] for m in mix] + [on_off(y, "on", "partition", ky["partition"])[0]]
    sh(". ~/rig/node.sh; sudo dmsetup suspend --nolockfs $dm; device streams; "
       "echo 1 | sudo tee /sys/fs/ext4/$(basename $(readlink -f /dev/mapper/$dm))/trigger_fs_error >/dev/null")
    heals = [fault_sh(n, "heal")[:2] for n in (x, y)]
    reset = heals[0][0] == 0 and bool(re.fullmatch(r"streams-reset fsck [01]\nclean", heals[0][1]))
    report({"heal": mix + ["partition on the other", "journal suspended", "streams errored"], "rc": rcs,
            "heals": heals}, not any(rcs) and reset and heals[1] == (0, "clean"))
    sh(CLEAN)
    ssh(y, CLEAN)
    return ok


# ---- the stand-in workload


def standin(journal="/mnt/rig-journal"):
    import mmap
    import socket

    def serve(port):
        s = socket.create_server(("0.0.0.0", port), reuse_port=True)
        while True:
            c, _ = s.accept()
            threading.Thread(target=feed, args=(c, port), daemon=True).start()

    def feed(c, port):
        try:
            if port == 7400:
                c.sendall(b"x" * (1 << 20))
            while port == 7001 and (b := c.recv(4096)):  # an echo, for the probes that hold a session
                c.sendall(b)
        except OSError:
            pass
        c.close()

    def burn():
        while True:
            pass

    def appends():  # a node's journal: page cache charged to the unit beside its own memory, and synced appends
        path = os.path.join(journal, "standin")
        while True:
            try:  # never truncated after a failure, which would give diskfull its space back
                with open(path, "r+b" if os.path.exists(path) else "wb") as f:
                    f.seek(0, os.SEEK_END)
                    while f.tell() < 512 << 20:
                        f.write(b"\0" * (1 << 20))
                    while True:
                        f.write(b"x" * 99 + b"\n")
                        f.flush()
                        os.fdatasync(f.fileno())
                        time.sleep(0.01)
            except OSError:  # diskerr, diskfull
                time.sleep(1)

    for p in (7400, 7001, 7900):
        threading.Thread(target=serve, args=(p,), daemon=True).start()
    threading.Thread(target=burn, daemon=True).start()
    threading.Thread(target=appends, daemon=True).start()
    base = b"\1" * (256 << 20)  # held and touched, so memory.current is real
    while True:
        m = mmap.mmap(-1, 16 << 20)
        for i in range(0, len(m), 4096):
            m[i] = 1
        m.close()
        time.sleep(0.02)


# ---- selftest

# (seed, mode, n, minutes, ONLY): sha256 of what `plan` printed at version 1 on five boxes, and on one, before plan
# versions existed (main at f75fecc, and the one-box filter at 419b9ee). A change to version 1 fails here.
V1 = {
    ("2609", "night", None, None, None): ("5ed81170dcc6ad38bddc75ba3101a589e2f845ccfca50f727960cd8da018f233",
                                          "719b178408f497a7c880047da2c111f9bd827e7d420e670a878651041c156bf8"),
    ("2709", "night", None, None, None): ("bd919a6a73e69f73d8a4fda7ce586f37b9ff798166bc42371a635bb600e04db7",
                                          "bdd7d0e304f8b7a50ae988fe5b650b67603fa7638eba486596769cf4b32bad67"),
    ("2609", "night", 5, 75, None): ("0c1b4e38e5d02e8e4c8d734e0b110522f3fe5d741e6cb60350a7445355104948",
                                     "1ee008b455a44c43793265b225607f310649cda9a1ea50485f7d6c54e6f7425f"),
    ("1", "smoke", None, None, None): ("9a4a724da70b1e1bdbeaf3b5d2fb6bb51ff05ffeb1be7e06129f8871b1850ef7",
                                       "560ed2eff737f41667e9c658453aed8b2f716c518e08710221635d3f5db6b56a"),
    ("32", "smoke", None, None, None): ("b32133b8aeec19207d77eec16bdabf9544f4875cd073ef405fbc3821338b2299",
                                        "413595da13b0f1de0ca9ca82220c01e4d09f5fb4ce07b2b934ddbcce9d5a98d6"),
    ("2609", "smoke", 3, None, None): ("a1f326e772daa50d3f21c621789d68a092a554a56d4c89353651ec0fe5593eca",
                                       "1981830e22c19e521d05cb51a2575c38e440a26d65c946fbe479d6d836ca4c4d"),
    ("501", "smoke", 5, 10, "pause,freeze,oneway,isolate,cpu"): (
        "8b72c2159351848e3fd0d6e91c3fb7a7560fa8c337d3ca6deed13d97175436f4",
        "8b72c2159351848e3fd0d6e91c3fb7a7560fa8c337d3ca6deed13d97175436f4"),
}


def selftest():
    for v, C in dict(PLANS, wan=WAN).items():
        assert all(c in TAGS and TAGS[c] <= VOCAB for c in C), v
    assert set(SOLE) | STOPS <= set(TAGS)
    for (seed, mode, n, m, only), want in V1.items():
        got = tuple(hashlib.sha256("".join(l + "\n" for l in plan_lines(seed, mode, n, m, 1, only, box)).encode())
                    .hexdigest() for box in (False, True))
        assert got == want, ("version 1 changed", seed, mode, n, m, only, got)
    for seed in range(30):
        for v in PLANS:
            for mode in MODES:
                a = plan(seed, mode, 20, v)
                assert a == plan(seed, mode, 20, v), (seed, v, mode)
                if mode == "night":
                    assert plan(seed, mode, 40, v)[:len(a)] == a, (seed, v)
                else:
                    assert {f["class"] for e in a for f in e["faults"]} == set(PLANS[v]), (seed, v)
                for n in range(2, 6):
                    fs = [f for l in plan_lines(seed, mode, n, 20, v, one_box=False) for f in json.loads(l)["faults"]]
                    assert all(used(f) <= set(names(n)) for f in fs), (seed, v, mode, n)
                fs = [f for l in plan_lines(seed, mode, None, 20, v, one_box=True) for f in json.loads(l)["faults"]]
                assert fs and not any(f["class"] in ONE_BOX_OUT for f in fs), (seed, v, mode)
    f = lambda c, n="b2", **a: {"class": c, "nodes": [n], "args": a}
    for fs in ([f("netem"), f("rate")], [f("slowdisk"), f("diskerr")], [f("pause"), f("kill")],
               [f("step", step_s=-2), f("oom")], [f("drift-far"), f("cpu", "b3")], [f("diskhang"), f("pause")],
               [f("diskhang"), f("oom")], [f("diskhang", dev="streams"), f("io", dev="streams")],
               [f("powercut"), f("diskfull")], [f("powercut"), f("inodes")],
               [f("inodes", dev="streams"), f("diskfull", dev="streams")], [f("leave-slowdisk"), f("slowdisk")],
               [f("asym"), f("corrupt")], [f("halfopen"), f("pause")], [f("swap"), f("memhigh")],
               [f("hook"), f("hook")], [f("crashloop"), f("pause")], [f("tasksmax"), f("cpu", "b3")],
               [f("drift-edge", k=49), f("drift", k=10)], [f("pause"), f("pause")], [f("roll"), f("kill")],
               [f("roll"), f("kill", "b3"), f("cpu", "b4")]):
        assert not legal(fs), fs
    for fs in ([f("netem"), f("rate", "b3")], [f("slowdisk"), f("diskerr", dev="streams")], [f("pause"), f("freeze")],
               [f("step", step_s=2), f("oom")], [f("drift-far")], [f("diskhang", dev="streams"), f("io")],
               [f("diskhang"), f("kill", "b3")], [f("powercut", dev="streams"), f("diskfull")],
               [f("leave-slowdisk"), f("slowdisk", "b3")], [f("backlog"), f("ports")], [f("hook"), f("kill")],
               [f("pause"), f("pause", "b3")], [f("kill"), f("leave")], [f("roll"), f("kill", "b3")]):
        assert legal(fs), fs
    assert all(f["args"]["dev"] == "streams" for e in plan(7, "smoke", None, 2) for f in e["faults"]
               if f["class"] in STREAMS_ONLY)
    for seed in range(30):  # a composed class's steps name fault.sh classes on the nodes its fault uses
        for e in plan(seed, "night", 150, 2):
            for g in e["faults"]:
                s = steps(g, {n: n for n in names(5)})
                if s:
                    st = s[0] + s[1]
                    assert s[0] and s[1] and {ip for cmds, _ in st for ip, _ in cmds} <= used(g), g
                    assert {cmd[0] for cmds, _ in st for _, cmd in cmds} <= set(TAGS) | {"upgrade"}, g
    fs = [x for seed in range(20) for e in plan(seed, "smoke", None, 2) for x in e["faults"]]
    for x in fs:  # each draw gives fault.sh every argument its arm reads
        assert set(ARGS.get(x["class"], ())) <= set(x["args"]), x
    two = {c: {len(x["nodes"]) for x in fs if x["class"] == c} for c in ("takeover-step", "drift-edge")}
    assert two == {"takeover-step": {2}, "drift-edge": {2}} and all(
        "b1" not in x["nodes"] for x in fs if x["class"] in CLOCK), two
    selftest_schedules(f)
    ss = ("ESTAB 0 0 10.9.0.1:7400 10.9.0.2:41822\nESTAB 0 0 10.9.0.1:51234 10.9.0.2:7400\n"
          "CLOSE-WAIT 1 0 10.9.0.1:7400 10.9.0.3:40000\nESTAB 0 0 [::ffff:10.9.0.1]:7400 [::ffff:10.9.0.3]:40001\n"
          "LISTEN 0 128 10.9.0.1:7400 0.0.0.0:*")
    got = sockets(ss, {"b1": "10.9.0.1", "b2": "10.9.0.2", "b3": "10.9.0.3"})
    assert got == {"estab": {"b2": 2, "b3": 1}, "close_wait": 1}, got
    print("nemesis selftest: ok")


def selftest_runner(f, at):
    """A slow step (b2's slowdisk on, 0.6 s) holds up the steps on b2 only: b4's pause runs on time, b2's cpu after.
    The bound on b4's slip leaves 250 ms for a loaded CI runner and still fails if b2's step held it."""
    log = os.path.join(tempfile.mkdtemp(), "nemesis.jsonl")
    slow = lambda ip, *a: (time.sleep(0.6 if "slowdisk" in a and "on" in a else 0), (0, "", ""))[1]
    e = {"i": 0, "at": 0, "dur": 0.5, "faults": [at(f("slowdisk", delay_ms=100), 0, 0.4),
                                                 at(f("pause", "b4"), 0.05, 0.1), at(f("cpu", quota_pct=5), 0.05, 0.1)]}
    with mock.patch.object(sys.modules[__name__], "fault_sh", slow), cf.ThreadPoolExecutor(4) as pool:
        episode(pool, Log(log), e, {n: "10.0.0.%s" % n[1:] for n in names(5)}, None)
    got = {(x["id"], x["ev"]): x for x in map(json.loads, open(log))}
    assert got["0.1", "on"]["slip_ms"] < 300 and got["0.1", "off"]["slip_ms"] < 300, got
    assert got["0.2", "on"]["sent"] >= got["0.0", "on"]["done"] <= got["0.0", "off"]["sent"], got


def selftest_schedules(f):
    """Timeline legality, triggers and roles, the schedules deterministic and legal, the pair shards covering every
    cell, coverage reading them back, the replay group."""
    at = lambda g, a, d, **kw: dict(g, at=a, dur=d, **kw)
    role = at(f("kill", "@owner:k1"), 0, 1, among=["b2", "b3"])
    assert timeline_legal([at(f("pause"), 0, 2), at(f("kill"), 2, 3)])
    assert not timeline_legal([at(f("pause"), 0, 2), at(f("kill"), 1, 3)])
    assert not timeline_legal([at(f("pause", "b3"), 0, 5), role]) and timeline_legal([at(f("pause", "b4"), 0, 5), role])
    trig = dict(role, trigger={"log": "store", "when": [{}], "within": 5})
    assert not timeline_legal([trig, at(f("pause", "b3"), 4, 1)])
    assert not timeline_legal([at(f("kill"), 0, 0), at(f("pause"), 0.5, 1)])  # a restart takes its unit a while
    env = match({"r": "O", "k": "k1", "n": "b2"}, {"r": "O", "n": ["b2", "b3"], "k": "$k"}, {})
    assert env == {"k": "k1"} and match({"k": "k2"}, {"k": "$k"}, env) is None
    assert match({"r": "A"}, {"r": "O"}, {}) is None
    w = Watch([{"r": "O", "k": "$k"}, {"r": "O", "k": "$k"}], 10)
    for x in ({"t": 5, "r": "O", "k": "k0"}, {"t": 11, "r": "O", "k": "k1"}, {"t": 12, "r": "O", "k": "k2"},
              {"t": 13, "r": "O", "k": "k1", "n": "b4"}):
        w.feed(x)
    assert w.hit and w.hit[1]["n"] == "b4"
    tails = type("Tails", (), {"owner": {"k1": "b3"}})
    assert resolve(role, None, tails) == (dict(role, nodes=["b3"]),
                                          {"role": {"want": "@owner:k1", "got": "b3", "hit": True}})
    tails.owner = {"k1": "b5"}
    assert resolve(role, None, tails)[0]["nodes"] == ["b2"] and resolve(role, None, tails)[1]["role"]["hit"] is False
    assert resolve(at(f("kill", "@trigger"), 0, 1, among=["b4"]), {"n": "b4"}, tails)[0]["nodes"] == ["b4"]
    real = Tails([], {}, "")  # an own carries the owner before it: the takeover of a key b2 held, not any own
    w = real.watch("store", [{"r": "O", "v": "ok", "prev": "b2"}])
    for k, n in (("k1", "b2"), ("k2", "b3"), ("k1", "b4")):
        real.feed("store", {"t": now_us(), "r": "O", "v": "ok", "k": k, "n": n})
    assert w.hit and w.hit[1]["n"] == "b4" and real.owner == {"k1": "b4", "k2": "b3"}, (w.hit, real.owner)
    selftest_runner(f, at)
    for T in (2, 6, WAN_T):
        for name, fs in library(T).items():
            assert timeline_legal(fs) and all(c["class"] in dict(PLANS[2], **WAN) for c in fs), (T, name)
    selftest_wan()
    with mock.patch.dict(os.environ):  # restored after
        for k in ("TIMING", "SHARD", "SCENARIOS"):
            os.environ.pop(k, None)
        for timing_ in ("smoke", "night"):
            os.environ["TIMING"] = timing_
            T = MODES[timing_]["T"]
            share, cascades, crowds = [], 0, 0
            for seed in range(4):
                for mode in ("chaos", "pairs", "scenarios"):
                    for box in (False, True):
                        a = plan_lines(seed, mode, None, 30, 2, None, box)
                        assert a == plan_lines(seed, mode, None, 30, 2, None, box), (seed, mode, box)
                        eps = [json.loads(l) for l in a]
                        fs = [x for e in eps for x in e["faults"]]
                        assert fs and not (box and any(x["class"] in ONE_BOX_OUT | PROBES for x in fs)), (seed, mode)
                        if mode == "chaos":
                            share += [x["dur"] in edges(T) for x in fs if x["class"] in EDGE]
                            gaps = [b["at"] - a_["at"] - a_["dur"] for a_, b in zip(eps, eps[1:])]
                            cascades += sum(g < 5 * T for g in gaps)
                            crowds += sum(len(e["faults"]) > 3 for e in eps)
                    for k in (1, 2, 3):  # every draw names nodes in use, and a peer where it names any
                        small = [x for l in plan_lines(seed, mode, k, 30, 2, None, False) for x in json.loads(l)["faults"]]
                        assert (small or k < 3) and all(used(x) <= set(names(k)) and all(
                            x["args"].get(a, 1) for a in ("other", "to")) for x in small), (seed, mode, k)
            assert sum(share) >= 0.2 * len(share) and cascades and crowds, (timing_, sum(share), len(share))
        os.environ["TIMING"] = "smoke"
        C, nodes, B = PLANS[2], names(5), 10
        pool = pool_of(C, 5, True, None)
        want = cells(C, pool, nodes, 2, True)
        got, ws = collections.Counter(), []
        for k in range(1, 4):
            os.environ["SHARD"] = "%d/3" % k
            for e in pairs(k, C, pool, nodes, 2, None, True):
                got.update(e["name"].split(","))
                ws += [(x["class"], set(x["nodes"]), (e["at"] + x["at"]) * 1e6, (e["at"] + x["at"] + x["dur"]) * 1e6)
                       for x in e["faults"]]
        assert got == collections.Counter({label(c): 2 if c[2] == "cascade" else 1 for c in want})
        assert set(want) <= related(ws, B * 1e6), sorted(set(want) - related(ws, B * 1e6))[:5]
        assert len(want) > 0.9 * len(pool) * (len(pool) + 1) // 2 * len(RELATIONS)
    nem = [{"ev": "on", "id": "0.0", "i": 0, "class": "kill", "nodes": ["b2"], "sent": 0, "done": 1, "rc": 0},
           {"ev": "off", "id": "0.0", "i": 0, "class": "kill", "nodes": ["b2"], "sent": 5, "done": 6, "rc": 0},
           {"ev": "on", "id": "0.1", "i": 0, "class": "pause", "nodes": ["b3"], "sent": 0, "done": 1, "rc": 1},
           {"ev": "off", "id": "0.1", "i": 0, "class": "pause", "nodes": ["b3"], "sent": 5, "done": 6, "rc": 0},
           {"ev": "on", "id": "0.2", "i": 0, "class": "leave", "nodes": ["b4"], "sent": 0, "done": 1, "rc": 0,
            "noop": ["b4"]},
           {"ev": "off", "id": "0.2", "i": 0, "class": "leave", "nodes": ["b4"], "sent": 5, "done": 6, "rc": 0}]
    assert windows(nem) == [("kill", {"b2"}, 0, 6)]
    slow = [dict(nem[0], id="1.0", i=1, **{"class": "slowdisk"}, done=6_000_000),  # its dm suspend took 6 s
            dict(nem[0], id="1.1", i=1, **{"class": "memhigh"}, sent=6_001_000, done=6_050_000, slip_ms=6001)]
    slow += [dict(x, ev="off", sent=20_000_000, done=20_100_000, slip_ms=0) for x in slow]
    assert related(windows(slow), 10e6) == {("memhigh", "slowdisk", "same")}, related(windows(slow), 10e6)
    # Coverage of a night run reads its cells at the night's T, from its lease; a smoke-timed run beside it has other
    # feasible cells: refused, not miscounted.
    d, smoke = tempfile.mkdtemp(), tempfile.mkdtemp()
    for run_, lease in ((d, 6.0), (smoke, 2.0)):
        with open(os.path.join(run_, "meta.json"), "w") as fh:
            json.dump({"lease_secs": lease, "plan_version": 2, "one_box": True, "nodes": {n: n for n in names(5)}}, fh)
        with open(os.path.join(run_, "nemesis.jsonl"), "w") as fh:
            fh.writelines(dumps(x) + "\n" for x in nem)
    with contextlib.redirect_stdout(io.StringIO()) as buf:
        coverage([d])
    out = buf.getvalue()
    assert "at night timing" in out and "of %d cells" % len(cells(PLANS[2], pool_of(PLANS[2], 5, True, None), names(
        5), 6, False)) in out, out
    for bad in (lambda: coverage([d, smoke]), lambda: plan(1, "scenarios", v=2)):
        with mock.patch.dict(os.environ, SCENARIOS="no-such-scenario"), contextlib.redirect_stdout(io.StringIO()):
            try:
                bad()
                raise AssertionError("not refused")
            except SystemExit:
                pass
    with mock.patch.object(sys.modules[__name__], "library", lambda T: {"x": [fault("kill", "@nope", 0, 1, among=["b2"])]}):
        try:  # a role ROLES lacks is refused when planned, not when the fault goes on
            plan(1, "scenarios", v=2)
            raise AssertionError("not refused")
        except AssertionError as e:
            assert "@nope" in str(e), e
    eps = [{"i": 0, "at": 0, "dur": 10}, {"i": 1, "at": 15, "dur": 5}, {"i": 2, "at": 100, "dur": 5}]
    assert [(e["i"], e["at"]) for e in group(eps, 1, 10)] == [(0, 0), (1, 15)] and len(group(eps, 2, 10)) == 1


def selftest_wan():
    """TIMING=wan plans the eight geo scenarios, the same each time, on one box too; the other timings none of them;
    a geo fault's fault.sh arguments."""
    with mock.patch.dict(os.environ):
        os.environ.pop("SCENARIOS", None)
        for timing in ("smoke", "night", "wan"):
            os.environ["TIMING"] = timing
            for box in (False, True):
                a = plan_lines(3, "scenarios", None, None, 2, None, box)
                assert a == plan_lines(3, "scenarios", None, None, 2, None, box), (timing, box)
                got = {json.loads(l)["name"] for l in a}
                assert got == GEO if timing == "wan" else not got & GEO, (timing, box, got)
        for bad in ({"SCENARIOS": "flap"}, {"TIMING": "night", "SCENARIOS": "geo-quiet"}):
            with mock.patch.dict(os.environ, bad):
                try:
                    plan(1, "scenarios", v=2)
                    raise AssertionError("not refused: %s" % bad)
                except SystemExit:
                    pass
    where = {n: "10.0.0.%s" % n[1:] for n in names(5)}
    rb = library(WAN_T)["rate-bulk"]
    d = lambda n: geo_delays(n, where)
    assert [commands(f, where) for f in rb] == [
        [(where[n], ["geo"] + d(n)) for n in names(5)[1:]], [(where["b1"], ["geo"] + d("b1") + ["rate=150"])],
        [(where["b1"], ["bulk"])]]
    assert d("b2") == ["10.0.0.1=95", "10.0.0.3=109.5", "10.0.0.4=95", "10.0.0.5=109.5"], d("b2")
    loss = library(WAN_T)["geo-loss"][1]
    assert commands(loss, where)[0] == (where["b1"], ["geo"] + d("b1") + ["loss=2", where["b2"]]), commands(loss, where)
    assert (geo_ms("b1", "b4"), geo_ms("b1", "b3"), geo_ms("b2", "b1"), geo_ms("b5", "b2")) == (0.12, 46, 95, 109.5)


def main(argv):
    if argv[:1] == ["plan"] and len(argv) in (3, 4, 5):
        for line in plan_lines(argv[1], argv[2], *map(int, argv[3:]), only=os.environ.get("ONLY")):
            print(line)
    elif argv[:1] == ["version"] and len(argv) == 1:
        print(version())
    elif argv[:1] == ["selftest"] and len(argv) == 1:
        selftest()
    elif argv[:1] == ["replay"] and len(argv) == 5:
        replay(*argv[1:])
    elif argv[:1] == ["coverage"] and len(argv) >= 2:
        coverage(argv[1:])
    elif argv[:1] == ["backdrop"] and len(argv) == 3 and argv[1] in ("on", "off"):
        sys.exit(0 if backdrop(argv[1], argv[2]) else 1)
    elif argv[:1] == ["run"] and len(argv) >= 3:
        run(argv[1], argv[2], int(argv[4]) if argv[3:4] == ["--only"] else None)
    elif argv[:1] == ["offset"]:
        with cf.ThreadPoolExecutor(16) as pool:
            for ip, ok, off, rtt in offsets(pool, argv[1:]):
                print(dumps({"ip": ip, "ssh": ok, "offset_ms": off, "rtt_ms": rtt}))
    elif argv[:1] == ["probe"] and len(argv) == 2:
        sys.exit(0 if probe(argv[1]) else 1)
    elif argv[:1] == ["combos"] and len(argv) == 3:
        sys.exit(0 if combos(argv[1], argv[2]) else 1)
    elif argv[:1] == ["standin"] and len(argv) <= 2:
        standin(*argv[1:])
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main(sys.argv[1:])
