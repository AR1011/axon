#!/usr/bin/env bash
# The rig on one machine, on loopback: a store and three nodes (b1..b3, all acceptors), 200 keys. b3 is killed with
# SIGKILL at 30 s and started again at 40 s, b2 is stopped with SIGSTOP for 8 s at 60 s, and every node leaves with
# SIGTERM at the end. Then: every node has acks, none gave up, the store has owns from all three nodes, and some key
# changed epoch; every node received stream records, subscribed after a cursor, logged no duplicate, reorder or gap,
# and at its last aggregate line heard from every stream it follows.
#
#   examples/rig/ops/local.sh [secs=90]     RIG_BIN (default: cargo build --release -p rig), OUT, BASE (17000)
set -euo pipefail
cd "$(dirname "$0")/../../.."
secs=${1:-90}
(( secs >= 75 )) || { echo "needs at least 75 s for its faults" >&2; exit 2; }
base=${BASE:-17000}
out=${OUT:-target/rig/local-$(date +%Y%m%d-%H%M%S)}
if [[ -z ${RIG_BIN:-} ]]; then
    cargo build --release -p rig
    RIG_BIN=${CARGO_TARGET_DIR:-target}/release/rig
fi
mkdir -p "$out"
out=$(cd "$out" && pwd)

# Only live children: a PID reaped after its kill may already be another process's.
declare -A node
store=
stop_all() { for p in $store "${node[@]}"; do kill -CONT "$p" 2>/dev/null; kill "$p" 2>/dev/null || true; done; }
trap stop_all EXIT

"$RIG_BIN" store "127.0.0.1:$((base + 900))" "$out/store.jsonl" >"$out/store.out" 2>&1 &
store=$!
start() {
    local i=$1
    env RUN=local NODE_ID="b$i" IP=127.0.0.1 PORT=$((base + 400 + i)) SEED_PORT=$((base + i)) \
        NODES="127.0.0.1:$((base + 1)),127.0.0.1:$((base + 2)),127.0.0.1:$((base + 3))" ACCEPTORS=b1,b2,b3 \
        STORE="127.0.0.1:$((base + 900))" LOAD=ledger KEYS=200 EVERY_MS=2000 LEASE_SECS=6 LOG_DIR="$out/b$i" \
        "$RIG_BIN" node >>"$out/b$i.out" 2>&1 &
    node[$i]=$!
}
say() { printf '%3ds  %s\n' "$SECONDS" "$*"; }
SECONDS=0
for i in 1 2 3; do start "$i"; done
say "store and b1..b3 up, logs in $out"
sleep 30; kill -KILL "${node[3]}"; wait "${node[3]}" 2>/dev/null || true; unset 'node[3]'; say "b3 killed"
sleep 10; start 3; say "b3 started again"
sleep 20; kill -STOP "${node[2]}"; say "b2 stopped"
sleep 8; kill -CONT "${node[2]}"; say "b2 continued"
sleep $((secs - 68))
for i in 1 2 3; do kill -TERM "${node[$i]}"; done
for i in 1 2 3; do wait "${node[$i]}" || say "b$i exited $?"; done
node=()
say "b1..b3 left"
sleep 0.5 # the store flushes its log every 100 ms
trap - EXIT
kill "$store"; wait "$store" 2>/dev/null || true

python3 - "$out" <<'EOF'
import json, sys, collections
out = sys.argv[1]
def lines(path):
    try:
        with open(path) as f:
            return [json.loads(l) for l in f if l.strip()]
    except FileNotFoundError:
        return []
bad = []
for n in ("b1", "b2", "b3"):
    c = collections.Counter(l["r"] for l in lines(f"{out}/{n}/client.jsonl"))
    s = collections.Counter(l["r"] for l in lines(f"{out}/{n}/node.jsonl"))
    print(f"{n}: {c['send']} sent, {c['ack']} acked, {c['give_up']} gave up; {s['up']} up, {s['start']} starts, "
          f"{s['stop']} stops, {s['stale']} stale")
    if not c["ack"]: bad.append(f"{n} has no acks")
    if c["give_up"]: bad.append(f"{n} gave up {c['give_up']} ops")
    sl = lines(f"{out}/{n}/stream.jsonl")
    r = collections.Counter(l["r"] for l in sl)
    agg = [l for l in sl if l["r"] == "streams"]
    recv, lag = sum(l["recv"] for l in agg), max([l["max_lag_ms"] for l in agg] or [None])
    afters = sum(l["afters"] for l in agg)
    print(f"{n} streams: {r['open']} opened, {recv} received, {afters} after a cursor, max lag {lag} ms, {r['resume']} resumes, "
          f"{r['reset']} resets, {r['origin_lost']} origin lost; {r['dup']} dups, {r['reorder']} reorders, {r['gap']} gaps")
    if not recv: bad.append(f"{n} received no stream records")
    if not afters: bad.append(f"{n} never subscribed after a cursor")
    if r["dup"] or r["reorder"] or r["gap"]: bad.append(f"{n} logged stream anomalies")
    if not agg or agg[-1]["quiet"]: bad.append(f"{n} silent streams at the end: {agg[-1]['quiet'] if agg else 'no line'}")
store = lines(f"{out}/store.jsonl")
v = collections.Counter((l["r"], l["v"]) for l in store)
owners = {l["n"] for l in store if l["r"] == "O" and l["v"] == "ok"}
epochs = collections.defaultdict(set)
for l in store:
    if l["r"] == "O" and l["v"] == "ok": epochs[l["k"]].add(l["e"])
moved = sum(len(e) > 1 for e in epochs.values())
print(f"store: {dict(sorted(v.items()))}; owns from {sorted(owners)}; {moved} of {len(epochs)} keys changed epoch")
if owners != {"b1", "b2", "b3"}: bad.append(f"owns only from {sorted(owners)}")
if not moved: bad.append("no key changed epoch")
print("FAIL: " + "; ".join(bad) if bad else "PASS")
sys.exit(1 if bad else 0)
EOF
