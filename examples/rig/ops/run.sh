#!/usr/bin/env bash
# One rig run, on b1 (spec/fault-rig.md § Running it). scripts/rig.sh starts it detached, so a run never depends on
# the machine that started it. Its first line of output is the run's name; everything after goes to runs/<run>/run.log.
#
#   run.sh smoke [seed]            ledger, 500 keys at 5 s a node, lease 2 s: 1 min steady, every class once, then
#                                  from plan version 2 a few pairs and scenarios (SMOKE_MIX=0 not), 1 min settle
#   run.sh soak [min=30]           serve, SOAK_KEYS (10 000; 2000 on one box) at 1 s a node: half plain, half with
#                                  b5 held to 4 cores
#   run.sh night [seed] [min=150]  soak min/5, then ledger, 2000 keys at 20 s a node, lease 6 s: 5 min steady, min
#                                  minutes of the plan, 3 min settle
#   run.sh replay <run> <i> [n=5]  that run's settings: 3 min steady, its episode i with the episodes before it whose
#                                  aftermath reached it, the group n times B + 15 s apart, 1 min settle
#   run.sh chaos|pairs|scenarios [seed] [min]
#                                  a schedule (nemesis.py plan), timed as a smoke (TIMING=smoke, the default: its ledger,
#                                  1 min steady and settle) or a night (TIMING=night: its ledger, 5 min and 3 min); or
#                                  for scenarios, TIMING=wan: the night's ledger at lease 10 s, every node on the rig's
#                                  WAN profile (TIMINGS=wan, src/main.rs), and the WAN scenarios (spec/wan.md § 12),
#                                  WORKLOADS accounts,streams,eager,echo,bulk,firehose unless set
#
# Then it stops the nodes, collects every node's logs and journals into runs/<run>/b<n>/, runs check.py, and writes
# summary.txt, then runs/<run>.tgz, which appears whole and last. Whatever the exit, every node is stopped and healed.
# RIG_N=n runs on the first n nodes of ~/rig/nodes only (the plan keeps the episodes that touch no other node);
# ACCEPTORS=5 makes all five acceptors. On one box (setup.sh --netns) it runs in b1's namespace, as it would on b1.
# probe.py polls every node's Health port throughout (probes.jsonl). TLS=1 runs the cluster over mutual TLS from
# ~/rig/tls, made here, and rotates its CA in three passes during a smoke or night (tls.jsonl); SEEDS=n serves seeds
# on b1..bn only.
set -euo pipefail
R=/home/ubuntu/rig
exec 9>$R/run.lock
flock -n 9 || { echo "a run is in progress: $(cat $R/current 2>/dev/null)" >&2; exit 1; }
# RIG_LOCK: held for the whole run, read-only so another user's file will do (flock(1) would create it)
[[ -z ${RIG_LOCK:-} ]] || { exec 8<"$RIG_LOCK" || exit 1; flock -n 8 || { echo "$RIG_LOCK held: a benchmark runs" >&2; exit 1; }; }
mode=${1:-}
mapfile -t ips < <(grep . $R/nodes)
ips=("${ips[@]:0:${RIG_N:-${#ips[@]}}}")
b1=${ips[0]}
export RIG_N=${#ips[@]}
PLAN_VERSION=$(python3 $R/nemesis.py version)   # PLAN_VERSION=v plans that version; a replay takes its run's
export PLAN_VERSION
case $mode in
    # TIMING times a schedule only: a smoke, soak or night left with TIMING=wan would run its nodes on the WAN profile
    smoke) seed=${2:-$RANDOM} plan_min=${3:-} TIMING= ;;   # ONLY=a,b.. limits the classes; with min, rounds of them
    night) seed=${2:-$RANDOM} plan_min=${3:-150} minutes=$((${3:-150} / 5)) TIMING= ;;   # the soak scales with the plan
    soak) seed=0 minutes=${2:-30} TIMING= ;;
    chaos | pairs | scenarios) seed=${2:-$RANDOM} plan_min=${3:-}
        [[ ${TIMING:=smoke} =~ ^(smoke|night)$ || $TIMING == wan && $mode == scenarios ]] ||
            { echo "TIMING is smoke or night, or wan for scenarios" >&2; exit 2; }
        # the WAN scenarios judge bulk, firehose and echo: without them live-under-bulk and stream-* judge nothing
        [[ $TIMING != wan ]] || export WORKLOADS=${WORKLOADS:-accounts,streams,eager,echo,bulk,firehose}
        export TIMING ;;
    replay)
        src=$R/runs/${2:?usage: run.sh replay <run> <i> [n]} ep=${3:?episode} reps=${4:-5}
        read -r seed TIMING < <(python3 -c 'import json, sys; m = json.load(open(sys.argv[1])); print(m["seed"], m.get("timing", ""))' "$src/meta.json") ;;
    *) sed -n '/^#   /p' "$0"; exit 2 ;;
esac
RUN=${RUN:-$mode-$(date -u +%Y%m%d-%H%M)-s$seed}
D=$R/runs/$RUN
mkdir -p "$D"
echo "$RUN"
exec >>"$D/run.log" 2>&1
echo "$RUN" >$R/current
echo $$ >$R/run.pid
run_start=$(($(date +%s) - 60))   # each node's unit log is collected from here, a minute of skew aside

say() { printf '%s  %s\n' "$(date -u +%T)" "$*"; }
now_us() { date +%s%6N; }
# options for the rig's addresses are in ~/.ssh/config (scripts/rig.sh setup); on one box, in.sh enters the node's namespace
on() { if [[ -e $R/netns ]]; then $R/in.sh "$@"; else ssh "$1" "${@:2}"; fi; }
each() {   # runs "$@ <i>" for every node at once; fails if one did
    local pids=() i rc=0
    for i in "${!ips[@]}"; do "$@" "$i" & pids+=($!); done
    for i in "${pids[@]}"; do wait "$i" || rc=1; done
    return $rc
}
pause() { sleep "$1" & wait $!; }   # a signal ends the run at once, not after the sleep
logdir() { echo "$D/b$(($1 + 1))"; }

quiesce() { on "${ips[$2]}" "sudo ~/rig/fault.sh $1" | sed "s/^/b$(($2 + 1)) heal: /"; }   # stop|heal i

store=
store_ok() {
    [[ -n $store ]] && kill -0 "$store" 2>/dev/null &&
        timeout 5 bash -c "exec 3<>/dev/tcp/$b1/7900 && echo PING >&3 && read -r l <&3 && [[ \$l == ERR* ]]"
}

acceptors=$(if [[ ${ACCEPTORS:-3} == 5 ]]; then echo b1,b2,b3,b4,b5; else echo b1,b2,b3; fi)
acceptors=$(cut -d, -f1-"${#ips[@]}" <<<"$acceptors")
secret=$(head -c 16 /dev/urandom | od -An -tx1 | tr -d ' \n')
phases=()   # name start end, b1 µs
load=- keys=0 every=0 lease=6

start_one() {   # i: env, then the unit, then until its node.jsonl says `up`
    local i=$1 ip=${ips[$1]} L; L=$(logdir "$1")
    on "$ip" "umask 077; . ~/rig/node.sh; { cat; echo JOURNAL=\$mnt; echo AUTHZ=\$state/authz; echo BULK=\$state/bulk; device streams; echo STREAM_DISK=\$mnt/runs/$RUN; } >\$env" <<EOF
RUN=$RUN
NODE_ID=b$((i + 1))
IP=$ip
NODES=$(IFS=,; echo "${ips[*]}")
ACCEPTORS=$acceptors
PORT=7400
SEED_PORT=7001
STORE=$b1:7900
LOAD=$load
KEYS=$keys
EVERY_MS=$every
LEASE_SECS=$lease
SECRET=$secret
LOG_DIR=$L
${WORKLOADS:+WORKLOADS=$WORKLOADS}
${FIREHOSE_MBS:+FIREHOSE_MBS=$FIREHOSE_MBS}
${FIREHOSE_BRANCH_MB:+FIREHOSE_BRANCH_MB=$FIREHOSE_BRANCH_MB}
$([[ ${TIMING:-} != wan ]] || echo TIMINGS=wan)
${TLS:+TLS_DIR=$R/tls}
${SEEDS:+SEEDS=$SEEDS}
EOF
    on "$ip" '. ~/rig/node.sh; ln -sfn rig $exe'   # every phase starts on the build checked below; a roll moves it
    on "$ip" ". ~/rig/node.sh; ups() { grep -c '\"r\":\"up\"' $L/node.jsonl 2>/dev/null; }; c=\$(ups)
        touch \$up && sudo systemctl start \$unit && for _ in \$(seq 60); do
        [ \"\$(ups)\" -gt \"\${c:-0}\" ] 2>/dev/null && exit 0; sleep 1; done
        systemctl status --no-pager -n 20 \$unit; exit 1"
}

phase() {   # name load keys every_ms lease_s: every node started with these
    local name=$1
    load=$2 keys=$3 every=$4 lease=$5
    store_ok || { say "the store is not answering"; exit 1; }
    each start_one || { say "a node did not come up"; exit 1; }
    phases+=("$name $(now_us)")
    say "phase $name: $load, $keys keys at $every ms a node, lease $lease s, on ${ips[*]}"
}

phase_end() {   # the phase ends before its nodes leave, so their stops are no drop
    phases[-1]+=" $(now_us)"
    each quiesce stop
    say "phase ${phases[-1]%% *} ended, nodes stopped"
}

nemesis() {   # plan: its episodes applied, then every node healed
    say "nemesis: $(wc -l <"$1") episodes"
    python3 $R/nemesis.py run "$1" "$D/nemesis.jsonl" 8>&- 9>&-& nem=$!
    wait $nem
    nem=
    each quiesce heal
}
ledger() {   # smoke|night|wan [tls rotation every s]: the ledger phase around the plan, at that timing
    local steady=60 settle=60 t=6   # not `lease`: phase sets the global
    [[ $1 != wan ]] || t=10
    if [[ $1 == night || $1 == wan ]]; then phase ledger ledger 2000 20000 $t; steady=300 settle=180
    else phase ledger ledger 500 5000 2; fi
    [[ -z ${TLS:-} || -z ${2:-} ]] || { rotator "$2" 8>&- 9>&-& }
    pause $steady
    nemesis "$D/plan.jsonl"
    pause $settle
    phase_end
}

meta() {
    python3 - "$D" "$mode" "$seed" "$lease" "$acceptors" "${ips[*]}" "$load $keys $every" "${phases[@]}" <<'EOF'
import hashlib, json, os, sys
d, mode, seed, lease, acceptors, ips, params, *phases = sys.argv[1:]
def sha(p):
    return hashlib.sha256(open(p, "rb").read()).hexdigest() if os.path.exists(p) else None
build = json.load(open(os.path.expanduser("~/rig/build.json"))) if os.path.exists(os.path.expanduser("~/rig/build.json")) else {}
ips = ips.split()
m = {"run": os.path.basename(d), "seed": int(seed), "mode": mode, "lease_secs": float(lease),
     "acceptors": acceptors.split(","), "nodes": {"b%d" % (i + 1): ip for i, ip in enumerate(ips)},
     "commit": build.get("commit"), "dirty": build.get("dirty"), "plant": build.get("plant", False),
     "binary_sha256": sha(os.path.expanduser("~/rig/rig")),
     "binary_v2_sha256": sha(os.path.expanduser("~/rig/rig-v2")),
     "plan_sha256": sha(os.path.join(d, "plan.jsonl")), "params": dict(zip(("load", "keys", "every_ms"), params.split())),
     "phases": [dict(zip(("name", "start", "end"), p.split()[:1] + [int(x) for x in p.split()[1:]])) for p in phases],
     "one_box": os.path.exists(os.path.expanduser("~/rig/netns")), "plan_version": int(os.environ["PLAN_VERSION"])}
m.update({k: os.environ[e] for k, e in (("tls", "TLS"), ("seeds", "SEEDS"), ("workloads", "WORKLOADS"),
                                      ("firehose_branch_mb", "FIREHOSE_BRANCH_MB")) if os.environ.get(e)})
m.update(json.loads(os.environ.get("META_EXTRA") or "{}"))
json.dump(m, open(os.path.join(d, "meta.json"), "w"), indent=1)
EOF
}

kernel_log() { echo "sudo journalctl -k --since @$run_start -o json --output-fields=MESSAGE --no-pager >$1 || true"; }

collect_one() {   # i: its journal, the archives of either disk's resets, its unit's log and its kernel's, into its log dir; then to b1
    local ip=${ips[$1]} L; L=$(logdir "$1")
    on "$ip" ". ~/rig/node.sh
        mkdir -p $L && cp \$mnt/journal-$RUN.jsonl $L/ 2>/dev/null || true
        for k in journal streams; do a=$D/\$k-archive\${n:+-\$n}
            if [ -d \$a ]; then mkdir -p $L/\$k-archive && mv \$a/* $L/\$k-archive/ && rmdir \$a; fi
        done
        sudo journalctl -u \$unit --since @$run_start -o json --output-fields=MESSAGE --no-pager >$L/unit.jsonl || true
        $([[ -e $R/netns ]] || kernel_log "$L/kernel.jsonl")"
    [[ $ip == "$b1" || -e $R/netns ]] || rsync -a "$ip:$L/" "$L/"
}

summary() {   # verdict
    python3 - "$D" "$1" <<'EOF' >"$D/summary.txt"
import collections, datetime, json, os, sys
d, verdict = sys.argv[1:]
def lines(p):
    if not os.path.exists(p):
        return []
    out = []
    for l in open(p):
        try:
            out.append(json.loads(l))
        except ValueError:
            pass
    return out
m = json.load(open(os.path.join(d, "meta.json")))
nem, plan = lines(os.path.join(d, "nemesis.jsonl")), lines(os.path.join(d, "plan.jsonl"))
t0 = m["phases"][0]["start"] if m.get("phases") else 0
hm = lambda us: datetime.datetime.fromtimestamp(us / 1e6, datetime.timezone.utc).strftime("%H:%M:%S")
print("run %s" % m["run"])
print("seed %s, mode %s, commit %s%s, binary %s, plan %s" % (m["seed"], m["mode"], m.get("commit"),
      " (dirty)" if m.get("dirty") else "", (m.get("binary_sha256") or "-")[:12], (m.get("plan_sha256") or "-")[:12]))
print("nodes %s; acceptors %s" % (" ".join("%s %s" % kv for kv in m["nodes"].items()), ",".join(m["acceptors"])))
for p in m.get("phases", []):
    print("phase %s %s to %s UTC (%d s)" % (p["name"], hm(p["start"]), hm(p.get("end", p["start"])),
                                           (p.get("end", p["start"]) - p["start"]) / 1e6))
seen, failed, bad = collections.defaultdict(set), [], set()
eps = {}
for x in nem:
    if x.get("ev") in ("on", "off"):
        if x["rc"] == 0:
            seen[x["class"]].add(x["ev"])
        else:
            bad.add(x["class"])
            failed.append("episode %s %s %s%s rc %s: %s" % (x["i"], x["class"], x["ev"], " " + x["sub"] if x.get(
                "sub") else "", x["rc"], x.get("stderr", "")[:200]))
        e = eps.setdefault(x["i"], {"classes": set(), "start": x["sent"], "end": x["done"]})
        e["classes"].add(x["class"])
        e["start"], e["end"] = min(e["start"], x["sent"]), max(e["end"], x["done"])
planned = sorted({f["class"] for e in plan for f in e["faults"]})
both = [c for c in planned if seen[c] == {"on", "off"} and c not in bad]
if plan:
    print("episodes run %d of %d planned; per class %s" % (len(eps), len(plan), dict(sorted(collections.Counter(
        c for e in eps.values() for c in e["classes"]).items()))))
    print("classes with on and off, every command rc 0: %d of %d planned%s" % (len(both), len(planned),
          "" if len(both) == len(planned) else "; missing " + " ".join(c for c in planned if c not in both)))
    print("forced heals: %d" % sum(1 for x in nem if x.get("forced")))
for f in failed:
    print("fault command failed: " + f)
if "busy" in m:
    print("busy: %s held to 4 cores from %.0f s to %.0f s" % (m["busy"]["node"], (m["busy"]["from"] - t0) / 1e6,
                                                            (m["busy"]["to"] - t0) / 1e6))
print("clock offsets to b1 at the end (ms): %s" % open(os.path.join(d, "offsets.txt")).read().strip()
      if os.path.exists(os.path.join(d, "offsets.txt")) else "clock offsets: none")
print("VERDICT %s" % verdict)
vs = [v for v in lines(os.path.join(d, "violations.jsonl")) if not v.get("probe")]
if vs:
    v = min(vs, key=lambda v: v["t"])
    i = v.get("episode")  # check.py's
    e = eps.get(i)
    print("first violation %s at %.1f s, episode %s%s" % (v["inv"], (v["t"] - t0) / 1e6, i, "" if e is None else
          " %s from %.1f s to %.1f s" % (sorted(e["classes"]), (e["start"] - t0) / 1e6, (e["end"] - t0) / 1e6)))
if m["mode"] == "replay":  # a repetition is hit when a violation, not a probe's, names an episode of its group
    g = int(m.get("group", 1))
    hit = len({v["episode"] // g for v in vs if v.get("episode") in eps})
    print("replay of %s episode %s, a group of %d: hit %d of %d" % (m.get("source"), m.get("episode"), g, hit,
                                                                  len(eps) // g))
ons = [x for x in nem if x.get("ev") in ("on", "off") and "slip_ms" in x]
if ons:
    trig = [x["trigger"] for x in ons if "trigger" in x]
    roles = [x["role"] for x in ons if "role" in x]
    late = {x["i"]: x.get("late_ms", 0) for x in ons}
    print("timeline: max slip %d ms, %d of %d steps over 100 ms late; %d episodes began over 1 s late (at most %d ms); "
          "triggers fired %d of %d, lag at most %s ms; roles on their node %d of %d" % (
          max(x["slip_ms"] for x in ons), sum(x["slip_ms"] > 100 for x in ons), len(ons),
          sum(v > 1000 for v in late.values()), max(late.values()), sum(t["hit"] for t in trig), len(trig),
          max((t["lag_ms"] for t in trig if t["hit"]), default="-"), sum(r["hit"] for r in roles), len(roles)))
named = [e for e in plan if e.get("name")]
if named:
    hits = collections.defaultdict(set)
    for v in lines(os.path.join(d, "violations.jsonl")):
        for i in v.get("episodes") or [v.get("episode")]:
            hits[i].add(v["inv"])
    print("named episodes %d, with a violation %d" % (len(named), sum(1 for e in named if e["i"] in hits)))
    for e in named:
        if e["i"] in hits:
            print("  episode %d %s: %s" % (e["i"], e["name"], ", ".join(sorted(hits[e["i"]]))))
EOF
    cat "$D/summary.txt"
}

wrap_up() {   # verdict, or check to run check.py: stops the store, collects, writes the summary and then the bundle
    local rc=0 verdict=$1
    [[ -z $probe ]] || { kill "$probe" 2>/dev/null; wait "$probe" 2>/dev/null || true; probe=; }
    pause 0.5   # the store flushes its log every 100 ms
    [[ -z $store ]] || { kill "$store" 2>/dev/null; wait "$store" 2>/dev/null || true; store=; }
    python3 $R/nemesis.py offset "${ips[@]:1}" | python3 -c '
import json, sys
print(" ".join("b%d %s" % (i + 2, "%.2f" % x["offset_ms"] if x["offset_ms"] is not None else "none")
               for i, x in enumerate(map(json.loads, sys.stdin))))' >"$D/offsets.txt" || true
    each collect_one || say "collecting failed on a node"
    [[ ! -e $R/netns ]] || eval "$(kernel_log "$D/kernel.jsonl")"   # one box: one kernel, every node's
    meta
    if [[ $verdict == check ]]; then
        python3 $R/check.py "$D" >/dev/null || rc=$?
        verdict="$(sed -n 's/^VERDICT //p' "$D/check.txt" 2>/dev/null || true) (check.py exit $rc)"
    fi
    summary "$verdict"
    tar -C $R/runs -czf "$R/runs/$RUN.tgz.part" "$RUN" && mv "$R/runs/$RUN.tgz.part" "$R/runs/$RUN.tgz"
    say "bundle $R/runs/$RUN.tgz"
}

# TLS=1: CA a then CA b, and per node a leaf of each for bN (the cluster) and bN-seed (its seed server), all valid from
# yesterday, so a box's clock behind b1's never sees one not yet valid. The passes: p0 trusts a with a's leaves, p1
# trusts a and b, p2 trusts both with b's leaves, p3 trusts b; `current` links the one in force.
tls_certs() {
    local T=$R/tls c i p from to
    from=$(date -u -d yesterday +%y%m%d%H%M%SZ) to=$(date -u -d 'next year' +%y%m%d%H%M%SZ)
    rm -rf "$T" && mkdir -p "$T/ca/new" && touch "$T/ca/index.txt" && echo 01 >"$T/ca/serial"
    cat >"$T/ca/ca.cnf" <<EOF
[ca]
default_ca = rig
[rig]
dir = $T/ca
database = \$dir/index.txt
new_certs_dir = \$dir/new
serial = \$dir/serial
default_md = sha256
policy = any
unique_subject = no
[any]
commonName = supplied
[root]
basicConstraints = critical,CA:true
keyUsage = critical,keyCertSign,cRLSign
[leaf]
basicConstraints = critical,CA:false
keyUsage = critical,digitalSignature
extendedKeyUsage = serverAuth,clientAuth
subjectAltName = \$ENV::SAN
EOF
    sign() { SAN=${SAN:-DNS:x} openssl ca -batch -notext -config "$T/ca/ca.cnf" -startdate "$from" -enddate "$to" "$@" 2>/dev/null; }
    key() { openssl req -new -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -keyout "$1.key" -out "$1.csr" -subj "/CN=$2" 2>/dev/null; }
    for c in a b; do
        key "$T/ca/$c" "rig-ca-$c" && sign -selfsign -keyfile "$T/ca/$c.key" -in "$T/ca/$c.csr" -out "$T/ca/$c.pem" -extensions root
        for i in "${!ips[@]}"; do
            key "$T/ca/b$((i + 1))-$c" "b$((i + 1))"
            SAN="DNS:b$((i + 1)),DNS:b$((i + 1))-seed" sign -cert "$T/ca/$c.pem" -keyfile "$T/ca/$c.key" \
                -in "$T/ca/b$((i + 1))-$c.csr" -out "$T/ca/b$((i + 1))-$c.crt" -extensions leaf
        done
    done
    for p in "0 a a" "1 a a b" "2 b a b" "3 b b"; do
        set -- $p
        mkdir -p "$T/p$1"
        for i in "${!ips[@]}"; do
            cp "$T/ca/b$((i + 1))-$2.crt" "$T/p$1/b$((i + 1)).crt" && cp "$T/ca/b$((i + 1))-$2.key" "$T/p$1/b$((i + 1)).key"
        done
        for c in "${@:3}"; do cat "$T/ca/$c.pem"; done >"$T/p$1/ca.pem"
    done
    ln -sfn p0 "$T/current"
    chmod -R go-rwx "$T"
    # set -e passes over a failure before an `&&`, and openssl's errors go nowhere: every file is checked here.
    for p in 0 1 2 3; do
        for c in ca.pem $(for i in "${!ips[@]}"; do echo "b$((i + 1)).crt b$((i + 1)).key"; done); do
            [[ -s $T/p$p/$c ]] || { say "tls: $T/p$p/$c not made"; exit 1; }
        done
    done
    [[ -e $R/netns ]] || for i in "${!ips[@]}"; do [[ ${ips[$i]} == "$b1" ]] || rsync -a --delete "$T/" "${ips[$i]}:$T/"; done
    say "tls: CA a and b, passes p0..p3, current p0"
}
rotate_one() {   # pass i: `current` to that pass on the node, then SIGHUP to its rig, which sets the certificates in it
    on "${ips[$2]}" "$([[ -e $R/netns ]] || echo "ln -sfn p$1 ~/rig/tls/current.new && mv -T ~/rig/tls/current.new ~/rig/tls/current;")
        . ~/rig/node.sh; sudo systemctl kill -s HUP --kill-whom=main \$unit 2>/dev/null || true"
}
rotator() {   # every_s: the three passes, each on every node before the next, every_s apart
    local k t
    for k in 1 2 3; do
        pause "$1"
        t=$(now_us)
        [[ ! -e $R/netns ]] || { ln -sfn "p$k" $R/tls/current.new && mv -T $R/tls/current.new $R/tls/current; }
        each rotate_one "$k" || true
        printf '{"t": %s, "done": %s, "pass": %d}\n' "$t" "$(now_us)" "$k" >>"$D/tls.jsonl"
        say "tls: pass $k"
    done
}

nem=
watch=
probe=
cleanup() {
    local rc=$?
    trap - EXIT TERM INT HUP
    kill $(jobs -p) 2>/dev/null || true   # a pause's sleep would keep run.lock, refusing the next run until it ends
    [[ -z $watch ]] || kill "$watch" 2>/dev/null || true
    [[ -z $nem ]] || { kill "$nem" 2>/dev/null; wait "$nem" 2>/dev/null || true; }
    each quiesce stop || true
    if [[ ! -e $R/runs/$RUN.tgz ]]; then
        (( ${#phases[@]} == 0 )) || [[ ${phases[-1]} == *" "*" "* ]] || phases[-1]+=" $(now_us)"
        say "aborted (exit $rc)"
        wrap_up "ABORTED (exit $rc): $(grep -v heal: "$D/run.log" | tail -2 | head -1)" || true
    fi
    rm -f $R/run.pid
    say "every node stopped and healed"
}
trap cleanup EXIT
trap 'exit 143' TERM INT HUP

say "run $RUN on ${ips[*]}"
for b in rig rig-v2; do   # meta.json names b1's binaries, rig-v2 the build a roll moves to: every node must run them
    [[ $b == rig || -e $R/$b ]] || continue
    sum=$(sha256sum <$R/$b | cut -d' ' -f1)
    for i in "${!ips[@]}"; do
        [[ $(on "${ips[$i]}" "sha256sum <~/rig/$b" | cut -d' ' -f1) == "$sum" ]] ||
            { say "b$((i + 1)) has no ~/rig/$b, or not b1's: scripts/rig.sh build"; exit 1; }
    done
done
each quiesce stop
for i in "${!ips[@]}"; do on "${ips[$i]}" ". ~/rig/node.sh; rm -f \$mnt/journal-*.jsonl; device streams; rm -rf \$mnt/runs; mkdir -p $(logdir "$i")"; done
$R/rig store "$b1:7900" "$D/store.jsonl" >"$D/store.out" 2>&1 8>&- 9>&-& store=$!
pause 1
[[ -z ${TLS:-} ]] || tls_certs
python3 $R/probe.py "$D/probes.jsonl" "${ips[@]}" 8>&- 9>&-&
probe=$!
( while pause 60; do store_ok || { say "the store stopped answering: ending the run"; kill -TERM $$; exit; }; done ) 8>&- 9>&-&
watch=$!

plan() { python3 $R/nemesis.py plan "$1" "$2" "$RIG_N" ${3:-} >"$D/plan.jsonl"; }   # seed mode [minutes], on the nodes in use

case $mode in
    smoke)
        plan "$seed" smoke "$plan_min"
        ledger smoke 30 ;;
    soak | night)
        busy=${ips[-1]}
        phase soak serve "${SOAK_KEYS:-$([[ -e $R/netns ]] && echo 2000 || echo 10000)}" 1000 6
        pause $((minutes * 30))
        on "$busy" 'sudo ~/rig/fault.sh on cpu 400'
        say "b${#ips[@]} rig-node held to 4 cores"
        META_EXTRA="{\"busy\": {\"node\": \"b${#ips[@]}\", \"from\": $(now_us)"
        pause $((minutes * 30))
        on "$busy" 'sudo ~/rig/fault.sh off cpu 400'
        META_EXTRA+=", \"to\": $(now_us)}}"
        export META_EXTRA
        phase_end
        if [[ $mode == night ]]; then
            plan "$seed" night "$plan_min"
            ledger night $((plan_min * 15))
        fi ;;
    replay)
        cp "$src/plan.jsonl" "$D/source-plan.jsonl"
        read -r load keys every lease PLAN_VERSION group < <(python3 $R/nemesis.py replay "$src" "$ep" "$reps" "$D/plan.jsonl")
        export META_EXTRA="{\"source\": \"$(basename "$src")\", \"episode\": $ep, \"group\": $group${TIMING:+, \"timing\": \"$TIMING\"}}"
        phase ledger "$load" "$keys" "$every" "$lease"
        pause 180
        nemesis "$D/plan.jsonl"
        pause 60
        phase_end ;;
    chaos | pairs | scenarios)
        plan "$seed" "$mode" "$plan_min"
        export META_EXTRA="{\"timing\": \"$TIMING\"${SHARD:+, \"shard\": \"$SHARD\"}}"
        # Geo's delays before the nodes start, until phase_end's stop clears them: put on under running sessions,
        # they stalled each stream 10 to 15 s (BBR kept the loopback's round trip), scenarios-20261027-0626-s1.
        [[ $TIMING != wan ]] || python3 $R/nemesis.py backdrop on "$D/nemesis.jsonl" ||
            { say "the geo backdrop did not go on"; exit 1; }
        ledger "$TIMING"
        [[ $TIMING != wan ]] || python3 $R/nemesis.py backdrop off "$D/nemesis.jsonl" || true ;;
esac
kill "$watch" 2>/dev/null || true; watch=
wrap_up check
