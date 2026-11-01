#!/usr/bin/env bash
# The real-fault rig on five boxes (spec/fault-rig.md). RIG_NODES="ip1 .. ip5" names them, b1..b5 by position; b1 is
# the control box. Needs ssh as ubuntu with passwordless sudo on each, and jq here.
#
#   scripts/rig.sh setup                  examples/rig/ops to ~/rig and setup.sh on every node, b1's ssh key on every
#                                         node with a ControlMaster config on b1; then doctor
#   scripts/rig.sh doctor                 one JSON line per node: what is missing, empty once set up, the offset to b1
#                                         and its host TCP settings (spec/wan.md § 8)
#   scripts/rig.sh doctor faults <node>   on an idle node (b2 or its address): the unit runs a stand-in, every fault.sh
#                                         class goes on and off, each seen by its own probe; one JSON line per class
#   scripts/rig.sh doctor combos <node>   the same stand-ins on it and a neighbour: pairs of faults stacked on it and
#                                         across the two, off in both orders, then a noop, a refused claim and a heal of
#                                         a mix; one JSON line per case, each with every node's fault.sh check
#   scripts/rig.sh build                  sync this tree to RIG_BUILD (a box with Rust) as ~/rig-src, build rig there,
#                                         put the binary, rig-v2 (feature v2, the other build a roll moves to) and
#                                         examples/rig/ops in ~/rig on every node; ~/rig/build.json on b1 records the
#                                         commit and both binaries' sha256
#   scripts/rig.sh build plant <name>     the same with a planted bug in the box copy only: examples/rig/plants/<name>.sh
#                                         run in the tree first (spec/fault-rig.md names the invariant each must fail);
#                                         build.json names the plant, and a plain build puts the tree back
#   scripts/rig.sh smoke [seed] [min]     a smoke run (ops/run.sh) started on b1, waited for; then fetch, exit 1 unless PASS
#                                         ONLY=pause,cpu.. limits it to those classes, in rounds for [min] minutes
#   scripts/rig.sh soak [min] | night [seed] [min]
#                                         started on b1, which runs it alone; prints the run's name
#   scripts/rig.sh replay <run> <i> [n]   episode i of that run n times on a fresh cluster, waited for; then fetch
#   scripts/rig.sh chaos|pairs|scenarios [seed] [min]
#                                         a schedule (spec/fault-rig.md § The nemesis and its schedules) started on
#                                         b1, waited for, as smoke;
#                                         TIMING=night times it as a night, TIMING=wan (scenarios only) as a night on
#                                         the WAN profile with the WAN scenarios (spec/wan.md § 12); SHARD=k/m the
#                                         kth of m shards of the pairs; SCENARIOS=a,b only those; SMOKE_MIX=0 a smoke
#                                         of single classes only
#   scripts/rig.sh coverage <run>..       the class pairs those runs covered, fetching them first
#   scripts/rig.sh status                 the run in progress or the last one: its log's tail, episodes begun, its summary
#   scripts/rig.sh fetch <run> [all]      summary, check, plan, meta, nemesis log and violations to target/rig/<run>/, and
#                                         the bundle when the check did not pass or with all
#   scripts/rig.sh gate <run> [baseline] [ratio]
#                                         a fetched run's recovery per class against a baseline's (examples/rig/
#                                         baseline-night.json), check.py --gate: exit 1 on a REGRESSION, 2 on another kind
#   scripts/rig.sh down                   ends a run in progress (it heals as it exits), then stops and heals every node
#   scripts/rig.sh heal                   fault.sh heal on every node; prints its check
#   scripts/rig.sh teardown               one box only: setup.sh --remove, every node's namespace, unit and journal gone
#
# RIG_N=n uses only the first n nodes, for runs and every command above; ACCEPTORS=5 makes all five acceptors;
# PLAN_VERSION=v plans a smoke or night with that version of the nemesis's classes (the newest by default).
# WORKLOADS=accounts,streams,.. runs those workloads on every node instead of src/main.rs's default; FIREHOSE_MBS the
# firehose workload's rate (13), FIREHOSE_BRANCH_MB every node's branch bytes (32). RIG_LOCK=<file> on the box: a run
# starts only while it holds that flock, for a box shared with benchmarks.
# TLS=1 runs the cluster over mutual TLS, its CA rotated in three passes mid-run; SEEDS=n serves seeds on b1..bn only.
# RIG_BOX=<ip> runs all of it on that one box instead (spec/fault-rig.md § Layout): nodes b1..b5 are network
# namespaces at 10.77.0.1..5, reached through ssh to the box and ~/rig/in.sh; RIG_BUILD defaults to it, and the clock
# classes are left out. RIG_SRC names the build's directory on the build box (rig-src).
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ ${1:-} == gate ]]; then   # local files only: no nodes needed
    exec python3 examples/rig/ops/check.py --gate "target/rig/${2:?usage: scripts/rig.sh gate <run> [baseline] [ratio]}" \
        "${3:-examples/rig/baseline-night.json}" ${4:-}
fi
if [[ -n ${RIG_BOX:-} ]]; then
    nodes=(10.77.0.1 10.77.0.2 10.77.0.3 10.77.0.4 10.77.0.5)   # setup.sh --netns
else
    read -ra nodes <<<"${RIG_NODES:?set RIG_NODES=\"ip1 .. ip5\", b1 first, or RIG_BOX=<ip> for one box}"
fi
nodes=("${nodes[@]:0:${RIG_N:-${#nodes[@]}}}")
b1=${nodes[0]} ops=examples/rig/ops ctl=${RIG_BOX:-${nodes[0]}}
command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }

box_ssh() { ssh -o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=15 -o ServerAliveCountMax=4 "ubuntu@$1" "${@:2}"; }
on() {
    if [[ -n ${RIG_BOX:-} ]]; then box_ssh "$RIG_BOX" "~/rig/in.sh $1 $(printf %q "${*:2}")"; else box_ssh "$@"; fi
}
push() { COPYFILE_DISABLE=1 tar --no-xattrs -C $ops -cf - . | box_ssh "${RIG_BOX:-$1}" 'mkdir -p ~/rig && tar -C ~/rig -xmf -'; }
pushes() { if [[ -n ${RIG_BOX:-} ]]; then push; else each push; fi; }   # one ~/rig on one box
each() {   # runs "$@ <ip>" on every node at once; fails if one did
    local pids=() ip rc=0
    for ip in "${nodes[@]}"; do "$@" "$ip" & pids+=($!); done
    for ip in "${pids[@]}"; do wait "$ip" || rc=1; done
    return $rc
}
address() {
    case $1 in b[1-9]) echo "${nodes[${1#b} - 1]}" ;; *) echo "$1" ;; esac
}

setup_one() { push "$1" && on "$1" "sudo bash ~/rig/setup.sh ${nodes[*]} >/dev/null"; }

setup() {
    if [[ -n ${RIG_BOX:-} ]]; then
        push && box_ssh "$RIG_BOX" "sudo bash ~/rig/setup.sh --netns ${#nodes[@]} >/dev/null"
        doctor
        return
    fi
    each setup_one
    on "$b1" '[ -f ~/.ssh/id_ed25519 ] || ssh-keygen -q -t ed25519 -N "" -f ~/.ssh/id_ed25519'
    local key; key=$(on "$b1" cat .ssh/id_ed25519.pub)
    for ip in "${nodes[@]}"; do on "$ip" "grep -qxF '$key' ~/.ssh/authorized_keys || echo '$key' >>~/.ssh/authorized_keys"; done
    on "$b1" "touch ~/.ssh/config && chmod 600 ~/.ssh/config && sed -i '/^# rig begin/,/^# rig end/d' ~/.ssh/config && cat >>~/.ssh/config" <<EOF
# rig begin
Host ${nodes[*]}
    User ubuntu
    BatchMode yes
    ConnectTimeout 5
    ServerAliveInterval 15
    ServerAliveCountMax 4
    StrictHostKeyChecking accept-new
    ControlMaster auto
    ControlPath ~/.ssh/rig-%C
    ControlPersist 10m
# rig end
EOF
    doctor
}

doctor() {
    local offs i=0 ip
    offs=$(on "$b1" "python3 - offset ${nodes[*]}" <$ops/nemesis.py)
    for ip in "${nodes[@]}"; do
        i=$((i + 1))
        on "$ip" 'bash -s doctor' <$ops/setup.sh |
            jq -c --arg node "b$i" --arg ip "$ip" --argjson o "$(jq -c --arg ip "$ip" 'select(.ip == $ip)' <<<"$offs")" '
                {node: $node, ip: $ip, offset_ms: $o.offset_ms, missing: (.missing
                    + (if $o.ssh then [] else ["ssh from b1"] end)
                    + (if $o.offset_ms != null and $o.offset_ms > -5 and $o.offset_ms < 5 then [] else ["offset to b1 under 5 ms"] end)),
                 tcp: .tcp}'
    done
}

faults() {   # probe|combos <node>: a neighbour (not b1) runs a stand-in too, for combos to stack with, and on one box
    local what=$1 ip x other= with=()   # because the probe wants every other node clean meanwhile
    ip=$(address "${2:?usage: scripts/rig.sh doctor faults|combos <node>}")
    for x in "${nodes[@]}"; do [[ $x == "$ip" || $x == "$b1" ]] || { other=$x; break; }; done
    with=("$ip")
    if [[ -n ${RIG_BOX:-} || $what == combos ]]; then
        with+=("${other:?no neighbour of $ip other than b1}")
    fi
    [[ $what == combos ]] || other=
    push "$b1"
    # A dropped ssh leaves its probe running on b1, which would stack its faults on this one's.
    ! on "$b1" "pgrep -f 'nemesis.py (probe|combos)'" || { echo "a doctor probe still runs on b1: end it first" >&2; exit 1; }
    [[ -n ${RIG_BOX:-} ]] || for x in "${with[@]}"; do push "$x"; done
    for x in "${with[@]}"; do
        on "$x" '. ~/rig/node.sh; systemctl is-active -q $unit' && { echo "rig-node runs on $x: doctor $what needs idle nodes" >&2; exit 1; }
    done
    # A stand-in left behind would be what the next run's unit starts: every failure below still reaches the cleanup.
    local rc=0 seen=0 out
    for x in "${with[@]}"; do
        on "$x" '. ~/rig/node.sh; sudo mkdir -p /run/systemd/system/$unit.service.d &&
            printf "[Service]\nExecStart=\nExecStart=/usr/bin/python3 /home/ubuntu/rig/nemesis.py standin $mnt\n" |
                sudo tee /run/systemd/system/$unit.service.d/standin.conf >/dev/null &&
            sudo systemctl daemon-reload && touch $up && sudo systemctl start $unit' || { rc=1; break; }
    done
    # Detached on b1 and polled, so a dropped ssh neither ends it halfway nor leaves it stacking faults on the next.
    if (( rc == 0 )) && on "$b1" "rm -f ~/rig/doctor.rc; setsid nohup bash -c 'python3 -u ~/rig/nemesis.py $what $ip $other >~/rig/doctor.out 2>&1;
            echo \$? >~/rig/doctor.rc' </dev/null >/dev/null 2>&1 &"; then
        until rc=$(on "$b1" "cat ~/rig/doctor.rc 2>/dev/null") && [[ -n $rc ]]; do
            sleep 5
            out=$(on "$b1" "tail -n +$((seen + 1)) ~/rig/doctor.out" 2>/dev/null) || continue
            [[ -z $out ]] || { echo "$out"; seen=$((seen + $(wc -l <<<"$out"))); }
        done
        on "$b1" "tail -n +$((seen + 1)) ~/rig/doctor.out" || true
    else
        rc=1
    fi
    for x in "${with[@]}"; do
        on "$x" '. ~/rig/node.sh; rm -f $up; sudo systemctl stop $unit; rm -f $mnt/standin; sudo rm -f /run/systemd/system/$unit.service.d/standin.conf;
            sudo systemctl daemon-reload; sudo ~/rig/fault.sh heal' >&2 || rc=1
    done
    return $rc
}

fault_sh() { echo "$2 $(on "$2" "sudo ~/rig/fault.sh $1" 2>&1 | tail -1)"; }   # heal|stop ip

# run.sh's PID on b1, if it is still run.sh; exits 1 otherwise, also when run.sh died without removing run.pid
alive='p=$(cat ~/rig/run.pid 2>/dev/null) && [ -e /proc/$p ] && grep -qa run.sh /proc/$p/cmdline'
running() { on "$b1" "$alive"; }

build() {
    local box=${RIG_BUILD:-${RIG_BOX:?set RIG_BUILD to a box with Rust}} dir=${RIG_SRC:-rig-src} commit dirty plant=false name
    ! running || { echo "a run is in progress: scripts/rig.sh down first" >&2; exit 1; }
    commit=$(git rev-parse HEAD)
    dirty=$([[ -n $(git status --porcelain) ]] && echo true || echo false)
    if [[ ${1:-} == plant ]]; then
        name=${2:-}
        [[ -n $name && $name != lib && -f examples/rig/plants/$name.sh ]] || {
            echo "build plant <name>, one of: $(cd examples/rig/plants && ls *.sh | sed 's/\.sh$//' | grep -vx lib | xargs)" >&2
            exit 2
        }
        plant=\"$name\"
    fi
    BOX=ubuntu@$box scripts/box.sh sync "$dir"
    [[ $plant == false ]] || BOX=ubuntu@$box scripts/box.sh run "$dir" "bash examples/rig/plants/$name.sh"
    # The rolling upgrade's other build first, kept apart: the plain build after it leaves release/rig as rig.
    BOX=ubuntu@$box scripts/box.sh run "$dir" 'cargo build --release -p rig --features v2 && cp target/release/rig target/rig-v2'
    BOX=ubuntu@$box scripts/box.sh run "$dir" 'cargo build --release -p rig'
    pushes
    local b
    for b in rig:release/rig rig-v2:rig-v2; do
        if [[ $box == "$ctl" ]]; then
            on "$b1" "cp ~/${BOX_ROOT:-axon}/$dir-target/${b#*:} ~/rig/${b%%:*}.new"
        else
            scp -3 -q "ubuntu@$box:${BOX_ROOT:-axon}/$dir-target/${b#*:}" "ubuntu@$ctl:rig/${b%%:*}.new"
        fi
    done
    on "$b1" "mv ~/rig/rig.new ~/rig/rig && mv ~/rig/rig-v2.new ~/rig/rig-v2 &&
        printf '{\"commit\": \"$commit\", \"dirty\": $dirty, \"plant\": $plant, \"sha256\": \"%s\", \"sha256_v2\": \"%s\"}\n' \
        \$(sha256sum ~/rig/rig | cut -d' ' -f1) \$(sha256sum ~/rig/rig-v2 | cut -d' ' -f1) >~/rig/build.json && cat ~/rig/build.json"
    each copy_one
}
copy_one() {   # from b1, which reaches every node; run.sh refuses a node whose binaries are not b1's
    local b
    [[ $1 == "$b1" || -n ${RIG_BOX:-} ]] || for b in rig rig-v2; do
        on "$b1" "scp -q ~/rig/$b $1:rig/$b.new && ssh $1 'mv ~/rig/$b.new ~/rig/$b'"
    done
}

start() {   # mode args..: run.sh detached on b1, which runs it alone; prints the run's name
    ! running || { echo "a run is in progress: scripts/rig.sh status, or down" >&2; exit 1; }
    pushes
    local run
    run=$(on "$b1" "RIG_N=${RIG_N:-} ACCEPTORS=${ACCEPTORS:-3} ONLY=${ONLY:-} SOAK_KEYS=${SOAK_KEYS:-} PLAN_VERSION=${PLAN_VERSION:-} WORKLOADS=${WORKLOADS:-} \
        TLS=${TLS:-} SEEDS=${SEEDS:-} TIMING=${TIMING:-} SHARD=${SHARD:-} SCENARIOS=${SCENARIOS:-} SMOKE_MIX=${SMOKE_MIX:-} FIREHOSE_MBS=${FIREHOSE_MBS:-} \
        FIREHOSE_BRANCH_MB=${FIREHOSE_BRANCH_MB:-} RIG_LOCK=${RIG_LOCK:-} setsid nohup ~/rig/run.sh $* >~/rig/run.out 2>&1 </dev/null &
        sleep 2; head -1 ~/rig/run.out")
    on "$b1" "test -d ~/rig/runs/$run" || { echo "run.sh did not start: ${run:-see ~/rig/run.out}" >&2; exit 1; }
    echo "$run"
}

wait_for() {   # run: until run.sh wrote its bundle, which comes last, or ended without one; ssh errors are retried
    local rc
    while :; do
        rc=0
        on "$b1" "[ ! -e ~/rig/runs/$1.tgz ] && $alive" 2>/dev/null || rc=$?
        (( rc == 1 )) && return
        sleep 20
    done
}

fetch() {
    local run=${1:?usage: scripts/rig.sh fetch <run> [all]} d=target/rig/$1
    mkdir -p "$d"
    on "$b1" "cd ~/rig/runs/$run && tar -cf - \$(ls summary.txt check.txt plan.jsonl meta.json nemesis.jsonl violations.jsonl \
        run.log recovery.json 2>/dev/null)" | tar -C "$d" -xf -
    if [[ ${2:-} == all ]] || ! grep -q '^VERDICT PASS' "$d/summary.txt" 2>/dev/null; then
        scp -q "ubuntu@$ctl:rig/runs/$run.tgz" "$d/"
    fi
    echo "$d"
}

waited() {   # mode args..: start, wait, fetch, print the summary; exit 1 unless it passed
    local run; run=$(start "$@")
    echo "$run started; scripts/rig.sh status shows it" >&2
    wait_for "$run"
    local d; d=$(fetch "$run")
    cat "$d/summary.txt"
    grep -q '^VERDICT PASS' "$d/summary.txt" && ! grep -q '^fault command failed' "$d/summary.txt"
}

status() {
    on "$b1" "run=\$(cat ~/rig/current 2>/dev/null); d=~/rig/runs/\$run
        if $alive; then echo \"running \$run\"; else echo \"idle, last run \${run:-none}\"; fi
        [ -n \"\$run\" ] || exit 0
        tail -3 \$d/run.log
        [ ! -e \$d/plan.jsonl ] || echo \"episodes begun \$(grep '\"ev\":\"on\"' \$d/nemesis.jsonl 2>/dev/null |
            grep -o '\"i\":[0-9]*' | sort -u | wc -l) of \$(wc -l <\$d/plan.jsonl)\"
        [ ! -e \$d/summary.txt ] || cat \$d/summary.txt"
}

down() {
    on "$b1" "if $alive; then kill \$p; while kill -0 \$p 2>/dev/null; do sleep 1; done; fi"
    each fault_sh stop
}

case ${1:-} in
    setup) setup ;;
    doctor) case ${2:-} in faults) faults probe "${3:-}" ;; combos) faults combos "${3:-}" ;; *) doctor ;; esac ;;
    build) build "${@:2}" ;;
    smoke) waited smoke "${2:-$RANDOM}" ${3:-} ;;
    soak | night) start "$@" ;;
    replay) waited "$@" ;;
    chaos | pairs | scenarios) waited "$1" "${2:-$RANDOM}" ${3:-} ;;
    coverage)
        shift
        for run in "$@"; do [[ -e target/rig/$run/meta.json ]] || fetch "$run" >/dev/null; done
        python3 $ops/nemesis.py coverage "${@/#/target/rig/}" ;;
    status) status ;;
    fetch) fetch "${@:2}" ;;
    down) down ;;
    heal) each fault_sh heal ;;
    teardown)
        [[ -n ${RIG_BOX:-} ]] || { echo "teardown is for one box: set RIG_BOX" >&2; exit 2; }
        ! running || { echo "a run is in progress: scripts/rig.sh down first" >&2; exit 1; }
        box_ssh "$RIG_BOX" 'sudo bash ~/rig/setup.sh --remove' ;;
    *) sed -n '/^#   /p' "$0"; exit 2 ;;
esac
