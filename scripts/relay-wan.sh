#!/usr/bin/env bash
# Streams, a plain request and Paxos renewals over real round trips, and one TCP connection's ceiling, on a Linux
# box: two network namespaces w1 and w2 at 10.78.0.1 and .2 on bridge wan0 (the rig's one-box layout, under names of
# its own), each link held to 1 Gbit/s with netem's delay. The harness: crates/axon-stream/examples/relay_wan.rs; the
# rows: crates/axon/guide/benchmarks.md § Streams across a WAN.
#
#   scripts/relay-wan.sh [runs] [cell..]   from the laptop: sync and build on BOX (box.sh, in BOX_DIR, default
#                                          relay-wan), run the cells through SUDO_BOX (a user with sudo
#                                          on the same machine) under nohup, and under flock of the box's file LOCK if
#                                          set, fetch target/relay-wan/<time>-<commit>/ and print its table. COMMIT
#                                          names a tree without .git
#   scripts/relay-wan.sh box <bin> <out> <runs> <cell..>   on the box, as root: the cells, then every namespace,
#                                          qdisc and process it made is gone again
#   scripts/relay-wan.sh table <results>   medians and ranges of a results file
#
# A cell is rtt_ms:loss_pct:tuned:what[:rate], rate netem's (1gbit, or as 50mbit), what one of tcp (one connection,
# 1 MiB writes), axon<frame>[s][x<streams>][b]
# or prof<frame>.. (that once, perf on w1 and w2): <frame> the pushes' max_frame and max_push_bytes in MiB (1 shipped),
# s the child's subscribers at the shipped limits (else each holds at least four pushes), x<streams> the streams (1),
# b the namespace on Lane::Bulk (Live).
# tuned 0 is Linux's default TCP settings, 1 those of spec/wan.md § 8 (with fq on each node's eth0, as
# net.core.default_qdisc is the host's), both set inside the namespaces only. netem sits on the bridge side of each
# veth: half the round trip and the 1 Gbit/s rate each way; a loss applies to both directions, with TSO and GSO off so
# that netem drops wire-sized packets. Node i runs on cores 4i-2..4i+1.
set -euo pipefail
cells=()
for rtt in 0 92 190 219; do
    for tuned in 0 1; do cells+=("$rtt:0:$tuned:tcp" "$rtt:0:$tuned:axon1" "$rtt:0:$tuned:axon1x64"); done
done
cells+=(219:0:1:axon1b 219:0:1:axon1x64b)
secs=10

table() {
    python3 - "$1" <<'EOF'
import collections, statistics, sys
rows = collections.defaultdict(list)
for line in open(sys.argv[1]):
    f = dict(p.split("=", 1) for p in line.split() if "=" in p)
    who = f.get("node", "tcp")
    for k, v in f.items():
        try:
            x = float(v)
            if x == x:   # NaN: a spread with no samples
                rows[(f["cell"], who, k)].append(x)
        except ValueError:
            if k in ("events", "key_kept"):
                rows[(f["cell"], who, k)].append(v)
skip = {"run", "load", "streams"}
for (cell, who, k), vs in sorted(rows.items()):
    if k in skip:
        continue
    if isinstance(vs[0], str):
        print(f"{cell:<18} {who:<4} {k:<18} {' | '.join(vs)}")
        continue
    m = statistics.median(vs)
    print(f"{cell:<18} {who:<4} {k:<18} {m:>10.2f}   [{min(vs):.2f} .. {max(vs):.2f}] n={len(vs)}")
EOF
}

if [[ ${1:-} == table ]]; then table "$2"; exit; fi

if [[ ${1:-} != box ]]; then   # the laptop
    cd "$(dirname "$0")/.."
    runs=${1:-3}
    (( $# > 1 )) && cells=("${@:2}")
    for cell in "${cells[@]}"; do   # they run as root
        [[ $cell =~ ^[0-9]+:[0-9.]+:[01]:[a-z0-9]+(:[0-9]+[mg]bit)?$ ]] || { echo "not a cell: $cell" >&2; exit 2; }
    done
    [[ ${LOCK:-} =~ ^[A-Za-z0-9/._-]*$ ]] || { echo "not a lock path: $LOCK" >&2; exit 2; }
    sudo_box=${SUDO_BOX:?set SUDO_BOX=user@host, a user with sudo on the box} dir=${BOX_DIR:-relay-wan}
    commit=${COMMIT:-$(git rev-parse --short HEAD)$(git diff --quiet HEAD || echo -dirty)}
    stamp=$(date +%Y%m%d-%H%M)-$commit
    scripts/box.sh sync "$dir"
    scripts/box.sh run "$dir" cargo build --release -p axon-stream --features cluster --example relay_wan
    src=$(scripts/box.sh run "$dir" pwd)
    bin=relay-wan/bin-$stamp
    ssh "$sudo_box" "mkdir -p ~/relay-wan/runs ~/$bin && sudo install -o \$USER -m 755 $src-target/release/examples/relay_wan $src/scripts/relay-wan.sh ~/$bin/ && echo $commit >~/$bin/commit"
    home=$(ssh "$sudo_box" 'echo $HOME')   # sudo's ~ is root's
    run="env LOCK=${LOCK:-} bash $home/$bin/relay-wan.sh box $home/$bin/relay_wan $home/relay-wan/runs/$stamp $runs ${cells[*]}"
    ssh -fn "$sudo_box" "cd ~/relay-wan && nohup setsid sudo $run >~/relay-wan/runs/$stamp.log 2>&1 </dev/null &"
    echo "running on $sudo_box: ~/relay-wan/runs/$stamp.log"
    until state=$(ssh "$sudo_box" "test -e ~/relay-wan/runs/$stamp/done && echo done || { pgrep -f '[r]elay-wan.sh box .*$stamp' >/dev/null && echo \"running: \$(tail -n 1 ~/relay-wan/runs/$stamp.log)\"; }" || true); [[ $state == done ]]; do
        [[ $state == running:* ]] || { echo "the run on $sudo_box ended without its results" >&2; exit 1; }
        echo "$state"
        sleep 20
    done
    mkdir -p target/relay-wan
    rsync -r "$sudo_box:relay-wan/runs/$stamp/" target/relay-wan/$stamp/
    ssh "$sudo_box" "rm -r ~/$bin"   # this run's copy of the binary
    cat target/relay-wan/$stamp/meta.txt
    table target/relay-wan/$stamp/results.txt
    exit
fi

# The box, as root.
(( EUID == 0 )) || { echo "run as root" >&2; exit 2; }
bin=$2 out=$3 runs=$4
cells=("${@:5}")
user=$(stat -c %U "$bin")
mkdir -p "$out"
chown "$user" "$out"

# One of the box's build slots, as scripts/slot.sh takes them, then LOCK, the lock others' benchmarks on the box take
# (in that order, as `box.sh run <dir> flock LOCK ..` does, so neither waits on the other). LOCK is opened without
# O_CREAT, as fs.protected_regular refuses root that on another user's file in /tmp, and only if a plain file.
slot_user=$(stat -c %U /tmp/box-slots 2>/dev/null || true)
slots=$(cat "$(getent passwd "$slot_user" | cut -d: -f6)/.config/box-slots" 2>/dev/null || echo 0)
take_slot() {
    local s
    for s in $(seq "$slots"); do   # slot.sh makes a slot's file only once it reaches it: make it as its user
        [[ -e /tmp/box-slots/$s ]] || { touch "/tmp/box-slots/$s" && chown "$slot_user" "/tmp/box-slots/$s"; }
        exec 9<"/tmp/box-slots/$s"
        flock -n 9 && return 0
    done
    return 1
}
if (( slots > 0 )); then until take_slot; do sleep 5; done; fi
if [[ -n ${LOCK:-} ]]; then
    [[ -f $LOCK && ! -L $LOCK ]] || { echo "LOCK $LOCK is not a plain file" >&2; exit 2; }
    exec 8<"$LOCK"
    flock 8
fi
ns=(w1 w2)
for n in "${ns[@]}"; do [[ ! -e /run/netns/$n ]] || { echo "namespace $n exists: not ours to use" >&2; exit 2; }; done
! ip link show wan0 >/dev/null 2>&1 || { echo "bridge wan0 exists" >&2; exit 2; }

bbr_loaded=$(lsmod | grep -c '^tcp_bbr ' || true)
pids=()
teardown() {
    for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
    for n in "${ns[@]}"; do ip netns del "$n" 2>/dev/null || true; done   # its veth and its qdiscs with it
    ip link del wan0 2>/dev/null || true
    local i   # a namespace's sockets go some seconds after it, and hold BBR until then
    if (( bbr_loaded == 0 )); then
        for i in $(seq 180); do   # sockets of a lossy cell linger past 30 s
            lsmod | grep -q '^tcp_bbr ' || break
            modprobe -r tcp_bbr 2>/dev/null || sleep 1
        done
        ! lsmod | grep -q '^tcp_bbr ' || echo "tcp_bbr is still loaded: modprobe -r tcp_bbr" >&2
    fi
}
trap teardown EXIT

ip link add wan0 type bridge
ip link set wan0 up
for i in 1 2; do
    ip netns add w$i
    ip link add wan-w$i type veth peer name eth0 netns w$i
    ip link set wan-w$i master wan0 up
    ip -n w$i addr add 10.78.0.$i/24 dev eth0
    ip -n w$i link set eth0 up
    ip -n w$i link set lo up
done
inns() { nsenter --net=/run/netns/"$1" "${@:2}"; }

# Half the round trip and the link's rate on the way into each namespace. netem's default limit of 1000 packets
# would cap a link.
netem() {   # <rtt_ms> <loss_pct> <rate>
    local i offload=on
    [[ $2 == 0 ]] || offload=off   # a lost segment is one packet on a wire, not a 64 KiB GSO one
    for i in 1 2; do
        inns w$i ethtool -K eth0 tso $offload gso $offload >/dev/null
        tc qdisc replace dev wan-w$i root netem limit 1000000 delay $(( $1 * 500 ))us rate "$3" loss "$2"%
    done
}
tune() {   # <0|1>
    local n
    (( $1 == 0 )) || modprobe tcp_bbr
    for n in "${ns[@]}"; do
        if (( $1 )); then
            inns "$n" sysctl -qw net.ipv4.tcp_wmem="4096 16384 67108864" net.ipv4.tcp_rmem="4096 131072 134217728" \
                net.ipv4.tcp_congestion_control=bbr net.ipv4.tcp_slow_start_after_idle=0
            inns "$n" tc qdisc replace dev eth0 root fq
        else
            inns "$n" sysctl -qw net.ipv4.tcp_wmem="4096 16384 4194304" net.ipv4.tcp_rmem="4096 131072 6291456" \
                net.ipv4.tcp_congestion_control=cubic net.ipv4.tcp_slow_start_after_idle=1
            inns "$n" tc qdisc del dev eth0 root 2>/dev/null || true
        fi
    done
}
run_as() {   # <ns> <log> <cmd..>: in the background as the binary's owner, on the node's cores; pid in $!
    local i=${1#w}
    nsenter --net=/run/netns/"$1" setpriv --reuid "$user" --regid "$user" --init-groups taskset -c $((4 * i - 2))-$((4 * i + 1)) "${@:3}" >"$2" 2>&1 &
    pids+=($!)
}

{
    echo "commit $(cat "$(dirname "$bin")/commit" 2>/dev/null || echo unknown)"
    echo "kernel $(uname -r), $(lscpu | sed -n 's/^Model name: *//p'), governor $(sort -u /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor | tr '\n' ' ')"
    echo "host tcp_wmem $(sysctl -n net.ipv4.tcp_wmem | tr '\t' ' '), tcp_rmem $(sysctl -n net.ipv4.tcp_rmem | tr '\t' ' '), cc $(sysctl -n net.ipv4.tcp_congestion_control), limit_output_bytes $(sysctl -n net.ipv4.tcp_limit_output_bytes), $(ip -n w1 -o link show eth0 | grep -o 'mtu [0-9]*')"
    echo "start $(date -u +%FT%TZ), load $(cut -d' ' -f1-3 /proc/loadavg)"
} >"$out/meta.txt"
cat "$out/meta.txt"

for cell in "${cells[@]}"; do
    IFS=: read -r rtt loss tuned what rate <<<"$cell"
    netem "$rtt" "$loss" "${rate:-1gbit}"
    tune "$tuned"
    echo "$cell: $(inns w1 sysctl -n net.ipv4.tcp_wmem net.ipv4.tcp_rmem net.ipv4.tcp_congestion_control net.ipv4.tcp_slow_start_after_idle | tr '\n\t' '  ')$(inns w1 tc qdisc show dev eth0 | head -1), $(tc qdisc show dev wan-w2 | head -1)" >>"$out/meta.txt"
    n=$runs
    [[ $what != prof* ]] || n=1
    args=()
    if [[ $what != tcp ]]; then
        [[ $what =~ ^(axon|prof)([0-9]+)(s?)(x([0-9]+))?(b?)$ ]] || { echo "cell $cell: what is $what?" >&2; exit 2; }
        args=("${BASH_REMATCH[2]}" "${BASH_REMATCH[5]:-1}")
        [[ -z ${BASH_REMATCH[3]} ]] || args+=(shipped)
        [[ -z ${BASH_REMATCH[6]} ]] || args+=(bulk)
    fi
    for run in $(seq "$n"); do
        d=$out/$cell-$run
        mkdir -p "$d"
        chown "$user" "$d"
        load=$(cut -d' ' -f1 /proc/loadavg)
        echo "$(date +%T) $cell run $run, load $load"
        pids=()
        if [[ $what == tcp ]]; then
            run_as w2 "$d/recv.out" "$bin" tcp-recv 10.78.0.2:7400 $secs
            sleep 0.5
            run_as w1 "$d/send.out" "$bin" tcp-send 10.78.0.2:7400 $secs
            sleep 8
        else
            for w in "${ns[@]}"; do run_as "$w" "$d/$w.out" "$bin" node "$w" $secs "${args[0]}" "${args[1]}" "$d" "${args[@]:2}"; done
            for _ in $(seq 600); do   # 120 s: a node whose peer died waits for it for ever
                grep -q measuring "$d/w1.out" 2>/dev/null || ! kill -0 "${pids[0]}" 2>/dev/null && break
                sleep 0.2
            done
            sleep 4
        fi
        inns w1 ss -tin dst 10.78.0.2 >"$d/ss.txt" || true
        if [[ $what == prof* ]]; then
            perf record -q -F 499 -o "$d/w1.perf" -p "${pids[0]}" -- sleep 5 2>/dev/null &
            perf record -q -F 499 -o "$d/w2.perf" -p "${pids[1]}" -- sleep 5 2>/dev/null || true
            wait $! || true
        fi
        if [[ $what != tcp ]]; then   # every node stays up until all have printed
            for _ in $(seq 600); do
                (( $(cat "$d"/w?.out 2>/dev/null | grep -c '^node=') == ${#ns[@]} )) && break
                sleep 0.2
            done
            touch "$d/stop"
        fi
        for p in "${pids[@]}"; do timeout 120 tail --pid="$p" -f /dev/null || kill "$p" 2>/dev/null || true; done
        for f in "$d"/recv.out "$d"/w?.out; do
            [[ -e $f ]] || continue
            grep -E '^(node=|tcp )' "$f" | sed "s/^/cell=$cell run=$run load=$load /" >>"$out/results.txt" || echo "$cell $run: no result in $f" >&2
        done
        if [[ $what == prof* ]]; then
            for w in w1 w2; do
                perf report -i "$d/$w.perf" --stdio --no-children --sort sym --percent-limit 1 2>/dev/null |
                    grep -Ev '^(#|$)' | head -40 >"$d/$w.top.txt"
                perf report -i "$d/$w.perf" --stdio --no-children --sort comm --percent-limit 2 2>/dev/null |
                    grep -Ev '^(#|$)' | head -20 >"$d/$w.threads.txt"
            done
        fi
    done
done
echo "end $(date -u +%FT%TZ), load $(cut -d' ' -f1-3 /proc/loadavg)" >>"$out/meta.txt"
chown -R "$user" "$out"
touch "$out/done"
