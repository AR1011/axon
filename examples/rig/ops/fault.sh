#!/usr/bin/env bash
# One node's faults, as root (spec/fault-rig.md § Fault classes). Network rules sit in chain RIG, which only TCP to or
# from ports 7400 and 7001 off loopback reaches, and name the rig's addresses; never 22 or the store's 7900. tc filters
# the same ports to the peers into band 4 of a prio qdisc, where netem sits. Disk faults swap the table of a dm disk,
# rig-journal or rig-streams; cgroup faults set runtime properties of the unit rig-node. Inside network namespace bN
# (one box, setup.sh --netns) these names carry bN (node.sh), and the clock classes are refused: the clock is the box's.
#
#   fault.sh [-i <id>] [-d <disk>] on|off <class> [args]
#       kill <hold_s> | pause | freeze | leave | isolate | partition <ip,..> | bridge <ip,..> | oneway <ip,..>
#       | netem <delay_ms> <jitter_ms> <loss%> <reorder%> <dup%> | rate <mbit> | cpu <quota%> | oom <% of anon memory>
#       | memhigh <%> | io <iops> | slowdisk <ms> | diskerr | diskdrop | diskfull | step <s> | drift <tick delta>
#       | sysctl <key> <value> | toggle <period_ms> <class> [args] | upgrade | cfgdrift <LEASE_SECS|HEARTBEAT_MS> <value>
#       | scale | powercut | diskhang | rofs | flaky <up_s> <down_s> | splitdelay <read_ms> <write_ms> | readerr
#       | diskdead | diskcorrupt | inodes   (the disk classes of spec/fault-rig.md § Fault classes, each on -d's disk)
#       | authz <id,..>: the node's echo workload refuses those nodes' calls and spawns until off (src/echo.rs)
#       | reject <tcp-reset|icmp-port-unreachable|icmp-host-unreachable> <ip,..> | syndrop <reset 0|1> <ip,..>
#       | halfopen | portcut <port> <ip,..> | graylink <loss%> <ip,..> | flap <up_ms> <down_ms> <ip|all> | mtu <bytes>
#       | asym <delay_ms> <ip,..> | dup <%> | corrupt <%> | conntrack | backlog [somaxconn] | ports <n>
#       | crashloop <period_ms> | killterm <kill_after_ms> <terms> | tasksmax <headroom> | nofile <headroom>
#       | threadstarve <busy %> [thread name prefix,..] | reclaim <mb> | hook <name> <ms> | coredump | swap <%>
#       | thp always|never | leap | peer <mode>
#       | geo <ip>=<ms>.. [rate=<mbit>] [loss=<%>] [ip,..]: each ip's packets delayed by its ms (nemesis.py's geo_ms,
#         spec/wan.md § 8), at most rate to each, loss to the ips listed or to all; under a backdrop its delays stay,
#         and only a rate or loss changes, back at off | bulk: the node's bulk workload uploads until off (src/bulk.rs)
#       ; off takes on's args
#     -i names the fault (default its class) so faults compose on one node (spec § Fault classes, Composing); -d picks
#     the disk of a disk class: journal (the default) or streams. A class acting on a unit that is not running prints
#     `noop inactive` and exits 0.
#   fault.sh check                   prints clean, or dirty and what; exit 1 when dirty
#   fault.sh heal                    undoes every fault, then check
#   fault.sh stop                    stops the node and marks it not meant to run, clears the backdrop, then heal
#   fault.sh backdrop on <ip>=<ms>.. TIMING=wan's geo delays for the whole run, on before the node starts: heal puts
#                                    them back, check counts them clean | backdrop off: gone
set -euo pipefail
rig=/home/ubuntu/rig
. $rig/node.sh
PATH=$PATH:/usr/local/sbin
id= disk=journal
while getopts i:d: o; do case $o in i) id=$OPTARG ;; d) disk=$OPTARG ;; *) exit 2 ;; esac; done
shift $((OPTIND - 1))
cmd=${1:-} class=${2:-}
id=${id:-$class}
[[ $id =~ ^[A-Za-z0-9_.:-]*$ ]] || { echo "a fault id is letters, digits and _.:- only: $id" >&2; exit 2; }
loops=rig-fault${n:+-$n}   # transient units of this node's running faults
mkdir -p $state/own $state/hold $state/clock.d $state/sysctl
if [[ -z ${RIG_FAULT_LOCKED:-} ]]; then   # one fault.sh at a time on a node, for its ledgers; toggle's off calls itself
    exec 8>>$state/lock
    flock -w 60 8 || { echo "another fault.sh on this node held its lock for 60 s" >&2; exit 1; }
    export RIG_FAULT_LOCKED=1
fi

own=" $(hostname -I) " peers=()
while read -r ip; do [[ $own == *" $ip "* ]] || peers+=("$ip"); done <$rig/nodes
dev=$(ip route get "${peers[0]}" | grep -oP ' dev \K\S+')
up() { [[ -e $up ]]; }   # run.sh marks a node that should be running
prop() { systemctl set-property --runtime $unit "$@"; }
# Of the process's own memory: memory.current also holds the journal's page cache, which a limit only reclaims.
anon_pct() { echo $(( $(awk '$1 == "anon" {print $2}' $cg/memory.stat) * $1 / 100 )); }
kill9() { live systemctl kill --kill-whom=main -s KILL $unit; }
# A transient unit heal stops (KillMode control-group, the default, takes its children with it).
loop() { systemd-run --quiet --collect --unit "$loops-$id.service" ${n:+-p NetworkNamespacePath=/run/netns/$n} "$@"; }
unloop() { systemctl stop "$loops-$id.service"; }
live() { "$@" && return; systemctl is-active -q $unit && return 1; echo "noop inactive"; }
secs() { awk -v ms="$1" 'BEGIN { print ms / 1000 }'; }

claim() {   # a resource of one fault at a time: a second fault's on fails, so a plan that stacks two shows
    local f=$state/own/$1
    if [[ -e $f && $(<"$f") != "$id" ]]; then echo "$1 is held by fault $(<"$f")" >&2; exit 3; fi
    echo "$id" >"$f"
}
release() {   # fails, printing noop, when another fault holds it: this off then undoes nothing
    local f=$state/own/$1
    if [[ -e $f && $(<"$f") != "$id" ]]; then echo "noop $1 held by fault $(<"$f")"; return 1; fi
    rm -f "$f"
}
runs() { up && [[ -z $(ls $state/hold) ]]; }   # the unit should run now: meant to, and no fault holds it down
listening() {   # the node's cluster port, bound once it is back: a roll waits for it before the next node
    local i
    for i in $(seq 60); do [[ -n $(ss -Hltn 'sport = :7400') ]] && return; sleep 0.5; done
    echo "nothing listens on 7400 30 s after the restart" >&2; return 1
}
hold() { touch $state/hold/"$id"; }
start_unheld() { if runs; then systemctl start $unit; fi; }
unhold() {   # the unit starts again once no fault holds it down
    rm -f $state/hold/"$id"
    local held; held=$(ls $state/hold)
    if [[ -n $held ]]; then echo "held by ${held//$'\n'/ }"; else systemctl start $unit; fi
}

tag() { echo "-m comment --comment rig:$id"; }
drop() { local ip; for ip in "$@"; do iptables -A RIG -s "$ip" $(tag) -j DROP; iptables -A RIG -d "$ip" $(tag) -j DROP; done; }
peerwise() {   # <ip,..> <target> [match..]: a rule of this fault to and from each address, the target after its tag
    local ip d
    for ip in ${1//,/ }; do for d in -s -d; do iptables -A RIG -p tcp $d "$ip" "${@:3}" $(tag) -j $2; done; done
}
undrop() {   # this fault's rules only; -S quotes the comment, and an id never needs it
    iptables -S RIG | tr -d '"' | { grep -F -- "--comment rig:$id -j" || true; } | sed 's/^-A /-D /' |
        while read -r r; do iptables $r; done
}

sockkill() {   # one-shot: the rig ports' sockets to these addresses aborted, an RST to the far end, so they redial
    local f= ip
    for ip; do f+="${f:+ or }dst $ip"; done
    ss -K -t "( sport = :7400 or sport = :7001 or dport = :7400 or dport = :7001 ) and ( $f )" >/dev/null
}

band() {   # ip band: the rig ports' packets to ip into that band of the root prio
    local p d
    for p in 7400 7001; do for d in sport dport; do
        tc filter add dev "$dev" parent 1: protocol ip prio 1 u32 match ip dst "$1"/32 match ip $d $p 0xffff flowid 1:$2
    done; done
}
band4() {
    tc qdisc add dev "$dev" root handle 1: prio bands 4
    local ip
    for ip in "${peers[@]}"; do band "$ip" 4; done
    tc qdisc add dev "$dev" parent 1:4 handle 40: netem "$@"
}
geo() {   # [change] ip=ms..: a band per ip=ms, each its own netem; change sets the bands a backdrop made, rate 0 none
    local verb=add rate= loss= to= a l b=4 ms=()
    [[ ${1:-} != change ]] || { verb=change rate=0; shift; }
    for a; do case $a in rate=*) rate=${a#rate=} ;; loss=*) loss=${a#loss=} ;; *=*) ms+=("$a") ;; *) to=,$a, ;; esac; done
    [[ $verb == change ]] || tc qdisc add dev "$dev" root handle 1: prio bands $((3 + ${#ms[@]}))
    for a in "${ms[@]}"; do
        [[ $verb == change ]] || band "${a%=*}" $b
        l=; [[ -z $loss || -n $to && $to != *",${a%=*},"* ]] || l="loss $loss%"
        tc qdisc $verb dev "$dev" parent 1:$b handle $((b * 10)): netem delay "${a#*=}"ms limit 100000 ${rate:+rate "$rate"mbit} $l
        b=$((b + 1))
    done
}
ours_qdisc() { tc qdisc show dev "$dev" | grep -q '^qdisc prio 1: root'; }
backdrop=$state/backdrop   # its geo arguments, a line each
# A geo on that changes no rate or loss: under the backdrop the sessions keep their round trips (spec/wan.md § 12).
reshaped() { [[ " $* " == *" rate="* || " $* " == *" loss="* ]]; }

use() {   # disk: the one the disk commands below work on; streams exists once setup made it
    case $1 in journal | streams) ;; *) echo "no disk $1: journal or streams" >&2; exit 2 ;; esac
    device "$1"
    dmsetup info $dm >/dev/null 2>&1 || { echo "no disk $1 on this node: setup has not made dm $dm" >&2; exit 2; }
    disk=$1 loop=$(losetup -j $img | cut -d: -f1 | head -1) filler=$mnt/.rig-filler
}
disks() { local d; for d in journal streams; do device $d; ! dmsetup info $dm >/dev/null 2>&1 || echo $d; done; }
# All of it the node's again: after an e2fsck its writes failed with EACCES (chaos-20261001-1931-s1001).
mount_disk() {
    mount -o errors=continue /dev/mapper/$dm $mnt || return
    chown -R ubuntu:ubuntu $mnt 2>/dev/null || true; chmod -R u+rwX $mnt 2>/dev/null || true
}
suspended() { [[ $(dmsetup info $dm) == *SUSPENDED* ]]; }
# ext4 of kernel 6.8 logs "Remounting filesystem read-only" on an error but keeps the mount rw and fails writes with
# EIO; its superblock's error count says so until e2fsck clears it. A mount rofs left at errors=remount-ro needs the
# reset too.
ext4() { echo /sys/fs/ext4/"$(basename "$(readlink -f /dev/mapper/$dm)")"/$1; }
errored() {
    local o; o=$(findmnt -no OPTIONS $mnt || true)
    [[ $o == ro* || $o == *errors=remount-ro* ]] || (( $(cat "$(ext4 errors_count)" 2>/dev/null || echo 0) ))
}

table() {   # load first, so a bad table never leaves the device suspended; error takes no device
    local t="$1 $loop 0${2:+ $2}"; [[ $1 != error ]] || t=error
    dmsetup load $dm --table "0 $(blockdev --getsz "$loop") $t"
    dmsetup suspend --nolockfs $dm
    dmsetup resume $dm
}
table_kind() { dmsetup table $dm | cut -d' ' -f3; }

reset() {   # after a device that failed or lied: fsck, else keep what reads and mkfs
    systemctl kill -s KILL $unit 2>/dev/null || true
    systemctl stop $unit
    ! mountpoint -q $mnt || umount $mnt   # a powercut unmounted it
    blockdev --flushbufs /dev/mapper/$dm   # what the device's cache holds of writes a lying disk dropped
    local rc=0
    e2fsck -fy /dev/mapper/$dm >/dev/null 2>&1 || rc=$?
    if (( rc < 4 )) && mount_disk; then
        echo "$disk-reset fsck $rc"
    else
        local run; run=$(sed -n 's/^RUN=//p' $env 2>/dev/null || true)
        local a=$rig/runs/${run:-norun}/$disk-archive${n:+-$n}/$(date +%s)   # one box: every node shares runs/
        mkdir -p "$a"
        if mount -o ro,noload /dev/mapper/$dm $mnt; then cp -r $mnt/. "$a"/ || true; umount $mnt; fi
        chown -R ubuntu:ubuntu "$rig/runs/${run:-norun}"   # not every run: on one box runs/ holds them all
        mkfs.ext4 -q -F -m 0 /dev/mapper/$dm
        mount_disk
        echo "$disk-reset mkfs $a"
    fi
    start_unheld
}

# Clock faults keep realtime - CLOCK_MONOTONIC_RAW from before the first one, and put realtime back from it when the
# last one goes off: that undoes steps and drifts alike, without asking another machine the time. An off while
# another clock fault stays on undoes only its own part.
clock_on() {
    [[ -e $state/clock ]] ||
        python3 -c 'import time; print(time.clock_gettime_ns(time.CLOCK_REALTIME) - time.clock_gettime_ns(time.CLOCK_MONOTONIC_RAW))' >$state/clock
    touch $state/clock.d/"$id"
    systemctl stop systemd-timesyncd
}
clock_off() {   # [command that undoes this fault alone]; heal has no id and has cleared clock.d
    [[ -z $id ]] || rm -f $state/clock.d/"$id"
    if [[ -n $(ls $state/clock.d) ]]; then "$@"; return; fi
    adjtimex --tick 10000
    if [[ -e $state/clock ]]; then
        python3 -c 'import sys, time; time.clock_settime_ns(time.CLOCK_REALTIME, int(sys.argv[1]) + time.clock_gettime_ns(time.CLOCK_MONOTONIC_RAW))' "$(<$state/clock)"
        rm $state/clock
    fi
    systemctl start systemd-timesyncd
}
step() { python3 -c 'import sys, time; time.clock_settime_ns(time.CLOCK_REALTIME, time.clock_gettime_ns(time.CLOCK_REALTIME) + round(float(sys.argv[1]) * 1e9))' "$1"; }

unsysctl() { sysctl -qw "$1=$(<$state/sysctl/"$1")"; rm $state/sysctl/"$1"; }

# The process lane's classes (spec/fault-rig.md § Fault classes).
pid() { systemctl show -p MainPID --value $unit; }
running() { p=$(pid); [[ $p != 0 ]] && return; echo "noop inactive"; return 1; }
ctl() {   # what the rig binary polls for its hooks: LOG_DIR/ctl of the node's run (src/hooks.rs)
    local d; d=$(sed -n 's/^LOG_DIR=//p' $env 2>/dev/null || true)
    [[ -n $d && -d $d ]] || d=$state
    echo "$d/ctl"
}
nofile_back() {   # the unit's own soft limit, which a restart gives it anyway
    local p; p=$(pid)
    [[ $p == 0 ]] || prlimit --pid "$p" --nofile="$(systemctl show -p LimitNOFILESoft --value $unit):" 2>/dev/null || true
}
unpin() {   # each pinned thread that still runs back to its cores, and each it started meanwhile to the process's
    [[ -e $state/affinity ]] || return 0
    python3 - "$(pid)" $state/affinity <<'EOF'
import os, sys
now, (head, *rows) = sys.argv[1], open(sys.argv[2]).read().splitlines()
pid, core = head.split()
pinned = set()
for row in rows:
    tid, cpus = row.split()
    pinned.add(tid)
    try:
        os.sched_setaffinity(int(tid), map(int, cpus.split(",")))
    except OSError:  # the thread ended
        pass
if now == pid:  # a thread inherits its creator's cores: the job pool's new threads took the one core
    try:
        back = os.sched_getaffinity(int(pid))
        for tid in set(os.listdir("/proc/%s/task" % pid)) - pinned:
            try:
                if os.sched_getaffinity(int(tid)) == {int(core)}:
                    os.sched_setaffinity(int(tid), back)
            except OSError:
                pass
    except OSError:  # the process ended
        pass
EOF
    rm $state/affinity
}
tasks_back() { prop TasksMax="$(<$state/tasksmax)"; rm $state/tasksmax; }
thp=/sys/kernel/mm/transparent_hugepage
thp_back() { local e d; { read -r e; read -r d; } <$state/thp; echo "$e" >$thp/enabled; echo "$d" >$thp/defrag; rm $state/thp; }
swapfile=/var/lib/rig-swap
swapped() { swapon --show=NAME --noheadings | grep -qx $swapfile; }
leap_back() { local s st; read -r s st <$state/leap; adjtimex --status "$st"; rm $state/leap; echo "$s"; }

check() {
    local d=() t i p
    [[ $(iptables -S RIG | wc -l) == 1 ]] || d+=(rules)
    if [[ -e $backdrop ]]; then   # the backdrop's bands, and no rate or loss of a geo left on them
        ours_qdisc && ! tc qdisc show dev "$dev" | grep -qE 'netem .*(rate|loss) ' || d+=(qdisc)
    else ! ours_qdisc || d+=(qdisc); fi
    [[ $(systemctl show -p CPUQuotaPerSecUSec --value $unit) == infinity ]] || d+=(cpu)
    [[ $(systemctl show -p MemoryMax --value $unit) == infinity ]] || d+=(memory.max)
    [[ $(systemctl show -p MemoryHigh --value $unit) == infinity ]] || d+=(memory.high)
    [[ -z $(systemctl show -p IOWriteIOPSMax --value $unit) ]] || d+=(io)
    if [[ -d $cg ]]; then
        [[ $(systemctl show -p FreezerState --value $unit) == running ]] || d+=(frozen)
        for t in $(cat $cg/cgroup.threads 2>/dev/null); do
            grep -qs $'^State:\tT' /proc/"$t"/status && { d+=(stopped); break; }
        done
    fi
    if up; then   # a restart after kill, oom or a reset takes a second
        for i in 1 2 3 4 5 6 7 8 9 10; do systemctl is-active -q $unit && break; sleep 0.5; done
        systemctl is-active -q $unit || d+=(inactive)
    fi
    for t in $(disks); do
        use $t
        p=${t#journal} p=${p:+$p-}
        # A read of a suspended device's mount blocks, under the lock heal needs to resume it.
        ! suspended || { d+=("${p}suspended"); continue; }
        [[ $(table_kind) == linear ]] || d+=("${p}table")
        mountpoint -q $mnt || d+=("${p}unmounted")
        ! compgen -G "$filler*" >/dev/null || d+=("${p}filler")
        ! errored || d+=("${p}errored")
    done
    p=$(sed -n 's/^STREAM_DISK=//p' $env 2>/dev/null || true)   # the node writes all of it
    [[ -z $p || ! -e $p || -z $(find "$p" ! -user ubuntu -o ! -perm -u+w 2>/dev/null | head -1) ]] || d+=(streams-owner)
    [[ -z $(systemctl list-units --all --plain --no-legend "$loops-*") ]] || d+=(loop)
    ! compgen -G "$state/sysctl/*" >/dev/null || d+=(sysctl)
    [[ ! -e $override ]] || d+=(override)
    ! compgen -G "$state/own/*" >/dev/null && ! compgen -G "$state/hold/*" >/dev/null || d+=(ledger)
    for t in tasksmax affinity thp leap; do [[ ! -e $state/$t ]] || d+=("$t"); done
    [[ ! -e $(ctl) ]] || d+=(hook)
    p=$(pid) i=
    [[ $p == 0 ]] || i=$(prlimit --pid "$p" --nofile -o SOFT --noheadings 2>/dev/null | tr -d ' ' || true)   # empty: it ended
    [[ -z $i || $i == "$(systemctl show -p LimitNOFILESoft --value $unit)" ]] || d+=(nofile)
    ! swapped || d+=(swap)
    ! compgen -G "/var/tmp/rig-core.*" >/dev/null || d+=(core)
    [[ ! -e $state/clock ]] || d+=(clock)
    [[ ! -e $state/authz ]] || d+=(authz)
    [[ ! -e $state/bulk ]] || d+=(bulk)
    [[ $(adjtimex -p | awk '$1 == "tick:" {print $2}') == 10000 ]] || d+=(tick)
    systemctl is-active -q systemd-timesyncd || d+=(timesyncd)
    if (( ${#d[@]} )); then echo "dirty ${d[*]}"; return 1; fi
    echo clean
}

# Before anything that touches a suspended disk's file system, or stops a process blocked on it: either would hang.
unsuspend() { local t; for t in $(disks); do use $t; ! suspended || dmsetup resume $dm; done; }

heal() {
    local t f
    unsuspend
    systemctl stop "$loops-*" 2>/dev/null || true
    rm -f $state/own/* $state/hold/* $state/clock.d/* $state/authz $state/bulk
    iptables -F RIG
    # The backdrop's bands set back in place: a moment without them gives BBR a round trip of microseconds, which
    # it keeps for 10 s, a stall at 190 ms (scenarios-20261027-0626-s1).
    if ! { [[ -e $backdrop ]] && ours_qdisc && geo change $(<$backdrop) 2>/dev/null; }; then
        if ours_qdisc; then tc qdisc del dev "$dev" root; fi
        [[ ! -e $backdrop ]] || geo $(<$backdrop)
    fi
    prop CPUQuota= MemoryMax=infinity MemoryHigh=infinity IOWriteIOPSMax=
    [[ ! -e $state/tasksmax ]] || tasks_back
    unpin
    nofile_back
    rm -f "$(ctl)" /var/tmp/rig-core.*
    [[ ! -e $state/thp ]] || thp_back
    [[ ! -e $state/leap ]] || leap_back >/dev/null
    if swapped; then prop MemorySwapMax=0; swapoff $swapfile; fi
    systemctl thaw $unit 2>/dev/null || true
    systemctl kill -s CONT $unit 2>/dev/null || true
    local relisten=; [[ ! -e $state/sysctl/net.core.somaxconn ]] || relisten=1   # backlog's: a listen() keeps it
    for f in $state/sysctl/*; do [[ ! -e $f ]] || unsysctl "${f##*/}"; done
    [[ -z $relisten ]] || systemctl kill --kill-whom=main -s KILL $unit 2>/dev/null || true
    for t in $(disks); do
        use $t
        rm -rf "$filler"* || true   # on a disk that fails its IO: the table is put back below, and check sees what is left
        case $(table_kind) in
            linear) if errored; then reset; else mountpoint -q $mnt || mount_disk; fi ;;
            delay) table linear ;;
            *) blockdev --flushbufs /dev/mapper/$dm 2>/dev/null || true; table linear; reset ;;
        esac
    done
    clock_off true
    if [[ -e $override ]]; then rm -f $override; systemctl try-restart $unit; fi   # back to the run's settings
    if up; then systemctl start $unit; fi
    check
}

case $class in io | slowdisk | diskerr | diskdrop | diskfull | powercut | diskhang | rofs | flaky | splitdelay | readerr \
    | diskdead | diskcorrupt | inodes) use "$disk" ;; esac
case $cmd/$class in on/step | on/drift | on/leap | on/coredump | on/swap | on/thp)
    [[ -z $n ]] || { echo "$class is box-wide in a network namespace" >&2; exit 2; } ;; esac
case $cmd/$class in
    check/) check ;;
    heal/) heal ;;
    stop/) rm -f $up $backdrop; unsuspend; systemctl stop $unit; heal ;;
    backdrop/on) [[ $# -gt 2 ]] || { echo "backdrop on takes geo's ip=ms" >&2; exit 2; }
        if ours_qdisc; then tc qdisc del dev "$dev" root; fi
        printf '%s\n' "${@:3}" >$backdrop; geo "${@:3}" ;;
    backdrop/off) rm -f $backdrop; if ours_qdisc; then tc qdisc del dev "$dev" root; fi ;;
    # The main process only: signalling the rest of the cgroup fails for the rig binary ("auxiliary processes:
    # Invalid argument"), and under set -e that skipped the stop, so Restart=always ended every hold after 1 s.
    on/kill) if [[ ${3:-0} != 0 ]]; then hold; fi
        kill9
        if [[ ${3:-0} != 0 ]]; then systemctl stop $unit; fi ;;
    off/kill) unhold ;;
    on/pause) live systemctl kill -s STOP $unit ;;
    off/pause) live systemctl kill -s CONT $unit ;;
    on/freeze) live systemctl freeze $unit ;;
    off/freeze) live systemctl thaw $unit ;;
    on/leave | on/scale) hold; systemctl stop $unit ;;   # scale as a leave held for minutes: the cluster shrinks, then grows back
    off/leave | off/scale) unhold ;;
    on/isolate) drop "${peers[@]}" ;;
    on/partition | on/bridge) drop ${3//,/ } ;;
    on/oneway) for ip in ${3//,/ }; do iptables -A RIG -d "$ip" $(tag) -j DROP; done ;;
    off/isolate | off/partition | off/bridge | off/oneway | off/reject | off/syndrop | off/halfopen | off/portcut \
        | off/graylink | off/mtu) undrop ;;
    on/netem) claim qdisc; band4 delay "$3"ms "$4"ms distribution normal loss "$5"% reorder "$6"% duplicate "$7"% ;;
    on/rate) claim qdisc; band4 rate "$3"mbit ;;
    on/geo) claim qdisc
        if [[ ! -e $backdrop ]]; then geo "${@:3}"; elif reshaped "${@:3}"; then geo change "${@:3}"; fi ;;
    off/geo) if release qdisc; then
            if [[ ! -e $backdrop ]]; then tc qdisc del dev "$dev" root; elif reshaped "${@:3}"; then geo change $(<$backdrop); fi
        fi ;;
    off/netem | off/rate | off/asym | off/dup | off/corrupt) if release qdisc; then tc qdisc del dev "$dev" root; fi ;;
    on/cpu) claim cpu; prop CPUQuota="$3"% ;;
    off/cpu) if release cpu; then prop CPUQuota=; fi ;;
    on/oom) claim memory.max; prop MemoryMax=$(anon_pct "$3") ;;
    off/oom) if release memory.max; then prop MemoryMax=infinity; fi ;;
    on/memhigh) claim memory.high; prop MemoryHigh=$(anon_pct "$3") ;;
    off/memhigh) if release memory.high; then prop MemoryHigh=infinity; fi ;;
    on/io) claim io:$disk; prop "IOWriteIOPSMax=/dev/mapper/$dm $3" ;;
    off/io) if release io:$disk; then prop "IOWriteIOPSMax=/dev/mapper/$dm infinity"; fi ;;
    on/slowdisk) claim table:$disk; table delay "$3" ;;
    off/slowdisk | off/splitdelay) if release table:$disk; then table linear; fi ;;
    on/diskerr) claim table:$disk; table flakey "0 1 1 error_writes" ;;
    on/diskdrop) claim table:$disk; table flakey "0 1 1 drop_writes" ;;
    on/diskfull)   # fallocate cannot take the last blocks its own extents need; dd takes them
        claim filler:$disk
        avail=$(df --output=avail -B1 $mnt | tail -1)
        fallocate -l $(( avail > 64 << 20 ? avail - (32 << 20) : 0 )) "$filler" 2>/dev/null || true
        dd if=/dev/zero of="$filler.tail" bs=64k status=none 2>/dev/null || true
        # Written back, delayed allocation returns the blocks it held for metadata: direct writes take those too.
        sync -f $mnt
        dd if=/dev/zero of="$filler.top" bs=4k oflag=direct status=none 2>/dev/null || true ;;
    # A power cut: frozen first, so nothing the process does reaches the disk after the table drops writes; then what
    # the page cache still holds is written into the void, and the device's own cache dropped before fsck reads it.
    on/powercut) claim table:$disk; hold
        live systemctl freeze $unit
        table flakey "0 1 1 drop_writes"
        kill9
        systemctl stop $unit
        sync -f $mnt
        umount $mnt ;;
    off/powercut) if release table:$disk; then table linear; reset; fi; unhold ;;
    # Every IO of the disk queued until the off; a restart beside it would wait on the process, which cannot die.
    on/diskhang) claim table:$disk; dmsetup suspend --nolockfs $dm ;;
    off/diskhang) if release table:$disk; then dmsetup resume $dm; fi ;;
    # A plain remount,ro is refused while the node holds files open for writing; an error stops writes at once (EIO on 6.8).
    on/rofs) claim table:$disk
        mount -o remount,errors=remount-ro $mnt
        echo 1 >"$(ext4 trigger_fs_error)" ;;
    off/rofs) if release table:$disk; then reset; fi ;;
    on/flaky) claim table:$disk; table flakey "$3 $4 1 error_writes" ;;
    on/splitdelay) claim table:$disk; table delay "$3 $loop 0 $4" ;;
    # Reads fail, for what the node reads back when it starts: its pages dropped first (a dd per file, so not the empty
    # ones of an inodes filler; find fails on a segment retention removed meanwhile), then its process killed.
    on/readerr) claim table:$disk
        sync -f $mnt
        find $mnt -xdev -type f ! -empty -exec dd if={} iflag=nocache count=0 status=none \; || true
        table flakey "0 1 1 error_reads"
        kill9 ;;
    on/diskdead) claim table:$disk; table error ;;
    on/diskcorrupt) claim table:$disk; table flakey "0 1 5 corrupt_bio_byte 32 w 1 0" ;;
    off/diskerr | off/diskdrop | off/flaky | off/readerr | off/diskdead | off/diskcorrupt) if release table:$disk; then table linear; reset; fi ;;
    on/inodes)   # every inode taken by empty files; a segment the node starts then cannot be created
        mountpoint -q $mnt || { echo "$mnt is not mounted" >&2; exit 3; }
        claim filler:$disk
        python3 -c 'import os, sys
os.makedirs(sys.argv[1], exist_ok=True)
i = 0
try:
    while True:
        open(os.path.join(sys.argv[1], str(i)), "x").close()
        i += 1
except OSError:
    pass' "$filler-inodes" ;;
    off/diskfull | off/inodes) if release filler:$disk; then rm -rf "$filler"*; fi ;;
    on/step) clock_on; step "$3" ;;
    on/drift) claim tick; clock_on; adjtimex --tick $(( 10000 + $3 )) ;;
    off/step) clock_off step "$(awk -v s="$3" 'BEGIN { print -s }')" ;;
    off/drift) if release tick; then clock_off adjtimex --tick 10000; fi ;;
    on/sysctl)   # its first value is kept for the off and for heal
        [[ -z $n || $3 == net.* ]] || { echo "sysctl $3 is box-wide in a network namespace" >&2; exit 2; }
        claim sysctl:"$3"
        [[ -e $state/sysctl/$3 ]] || sysctl -n "$3" >$state/sysctl/"$3"
        sysctl -qw "$3=$4" ;;
    off/sysctl) if release sysctl:"$3"; then unsysctl "$3"; fi ;;
    on/authz) claim authz; echo "$3" >$state/authz ;;
    off/authz) if release authz; then rm -f $state/authz; fi ;;
    on/bulk) claim bulk; touch $state/bulk ;;
    off/bulk) if release bulk; then rm -f $state/bulk; fi ;;
    on/toggle)   # the fault the rest names goes on and off every period, in a transient unit that heal stops
        (( $3 >= 100 )) || { echo "toggle: a period of 100 ms or more" >&2; exit 2; }
        loop bash -c 'while :; do "$0" -i "$2" -d "$3" on "${@:4}"; sleep "$1"; "$0" -i "$2" -d "$3" off "${@:4}"; sleep "$1"; done' \
            $rig/fault.sh "$(secs "$3")" "$id" "$disk" "${@:4}" ;;
    off/toggle) unloop; $rig/fault.sh -i "$id" -d "$disk" off "${@:4}" ;;
    on/upgrade)   # onto this node's other build, rig <-> rig-v2, by a clean restart, and until it listens again
        to=$([[ $(readlink $exe) == rig-v2 ]] && echo rig || echo rig-v2)
        [[ -x $rig/$to ]] || { echo "no $rig/$to: scripts/rig.sh build" >&2; exit 2; }
        ln -sfn $to $exe
        chown -h ubuntu:ubuntu $exe
        echo "build $to"
        if runs; then systemctl restart $unit; listening; else echo "noop inactive"; fi ;;
    off/upgrade) echo "build $(readlink $exe)" ;;   # a roll stays: run.sh points the next phase at rig
    on/cfgdrift)   # this node alone restarts with another value of a setting
        [[ $3 == LEASE_SECS && $4 =~ ^[0-9]+(\.[0-9]+)?$ && $4 =~ [1-9] || $3 == HEARTBEAT_MS && $4 =~ ^[1-9][0-9]*$ ]] ||
            { echo "cfgdrift LEASE_SECS <s> | HEARTBEAT_MS <whole ms>, above 0" >&2; exit 2; }
        claim override
        echo "$3=$4" >$override
        if runs; then systemctl restart $unit; else echo "noop inactive"; fi ;;
    off/cfgdrift) if release override; then rm -f $override; ! runs || systemctl restart $unit; fi ;;
    on/reject)   # an answer instead of silence: an RST, or an ICMP unreachable, both ways; RSTs pass, the answers too
        peerwise "$4" "REJECT --reject-with $3" ! --tcp-flags RST RST ;;
    on/syndrop)   # new connections only; with reset 1, the sessions up are aborted too, so they redial into it
        peerwise "$4" DROP --syn
        if [[ $3 == 1 ]]; then sockkill ${4//,/ }; fi ;;
    on/halfopen)   # no FIN or RST in or out, then a crash: the peers keep sessions to a process that is gone
        for fl in FIN RST; do iptables -A RIG -p tcp --tcp-flags $fl $fl $(tag) -j DROP; done
        kill9 ;;
    on/portcut)   # one port to these addresses, both ways and either end's: membership and data path apart
        peerwise "$4" DROP --sport "$3"; peerwise "$4" DROP --dport "$3" ;;
    on/graylink)   # a link that loses loss% of its packets each way, neither up nor down
        peerwise "$4" DROP -m statistic --mode random --probability "$(awk -v l="$3" 'BEGIN { print l / 100 }')" ;;
    on/flap)   # the link to ip (its packets in dropped stalls both ways), or all, down and up in a loop that adds and
        # deletes its one rule: two iptables calls a cycle, at most 5 a second
        (( $3 + $4 >= 400 )) || { echo "flap: up and down 400 ms or more together" >&2; exit 2; }
        [[ $5 == all || $5 =~ ^[0-9.]+$ ]] || { echo "flap: one address or all" >&2; exit 2; }
        loop bash -c 'r="RIG $1 -m comment --comment rig:$2 -j DROP"; while :; do iptables -w -A $r; sleep "$3"; iptables -w -D $r; sleep "$4"; done' \
            flap "$([[ $5 == all ]] || echo "-s $5")" "$id" "$(secs "$4")" "$(secs "$3")" ;;
    off/flap) unloop; undrop ;;
    on/mtu)   # a path MTU black hole: every packet above it dropped, no ICMP; GSO's super-packets are dropped whole
        iptables -A RIG -p tcp -m length --length "$(($3 + 1)):65535" $(tag) -j DROP ;;
    on/asym) claim qdisc; peers=(${4//,/ }); band4 delay "$3"ms ;;   # to these peers only: the way back is not delayed
    on/dup) claim qdisc; band4 duplicate "$3"% ;;
    on/corrupt) claim qdisc; band4 corrupt "$3"% ;;   # a flipped bit fails the TCP checksum: loss the kernel counts
    on/conntrack)   # a stateful firewall that lost its state: flows it did not see begin are INVALID, and dropped
        modprobe nf_conntrack
        $rig/fault.sh -i "$id" on sysctl net.netfilter.nf_conntrack_tcp_loose 0
        iptables -A RIG -m conntrack --ctstate INVALID $(tag) -j DROP
        # the rig's entries only (every session dials a rig port): on five boxes the table is the box's
        for p in 7400 7001; do conntrack -D -p tcp --orig-port-dst $p >/dev/null 2>&1 || true; done ;;
    off/conntrack) undrop; $rig/fault.sh -i "$id" off sysctl net.netfilter.nf_conntrack_tcp_loose ;;
    on/backlog)   # a listen() takes the backlog then: the node restarts under it, and again after
        $rig/fault.sh -i "$id" on sysctl net.core.somaxconn "${3:-1}"; kill9 ;;
    off/backlog) $rig/fault.sh -i "$id" off sysctl net.core.somaxconn; kill9 ;;
    on/ports)   # n ephemeral ports, and every session reset so each redial needs one; never on b1, whose nemesis
        # leaves an ssh in TIME_WAIT per call
        $rig/fault.sh -i "$id" on sysctl net.ipv4.ip_local_port_range "60000 $((60000 + $3))"; sockkill "${peers[@]}" ;;
    off/ports) $rig/fault.sh -i "$id" off sysctl net.ipv4.ip_local_port_range ;;
    on/crashloop)   # SIGKILL every period, also while it starts again: Restart=always brings it back after 1 s
        loop bash -c 'while :; do
            p=$(systemctl show -p MainPID --value "$0"); [ "$p" = 0 ] || kill -KILL "$p" 2>/dev/null; sleep "$1"; done' \
            $unit "$(secs "$3")" ;;
    off/crashloop) unloop; start_unheld ;;
    on/killterm)   # a clean leave cut short: TERM (a storm of them, 10 ms apart), then KILL after ms if it still runs
        if running; then
            for ((i = 0; i < ${4:-1}; i++)); do kill -TERM "$p" 2>/dev/null || true; sleep 0.01; done
            sleep "$(secs "$3")"
            [[ $(pid) != "$p" ]] || kill -KILL "$p" 2>/dev/null || true   # not a PID the box gave out again
        fi ;;
    off/killterm) ;;
    on/tasksmax)   # pids.max at the tasks it has plus headroom: a new thread fails with EAGAIN
        claim tasks
        if running; then   # its cgroup stays at 0 tasks: headroom alone would keep a start from running
            [[ -e $state/tasksmax ]] || systemctl show -p TasksMax --value $unit >$state/tasksmax
            prop TasksMax=$(( $(<$cg/pids.current) + $3 ))
        fi ;;
    off/tasksmax) if release tasks && [[ -e $state/tasksmax ]]; then tasks_back; fi ;;
    on/nofile)   # the soft limit at the fds open now plus headroom: accepts, dials and new files fail with EMFILE
        claim nofile
        if running; then prlimit --pid "$p" --nofile="$(( $(ls /proc/$p/fd | wc -l) + $3 )):"; fi ;;
    off/nofile) if release nofile; then nofile_back; fi ;;
    on/threadstarve)   # the named threads on one core of the node's, beside a SCHED_FIFO hog busy that % of each 100 ms
        claim affinity
        if running; then
            python3 - "$p" "${4:-axon-clock,tokio-,axon-worker}" >$state/affinity <<'EOF'
import os, sys
pid, names = sys.argv[1], tuple(sys.argv[2].split(","))
core = min(os.sched_getaffinity(int(pid)))
print(pid, core)  # for unpin
for tid in os.listdir("/proc/%s/task" % pid):
    try:
        if open("/proc/%s/task/%s/comm" % (pid, tid)).read().strip().startswith(names):
            old = sorted(os.sched_getaffinity(int(tid)))
            os.sched_setaffinity(int(tid), {core})
            print(tid, ",".join(map(str, old)))
    except OSError:  # the thread ended
        pass
EOF
            read -r _ core <$state/affinity
            loop -p CPUAffinity="$core" -p CPUSchedulingPolicy=fifo \
                -p CPUSchedulingPriority=1 python3 -c 'import sys, time
b = min(float(sys.argv[1]), 95) / 1000  # 5 ms of each 100 left to the core, whatever the RT throttle says
while True:
    t = time.monotonic() + b
    while time.monotonic() < t:
        pass
    time.sleep(max(0, 0.1 - b))' "$3"
        fi ;;
    off/threadstarve) if release affinity; then unloop 2>/dev/null || true; unpin; fi ;;
    on/reclaim)   # memory.reclaim of mb every 200 ms: the page cache of its logs and journal, its anon memory once it can swap
        loop \
            bash -c 'while :; do echo "$1" >"$0" 2>/dev/null; sleep 0.2; done' $cg/memory.reclaim "$3M" ;;
    off/reclaim) unloop ;;
    on/hook) claim hook; echo "$3 ${4:-0}" >"$(ctl)" ;;   # src/hooks.rs polls it every 500 ms
    off/hook) if release hook; then rm -f "$(ctl)"; fi ;;
    on/coredump)   # SIGABRT with cores on: the process ends once its memory is written out
        claim sysctl:kernel.core_pattern
        [[ -e $state/sysctl/kernel.core_pattern ]] || sysctl -n kernel.core_pattern >$state/sysctl/kernel.core_pattern
        sysctl -qw kernel.core_pattern=/var/tmp/rig-core.%p
        if running; then prlimit --pid "$p" --core=unlimited; kill -ABRT "$p"; fi ;;
    off/coredump) if release sysctl:kernel.core_pattern; then unsysctl kernel.core_pattern; rm -f /var/tmp/rig-core.*; fi ;;
    on/swap)   # a swap file, the unit let swap, and MemoryHigh at % of its anon memory to push it there
        claim memory.high
        claim swap
        [[ -e $swapfile ]] || { fallocate -l 2G $swapfile; chmod 600 $swapfile; mkswap $swapfile >/dev/null; }
        swapped || swapon $swapfile
        prop MemorySwapMax=infinity MemoryHigh=$(anon_pct "$3") ;;
    off/swap) if release swap; then release memory.high; prop MemoryHigh=infinity MemorySwapMax=0; swapoff $swapfile; fi ;;
    on/thp)   # transparent huge pages on (compaction stalls) or off, the box's
        claim thp
        [[ -e $state/thp ]] || for f in enabled defrag; do grep -o '\[[a-z+]*\]' $thp/$f | tr -d '[]'; done >$state/thp
        echo "$3" >$thp/enabled
        echo "$3" >$thp/defrag ;;
    off/thp) if release thp; then thp_back; fi ;;
    on/leap)   # realtime 10 s before a UTC midnight, with a leap second to insert there (STA_INS)
        claim leap
        clock_on
        s=$(python3 -c 'import time; print(round(86400 - time.time() % 86400 - 10, 3))')
        echo "$s $(adjtimex -p | awk '$1 == "status:" {print $2}')" >$state/leap
        step "$s"
        adjtimex --status 16 ;;
    off/leap) if release leap; then s=$(leap_back); clock_off step "$(awk -v s="$s" 'BEGIN { print -s }')"; fi ;;
    on/peer)   # a low-volume malformed/confused-peer probe at this node's own rig port, in a transient unit heal stops
        [[ -f $env ]] && . "$env" || true
        pself=${NODE_ID:-b1} pcount=$(awk -F, '{print NF}' <<<"${NODES:-x}")
        loop -p EnvironmentFile=-$env -E RIG_PEER_LOG="${LOG_DIR:-$mnt}/peer.jsonl" \
            $rig/rig peer "${IP:-127.0.0.1}:${PORT:-7400}" "${3:-secret}" "b$(( ${pself#b} % pcount + 1 ))" ;;   # claims its neighbour's id
    off/peer) unloop ;;
    *) sed -n '/^#   /,/^set /p' "$0" | sed '$d'; exit 2 ;;
esac
