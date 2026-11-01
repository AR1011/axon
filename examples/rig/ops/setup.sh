#!/usr/bin/env bash
# Prepares one rig node. Safe to rerun, and safe on a box busy with other work: no reboot, no clock change, no rule
# outside the rig ports on non-loopback interfaces, nothing started.
#
#   sudo setup.sh <ip1> .. <ip5>   tools, dm modules, the journal and streams devices, the rig port allowlist, the unit
#   sudo setup.sh --netns <n>      one box for n nodes: node i in network namespace b<i> at 10.77.0.<i>, its veth on
#                                  bridge rig0; each set up as above with its own names (node.sh), journal, chains
#                                  and unit instance rig-node@b<i>
#   sudo setup.sh --remove         undoes --netns: units, journals, namespaces, bridge
#   setup.sh doctor                {"missing":[..],"tcp":{..}}, missing empty once set up (it runs before setup too);
#                                  tcp the four host TCP settings spec/wan.md § 8 names, as this node sees them
set -euo pipefail
rig=/home/ubuntu/rig
[[ -e $rig/node.sh ]] || { echo '{"missing":["ops: scripts/rig.sh setup"]}'; exit; }   # 0, so rig.sh doctor asks every node
. $rig/node.sh
tools="tc iptables dmsetup python3 losetup blockdev mkfs.ext4 e2fsck fallocate nsenter"
tools+=" ss conntrack"   # the network classes: sockkill, conntrack's flush

doctor() {
    local missing=() t
    for t in $tools; do command -v "$t" >/dev/null || missing+=("$t"); done
    adjtimex -p 2>/dev/null | grep -q 'tick:' || missing+=(adjtimex)
    local targets; targets=$(sudo -n dmsetup targets 2>/dev/null || true)
    for t in flakey delay error; do grep -q "^$t " <<<"$targets" || missing+=("dm_$t"); done
    for t in cpu io memory; do grep -qw "$t" /sys/fs/cgroup/cgroup.controllers || missing+=("cgroup_$t"); done
    for d in journal streams; do
        device $d
        mountpoint -q $mnt && [[ $(sudo -n dmsetup table $dm 2>/dev/null | cut -d' ' -f3) == linear ]] &&
            touch $mnt/.doctor 2>/dev/null && rm -f $mnt/.doctor || missing+=($d)
    done
    device journal
    [[ $(systemctl show -p LoadState --value $unit) == loaded ]] || missing+=(unit)
    # A unit installed before the link and the override runs ~/rig/rig: upgrade and cfgdrift would change nothing.
    [[ $(systemctl show -p ExecStart --value $unit) == *"path=$exe ;"* &&
        $(systemctl show -p EnvironmentFiles --value $unit) == *"$override "* ]] || missing+=("unit runs $exe: setup")
    sudo -n iptables -S RIG-ALLOW 2>/dev/null | grep -q -- '-j DROP' && sudo -n iptables -S RIG >/dev/null 2>&1 &&
        sudo -n iptables -S INPUT | grep -q -- '-j RIG$' || missing+=(allowlist)
    # One box: every node over the bridge. A br_netfilter with a FORWARD policy of DROP (Docker's) would cut it.
    if [[ -n $n ]]; then for t in $(<$rig/nodes); do sudo -n ping -c1 -W1 -q "$t" >/dev/null 2>&1 || missing+=("bridge to $t"); done; fi
    local k tcp=
    for k in wmem:tcp_wmem rmem:tcp_rmem cc:tcp_congestion_control slow_start:tcp_slow_start_after_idle; do
        tcp+="${tcp:+,}\"${k%%:*}\":\"$(tr '\t' ' ' </proc/sys/net/ipv4/${k#*:})\""
    done
    printf '{"missing":[%s],"tcp":{%s}}\n' "$( (( ${#missing[@]} )) && printf '"%s",' "${missing[@]}" | sed 's/,$//')" "$tcp"
}

[[ ${1:-} == doctor ]] && { doctor; exit; }
[[ ${1:-} == --remove || $# -ge 2 ]] || { sed -n '/^#   /p' "$0"; exit 2; }
(( EUID == 0 )) || { echo "run as root" >&2; exit 2; }
export DEBIAN_FRONTEND=noninteractive

if [[ $1 == --remove && -n $n ]]; then   # this node, from its namespace; its chains go with the namespace
    systemctl stop $unit 2>/dev/null || true
    systemctl reset-failed $unit 2>/dev/null || true
    for d in journal streams; do
        device $d
        ! mountpoint -q $mnt || umount $mnt
        ! dmsetup info $dm >/dev/null 2>&1 || dmsetup remove $dm
        for l in $(losetup -j $img | cut -d: -f1); do losetup -d "$l"; done
        rm -f $img
        [[ ! -d $mnt ]] || rmdir $mnt
    done
    rm -rf $env $override $exe $up $state /run/systemd/system/$unit.service.d
    exit
elif [[ $1 == --remove ]]; then
    for ns in /run/netns/b[0-9]*; do
        [[ -e $ns ]] || continue
        nsenter --net="$ns" bash "$0" --remove
        ip netns del "${ns##*/}"
    done
    ! ip link show rig0 >/dev/null 2>&1 || ip link del rig0
    rm -f /etc/systemd/system/rig-node@.service $rig/netns
    systemctl daemon-reload
    exit
fi

install_pkgs() { apt-get install -y -q "$@" >/dev/null || { apt-get update -q >/dev/null && apt-get install -y -q "$@" >/dev/null; }; }
if [[ -z $n ]]; then   # the box's tools, once; a network namespace has no route to fetch them
    command -v dmsetup >/dev/null || install_pkgs dmsetup
    command -v conntrack >/dev/null || install_pkgs conntrack
    modprobe dm_flakey && modprobe dm_delay || { install_pkgs "linux-modules-extra-$(uname -r)" && modprobe dm_flakey && modprobe dm_delay; }
    # The adjtimex package's service sets the tick and frequency when installed, which would move a busy box's clock:
    # take the binary alone.
    if ! adjtimex -p 2>/dev/null | grep -q 'tick:'; then
        tmp=$(mktemp -d)
        (cd "$tmp" && apt-get download -q adjtimex >/dev/null 2>&1 || { apt-get update -q >/dev/null && apt-get download -q adjtimex >/dev/null 2>&1; })
        dpkg-deb -x "$tmp"/adjtimex_*.deb "$tmp/x"
        install -m 755 "$tmp/x/usr/sbin/adjtimex" /usr/local/sbin/adjtimex
        rm -rf "$tmp"
    fi

    # timesyncd polls every 34 min at most by default; a VM's clock wanders tens of ms in that, past the checker's δ of 5 ms.
    mkdir -p /etc/systemd/timesyncd.conf.d
    printf '[Time]\nPollIntervalMaxSec=64\n' >/etc/systemd/timesyncd.conf.d/rig.conf
    systemctl try-restart systemd-timesyncd
fi

if [[ $1 == --netns ]]; then
    modprobe -a veth bridge
    ip link show rig0 >/dev/null 2>&1 || ip link add rig0 type bridge
    ip link set rig0 up
    ips=()
    for i in $(seq "$2"); do ips+=(10.77.0.$i); done
    for i in $(seq "$2"); do
        b=b$i
        [[ -e /run/netns/$b ]] || ip netns add $b
        ip link show rig-$b >/dev/null 2>&1 || ip link add rig-$b type veth peer name eth0 netns $b
        ip link set rig-$b master rig0 up
        ip -n $b addr replace 10.77.0.$i/24 dev eth0
        ip -n $b link set eth0 up
        ip -n $b link set lo up   # a node reaches its own address over lo
    done
    # sudo looks its host name up, and a namespace cannot reach resolved's 127.0.0.53
    grep -qw "$(hostname)" /etc/hosts || echo "127.0.1.1 $(hostname)" >>/etc/hosts
    mkdir -p $rig
    touch $rig/netns
    chown ubuntu:ubuntu $rig $rig/netns
    for i in $(seq "$2"); do nsenter --net=/run/netns/b$i bash "$0" "${ips[@]}"; done
    exit
fi

# The journal: ext4 on dm rig-journal over a loop file, so fault.sh can swap the table under a mounted file system.
# The streams disk the same way: the stream history of the disk workload (src/disk.rs).
for d in journal:4G streams:2G; do
    device ${d%:*}
    [[ -f $img ]] || fallocate -l ${d#*:} $img
    loop=$(losetup -j $img | cut -d: -f1 | head -1)
    [[ -n $loop ]] || loop=$(losetup -f --show --direct-io=on $img)
    dmsetup info $dm >/dev/null 2>&1 || dmsetup create $dm --table "0 $(blockdev --getsz "$loop") linear $loop 0"
    blkid /dev/mapper/$dm >/dev/null || mkfs.ext4 -q -m 0 /dev/mapper/$dm
    mkdir -p $mnt
    mountpoint -q $mnt || mount -o errors=continue /dev/mapper/$dm $mnt
    chown ubuntu:ubuntu $mnt
done
device journal

# The cluster is plaintext with a shared secret: the rig ports take connections from the five addresses only. RIG is
# where fault.sh puts its rules; heal flushes RIG, never RIG-ALLOW.
iptables -N RIG-ALLOW 2>/dev/null || true
iptables -F RIG-ALLOW
for ip; do iptables -A RIG-ALLOW -s "$ip" -j RETURN; done
iptables -A RIG-ALLOW -j DROP
iptables -N RIG 2>/dev/null || true
hook() { iptables -C "$@" 2>/dev/null || iptables -I "$@"; }
hook INPUT ! -i lo -p tcp -m multiport --dports 7400,7001,7900 -j RIG-ALLOW
hook INPUT ! -i lo -p tcp -m multiport --ports 7400,7001 -j RIG
hook OUTPUT ! -o lo -p tcp -m multiport --ports 7400,7001 -j RIG
# The Health port (src/surface.rs) takes the five addresses too, and is never in RIG: a network fault leaves the
# probes' path to the node open, so a probe sees what the node says of itself while it is cut off.
hook INPUT ! -i lo -p tcp --dport 7401 -j RIG-ALLOW

if [[ -n $n ]]; then   # spec/wan.md § 8's settings for a cross-region link, which Linux keeps per namespace
    modprobe tcp_bbr
    sysctl -qw net.ipv4.tcp_congestion_control=bbr net.ipv4.tcp_slow_start_after_idle=0 \
        "net.ipv4.tcp_wmem=4096 16384 67108864" "net.ipv4.tcp_rmem=4096 131072 134217728"
fi

mkdir -p $rig/runs
printf '%s\n' "$@" >$rig/nodes
chown ubuntu:ubuntu $rig $rig/runs $rig/nodes
if [[ -n $n ]]; then   # an instance per namespace; system.slice keeps fault.sh's cgroup path, CPU 0 stays the store's
    { sed -e "s|^EnvironmentFile=-$rig/env|EnvironmentFile=-$rig/env-%i|" -e "s|^ExecStart=$rig/exe|ExecStart=$rig/exe-%i|" \
          $rig/rig-node.service
      printf '[Service]\nSlice=system.slice\nNetworkNamespacePath=/run/netns/%%i\nAllowedCPUs=1-%d\n' $(($(nproc) - 1))
    } >/etc/systemd/system/rig-node@.service
else
    install -m 644 $rig/rig-node.service /etc/systemd/system/rig-node.service
fi
systemctl daemon-reload
