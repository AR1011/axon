#!/usr/bin/env bash
# `ssh <node> <cmd>` on one box (setup.sh --netns): cmd as ubuntu inside that node's network namespace. nsenter
# --net keeps the host's mounts; `ip netns exec` remounts /sys, and fault.sh would find /sys/fs/cgroup empty.
#
#   in.sh <ip> <cmd..>
set -euo pipefail
i=$(grep -nxF "${1:?usage: in.sh <ip> <cmd..>}" /home/ubuntu/rig/nodes | cut -d: -f1 || true)
exec sudo nsenter --net=/run/netns/b"${i:?not a rig node: $1}" sudo -u ubuntu -H bash -c "cd; ${*:2}"
