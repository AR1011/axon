# This node's names, sourced by fault.sh, setup.sh and run.sh's commands on a node: on a box of its own they are the
# plain ones; inside network namespace bN (setup.sh --netns, five nodes on one box) each carries bN.
n=$(ip netns identify 2>/dev/null || true)
# A disk's dm device, mount and image: journal, the default, or streams.
device() { dm=rig-$1${n:+-$n} mnt=/mnt/rig-$1${n:+-$n} img=/var/lib/rig-$1${n:+-$n}.img; }
device journal
unit=rig-node${n:+@$n}
env=/home/ubuntu/rig/env${n:+-$n} up=/home/ubuntu/rig/up${n:+-$n} state=/run/rig-fault${n:+-$n}
cg=/sys/fs/cgroup/system.slice/$unit.service
# The build the unit runs, a link to rig or rig-v2 (fault.sh upgrade), and settings over env (fault.sh cfgdrift).
exe=/home/ubuntu/rig/exe${n:+-$n} override=$env.override
