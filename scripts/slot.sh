# Sourced by box.sh run on the box: holds one of the box's build slots for as long as the run and what it starts live,
# so lanes sharing a box take turns instead of thrashing it. ~/.config/box-slots says how many; without it, no limit.
slots=$(cat ~/.config/box-slots 2>/dev/null || echo 0)
if [ "$slots" -gt 0 ] 2>/dev/null; then
    mkdir -p /tmp/box-slots
    until for slot in $(seq 1 "$slots"); do exec 9>"/tmp/box-slots/$slot"; flock -n 9 && break; done; do
        [ -n "${slot_wait:-}" ] || echo "box.sh: all $slots slots busy, waiting" >&2
        slot_wait=1
        sleep 2
    done
fi
