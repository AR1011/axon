#!/usr/bin/env bash
# World seeds across dev boxes: splits a..b into one SEEDS shard per box, runs `scripts/check.sh sim` on every box at
# once through scripts/box.sh, then prints per box the seeds each World test ran and each failing seed with the command
# that replays it on any box.
#
#   scripts/seeds.sh a..b user@host..    box dir DIR (sim-seeds), logs in target/check/seeds-<n>.log
set -euo pipefail
cd "$(dirname "$0")/.."
range=${1:-} from=${range%..*} to=${range#*..}
[ $# -gt 1 ] && [ "$from" -lt "$to" ] 2>/dev/null || { sed -n '/^#   /p' "$0"; exit 2; }
shift
dir=${DIR:-sim-seeds} per=$(( (to - from + $# - 1) / $# )) n=0
mkdir -p target/check
for box in "$@"; do
    a=$(( from + n * per )); b=$(( a + per < to ? a + per : to ))
    [ "$a" -lt "$to" ] || break
    shards[n]="$box SEEDS=$a..$b"
    (BOX=$box scripts/box.sh sync "$dir" && BOX=$box scripts/box.sh run "$dir" "SEEDS=$a..$b scripts/check.sh sim") \
        >"target/check/seeds-$n.log" 2>&1 &
    pids[n]=$!
    n=$((n + 1))
done
failed=0
for n in "${!pids[@]}"; do
    status=ok
    wait "${pids[n]}" || { status=FAILED; failed=1; }
    printf '\n==== %s: %s\n' "${shards[n]}" "$status"
    grep -q '^== seeds run' "target/check/seeds-$n.log" && sed -n '/^== seeds run/,$p' "target/check/seeds-$n.log" ||
        tail -20 "target/check/seeds-$n.log"
done
exit $failed
