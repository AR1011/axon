#!/usr/bin/env bash
# The performance gate on a bench box; what it runs and how it judges: crates/axon/guide/benchmarks.md, The gate.
#
#   scripts/bench.sh [full] [pattern]   check the machine, build, run the jobs (names matching pattern), compare with
#                                       scripts/bench/<cpu>.csv; exits 1 on a REGRESSION or MISSING row, 2 when it
#                                       cannot measure. full adds quota check at 10^7 keys
#   scripts/bench.sh record [full] [pattern]   the same, then replaces the baseline's rows of the jobs that ran
#   scripts/bench.sh build              the harnesses into target/bench/bin, to copy to a box that must not build
#   scripts/bench.sh compare <base.csv> <now.csv>   the comparison alone
#
# BIN=dir runs the binaries there instead of building; ROUNDS=n overrides every job's rounds (not with record); COMMIT
# names the tree where there is no .git (a box.sh copy). Each run lands in target/bench/<time>-<commit>/: every log
# with its command, uptime before and after, wall time and exit code; results.csv; compare.txt. On a box others
# measure on, run it under their lock.
set -euo pipefail
cd "$(dirname "$0")/.."

fail() { printf '\nFAIL: %s\n' "$*" >&2; exit 2; }

# name|rounds|cores|env|command. Pinning as in the guide's latest runs on this CPU. The short perf modes time 1 s, not
# the default 2 M messages (25-180 ms, whose range was 11-38 %); loopback and remote_calls 5-10x their defaults.
table() {
    cat <<'EOF'
perf-embedded|5|0-11||perf embedded 80000000
perf-hosted|5|0-11||perf hosted 60000000
perf-traced|5|||perf traced 8000000
perf-contended|5|0-11||perf contended 8000000
perf-fanout|5|0-11||perf fanout 12000000
perf-ask|5|0-11||perf ask
perf-asks|5|0-11||perf asks
perf-start|5|0-11||perf start
perf-route|5|||perf route
perf-topic|5|||perf topic
pipeline-w64|5|0-11||pipeline
pipeline-w8|5|0-11||pipeline window=8
pipeline-w4pin-w64|5|0-11||pipeline workers=4 pin=1
loopback|5|0-11||loopback 2000000
remote_calls|5|0-11||remote_calls 2000000 5000000
fanout|5|1||fanout
disk|5|4-7||disk disk-bench
log|5|4-7||log secs=5 owners=1 dir=log-bench
log-roll|5|4-7||log secs=10 owners=1 body=4096 per=64 shapes=zero-filled dir=log-bench
quota-check|3|||quota check 2 1000 1000000
quota-memory|3|||quota memory
quota-bytes|3|||quota bytes
quota-pull|3|||quota pull
overhead|3|||overhead
scale-actors|3|||scale actors 1000000
scale-keyed|3|||scale keyed 100000
scale-keyed-paxos|3||AXON_PAXOS=3|scale keyed 100000
scale-nodes-50|3|||scale nodes 50
scale-nodes-200|3|||scale nodes 200
scale-nodes-50-paxos|3||AXON_PAXOS=3|scale nodes 50
scale-nodes-200-paxos|3||AXON_PAXOS=3|scale nodes 200
scale-nodes-200-seeds|3||AXON_SEEDS=3|scale nodes 200
scale-streams|3|0-15||scale streams 50000
scale-subscribers|3|||scale subscribers
scale-metrics|3|||scale metrics 256
EOF
    [ "$full" ] && echo 'quota-check-10m|3|||quota check 2 10000000'
    return 0
}

build() {
    local out=target/bench/bin spec crate features examples e
    mkdir -p "$out"
    # One crate at a time, each with the features `cargo run -p <crate>` gives it; disk needs the cluster feature.
    for spec in "axon||perf pipeline log" "axon-net||loopback" "axon-cluster||remote_calls quota" "axon-stream||fanout" \
        "axon-stream|cluster|disk" "axon-obs||overhead scale"; do
        IFS='|' read -r crate features examples <<<"$spec"
        # shellcheck disable=SC2046 # one --example per word
        cargo build --release -p "$crate" ${features:+--features "$features"} $(printf -- '--example %s ' $examples)
        for e in $examples; do cp "${CARGO_TARGET_DIR:-target}/release/examples/$e" "$out/"; done
    done
}

machine() {
    cpu=$(sed -n 's/^model name[[:space:]]*: //p' /proc/cpuinfo 2>/dev/null | head -1 || true)
    [ "$cpu" ] || fail "no /proc/cpuinfo: bench.sh measures on a Linux bench box"
    baseline=scripts/bench/$(tr 'A-Z' 'a-z' <<<"$cpu" | tr -cs 'a-z0-9' '-' | sed 's/-*$//').csv
    [ "$record" ] || [ -f "$baseline" ] || fail "no baseline for $cpu ($baseline): scripts/bench.sh record on an idle box first"
    local governors
    governors=$(sort -u /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor 2>/dev/null | paste -sd' ' - || true)
    [ "$governors" = performance ] || fail "governor '${governors:-none}', not performance on every CPU:" \
        "echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor"
}

quiet() {
    # The 1-minute load average: after a build here, or the job before ours, it takes a few minutes to fall.
    local waited=0 load
    while load=$(cut -d' ' -f1 /proc/loadavg); ! awk -v l="$load" 'BEGIN { exit !(l < 1) }'; do
        [ "$waited" -lt 300 ] || fail "load average $load: something else runs here ($(uptime))"
        sleep 10; waited=$((waited + 10))
    done
}

interleave() {
    local run=$1 max r name rounds cores envs cmd log s rc failed=0
    max=$(cut -d'|' -f2 "$run/jobs.txt" | sort -n | tail -1)
    for r in $(seq 1 "$max"); do
        while IFS='|' read -r name rounds cores envs cmd; do
            [ "$r" -le "$rounds" ] || continue
            log=$run/logs/$name.r$r.log
            local pin=()
            [ "$cores" ] && pin=(taskset -c "$cores")
            printf '# job %s round %s, commit %s\n# cmd: %s%s%s\n# start %s uptime: %s\n' "$name" "$r" "$commit" \
                "${envs:+$envs }" "${pin[*]:+${pin[*]} }" "$cmd" "$(date -Is)" "$(uptime)" >"$log"
            s=$(date +%s%N)
            # --foreground keeps the harness in the terminal's process group, so Ctrl-C reaches it.
            # shellcheck disable=SC2086 # the command's words
            (cd "$run" && env $envs "${pin[@]}" timeout --foreground 45m "$bin/"$cmd) >>"$log" 2>&1 </dev/null && rc=0 || rc=$?
            printf '# end %s rc=%s wall_ms=%s uptime: %s\n' "$(date -Is)" "$rc" $((($(date +%s%N) - s) / 1000000)) \
                "$(uptime)" >>"$log"
            printf '%s r%s %-24s rc=%s\n' "$(date +%T)" "$r" "$name" "$rc"
            [ "$rc" != 130 ] || fail "interrupted"  # timeout exits 130 when Ctrl-C ends the harness
            [ "$rc" = 0 ] || failed=1
        done <"$run/jobs.txt"
    done
    [ "$failed" = 0 ] || fail "a job exited non-zero: grep -L 'rc=0' $run/logs/*"
}

measure() {
    full='' record=''
    [ "${1:-}" = record ] && { record=1; shift; }
    [ "${1:-}" = full ] && { full=1; shift; }
    [ -z "$record" ] || [ -z "${ROUNDS:-}" ] || fail "record takes each job's own rounds: its bands come from them"
    local pattern=${1:-.} run load
    machine
    commit=${COMMIT:-$(git rev-parse --short HEAD 2>/dev/null || echo unknown)}
    bin=${BIN:-}
    [ "$bin" ] || { build; bin=target/bench/bin; }
    bin=$(cd "$bin" && pwd)
    quiet
    run=target/bench/$(date +%Y%m%d-%H%M%S)-$commit
    mkdir -p "$run/logs"
    table | grep -E "^[^|]*($pattern)[^|]*\|" | sed "${ROUNDS:+s/|[0-9]*|/|$ROUNDS|/}" >"$run/jobs.txt" ||
        fail "no job matches $pattern"
    load=$(uptime)
    ulimit -n 500000 || fail "ulimit -n 500000: scale nodes opens that many sockets"
    echo "== $run: $(wc -l <"$run/jobs.txt") jobs, commit $commit, $cpu, $load"
    interleave "$run"
    python3 scripts/bench.py parse "$run" >"$run/results.csv" || fail "cannot parse $run"
    if [ "$record" ]; then
        mkdir -p scripts/bench
        { grep -s '^#' "$baseline" || true
          echo "# scripts/bench.sh record $pattern: $cpu, $(hostname), $(uname -r), commit $commit, $(date -Is), before: $load"
          python3 scripts/bench.py merge "$baseline" "$run/results.csv"; } >"$run/baseline.csv" || fail "cannot merge"
        mv "$run/baseline.csv" "$baseline"
        echo "== recorded $(($(wc -l <"$run/results.csv") - 1)) rows into $baseline"
    else
        local rc=0
        python3 scripts/bench.py compare "$baseline" "$run/results.csv" >"$run/compare.txt" || rc=$?
        cat "$run/compare.txt"
        [ "$rc" -le 1 ] || fail "cannot compare with $baseline"
        exit "$rc"
    fi
}

case ${1:-} in
    compare) python3 scripts/bench.py compare "${2:?base.csv}" "${3:?now.csv}" ;;
    build)   build; echo target/bench/bin ;;
    -*|help) sed -n '/^#   /p' "$0"; exit 2 ;;
    *)       measure "$@" ;;
esac
