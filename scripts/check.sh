#!/usr/bin/env bash
# The repository's checks, for people and agents alike. Logs land in target/check/.
#
#   scripts/check.sh          fmt, clippy (warnings denied), every workspace test and doctest, doc links,
#                             I/O-free cores, no tokio test-util in published crates, fault matrix, a reason on
#                             every timing a test or example sets, built UI in step with its source,
#                             scripts/bench.sh on fixtures, rig (below)
#                                                  ~3 min cold on 24 threads, on the Rust rust-toolchain.toml pins
#   scripts/check.sh quick <path|crate>..  a lane's check before it hands over: fmt, then clippy and the tests of
#                             the crates the paths are in and of every workspace crate that depends on them, the World
#                             tests on SEEDS (0..4), then doc links. Pass `git diff --name-only main` (a box copy has no
#                             git). The full check and sim run where lanes are merged
#   scripts/check.sh live     the #[ignore]d ClickHouse and Consul tests           needs `docker compose up -d`
#   scripts/check.sh smoke    a playground from scripts/ctl.sh passes doctor, then is stopped
#   scripts/check.sh loom     the loom models (mailbox, idle workers, reply state, session write order), release
#   scripts/check.sh sim [test]  the World tests (tests/world*.rs) in the sim build (tokio seeded), profile sim
#                             (release without LTO), under target/sim, in two processes whose digests must match;
#                             prints the seeds each check_seeds test ran, and per failing seed the SIM_SEED=n command
#                             that replays it. SEEDS=a..b picks the seeds; [test] is a file (world_disk) or a filter
#   scripts/check.sh tla      each small TLA+ model's base config and the other configs tla() names pass, each
#                             spec/tla/mutations/*.cfg ends with a violation; needs java
#                             and tla2tools.jar
#                             (TLA2TOOLS, ./ or ~/tla/)                ~13 min on 24 threads
#   scripts/check.sh faults   every test spec/faults.md cites exists; prints the gaps per feature
#   scripts/check.sh rig      the fault rig's selftests: nemesis.py (plans deterministic, version 1 byte for byte,
#                             legal stacks, the chaos, pairs and scenarios schedules, the timeline runner) and
#                             check.py (a plant per invariant); python3 only
#   scripts/check.sh repeat [n]  the workspace tests n times (5) to catch flakes; on Linux `taskset -c 1,2` is CI's two cores without core 0, so a hardcoded core id fails
#
# Any failure exits non-zero with the failing command's output. CI runs the default mode.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p target/check

step() { printf '\n== %s\n' "$*"; }
fail() { printf '\nFAIL: %s\n' "$*" >&2; exit 1; }

links() {
    # Every relative markdown link points at a file or directory that exists.
    local bad=0 f dir targets t
    for f in README.md AGENTS.md crates/*/README.md crates/axon/guide/*.md spec/*.md examples/*.md examples/*/README.md; do
        dir=$(dirname "$f")
        targets=$( (grep -o '](\([^)#]*\)' "$f" | sed 's/^](//'; sed -n 's/^\[[^]]*\]: \([^ #]*\).*/\1/p' "$f") | grep -v '^\(http\|mailto\|$\)' | sort -u || true)
        for t in $targets; do
            [ -e "$dir/$t" ] || { echo "$f: broken link $t"; bad=1; }
        done
    done
    # The examples' index lists every example.
    for dir in examples/*/Cargo.toml; do
        dir=$(basename "$(dirname "$dir")")
        grep -q "](${dir}/README.md)" examples/README.md || { echo "examples/README.md: $dir is not listed"; bad=1; }
    done
    return $bad
}

no_test_util() {
    # testkit needs tokio's test-util, and a --workspace build unifies it into every crate; the graph of every other
    # published crate must not pull it in.
    local tree
    tree=$(cargo tree -p axon-actors -p axon-net -p axon-cluster -p axon-stream -p axon-obs --all-features -e normal,features -i tokio)
    ! grep test-util <<<"$tree"
}

cores() {
    # The relay, seed, lease and quota state machines have no clock or runtime of their own; the simulations drive them.
    ! grep -n 'Instant::now\|SystemTime::now\|tokio::' crates/axon-stream/src/core.rs crates/axon-cluster/src/seed/core.rs crates/axon-cluster/src/paxos/core.rs crates/axon-cluster/src/quota/core.rs
}

faults() {
    # Every test spec/faults.md cites exists, every cell is tests, `n/a: why` or `gap`, and every feature lists
    # every fault. Gaps are counted, not failed.
    local bad=0 ref
    for ref in $(grep -o '`[^` ]*\.rs::[A-Za-z0-9_]*`' spec/faults.md | tr -d '`' | sort -u); do
        grep -Eq "fn +${ref##*::}[^A-Za-z0-9_]|${ref##*::}!" "${ref%%::*}" 2>/dev/null ||
            { echo "spec/faults.md: no fn ${ref##*::} in ${ref%%::*}"; bad=1; }
    done
    awk -F'|' '
        /^### / { f = substr($0, 5); order[++n] = f; gaps[f] = 0 }
        f && /^[|] / && !/^[|] Fault/ {
            rows[f]++; cell = $3; gsub(/^ +| +$/, "", cell)
            ref = "`[^`]+\\.rs::[A-Za-z0-9_]+`"
            if ($NF !~ /^ *$/) { print "spec/faults.md: text after the last cell: " $0; bad = 1 }
            if (cell == "gap") gaps[f]++
            else if (cell !~ /^n\/a: ./ && cell !~ ("^" ref "(, " ref ")*$")) { print "spec/faults.md: bad cell: " $0; bad = 1 }
        }
        END {
            for (i = 1; i <= n; i++) {
                printf "%3d gaps  %s\n", gaps[order[i]], order[i]
                if (rows[order[i]] != rows[order[1]]) { print "spec/faults.md: " order[i] " has " rows[order[i]] " faults, not " rows[order[1]]; bad = 1 }
            }
            exit bad
        }' spec/faults.md || bad=1
    return $bad
}

timing() {
    # A test or example that sets a timing field of a shipped *Config or *Timings (or of a Backoff) says why on that
    # line or in a comment right above it, which covers the settings that follow it. Every test once shortened the
    # redial backoff, so none ran the shipped 30 s cap the rig then hit (spec/fault-rig.md § Findings fixed).
    local pairs fields structs
    pairs=$(awk '/^pub struct ([A-Za-z]*(Config|Timings)|Backoff)[ <{]/ { s = $3; sub(/[<{].*/, "", s); next } /^}/ { s = "" }
        s && /^    pub [a-z_]+: (Option<Duration>|Duration|Backoff),/ { sub(/^    pub /, ""); sub(/:.*/, ""); print s, $0 }' \
        $(find crates/axon/src crates/axon-net/src crates/axon-cluster/src crates/axon-stream/src crates/axon-obs/src -name '*.rs'))
    fields=$(echo "$pairs" | cut -d' ' -f2 | sort -u | paste -sd'|' -)
    # A bare Config literal is an app's own; axon-net's is taken only by its path, as in a `..Config::new(..)` update.
    structs=$( (echo "$pairs" | cut -d' ' -f1 | grep -vx Config; echo '(axon_)?net::Config') | sort -u | paste -sd'|' -)
    # A struct the scan no longer reads would switch the check off.
    case "|$fields|" in *'|backoff|'*'|idle_hold|'*'|initial|'*'|lease|'*) ;; *) echo "timing: no backoff, idle_hold, initial or lease field found: $fields"; return 1 ;; esac
    # `x.field = ` anywhere, and `field: ` only inside a literal of a shipped struct, so an actor's own `idle` or
    # a broker's own `backoff` is not taken for one.
    find crates/*/tests crates/*/examples examples -name '*.rs' | sort | xargs awk -v f="$fields" -v s="$structs" '
        FNR == 1 { why = 0; depth = 0 }
        { code = $0; sub(/\/\/.*/, "", code) }
        code ~ /^ *$/ && /\/\// { why = 1; next }
        {
            lit = depth > 0 ? code : match(code, "(^|[^A-Za-z0-9_])(" s ") \\{") ? substr(code, RSTART) : ""
            if (lit != "") depth += gsub(/\{/, "{", lit) - gsub(/\}/, "}", lit)
            if (depth < 0) depth = 0
        }
        (code ~ ("[a-z_)]\\.(" f ")(\\.[a-z_]+)?\\)? = ") && code !~ /self\.[a-z_]+ = /) || lit ~ ("(^ *|[{,] )(" f "): ") {
            if (!why && !/\/\//) { print FILENAME ":" FNR ": a timing set without a // reason: " $0; bad = 1 }
            next
        }
        { why = 0 }
        END { exit bad }'
}

dist() {
    # The dashboard binary embeds ui/dist, so a UI source change without a rebuild ships the old console.
    local ui=crates/axon-obs/ui
    [ -d .git ] || [ -f .git ] || { echo "skipped: no git here (a box copy); the laptop and CI run it"; return 0; }
    git diff --quiet HEAD -- "$ui/src" "$ui/index.html" || fail "$ui/src is modified: npm run build there and commit dist"
    local src dist
    src=$(git log -1 --format=%ct -- "$ui/src" "$ui/index.html")
    dist=$(git log -1 --format=%ct -- "$ui/dist")
    [ "${src:-0}" -le "${dist:-0}" ] || fail "$ui/src is newer than $ui/dist: npm run build there and commit dist"
}

tests() {
    # A passing run is summarised; a failing one also prints each failing test's own output, values included.
    local n=$(($(nproc 2>/dev/null || getconf _NPROCESSORS_ONLN) / 3))
    if [ "$n" -ge 2 ]; then
        lanes "$1" "$n" | grep -E 'Running|Doc-tests|test result|FAILED|panicked|ignored' && return
    else
        cargo test --workspace --all-features --no-fail-fast 2>&1 | tee "$1" |
            grep -E 'Running|Doc-tests|test result|FAILED|panicked|ignored' && return
    fi
    sed -n '/^failures:$/,/^test result:/p' "$1"
    return 1
}

lanes() {
    # One build, then the World tests one at a time, since each fills every core through check_seeds, beside every
    # other test executable, n at once. Every executable the build made runs, once. Prints the log it writes to $1.
    local cargo="cargo test --workspace --all-features --no-fail-fast" dir=${1%.log} rc=0 world rest
    rm -rf "$dir" && mkdir -p "$dir"
    $cargo --no-run >"$dir/build.log" 2>&1 || { tee "$1" <"$dir/build.log" >&2; return 1; }
    rest=$(echo --lib
        sed -n 's/^ *Executable unittests src\/main\.rs (.*\/\([a-z0-9_]*\)-[0-9a-f]*)$/--bin \1/p' "$dir/build.log"
        sed -n 's/^ *Executable tests\/.* (.*\/\([a-z0-9_]*\)-[0-9a-f]*)$/--test \1/p' "$dir/build.log" |
            grep -v '^--test world' | sort -u
        echo --doc)
    $cargo --test 'world*' >"$dir/world.log" 2>&1 &
    world=$!
    echo "$rest" | xargs -P "$2" -I{} sh -c '$0 {} >"$1/$(echo {} | tr -d " -").log" 2>&1' "$cargo" "$dir" || rc=1
    wait "$world" || rc=1
    cat "$dir"/*.log | tee "$1"
    diff <(sed -n 's/^ *Executable //p' "$dir/build.log" | sort) <(sed -n 's/^ *Running //p' "$dir"/*.log | sort) >&2 ||
        { echo "FAILED: the lanes ran other test executables than the build made"; rc=1; }
    return $rc
}

repeat() {
    local runs=${1:-5} failed=0 i
    for i in $(seq 1 "$runs"); do
        step "cargo test, run $i of $runs"
        tests "target/check/repeat-$i.log" || failed=$((failed + 1))
    done
    [ "$failed" -eq 0 ] || fail "$failed of $runs runs failed (logs: target/check/repeat-*.log)"
    printf '\nOK: repeat (%s runs)\n' "$runs"
}

check() {
    # rust-toolchain.toml pins the toolchain every machine lints with, CI and the boxes too.
    step "toolchain pinned at rust-version"
    [ "$(sed -n 's/^channel = "\(.*\)"/\1/p' rust-toolchain.toml)" = "$(sed -n 's/^rust-version = "\(.*\)"/\1/p' Cargo.toml)" ] ||
        fail "rust-toolchain.toml's channel is not Cargo.toml's rust-version"
    step "cargo fmt --check";  cargo fmt --all --check
    step "cargo clippy";       cargo clippy --workspace --all-features --all-targets -- -D warnings
    step "cargo test";         tests target/check/test.log
    step "doc links";          links
    step "I/O-free cores";     cores
    step "no tokio test-util in published crates"; no_test_util
    step "fault matrix";       faults
    step "timing overrides";   timing
    step "ui dist in step";    dist
    step "bench.sh on fixtures"; scripts/test-bench.sh
    step "rig planner and checker"; rig
    printf '\nOK: check\n'
}

quick() {
    [ $# -gt 0 ] || fail "quick takes the changed paths (git diff --name-only main) or crate names"
    local meta pkgs p name dir best found crates=() test=()
    meta=$(cargo metadata --format-version 1 --no-deps)
    pkgs=$(jq -r '.workspace_root as $w | .packages[] | "\(.name) \(.manifest_path | ltrimstr($w + "/") | rtrimstr("Cargo.toml"))"' <<<"$meta")
    for p in "$@"; do
        # The guides and the API page are doctests of axon-obs.
        case $p in crates/axon/guide/*|crates/axon/README.md) crates+=(axon-obs) ;; esac
        best= found=
        while read -r name dir; do
            [[ $p == "$name" || $p == "$dir"* ]] && (( ${#dir} > ${#best} )) && best=$dir found=$name
        done <<<"$pkgs"
        [ -z "$found" ] || crates+=("$found")
    done
    step "cargo fmt --check"; cargo fmt --all --check
    if [ ${#crates[@]} -gt 0 ]; then
        # The crates that depend on them, through any kind of dependency, until no more are added.
        mapfile -t crates < <(jq -r --argjson seed "$(printf '%s\n' "${crates[@]}" | jq -R . | jq -s .)" '
            [.packages[] | {n: .name, d: [.dependencies[].name]}] as $p
            | def grow($s): ([$p[] | select(any(.d[]; . as $x | $s | index($x))) | .n] + $s | unique) as $t
                | if ($t | length) == ($s | length) then $s else grow($t) end;
            grow($seed | unique)[]' <<<"$meta")
        for name in "${crates[@]}"; do test+=(-p "$name"); done
        step "cargo clippy ${crates[*]}"; cargo clippy "${test[@]}" --all-features --all-targets -- -D warnings
        step "cargo test ${crates[*]}, World tests on SEEDS=${SEEDS:-0..4}"
        if ! SEEDS=${SEEDS:-0..4} cargo test "${test[@]}" --all-features --no-fail-fast >target/check/quick.log 2>&1; then
            grep -E 'test result|FAILED|panicked|^error' target/check/quick.log
            sed -n '/^failures:$/,/^test result:/p' target/check/quick.log
            fail "quick (log: target/check/quick.log)"
        fi
        grep 'test result' target/check/quick.log | awk '{p += $4; f += $6} END {print p " passed, " f " failed"}'
    fi
    step "doc links"; links
    printf '\nOK: quick\n'
}

rig() {   # the fault rig's plans (version 1 byte for byte, legal stacks) and a plant per invariant its checker holds
    python3 examples/rig/ops/nemesis.py selftest
    python3 examples/rig/ops/check.py --selftest 0 >target/check/rig-check.log || { cat target/check/rig-check.log; fail "check.py --selftest"; }
    tail -1 target/check/rig-check.log
}

live() {
    export AXON_CLICKHOUSE=${AXON_CLICKHOUSE:-http://127.0.0.1:18123}
    export AXON_CLICKHOUSE_USER=${AXON_CLICKHOUSE_USER:-axon}
    export AXON_CLICKHOUSE_PASSWORD=${AXON_CLICKHOUSE_PASSWORD:-axon-playground}
    export AXON_CONSUL=${AXON_CONSUL:-http://127.0.0.1:18500}
    curl -fsS "$AXON_CLICKHOUSE/ping" >/dev/null || fail "no ClickHouse at $AXON_CLICKHOUSE: docker compose up -d"
    curl -fsS "$AXON_CONSUL/v1/status/leader" >/dev/null || fail "no Consul at $AXON_CONSUL: docker compose up -d"
    step "ClickHouse";  cargo test -p axon-obs --all-features --test metrics --test faults -- --ignored
    step "Consul";      cargo test -p axon-cluster --all-features --test consul --test ownership -- --ignored
    printf '\nOK: live\n'
}

smoke() {
    step "playground"
    local run port
    run=$(scripts/ctl.sh up) || fail "the playground did not start"
    port=$(jq .port <<<"$run")
    trap "scripts/ctl.sh -p $port down >/dev/null" EXIT
    step "doctor";   scripts/ctl.sh -p "$port" doctor || { scripts/ctl.sh -p "$port" logs; fail "doctor"; }
    step "overview"; scripts/ctl.sh -p "$port" api overview >target/check/overview.json
    printf '\nOK: smoke (body: target/check/overview.json)\n'
}

loom() {
    step "loom models"
    # axon_loom, not loom: tokio drops tokio::net under --cfg loom, and axon-cluster needs it.
    RUSTFLAGS="--cfg axon_loom" cargo test -p axon-actors -p axon-cluster -p axon-net --lib --release --target-dir target/loom \
        -- mailbox::model runtime::model engine::model queued::model
    RUSTFLAGS="--cfg axon_loom" cargo clippy -p axon-actors -p axon-cluster -p axon-net --lib --profile test \
        --target-dir target/loom -- -D warnings
    printf '\nOK: loom\n'
}

sim() {
    # tokio's own generator is seeded per node only under this cfg, so the World tests reproduce only here. Two
    # processes run them, and every digest they print must match.
    # The argument is a World test file (world_disk) or a filter on test names.
    local run failed=0 tests=(--test 'world*' -- --nocapture ${1:+"$1"})
    if [[ ${1:-} == world* ]] && compgen -G "crates/*/tests/$1.rs" >/dev/null; then
        tests=(--test "$1" -- --nocapture)
    fi
    for run in 1 2; do
        step "World tests, sim build, process $run of 2"
        if RUSTFLAGS="--cfg tokio_unstable" cargo test --workspace --all-features --profile sim --target-dir target/sim \
            --no-fail-fast "${tests[@]}" >"target/check/sim-$run.log" 2>&1; then
            grep 'test result' "target/check/sim-$run.log"
        else
            # A build error, the watchdog's abort or a signal prints no panic: then the log's end says why.
            grep -B2 -A12 'panicked' "target/check/sim-$run.log" || tail -40 "target/check/sim-$run.log"
            sed -n '/^failures:$/,/^test result:/p' "target/check/sim-$run.log"
            failed=1
        fi
        grep -o 'world digest .*' "target/check/sim-$run.log" | sort >"target/check/sim-$run.digests" || true
    done
    step "seeds run by each check_seeds test, on $(rustc -V), Cargo.lock cksum $(cksum <Cargo.lock | cut -d' ' -f1)"
    # -o, and no ^: under --nocapture a line can land after libtest's "test x ... " and before its "ok".
    grep -o 'seeds [^ ]*: .*' target/check/sim-1.log | sort || true
    if [ "$failed" -ne 0 ]; then
        step "failing seeds, each with the command that replays it"
        sed -n 's/.*seeds \([^ ]*\): .* then \(SIM_SEED=[0-9]*\) failed$/\2 scripts\/check.sh sim \1/p' target/check/sim-*.log |
            sort -u
    fi
    step "digests"
    diff target/check/sim-1.digests target/check/sim-2.digests || fail "the two processes printed different digests"
    [ "$failed" -eq 0 ] || fail "sim (logs: target/check/sim-*.log)"
    grep -Eq 'test result: ok\. [1-9][0-9]* passed' target/check/sim-1.log || fail "sim: no test matched ${1:-}"
    printf '\nOK: sim (%s digests matched)\n' "$(wc -l <target/check/sim-1.digests)"
}

tla() {
    # Each small model's base config must pass and each of its mutations end with an invariant or property violated,
    # four TLC workers a config, as many configs at once as fill the cores. PaxosLeases.cfg and Skew.cfg never finish:
    # SkewAtWait.cfg, SkewLogFloor.cfg and PaxosLeasesListed.cfg stand in for them, and the unprefixed mutations are
    # PaxosLeases'.
    local jar=${TLA2TOOLS:-} f m dir=$PWD/target/check/tla jobs=$(($(nproc 2>/dev/null || getconf _NPROCESSORS_ONLN) / 4))
    [ -n "$jar" ] || for f in tla2tools.jar ~/tla/tla2tools.jar; do [ -f "$f" ] && jar=$f && break; done
    [ -f "$jar" ] && command -v java >/dev/null || fail "tla needs java and tla2tools.jar (TLA2TOOLS, ./ or ~/tla/)"
    jar=$(cd "$(dirname "$jar")" && pwd)/$(basename "$jar")
    [ "$jobs" -ge 1 ] || jobs=1
    rm -rf "$dir" && mkdir -p "$dir"
    step "TLC $(java -cp "$jar" tlc2.TLC -h 2>&1 | grep -o 'Version [0-9.]*' | head -1), $jobs configs at once"
    # The base configs first: they take longest.
    { for f in spec/tla/*.tla; do m=$(basename "$f" .tla); [ "$m" = PaxosLeases ] || echo "pass $m.cfg $m"; done
      echo "pass SkewAtWait.cfg PaxosLeases"
      echo "pass SkewLogFloor.cfg PaxosLeases"
      echo "pass PaxosLeasesListed.cfg PaxosLeases"
      # The property Handover-MoveBlind and Handover-NoRedirect must break, which Handover.cfg does not check.
      echo "pass HandoverReach.cfg Handover"
      # A seed nodes do not count, and the WAN timings.
      echo "pass SeedMembership-Odd.cfg SeedMembership"
      echo "pass SeedMembership-Wan.cfg SeedMembership"
      # PushWindow.cfg has two streams and no slow-consumer reset; this one, one stream reset with pushes in flight.
      echo "pass PushWindowOneStream.cfg PushWindow"
      # Three faults, where the drop for an answer below acked matters (PushWindow-IgnoreWentBack).
      echo "pass PushWindowThreeFaults.cfg PushWindow"
      for f in spec/tla/mutations/*.cfg; do f=$(basename "$f") m=${f%%-*}; [ "$m" != "$f" ] || m=PaxosLeases; echo "fail mutations/$f $m"; done
    } | xargs -P $jobs -L1 bash -c '
        dir=$1 jar=$2 want=$3 cfg=$4 log=$1/$(basename "$4" .cfg).log t=$SECONDS
        (cd spec/tla && java -Xmx8g -XX:+UseParallelGC -cp "$jar" tlc2.TLC -workers 4 -metadir "$log.states" \
            -config "$cfg" "$5") >"$log" 2>&1
        case $? in
            0) got=pass ;;
            12) got=$(grep -o -m1 "Invariant [A-Za-z0-9_]* is violated" "$log" | cut -d" " -f2); got=${got:-safety} ;;
            13) got=temporal ;;
            *) got=error ;;
        esac
        rm -rf "$log.states"
        case $want:$got in pass:pass) ok=ok ;; fail:pass | fail:error | pass:*) ok=FAIL ;; *) ok=ok ;; esac
        printf "%-4s %-50s %-22s %4ss\n" "$ok" "$cfg" "$got" $((SECONDS - t))
        [ "$ok" = ok ]' _ "$dir" "$jar" || fail "tla (logs: target/check/tla/)"
    printf '\nOK: tla (%s s)\n' "$SECONDS"
}

mode=${1:-check}
case "$mode" in
    check|live|smoke|repeat|loom|sim|links|cores|faults|timing|dist|tla|rig) "$mode" "${2:-}" ;;
    quick) shift; quick "$@" ;;
    *) sed -n '/^#   /p' "$0"; exit 2 ;;
esac
