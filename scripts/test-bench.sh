#!/usr/bin/env bash
# scripts/bench.sh's parse, compare and merge steps on fixtures: harness logs as printed; a baseline and a run with
# regressions, improvements and rows within their noise band in both directions (one round of five far off does not
# widen it, one printed digit is not a change, a count at 0 has no band), a row the run no longer prints, rows only
# reported, one of a job not run and one new. Nothing is built or measured. Needs bash and python3.
set -euo pipefail
cd "$(dirname "$0")/.."
t=$(mktemp -d)
trap 'rm -rf "$t"' EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }

# parse: each row's unit and direction, and its values as printed.
mkdir -p "$t/run/logs"
printf '%s\n' 'pipeline-w64|5|0-11||pipeline' 'remote_calls|5|0-11||remote_calls 2000000 5000000' 'fanout|5|1||fanout' \
    'log-roll|5|4-7||log per=64' >"$t/run/jobs.txt"
cat >"$t/run/logs/pipeline-w64.r1.log" <<'EOF'
# job pipeline-w64 round 1
pipeline: shards=4 clients=16 window=64 sync=true workers=8 pin=false hot=false req/s=6011512 p50=139.7us p99=504.8us p99.9=1279.4us max=5603.3us syncs/s=79875 entries/sync=75.3 feed/s=100000 feed_dropped=0 breaches=0
EOF
sed 's/p99=504.8us/p99=495.2us/' "$t/run/logs/pipeline-w64.r1.log" >"$t/run/logs/pipeline-w64.r2.log"
cat >"$t/run/logs/remote_calls.r1.log" <<'EOF'
sequential request, 256 B: p50 16.972µs  p99 20.288µs  p99.9 23.754µs
  16 concurrent requests:    381056 req/s
tell, 256 B:   4458795 msg/s sent, 4995487 delivered, 4513 dropped
EOF
cat >"$t/run/logs/fanout.r1.log" <<'EOF'
batch 16 x 64 B, median of 7
fanout    1000 subscribers      2.0 ns/record/subscriber   527.5 M deliveries/s
publish replay required          10.6 ns/record    94.8 M records/s
EOF
cat >"$t/run/logs/log-roll.r1.log" <<'EOF'
| Owners | Segments | records a write | records/s | writes/s | p50 µs | p99 µs | p99.9 µs | max µs |
|---|---|---|---|---|---|---|---|---|
| 1 | zero-filled | 64 | 101376 | 1584 | 532 | 1572 | 3407 | 4194 |
EOF
parsed=$(python3 scripts/bench.py parse "$t/run")
echo "$parsed"
has() { grep -qxF "$1" <<<"$parsed" || fail "parse: no row '$1'"; }
has 'pipeline-w64,w64,p99,µs,lower,500,504.8 495.2'
has 'pipeline-w64,w64,req/s,req/s,higher,6.01151e+06,6011512 6011512'
has 'pipeline-w64,w64,max,µs,,5603.3,5603.3 5603.3'
has 'remote_calls,sequential request,p99.9,µs,lower,23.754,23.754'
has 'remote_calls,16 in flight,throughput,req/s,higher,381056,381056'
has 'remote_calls,tell,sent,msg/s,higher,4.4588e+06,4458795'
has 'fanout,fanout 1000 subscribers,cost,ns/record/subscriber,lower,2,2.0'
has 'fanout,publish replay required,cost,ns/record,lower,10.6,10.6'
has 'log-roll,"zero-filled, 64 a write",p99.9,µs,lower,3407,3407'
has 'log-roll,"zero-filled, 64 a write",max,µs,,4194,4194'
(( $(wc -l <<<"$parsed") == 21 )) || fail "parse: $(wc -l <<<"$parsed") lines, not 21"

cat >"$t/base.csv" <<'EOF'
# scripts/bench.sh record: fixture
job,mode,metric,unit,better,median,runs
perf-hosted,hosted,hosted,ns/msg,lower,16.0,15.5 16.0 16.5
pipeline-w64,w64,req/s,req/s,higher,5000000,4950000 5000000 4900000 5200000 5050000
remote_calls,sequential request,p50,µs,lower,17.0,17.0 24.6 16.9 17.1 17.0
loopback,repeated None,echo RTT p50,µs,lower,15.0,14.0 15.0 16.0
remote_calls,256 in flight,throughput,req/s,higher,1000000,900000 1000000 1100000
remote_calls,tell,sent,msg/s,higher,4458795,4458795 4458795 4458795
quota-check,check Take Uniform keys=1000,1 threads,M calls/s,higher,11.30,11.20 11.30 11.40
quota-memory,memory 200 k keys,further row,B,lower,-10,-10 -10 -10
scale-actors,actors 1 M,per idle actor,B,lower,1202,1202 1202 1202
scale-actors,actors 1 M,"idle, no telemetry",cores,lower,0,0.000 0.000 0.000
scale-nodes-50,nodes-50,leases the other nodes lost,leases,lower,0,0 0 0
fanout,fanout 1000 subscribers,cost,ns/record/subscriber,lower,1.9,1.9 1.9 1.9 1.9 1.9
pipeline-w64,w64,max,µs,,6000,5000 6000 7000
overhead,embedded,timings off,ns/msg,lower,7.66,7.66 8.55 8.52
EOF
cat >"$t/now.csv" <<'EOF'
job,mode,metric,unit,better,median,runs
perf-hosted,hosted,hosted,ns/msg,lower,18.0,17.8 18.0 18.4
pipeline-w64,w64,req/s,req/s,higher,5300000,5290000 5300000 5310000
remote_calls,sequential request,p50,µs,lower,18.0,17.9 18.0 18.1
loopback,repeated None,echo RTT p50,µs,lower,16.5,16.1 16.5 16.9
remote_calls,256 in flight,throughput,req/s,higher,850000,840000 850000 860000
quota-check,check Take Uniform keys=1000,1 threads,M calls/s,higher,10.70,10.60 10.70 10.80
quota-memory,memory 200 k keys,further row,B,lower,-20,-20 -20 -20
scale-actors,actors 1 M,per idle actor,B,lower,1180,1180 1180 1180
scale-actors,actors 1 M,"idle, no telemetry",cores,lower,0.2,0.200 0.200 0.200
scale-nodes-50,nodes-50,leases the other nodes lost,leases,lower,1,0 1 1
fanout,fanout 1000 subscribers,cost,ns/record/subscriber,lower,2,2.0 2.0 1.9 2.0 2.0
pipeline-w64,w64,max,µs,,9000,8000 9000 9500
disk,disk,published and durable,M records/s,higher,4.8,4.7 4.8 4.9
EOF

rc=0
out=$(scripts/bench.sh compare "$t/base.csv" "$t/now.csv" 2>&1) || rc=$?
echo "$out"
[[ $rc == 1 ]] || fail "a run with regressions exited $rc, not 1"
row() { grep -E "^$1 .* $2\$" <<<"$out" >/dev/null || fail "no row '$1 ... $2'"; }
row 'perf-hosted +hosted +hosted' 'REGRESSION'                  # +12.5 % on a 6.3 % band, lower is better
row 'quota-check +check Take Uniform keys=1000 +1 threads' 'REGRESSION'   # -5.3 % on the 3 % floor, higher is better
row 'remote_calls +sequential request +p50' 'REGRESSION'        # +5.9 %: the 24.6 round does not make the band 45 %
row 'scale-actors +actors 1 M +idle, no telemetry' 'REGRESSION' # 0 -> 0.2 cores, past a hundredth of a core
row 'scale-nodes-50 +nodes-50 +leases the other nodes lost' 'REGRESSION'  # a count at 0 may not rise
row 'remote_calls +tell +sent' 'MISSING'                        # its job ran and no longer prints it
row 'pipeline-w64 +w64 +req/s' 'improved'                       # +6 %: without 4.9 M and 5.2 M, the 3 % floor
row 'quota-memory +memory 200 k keys +further row' 'improved'   # -10 B -> -20 B, lower is better below 0 too
row 'scale-actors +actors 1 M +per idle actor' 'within noise'   # -1.8 %: a range of 0 still has the floor
row 'loopback +repeated None +echo RTT p50' 'within noise'      # +10 % on a 13.3 % band
row 'remote_calls +256 in flight +throughput' 'within noise'    # -15 % on a 20 % band
row 'fanout +fanout 1000 subscribers +cost' 'within noise'      # 1.9 -> 2.0: one printed digit
row 'pipeline-w64 +w64 +max' 'reported'                         # no direction: never gated
row 'disk +disk +published and durable' 'new'
! grep -q '^overhead ' <<<"$out" || fail "a row of a job not run was shown"
grep -q '^5 REGRESSION, 1 MISSING, 2 improved, 4 within noise, 1 reported, 1 new, 1 not run ' <<<"$out" ||
    fail "summary: $out"

# Without the regressions the same comparison passes; a MISSING row alone fails it.
{ grep -v -e '^perf-hosted,' -e '^quota-check,' -e '^remote_calls,' -e 'idle, no telemetry' \
    -e '^scale-nodes' "$t/now.csv"; grep -e '^remote_calls,' -e 'idle, no telemetry' "$t/base.csv"; } >"$t/fine.csv"
scripts/bench.sh compare "$t/base.csv" "$t/fine.csv" >/dev/null || fail "a run without regressions exited non-zero"
grep -v '^remote_calls,tell' "$t/fine.csv" >"$t/missing.csv"
rc=0; scripts/bench.sh compare "$t/base.csv" "$t/missing.csv" >/dev/null || rc=$?
[[ $rc == 1 ]] || fail "a run with a MISSING row exited $rc, not 1"
rc=0; scripts/bench.sh compare "$t/base.csv" "$t/nonexistent.csv" >/dev/null 2>&1 || rc=$?
[[ $rc == 2 ]] || fail "a comparison that cannot run exited $rc, not 2"

# merge: the jobs the run has replace theirs (remote_calls loses its tell row), the rest stay.
merged=$(python3 scripts/bench.py merge "$t/base.csv" "$t/now.csv")
grep -qx 'perf-hosted,hosted,hosted,ns/msg,lower,18.0,17.8 18.0 18.4' <<<"$merged" || fail "merge: $merged"
grep -q '^overhead,embedded,timings off,' <<<"$merged" || fail "merge dropped a job not run: $merged"
! grep -q '^remote_calls,tell' <<<"$merged" || fail "merge kept a row its job no longer prints: $merged"
(( $(wc -l <<<"$merged") == 15 )) || fail "merge: $(wc -l <<<"$merged") lines, not 15"
echo "OK: bench.sh parse, compare, merge"
