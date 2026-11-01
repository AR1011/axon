#!/usr/bin/env bash
# Drive a live playground cluster: start your own, check it, query it, screenshot the console, stop it.
#
#   scripts/ctl.sh up [nodes=3] [--base /p/] [--churn] [--open]
#                                                         build, start on a free port, wait for /api/overview; prints the run
#                                                         and the console's user and password (a new one per run);
#                                                         --open: no sign-in (the console binds loopback)
#   scripts/ctl.sh ls                                     runs started from this checkout, alive or not
#   scripts/ctl.sh [-p port] doctor                       is this run worth driving: process up, port ours, nodes reachable
#   scripts/ctl.sh [-p port] api <path?query>             GET <base>api/<path>, the JSON body on stdout
#   scripts/ctl.sh [-p port] shot <route> [--wait css] [--steps f.mjs] [--out f.png]
#                                                         screenshot a console page once it says Connected, css is visible
#                                                         (up to 40 s) and f.mjs ran: export default async (page, expect) => ..;
#                                                         prints the path, the final URL and any page errors
#   scripts/ctl.sh [-p port] logs [lines=50]              the tail of the run's log
#   scripts/ctl.sh [-p port] down | down all              stop what ctl started; evidence stays in target/ctl/evidence
#
# api, doctor and shot sign in as that user. -p is needed only when more than one run is up. Output is JSON unless it is a log. AXON_CONSUL, AXON_PAXOS, AXON_KEYS and AXON_CLICKHOUSE
# (with _USER, _PASSWORD) pass through to the playground; see crates/axon/guide/dashboard.md.
set -euo pipefail
cd "$(dirname "$0")/.."
runs=target/ctl/runs
evidence=target/ctl/evidence
mkdir -p "$runs" "$evidence"

die() { printf '{"ok":false,"error":%s}\n' "$(jq -Rn --arg m "$*" '$m')" >&2; exit 1; }
command -v jq >/dev/null || { echo '{"ok":false,"error":"jq is required: brew install jq"}' >&2; exit 1; }

alive() { [ -n "${1:-}" ] && kill -0 "$1" 2>/dev/null && ps -p "$1" -o command= | grep -q 'examples/playground'; }

pick() {
    if [ -n "${port:-}" ]; then
        [ -d "$runs/$port" ] || die "no run on port $port; scripts/ctl.sh ls shows the runs"
        return
    fi
    local all; all=$(ls "$runs")
    case $(printf '%s' "$all" | grep -c .) in
        0) die "no run is up; start one with scripts/ctl.sh up" ;;
        1) port=$all ;;
        *) die "several runs are up ($(echo $all)); pass -p <port>" ;;
    esac
}

load() { pick; pid=$(cat "$runs/$port/pid"); base=$(cat "$runs/$port/base"); url="http://127.0.0.1:$port$base"; }

signin() {
    curl -fsS -o /dev/null -c "$runs/$port/cookies" -H "Origin: http://127.0.0.1:$port" \
        --data-urlencode user=admin --data-urlencode "password=$(cat "$runs/$port/password")" "${url}login"
}

# GET <base><path> as admin into last.json and print the status, signing in again when the session has lapsed.
get() {
    local jar="$runs/$port/cookies" status
    fetch() { curl -sS -o "$runs/$port/last.json" -w '%{http_code}' -b "$jar" -c "$jar" "$url$1"; }
    status=$(fetch "$1") || return 1
    if [ "$status" = 401 ]; then
        signin || return 1
        status=$(fetch "$1") || return 1
    fi
    echo "$status"
}

up() {
    local nodes=3 base=/ churn= open= features=dashboard,system-metrics
    while [ $# -gt 0 ]; do
        case $1 in
            --base) base=$2; shift ;;
            --churn) churn=churn ;;
            --open) open=1 ;;
            [0-9]*) nodes=$1 ;;
            *) die "unknown argument $1; usage: up [nodes] [--base /p/] [--churn] [--open]" ;;
        esac
        shift
    done
    [ -n "${AXON_CLICKHOUSE:-}" ] && features=$features,clickhouse
    cargo build -q -p axon-obs --features "$features" --example playground >&2
    port=8100
    while nc -z 127.0.0.1 "$port" 2>/dev/null || [ -e "$runs/$port" ]; do port=$((port + 1)); done
    mkdir -p "$runs/$port"
    echo "$base" >"$runs/$port/base"
    (umask 077 && { [ -n "$open" ] || { od -An -tx1 -N12 /dev/urandom | tr -d " \n"; echo; }; } >"$runs/$port/password")
    if [ -n "$open" ]; then unset AXON_DASHBOARD_PASSWORD; else export AXON_DASHBOARD_PASSWORD="$(cat "$runs/$port/password")"; fi
    nohup "${CARGO_TARGET_DIR:-target}/debug/examples/playground" "$nodes" "$port" "$base" $churn >"$runs/$port/log" 2>&1 &
    echo $! >"$runs/$port/pid"
    load
    failed() {
        alive "$pid" && kill -9 "$pid"
        mv "$runs/$port/log" "$evidence/up-$port.log"; rm -rf "${runs:?}/$port"
        die "$1; its log is $evidence/up-$port.log"
    }
    for _ in $(seq 1 60); do
        alive "$pid" || failed "playground exited at start"
        if [ "$(get api/overview 2>/dev/null)" = 200 ]; then
            jq -n --argjson port "$port" --argjson pid "$pid" --arg url "$url" --arg log "$runs/$port/log" \
                --arg password "$(cat "$runs/$port/password")" \
                '{ok: true, port: $port, pid: $pid, url: $url, user: "admin", password: $password, log: $log}'
            return
        fi
        sleep 1
    done
    failed "no /api/overview within 60 s"
}

ls_runs() {
    local p out=()
    for p in $(ls "$runs"); do
        out+=("$(jq -n --argjson port "$p" --argjson pid "$(cat "$runs/$p/pid")" --arg base "$(cat "$runs/$p/base")" \
            --argjson alive "$(alive "$(cat "$runs/$p/pid")" && echo true || echo false)" '{port: $port, pid: $pid, base: $base, alive: $alive}')")
    done
    printf '%s\n' "${out[@]:-}" | jq -s .
}

doctor() {
    load
    local up=false owns=false overview=false total=0 reachable=0 body
    alive "$pid" && up=true
    # lsof on macOS, ss where lsof is missing (a fresh Ubuntu box)
    { lsof -nP -a -p "$pid" -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1 ||
        ss -Hltnp "sport = :$port" 2>/dev/null | grep -q "pid=$pid,"; } && owns=true
    if [ "$(get api/overview 2>/dev/null)" = 200 ]; then
        body=$(cat "$runs/$port/last.json")
        overview=true
        total=$(jq '.nodes | length' <<<"$body")
        reachable=$(jq '[.nodes[] | select(.reachable)] | length' <<<"$body")
    fi
    body=$(jq -nc --argjson port "$port" --argjson up $up --argjson owns $owns --argjson overview $overview \
        --argjson total "$total" --argjson reachable "$reachable" \
        '{ok: ($up and $owns and $overview and $reachable > 0 and $reachable == $total), port: $port,
          process: $up, owns_port: $owns, overview: $overview, nodes: $total, reachable: $reachable}
         | if .ok then . else . + {fix: "scripts/ctl.sh -p \(.port) logs; if the cause is not yours, down and up again"} end')
    echo "$body"
    jq -e .ok <<<"$body" >/dev/null
}

api() {
    load
    [ $# -eq 1 ] || die "usage: api <path?query>, e.g. api overview, api 'inventory?limit=5'"
    local status
    status=$(get "api/${1#/}") || die "no answer from $url; run doctor"
    cat "$runs/$port/last.json"; echo
    [ "$status" = 200 ] || die "HTTP $status from api/$1 (body above); routes are listed in crates/axon/guide/dashboard.md#http"
}

shot() {
    load
    [ $# -ge 1 ] || die "usage: shot <route> [--wait css] [--steps f.mjs] [--out f.png], e.g. shot streams --wait '[data-flow]'"
    local route=${1#/} wait= steps= out= password
    shift
    while [ $# -gt 0 ]; do
        case $1 in
            --wait) wait=$2; shift ;;
            --steps) steps=$(cd "$(dirname "$2")" && pwd)/$(basename "$2"); [ -f "$steps" ] || die "no steps file $2"; shift ;;
            --out) out=$2; shift ;;
            *) die "unknown argument $1; usage: shot <route> [--wait css] [--steps f.mjs] [--out f.png]" ;;
        esac
        shift
    done
    out=${out:-$evidence/$(echo "$route" | tr '/?=&' '----')-$(date +%H%M%S).png}
    out=$(cd "$(dirname "$out")" && pwd)/$(basename "$out")
    password=$(cat "$runs/$port/password")
    (cd crates/axon-obs/ui && BASE="$url" PASSWORD="$password" URL="$url$route" OUT="$out" WAIT="$wait" STEPS="$steps" node --input-type=module -e '
        import { chromium, expect } from "@playwright/test";
        const browser = await chromium.launch();
        const page = await browser.newPage({ viewport: { width: 1280, height: 800 } });
        const errors = [];
        page.on("console", (m) => m.type() === "error" && errors.push(m.text()));
        page.on("pageerror", (e) => errors.push(String(e)));
        let failure = null;
        try {
            const form = { user: "admin", password: process.env.PASSWORD };
            const origin = new URL(process.env.BASE).origin;
            if (process.env.PASSWORD) {
                const signed = await page.request.post(`${process.env.BASE}login`, { form, headers: { Origin: origin } });
                if (!signed.ok()) throw new Error(`sign-in answered ${signed.status()}`);
            }
            await page.goto(process.env.URL);
            await page.getByRole("button", { name: /^Connected, updated/ }).waitFor({ timeout: 15000 });
            if (process.env.WAIT) await page.locator(process.env.WAIT).first().waitFor({ timeout: 40000 });
            if (process.env.STEPS) await (await import(process.env.STEPS)).default(page, expect);
            await page.waitForTimeout(1000);
        } catch (e) {
            failure = e.message.split("\n")[0];
        }
        await page.screenshot({ path: process.env.OUT, fullPage: true });
        const ok = !failure && errors.length === 0;
        console.log(JSON.stringify({ ok, path: process.env.OUT, url: page.url(), ...(failure && { error: failure }), errors }));
        await browser.close();
        process.exit(ok ? 0 : 1);
    ') || die "the screenshot above shows the page as it was; check the route and --wait selector against .claude/skills/verify-axon/features, or run npm ci in crates/axon-obs/ui if Playwright is missing"
}

logs() { load; tail -n "${1:-50}" "$runs/$port/log"; }

down() {
    local ports p pid
    if [ "${1:-}" = all ]; then ports=$(ls "$runs"); else pick; ports=$port; fi
    for p in $ports; do
        pid=$(cat "$runs/$p/pid")
        # It may exit on its own between the check and the kill.
        if alive "$pid"; then kill "$pid" 2>/dev/null || true; for _ in $(seq 1 20); do alive "$pid" || break; sleep 0.25; done; fi
        if alive "$pid"; then kill -9 "$pid" 2>/dev/null || true; fi
        rm -rf "${runs:?}/$p"
    done
    jq -n --arg stopped "$(echo $ports)" --arg evidence "$evidence" '{ok: true, stopped: ($stopped | split(" ") | map(select(. != "") | tonumber)), evidence: $evidence}'
}

port=
if [ "${1:-}" = -p ]; then port=${2:?-p needs a port}; shift 2; fi
cmd=${1:-help}
shift || true
case $cmd in
    up|doctor|api|shot|logs|down) "$cmd" "$@" ;;
    ls) ls_runs ;;
    *) sed -n '2,/^set /{/^#/p;}' "$0"; exit 2 ;;
esac
