#!/usr/bin/env python3
"""Polls every node's Health port from outside it (spec/fault-rig.md § Workloads): /readyz and /healthz every second,
/metrics every 10 s, each with a timeout under the period, one thread per node. run.sh runs it on b1 for the whole run.

    probe.py <out.jsonl> <ip>..    one line per request on b1's clock: {t, node, path, sent, code, ms}, and
                                   `checks` of a probe's JSON body, `err` when nothing came back (a timeout is no code,
                                   never a 503), `counters` of a scrape, its counter and histogram-count series
    probe.py selftest

Python 3.9, stdlib only. HEALTH_PORT (7401) is the port.
"""
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request

PORT = int(os.environ.get("HEALTH_PORT", 7401))
TIMEOUT = 0.8
lock = threading.Lock()


def now_us():
    return time.time_ns() // 1000


def counters(text):
    """The series of each `counter` family, and each histogram's `_count`, by name and labels: what only goes up."""
    kinds, out = {}, {}
    for line in text.splitlines():
        if line.startswith("# TYPE "):
            _, _, name, kind = line.split(None, 3)
            kinds[name] = kind
        elif line and not line.startswith("#"):
            series, _, value = line.rpartition(" ")
            name = series.split("{", 1)[0]
            if kinds.get(name) == "counter" or name.endswith("_count") and kinds.get(name[:-6]) == "histogram":
                out[series] = float(value)
    return out


def get(ip, path):
    sent = now_us()
    x = {"path": path, "sent": sent, "code": None}
    try:
        with urllib.request.urlopen("http://%s:%d%s" % (ip, PORT, path), timeout=TIMEOUT) as r:
            x["code"], body = r.status, r.read()
    except urllib.error.HTTPError as e:
        x["code"], body = e.code, e.read()
    except Exception as e:  # refused, reset or timed out: no answer
        x["err"], body = type(e).__name__ + ": " + str(getattr(e, "reason", e))[:80], b""
    x["t"] = now_us()
    x["ms"] = (x["t"] - sent) / 1000
    if x["code"] is not None and path != "/metrics":
        try:
            x["checks"] = json.loads(body).get("checks")
        except (ValueError, AttributeError):  # not JSON, or not an object: a thread that raised would stop probing
            x["checks"] = None
    elif x["code"] == 200:
        x["counters"] = counters(body.decode(errors="replace"))
    return x


def poll(node, ip, out):
    i = 0
    while True:
        at = time.monotonic()
        for path in ("/readyz", "/healthz") + (("/metrics",) if i % 10 == 0 else ()):
            x = dict(get(ip, path), node=node)
            with lock:
                out.write(json.dumps(x) + "\n")
                out.flush()
        i += 1
        time.sleep(max(0.0, 1 - (time.monotonic() - at)))


def selftest():
    text = ("# TYPE axon_a_total counter\naxon_a_total{x=\"1\"} 5\n# TYPE axon_g gauge\naxon_g 2\n"
            "# TYPE axon_h_seconds histogram\naxon_h_seconds_bucket{le=\"+Inf\"} 3\naxon_h_seconds_sum 0.5\n"
            "axon_h_seconds_count 3\n")
    got = counters(text)
    assert got == {'axon_a_total{x="1"}': 5.0, "axon_h_seconds_count": 3.0}, got
    x = get("127.0.0.1", "/readyz")  # nothing listens on the Health port here, or something that answers
    assert (x["code"] is None) == ("err" in x), x
    print("selftest PASS")


def main():
    if sys.argv[1:2] == ["selftest"]:
        return selftest()
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    out = open(sys.argv[1], "a")  # each line flushed whole: run.sh's SIGTERM loses none
    for i, ip in enumerate(sys.argv[2:]):
        threading.Thread(target=poll, args=("b%d" % (i + 1), ip, out)).start()


if __name__ == "__main__":
    main()
