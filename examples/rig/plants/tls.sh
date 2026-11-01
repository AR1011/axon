#!/usr/bin/env bash
# TLS mode's plant: a SIGHUP logs the pass but sets nothing, so every node keeps the certificates it booted with, and
# one restarted after the second pass boots with CA b's leaf, which the others do not trust (check.py
# handshake-quiet, probe-quiet, and the accounts' liveness). Needs a restart class in the plan.
. "$(dirname "$0")/lib.sh"
plant examples/rig/src/main.rs 'certs.reload(&cluster, &discovery, seed.as_ref(), &sh),' \
    'sh.node_log.line("tls", json!({"pass": certs.load().ok().map(|l| l.0), "err": null, "at": "hup"})),'
