#!/usr/bin/env bash
# The rolling upgrade's plant: the v2 build's schema 2 reads each op's amount negated, so the record a v2 node writes
# for a v2 caller is not the amount its client sent (`phantom`). Only rig-v2 has it; the roll puts it in the cluster.
. "$(dirname "$0")/lib.sh"
plant examples/rig/src/account.rs "decode_op(&body, '|', reply)" \
    "decode_op(&body, '|', reply).map(|m| match m { Msg::Op(id, d, r) => Msg::Op(id, -d, r), m => m })"
