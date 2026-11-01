#!/usr/bin/env bash
# Heavy work (builds, check.sh, sims, benches) runs on a dev box, not the laptop. BOX=user@host names it, or without
# it the one line in ~/.config/axon/box, so an agent's command needs no VAR= prefix.
#
#   scripts/box.sh sync <dir>           copy this tree to ~/axon/<dir> on the box, without target/ or .git
#   scripts/box.sh run <dir> <cmd..>    run cmd in ~/axon/<dir> with cargo, sccache if present, and target dir ~/axon/<dir>-target
#
# BOX_ROOT=<name> puts them under ~/<name> instead, so a box shared with other projects keeps axon in one place.
#
# sync compares checksums: rsync -a keeps the laptop's mtimes, and cargo then skips edits older than its last build.
# run fails within a minute of a dropped connection instead of hanging; a job that must outlive one runs under
# nohup setsid on the box, from a script file synced with the tree (the worktree guard refuses an inline sh -c), and is
# polled from its log.
set -euo pipefail
here=$(git rev-parse --show-toplevel 2>/dev/null || pwd -P)
cd "$(dirname "$0")/.."
box=${BOX:-$(cat ~/.config/axon/box 2>/dev/null || true)}
[[ -n $box ]] || { echo "set BOX=user@host, or put the default box in ~/.config/axon/box" >&2; exit 2; }
dir=${2:?usage: scripts/box.sh sync|run <dir> ..}
root=${BOX_ROOT:-axon}
case $dir/$root in */*/*|.*|*/.*) echo "dir and BOX_ROOT are plain names under ~ on the box" >&2; exit 2 ;; esac

case $1 in
    sync) [[ $(pwd -P) == $(cd "$here" && pwd -P) ]] ||   # another checkout's box.sh would sync that checkout
              { echo "box.sh syncs the tree it is in, $(pwd -P), not $here: run $here/scripts/box.sh" >&2; exit 2; }
          ssh "$box" "mkdir -p ~/$root"
          rsync -rlpcz --delete --exclude target/ --exclude /target --exclude .git --exclude node_modules/ --exclude /.env --exclude /.claude/worktrees/ ./ "$box:$root/$dir/" ;;
    run)  shift 2
          # CARGO_INCREMENTAL stays unset: the workspace's own crates then build incrementally, as the dev profile does,
          # and sccache caches the dependencies, passing ours through. Debug info is line tables only: backtraces keep
          # their lines, links and target dirs shrink. ~/.config/box-slots caps the runs at once (scripts/slot.sh).
          # The tree's target/ is a link to <dir>-target, not CARGO_TARGET_DIR: sccache keys on CARGO_ variables, so a
          # different one per tree gave no tree a hit from another's dependencies.
          ssh -o ServerAliveInterval=15 -o ServerAliveCountMax=4 "$box" "source ~/.cargo/env; command -v sccache >/dev/null && export RUSTC_WRAPPER=sccache;
                      export CARGO_PROFILE_DEV_DEBUG=line-tables-only CARGO_PROFILE_TEST_DEBUG=line-tables-only;
                      cd ~/$root/$dir && mkdir -p ../$dir-target && { [ -L target ] || [ ! -e target ] ||
                          { echo 'box.sh: $dir/target is a directory, not the link to $dir-target: move what is in it there' >&2; exit 2; }; } &&
                      ln -sfn ../$dir-target target && . scripts/slot.sh && $*" ;;
    *)    sed -n '/^#   /p' "$0"; exit 2 ;;
esac
