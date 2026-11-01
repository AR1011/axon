#!/usr/bin/env bash
# After an edit to a .rs file: rustfmt it, then clippy its crate as check.sh does, less dead_code and unused_imports.
# A half-done change always has those, and under one crate's features its workspace dependencies can too, which would
# stop the build before this crate; check.sh still denies them. Only the edited file's findings come back as an error;
# the crate's other files, often a later step of the same change, come as context.
file=$(jq -r '.tool_input.file_path // empty')
[[ $file == *.rs && -f $file ]] || exit 0
cd "$(dirname "$file")" || exit 0
manifest=$(cargo locate-project --message-format plain 2>/dev/null) || exit 0
root=$(cargo locate-project --workspace --message-format plain 2>/dev/null) || exit 0
out=$(rustfmt --edition 2024 "$file" 2>&1) || { echo "rustfmt $file:"$'\n'"$out" >&2; exit 2; }
out=$(cargo clippy -q --manifest-path "$manifest" --all-features --all-targets --message-format short -- \
    -D warnings -A dead_code -A unused_imports 2>&1) && exit 0
# A toolchain or workspace problem is not the edit's fault.
grep -qE "requires rustc|believes it's in a workspace" <<<"$out" && exit 0
rel=$(realpath "$file")
rel=${rel#"$(dirname "$(realpath "$root")")"/}
mine=$(grep -F "$rel:" <<<"$out")
[[ -n $mine ]] && { echo "clippy $rel:"$'\n'"$(head -n 40 <<<"$mine")" >&2; exit 2; }
jq -n --arg c "clippy, other files of $manifest (maybe a later step of this change):"$'\n'"$(tail -n 40 <<<"$out")" \
    '{hookSpecificOutput: {hookEventName: "PostToolUse", additionalContext: $c}}'
