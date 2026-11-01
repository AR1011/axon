# Sourced by each plant, run from the tree's root. `plant FILE ANCHOR REPLACEMENT` puts REPLACEMENT in place of ANCHOR,
# both literal text; `plant FILE ANCHOR` removes the line that holds it. Either fails unless ANCHOR is in FILE once.
set -euo pipefail
plant() {
    python3 - "$@" <<'EOF'
import sys
path, anchor, *new = sys.argv[1:]
text = open(path).read()
if text.count(anchor) != 1:
    sys.exit(f"the plant anchor is not in {path} once: {anchor}")
if new:
    text = text.replace(anchor, new[0])
else:
    at = text.index(anchor)
    start, end = text.rfind("\n", 0, at) + 1, text.find("\n", at) + 1 or len(text)
    text = text[:start] + text[end:]
open(path, "w").write(text)
EOF
}
