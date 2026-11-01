@AGENTS.md

Before working on axon itself, read `crates/axon/README.md`: it maps each part to its guide.

Habits for Claude sessions here, beyond AGENTS.md:

- Before committing a change, unasked: /simplify, then /code-review high --fix, and fix what they find.
- Commit and push only when asked. "Merge" is the whole path without asking again: the review above, the gates,
  merge to main, push origin, delete the branch local and remote. A lane the user approved merges once its gates pass.
- Prove console-visible changes with the verify-axon skill. Before merging a wire, ordering or API change, run
  blast-radius.
- A console change, a subagent's too, is seen in the browser before it is handed over: click what changed, watch for
  layout shift and flicker. Console text is short and in ASD-STE100; an explanation goes in the page's help dialog or
  a tooltip, never as prose under a heading. A first draft is plain: few boxes, no uppercase, no decoration.
- In chat, plain words. Say what a name is the first time (never a bare D7, W18 or "lane 3"), and the file and the
  symptom before the fix. A question gives each option in one sentence, a pick and what it costs.
