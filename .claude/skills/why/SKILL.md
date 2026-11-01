---
name: why
description: Find out why code in this repo is shaped the way it is, from git history, the spec, the docs and the benchmarks, with every claim labelled by how well it is supported. Use for "why does X work this way", "why did we pick Y", "where does this number come from", before changing a design decision, and when a regression's origin matters.
---

# Why

Code shows what it does, not why. The why lives in commits, `spec/`, `crates/axon/guide/benchmarks.md`, comments, tests and earlier
sessions. All of these are incomplete. The job is to find what was actually written down, and to say plainly where it
runs out.

## 1. Pin the target

Name the files, line ranges and symbols, and restate the question in one line. If the target is vague, choose the most
likely reading from the conversation, say which reading you chose, and go on.

## 2. Search every source, and record what each returned

Run these in parallel. Use subagents when the history is long.

- **Git.** `git blame -L a,b <file>`, `git log --follow -p -- <file>`, and the pickaxe for a constant or name that
  moved: `git log -S'<literal>' --oneline`. Read the full message of every commit that shaped the lines. Commit
  subjects here name the area ("Runtime:", "Cluster:", "Dashboard:") and bodies often carry the reason.
- **GitHub.** `gh pr list --search <sha>` and `gh pr view <n> --json title,body,comments,reviews`, if the commit came
  through a PR.
- **The spec.** `spec/` holds the requirements and the design reasoning (`spec/00-overview-and-requirements.md`,
  `spec/seed-discovery.md`). Grep for the symbol and for the concept.
- **Docs and numbers.** The guide for the area (AGENTS.md table), and `crates/axon/guide/benchmarks.md` for any threshold,
  capacity or batch size.
- **Tests.** The test that fails if the behaviour changes often names the reason in its title or its assertion message.
- **Earlier sessions.** Search the Claude Code transcripts for this project, and memory, when the decision was made
  with an agent.

A source that returns nothing is a finding. Say what you searched and with which terms.

## 3. Label every claim

- **Direct.** Someone wrote the reason down: a commit body, the spec, a comment, a benchmark note. Cite it.
- **Supported.** Several indirect signals agree, such as the commit, the test added with it and the numbers. Name each one.
- **Inferred.** Your reading of the context. Show the chain ("given A and B, C is likely because D").
- **Speculative.** A plausible story with thin evidence. Put it next to the alternatives.
- **Unknown.** You looked and found nothing. List where you looked.

Use "because" and "was designed to" only for Direct and Supported claims. The latest commit is not the whole story,
because the current shape usually builds up over several of them, so trace back.

## 4. Answer

- The question and the code in question.
- What the record says (Direct and Supported, with citations).
- What can reasonably be inferred, and the competing explanations.
- What is not known, and where you searched.
- If the question comes before a change, list what to **preserve**, what can **change**, what to **avoid**, and the
  **risks**. Hand a risky change to the **blast-radius** skill.
