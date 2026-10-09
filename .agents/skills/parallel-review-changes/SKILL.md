---
name: parallel-review-changes
description: >-
  Runs a one-shot, multi-reviewer code review of some set of changes — a branch,
  a working tree, or a PR. Invoke it on demand whenever a review is requested,
  or once just after opening a new pull request. Fans out three reviewer subagents 
  over the diff, then makes an independent judgment about which of their findings
  are real, useful findings. Reports the findings appropriately depending on 
  requirements.
---

# Review Orchestrator

Review whatever the user asks of you. If the scope isn't clear, inspect the 
current working state/branch as a starting point. If you're reviewing a PR 
you just opened, there's nothing to check out; review the branch as
it currently stands.

Don't over-review: one orchestrated review per set of changes is enough. If you
already ran this review on these changes, don't run it again on the same
changes.

## Step 1 — Fan out three reviewers

Launch **three** reviewer subagents in a single message so they run in parallel.
Give them the **same base goal** — review the diff against the base branch and
report only important issues (correctness/logic bugs, security holes, data-loss
or corruption risks, concurrency hazards, broken error handling, API/contract
breaks, missing tests for risky changes). Minor findings should be omitted.

Vary their *emphasis* (general purpose should be constant) slightly, e.g.:

- Reviewer A — general with emphasis on internal correctness & logic
- Reviewer B — general with emphasis on security, testing & data integrity
- Reviewer C — general with emphasis on broader codebase consistency & interface contracts

For reviewer C, in particular, order it to ensure that the DRY principle is carried out.
Only non-minor violations of the principle should be reported. 

Each reviewer reads the diff of the required changes and returns a short list: each finding as
`file:line` — description — severity — suggested fix.

## Step 2 — Decide what's worth the author's time

AI reviewers can over-report: they flag things that aren't real problems. Your job is
not to just relay all the subagents' findings — it's to make an **independent decision** 
about what genuinely belongs in front of a person who will spend their time going through it.

Weigh several things, not one:

- **Cross-validation** — a finding independently raised by more than one reviewer
  is more likely real. Strong signal.
- **Is it actually a problem?** You often know more than the reviewers do — due to
  your broader context about the task at hand — so use that in addition to selective
  skepticism to confirm bold issues some reviewer(s) surfaced and to discard
  ones that aren't.

Drop everything that doesn't clear that bar. If there's nothing important to say, say 
so plainly rather than forcing a set of points.


Write the final review report in concise yet precise language that's easy to understand
without unnecessary technical jargon. Do not use emojis or symbols in the response.

## Step 3 — Emit the surviving findings

Output the surviving findings, ordered by severity. Where they go depends on how
you were invoked:

- **Reviewing a just-opened PR** — post your concluding points in a single PR comment.
  One comment, then stop. Attribute it to yourself — an AI agent — for transparency.
  Write a brief sentence about the review process (three parallel sub-agents, cross-verified).
- **Any other review** — simply report it back to the user and depending on the context,
  ask the user whether they want it posted/formatted in a particular way.