---
name: executing-plans
description: Use when executing a multi-step plan or task list — implementing a
  plan document, working through a checklist, or deciding whether a plan step is
  done. Covers pairing steps with test-backed acceptance criteria and when a step
  may be marked complete.
---

# Executing plans and tasks

## Pair every step with an acceptance criterion

Each plan step must have a test-backed or observable acceptance criterion, e.g.
asserting the exact command string, the path format, or the error raised. If a
step has none, write one before starting the step.

## Never mark a step complete unverified

Do not mark a plan step or task complete until you have run the test or check
that proves it, and seen it pass. "The code looks right" is not verification.
