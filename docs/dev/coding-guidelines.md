# Coding guidelines

Conventions and review expectations for writing or reviewing Python in this repo. These are
distilled from recurring code-review feedback and the maintainers' own commits — follow them
*before* asking for review, not after. They apply repo-wide, not just to one benchmark.

## Typing

- **Strict typing, no loosening.** Fully type every parameter, return value, and local.
  Subtype containers (`dict[str, str]`, not bare `dict`).
- **Avoid `Any`** when possible. mypy silently skips it, so it hides bugs rather than catching them. Some
  existing modules lean on `Any` and `# type: ignore` — that is NOT a style to copy or extend.
- **Reuse shared type aliases.** They live in a dedicated module (e.g.
  `workloads/workload_types.py`), PascalCase, with no pylint disables. Don't reintroduce ad-hoc
  inline aliases.
- **Option values reach command classes as strings, always.** The workloads layer stringifies
  every option value (and every element of a list value) before it reaches a `Command`:
  booleans become `"true"`/`"false"` (lowercased), ints become their string form. The bool
  check *must precede* the int check, because `bool` is a subclass of `int`.
  - In command classes, compare against string literals, never `bool(...)`: use
    `options.get("time_based", "false") == "true"` or `!= "false"`, not `bool(options.get(...))`.

## Code shape

- **No `else` after `return`** — use guard clauses and early returns.
- **`yield from`** over a manual `for x in …: yield x`.
- **No functions nested inside functions** — use module-level helpers; they read better.

## Architecture & separation of concerns

- **One responsibility per layer:**
  - command construction → `command/*.py` (subclasses of `Command`)
  - workload iteration / option merging → `workloads/`
  - transport (SSH / exec) → `remote/`

  Command-line building belongs in `command/*.py`, **not** in a benchmark module or the
  Workloads runner. (See the docs/Workloads.md for the background of the difference between
  legacy benchmark modules and Workloads runners)
- **DRY.** Reuse shared logic (e.g. the iodepth-split in `workloads/workload.py`); do not copy
  it into a benchmark. If you find an existing duplicate, prefer consolidating over adding a
  third copy.
- **Concrete shared behaviour goes in base classes**, inherited rather than reimplemented. Always
  look for base class functionalities before writing new code to avoid the risk of re-writing existing
  material.
- **When a second implementation appears, refactor to base class + subclasses** instead of
  branching with conditionals.
- **When you change a shared seam, sweep every caller.** A change to `workloads/` (or any shared
  module) must stay behaviour-preserving for *all* benchmarks, not just the one you're working
  on.

## Validation & error handling

- **Validate and convert numeric inputs defensively** — wrap `int(...)` conversions in
  try/except and raise a clear `ValueError` naming the offending key.
- **Fail pre-flight on non-zero remote status** — Reuse the `RemoteExecutor` error-checking path
  (`run_command_with_error_checking` / `continue_if_error=False`); don't rely on a bare
  `communicate()`.

## Testing

- **New or changed behaviour ships with unit tests in the same PR.**
- **Before writing new code, write unit tests that test the functionality you want**, then run
  the test to ensure it fails, then write the code to pass them.
- **Test command building and error conditions.** Mock/patch the remote calls and assert the
  *exact* command string and directory path produced. Do **not** test redundant happy paths.
- **Use descriptive assertion messages.**
- **Run `python3 -m pytest` on the touched test file and the full `tox` (pytest + flake8) before
  pushing.** Confirm no regressions in shared-code callers.

## Commits & contributions

- Subject line: `component: Sentence-case summary`. The body explains *why*, not what.
- `Signed-off-by:` (DCO) on every commit, plus the `Assisted-by: LLM` trailer.
- **New dependencies only if added to `requirements.txt`.**
- **Docs ship with a concrete, point-to-point example** — a test-plan YAML plus an expected
  output snapshot, not just prose.
