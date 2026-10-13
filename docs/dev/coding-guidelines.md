# Coding guidelines

Conventions and review expectations for writing or reviewing Python in this repo. These are
distilled from recurring code-review feedback and the maintainers' own commits — follow them
*before* asking for review, not after. They apply repo-wide, not just to one benchmark.

## Toolchain

### Python version

The project targets **Python 3.9**. Your development virtual environment must use Python 3.9.

**With `uv` (recommended):**

```bash
uv venv --python 3.9 ceph.venv
source ceph.venv/bin/activate
uv pip install -e ".[dev]"   # installs runtime + dev deps from pyproject.toml
```

**With plain venv:**

```bash
python3.9 -m venv ceph.venv
source ceph.venv/bin/activate
pip install -e ".[dev]"
```

> `pyproject.toml` is the single source of truth for both runtime and dev dependencies.
> `requirements.txt` exists as a legacy file but is superseded by `pyproject.toml` — do not add
> new dependencies there.

### Running the tools

Activate the venv and set `PYTHONPATH` before running any tool:

```bash
source ceph.venv/bin/activate
export PYTHONPATH=/path/to/cbt
```

Then, on **changed files only**:

```bash
black --config pyproject.toml <changed files>         # format
ruff check --config pyproject.toml <changed files>    # lint
mypy --config-file pyproject.toml <changed files>     # type-check
pylint --rcfile pyproject.toml <changed file>         # score ≥ 10.0; one invocation per file
pytest -c pyproject.toml tests/                       # full test suite
```

- **Black** — formatter, 120-char lines. Run in format mode before committing.
- **Ruff** — linter (replaces flake8). Rule sets: `E/F/I/UP/N/B/PIE/RUF`. `E501` is suppressed
  (Black owns line length).
- **MyPy** — strict mode. See the mypy override strata note in the Typing section below.
- **Pylint** — score must be `≥ 10.0`. Run individually per changed file, not as a glob.
- **Pytest** — `python3 -m pytest`, never bare `python`. Uses `testpaths = ["tests"]`,
  verbose, `--exitfirst`.

---

## Typing

- **Strict typing, no loosening.** Fully type every parameter, return value, and local.
  Subtype containers (`dict[str, str]`, not bare `dict`).
- **Avoid `Any`** when possible. mypy silently skips it, so it hides bugs rather than catching them. Some
  existing modules lean on `Any` and `# type: ignore` — that is NOT a style to copy or extend.
- **Reuse shared type aliases.** They live in a layer-scoped `*_types.py` module
  (e.g. `workloads/workload_types.py`, `post_processing/post_processing_types.py`), PascalCase,
  with no pylint disables. Types shared across layers go in the top-level `cbt_types.py`.
  Don't reintroduce ad-hoc inline aliases.
- **Option values reach command classes as strings, always.** The workloads layer stringifies
  every option value (and every element of a list value) before it reaches a `Command`:
  booleans become `"true"`/`"false"` (lowercased), ints become their string form. The bool
  check *must precede* the int check, because `bool` is a subclass of `int`.
  - In command classes, compare against string literals, never `bool(...)`: use
    `options.get("time_based", "false") == "true"` or `!= "false"`, not `bool(options.get(...))`.
- **Do not add modules to the mypy override blocks in `pyproject.toml`.** Two legacy strata
  exist: `ignore_errors = true` (fully untyped legacy modules such as `common`, `settings`,
  `cluster.*`) and `disallow_untyped_calls/defs = false` (the fio family, partially typed).
  New code must pass strict mypy without overrides. If you hit a mypy error, fix the typing —
  don't suppress it.

## Code shape

- **No `else` after `return`** — use guard clauses and early returns.
- **`yield from`** over a manual `for x in …: yield x` — it's clearer and avoids an
  unnecessary loop variable.
- **No functions nested inside functions** — use module-level helpers instead. Nested functions
  are invisible to tests, can't be reused, and are harder to read in isolation.

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
- **Real instantiation over constructor stubs.** Do not mock class constructors (`__init__`) or
  rely solely on `settings.mock_initialize()`. Instantiate benchmark, command, and monitoring classes
  via their real public interface with concrete test configurations so path construction (`run_dir`,
  `out_dir`) and parameter validations are exercised.
- **Golden CLI tests and negative path coverage.** Mock only the remote boundary (SSH/pdsh) and
  assert the *exact* CLI command strings and output directory paths generated. Cover error conditions
  (missing parameters, invalid types, unsupported template placeholders raising `ValueError`) rather
  than testing only happy paths with defaults.
- **Use descriptive assertion messages.**
- **Run the full tool suite before pushing** — see the Toolchain section for the exact commands.
  Confirm no regressions in shared-code callers.

## Working with the codebase

- **Tool economy.** Prefer targeted symbol lookups, grep, and line-range reads over dumping
  or reading entire large files.

## Commits & contributions

- Subject line: `component: Sentence-case summary`. The body explains *why*, not what.
- `Signed-off-by:` ([DCO](https://developercertificate.org/)) on every commit (`git commit -s`),
  plus the `Assisted-by: LLM` trailer added manually to the commit body.
- **New dependencies only if added to `pyproject.toml`** (both `dependencies` for runtime, `[dependency-groups] dev` for dev-only).
- **User-visible changes ship with docs** — a concrete, point-to-point example (test-plan YAML
  plus expected output snapshot, not just prose). Internal refactors, test-only changes, and
  private API changes are exempt.
