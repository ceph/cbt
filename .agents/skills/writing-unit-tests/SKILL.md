---
name: writing-unit-tests
description: Use when writing or updating unit tests — adding coverage for
  new code, or a regression test for a bug. Covers the pytest/unittest layout, the
  settings+cluster mock bootstrap, which remote-exec boundary to mock per benchmark
  type, what actually needs testing, and how to run the suite.
---

# Writing unit tests for CBT

A good test pins down *behaviour*, not code: assert what a component promises
(its command string, its run-dir path, the error it raises), so it survives a
refactor and goes red only when the contract breaks. See the testing section of
`docs/dev/coding-guidelines.md` for the review bar; this skill is the CBT-specific *how*.

## Framework & layout

- **pytest** driving **`unittest.TestCase`** classes (preferred). Tests live in
  `tests/test_*.py`. Some files use bare `def test_...()` functions; both styles are
  acceptable — choose the class form when shared `setUp`/`setUpClass` state is needed,
  bare functions otherwise.
- No conftest and **no `sys.path` block** — pytest runs with `pythonpath = ["."]` (set
  in `pyproject.toml`), so imports (`import settings`, `from benchmark.elbencho import
  Elbencho`) resolve from the repo root.
- Naming: `class Test<Thing><Concern>(unittest.TestCase)`; methods
  `test_<behaviour>` (e.g. `test_missing_s3_bucket_raises`), not `test_<method_called>`.

## Bootstrap (the CBT-specific fixture)

`settings` is global module state, and a `Ceph` cluster normally needs SSH. Both
have mock entrypoints — stand them up once in `setUpClass`:

```python
import settings
import benchmarkfactory
from cluster.ceph import Ceph

INVARIANT_YAML = "tools/invariant.yaml"

@classmethod
def setUpClass(cls) -> None:
    settings.mock_initialize(config_file=INVARIANT_YAML)  # type: ignore[no-untyped-call]
    cls.cluster = Ceph.mockinit(settings.cluster)          # type: ignore[no-untyped-call, attr-defined]
```

The `# type: ignore` comments are required because `settings` and `Ceph` are in the
legacy `ignore_errors = true` mypy stratum — strict mypy cannot see their signatures.

Build a benchmark with `benchmarkfactory.get_object(archive_dir, cluster, name, config)`.
`config` is the parsed YAML block as a dict.

> **Important**: `settings.mock_initialize()` is solely for setting up global test harness
> state. **Do not** mock out class constructors (`__init__`) or bypass real object instantiation.
> Instantiating objects through their public interface ensures real path generation
> (`run_dir`, `out_dir`) and option validations are genuinely executed during tests.

## The one boundary to mock: remote execution

Everything else is pure Python — test it with real objects. Only the SSH fan-out
touches the outside world, and which layer you patch depends on the benchmark:

- **Workloads-based benchmarks** (e.g. elbencho) go through the executor. Patch
  `AsyncSSHExecutor` methods via `@patch.object`: `run_command`, `make_remote_dir`,
  `clean_remote_dir`, `sync_files`.
- **Legacy benchmarks & monitoring** go through `common.pdsh` (and `pdcp`/`scp`).
  Patch those at their import site (e.g. `patch("monitoring.collectl_monitoring.common.pdsh")`).

Mock only the boundary; never patch internal CBT functions or isolated parts of the
code, thereby simply testing happy-paths. Unit tests should be meaningful and not
add technical debt without real reason.

## What to cover

1. **Golden CLI & Directory Assertions (`tests/test_*_cli.py`)** — assert the **exact**
   command string generated for given configuration inputs, and verify exact `run_dir`
   and `out_dir` path formatting.
2. **Validation & Negative Error Paths** — test error conditions, not just happy paths.
   Verify that missing required keys, invalid parameter types, or unknown template
   placeholders raise descriptive `ValueError` or `KeyError` exceptions naming the offending key.
3. **Run-loop fan-out** — the number of run cells matches the `all_configs`
   permutation matrix (list-valued params → cartesian product).
4. **Regressions** — write the failing test first, then the fix.
5. **System boundaries** — unit tests should be meaningful by aiming to not care about
   *how* code achieves what it does — they should care about the functionality of the
   code instead by asserting desired outcomes and not copying apparent effects.

## Running

```bash
# Run the full suite (use this before pushing)
python3 -m pytest -c pyproject.toml tests/

# Run a single file
python3 -m pytest -c pyproject.toml tests/test_bm_elbencho.py

# Run a single test
python3 -m pytest -c pyproject.toml tests/test_bm_elbencho.py::TestRunLoop::test_scalar_threads_and_iodepth
```

`python3` always — never bare `python`. The toolchain gate is `black`, `ruff`, `mypy`,
and `pylint` on changed files plus `pytest -c pyproject.toml tests/` — **not** `tox`
or `flake8` (ruff has replaced flake8). Confirm zero new failures before reporting done.
