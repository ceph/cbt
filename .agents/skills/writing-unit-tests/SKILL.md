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

- **pytest** driving **`unittest.TestCase`** classes. Tests live in `tests/test_*.py`.
- No conftest and **no `sys.path` block** — tox runs with `usedevelop`, so imports
  (`import settings`, `from benchmark.elbencho import Elbencho`) resolve from the repo root.
- Naming: `class Test<Thing><Concern>(unittest.TestCase)`; methods
  `test_<behaviour>` (e.g. `test_missing_s3_bucket_raises`), not `test_<method_called>`.

## Bootstrap (the CBT-specific fixture)

`settings` is global module state, and a `Ceph` cluster normally needs SSH. Both
have mock entrypoints — stand them up once in `setUpClass`:

```python
import settings, benchmarkfactory
from cluster.ceph import Ceph

@classmethod
def setUpClass(cls):
    settings.mock_initialize(config_file="tools/invariant.yaml")
    cls.cluster = Ceph.mockinit(settings.cluster)
```

Build a benchmark with `benchmarkfactory.get_object(archive_dir, cluster, name, config)`.
`config` is the parsed YAML block as a dict.

## The one boundary to mock: remote execution

Everything else is pure Python — test it with real objects. Only the SSH fan-out
touches the outside world, and which layer you patch depends on the benchmark:

- **Workloads-based benchmarks** (e.g. elbencho) go through the executor. Patch
  `AsyncSSHExecutor` methods: `run_command`, `make_remote_dir`, `clean_remote_dir`
  via `@patch.object(AsyncSSHExecutor, "run_command")`.
- **Legacy benchmarks & monitoring** go through `common.pdsh` (and `pdcp`/`scp`).
  Patch those.

Mock only the boundary; never patch internal CBT functions or isolated parts of the
code, thereby simply testing happy-paths. Unit tests should be meaningful and not 
add technical debt without real reason.

## What to cover

1. **Command builders (`command/*.py`)** — assert the **exact** command string and
   the run-directory path. Include negatives: optional flags omitted when unset,
   blocksize/parse edge cases, such as arguments in formats that aren't allowed.
2. **Validation/error paths** — bad input raises a clear `ValueError` naming the key.
3. **Run-loop fan-out** — the number of run cells matches the `all_configs`
   permutation matrix (list-valued params → cartesian product).
4. **Regressions** — write the failing test first, then the fix.
5. **System boundaries** - unit tests should be meaningful by aiming to not care about
   *how* code achieves what it does--they should care about the functionality of the
   code instead by asserting desired outcomes and not copying apparent effects.

## Running

```bash
python3 -m pytest tests/test_bm_elbencho.py            # one file
python3 -m pytest tests/test_bm_elbencho.py::TestRunLoop::test_scalar_threads_and_iodepth
tox                                                     # full gate: pytest + flake8
```

`python3` always — never bare `python`. flake8 ignores `E501`. Confirm zero new
failures before reporting done.
