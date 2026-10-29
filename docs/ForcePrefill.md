# Forcing a prefill

Some benchmarks skip their `prefill()` step under certain settings:

| Benchmark   | Prefill skipped when                              |
|-------------|---------------------------------------------------|
| `librbdfio` | `use_existing_volumes: True`                      |
| `fio`       | `prefill: False` (default is `True`)              |
| `hsbench`   | `prefill` not set (default is `False`)            |

Set `force_prefill: True` in the benchmark section of the test plan to run the
benchmark's `prefill()` regardless of those settings. The default is `False`, which
leaves the behaviour above unchanged.

`force_prefill` is a benchmark-level option. It is not passed through to the I/O
exerciser command line, and is ignored in `workloads` sections.

## Example

Prefill pre-existing RBD images before running a `librbdfio` workload:

```yaml
benchmarks:
  librbdfio:
    use_existing_volumes: True
    force_prefill: True
    poolname: 'mypool'
    rbdname: 'myimage'
    volumes_per_client: 2
    vol_size: 65536
    prefill:
      blocksize: '4M'
      numjobs: '1'
    mode: 'randread'
    time: 300
```

Expected result: `cbt.py` logs `Prefilling volume 1/2: myimage-<client fqdn>-0` (and `2/2` for
the second volume) during the initialize phase, then `Prefill complete.`, before the benchmark runs.
Without `force_prefill: True` no prefill happens for this configuration.

## Limitation

`cbt.py` only calls `prefill()` when `rebuild_every_test` is not set in the `cluster` section, so
`force_prefill` has no effect when `rebuild_every_test: True`.
