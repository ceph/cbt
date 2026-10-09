# CBT Repository Structure

Git-tracked source files only. Excludes `__init__.py`, images, JS/CSS/TeX assets, and untracked/ignored local files.

```
cbt/
├── cbt.py                          # entry point — parses config, drives the run
├── benchmarkfactory.py             # maps benchmark name → benchmark class
├── client_endpoints_factory.py     # maps endpoint name → client_endpoints class
├── settings.py                     # global settings object (singleton)
├── cbt_types.py                    # shared type aliases
├── common.py                       # shared utilities (pdsh, async SSH wrappers, paths)
├── cli_options.py                  # argparse CLI definition
├── logging_configuration.py        # logging setup
├── compare.py                      # result comparison helpers
├── plot_results.py                  # standalone plot-results script
├── progress.py                     # progress bar / ETA tracking
├── statistic.py                    # statistical helpers
├── requirements.txt
├── tox.ini                         # test/lint environments (pytest, ruff, mypy)
├── run_tests.sh
├── setup.sh
│
├── benchmark/                      # one module per benchmark tool
│   ├── benchmark.py                # abstract Benchmark base class
│   ├── elbencho.py                 # Elbencho (file/S3) benchmark
│   ├── fio.py
│   ├── rbdfio.py
│   ├── kvmrbdfio.py
│   ├── librbdfio.py
│   ├── rawfio.py
│   ├── radosbench.py
│   ├── cephtestrados.py
│   ├── cosbench.py
│   ├── hsbench.py
│   ├── getput.py
│   ├── nullbench.py
│   └── lis.py
│
├── command/                        # CLI command builders (separate from benchmark logic)
│   ├── command.py                  # abstract Command base class
│   ├── elbencho_command.py         # builds elbencho CLI invocations
│   ├── fio_command.py
│   └── rbd_fio_command.py
│
├── workloads/                      # workload iteration (iodepth/bs/threads sweeps)
│   ├── workload.py                 # Workload base class + sweep logic
│   ├── workloads.py                # Workloads collection / run orchestration
│   └── workload_types.py           # type aliases for workload config dicts
│
├── cluster/
│   ├── cluster.py                  # abstract Cluster base class
│   └── ceph.py                     # Ceph cluster lifecycle (start/stop/dump logs)
│
├── client_endpoints/               # storage access layer per protocol
│   ├── client_endpoints.py         # abstract base
│   ├── ceph_client_endpoints.py
│   ├── rgws3_client_endpoints.py   # RGW/S3 endpoint
│   ├── cephfsfuse_client_endpoints.py
│   ├── cephfskernel_client_endpoints.py
│   ├── librbd_client_endpoints.py
│   ├── rbdfuse_client_endpoints.py
│   ├── rbdkernel_client_endpoints.py
│   ├── rbdnbd_client_endpoints.py
│   └── rbdtcmu_client_endpoints.py
│
├── remote/                         # remote execution layer
│   ├── remote_executor.py          # RemoteExecutor base + pdsh impl
│   └── async_ssh.py                # asyncio SSH executor (pdsh-free)
│
├── monitoring/                     # in-run monitoring plugins
│   ├── monitoring.py               # abstract base
│   ├── monitoring_factory.py
│   ├── collectl_monitoring.py
│   ├── blktrace_monitoring.py
│   ├── perf_monitoring.py
│   └── top_monitoring.py
│
├── parsing/                        # legacy HTML/DB result parsing
│   ├── database.py
│   └── htmlgenerator.py
│
├── post_processing/                # result post-processing pipeline
│   ├── common.py
│   ├── post_processing_types.py
│   ├── report.py
│   ├── formatter/
│   │   ├── base_formatter.py
│   │   ├── common_output_formatter.py
│   │   └── time_series_output_formatter.py
│   ├── parsers/
│   │   ├── fio_log_parser.py
│   │   ├── fio_time_series_parser.py
│   │   └── timestamp_aligner.py
│   ├── plotter/                    # ~15 plotter modules (io, cpu, memory, time-series…)
│   ├── reports/
│   │   ├── report_generator.py
│   │   ├── simple_report_generator.py
│   │   ├── comparison_report_generator.py
│   │   └── time_series_report_generator.py
│   └── run_results/
│       ├── run_result.py
│       ├── run_result_factory.py
│       ├── benchmark_result.py
│       ├── resource_result.py
│       ├── resource_result_factory.py
│       ├── benchmarks/fio.py
│       └── resources/              # collectl_resource, fio_resource, top_resource
│
├── tests/                          # pytest suite — mirrors src layout
│   ├── test_bm_<name>.py           # benchmark unit tests (one per benchmark)
│   ├── test_elbencho_command.py
│   ├── test_workload.py
│   ├── test_workloads.py
│   ├── test_async_ssh.py
│   ├── test_monitoring_*.py
│   └── test_post_processing_*.py / test_*_plotter.py / test_*_formatter.py …
│
├── tools/                          # standalone offline analysis scripts (not imported)
│   ├── config_wizard.py
│   ├── serialise_benchmark.py
│   ├── crimson/                    # Crimson OSD-specific bench helpers
│   ├── fio_objectstore_tools/
│   ├── fio_visualize_data/
│   └── fio-parse-jsons/
│
├── example/                        # reference YAML configs for various setups
│   └── wip-elbencho/elbencho_ex.yaml
│
├── docs/
│   ├── dev/coding-guidelines.md    # ← read before writing any code (see AGENTS.md)
│   ├── workloads/elbencho-s3.md    # Elbencho S3 feature doc
│   └── *.md                        # design/release/integration docs
│
└── .agents/
    └── skills/
        ├── writing-unit-tests/SKILL.md
        └── parallel-review-changes/SKILL.md
```
