#!/usr/bin/python3
import argparse
import collections
import logging
import pprint
import sys

import benchmarkfactory
import progress
import settings
from benchmark.benchmark import Benchmark
from cluster.ceph import Ceph
from logging_configuration import setup_loggers

logger = logging.getLogger("cbt")
# Uncomment this if further debug detail (module, funcname) are needed
# FORMAT = "%(asctime)s] [%(levelname)s] [%(name)s] [%(funcName)s():%(lineno)s] %(message)s"
# logging.basicConfig(format=FORMAT, force=True)
# logger.setLevel(logging.DEBUG)


def parse_args(args):
    parser = argparse.ArgumentParser(description="Continuously run ceph tests.")
    parser.add_argument(
        "-a",
        "--archive",
        required=True,
        help="Directory where the results should be archived.",
    )

    parser.add_argument(
        "-c",
        "--conf",
        required=False,
        help="The ceph.conf file to use.",
    )

    parser.add_argument(
        "--no-progress",
        action="store_true",
        default=False,
        help="Disable CLI progress bars (implied when stdout is not a TTY or is_teuthology is set).",
    )

    parser.add_argument(
        "config_file",
        help="YAML config file.",
    )

    return parser.parse_args(args[1:])


def _compute_total_duration(benchmarks_list: list[Benchmark], rebuild_every_test: bool) -> int:
    """Return the estimated total wall-clock seconds for the whole CBT run.

    Includes:
    * cluster.initialize() cost (once, or once per benchmark when rebuild_every_test)
    * per-benchmark estimate_duration() for all non-skipped benchmarks

    Args:
        benchmarks_list:      Pre-materialised list of Benchmark objects.
        rebuild_every_test:   Whether the cluster is rebuilt for each benchmark.

    Returns:
        Estimated run time in seconds.
    """
    cluster_init_secs: int = progress.cluster_init_estimate()
    # When rebuild_every_test is False, one cluster init happens upfront.
    # When True, the init happens inside the per-benchmark loop below instead.
    total: int = 0 if rebuild_every_test else cluster_init_secs

    for b in benchmarks_list:
        if rebuild_every_test:
            total += cluster_init_secs
        total += b.estimate_duration()

    return max(total, 1)


def main(argv):
    # Set up console-only logging early so any startup errors are visible
    setup_loggers()
    ctx = parse_args(argv)
    settings.initialize(ctx)

    # Now that archive_dir is known, add the file handler
    archive_dir = settings.cluster.get("archive_dir")
    setup_loggers(logfile_name=f"{archive_dir}/cbt.log")

    # Initialise progress reporting (after settings so is_teuthology is readable)
    progress.setup(no_progress=ctx.no_progress)

    logger.debug("Settings.cluster:\n    %s", pprint.pformat(settings.cluster).replace("\n", "\n    "))

    global_init = collections.OrderedDict()
    rebuild_every_test = settings.cluster.get("rebuild_every_test", False)

    # FIXME: Create ClusterFactory and parametrically match benchmarks and clusters.
    cluster = Ceph(settings.cluster)

    # Materialise the first-iteration benchmarks into a list so we can iterate
    # it multiple times (duration estimate + init + run loops).
    # benchmarkfactory.get_all() is a generator — wrapping in list() here
    # prevents it from being exhausted by _compute_total_duration.
    first_iter_benchmarks: list[Benchmark] = list(benchmarkfactory.get_all(archive_dir, cluster, 0))
    iterations = settings.cluster.get("iterations", 0)
    # Scale estimated duration across all iterations
    total_secs = _compute_total_duration(first_iter_benchmarks, rebuild_every_test) * max(iterations, 1)

    with progress.overall_bar(total_secs) as overall:
        # Only initialize and prefill upfront if we aren't rebuilding for each test.
        if not rebuild_every_test:
            if not cluster.use_existing:
                with progress.phase_bar("Cluster initialize", progress.cluster_init_estimate(), overall):
                    cluster.initialize()
            for iteration in range(iterations):
                benchmarks = (
                    first_iter_benchmarks
                    if iteration == 0
                    else list(benchmarkfactory.get_all(archive_dir, cluster, iteration))
                )
                for b in benchmarks:
                    if b.exists():
                        continue
                    if b.getclass() not in global_init:
                        with progress.phase_bar(
                            f"Initialize {b.getclass()}",
                            overall=overall,
                        ):
                            b.initialize()
                            b.initialize_endpoints()
                            b.prefill()
                            b.cleanup()
                    # Only initialize once per class.
                    global_init[b.getclass()] = b

        # logger.debug("Settings.cluster.is_teuthology:%s",settings.cluster.get('is_teuthology', False))
        # Run the benchmarks
        return_code = 0
        try:
            for iteration in range(iterations):
                benchmarks = (
                    first_iter_benchmarks
                    if iteration == 0
                    else list(benchmarkfactory.get_all(archive_dir, cluster, iteration))
                )
                for b in benchmarks:
                    if not b.exists() and not settings.cluster.get("is_teuthology", False):
                        continue

                    if rebuild_every_test:
                        with progress.phase_bar("Cluster initialize", progress.cluster_init_estimate(), overall):
                            cluster.initialize()
                        b.initialize()

                    # Always try to initialize endpoints before running the test
                    b.initialize_endpoints()
                    logger.info("Running benchmark %s == iteration %d ==", b.getclass(), iteration)
                    run_secs = b.estimate_duration() or None
                    has_workloads = getattr(b, "_workloads", None) and b._workloads.exist()  # type: ignore[union-attr]
                    if has_workloads:
                        # Workload benchmarks drive their own per-param-set phase bars
                        # inside Workloads.run().  Wrapping with an outer phase bar here
                        # would put two bars at position=1 simultaneously, causing them
                        # to swap/flicker.  The inner bars also advance the overall bar
                        # directly via progress.get_overall_bar(), so no outer bar is needed.
                        b.run()
                    else:
                        with progress.phase_bar(f"Run {b.getclass()} iter {iteration}", run_secs, overall):
                            b.run()
                    logger.info("Benchmark %s iteration %d complete.", b.getclass(), iteration)
        except:
            return_code = 1  # FAIL
            logger.exception("During tests")

    return return_code


if __name__ == "__main__":
    exit(main(sys.argv))
