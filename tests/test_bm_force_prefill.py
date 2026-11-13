"""Unit tests for the force_prefill option shared by the benchmarks that implement prefill()"""

import unittest
from typing import Any
from unittest.mock import MagicMock, patch

import benchmarkfactory
import settings
from cluster.ceph import Ceph
from workloads.workloads import Workloads

INVARIANT_YAML = "tools/invariant.yaml"
ARCHIVE_DIR = "/tmp"
HSBENCH_ENDPOINT: dict[str, str] = {"secret_key": "secret", "access_key": "access", "url": "http://localhost:7480"}


class ForcePrefillTestBase(unittest.TestCase):
    """Common bootstrap: mock settings and cluster once, build benchmarks via the factory"""

    cluster: Any

    @classmethod
    def setUpClass(cls) -> None:
        settings.mock_initialize(config_file=INVARIANT_YAML)  # type: ignore[no-untyped-call]
        cls.cluster = Ceph.mockinit(settings.cluster)  # type: ignore[no-untyped-call]

    def _make_benchmark(self, name: str, **options: Any) -> Any:
        config: dict[str, Any] = {"iteration": 0}
        config.update(options)
        benchmark = benchmarkfactory.get_object(  # type: ignore[no-untyped-call]
            ARCHIVE_DIR, self.cluster, name, config
        )
        self.assertIsNotNone(benchmark, f"{name} benchmark should be created by the factory")
        return benchmark


class TestForcePrefillOption(ForcePrefillTestBase):
    """force_prefill is consumed by the base Benchmark so every benchmark sees it"""

    def test_force_prefill_defaults_to_false(self) -> None:
        """force_prefill is off unless requested"""
        benchmark = self._make_benchmark("librbdfio")

        self.assertFalse(benchmark.force_prefill, "force_prefill should default to False")

    def test_force_prefill_true_is_read_from_config(self) -> None:
        """force_prefill: True in the yaml is exposed on the benchmark"""
        benchmark = self._make_benchmark("fio", force_prefill=True)

        self.assertTrue(benchmark.force_prefill, "force_prefill: True should be consumed")

    def test_force_prefill_is_not_a_workload_global_option(self) -> None:
        """force_prefill is a Benchmark option, so it must not leak into the I/O exerciser command options"""
        config: dict[str, Any] = {"time": "30", "force_prefill": True, "workloads": {"w": {"mode": "randwrite"}}}
        workloads = Workloads(config, "/tmp/")

        # The global options are only exposed via the private attribute; tests/test_workloads.py does the same.
        global_options = workloads._global_options  # pylint: disable=protected-access

        self.assertNotIn("force_prefill", global_options, "force_prefill must be excluded from globals")
        self.assertIn("time", global_options, "ordinary options must still be global")


class TestLibrbdFioForcePrefill(ForcePrefillTestBase):
    """force_prefill overrides use_existing_volumes for librbdfio"""

    @patch("benchmark.librbdfio.common.pdsh")
    def test_use_existing_volumes_skips_prefill_by_default(self, mock_pdsh: MagicMock) -> None:
        """Existing behaviour: use_existing_volumes alone does not prefill"""
        benchmark = self._make_benchmark("librbdfio", use_existing_volumes=True, volumes_per_client=2)

        benchmark.prefill()

        mock_pdsh.assert_not_called()

    @patch("benchmark.librbdfio.common.pdsh")
    def test_force_prefill_runs_prefill_with_use_existing_volumes(self, mock_pdsh: MagicMock) -> None:
        """force_prefill: one fio write command per volume despite use_existing_volumes"""
        benchmark = self._make_benchmark(
            "librbdfio", use_existing_volumes=True, force_prefill=True, volumes_per_client=2
        )

        benchmark.prefill()

        self.assertEqual(mock_pdsh.call_count, 2, "Expected one prefill command per volume")
        for call in mock_pdsh.call_args_list:
            self.assertIn("--rw=write", call.args[1], "Prefill command should be a fio write")

    @patch("benchmark.librbdfio.common.pdsh")
    def test_force_prefill_uses_configured_rbdname(self, mock_pdsh: MagicMock) -> None:
        """A forced prefill targets the user's existing rbd image names"""
        benchmark = self._make_benchmark(
            "librbdfio", use_existing_volumes=True, force_prefill=True, rbdname="myimage", poolname="mypool"
        )

        benchmark.prefill()

        command: str = mock_pdsh.call_args_list[0].args[1]
        self.assertIn("--rbdname=myimage-", command, "Prefill should use the configured rbdname prefix")
        self.assertIn("--pool=mypool", command, "Prefill should use the configured pool")

    @patch("benchmark.librbdfio.common.pdsh")
    def test_prefill_runs_when_volumes_not_existing(self, mock_pdsh: MagicMock) -> None:
        """Existing behaviour: prefill runs when CBT creates the volumes"""
        benchmark = self._make_benchmark("librbdfio", volumes_per_client=2)

        benchmark.prefill()

        self.assertEqual(mock_pdsh.call_count, 2, "Expected one prefill command per volume")

    @patch("benchmark.librbdfio.common.pdsh")
    def test_force_prefill_false_with_existing_volumes_skips_prefill(self, mock_pdsh: MagicMock) -> None:
        """An explicit force_prefill: False behaves like the default"""
        benchmark = self._make_benchmark("librbdfio", use_existing_volumes=True, force_prefill=False)

        benchmark.prefill()

        mock_pdsh.assert_not_called()


class TestFioForcePrefill(ForcePrefillTestBase):
    """force_prefill overrides prefill: False for fio"""

    def _make_fio(self, **options: Any) -> Any:
        benchmark = self._make_benchmark("fio", **options)
        # Normally populated by initialize_endpoints(), which needs a real cluster.
        benchmark.endpoint_type = "directory"
        benchmark.endpoints_per_client = 1
        benchmark.endpoints = ["/tmp/fio-data"]
        return benchmark

    @patch("benchmark.fio.common.pdsh")
    def test_prefill_false_skips_prefill(self, mock_pdsh: MagicMock) -> None:
        """Existing behaviour: prefill: False disables the prefill"""
        benchmark = self._make_fio(prefill=False)

        benchmark.prefill()

        mock_pdsh.assert_not_called()

    @patch("benchmark.fio.common.pdsh")
    def test_force_prefill_overrides_prefill_false(self, mock_pdsh: MagicMock) -> None:
        """force_prefill: True runs the prefill even with prefill: False"""
        benchmark = self._make_fio(prefill=False, force_prefill=True)

        benchmark.prefill()

        self.assertEqual(mock_pdsh.call_count, 1, "Expected one prefill command for the single endpoint")
        self.assertIn("--rw=write", mock_pdsh.call_args.args[1], "Prefill command should be a fio write")

    @patch("benchmark.fio.common.pdsh")
    def test_prefill_runs_by_default(self, mock_pdsh: MagicMock) -> None:
        """Existing behaviour: fio prefills by default"""
        benchmark = self._make_fio()

        benchmark.prefill()

        self.assertEqual(mock_pdsh.call_count, 1, "Expected one prefill command for the single endpoint")


class TestHsbenchForcePrefill(ForcePrefillTestBase):
    """force_prefill overrides the default prefill: False for hsbench"""

    def _make_hsbench(self, **options: Any) -> Any:
        benchmark = self._make_benchmark("hsbench", **options)
        # Normally populated by initialize_endpoints(), which needs a real cluster.
        benchmark.endpoints_per_client = 1
        benchmark.endpoints = [HSBENCH_ENDPOINT]
        return benchmark

    @patch("benchmark.hsbench.common.pdsh")
    def test_prefill_skipped_by_default(self, mock_pdsh: MagicMock) -> None:
        """Existing behaviour: hsbench does not prefill unless asked"""
        benchmark = self._make_hsbench()

        benchmark.prefill()

        mock_pdsh.assert_not_called()

    @patch("benchmark.hsbench.common.pdsh")
    def test_force_prefill_runs_prefill(self, mock_pdsh: MagicMock) -> None:
        """force_prefill: True runs the prefill even though prefill defaults to False"""
        benchmark = self._make_hsbench(force_prefill=True)

        benchmark.prefill()

        self.assertEqual(mock_pdsh.call_count, 1, "Expected one prefill command for the single endpoint")
        self.assertIn(".prefill", mock_pdsh.call_args.args[1], "Prefill output files should carry the .prefill suffix")

    @patch("benchmark.hsbench.common.pdsh")
    def test_prefill_true_still_runs_prefill(self, mock_pdsh: MagicMock) -> None:
        """Existing behaviour: prefill: True runs the prefill"""
        benchmark = self._make_hsbench(prefill=True)

        benchmark.prefill()

        self.assertEqual(mock_pdsh.call_count, 1, "Expected one prefill command for the single endpoint")


if __name__ == "__main__":
    unittest.main()
