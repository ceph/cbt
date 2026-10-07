"""Unit tests for the Workloads class"""

# pyright: strict, reportPrivateUsage=false
#
# We are OK to ignore private use in unit tests as the whole point of the tests
# is to validate the functions contained in the module

import unittest
from typing import Any, Optional
from unittest.mock import MagicMock, patch

from workloads.workload import Workload
from workloads.workloads import Workloads


class TestWorkloads(unittest.TestCase):
    """Tests for the Workloads class"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        self.base_run_directory = "/tmp/test_run/"
        self.benchmark_configuration: dict[str, Any] = {
            "time": "300",
            "ramp": "30",
            "mode": "randwrite",
            "iodepth": ["4", "8"],
            "numjobs": ["1", "2"],
            "workloads": {
                "workload1": {
                    "mode": "randread",
                    "iodepth": ["16"],
                },
                "workload2": {
                    "mode": "randwrite",
                    "numjobs": ["4"],
                },
            },
        }

    def _create_workloads(self, configuration: Optional[dict[str, Any]] = None) -> Workloads:
        """Helper to create a Workloads instance"""
        if configuration is None:
            configuration = self.benchmark_configuration
        return Workloads(configuration, self.base_run_directory)

    def test_workloads_initialization(self) -> None:
        """Test that Workloads initializes correctly"""
        workloads: Workloads = self._create_workloads()

        self.assertIsNotNone(workloads)
        self.assertTrue(workloads.exist())
        self.assertEqual(len(workloads._workloads), 2)

    def test_workloads_initialization_no_workloads(self) -> None:
        """Test initialization when no workloads are defined"""
        config: dict[str, Any] = {
            "time": "300",
            "mode": "randwrite",
        }
        workloads: Workloads = self._create_workloads(config)

        self.assertIsNotNone(workloads)
        self.assertFalse(workloads.exist())
        self.assertEqual(len(workloads._workloads), 0)

    def test_exist_with_workloads(self) -> None:
        """Test exist() returns True when workloads are defined"""
        workloads: Workloads = self._create_workloads()

        self.assertTrue(workloads.exist())

    def test_exist_without_workloads(self) -> None:
        """Test exist() returns False when no workloads are defined"""
        config: dict[str, Any] = {"time": "300"}
        workloads: Workloads = self._create_workloads(config)

        self.assertFalse(workloads.exist())

    def test_get_names(self) -> None:
        """Test getting workload names"""
        workloads: Workloads = self._create_workloads()

        names = workloads.get_names()

        self.assertIn("workload1", names)
        self.assertIn("workload2", names)

    def test_set_benchmark_type(self) -> None:
        """Test setting the benchmark type"""
        workloads: Workloads = self._create_workloads()
        benchmark_type = "fio"

        workloads.set_benchmark_type(benchmark_type)

        self.assertEqual(workloads._benchmark_type, benchmark_type)

    def test_set_executable(self) -> None:
        """Test setting the executable path"""
        workloads: Workloads = self._create_workloads()
        executable_path = "/usr/bin/fio"

        workloads.set_executable(executable_path)

        self.assertEqual(workloads._executable, executable_path)

    def test_get_base_run_directory(self) -> None:
        """Test getting the base run directory"""
        workloads: Workloads = self._create_workloads()

        base_dir: str = workloads.get_base_run_directory()

        self.assertEqual(base_dir, self.base_run_directory)

    def test_get_global_options_from_configuration(self) -> None:
        """Test extracting global options from configuration"""
        workloads: Workloads = self._create_workloads()

        # Global options should include everything except 'workloads' and 'prefill'
        self.assertIn("time", workloads._global_options)
        self.assertIn("ramp", workloads._global_options)
        self.assertIn("mode", workloads._global_options)
        self.assertNotIn("workloads", workloads._global_options)

    def test_get_global_options_excludes_prefill(self) -> None:
        """Test that prefill is excluded from global options"""
        config: dict[str, Any] = {
            "time": "300",
            "prefill": {"blocksize": "4M"},
            "workloads": {
                "test": {"mode": "randwrite"},
            },
        }
        workloads: Workloads = self._create_workloads(config)

        self.assertNotIn("prefill", workloads._global_options)

    @patch("workloads.workloads.pdsh")
    @patch("workloads.workloads.make_remote_dir")
    @patch("workloads.workloads.monitoring")
    @patch("workloads.workloads.getnodes")
    @patch("workloads.workloads.sleep")
    def test_run_with_workloads(
        self,
        mock_sleep: MagicMock,
        mock_getnodes: MagicMock,
        mock_monitoring: MagicMock,
        mock_make_remote_dir: MagicMock,
        mock_pdsh: MagicMock,
    ) -> None:
        """Test running workloads"""
        mock_getnodes.return_value = "client1,client2"
        mock_process: MagicMock = MagicMock()
        mock_pdsh.return_value = mock_process

        workloads: Workloads = self._create_workloads()
        workloads.set_benchmark_type("rbdfio")
        workloads.set_executable("/usr/bin/fio")

        workloads.run()

        # Verify monitoring was started and stopped
        self.assertTrue(mock_monitoring.start.called)
        self.assertTrue(mock_monitoring.stop.called)

    @patch("workloads.workloads.pdsh")
    @patch("workloads.workloads.make_remote_dir")
    @patch("workloads.workloads.monitoring")
    @patch("workloads.workloads.getnodes")
    def test_run_with_script(
        self,
        mock_getnodes: MagicMock,
        mock_monitoring: MagicMock,
        mock_make_remote_dir: MagicMock,
        mock_pdsh: MagicMock,
    ) -> None:
        """Test running workloads with pre_workload_script"""
        mock_getnodes.return_value = "client1"
        mock_process: MagicMock = MagicMock()
        mock_pdsh.return_value = mock_process

        config: dict[str, Any] = {
            "time": "300",
            "workloads": {
                "test_workload": {
                    "mode": "randwrite",
                    "pre_workload_script": "/path/to/script.sh",
                },
            },
        }

        workloads: Workloads = self._create_workloads(config)
        workloads.set_benchmark_type("rbdfio")
        workloads.set_executable("/usr/bin/fio")

        workloads.run()

        # Verify script was executed
        script_calls = [call_args for call_args in mock_pdsh.call_args_list if "/path/to/script.sh" in str(call_args)]
        self.assertGreater(len(script_calls), 0)

    def test_run_without_benchmark_type(self) -> None:
        """Test that run() handles missing benchmark type gracefully"""
        workloads: Workloads = self._create_workloads()
        workloads.set_executable("/usr/bin/fio")

        # Should not raise an exception, just log an error
        workloads.run()

    def test_run_without_executable(self) -> None:
        """Test that run() handles missing executable gracefully"""
        workloads: Workloads = self._create_workloads()
        workloads.set_benchmark_type("rbdfio")

        # Should not raise an exception, just log an error
        workloads.run()

    def test_run_without_workloads(self) -> None:
        """Test that run() handles no workloads gracefully"""
        config: dict[str, Any] = {"time": "300"}
        workloads: Workloads = self._create_workloads(config)
        workloads.set_benchmark_type("rbdfio")
        workloads.set_executable("/usr/bin/fio")

        # Should not raise an exception, just log an error
        workloads.run()

    @patch("workloads.workloads.pdsh")
    @patch("workloads.workloads.make_remote_dir")
    @patch("workloads.workloads.monitoring")
    @patch("workloads.workloads.getnodes")
    @patch("workloads.workloads.sleep")
    def test_run_with_ramp_time(
        self,
        mock_sleep: MagicMock,
        mock_getnodes: MagicMock,
        mock_monitoring: MagicMock,
        mock_make_remote_dir: MagicMock,
        mock_pdsh: MagicMock,
    ) -> None:
        """Test running workloads with ramp time"""
        mock_getnodes.return_value = "client1"
        mock_process: MagicMock = MagicMock()
        mock_pdsh.return_value = mock_process

        config: dict[str, Any] = {
            "time": "300",
            "ramp": "30",
            "workloads": {
                "test": {"mode": "randwrite"},
            },
        }

        workloads: Workloads = self._create_workloads(config)
        workloads.set_benchmark_type("rbdfio")
        workloads.set_executable("/usr/bin/fio")

        workloads.run()

        # Verify sleep was called with ramp time
        mock_sleep.assert_called_with(30)

    def test_create_configurations(self) -> None:
        """Test that workload configurations are created correctly"""
        workloads: Workloads = self._create_workloads()

        # Should have created 2 workload objects
        self.assertEqual(len(workloads._workloads), 2)

        # Check workload names
        workload_names: list[str] = [w.get_name() for w in workloads._workloads]
        self.assertIn("workload1", workload_names)
        self.assertIn("workload2", workload_names)

    def test_workload_inherits_global_options(self) -> None:
        """Test that workloads inherit global options"""
        workloads: Workloads = self._create_workloads()

        # Each workload should have access to global options
        for workload in workloads._workloads:
            # Global options should be available
            self.assertIn("time", workload._all_options)
            self.assertEqual(workload._all_options["time"], "300")

    def test_workload_local_options_override_global(self) -> None:
        """Test that local workload options override global options"""
        workloads: Workloads = self._create_workloads()

        # Find workload1 which has mode: randread (overriding global randwrite)
        workload1: Workload = next(w for w in workloads._workloads if w.get_name() == "workload1")

        # Local mode should override global mode
        self.assertEqual(workload1._all_options["mode"], "randread")

    def test_list_conversion_in_global_options(self) -> None:
        """Test that list values in configuration are preserved"""
        workloads: Workloads = self._create_workloads()

        # iodepth is a list in the configuration
        self.assertIsInstance(workloads._global_options["iodepth"], list)
        self.assertEqual(workloads._global_options["iodepth"], ["4", "8"])

    def test_string_conversion_in_global_options(self) -> None:
        """Test that non-list values are converted to strings"""
        config: dict[str, Any] = {
            "time": 300,  # int
            "ramp": 30,  # int
            "workloads": {
                "test": {"mode": "randwrite"},
            },
        }
        workloads: Workloads = self._create_workloads(config)

        # Should be converted to strings
        self.assertIsInstance(workloads._global_options["time"], str)
        self.assertEqual(workloads._global_options["time"], "300")


if __name__ == "__main__":
    unittest.main()

# Made with Bob
