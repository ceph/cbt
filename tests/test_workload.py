"""Unit tests for the Workload class"""

# pyright: strict, reportPrivateUsage=false
#
# We are OK to ignore private use in unit tests as the whole point of the tests
# is to validate the functions contained in the module

import unittest
from typing import Optional
from unittest.mock import MagicMock, patch

from command.command import Command
from workloads.workload import Workload
from workloads.workload_types import WorkloadType


class TestWorkload(unittest.TestCase):
    """Tests for the Workload class"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        self.workload_name = "test_workload"
        self.base_run_directory = "/tmp/test_run/"
        self.workload_options: WorkloadType = {
            "mode": "randwrite",
            "iodepth": ["4", "8"],
            "numjobs": ["1", "2"],
            "op_size": ["4096"],
            "volumes_per_client": "1",
        }

    def _create_workload(self, options: Optional[WorkloadType] = None) -> Workload:
        """Helper to create a Workload instance"""
        if options is None:
            options = self.workload_options
        return Workload(self.workload_name, options, self.base_run_directory)

    def test_workload_initialization(self) -> None:
        """Test that a Workload initializes correctly"""
        workload = self._create_workload()

        self.assertEqual(workload.get_name(), self.workload_name)
        self.assertIsNotNone(workload)
        self.assertFalse(workload.has_script())

    def test_workload_with_script(self) -> None:
        """Test workload with pre_workload_script"""
        options = self.workload_options.copy()
        options["pre_workload_script"] = "/path/to/script.sh"
        workload = self._create_workload(options)

        self.assertTrue(workload.has_script())
        self.assertEqual(workload.get_script_command(), "/path/to/script.sh")

    def test_workload_without_script(self) -> None:
        """Test workload without pre_workload_script"""
        workload = self._create_workload()

        self.assertFalse(workload.has_script())
        self.assertIsNone(workload.get_script_command())

    def test_set_executable(self) -> None:
        """Test setting the executable path"""
        workload = self._create_workload()
        executable_path = "/usr/bin/fio"

        workload.set_executable(executable_path)
        self.assertEqual(workload._executable_path, executable_path)

    def test_set_benchmark_type_rbdfio(self) -> None:
        """Test setting benchmark type to rbdfio"""
        workload = self._create_workload()
        workload.set_benchmark_type("rbdfio")

        self.assertEqual(workload._parent_benchmark_type, "rbdfio")

    def test_set_benchmark_type_fio(self) -> None:
        """Test setting benchmark type to fio"""
        workload = self._create_workload()
        workload.set_benchmark_type("fio")

        self.assertEqual(workload._parent_benchmark_type, "fio")

    def test_add_global_options(self) -> None:
        """Test adding global options to workload"""
        workload = self._create_workload()
        global_options: WorkloadType = {
            "time": "300",
            "ramp": "30",
        }

        workload.add_global_options(global_options)

        self.assertEqual(workload._all_options["time"], "300")
        self.assertEqual(workload._all_options["ramp"], "30")

    def test_add_global_options_no_override(self) -> None:
        """Test that global options don't override existing options"""
        workload = self._create_workload()
        original_mode = workload._all_options["mode"]

        global_options: WorkloadType = {
            "mode": "randread",  # Try to override
            "time": "300",
        }

        workload.add_global_options(global_options)

        # Original mode should be preserved
        self.assertEqual(workload._all_options["mode"], original_mode)
        # New option should be added
        self.assertEqual(workload._all_options["time"], "300")

    @patch("workloads.workload.all_configs")
    def test_get_output_directories(self, mock_all_configs: MagicMock) -> None:
        """Test getting output directories"""
        # Mock all_configs to return a single configuration
        mock_all_configs.return_value = [
            {
                "mode": "randwrite",
                "iodepth": "4",
                "numjobs": "1",
                "op_size": "4096",
                "volumes_per_client": "1",
                "target_number": "0",
            }
        ]

        workload = self._create_workload()
        workload.set_benchmark_type("rbdfio")
        workload.set_executable("/usr/bin/fio")

        directories = list(workload.get_output_directories())

        self.assertGreater(len(directories), 0)
        for directory in directories:
            self.assertIsInstance(directory, str)
            self.assertIn(self.workload_name, directory)

    def test_get_iodepth_key_default(self) -> None:
        """Test getting iodepth key when only iodepth is present"""
        workload = self._create_workload()
        configuration_keys = ["mode", "iodepth", "numjobs"]

        iodepth_key = workload._get_iodepth_key(configuration_keys)

        self.assertEqual(iodepth_key, "iodepth")

    def test_get_iodepth_key_total(self) -> None:
        """Test getting iodepth key when total_iodepth is present"""
        workload = self._create_workload()
        configuration_keys = ["mode", "iodepth", "total_iodepth", "numjobs"]

        iodepth_key = workload._get_iodepth_key(configuration_keys)

        self.assertEqual(iodepth_key, "total_iodepth")

    def test_calculate_iodepth_per_target_equal_distribution(self) -> None:
        """Test calculating iodepth per target with equal distribution"""
        workload = self._create_workload()
        number_of_targets = 4
        total_iodepth = 16

        result = workload._calculate_iodepth_per_target_from_total_iodepth(number_of_targets, total_iodepth)

        self.assertEqual(len(result), number_of_targets)
        self.assertEqual(sum(result.values()), total_iodepth)
        for iodepth in result.values():
            self.assertEqual(iodepth, 4)

    def test_calculate_iodepth_per_target_unequal_distribution(self) -> None:
        """Test calculating iodepth per target with unequal distribution"""
        workload = self._create_workload()
        number_of_targets = 3
        total_iodepth = 10

        result = workload._calculate_iodepth_per_target_from_total_iodepth(number_of_targets, total_iodepth)

        self.assertEqual(len(result), number_of_targets)
        self.assertEqual(sum(result.values()), total_iodepth)
        # Should distribute as evenly as possible: 4, 3, 3
        self.assertIn(4, result.values())
        self.assertEqual(list(result.values()).count(3), 2)

    def test_calculate_iodepth_per_target_insufficient_iodepth(self) -> None:
        """Test calculating iodepth when total is less than number of targets"""
        workload = self._create_workload()
        number_of_targets = 10
        total_iodepth = 5

        result = workload._calculate_iodepth_per_target_from_total_iodepth(number_of_targets, total_iodepth)

        # Should reduce number of targets to match iodepth
        self.assertEqual(len(result), total_iodepth)
        self.assertEqual(sum(result.values()), total_iodepth)
        for iodepth in result.values():
            self.assertEqual(iodepth, 1)

    def test_set_iodepth_for_every_target(self) -> None:
        """Test setting same iodepth for all targets"""
        workload = self._create_workload()
        number_of_targets = 5
        iodepth = 8

        result = workload._set_iodepth_for_every_target(number_of_targets, iodepth)

        self.assertEqual(len(result), number_of_targets)
        for target_iodepth in result.values():
            self.assertEqual(target_iodepth, iodepth)

    def test_create_command_class_rbdfio(self) -> None:
        """Test creating RbdFioCommand for rbdfio benchmark"""
        workload = self._create_workload()
        workload.set_benchmark_type("rbdfio")

        options = {
            "mode": "randwrite",
            "iodepth": "4",
            "numjobs": "1",
            "target_number": "0",
            "name": self.workload_name,
        }

        command = workload._create_command_class(options)

        self.assertIsNotNone(command)
        self.assertIsInstance(command, Command)

    def test_create_command_class_unsupported(self) -> None:
        """Test creating command for unsupported benchmark type"""
        workload = self._create_workload()
        workload.set_benchmark_type("unsupported_benchmark")

        options = {
            "mode": "randwrite",
            "iodepth": "4",
            "numjobs": "1",
            "target_number": "0",
            "name": self.workload_name,
        }

        with self.assertRaises(NotImplementedError):
            workload._create_command_class(options)

    def test_workload_str_representation(self) -> None:
        """Test string representation of workload"""
        workload = self._create_workload()

        str_repr = str(workload)

        self.assertIn(self.workload_name, str_repr)
        self.assertIn("Name:", str_repr)


if __name__ == "__main__":
    unittest.main()

# Made with Bob
