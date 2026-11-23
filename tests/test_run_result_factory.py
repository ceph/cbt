"""
Tests for the run_result_factory module.

This module tests the factory function that creates appropriate RunResult
subclasses based on benchmark type.
"""

# pylint: disable=protected-access

import tempfile
from pathlib import Path

import pytest

from post_processing.run_results.rbdfio import RBDFIO
from post_processing.run_results.run_result_factory import (
    BENCHMARK_TYPE_MAP,
    get_run_result_from_directory_name,
)


class TestBenchmarkTypeMap:
    """Test the BENCHMARK_TYPE_MAP constant."""

    def test_map_contains_rbdfio(self) -> None:
        """Test that map contains rbdfio."""
        assert "rbdfio" in BENCHMARK_TYPE_MAP
        assert BENCHMARK_TYPE_MAP["rbdfio"] == RBDFIO

    def test_map_contains_librbdfio(self) -> None:
        """Test that map contains librbdfio."""
        assert "librbdfio" in BENCHMARK_TYPE_MAP
        assert BENCHMARK_TYPE_MAP["librbdfio"] == RBDFIO

    def test_map_contains_kvmrbdfio(self) -> None:
        """Test that map contains kvmrbdfio."""
        assert "kvmrbdfio" in BENCHMARK_TYPE_MAP
        assert BENCHMARK_TYPE_MAP["kvmrbdfio"] == RBDFIO

    def test_map_contains_rawfio(self) -> None:
        """Test that map contains rawfio."""
        assert "rawfio" in BENCHMARK_TYPE_MAP
        assert BENCHMARK_TYPE_MAP["rawfio"] == RBDFIO

    def test_all_map_to_same_class(self) -> None:
        """Test that all FIO variants map to RBDFIO class."""
        fio_variants = ["rbdfio", "librbdfio", "kvmrbdfio", "rawfio"]
        for variant in fio_variants:
            assert BENCHMARK_TYPE_MAP[variant] == RBDFIO


class TestGetRunResultFromDirectoryName:
    """Test the get_run_result_from_directory_name factory function."""

    def test_rbdfio_directory(self) -> None:
        """Test creating RunResult for rbdfio directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir)
            rbdfio_dir = parent_dir / "rbdfio"
            rbdfio_dir.mkdir()

            result = get_run_result_from_directory_name(parent_dir, "json_output")

            assert isinstance(result, RBDFIO)
            assert result.type == "rbdfio"

    def test_librbdfio_directory(self) -> None:
        """Test creating RunResult for librbdfio directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir)
            librbdfio_dir = parent_dir / "librbdfio"
            librbdfio_dir.mkdir()

            result = get_run_result_from_directory_name(parent_dir, "json_output")

            assert isinstance(result, RBDFIO)
            assert result.type == "rbdfio"

    def test_kvmrbdfio_directory(self) -> None:
        """Test creating RunResult for kvmrbdfio directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir)
            kvmrbdfio_dir = parent_dir / "kvmrbdfio"
            kvmrbdfio_dir.mkdir()

            result = get_run_result_from_directory_name(parent_dir, "json_output")

            assert isinstance(result, RBDFIO)
            assert result.type == "rbdfio"

    def test_rawfio_directory(self) -> None:
        """Test creating RunResult for rawfio directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir)
            rawfio_dir = parent_dir / "rawfio"
            rawfio_dir.mkdir()

            result = get_run_result_from_directory_name(parent_dir, "json_output")

            assert isinstance(result, RBDFIO)
            assert result.type == "rbdfio"

    def test_case_insensitive_matching(self) -> None:
        """Test that directory matching is case-insensitive."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir)
            # Create directory with mixed case
            rbdfio_dir = parent_dir / "RbdFio"
            rbdfio_dir.mkdir()

            result = get_run_result_from_directory_name(parent_dir, "json_output")

            assert isinstance(result, RBDFIO)

    def test_directory_with_prefix(self) -> None:
        """Test matching when benchmark type is part of longer directory name."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir)
            # Directory name contains 'rbdfio' as substring
            rbdfio_dir = parent_dir / "test_rbdfio_results"
            rbdfio_dir.mkdir()

            result = get_run_result_from_directory_name(parent_dir, "json_output")

            assert isinstance(result, RBDFIO)

    def test_directory_with_suffix(self) -> None:
        """Test matching when benchmark type has suffix."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir)
            rbdfio_dir = parent_dir / "rbdfio_test"
            rbdfio_dir.mkdir()

            result = get_run_result_from_directory_name(parent_dir, "json_output")

            assert isinstance(result, RBDFIO)

    def test_unknown_benchmark_type_raises_error(self) -> None:
        """Test that unknown benchmark type raises NotImplementedError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir)
            unknown_dir = parent_dir / "unknown_benchmark"
            unknown_dir.mkdir()

            with pytest.raises(NotImplementedError, match="Could not determine benchmark type"):
                get_run_result_from_directory_name(parent_dir, "json_output")

    @pytest.mark.parametrize("files", [[], ["test.txt"]], ids=["empty_directory", "file_only"])
    def test_no_subdirectories_raises_error(self, files: list[str]) -> None:
        """Test that a directory with no subdirectories (empty, or containing only files) raises error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir)
            for file in files:
                (parent_dir / file).touch()

            with pytest.raises(NotImplementedError, match="Could not determine benchmark type"):
                get_run_result_from_directory_name(parent_dir, "json_output")

    def test_multiple_subdirectories_first_sorted_match_wins(self) -> None:
        """Test that non-matching directories are skipped and a later matching one is used."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir)
            (parent_dir / "aaa_unknown").mkdir()
            (parent_dir / "rbdfio").mkdir()
            (parent_dir / "zzz_other").mkdir()

            result = get_run_result_from_directory_name(parent_dir, "json_output")

            assert isinstance(result, RBDFIO)

    def test_custom_filename_root(self) -> None:
        """Test that custom filename root is passed to RunResult and used to select result files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir)
            rbdfio_dir = parent_dir / "rbdfio"
            rbdfio_dir.mkdir()

            # Files matching the custom root, followed by digits, should be found
            matching_files = [rbdfio_dir / "custom_output.1", rbdfio_dir / "custom_output.2"]
            for file in matching_files:
                file.touch()
            # Files with a different root, or a non-numeric suffix, should be ignored
            (rbdfio_dir / "json_output.1").touch()
            (rbdfio_dir / "custom_output.txt").touch()

            result = get_run_result_from_directory_name(parent_dir, "custom_output")

            assert isinstance(result, RBDFIO)
            assert sorted(result._files) == matching_files

    def test_only_immediate_subdirectories_are_checked(self) -> None:
        """Test that a benchmark directory nested below an immediate subdirectory is not found."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir)
            # parent/results/rbdfio: rbdfio is two levels below parent_dir
            (parent_dir / "results" / "rbdfio").mkdir(parents=True)

            with pytest.raises(NotImplementedError, match="Could not determine benchmark type"):
                get_run_result_from_directory_name(parent_dir, "json_output")

    def test_hidden_directories_ignored(self) -> None:
        """Test that a hidden directory is not used to determine the benchmark type."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir)
            # The hidden directory name contains a benchmark type, so it would match if not skipped.
            (parent_dir / ".rbdfio_cache").mkdir()
            (parent_dir / "visualisation").mkdir()

            with pytest.raises(NotImplementedError, match="Could not determine benchmark type"):
                get_run_result_from_directory_name(parent_dir, "json_output")

    def test_benchmark_type_matched_on_directory_name_not_parent_path(self) -> None:
        """Test that a benchmark type in a parent path does not cause unrelated subdirectories to match."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir) / "rbdfio_run"
            (parent_dir / "unrelated").mkdir(parents=True)

            with pytest.raises(NotImplementedError, match="Could not determine benchmark type"):
                get_run_result_from_directory_name(parent_dir, "json_output")

    def test_ignored_directories_do_not_prevent_finding_benchmark_directory(self) -> None:
        """Test that a valid benchmark directory is still found alongside ignored directories."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir)
            (parent_dir / "visualisation").mkdir()
            (parent_dir / ".hidden_data").mkdir()
            (parent_dir / "rbdfio").mkdir()

            result = get_run_result_from_directory_name(parent_dir, "json_output")

            assert isinstance(result, RBDFIO)
            assert result.type == "rbdfio"

    def test_priority_when_multiple_types_match(self) -> None:
        """Test behavior when directory name could match multiple types."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir)
            # Create directory that contains multiple benchmark type names
            # This tests the iteration order in BENCHMARK_TYPE_MAP
            mixed_dir = parent_dir / "rbdfio_and_librbdfio"
            mixed_dir.mkdir()

            result = get_run_result_from_directory_name(parent_dir, "json_output")

            # Should match one of them (whichever is found first in iteration)
            assert isinstance(result, RBDFIO)

    def test_factory_preserves_directory_path(self) -> None:
        """Test that factory preserves the directory path in the result object."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir)
            rbdfio_dir = parent_dir / "rbdfio"
            rbdfio_dir.mkdir()

            result = get_run_result_from_directory_name(parent_dir, "json_output")

            # The result should store the parent directory
            assert result._path == parent_dir


# Made with Bob
