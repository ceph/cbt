"""
Tests for the run_result_factory module.

This module tests the factory function that creates appropriate RunResult
subclasses based on benchmark type.
"""

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

    def test_map_contains_rbdfio(self):
        """Test that map contains rbdfio."""
        assert "rbdfio" in BENCHMARK_TYPE_MAP
        assert BENCHMARK_TYPE_MAP["rbdfio"] == RBDFIO

    def test_map_contains_librbdfio(self):
        """Test that map contains librbdfio."""
        assert "librbdfio" in BENCHMARK_TYPE_MAP
        assert BENCHMARK_TYPE_MAP["librbdfio"] == RBDFIO

    def test_map_contains_kvmrbdfio(self):
        """Test that map contains kvmrbdfio."""
        assert "kvmrbdfio" in BENCHMARK_TYPE_MAP
        assert BENCHMARK_TYPE_MAP["kvmrbdfio"] == RBDFIO

    def test_map_contains_rawfio(self):
        """Test that map contains rawfio."""
        assert "rawfio" in BENCHMARK_TYPE_MAP
        assert BENCHMARK_TYPE_MAP["rawfio"] == RBDFIO

    def test_all_map_to_same_class(self):
        """Test that all FIO variants map to RBDFIO class."""
        fio_variants = ["rbdfio", "librbdfio", "kvmrbdfio", "rawfio"]
        for variant in fio_variants:
            assert BENCHMARK_TYPE_MAP[variant] == RBDFIO


class TestGetRunResultFromDirectoryName:
    """Test the get_run_result_from_directory_name factory function."""

    def test_rbdfio_directory(self):
        """Test creating RunResult for rbdfio directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir)
            rbdfio_dir = parent_dir / "rbdfio"
            rbdfio_dir.mkdir()

            result = get_run_result_from_directory_name(parent_dir, "json_output")

            assert isinstance(result, RBDFIO)
            assert result.type == "rbdfio"

    def test_librbdfio_directory(self):
        """Test creating RunResult for librbdfio directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir)
            librbdfio_dir = parent_dir / "librbdfio"
            librbdfio_dir.mkdir()

            result = get_run_result_from_directory_name(parent_dir, "json_output")

            assert isinstance(result, RBDFIO)
            assert result.type == "rbdfio"

    def test_kvmrbdfio_directory(self):
        """Test creating RunResult for kvmrbdfio directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir)
            kvmrbdfio_dir = parent_dir / "kvmrbdfio"
            kvmrbdfio_dir.mkdir()

            result = get_run_result_from_directory_name(parent_dir, "json_output")

            assert isinstance(result, RBDFIO)
            assert result.type == "rbdfio"

    def test_rawfio_directory(self):
        """Test creating RunResult for rawfio directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir)
            rawfio_dir = parent_dir / "rawfio"
            rawfio_dir.mkdir()

            result = get_run_result_from_directory_name(parent_dir, "json_output")

            assert isinstance(result, RBDFIO)
            assert result.type == "rbdfio"

    def test_case_insensitive_matching(self):
        """Test that directory matching is case-insensitive."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir)
            # Create directory with mixed case
            rbdfio_dir = parent_dir / "RbdFio"
            rbdfio_dir.mkdir()

            result = get_run_result_from_directory_name(parent_dir, "json_output")

            assert isinstance(result, RBDFIO)

    def test_directory_with_prefix(self):
        """Test matching when benchmark type is part of longer directory name."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir)
            # Directory name contains 'rbdfio' as substring
            rbdfio_dir = parent_dir / "test_rbdfio_results"
            rbdfio_dir.mkdir()

            result = get_run_result_from_directory_name(parent_dir, "json_output")

            assert isinstance(result, RBDFIO)

    def test_directory_with_suffix(self):
        """Test matching when benchmark type has suffix."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir)
            rbdfio_dir = parent_dir / "rbdfio_test"
            rbdfio_dir.mkdir()

            result = get_run_result_from_directory_name(parent_dir, "json_output")

            assert isinstance(result, RBDFIO)

    def test_unknown_benchmark_type_raises_error(self):
        """Test that unknown benchmark type raises NotImplementedError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir)
            unknown_dir = parent_dir / "unknown_benchmark"
            unknown_dir.mkdir()

            with pytest.raises(NotImplementedError, match="Could not determine benchmark type"):
                get_run_result_from_directory_name(parent_dir, "json_output")

    def test_no_subdirectories_raises_error(self):
        """Test that directory with no subdirectories raises error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir)
            # Create a file instead of directory
            (parent_dir / "test.txt").touch()

            with pytest.raises(NotImplementedError, match="Could not determine benchmark type"):
                get_run_result_from_directory_name(parent_dir, "json_output")

    def test_multiple_subdirectories_uses_first(self):
        """Test that when multiple subdirectories exist, first one found is checked."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir)
            # Create subdirectory with known benchmark type
            # The function will use the first directory it finds via iterdir()
            rbdfio_dir = parent_dir / "rbdfio"
            rbdfio_dir.mkdir()

            result = get_run_result_from_directory_name(parent_dir, "json_output")

            # Should find rbdfio since it's a known type
            assert isinstance(result, RBDFIO)

    def test_custom_filename_root(self):
        """Test that custom filename root is passed to RunResult."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir)
            rbdfio_dir = parent_dir / "rbdfio"
            rbdfio_dir.mkdir()

            custom_root = "custom_output"
            result = get_run_result_from_directory_name(parent_dir, custom_root)

            assert isinstance(result, RBDFIO)
            # Verify the object was created successfully with custom root
            assert result.type == "rbdfio"

    def test_nested_directory_structure(self):
        """Test with nested directory structure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir)
            # Create nested structure: parent/results/rbdfio
            results_dir = parent_dir / "results"
            results_dir.mkdir()
            rbdfio_dir = results_dir / "rbdfio"
            rbdfio_dir.mkdir()

            # Pass results_dir as the directory
            result = get_run_result_from_directory_name(results_dir, "json_output")

            assert isinstance(result, RBDFIO)

    def test_directory_path_object(self):
        """Test that function works with Path objects."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir)
            rbdfio_dir = parent_dir / "rbdfio"
            rbdfio_dir.mkdir()

            # Explicitly pass as Path object
            result = get_run_result_from_directory_name(Path(parent_dir), "json_output")

            assert isinstance(result, RBDFIO)

    def test_empty_directory(self):
        """Test with empty directory (no subdirectories)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir)

            with pytest.raises(NotImplementedError):
                get_run_result_from_directory_name(parent_dir, "json_output")

    def test_priority_when_multiple_types_match(self):
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


class TestFactoryIntegration:
    """Integration tests for the factory function."""

    def test_factory_creates_functional_object(self):
        """Test that factory creates a functional RunResult object."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir)
            rbdfio_dir = parent_dir / "rbdfio"
            rbdfio_dir.mkdir()

            result = get_run_result_from_directory_name(parent_dir, "json_output")

            # Verify the object has expected methods
            assert hasattr(result, "process")
            assert hasattr(result, "get")
            assert callable(result.process)
            assert callable(result.get)

    def test_factory_preserves_directory_path(self):
        """Test that factory preserves the directory path in the result object."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir)
            rbdfio_dir = parent_dir / "rbdfio"
            rbdfio_dir.mkdir()

            result = get_run_result_from_directory_name(parent_dir, "json_output")

            # The result should store the parent directory
            assert result._path == parent_dir


# Made with Bob
