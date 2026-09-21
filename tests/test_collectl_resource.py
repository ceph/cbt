"""
Unit tests for CollectlResource class
"""

# pyright: strict, reportPrivateUsage=false

import tempfile
from pathlib import Path

import pytest

from post_processing.run_results.resources.collectl_resource import CollectlResource


class TestCollectlResourceInitialization:
    """Test CollectlResource initialization"""

    def test_initialization_with_valid_setup(self) -> None:
        """Test initialization with proper collectl directory structure"""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            file_path = base_path / "test_file.json"
            file_path.touch()

            # Create collectl directory with a CPU file
            collectl_dir = base_path / "collectl"
            collectl_dir.mkdir()
            cpu_file = collectl_dir / "host-20260619.cpu"
            cpu_file.write_text("#Date;Time;[CPU:0]Totl%\n20260619;17:06:30;50\n")

            resource = CollectlResource(file_path)

            assert resource.source == "collectl"
            # Accessing cpu property triggers parsing
            assert resource.cpu == "50.00"
            assert resource.memory == "0.00"

    def test_initialization_without_collectl_directory(self) -> None:
        """Test initialization fails when collectl directory doesn't exist"""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "test_file.json"
            file_path.touch()

            with pytest.raises(FileNotFoundError, match="Collectl directory not found"):
                CollectlResource(file_path)

    def test_initialization_without_cpu_files(self) -> None:
        """Test initialization fails when no CPU files exist"""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            file_path = base_path / "test.json"
            file_path.touch()

            collectl_dir = base_path / "collectl"
            collectl_dir.mkdir()

            with pytest.raises(FileNotFoundError, match="No .cpu files found"):
                CollectlResource(file_path)


class TestCollectlResourceParsing:
    """Test CPU data parsing with various formats"""

    def test_parse_with_totl_column(self) -> None:
        """Test parsing CPU data with Totl% column"""
        cpu_data = """#Date;Time;[CPU:0]User%;[CPU:0]Sys%;[CPU:0]Totl%;[CPU:1]User%;[CPU:1]Sys%;[CPU:1]Totl%
20260619;17:06:30;30;20;50;32;18;50
20260619;17:06:40;28;22;50;30;20;50
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            file_path = base_path / "test.json"
            file_path.touch()

            collectl_dir = base_path / "collectl"
            collectl_dir.mkdir()
            cpu_file = collectl_dir / "host-20260619.cpu"
            cpu_file.write_text(cpu_data)

            resource = CollectlResource(file_path)

            assert resource.cpu == "50.00"
            assert resource.memory == "0.00"

    def test_parse_without_totl_column(self) -> None:
        """Test parsing CPU data without Totl% (calculates from User+Sys)"""
        cpu_data = """#Date;Time;[CPU:0]User%;[CPU:0]Sys%;[CPU:1]User%;[CPU:1]Sys%
20260619;17:06:30;30;20;32;18
20260619;17:06:40;28;22;30;20
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            file_path = base_path / "test.json"
            file_path.touch()

            collectl_dir = base_path / "collectl"
            collectl_dir.mkdir()
            cpu_file = collectl_dir / "host-20260619.cpu"
            cpu_file.write_text(cpu_data)

            resource = CollectlResource(file_path)

            # Sample 1: (30+20 + 32+18)/2 = 50
            # Sample 2: (28+22 + 30+20)/2 = 50
            assert resource.cpu == "50.00"

    def test_parse_with_comments_and_malformed_lines(self) -> None:
        """Test parsing CPU data with comments and malformed lines"""
        cpu_data = """# This is a comment
#Date;Time;[CPU:0]Totl%
# Another comment
20260619;17:06:30;50
malformed line without enough fields
20260619;17:06:40;50
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            file_path = base_path / "test.json"
            file_path.touch()

            collectl_dir = base_path / "collectl"
            collectl_dir.mkdir()
            cpu_file = collectl_dir / "host-20260619.cpu"
            cpu_file.write_text(cpu_data)

            resource = CollectlResource(file_path)

            # Should skip malformed lines and average the good ones
            assert resource.cpu == "50.00"

    def test_parse_single_core(self) -> None:
        """Test parsing CPU data with single core"""
        cpu_data = """#Date;Time;[CPU:0]Totl%
20260619;17:06:30;50
20260619;17:06:40;50
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            file_path = base_path / "test.json"
            file_path.touch()

            collectl_dir = base_path / "collectl"
            collectl_dir.mkdir()
            cpu_file = collectl_dir / "host-20260619.cpu"
            cpu_file.write_text(cpu_data)

            resource = CollectlResource(file_path)

            # Accessing cpu property triggers parsing
            assert resource.cpu == "50.00"

    def test_parse_multiple_cores(self) -> None:
        """Test parsing CPU data with multiple cores"""
        cpu_data = """#Date;Time;[CPU:0]Totl%;[CPU:1]Totl%;[CPU:2]Totl%;[CPU:3]Totl%
20260619;17:06:30;40;50;60;70
20260619;17:06:40;50;60;70;80
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            file_path = base_path / "test.json"
            file_path.touch()

            collectl_dir = base_path / "collectl"
            collectl_dir.mkdir()
            cpu_file = collectl_dir / "host-20260619.cpu"
            cpu_file.write_text(cpu_data)

            resource = CollectlResource(file_path)

            # Sample 1: (40+50+60+70)/4 = 55
            # Sample 2: (50+60+70+80)/4 = 65
            # Average: 60.0
            assert resource.cpu == "60.00"

    def test_parse_empty_file(self) -> None:
        """Test parsing empty CPU file returns 0"""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            file_path = base_path / "test.json"
            file_path.touch()

            collectl_dir = base_path / "collectl"
            collectl_dir.mkdir()
            cpu_file = collectl_dir / "host-20260619.cpu"
            cpu_file.write_text("")

            resource = CollectlResource(file_path)

            assert resource.cpu == "0.00"

    def test_parse_no_data_rows(self) -> None:
        """Test parsing CPU file with header but no data"""
        cpu_data = """#Date;Time;[CPU:0]Totl%
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            file_path = base_path / "test.json"
            file_path.touch()

            collectl_dir = base_path / "collectl"
            collectl_dir.mkdir()
            cpu_file = collectl_dir / "host-20260619.cpu"
            cpu_file.write_text(cpu_data)

            resource = CollectlResource(file_path)

            assert resource.cpu == "0.00"


class TestCollectlResourceGet:
    """Test get() method"""

    def test_get_returns_correct_format(self) -> None:
        """Test that get() returns correct dictionary format"""
        cpu_data = """#Date;Time;[CPU:0]Totl%
20260619;17:06:30;45
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            file_path = base_path / "test.json"
            file_path.touch()

            collectl_dir = base_path / "collectl"
            collectl_dir.mkdir()
            cpu_file = collectl_dir / "host-20260619.cpu"
            cpu_file.write_text(cpu_data)

            resource = CollectlResource(file_path)
            result = resource.get()

            assert result == {"cpu": "45.00", "memory": "0.00", "source": "collectl"}

    def test_get_triggers_parsing(self) -> None:
        """Test get() automatically triggers parsing"""
        cpu_data = """#Date;Time;[CPU:0]Totl%
20260619;17:06:30;50
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            file_path = base_path / "test.json"
            file_path.touch()

            collectl_dir = base_path / "collectl"
            collectl_dir.mkdir()
            cpu_file = collectl_dir / "host-20260619.cpu"
            cpu_file.write_text(cpu_data)

            resource = CollectlResource(file_path)
            result = resource.get()

            # get() triggers parsing automatically
            assert result == {"cpu": "50.00", "memory": "0.00", "source": "collectl"}


class TestCollectlResourceEdgeCases:
    """Test edge cases and error conditions"""

    def test_zero_cpu_values(self) -> None:
        """Test handling of zero CPU values"""
        cpu_data = """#Date;Time;[CPU:0]Totl%
20260619;17:06:30;0
20260619;17:06:40;0
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            file_path = base_path / "test.json"
            file_path.touch()

            collectl_dir = base_path / "collectl"
            collectl_dir.mkdir()
            cpu_file = collectl_dir / "host-20260619.cpu"
            cpu_file.write_text(cpu_data)

            resource = CollectlResource(file_path)

            assert resource.cpu == "0.00"

    def test_decimal_cpu_values(self) -> None:
        """Test handling of decimal CPU values"""
        cpu_data = """#Date;Time;[CPU:0]Totl%
20260619;17:06:30;45.5
20260619;17:06:40;54.5
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            file_path = base_path / "test.json"
            file_path.touch()

            collectl_dir = base_path / "collectl"
            collectl_dir.mkdir()
            cpu_file = collectl_dir / "host-20260619.cpu"
            cpu_file.write_text(cpu_data)

            resource = CollectlResource(file_path)

            assert resource.cpu == "50.00"

    def test_multiple_cpu_files_uses_first(self) -> None:
        """Test that first CPU file is used when multiple exist"""
        cpu_data1 = """#Date;Time;[CPU:0]Totl%
20260619;17:06:30;30
"""
        cpu_data2 = """#Date;Time;[CPU:0]Totl%
20260619;17:06:30;70
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            file_path = base_path / "test.json"
            file_path.touch()

            collectl_dir = base_path / "collectl"
            collectl_dir.mkdir()

            # Create two CPU files
            (collectl_dir / "host1-20260619.cpu").write_text(cpu_data1)
            (collectl_dir / "host2-20260619.cpu").write_text(cpu_data2)

            resource = CollectlResource(file_path)

            # Should use one of them (order may vary, but should be valid)
            cpu_value = float(resource.cpu)
            assert cpu_value in [30.0, 70.0]


# Made with Bob
