"""
Unit tests for the FIO log parser module.
"""

# pyright: strict, reportPrivateUsage=false

import shutil
import tempfile
import unittest
from pathlib import Path

from post_processing.parsers.fio_log_parser import FIOLogParser


class TestFIOLogParser(unittest.TestCase):
    """Test cases for FIOLogParser class"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.parser = FIOLogParser()

    def tearDown(self) -> None:
        """Clean up test fixtures"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_parse_iops_log(self) -> None:
        """Test parsing IOPS log file"""
        # Create test IOPS log file
        iops_file = Path(self.temp_dir) / "test_iops.log"
        with iops_file.open("w", encoding="utf-8") as f:
            f.write("1000, 15234, 0, 0\n")
            f.write("2000, 15456, 0, 4096\n")
            f.write("3000, 15123, 1, 8192\n")

        result = self.parser.parse_iops_log(iops_file)

        self.assertIsNotNone(result)
        assert result is not None  # Type narrowing for mypy
        self.assertEqual(len(result), 3)
        self.assertIn("timestamp_sec", result.columns)
        self.assertIn("iops", result.columns)
        self.assertIn("direction", result.columns)

        # Check timestamp conversion (ms to seconds)
        self.assertAlmostEqual(result.iloc[0]["timestamp_sec"], 1.0, places=3)
        self.assertAlmostEqual(result.iloc[1]["timestamp_sec"], 2.0, places=3)

        # Check IOPS values
        self.assertAlmostEqual(result.iloc[0]["iops"], 15234.0, places=1)
        self.assertAlmostEqual(result.iloc[1]["iops"], 15456.0, places=1)

    def test_parse_clat_log(self) -> None:
        """Test parsing completion latency log file"""
        # Create test clat log file
        clat_file = Path(self.temp_dir) / "test_clat.log"
        with clat_file.open("w", encoding="utf-8") as f:
            f.write("1000, 2340000, 0, 0\n")  # 2.34ms in nanoseconds
            f.write("2000, 2310000, 0, 4096\n")  # 2.31ms
            f.write("3000, 15670000, 1, 8192\n")  # 15.67ms

        result = self.parser.parse_clat_log(clat_file)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(len(result), 3)
        self.assertIn("timestamp_sec", result.columns)
        self.assertIn("latency_ms", result.columns)
        self.assertIn("direction", result.columns)

        # Check latency conversion (ns to ms)
        self.assertAlmostEqual(result.iloc[0]["latency_ms"], 2.34, places=2)
        self.assertAlmostEqual(result.iloc[1]["latency_ms"], 2.31, places=2)
        self.assertAlmostEqual(result.iloc[2]["latency_ms"], 15.67, places=2)

    def test_parse_bw_log(self) -> None:
        """Test parsing bandwidth log file"""
        # Create test bandwidth log file
        bw_file = Path(self.temp_dir) / "test_bw.log"
        with bw_file.open("w", encoding="utf-8") as f:
            f.write("1000, 60892, 0, 0\n")  # KB/s
            f.write("2000, 61769, 0, 4096\n")
            f.write("3000, 58901, 1, 8192\n")

        result = self.parser.parse_bw_log(bw_file)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(len(result), 3)
        self.assertIn("timestamp_sec", result.columns)
        self.assertIn("bandwidth_bytes", result.columns)
        self.assertIn("direction", result.columns)

        # Check bandwidth conversion (KB to bytes)
        self.assertAlmostEqual(result.iloc[0]["bandwidth_bytes"], 60892 * 1024, places=0)
        self.assertAlmostEqual(result.iloc[1]["bandwidth_bytes"], 61769 * 1024, places=0)

    def test_parse_lat_log(self) -> None:
        """Test parsing total latency log file (same format as clat)"""
        lat_file = Path(self.temp_dir) / "test_lat.log"
        with lat_file.open("w", encoding="utf-8") as f:
            f.write("1000, 2500000, 0, 0\n")

        result = self.parser.parse_lat_log(lat_file)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(len(result), 1)
        self.assertAlmostEqual(result.iloc[0]["latency_ms"], 2.5, places=2)

    def test_parse_slat_log(self) -> None:
        """Test parsing submission latency log file (same format as clat)"""
        slat_file = Path(self.temp_dir) / "test_slat.log"
        with slat_file.open("w", encoding="utf-8") as f:
            f.write("1000, 100000, 0, 0\n")  # 0.1ms

        result = self.parser.parse_slat_log(slat_file)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(len(result), 1)
        self.assertAlmostEqual(result.iloc[0]["latency_ms"], 0.1, places=2)

    def test_parse_empty_file(self) -> None:
        """Test parsing an empty log file"""
        empty_file = Path(self.temp_dir) / "empty.log"
        empty_file.touch()

        result = self.parser.parse_iops_log(empty_file)

        # Should return empty DataFrame, not None
        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(len(result), 0)

    def test_parse_nonexistent_file(self) -> None:
        """Test parsing a file that doesn't exist"""
        nonexistent = Path(self.temp_dir) / "nonexistent.log"

        result = self.parser.parse_iops_log(nonexistent)

        # Should return None on error
        self.assertIsNone(result)

    def test_parse_malformed_file(self) -> None:
        """Test parsing a malformed log file"""
        malformed_file = Path(self.temp_dir) / "malformed.log"
        with malformed_file.open("w", encoding="utf-8") as f:
            f.write("not,valid,data\n")
            f.write("1000, abc, 0, 0\n")  # Invalid IOPS value

        result = self.parser.parse_iops_log(malformed_file)

        # Should return None on parsing error
        self.assertIsNone(result)

    def test_parse_with_whitespace(self) -> None:
        """Test parsing log file with extra whitespace"""
        iops_file = Path(self.temp_dir) / "whitespace.log"
        with iops_file.open("w", encoding="utf-8") as f:
            f.write(" 1000 , 15234 , 0 , 0 \n")
            f.write("  2000,  15456,  0,  4096  \n")

        result = self.parser.parse_iops_log(iops_file)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(len(result), 2)
        self.assertAlmostEqual(result.iloc[0]["iops"], 15234.0, places=1)

    def test_parse_and_combine_logs_iops(self) -> None:
        """Test combining multiple IOPS log files"""
        # Create multiple IOPS log files
        iops_file1 = Path(self.temp_dir) / "output.0_iops.1.log"
        with iops_file1.open("w", encoding="utf-8") as f:
            f.write("1000, 15000, 0, 0\n")
            f.write("2000, 16000, 0, 4096\n")

        iops_file2 = Path(self.temp_dir) / "output.1_iops.1.log"
        with iops_file2.open("w", encoding="utf-8") as f:
            f.write("1000, 14000, 0, 0\n")
            f.write("2000, 15000, 0, 4096\n")

        result = self.parser.parse_and_combine_logs(Path(self.temp_dir), "iops", "output.*_iops.1.log")

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(len(result), 2)
        # Values should be summed: 15000 + 14000 = 29000
        self.assertAlmostEqual(result.iloc[0]["iops"], 29000.0, places=1)
        self.assertAlmostEqual(result.iloc[1]["iops"], 31000.0, places=1)

    def test_parse_and_combine_logs_clat(self) -> None:
        """Test combining multiple completion latency log files"""
        clat_file1 = Path(self.temp_dir) / "output.0_clat.1.log"
        with clat_file1.open("w", encoding="utf-8") as f:
            f.write("1000, 2000000, 0, 0\n")  # 2ms
            f.write("2000, 3000000, 0, 4096\n")  # 3ms

        clat_file2 = Path(self.temp_dir) / "output.1_clat.1.log"
        with clat_file2.open("w", encoding="utf-8") as f:
            f.write("1000, 2500000, 0, 0\n")  # 2.5ms
            f.write("2000, 3500000, 0, 4096\n")  # 3.5ms

        result = self.parser.parse_and_combine_logs(Path(self.temp_dir), "clat", "output.*_clat.1.log")

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(len(result), 2)
        # Latencies should be summed: 2.0 + 2.5 = 4.5ms
        self.assertAlmostEqual(result.iloc[0]["latency_ms"], 4.5, places=1)
        self.assertAlmostEqual(result.iloc[1]["latency_ms"], 6.5, places=1)

    def test_parse_and_combine_logs_bw(self) -> None:
        """Test combining multiple bandwidth log files"""
        bw_file1 = Path(self.temp_dir) / "output.0_bw.1.log"
        with bw_file1.open("w", encoding="utf-8") as f:
            f.write("1000, 50000, 0, 0\n")  # 50MB/s in KB/s

        bw_file2 = Path(self.temp_dir) / "output.1_bw.1.log"
        with bw_file2.open("w", encoding="utf-8") as f:
            f.write("1000, 30000, 0, 0\n")  # 30MB/s in KB/s

        result = self.parser.parse_and_combine_logs(Path(self.temp_dir), "bw", "output.*_bw.1.log")

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(len(result), 1)
        # Bandwidth should be summed: (50000 + 30000) * 1024 = 81920000 bytes
        self.assertAlmostEqual(result.iloc[0]["bandwidth_bytes"], 81920000.0, places=1)

    def test_parse_and_combine_logs_no_files(self) -> None:
        """Test combining when no matching files exist"""
        result = self.parser.parse_and_combine_logs(Path(self.temp_dir), "iops", "nonexistent_*.log")

        self.assertIsNone(result)

    def test_parse_and_combine_logs_invalid_type(self) -> None:
        """Test combining with invalid log type"""
        result = self.parser.parse_and_combine_logs(Path(self.temp_dir), "invalid_type", "*.log")

        self.assertIsNone(result)

    def test_parse_and_combine_logs_empty_files(self) -> None:
        """Test combining when all files are empty"""
        empty_file1 = Path(self.temp_dir) / "output.0_iops.1.log"
        empty_file1.open("w", encoding="utf-8").close()

        empty_file2 = Path(self.temp_dir) / "output.1_iops.1.log"
        empty_file2.open("w", encoding="utf-8").close()

        result = self.parser.parse_and_combine_logs(Path(self.temp_dir), "iops", "output.*_iops.1.log")

        self.assertIsNone(result)

    def test_parse_and_combine_logs_mixed_directions(self) -> None:
        """Test combining logs with different I/O directions"""
        iops_file1 = Path(self.temp_dir) / "output.0_iops.1.log"
        with iops_file1.open("w", encoding="utf-8") as f:
            f.write("1000, 10000, 0, 0\n")  # Read
            f.write("1000, 5000, 1, 0\n")  # Write

        iops_file2 = Path(self.temp_dir) / "output.1_iops.1.log"
        with iops_file2.open("w", encoding="utf-8") as f:
            f.write("1000, 12000, 0, 0\n")  # Read
            f.write("1000, 6000, 1, 0\n")  # Write

        result = self.parser.parse_and_combine_logs(Path(self.temp_dir), "iops", "output.*_iops.1.log")

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(len(result), 2)
        # Should have separate entries for read (direction=0) and write (direction=1)
        read_row = result[result["direction"] == 0].iloc[0]
        write_row = result[result["direction"] == 1].iloc[0]
        self.assertAlmostEqual(read_row["iops"], 22000.0, places=1)
        self.assertAlmostEqual(write_row["iops"], 11000.0, places=1)

    def test_parse_and_combine_logs_single_file(self) -> None:
        """Test combining with only one file (should still work)"""
        iops_file = Path(self.temp_dir) / "output.0_iops.1.log"
        with iops_file.open("w", encoding="utf-8") as f:
            f.write("1000, 15000, 0, 0\n")
            f.write("2000, 16000, 0, 4096\n")

        result = self.parser.parse_and_combine_logs(Path(self.temp_dir), "iops", "output.0_iops.1.log")

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(len(result), 2)
        self.assertAlmostEqual(result.iloc[0]["iops"], 15000.0, places=1)


# Made with Bob
