"""
Tests for the TimestampAligner class.

This module tests timestamp alignment and aggregation functionality for
time-series data from multiple volumes.
"""

import numpy as np
import pandas as pd
import pytest

from post_processing.parsers.timestamp_aligner import TimestampAligner


class TestTimestampAlignerInitialization:
    """Test TimestampAligner initialization."""

    def test_default_window_size(self):
        """Test default window size is 1000ms (1 second)."""
        aligner = TimestampAligner()
        assert aligner._window_size_sec == 1.0

    def test_custom_window_size(self):
        """Test custom window size."""
        aligner = TimestampAligner(window_size_ms=500)
        assert aligner._window_size_sec == 0.5

    def test_large_window_size(self):
        """Test large window size."""
        aligner = TimestampAligner(window_size_ms=5000)
        assert aligner._window_size_sec == 5.0


class TestAlignAndAggregate:
    """Test align_and_aggregate method."""

    def test_empty_dataframes_list(self):
        """Test with empty list of dataframes."""
        aligner = TimestampAligner()
        result = aligner.align_and_aggregate([], "iops", "sum")
        assert result.empty
        assert list(result.columns) == ["timestamp_sec", "iops", "num_samples"]

    def test_all_none_dataframes(self):
        """Test with all None dataframes."""
        aligner = TimestampAligner()
        result = aligner.align_and_aggregate([None, None], "iops", "sum")
        assert result.empty
        assert list(result.columns) == ["timestamp_sec", "iops", "num_samples"]

    def test_all_empty_dataframes(self):
        """Test with all empty dataframes."""
        aligner = TimestampAligner()
        df1 = pd.DataFrame(columns=["timestamp_sec", "iops"])
        df2 = pd.DataFrame(columns=["timestamp_sec", "iops"])
        result = aligner.align_and_aggregate([df1, df2], "iops", "sum")
        assert result.empty

    def test_single_dataframe_sum(self):
        """Test with single dataframe using sum aggregation."""
        aligner = TimestampAligner(window_size_ms=1000)
        df = pd.DataFrame({"timestamp_sec": [0.5, 1.5, 2.5], "iops": [100, 200, 300]})
        result = aligner.align_and_aggregate([df], "iops", "sum")
        
        assert len(result) == 3
        assert list(result.columns) == ["timestamp_sec", "iops", "num_samples"]
        assert result["iops"].tolist() == [100, 200, 300]
        assert result["num_samples"].tolist() == [1, 1, 1]

    def test_multiple_dataframes_sum(self):
        """Test with multiple dataframes using sum aggregation."""
        aligner = TimestampAligner(window_size_ms=1000)
        df1 = pd.DataFrame({"timestamp_sec": [0.5, 1.5], "iops": [100, 200]})
        df2 = pd.DataFrame({"timestamp_sec": [0.7, 1.3], "iops": [50, 150]})
        result = aligner.align_and_aggregate([df1, df2], "iops", "sum")
        
        assert len(result) == 2
        # Both 0.5 and 0.7 fall in window 0.0, both 1.5 and 1.3 fall in window 1.0
        assert result["timestamp_sec"].tolist() == [0.0, 1.0]
        assert result["iops"].tolist() == [150, 350]  # 100+50, 200+150
        assert result["num_samples"].tolist() == [2, 2]

    def test_aggregation_mean(self):
        """Test mean aggregation."""
        aligner = TimestampAligner(window_size_ms=1000)
        df1 = pd.DataFrame({"timestamp_sec": [0.5, 1.5], "latency_ms": [10.0, 20.0]})
        df2 = pd.DataFrame({"timestamp_sec": [0.7, 1.3], "latency_ms": [20.0, 30.0]})
        result = aligner.align_and_aggregate([df1, df2], "latency_ms", "mean")
        
        assert len(result) == 2
        assert result["latency_ms"].tolist() == [15.0, 25.0]  # (10+20)/2, (20+30)/2

    def test_aggregation_max(self):
        """Test max aggregation."""
        aligner = TimestampAligner(window_size_ms=1000)
        df1 = pd.DataFrame({"timestamp_sec": [0.5, 1.5], "latency_ms": [10.0, 20.0]})
        df2 = pd.DataFrame({"timestamp_sec": [0.7, 1.3], "latency_ms": [20.0, 15.0]})
        result = aligner.align_and_aggregate([df1, df2], "latency_ms", "max")
        
        assert len(result) == 2
        assert result["latency_ms"].tolist() == [20.0, 20.0]  # max(10,20), max(20,15)

    def test_invalid_aggregation_method(self):
        """Test with invalid aggregation method."""
        aligner = TimestampAligner()
        df = pd.DataFrame({"timestamp_sec": [0.5], "iops": [100]})
        with pytest.raises(ValueError, match="Unknown aggregation method"):
            aligner.align_and_aggregate([df], "iops", "invalid")

    def test_mixed_none_and_valid_dataframes(self):
        """Test with mix of None and valid dataframes."""
        aligner = TimestampAligner(window_size_ms=1000)
        df1 = pd.DataFrame({"timestamp_sec": [0.5], "iops": [100]})
        df2 = None
        df3 = pd.DataFrame({"timestamp_sec": [0.7], "iops": [50]})
        result = aligner.align_and_aggregate([df1, df2, df3], "iops", "sum")
        
        assert len(result) == 1
        assert result["iops"].tolist() == [150]

    def test_small_window_size(self):
        """Test with small window size (100ms)."""
        aligner = TimestampAligner(window_size_ms=100)
        df = pd.DataFrame({"timestamp_sec": [0.05, 0.15, 0.25], "iops": [100, 200, 300]})
        result = aligner.align_and_aggregate([df], "iops", "sum")
        
        assert len(result) == 3
        assert result["timestamp_sec"].tolist() == [0.0, 0.1, 0.2]

    def test_large_window_size(self):
        """Test with large window size (5 seconds)."""
        aligner = TimestampAligner(window_size_ms=5000)
        df = pd.DataFrame({"timestamp_sec": [1.0, 2.0, 3.0, 6.0], "iops": [100, 200, 300, 400]})
        result = aligner.align_and_aggregate([df], "iops", "sum")
        
        assert len(result) == 2
        assert result["timestamp_sec"].tolist() == [0.0, 5.0]
        assert result["iops"].tolist() == [600, 400]  # 100+200+300, 400


class TestCalculatePercentiles:
    """Test calculate_percentiles method."""

    def test_empty_dataframes_list(self):
        """Test with empty list of dataframes."""
        aligner = TimestampAligner()
        result = aligner.calculate_percentiles([])
        assert result.empty
        assert "timestamp_sec" in result.columns

    def test_all_none_dataframes(self):
        """Test with all None dataframes."""
        aligner = TimestampAligner()
        result = aligner.calculate_percentiles([None, None])
        assert result.empty

    def test_all_empty_dataframes(self):
        """Test with all empty dataframes."""
        aligner = TimestampAligner()
        df1 = pd.DataFrame(columns=["timestamp_sec", "latency_ms"])
        df2 = pd.DataFrame(columns=["timestamp_sec", "latency_ms"])
        result = aligner.calculate_percentiles([df1, df2])
        assert result.empty

    def test_default_percentiles(self):
        """Test with default percentiles (50, 95, 99)."""
        aligner = TimestampAligner(window_size_ms=1000)
        df = pd.DataFrame({
            "timestamp_sec": [0.5] * 100,
            "latency_ms": list(range(100))
        })
        result = aligner.calculate_percentiles([df])
        
        assert len(result) == 1
        assert "timestamp_sec" in result.columns
        assert "p50_latency_ms" in result.columns
        assert "p95_latency_ms" in result.columns
        assert "p99_latency_ms" in result.columns
        assert result["p50_latency_ms"].iloc[0] == pytest.approx(49.5, rel=0.1)
        assert result["p95_latency_ms"].iloc[0] == pytest.approx(94.05, rel=0.1)
        assert result["p99_latency_ms"].iloc[0] == pytest.approx(98.01, rel=0.1)

    def test_custom_percentiles(self):
        """Test with custom percentiles."""
        aligner = TimestampAligner(window_size_ms=1000)
        df = pd.DataFrame({
            "timestamp_sec": [0.5] * 100,
            "latency_ms": list(range(100))
        })
        result = aligner.calculate_percentiles([df], percentiles=[25, 75])
        
        assert "p25_latency_ms" in result.columns
        assert "p75_latency_ms" in result.columns
        assert "p50_latency_ms" not in result.columns

    def test_multiple_time_windows(self):
        """Test percentiles across multiple time windows."""
        aligner = TimestampAligner(window_size_ms=1000)
        df = pd.DataFrame({
            "timestamp_sec": [0.5] * 50 + [1.5] * 50,
            "latency_ms": list(range(50)) + list(range(100, 150))
        })
        result = aligner.calculate_percentiles([df])
        
        assert len(result) == 2
        assert result["timestamp_sec"].tolist() == [0.0, 1.0]

    def test_multiple_dataframes(self):
        """Test percentiles with multiple dataframes."""
        aligner = TimestampAligner(window_size_ms=1000)
        df1 = pd.DataFrame({"timestamp_sec": [0.5] * 50, "latency_ms": list(range(50))})
        df2 = pd.DataFrame({"timestamp_sec": [0.7] * 50, "latency_ms": list(range(50, 100))})
        result = aligner.calculate_percentiles([df1, df2])
        
        assert len(result) == 1
        # All data falls in the same window (0.0)


class TestMergeMetrics:
    """Test merge_metrics method."""

    def test_no_dataframes(self):
        """Test with no dataframes."""
        aligner = TimestampAligner()
        result = aligner.merge_metrics()
        assert result.empty

    def test_all_none_dataframes(self):
        """Test with all None dataframes."""
        aligner = TimestampAligner()
        result = aligner.merge_metrics(None, None)
        assert result.empty

    def test_all_empty_dataframes(self):
        """Test with all empty dataframes."""
        aligner = TimestampAligner()
        df1 = pd.DataFrame()
        df2 = pd.DataFrame()
        result = aligner.merge_metrics(df1, df2)
        assert result.empty

    def test_single_dataframe(self):
        """Test with single dataframe."""
        aligner = TimestampAligner()
        df = pd.DataFrame({"timestamp_sec": [0.0, 1.0], "iops": [100, 200]})
        result = aligner.merge_metrics(df)
        
        assert len(result) == 2
        assert list(result.columns) == ["timestamp_sec", "iops"]
        pd.testing.assert_frame_equal(result, df)

    def test_merge_two_dataframes(self):
        """Test merging two dataframes."""
        aligner = TimestampAligner()
        df1 = pd.DataFrame({"timestamp_sec": [0.0, 1.0], "iops": [100, 200]})
        df2 = pd.DataFrame({"timestamp_sec": [0.0, 1.0], "latency_ms": [10.0, 20.0]})
        result = aligner.merge_metrics(df1, df2)
        
        assert len(result) == 2
        assert set(result.columns) == {"timestamp_sec", "iops", "latency_ms"}
        assert result["iops"].tolist() == [100, 200]
        assert result["latency_ms"].tolist() == [10.0, 20.0]

    def test_merge_with_different_timestamps(self):
        """Test merging dataframes with different timestamps (outer join)."""
        aligner = TimestampAligner()
        df1 = pd.DataFrame({"timestamp_sec": [0.0, 1.0], "iops": [100, 200]})
        df2 = pd.DataFrame({"timestamp_sec": [1.0, 2.0], "latency_ms": [20.0, 30.0]})
        result = aligner.merge_metrics(df1, df2)
        
        assert len(result) == 3
        assert result["timestamp_sec"].tolist() == [0.0, 1.0, 2.0]
        assert result["iops"].tolist()[0] == 100
        assert pd.isna(result["iops"].tolist()[2])  # NaN for missing value

    def test_merge_multiple_dataframes(self):
        """Test merging multiple dataframes."""
        aligner = TimestampAligner()
        df1 = pd.DataFrame({"timestamp_sec": [0.0, 1.0], "iops": [100, 200]})
        df2 = pd.DataFrame({"timestamp_sec": [0.0, 1.0], "latency_ms": [10.0, 20.0]})
        df3 = pd.DataFrame({"timestamp_sec": [0.0, 1.0], "bandwidth_mb": [50.0, 100.0]})
        result = aligner.merge_metrics(df1, df2, df3)
        
        assert len(result) == 2
        assert set(result.columns) == {"timestamp_sec", "iops", "latency_ms", "bandwidth_mb"}

    def test_merge_removes_duplicate_columns(self):
        """Test that merge removes duplicate columns."""
        aligner = TimestampAligner()
        df1 = pd.DataFrame({"timestamp_sec": [0.0, 1.0], "iops": [100, 200], "extra": [1, 2]})
        df2 = pd.DataFrame({"timestamp_sec": [0.0, 1.0], "iops": [100, 200], "latency_ms": [10.0, 20.0]})
        result = aligner.merge_metrics(df1, df2)
        
        # Should keep first occurrence of 'iops', not create 'iops_dup'
        assert "iops_dup" not in result.columns
        assert "iops" in result.columns

    def test_merge_sorts_by_timestamp(self):
        """Test that merge sorts results by timestamp."""
        aligner = TimestampAligner()
        df1 = pd.DataFrame({"timestamp_sec": [2.0, 0.0, 1.0], "iops": [300, 100, 200]})
        df2 = pd.DataFrame({"timestamp_sec": [1.0, 2.0, 0.0], "latency_ms": [20.0, 30.0, 10.0]})
        result = aligner.merge_metrics(df1, df2)
        
        assert result["timestamp_sec"].tolist() == [0.0, 1.0, 2.0]

    def test_merge_with_none_and_valid_dataframes(self):
        """Test merging with mix of None and valid dataframes."""
        aligner = TimestampAligner()
        df1 = pd.DataFrame({"timestamp_sec": [0.0, 1.0], "iops": [100, 200]})
        df2 = None
        df3 = pd.DataFrame({"timestamp_sec": [0.0, 1.0], "latency_ms": [10.0, 20.0]})
        result = aligner.merge_metrics(df1, df2, df3)
        
        assert len(result) == 2
        assert set(result.columns) == {"timestamp_sec", "iops", "latency_ms"}


class TestIntegration:
    """Integration tests combining multiple methods."""

    def test_full_workflow(self):
        """Test complete workflow: align, calculate percentiles, merge."""
        aligner = TimestampAligner(window_size_ms=1000)
        
        # Create sample data from two volumes
        df1 = pd.DataFrame({
            "timestamp_sec": [0.5, 1.5, 2.5],
            "iops": [100, 200, 300],
            "latency_ms": [10.0, 20.0, 30.0]
        })
        df2 = pd.DataFrame({
            "timestamp_sec": [0.7, 1.3, 2.8],
            "iops": [50, 150, 250],
            "latency_ms": [15.0, 25.0, 35.0]
        })
        
        # Align and aggregate IOPS
        iops_result = aligner.align_and_aggregate([df1, df2], "iops", "sum")
        
        # Calculate latency percentiles
        latency_result = aligner.calculate_percentiles([df1, df2], percentiles=[50, 95])
        
        # Merge results
        final_result = aligner.merge_metrics(iops_result, latency_result)
        
        assert "timestamp_sec" in final_result.columns
        assert "iops" in final_result.columns
        assert "p50_latency_ms" in final_result.columns
        assert "p95_latency_ms" in final_result.columns
        assert len(final_result) == 3


# Made with Bob
