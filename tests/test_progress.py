"""
Tests for progress.py — CLI progress bars for CBT runs.
"""

# pylint: disable=protected-access  # deliberate in unit tests

import threading
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any, Optional
from unittest.mock import MagicMock, patch

import pytest

import progress
import settings as _settings
from benchmark.benchmark import Benchmark
from benchmark.radosbench import Radosbench

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────


@contextmanager
def _mock_settings(cluster_dict: dict[str, object]) -> Generator[None, None, None]:
    """Temporarily replace settings.cluster with *cluster_dict*."""
    with patch("progress.settings") as mock_settings:
        mock_settings.cluster = cluster_dict
        yield


# ──────────────────────────────────────────────────────────────────────────────
# setup()
# ──────────────────────────────────────────────────────────────────────────────


class TestSetup:
    """Tests for progress.setup()."""

    def test_disabled_when_not_a_tty(self) -> None:
        """Progress is disabled when stdout is not a TTY."""
        with _mock_settings({}):
            with patch("sys.stdout.isatty", return_value=False):
                progress.setup(no_progress=False)
        assert progress._disabled is True

    def test_disabled_when_is_teuthology(self) -> None:
        """Progress is disabled when the teuthology flag is set."""
        with _mock_settings({"is_teuthology": True}):
            with patch("sys.stdout.isatty", return_value=True):
                progress.setup(no_progress=False)
        assert progress._disabled is True

    def test_disabled_when_no_progress_flag(self) -> None:
        """Progress is disabled when --no-progress was passed."""
        with _mock_settings({}):
            with patch("sys.stdout.isatty", return_value=True):
                progress.setup(no_progress=True)
        assert progress._disabled is True

    def test_enabled_when_tty_and_no_flags(self) -> None:
        """Progress is enabled when stdout is a TTY and no suppression flags are set."""
        with _mock_settings({}):
            with patch("sys.stdout.isatty", return_value=True):
                progress.setup(no_progress=False)
        assert progress._disabled is False

    def test_disabled_when_all_suppression_conditions(self) -> None:
        """Progress is disabled when multiple suppression conditions are present."""
        with _mock_settings({"is_teuthology": True}):
            with patch("sys.stdout.isatty", return_value=False):
                progress.setup(no_progress=True)
        assert progress._disabled is True


# ──────────────────────────────────────────────────────────────────────────────
# cluster_init_estimate()
# ──────────────────────────────────────────────────────────────────────────────


class TestClusterInitEstimate:
    """Tests for progress.cluster_init_estimate()."""

    def test_returns_default_when_not_configured(self) -> None:
        """Returns the module default when the setting is absent."""
        with _mock_settings({}):
            result = progress.cluster_init_estimate()
        assert result == progress.DEFAULT_CLUSTER_INIT_SECS

    def test_returns_configured_value(self) -> None:
        """Returns the value from settings when configured."""
        with _mock_settings({"init_estimate_secs": 300}):
            result = progress.cluster_init_estimate()
        assert result == 300

    def test_returns_int(self) -> None:
        """Always returns an int, even when the config value is a string."""
        with _mock_settings({"init_estimate_secs": "240"}):
            result = progress.cluster_init_estimate()
        assert isinstance(result, int)
        assert result == 240


# ──────────────────────────────────────────────────────────────────────────────
# overall_bar()
# ──────────────────────────────────────────────────────────────────────────────


class TestOverallBar:
    """Tests for progress.overall_bar() context manager."""

    def test_yields_overall_bar_handle(self) -> None:
        """The context manager yields an OverallBar instance."""
        progress._disabled = True
        with progress.overall_bar(100) as handle:
            assert isinstance(handle, progress.OverallBar)

    def test_handle_advance_does_not_raise(self) -> None:
        """advance() can be called without error."""
        progress._disabled = True
        with progress.overall_bar(100) as handle:
            handle.advance(30)

    def test_advance_is_clamped_implicitly(self) -> None:
        """Advancing beyond the total (when disabled) doesn't raise."""
        progress._disabled = True
        with progress.overall_bar(50) as handle:
            handle.advance(200)


# ──────────────────────────────────────────────────────────────────────────────
# phase_bar()
# ──────────────────────────────────────────────────────────────────────────────


class TestPhaseBar:
    """Tests for progress.phase_bar() context manager."""

    def setup_method(self) -> None:
        """Force disabled mode so bars don't write to stderr during tests."""
        progress._disabled = True

    def test_runs_body_with_known_duration(self) -> None:
        """Body executes correctly with a known duration."""
        executed: list[bool] = []
        with progress.phase_bar("test phase", 60):
            executed.append(True)
        assert executed == [True]

    def test_runs_body_with_no_duration(self) -> None:
        """Body executes correctly with an indeterminate duration."""
        executed: list[bool] = []
        with progress.phase_bar("indeterminate phase"):
            executed.append(True)
        assert executed == [True]

    def test_advances_overall_bar_on_exit(self) -> None:
        """overall.advance() is called with the phase duration on exit."""
        mock_overall = MagicMock(spec=progress.OverallBar)
        with progress.phase_bar("test phase", 45, overall=mock_overall):
            pass
        mock_overall.advance.assert_called_once_with(45)

    def test_does_not_advance_overall_when_no_duration(self) -> None:
        """overall.advance() is NOT called when duration is None."""
        mock_overall = MagicMock(spec=progress.OverallBar)
        with progress.phase_bar("no duration phase", None, overall=mock_overall):
            pass
        mock_overall.advance.assert_not_called()

    def test_does_not_advance_overall_when_not_provided(self) -> None:
        """overall.advance() is not called when overall is not provided."""
        with progress.phase_bar("test phase", 30):
            pass

    def test_exception_propagates(self) -> None:
        """Exceptions raised in the body propagate correctly."""
        with pytest.raises(RuntimeError, match="boom"):
            with progress.phase_bar("failing phase", 10):
                raise RuntimeError("boom")

    def test_overall_still_advanced_on_exception(self) -> None:
        """overall.advance() is still called even when the body raises."""
        mock_overall = MagicMock(spec=progress.OverallBar)
        with pytest.raises(RuntimeError):
            with progress.phase_bar("failing phase", 20, overall=mock_overall):
                raise RuntimeError("oops")
        mock_overall.advance.assert_called_once_with(20)

    def test_no_ticker_when_disabled(self) -> None:
        """No ticker thread is started when progress bars are disabled."""
        progress._disabled = True
        with patch("progress.threading.Thread") as mock_thread_cls:
            with progress.phase_bar("test", 60):
                pass
        mock_thread_cls.assert_not_called()

    def test_no_ticker_when_no_duration(self) -> None:
        """No ticker thread is started for indeterminate phases."""
        progress._disabled = False
        with patch("progress.threading.Thread") as mock_thread_cls:
            with patch("progress.tqdm.tqdm"):
                with progress.phase_bar("test"):  # no duration_seconds
                    pass
        mock_thread_cls.assert_not_called()

    def test_ticker_started_when_enabled_with_duration(self) -> None:
        """Ticker thread is started when bars are enabled and duration is known."""
        progress._disabled = False
        with patch("progress.threading.Thread") as mock_thread_cls:
            mock_thread = mock_thread_cls.return_value
            mock_thread.join = lambda timeout=None: None
            with patch("progress.tqdm.tqdm"):
                with progress.phase_bar("test", 60):
                    pass
        mock_thread_cls.assert_called_once()
        mock_thread.start.assert_called_once()

    def test_overall_advanced_by_remaining_when_disabled(self) -> None:
        """When bars are disabled the full duration is credited on exit (no ticker ran)."""
        progress._disabled = True
        mock_overall = MagicMock(spec=progress.OverallBar)
        with progress.phase_bar("test phase", 90, overall=mock_overall):
            pass
        # pbar.n == 0 because disabled, so remaining_credit == 90
        mock_overall.advance.assert_called_once_with(90)

    def test_overall_advanced_incrementally_by_ticker(self) -> None:
        """_tick advances overall on each tick; exit credits only the remainder."""
        mock_overall = MagicMock(spec=progress.OverallBar)
        mock_pbar: MagicMock = MagicMock()
        mock_pbar.total = 60
        mock_pbar.n = 0
        stop = threading.Event()

        # Simulate one tick: remaining=60, increment=15
        wait_responses = [False, True]

        def fake_wait(timeout: Optional[float] = None) -> bool:  # pylint: disable=unused-argument
            return wait_responses.pop(0)

        stop.wait = fake_wait  # type: ignore[method-assign]
        progress._tick(mock_pbar, stop, mock_overall)

        mock_pbar.update.assert_called_once_with(15)
        mock_overall.advance.assert_called_once_with(15)


# ──────────────────────────────────────────────────────────────────────────────
# estimate_duration() — concrete benchmark overrides
# ──────────────────────────────────────────────────────────────────────────────


class TestEstimateDuration:
    """Smoke tests to verify estimate_duration() returns sensible ints."""

    def _make_radosbench(self, **kwargs: object) -> Any:
        """Create a Radosbench instance with minimal settings for testing."""
        _settings.cluster = {
            "tmp_dir": "/tmp/cbt_test",
            "osd_ra": "0",
            "clients": "localhost",
        }
        cluster_mock = MagicMock()
        cluster_mock.config = {}
        cluster_mock.tmp_conf = "/tmp/ceph.conf"
        cluster_mock.mnt_dir = "/mnt"
        config = {
            "iteration": 0,
            "time": "300",
            "write_time": "300",
            "read_time": "300",
            **kwargs,
        }

        return Radosbench("/tmp/archive", cluster_mock, config)  # type: ignore[no-untyped-call]

    def test_radosbench_default_no_prefill(self) -> None:
        """Write + read phases give 600s total."""
        b = self._make_radosbench()
        assert b.estimate_duration() == 600

    def test_radosbench_write_only(self) -> None:
        """Write-only mode gives only the write duration."""
        b = self._make_radosbench(write_only=True)
        assert b.estimate_duration() == 300

    def test_radosbench_read_only_with_prefill(self) -> None:
        """Prefill + read = 120 + 300 = 420."""
        b = self._make_radosbench(read_only=True, prefill_time="120")
        assert b.estimate_duration() == 420

    def test_base_benchmark_returns_zero(self) -> None:
        """The base Benchmark.estimate_duration() always returns 0."""
        b = MagicMock(spec=Benchmark)
        result = Benchmark.estimate_duration(b)
        assert result == 0
