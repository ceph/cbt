"""Tests for the blktrace monitoring backend."""

# pylint: disable=protected-access

from typing import Any, Optional
from unittest.mock import MagicMock, call, patch

from monitoring.blktrace_monitoring import BlktraceMonitoring


def _make_monitor(
    osds_per_node: int = 2,
    use_existing: bool = True,
    user: str = "ceph",
    mconfig: Optional[dict[str, Any]] = None,
) -> BlktraceMonitoring:
    """Construct a BlktraceMonitoring instance with mocked settings."""
    if mconfig is None:
        mconfig = {}
    with (
        patch("monitoring.monitoring.settings") as mock_base_settings,
        patch("monitoring.blktrace_monitoring.settings") as mock_settings,
        patch("monitoring.blktrace_monitoring.common.pdsh"),
    ):
        mock_base_settings.getnodes.return_value = "resolved-nodes"
        mock_settings.cluster.get.side_effect = lambda key, default=None: {
            "osds_per_node": osds_per_node,
            "use_existing": use_existing,
            "user": user,
        }.get(key, default)
        return BlktraceMonitoring(mconfig)


def test_default_nodes_is_osds() -> None:
    """DEFAULT_NODES must be ['osds']."""
    assert BlktraceMonitoring.DEFAULT_NODES == ["osds"]


def test_init_stores_cluster_settings() -> None:
    """__init__ reads osds_per_node, use_existing and user from settings.cluster."""
    monitor = _make_monitor(osds_per_node=4, use_existing=False, user="admin")
    assert monitor._osds_per_node == 4
    assert monitor._use_existing is False
    assert monitor._user == "admin"


def test_start_creates_directory_and_starts_traces() -> None:
    """start() calls pdsh for mkdir and once per device."""
    mkdir_runner = MagicMock()
    trace_runner = MagicMock()

    with (
        patch("monitoring.monitoring.settings") as mock_base_settings,
        patch("monitoring.blktrace_monitoring.settings") as mock_settings,
        patch("monitoring.blktrace_monitoring.common.pdsh") as mock_pdsh,
    ):
        mock_base_settings.getnodes.return_value = "resolved-nodes"
        mock_settings.cluster.get.side_effect = lambda key, default=None: {
            "osds_per_node": 2,
            "use_existing": True,
            "user": "ceph",
        }.get(key, default)
        mock_pdsh.side_effect = [mkdir_runner, trace_runner, trace_runner]

        monitor = BlktraceMonitoring({})
        monitor.start("/tmp/output")

    mock_pdsh.assert_any_call("resolved-nodes", "mkdir -p -m0755 -- /tmp/output/blktrace")
    mkdir_runner.communicate.assert_called_once_with()

    mock_pdsh.assert_any_call(
        "resolved-nodes",
        "cd /tmp/output/blktrace;sudo blktrace -o device0 -d /dev/disk/by-partlabel/osd-device-0-data",
    )
    mock_pdsh.assert_any_call(
        "resolved-nodes",
        "cd /tmp/output/blktrace;sudo blktrace -o device1 -d /dev/disk/by-partlabel/osd-device-1-data",
    )
    assert mock_pdsh.call_count == 3  # mkdir + 2 devices


def test_stop_issues_pkill() -> None:
    """stop() always calls pdsh pkill blktrace."""
    pkill_runner = MagicMock()
    monitor = _make_monitor()

    with (
        patch("monitoring.blktrace_monitoring.common.pdsh", return_value=pkill_runner) as mock_pdsh,
        patch.object(monitor, "_make_movies") as mock_movies,
    ):
        monitor.stop(None)

    mock_pdsh.assert_called_once_with("resolved-nodes", "sudo pkill -SIGINT -f blktrace")
    pkill_runner.communicate.assert_called_once_with()
    mock_movies.assert_not_called()


def test_stop_calls_make_movies_when_not_use_existing() -> None:
    """stop() calls _make_movies when use_existing is False and directory is provided."""
    pkill_runner = MagicMock()
    monitor = _make_monitor(use_existing=False)

    with (
        patch("monitoring.blktrace_monitoring.common.pdsh", return_value=pkill_runner),
        patch.object(monitor, "_make_movies") as mock_movies,
    ):
        monitor.stop("/tmp/output")

    mock_movies.assert_called_once_with("/tmp/output")


def test_stop_does_not_call_make_movies_when_use_existing() -> None:
    """stop() skips _make_movies when use_existing is True."""
    pkill_runner = MagicMock()
    monitor = _make_monitor(use_existing=True)

    with (
        patch("monitoring.blktrace_monitoring.common.pdsh", return_value=pkill_runner),
        patch.object(monitor, "_make_movies") as mock_movies,
    ):
        monitor.stop("/tmp/output")

    mock_movies.assert_not_called()


def test_stop_does_not_call_make_movies_when_directory_is_none() -> None:
    """stop() skips _make_movies when directory is None even if use_existing is False."""
    pkill_runner = MagicMock()
    monitor = _make_monitor(use_existing=False)

    with (
        patch("monitoring.blktrace_monitoring.common.pdsh", return_value=pkill_runner),
        patch.object(monitor, "_make_movies") as mock_movies,
    ):
        monitor.stop(None)

    mock_movies.assert_not_called()


def test_make_movies_issues_seekwatcher_per_device() -> None:
    """_make_movies() calls pdsh with seekwatcher command for each device."""
    movie_runner = MagicMock()
    monitor = _make_monitor(osds_per_node=2, user="ceph")

    with patch("monitoring.blktrace_monitoring.common.pdsh", return_value=movie_runner) as mock_pdsh:
        monitor._make_movies("/tmp/output")

    expected_calls = [
        call(
            "resolved-nodes",
            "cd /tmp/output/blktrace;/home/ceph/bin/seekwatcher -t device0 -o device0.mpg --movie",
        ),
        call(
            "resolved-nodes",
            "cd /tmp/output/blktrace;/home/ceph/bin/seekwatcher -t device1 -o device1.mpg --movie",
        ),
    ]
    mock_pdsh.assert_has_calls(expected_calls)
    assert mock_pdsh.call_count == 2
    assert movie_runner.communicate.call_count == 2
