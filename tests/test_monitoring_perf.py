"""Tests for the perf monitoring backend."""

# pylint: disable=protected-access

from typing import Any, Optional
from unittest.mock import MagicMock, mock_open, patch

from monitoring.perf_monitoring import OsdPerfMonitoring, PerfMonitoring

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ARGS = "stat -p {pid} -o {perf_dir}/perf_stat.{pid}"


def _make_perf_monitor(
    user: str = "ceph",
    mconfig: Optional[dict[str, Any]] = None,
) -> PerfMonitoring:
    """Construct a PerfMonitoring instance with mocked settings."""
    if mconfig is None:
        mconfig = {"args": _ARGS}
    with (
        patch("monitoring.monitoring.settings") as mock_base_settings,
        patch("monitoring.perf_monitoring.settings") as mock_settings,
    ):
        mock_base_settings.getnodes.return_value = "resolved-nodes"
        mock_settings.cluster.get.side_effect = lambda key, default=None: {"user": user}.get(key, default)
        return PerfMonitoring(mconfig)


def _make_osd_perf_monitor(
    pid_dir: str = "/var/run/ceph",
    user: str = "ceph",
    mconfig: Optional[dict[str, Any]] = None,
) -> OsdPerfMonitoring:
    """Construct an OsdPerfMonitoring instance with mocked settings."""
    if mconfig is None:
        mconfig = {"args": _ARGS}
    with (
        patch("monitoring.monitoring.settings") as mock_base_settings,
        patch("monitoring.perf_monitoring.settings") as mock_settings,
    ):
        mock_base_settings.getnodes.return_value = "resolved-nodes"
        mock_settings.cluster.get.side_effect = lambda key, default=None: {
            "pid_dir": pid_dir,
            "user": user,
        }.get(key, default)
        return OsdPerfMonitoring(mconfig)


# ---------------------------------------------------------------------------
# PerfMonitoring
# ---------------------------------------------------------------------------


def test_perf_default_nodes_is_osds() -> None:
    """PerfMonitoring.DEFAULT_NODES must be ['osds']."""
    assert PerfMonitoring.DEFAULT_NODES == ["osds"]


def test_perf_init_stores_user_and_defaults() -> None:
    """PerfMonitoring.__init__ reads user from settings and stores perf_cmd."""
    monitor = _make_perf_monitor(user="admin")
    assert monitor._user == "admin"
    assert monitor._perf_cmd == "sudo perf"


def test_perf_has_no_pid_dir_or_pid_glob() -> None:
    """PerfMonitoring must not have _pid_dir or _pid_glob attributes."""
    monitor = _make_perf_monitor()
    assert not hasattr(monitor, "_pid_dir")
    assert not hasattr(monitor, "_pid_glob")


def test_perf_start_local_node_runs_perf() -> None:
    """PerfMonitoring.start() runs a single perf command locally."""
    mkdir_runner = MagicMock()
    local_runner = MagicMock()
    with (
        patch("monitoring.monitoring.settings") as mock_base_settings,
        patch("monitoring.perf_monitoring.settings") as mock_settings,
        patch("monitoring.perf_monitoring.common.pdsh", return_value=mkdir_runner) as mock_pdsh,
        patch("monitoring.perf_monitoring.common.get_localnode", return_value="node1"),
        patch("monitoring.perf_monitoring.common.sh", return_value=local_runner) as mock_sh,
    ):
        mock_base_settings.getnodes.return_value = "resolved-nodes"
        mock_settings.cluster.get.side_effect = lambda key, default=None: {"user": "ceph"}.get(key, default)
        monitor = PerfMonitoring({"args": "stat -o {perf_dir}/perf_stat.out"})

        monitor.start("/tmp/output")

    mock_pdsh.assert_called_once_with("resolved-nodes", "mkdir -p -m0755 -- /tmp/output/perf")
    mkdir_runner.communicate.assert_called_once_with()
    mock_sh.assert_called_once_with("node1", "sudo perf stat -o /tmp/output/perf/perf_stat.out &")
    assert monitor._perf_runners == [local_runner]
    assert monitor._perf_dir == "/tmp/output/perf"


def test_perf_start_remote_node_uses_pdsh() -> None:
    """PerfMonitoring.start() dispatches a single pdsh command for remote nodes."""
    mkdir_runner = MagicMock()
    remote_runner = MagicMock()
    with (
        patch("monitoring.monitoring.settings") as mock_base_settings,
        patch("monitoring.perf_monitoring.settings") as mock_settings,
        patch("monitoring.perf_monitoring.common.pdsh") as mock_pdsh,
        patch("monitoring.perf_monitoring.common.get_localnode", return_value=None),
    ):
        mock_base_settings.getnodes.return_value = "resolved-nodes"
        mock_settings.cluster.get.side_effect = lambda key, default=None: {"user": "ceph"}.get(key, default)
        mock_pdsh.side_effect = [mkdir_runner, remote_runner]
        PerfMonitoring({"args": "stat -o {perf_dir}/perf_stat.out"}).start("/tmp/output")

    mock_pdsh.assert_any_call("resolved-nodes", "mkdir -p -m0755 -- /tmp/output/perf")
    mock_pdsh.assert_any_call("resolved-nodes", "sudo perf stat -o /tmp/output/perf/perf_stat.out &")


def test_perf_stop_kills_local_runners() -> None:
    """PerfMonitoring.stop() kills locally started runners when present."""
    runner = MagicMock()
    monitor = _make_perf_monitor()
    monitor._perf_runners = [runner]

    with patch("monitoring.perf_monitoring.common.pdsh"):
        monitor.stop(None)

    runner.kill.assert_called_once_with()


def test_perf_stop_uses_pdsh_when_no_local_runners() -> None:
    """PerfMonitoring.stop() issues pkill via pdsh when no local runners are tracked."""
    stop_runner = MagicMock()
    monitor = _make_perf_monitor()

    with patch("monitoring.perf_monitoring.common.pdsh", return_value=stop_runner) as mock_pdsh:
        monitor.stop(None)

    mock_pdsh.assert_called_once_with("resolved-nodes", r"sudo pkill -SIGINT -f perf\ ")
    stop_runner.communicate.assert_called_once_with()


def test_perf_stop_chowns_output_files_when_directory_provided() -> None:
    """PerfMonitoring.stop() adjusts ownership of generated perf files when a directory is given."""
    stop_runner = MagicMock()
    chown_data_runner = MagicMock()
    chown_stat_runner = MagicMock()
    monitor = _make_perf_monitor()

    with patch("monitoring.perf_monitoring.common.pdsh") as mock_pdsh:
        mock_pdsh.side_effect = [stop_runner, chown_data_runner, chown_stat_runner]
        monitor.stop("/tmp/output")

    mock_pdsh.assert_any_call("resolved-nodes", r"sudo pkill -SIGINT -f perf\ ")
    mock_pdsh.assert_any_call("resolved-nodes", "sudo chown ceph.ceph /tmp/output/perf/perf.data")
    mock_pdsh.assert_any_call("resolved-nodes", "sudo chown ceph.ceph /tmp/output/perf/perf_stat.*")


def test_perf_get_cpu_cycles_returns_total_cycles() -> None:
    """PerfMonitoring.get_cpu_cycles() sums cycle counts from all perf stat output files."""
    with (
        patch("monitoring.monitoring.settings") as mock_base_settings,
        patch("monitoring.perf_monitoring.settings") as mock_settings,
        patch("monitoring.perf_monitoring.glob.glob", return_value=["/tmp/output/perf"]),
        patch("monitoring.perf_monitoring.os.listdir", return_value=["perf_stat.1", "perf_stat.2"]),
        patch(
            "builtins.open",
            side_effect=[
                mock_open(read_data="1,000 cycles user\n").return_value,
                mock_open(read_data="2,500 cycles user\n").return_value,
            ],
        ),
    ):
        mock_base_settings.getnodes.return_value = "resolved-nodes"
        mock_settings.cluster.get.side_effect = lambda key, default=None: {"user": "ceph"}.get(key, default)
        monitor = PerfMonitoring({"args": _ARGS})

        assert monitor.get_cpu_cycles("/tmp/output") == 3500


def test_perf_get_cpu_cycles_returns_none_when_cycles_missing() -> None:
    """PerfMonitoring.get_cpu_cycles() returns None when perf output has no cycles line."""
    with (
        patch("monitoring.monitoring.settings") as mock_base_settings,
        patch("monitoring.perf_monitoring.settings") as mock_settings,
        patch("monitoring.perf_monitoring.glob.glob", return_value=["/tmp/output/perf"]),
        patch("monitoring.perf_monitoring.os.listdir", return_value=["perf_stat.1"]),
        patch("builtins.open", mock_open(read_data="nothing to match\n")),
    ):
        mock_base_settings.getnodes.return_value = "resolved-nodes"
        mock_settings.cluster.get.side_effect = lambda key, default=None: {"user": "ceph"}.get(key, default)
        monitor = PerfMonitoring({"args": _ARGS})

        assert monitor.get_cpu_cycles("/tmp/output") is None


# ---------------------------------------------------------------------------
# OsdPerfMonitoring
# ---------------------------------------------------------------------------


def test_osd_perf_default_nodes_is_osds() -> None:
    """OsdPerfMonitoring.DEFAULT_NODES must be ['osds'] (inherited)."""
    assert OsdPerfMonitoring.DEFAULT_NODES == ["osds"]


def test_osd_perf_is_subclass_of_perf_monitoring() -> None:
    """OsdPerfMonitoring must be a subclass of PerfMonitoring."""
    assert issubclass(OsdPerfMonitoring, PerfMonitoring)


def test_osd_perf_init_stores_pid_dir_and_glob() -> None:
    """OsdPerfMonitoring.__init__ reads pid_dir from cluster settings and sets pid_glob."""
    monitor = _make_osd_perf_monitor(pid_dir="/run/ceph")
    assert monitor._pid_dir == "/run/ceph"
    assert monitor._pid_glob == "osd.*.pid"


def test_osd_perf_init_accepts_custom_pid_glob() -> None:
    """OsdPerfMonitoring accepts a custom pid_glob from mconfig."""
    monitor = _make_osd_perf_monitor(mconfig={"args": _ARGS, "pid_glob": "*.pid"})
    assert monitor._pid_glob == "*.pid"


def test_osd_perf_start_local_node_runs_perf_per_pid() -> None:
    """OsdPerfMonitoring.start() runs perf locally for each matching pid file."""
    mkdir_runner = MagicMock()
    local_runner = MagicMock()
    with (
        patch("monitoring.monitoring.settings") as mock_base_settings,
        patch("monitoring.perf_monitoring.settings") as mock_settings,
        patch("monitoring.perf_monitoring.common.pdsh", return_value=mkdir_runner) as mock_pdsh,
        patch("monitoring.perf_monitoring.common.get_localnode", return_value="node1"),
        patch("monitoring.perf_monitoring.common.sh", return_value=local_runner) as mock_sh,
        patch("monitoring.perf_monitoring.glob.glob", return_value=["/var/run/ceph/osd.1.pid"]),
        patch("builtins.open", mock_open(read_data="123\n")),
    ):
        mock_base_settings.getnodes.return_value = "resolved-nodes"
        mock_settings.cluster.get.side_effect = lambda key, default=None: {
            "pid_dir": "/var/run/ceph",
            "user": "ceph",
        }.get(key, default)
        monitor = OsdPerfMonitoring({"args": _ARGS})

        monitor.start("/tmp/output")

    mock_pdsh.assert_called_once_with("resolved-nodes", "mkdir -p -m0755 -- /tmp/output/perf")
    mkdir_runner.communicate.assert_called_once_with()
    mock_sh.assert_called_once_with("node1", "sudo perf stat -p 123 -o /tmp/output/perf/perf_stat.123 &")
    assert monitor._perf_runners == [local_runner]
    assert monitor._perf_dir == "/tmp/output/perf"


def test_osd_perf_start_remote_node_uses_pdsh_loop() -> None:
    """OsdPerfMonitoring.start() dispatches via pdsh for-loop when no local node is available."""
    mkdir_runner = MagicMock()
    remote_runner = MagicMock()
    with (
        patch("monitoring.monitoring.settings") as mock_base_settings,
        patch("monitoring.perf_monitoring.settings") as mock_settings,
        patch("monitoring.perf_monitoring.common.pdsh") as mock_pdsh,
        patch("monitoring.perf_monitoring.common.get_localnode", return_value=None),
    ):
        mock_base_settings.getnodes.return_value = "resolved-nodes"
        mock_settings.cluster.get.side_effect = lambda key, default=None: {
            "pid_dir": "/var/run/ceph",
            "user": "ceph",
        }.get(key, default)
        mock_pdsh.side_effect = [mkdir_runner, remote_runner]
        OsdPerfMonitoring({"args": _ARGS}).start("/tmp/output")

    mock_pdsh.assert_any_call("resolved-nodes", "mkdir -p -m0755 -- /tmp/output/perf")
    mock_pdsh.assert_any_call(
        "resolved-nodes",
        [
            "for pid in `cat /var/run/ceph/osd.*.pid`;",
            "do",
            "sudo perf stat -p ${pid} -o /tmp/output/perf/perf_stat.${pid} &",
            ";",
            "done",
        ],
    )


def test_osd_perf_stop_inherited_from_perf_monitoring() -> None:
    """OsdPerfMonitoring inherits stop() from PerfMonitoring without override."""
    assert OsdPerfMonitoring.stop is PerfMonitoring.stop
