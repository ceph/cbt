"""Tests for the top monitoring backend."""

# pylint: disable=protected-access

from typing import Any, Optional
from unittest.mock import MagicMock, mock_open, patch

from monitoring.top_monitoring import OsdTopMonitoring, TopMonitoring

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_top_monitor(
    user: str = "ceph",
    mconfig: Optional[dict[str, Any]] = None,
) -> TopMonitoring:
    """Construct a TopMonitoring instance with mocked settings."""
    if mconfig is None:
        mconfig = {}
    with (
        patch("monitoring.monitoring.settings") as mock_base_settings,
        patch("monitoring.top_monitoring.settings") as mock_settings,
    ):
        mock_base_settings.getnodes.return_value = "resolved-nodes"
        mock_settings.cluster.get.side_effect = lambda key, default=None: {"user": user}.get(key, default)
        return TopMonitoring(mconfig)


def _make_osd_top_monitor(
    pid_dir: str = "/var/run/ceph",
    user: str = "ceph",
    mconfig: Optional[dict[str, Any]] = None,
) -> OsdTopMonitoring:
    """Construct an OsdTopMonitoring instance with mocked settings."""
    if mconfig is None:
        mconfig = {}
    with (
        patch("monitoring.monitoring.settings") as mock_base_settings,
        patch("monitoring.top_monitoring.settings") as mock_settings,
    ):
        mock_base_settings.getnodes.return_value = "resolved-nodes"
        mock_settings.cluster.get.side_effect = lambda key, default=None: {
            "pid_dir": pid_dir,
            "user": user,
        }.get(key, default)
        return OsdTopMonitoring(mconfig)


# ---------------------------------------------------------------------------
# TopMonitoring
# ---------------------------------------------------------------------------


def test_top_default_nodes_is_osds() -> None:
    """TopMonitoring.DEFAULT_NODES must be ['osds']."""
    assert TopMonitoring.DEFAULT_NODES == ["osds"]


def test_top_init_stores_user_and_defaults() -> None:
    """TopMonitoring.__init__ reads user from settings and applies default args."""
    monitor = _make_top_monitor(user="admin")
    assert monitor._user == "admin"
    assert monitor._top_cmd == "top"
    assert "{top_dir}" in monitor._args
    assert "{pid}" not in monitor._args


def test_top_init_accepts_custom_args() -> None:
    """TopMonitoring accepts custom top_cmd and args from mconfig."""
    monitor = _make_top_monitor(mconfig={"top_cmd": "htop", "args": "-d 1 > {top_dir}/out.txt"})
    assert monitor._top_cmd == "htop"
    assert monitor._args == "-d 1 > {top_dir}/out.txt"


def test_top_has_no_pid_dir_or_pid_glob() -> None:
    """TopMonitoring must not have _pid_dir or _pid_glob attributes."""
    monitor = _make_top_monitor()
    assert not hasattr(monitor, "_pid_dir")
    assert not hasattr(monitor, "_pid_glob")


def test_top_start_local_node_runs_top() -> None:
    """TopMonitoring.start() runs top locally without per-pid iteration."""
    mkdir_runner = MagicMock()
    local_runner = MagicMock()
    with (
        patch("monitoring.monitoring.settings") as mock_base_settings,
        patch("monitoring.top_monitoring.settings") as mock_settings,
        patch("monitoring.top_monitoring.common.pdsh", return_value=mkdir_runner) as mock_pdsh,
        patch("monitoring.top_monitoring.common.get_localnode", return_value="node1"),
        patch("monitoring.top_monitoring.common.sh", return_value=local_runner) as mock_sh,
    ):
        mock_base_settings.getnodes.return_value = "resolved-nodes"
        mock_settings.cluster.get.side_effect = lambda key, default=None: {"user": "ceph"}.get(key, default)
        monitor = TopMonitoring({})

        monitor.start("/tmp/output")

    mock_pdsh.assert_called_once_with("resolved-nodes", "mkdir -p -m0755 -- /tmp/output/top")
    mkdir_runner.communicate.assert_called_once_with()
    expected_cmd = "top -b -H -1 -n 30 > /tmp/output/top/top.out"
    mock_sh.assert_called_once_with("node1", expected_cmd)
    assert monitor._top_runners == [local_runner]


def test_top_start_remote_node_uses_pdsh() -> None:
    """TopMonitoring.start() dispatches a single pdsh command for remote nodes."""
    mkdir_runner = MagicMock()
    remote_runner = MagicMock()
    with (
        patch("monitoring.monitoring.settings") as mock_base_settings,
        patch("monitoring.top_monitoring.settings") as mock_settings,
        patch("monitoring.top_monitoring.common.pdsh") as mock_pdsh,
        patch("monitoring.top_monitoring.common.get_localnode", return_value=None),
    ):
        mock_base_settings.getnodes.return_value = "resolved-nodes"
        mock_settings.cluster.get.side_effect = lambda key, default=None: {"user": "ceph"}.get(key, default)
        mock_pdsh.side_effect = [mkdir_runner, remote_runner]
        monitor = TopMonitoring({})

        monitor.start("/tmp/output")

    mock_pdsh.assert_any_call("resolved-nodes", "mkdir -p -m0755 -- /tmp/output/top")
    mock_pdsh.assert_any_call("resolved-nodes", "top -b -H -1 -n 30 > /tmp/output/top/top.out")


def test_top_stop_kills_local_runners() -> None:
    """TopMonitoring.stop() kills locally started runners when present."""
    runner = MagicMock()
    monitor = _make_top_monitor()
    monitor._top_runners = [runner]

    with patch("monitoring.top_monitoring.common.pdsh") as mock_pdsh:
        monitor.stop(None)

    runner.kill.assert_called_once_with()
    mock_pdsh.assert_not_called()


def test_top_stop_uses_pdsh_when_no_local_runners() -> None:
    """TopMonitoring.stop() issues pkill via pdsh when no local runners are tracked."""
    stop_runner = MagicMock()
    monitor = _make_top_monitor()
    expected_pkill = f"sudo pkill -SIGINT -f '{monitor._top_cmd} {monitor._args}'"

    with patch("monitoring.top_monitoring.common.pdsh", return_value=stop_runner) as mock_pdsh:
        monitor.stop(None)

    mock_pdsh.assert_called_once_with("resolved-nodes", expected_pkill)
    stop_runner.communicate.assert_called_once_with()


def test_top_stop_chowns_output_files_when_directory_provided() -> None:
    """TopMonitoring.stop() adjusts ownership of top output files when a directory is given."""
    stop_runner = MagicMock()
    chown_runner = MagicMock()
    monitor = _make_top_monitor(user="ceph")

    with patch("monitoring.top_monitoring.common.pdsh") as mock_pdsh:
        mock_pdsh.side_effect = [stop_runner, chown_runner]
        monitor.stop("/tmp/output")

    mock_pdsh.assert_any_call("resolved-nodes", "sudo chown ceph.ceph /tmp/output/top/*top.out")


# ---------------------------------------------------------------------------
# OsdTopMonitoring
# ---------------------------------------------------------------------------


def test_osd_top_default_nodes_is_osds() -> None:
    """OsdTopMonitoring.DEFAULT_NODES must be ['osds'] (inherited)."""
    assert OsdTopMonitoring.DEFAULT_NODES == ["osds"]


def test_osd_top_is_subclass_of_top_monitoring() -> None:
    """OsdTopMonitoring must be a subclass of TopMonitoring."""
    assert issubclass(OsdTopMonitoring, TopMonitoring)


def test_osd_top_init_stores_pid_dir_and_glob() -> None:
    """OsdTopMonitoring.__init__ reads pid_dir from cluster settings and sets pid_glob."""
    monitor = _make_osd_top_monitor(pid_dir="/run/ceph")
    assert monitor._pid_dir == "/run/ceph"
    assert monitor._pid_glob == "osd.*.pid"


def test_osd_top_init_args_include_pid_placeholder() -> None:
    """OsdTopMonitoring default args template must contain {pid}."""
    monitor = _make_osd_top_monitor()
    assert "{pid}" in monitor._args
    assert "{top_dir}" in monitor._args


def test_osd_top_init_accepts_custom_pid_glob() -> None:
    """OsdTopMonitoring accepts a custom pid_glob from mconfig."""
    monitor = _make_osd_top_monitor(mconfig={"pid_glob": "*.pid"})
    assert monitor._pid_glob == "*.pid"


def test_osd_top_start_local_node_runs_top_per_pid() -> None:
    """OsdTopMonitoring.start() runs top locally for each matching pid file."""
    mkdir_runner = MagicMock()
    local_runner = MagicMock()
    with (
        patch("monitoring.monitoring.settings") as mock_base_settings,
        patch("monitoring.top_monitoring.settings") as mock_settings,
        patch("monitoring.top_monitoring.common.pdsh", return_value=mkdir_runner) as mock_pdsh,
        patch("monitoring.top_monitoring.common.get_localnode", return_value="node1"),
        patch("monitoring.top_monitoring.common.sh", return_value=local_runner) as mock_sh,
        patch("monitoring.top_monitoring.glob.glob", return_value=["/var/run/ceph/osd.1.pid"]),
        patch("builtins.open", mock_open(read_data="42\n")),
    ):
        mock_base_settings.getnodes.return_value = "resolved-nodes"
        mock_settings.cluster.get.side_effect = lambda key, default=None: {
            "pid_dir": "/var/run/ceph",
            "user": "ceph",
        }.get(key, default)
        monitor = OsdTopMonitoring({})

        monitor.start("/tmp/output")

    mock_pdsh.assert_called_once_with("resolved-nodes", "mkdir -p -m0755 -- /tmp/output/top")
    mkdir_runner.communicate.assert_called_once_with()
    expected_cmd = "top -b -H -1 -p 42 -n 30 > /tmp/output/top/42_osd_top.out"
    mock_sh.assert_called_once_with("node1", expected_cmd)
    assert monitor._top_runners == [local_runner]


def test_osd_top_start_local_node_warns_when_no_pid_files() -> None:
    """OsdTopMonitoring.start() logs a warning when no local PID files are found."""
    mkdir_runner = MagicMock()
    with (
        patch("monitoring.monitoring.settings") as mock_base_settings,
        patch("monitoring.top_monitoring.settings") as mock_settings,
        patch("monitoring.top_monitoring.common.pdsh", return_value=mkdir_runner),
        patch("monitoring.top_monitoring.common.get_localnode", return_value="node1"),
        patch("monitoring.top_monitoring.glob.glob", return_value=[]),
        patch("monitoring.top_monitoring.logger") as mock_logger,
    ):
        mock_base_settings.getnodes.return_value = "resolved-nodes"
        mock_settings.cluster.get.side_effect = lambda key, default=None: {
            "pid_dir": "/var/run/ceph",
            "user": "ceph",
        }.get(key, default)
        OsdTopMonitoring({}).start("/tmp/output")

    warning_calls = [str(c) for c in mock_logger.warning.call_args_list]
    assert any("no PID files" in c for c in warning_calls)


def test_osd_top_start_remote_node_uses_pdsh_loop() -> None:
    """OsdTopMonitoring.start() dispatches via pdsh for-loop when no local node is available."""
    mkdir_runner = MagicMock()
    ls_runner = MagicMock()
    ls_runner.communicate.return_value = ("/var/run/ceph/osd.1.pid\n", "")
    remote_runner = MagicMock()
    with (
        patch("monitoring.monitoring.settings") as mock_base_settings,
        patch("monitoring.top_monitoring.settings") as mock_settings,
        patch("monitoring.top_monitoring.common.pdsh") as mock_pdsh,
        patch("monitoring.top_monitoring.common.get_localnode", return_value=None),
    ):
        mock_base_settings.getnodes.return_value = "resolved-nodes"
        mock_settings.cluster.get.side_effect = lambda key, default=None: {
            "pid_dir": "/var/run/ceph",
            "user": "ceph",
        }.get(key, default)
        mock_pdsh.side_effect = [mkdir_runner, ls_runner, remote_runner]
        OsdTopMonitoring({}).start("/tmp/output")

    mock_pdsh.assert_any_call("resolved-nodes", "mkdir -p -m0755 -- /tmp/output/top")
    mock_pdsh.assert_any_call("resolved-nodes", "ls /var/run/ceph/osd.*.pid 2>/dev/null")
    mock_pdsh.assert_any_call(
        "resolved-nodes",
        [
            "for pid in `cat /var/run/ceph/osd.*.pid`;",
            "do",
            "top -b -H -1 -p ${pid} -n 30 > /tmp/output/top/${pid}_osd_top.out",
            ";",
            "done",
        ],
    )


def test_osd_top_start_remote_node_warns_when_no_pid_files() -> None:
    """OsdTopMonitoring.start() logs a warning when the remote ls finds no PID files."""
    mkdir_runner = MagicMock()
    ls_runner = MagicMock()
    ls_runner.communicate.return_value = ("", "")
    remote_runner = MagicMock()
    with (
        patch("monitoring.monitoring.settings") as mock_base_settings,
        patch("monitoring.top_monitoring.settings") as mock_settings,
        patch("monitoring.top_monitoring.common.pdsh") as mock_pdsh,
        patch("monitoring.top_monitoring.common.get_localnode", return_value=None),
        patch("monitoring.top_monitoring.logger") as mock_logger,
    ):
        mock_base_settings.getnodes.return_value = "resolved-nodes"
        mock_settings.cluster.get.side_effect = lambda key, default=None: {
            "pid_dir": "/var/run/ceph",
            "user": "ceph",
        }.get(key, default)
        mock_pdsh.side_effect = [mkdir_runner, ls_runner, remote_runner]
        OsdTopMonitoring({}).start("/tmp/output")

    warning_calls = [str(c) for c in mock_logger.warning.call_args_list]
    assert any("no PID files" in c for c in warning_calls)


def test_osd_top_stop_inherited_from_top_monitoring() -> None:
    """OsdTopMonitoring inherits stop() from TopMonitoring without override."""
    assert OsdTopMonitoring.stop is TopMonitoring.stop
