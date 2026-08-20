"""Tests for the perf monitoring backend."""

# pylint: disable=protected-access,duplicate-code

from typing import Any, Optional
from unittest.mock import MagicMock, mock_open, patch

import pytest

from monitoring.osd_perf_monitoring import OsdPerfMonitoring
from monitoring.perf_monitoring import PerfMonitoring

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ARGS = "stat -p {pid} -o {output_dir}/perf_stat.{pid}"


def _make_perf_monitor(
    user: str = "ceph",
    mconfig: Optional[dict[str, Any]] = None,
) -> PerfMonitoring:
    """Construct a PerfMonitoring instance with mocked settings."""
    if mconfig is None:
        mconfig = {"args": _ARGS}
    assert "args" in mconfig, "_make_perf_monitor: mconfig must include 'args'"
    with patch("monitoring.monitoring.settings") as mock_base_settings:
        mock_base_settings.getnodes.return_value = "resolved-nodes"
        mock_base_settings.cluster.get.side_effect = lambda key, default=None: {"user": user}.get(key, default)
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
        patch("monitoring.osd_pid_monitoring.settings") as mock_osd_settings,
    ):
        mock_base_settings.getnodes.return_value = "resolved-nodes"
        mock_base_settings.cluster.get.side_effect = lambda key, default=None: {
            "user": user,
        }.get(key, default)
        mock_osd_settings.cluster.get.side_effect = lambda key, default=None: {
            "pid_dir": pid_dir,
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
    # _check_tool and _make_remote_dir both resolve through the same common.pdsh
    # reference.  Use side_effect to return distinct runners per call.
    check_runner = MagicMock()
    check_runner.communicate.return_value = ("/usr/bin/perf\n", "")
    mkdir_runner = MagicMock()
    local_runner = MagicMock()
    with (
        patch("monitoring.monitoring.settings") as mock_base_settings,
        patch("monitoring.perf_monitoring.common.pdsh") as mock_pdsh,
        patch("monitoring.perf_monitoring.common.get_localnode", return_value="node1"),
        patch("monitoring.perf_monitoring.common.sh", return_value=local_runner) as mock_sh,
    ):
        mock_base_settings.getnodes.return_value = "resolved-nodes"
        mock_base_settings.cluster.get.side_effect = lambda key, default=None: {"user": "ceph"}.get(key, default)
        mock_pdsh.side_effect = [check_runner, mkdir_runner]
        monitor = PerfMonitoring({"args": "stat -o {perf_dir}/perf_stat.out"})

        monitor.start("/tmp/output")

    mock_pdsh.assert_any_call("resolved-nodes", "command -v perf", continue_if_error=False)
    mock_pdsh.assert_any_call("resolved-nodes", "mkdir -p -m0755 -- /tmp/output/perf")
    mkdir_runner.communicate.assert_called_once_with()
    mock_sh.assert_called_once_with("node1", "sudo perf stat -o /tmp/output/perf/perf_stat.out")
    assert monitor._perf_runners == [local_runner]
    assert monitor._perf_dir == "/tmp/output/perf"


def test_perf_start_remote_node_uses_pdsh() -> None:
    """PerfMonitoring.start() dispatches a single pdsh command for remote nodes and awaits it."""
    check_runner = MagicMock()
    check_runner.communicate.return_value = ("/usr/bin/perf\n", "")
    mkdir_runner = MagicMock()
    remote_runner = MagicMock()
    with (
        patch("monitoring.monitoring.settings") as mock_base_settings,
        patch("monitoring.perf_monitoring.common.pdsh") as mock_pdsh,
        patch("monitoring.perf_monitoring.common.get_localnode", return_value=None),
    ):
        mock_base_settings.getnodes.return_value = "resolved-nodes"
        mock_base_settings.cluster.get.side_effect = lambda key, default=None: {"user": "ceph"}.get(key, default)
        mock_pdsh.side_effect = [check_runner, mkdir_runner, remote_runner]
        PerfMonitoring({"args": "stat -o {perf_dir}/perf_stat.out"}).start("/tmp/output")

    mock_pdsh.assert_any_call("resolved-nodes", "command -v perf", continue_if_error=False)
    mock_pdsh.assert_any_call("resolved-nodes", "mkdir -p -m0755 -- /tmp/output/perf")
    mock_pdsh.assert_any_call("resolved-nodes", "sudo perf stat -o /tmp/output/perf/perf_stat.out")
    remote_runner.communicate.assert_called_once_with()


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

    mock_pdsh.assert_called_once_with("resolved-nodes", "sudo pkill -SIGINT -f 'perf '")
    stop_runner.communicate.assert_called_once_with()


def test_perf_stop_chowns_output_files_when_directory_provided() -> None:
    """PerfMonitoring.stop() adjusts ownership of generated perf files and awaits each chown."""
    stop_runner = MagicMock()
    chown_data_runner = MagicMock()
    chown_stat_runner = MagicMock()
    monitor = _make_perf_monitor(user="ceph")

    with patch("monitoring.perf_monitoring.common.pdsh") as mock_pdsh:
        mock_pdsh.side_effect = [stop_runner, chown_data_runner, chown_stat_runner]
        monitor.stop("/tmp/output")

    mock_pdsh.assert_any_call("resolved-nodes", "sudo pkill -SIGINT -f 'perf '")
    mock_pdsh.assert_any_call("resolved-nodes", "sudo chown ceph:ceph /tmp/output/perf/perf.data")
    chown_data_runner.communicate.assert_called_once_with()
    mock_pdsh.assert_any_call("resolved-nodes", "sudo chown ceph:ceph /tmp/output/perf/perf_stat.*")
    chown_stat_runner.communicate.assert_called_once_with()


def test_perf_get_cpu_cycles_returns_total_cycles() -> None:
    """PerfMonitoring.get_cpu_cycles() sums cycle counts from all perf stat output files."""
    with (
        patch("monitoring.monitoring.settings") as mock_base_settings,
        patch("monitoring.perf_monitoring.os.path.isdir", return_value=True),
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
        mock_base_settings.cluster.get.side_effect = lambda key, default=None: {"user": "ceph"}.get(key, default)
        monitor = PerfMonitoring({"args": _ARGS})

        assert monitor.get_cpu_cycles("/tmp/output") == 3500


def test_perf_get_cpu_cycles_returns_none_when_cycles_missing() -> None:
    """PerfMonitoring.get_cpu_cycles() returns None when perf output has no cycles line."""
    with (
        patch("monitoring.monitoring.settings") as mock_base_settings,
        patch("monitoring.perf_monitoring.os.path.isdir", return_value=True),
        patch("monitoring.perf_monitoring.os.listdir", return_value=["perf_stat.1"]),
        patch("builtins.open", mock_open(read_data="nothing to match\n")),
    ):
        mock_base_settings.getnodes.return_value = "resolved-nodes"
        mock_base_settings.cluster.get.side_effect = lambda key, default=None: {"user": "ceph"}.get(key, default)
        monitor = PerfMonitoring({"args": _ARGS})

        assert monitor.get_cpu_cycles("/tmp/output") is None


def test_perf_get_cpu_cycles_returns_none_when_no_perf_dir() -> None:
    """PerfMonitoring.get_cpu_cycles() returns None when the perf directory does not exist."""
    with (
        patch("monitoring.monitoring.settings") as mock_base_settings,
        patch("monitoring.perf_monitoring.os.path.isdir", return_value=False),
        patch("monitoring.perf_monitoring.logger") as mock_logger,
    ):
        mock_base_settings.getnodes.return_value = "resolved-nodes"
        mock_base_settings.cluster.get.side_effect = lambda key, default=None: {"user": "ceph"}.get(key, default)
        monitor = PerfMonitoring({"args": _ARGS})

        result = monitor.get_cpu_cycles("/tmp/output")

    assert result is None
    warning_calls = [str(c) for c in mock_logger.warning.call_args_list]
    assert any("perf directory not found" in c for c in warning_calls)


def test_perf_get_cpu_cycles_returns_none_when_perf_dir_is_empty() -> None:
    """PerfMonitoring.get_cpu_cycles() returns None (not 0) when perf dir exists but has no files."""
    with (
        patch("monitoring.monitoring.settings") as mock_base_settings,
        patch("monitoring.perf_monitoring.os.path.isdir", return_value=True),
        patch("monitoring.perf_monitoring.os.listdir", return_value=[]),
        patch("monitoring.perf_monitoring.logger") as mock_logger,
    ):
        mock_base_settings.getnodes.return_value = "resolved-nodes"
        mock_base_settings.cluster.get.side_effect = lambda key, default=None: {"user": "ceph"}.get(key, default)
        monitor = PerfMonitoring({"args": _ARGS})

        result = monitor.get_cpu_cycles("/tmp/output")

    assert result is None
    warning_calls = [str(c) for c in mock_logger.warning.call_args_list]
    assert any("no perf stat files" in c for c in warning_calls)


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
    check_runner = MagicMock()
    check_runner.communicate.return_value = ("/usr/bin/perf\n", "")
    mkdir_runner = MagicMock()
    local_runner = MagicMock()
    with (
        patch("monitoring.monitoring.settings") as mock_base_settings,
        patch("monitoring.osd_pid_monitoring.settings") as mock_osd_settings,
        patch("monitoring.perf_monitoring.common.pdsh") as mock_pdsh,
        patch("monitoring.osd_pid_monitoring.common.get_localnode", return_value="node1"),
        patch("monitoring.osd_pid_monitoring.common.sh", return_value=local_runner) as mock_sh,
        patch("monitoring.osd_pid_monitoring._glob.glob", return_value=["/var/run/ceph/osd.1.pid"]),
        patch("builtins.open", mock_open(read_data="123\n")),
    ):
        mock_base_settings.getnodes.return_value = "resolved-nodes"
        mock_base_settings.cluster.get.side_effect = lambda key, default=None: {"user": "ceph"}.get(key, default)
        mock_osd_settings.cluster.get.side_effect = lambda key, default=None: {"pid_dir": "/var/run/ceph"}.get(
            key, default
        )
        mock_pdsh.side_effect = [check_runner, mkdir_runner]
        monitor = OsdPerfMonitoring({"args": _ARGS})

        monitor.start("/tmp/output")

    mock_pdsh.assert_any_call("resolved-nodes", "command -v perf", continue_if_error=False)
    mock_pdsh.assert_any_call("resolved-nodes", "mkdir -p -m0755 -- /tmp/output/perf")
    mkdir_runner.communicate.assert_called_once_with()
    mock_sh.assert_called_once_with("node1", "sudo perf stat -p 123 -o /tmp/output/perf/perf_stat.123")
    assert monitor._perf_runners == [local_runner]
    assert monitor._perf_dir == "/tmp/output/perf"


def test_osd_perf_start_remote_node_uses_pdsh_loop() -> None:
    """OsdPerfMonitoring.start() dispatches via pdsh for-loop when no local node is available."""
    check_runner = MagicMock()
    check_runner.communicate.return_value = ("/usr/bin/perf\n", "")
    mkdir_runner = MagicMock()
    ls_runner = MagicMock()
    ls_runner.communicate.return_value = ("/var/run/ceph/osd.1.pid\n", "")
    remote_runner = MagicMock()
    with (
        patch("monitoring.monitoring.settings") as mock_base_settings,
        patch("monitoring.osd_pid_monitoring.settings") as mock_osd_settings,
        patch("common.pdsh") as mock_pdsh,
        patch("monitoring.osd_pid_monitoring.common.get_localnode", return_value=None),
    ):
        mock_base_settings.getnodes.return_value = "resolved-nodes"
        mock_base_settings.cluster.get.side_effect = lambda key, default=None: {"user": "ceph"}.get(key, default)
        mock_osd_settings.cluster.get.side_effect = lambda key, default=None: {"pid_dir": "/var/run/ceph"}.get(
            key, default
        )
        mock_pdsh.side_effect = [check_runner, mkdir_runner, ls_runner, remote_runner]
        OsdPerfMonitoring({"args": _ARGS}).start("/tmp/output")

    mock_pdsh.assert_any_call("resolved-nodes", "command -v perf", continue_if_error=False)
    mock_pdsh.assert_any_call("resolved-nodes", "mkdir -p -m0755 -- /tmp/output/perf")
    mock_pdsh.assert_any_call("resolved-nodes", "ls /var/run/ceph/osd.*.pid 2>/dev/null")
    expected_loop = (
        'for f in /var/run/ceph/osd.*.pid; do pid=$(cat "$f");'
        ' sudo perf stat -p "$pid" -o /tmp/output/perf/perf_stat."$pid"; done'
    )  # {output_dir} resolved
    mock_pdsh.assert_any_call("resolved-nodes", expected_loop)
    remote_runner.communicate.assert_called_once_with()


def test_osd_perf_start_local_node_warns_when_no_pid_files() -> None:
    """OsdPerfMonitoring.start() logs a warning when no local PID files are found."""
    check_runner = MagicMock()
    check_runner.communicate.return_value = ("/usr/bin/perf\n", "")
    mkdir_runner = MagicMock()
    with (
        patch("monitoring.monitoring.settings") as mock_base_settings,
        patch("monitoring.osd_pid_monitoring.settings") as mock_osd_settings,
        patch("monitoring.perf_monitoring.common.pdsh") as mock_pdsh,
        patch("monitoring.osd_pid_monitoring.common.get_localnode", return_value="node1"),
        patch("monitoring.osd_pid_monitoring._glob.glob", return_value=[]),
        patch("monitoring.osd_pid_monitoring.logger") as mock_logger,
    ):
        mock_base_settings.getnodes.return_value = "resolved-nodes"
        mock_base_settings.cluster.get.side_effect = lambda key, default=None: {"user": "ceph"}.get(key, default)
        mock_osd_settings.cluster.get.side_effect = lambda key, default=None: {"pid_dir": "/var/run/ceph"}.get(
            key, default
        )
        mock_pdsh.side_effect = [check_runner, mkdir_runner]
        OsdPerfMonitoring({"args": _ARGS}).start("/tmp/output")

    warning_calls = [str(c) for c in mock_logger.warning.call_args_list]
    assert any("no PID files" in c for c in warning_calls)


def test_osd_perf_start_remote_node_warns_when_no_pid_files() -> None:
    """OsdPerfMonitoring.start() logs a warning when the remote ls finds no PID files."""
    check_runner = MagicMock()
    check_runner.communicate.return_value = ("/usr/bin/perf\n", "")
    mkdir_runner = MagicMock()
    ls_runner = MagicMock()
    ls_runner.communicate.return_value = ("", "")
    remote_runner = MagicMock()
    with (
        patch("monitoring.monitoring.settings") as mock_base_settings,
        patch("monitoring.osd_pid_monitoring.settings") as mock_osd_settings,
        patch("common.pdsh") as mock_pdsh,
        patch("monitoring.osd_pid_monitoring.common.get_localnode", return_value=None),
        patch("monitoring.osd_pid_monitoring.logger") as mock_logger,
    ):
        mock_base_settings.getnodes.return_value = "resolved-nodes"
        mock_base_settings.cluster.get.side_effect = lambda key, default=None: {"user": "ceph"}.get(key, default)
        mock_osd_settings.cluster.get.side_effect = lambda key, default=None: {"pid_dir": "/var/run/ceph"}.get(
            key, default
        )
        mock_pdsh.side_effect = [check_runner, mkdir_runner, ls_runner, remote_runner]
        OsdPerfMonitoring({"args": _ARGS}).start("/tmp/output")

    warning_calls = [str(c) for c in mock_logger.warning.call_args_list]
    assert any("no PID files" in c for c in warning_calls)


def test_osd_perf_stop_inherited_from_perf_monitoring() -> None:
    """OsdPerfMonitoring inherits stop() from PerfMonitoring without override."""
    assert OsdPerfMonitoring.stop is PerfMonitoring.stop


def test_perf_init_raises_when_args_missing() -> None:
    """PerfMonitoring.__init__ raises ValueError when 'args' is absent from mconfig."""
    with patch("monitoring.monitoring.settings") as mock_base_settings:
        mock_base_settings.getnodes.return_value = "resolved-nodes"
        mock_base_settings.cluster.get.side_effect = lambda key, default=None: {"user": "ceph"}.get(key, default)
        with pytest.raises(ValueError, match="args"):
            PerfMonitoring({})


def test_osd_perf_init_raises_when_args_missing() -> None:
    """OsdPerfMonitoring.__init__ raises ValueError when 'args' is absent from mconfig."""
    with (
        patch("monitoring.monitoring.settings") as mock_base_settings,
        patch("monitoring.osd_pid_monitoring.settings") as mock_osd_settings,
    ):
        mock_base_settings.getnodes.return_value = "resolved-nodes"
        mock_base_settings.cluster.get.side_effect = lambda key, default=None: {"user": "ceph"}.get(key, default)
        mock_osd_settings.cluster.get.side_effect = lambda key, default=None: {"pid_dir": "/var/run/ceph"}.get(
            key, default
        )
        with pytest.raises(ValueError, match="args"):
            OsdPerfMonitoring({})


# ---------------------------------------------------------------------------
# Tool availability checks
# ---------------------------------------------------------------------------


def test_perf_start_raises_when_perf_not_installed() -> None:
    """PerfMonitoring.start() raises RuntimeError when perf is not on the nodes."""
    with patch("monitoring.monitoring.settings") as mock_base_settings:
        mock_base_settings.getnodes.return_value = "resolved-nodes"
        mock_base_settings.cluster.get.side_effect = lambda key, default=None: {"user": "ceph"}.get(key, default)
        monitor = PerfMonitoring({"args": "stat -o {perf_dir}/out"})

    with patch("monitoring.perf_monitoring.common.pdsh") as mock_pdsh:
        mock_pdsh.return_value.communicate.side_effect = Exception("exit 1")
        with pytest.raises(RuntimeError, match="perf"):
            monitor.start("/tmp/output")


def test_perf_start_proceeds_when_perf_is_installed() -> None:
    """PerfMonitoring.start() continues normally when perf is found on the nodes."""
    check_runner = MagicMock()
    check_runner.communicate.return_value = ("/usr/bin/perf\n", "")
    mkdir_runner = MagicMock()
    local_runner = MagicMock()
    with (
        patch("monitoring.monitoring.settings") as mock_base_settings,
        patch("monitoring.perf_monitoring.common.pdsh") as mock_pdsh,
        patch("monitoring.perf_monitoring.common.get_localnode", return_value="node1"),
        patch("monitoring.perf_monitoring.common.sh", return_value=local_runner),
    ):
        mock_base_settings.getnodes.return_value = "resolved-nodes"
        mock_base_settings.cluster.get.side_effect = lambda key, default=None: {"user": "ceph"}.get(key, default)
        mock_pdsh.side_effect = [check_runner, mkdir_runner]
        monitor = PerfMonitoring({"args": "stat -o {perf_dir}/out"})
        monitor.start("/tmp/output")

    assert monitor._perf_dir == "/tmp/output/perf"


def test_osd_perf_start_raises_when_perf_not_installed() -> None:
    """OsdPerfMonitoring.start() raises RuntimeError when perf is not on the nodes."""
    with (
        patch("monitoring.monitoring.settings") as mock_base_settings,
        patch("monitoring.osd_pid_monitoring.settings") as mock_osd_settings,
    ):
        mock_base_settings.getnodes.return_value = "resolved-nodes"
        mock_base_settings.cluster.get.side_effect = lambda key, default=None: {"user": "ceph"}.get(key, default)
        mock_osd_settings.cluster.get.side_effect = lambda key, default=None: {"pid_dir": "/var/run/ceph"}.get(
            key, default
        )
        monitor = OsdPerfMonitoring({"args": _ARGS})

    with patch("monitoring.perf_monitoring.common.pdsh") as mock_pdsh:
        mock_pdsh.return_value.communicate.side_effect = Exception("exit 1")
        with pytest.raises(RuntimeError, match="perf"):
            monitor.start("/tmp/output")
