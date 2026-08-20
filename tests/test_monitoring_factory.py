"""Tests for MonitoringFactory."""

# pylint: disable=protected-access

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from monitoring.blktrace_monitoring import BlktraceMonitoring
from monitoring.collectl_monitoring import CollectlMonitoring
from monitoring.monitoring_factory import MonitoringFactory
from monitoring.osd_perf_monitoring import OsdPerfMonitoring
from monitoring.osd_top_monitoring import OsdTopMonitoring
from monitoring.perf_monitoring import PerfMonitoring
from monitoring.top_monitoring import TopMonitoring

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _patch_settings(profiles: dict[str, Any]) -> Any:
    """Return a context manager that patches settings.monitoring_profiles."""
    return patch("monitoring.monitoring_factory.settings.monitoring_profiles", profiles)


def _stub_monitor() -> MagicMock:
    """Return a MagicMock that behaves like a Monitoring instance."""
    m = MagicMock()
    m.start = MagicMock()
    m.stop = MagicMock()
    return m


# ---------------------------------------------------------------------------
# get_object
# ---------------------------------------------------------------------------


def test_get_object_returns_collectl_monitoring() -> None:
    """get_object('collectl') returns a CollectlMonitoring instance."""
    with patch("monitoring.monitoring.settings") as mock_base_settings:
        mock_base_settings.getnodes.return_value = "node1"
        instance = MonitoringFactory.get_object("collectl", {})
    assert isinstance(instance, CollectlMonitoring)


def test_get_object_returns_perf_monitoring() -> None:
    """get_object('perf') returns a PerfMonitoring instance."""
    with patch("monitoring.monitoring.settings") as mock_base_settings:
        mock_base_settings.getnodes.return_value = "node1"
        mock_base_settings.cluster.get.return_value = "dummy"
        instance = MonitoringFactory.get_object("perf", {"args": "record"})
    assert isinstance(instance, PerfMonitoring)


def test_get_object_returns_blktrace_monitoring() -> None:
    """get_object('blktrace') returns a BlktraceMonitoring instance."""
    with (
        patch("monitoring.monitoring.settings") as mock_base_settings,
        patch("monitoring.blktrace_monitoring.settings") as mock_settings,
    ):
        mock_base_settings.getnodes.return_value = "node1"
        mock_base_settings.cluster.get.return_value = "dummy"
        mock_settings.cluster.get.return_value = "dummy"
        instance = MonitoringFactory.get_object("blktrace", {})
    assert isinstance(instance, BlktraceMonitoring)


def test_get_object_returns_top_monitoring() -> None:
    """get_object('top') returns a TopMonitoring instance."""
    with patch("monitoring.monitoring.settings") as mock_base_settings:
        mock_base_settings.getnodes.return_value = "node1"
        mock_base_settings.cluster.get.return_value = "dummy"
        instance = MonitoringFactory.get_object("top", {})
    assert isinstance(instance, TopMonitoring)


def test_get_object_returns_osd_top_monitoring() -> None:
    """get_object('osd_top') returns an OsdTopMonitoring instance."""
    with patch("monitoring.monitoring.settings") as mock_base_settings:
        mock_base_settings.getnodes.return_value = "node1"
        mock_base_settings.cluster.get.return_value = "dummy"
        instance = MonitoringFactory.get_object("osd_top", {})
    assert isinstance(instance, OsdTopMonitoring)


def test_get_object_returns_osd_perf_monitoring() -> None:
    """get_object('osd_perf') returns an OsdPerfMonitoring instance."""
    with patch("monitoring.monitoring.settings") as mock_base_settings:
        mock_base_settings.getnodes.return_value = "node1"
        mock_base_settings.cluster.get.return_value = "dummy"
        instance = MonitoringFactory.get_object("osd_perf", {"args": "record"})
    assert isinstance(instance, OsdPerfMonitoring)


def test_get_object_raises_for_unknown_key() -> None:
    """get_object() raises ValueError for an unrecognised backend name."""
    with pytest.raises(ValueError, match="Unknown monitoring backend: 'bogus'"):
        MonitoringFactory.get_object("bogus", {})


# ---------------------------------------------------------------------------
# get_all
# ---------------------------------------------------------------------------


def test_get_all_yields_one_instance_per_profile() -> None:
    """get_all() yields one Monitoring instance for each configured profile."""
    profiles: dict[str, Any] = {"collectl": {}, "perf": {}}
    collectl_inst = _stub_monitor()
    perf_inst = _stub_monitor()

    with (
        _patch_settings(profiles),
        patch.object(MonitoringFactory, "get_object") as mock_get_object,
    ):
        mock_get_object.side_effect = [collectl_inst, perf_inst]
        result = list(MonitoringFactory.get_all())

    assert result == [collectl_inst, perf_inst]
    mock_get_object.assert_any_call("collectl", {})
    mock_get_object.assert_any_call("perf", {})


def test_get_all_yields_nothing_when_no_profiles() -> None:
    """get_all() yields nothing when monitoring_profiles is empty."""
    with _patch_settings({}):
        result = list(MonitoringFactory.get_all())
    assert not result


# ---------------------------------------------------------------------------
# start
# ---------------------------------------------------------------------------


def test_start_calls_start_on_every_monitor() -> None:
    """start() calls m.start(directory) for every monitor returned by get_all."""
    m1, m2 = _stub_monitor(), _stub_monitor()
    with patch.object(MonitoringFactory, "get_all", return_value=iter([m1, m2])):
        MonitoringFactory.start("/tmp/out")

    m1.start.assert_called_once_with("/tmp/out")
    m2.start.assert_called_once_with("/tmp/out")


# ---------------------------------------------------------------------------
# stop
# ---------------------------------------------------------------------------


def test_stop_calls_stop_on_every_monitor() -> None:
    """stop() calls m.stop(directory) for every monitor returned by get_all."""
    m1, m2 = _stub_monitor(), _stub_monitor()
    with patch.object(MonitoringFactory, "get_all", return_value=iter([m1, m2])):
        MonitoringFactory.stop("/tmp/out")

    m1.stop.assert_called_once_with("/tmp/out")
    m2.stop.assert_called_once_with("/tmp/out")


def test_stop_passes_none_by_default() -> None:
    """stop() passes None as directory when called with no argument."""
    m = _stub_monitor()
    with patch.object(MonitoringFactory, "get_all", return_value=iter([m])):
        MonitoringFactory.stop()

    m.stop.assert_called_once_with(None)


# ---------------------------------------------------------------------------
# monitor (context manager)
# ---------------------------------------------------------------------------


def test_monitor_starts_then_stops_all() -> None:
    """monitor() starts all monitors before yield and stops all after."""
    m1, m2 = _stub_monitor(), _stub_monitor()
    call_order: list[str] = []

    m1.start.side_effect = lambda d: call_order.append("m1.start")
    m2.start.side_effect = lambda d: call_order.append("m2.start")
    m1.stop.side_effect = lambda d: call_order.append("m1.stop")
    m2.stop.side_effect = lambda d: call_order.append("m2.stop")

    with (patch.object(MonitoringFactory, "get_all", return_value=iter([m1, m2])),):
        with MonitoringFactory.monitor("/tmp/out"):
            call_order.append("body")

    assert call_order == ["m1.start", "m2.start", "body", "m1.stop", "m2.stop"]


def test_monitor_stops_all_even_if_body_raises() -> None:
    """monitor() runs stop for all monitors even when the body raises."""
    m = _stub_monitor()
    with patch.object(MonitoringFactory, "get_all", return_value=iter([m])):
        with pytest.raises(RuntimeError):
            with MonitoringFactory.monitor("/tmp/out"):
                raise RuntimeError("boom")

    m.stop.assert_called_once_with("/tmp/out")


# ---------------------------------------------------------------------------
# get_cpu_cycles
# ---------------------------------------------------------------------------


def test_get_cpu_cycles_delegates_to_perf_monitor() -> None:
    """get_cpu_cycles() returns the value from the first PerfMonitoring instance."""
    perf_inst = MagicMock(spec=PerfMonitoring)
    perf_inst.get_cpu_cycles.return_value = 42000

    with patch.object(MonitoringFactory, "get_all", return_value=iter([perf_inst])):
        result = MonitoringFactory.get_cpu_cycles("/tmp/out")

    assert result == 42000
    perf_inst.get_cpu_cycles.assert_called_once_with("/tmp/out")


def test_get_cpu_cycles_skips_non_perf_monitors() -> None:
    """get_cpu_cycles() skips non-PerfMonitoring instances."""
    collectl_inst = MagicMock(spec=CollectlMonitoring)
    perf_inst = MagicMock(spec=PerfMonitoring)
    perf_inst.get_cpu_cycles.return_value = 99

    with patch.object(MonitoringFactory, "get_all", return_value=iter([collectl_inst, perf_inst])):
        result = MonitoringFactory.get_cpu_cycles("/tmp/out")

    assert result == 99
    assert not hasattr(collectl_inst, "get_cpu_cycles") or not collectl_inst.get_cpu_cycles.called


def test_get_cpu_cycles_returns_none_when_no_perf_configured() -> None:
    """get_cpu_cycles() returns None when no PerfMonitoring is in the profiles."""
    collectl_inst = MagicMock(spec=CollectlMonitoring)

    with patch.object(MonitoringFactory, "get_all", return_value=iter([collectl_inst])):
        result = MonitoringFactory.get_cpu_cycles("/tmp/out")

    assert result is None


def test_get_cpu_cycles_returns_none_when_profiles_empty() -> None:
    """get_cpu_cycles() returns None when no profiles are configured at all."""
    with patch.object(MonitoringFactory, "get_all", return_value=iter([])):
        result = MonitoringFactory.get_cpu_cycles("/tmp/out")

    assert result is None
