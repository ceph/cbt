"""Tests for the Monitoring base class helpers."""

# pylint: disable=protected-access

from typing import ClassVar
from unittest.mock import MagicMock, patch

import pytest

from monitoring.monitoring import Monitoring

# ---------------------------------------------------------------------------
# Concrete stub to instantiate the abstract base
# ---------------------------------------------------------------------------


class _StubMonitoring(Monitoring):
    """Minimal concrete subclass used only for testing base-class helpers."""

    DEFAULT_NODES: ClassVar[list[str]] = ["osds"]

    def start(self, directory: str) -> None:  # pragma: no cover
        pass

    def stop(self, directory):  # pragma: no cover
        pass


def _make_stub(nodes: str = "node1") -> _StubMonitoring:
    with patch("monitoring.monitoring.settings") as mock_settings:
        mock_settings.getnodes.return_value = nodes
        mock_settings.cluster.get.return_value = "ceph"
        return _StubMonitoring({})


# ---------------------------------------------------------------------------
# _check_tool — fatal=True (default)
# ---------------------------------------------------------------------------


def test_check_tool_returns_true_when_tool_found() -> None:
    """_check_tool returns True when command -v succeeds on all nodes."""
    ok_runner = MagicMock()
    ok_runner.communicate.return_value = ("/usr/bin/perf\n", "")
    stub = _make_stub()
    with patch("monitoring.monitoring.common.pdsh", return_value=ok_runner):
        assert stub._check_tool("perf") is True


def test_check_tool_raises_runtime_error_when_tool_missing() -> None:
    """_check_tool raises RuntimeError when command -v fails on any node."""
    stub = _make_stub()
    with patch("monitoring.monitoring.common.pdsh") as mock_pdsh:
        mock_pdsh.return_value.communicate.side_effect = Exception("exit 1")
        with pytest.raises(RuntimeError, match="perf"):
            stub._check_tool("perf")


def test_check_tool_error_message_includes_nodes() -> None:
    """_check_tool RuntimeError message contains the node list."""
    stub = _make_stub(nodes="osd-node1,osd-node2")
    with patch("monitoring.monitoring.common.pdsh") as mock_pdsh:
        mock_pdsh.return_value.communicate.side_effect = Exception("exit 1")
        with pytest.raises(RuntimeError, match="osd-node1,osd-node2"):
            stub._check_tool("blktrace")


def test_check_tool_uses_continue_if_error_false() -> None:
    """_check_tool passes continue_if_error=False so pdsh raises on non-zero exit."""
    ok_runner = MagicMock()
    ok_runner.communicate.return_value = ("/usr/bin/top\n", "")
    stub = _make_stub()
    with patch("monitoring.monitoring.common.pdsh", return_value=ok_runner) as mock_pdsh:
        stub._check_tool("top")
    mock_pdsh.assert_called_once_with("node1", "command -v top", continue_if_error=False)


# ---------------------------------------------------------------------------
# _check_tool — fatal=False
# ---------------------------------------------------------------------------


def test_check_tool_fatal_false_returns_true_when_tool_found() -> None:
    """_check_tool(fatal=False) returns True when the tool is present."""
    ok_runner = MagicMock()
    ok_runner.communicate.return_value = ("/usr/bin/collectl\n", "")
    stub = _make_stub()
    with patch("monitoring.monitoring.common.pdsh", return_value=ok_runner):
        assert stub._check_tool("collectl", fatal=False) is True


def test_check_tool_fatal_false_returns_false_when_tool_missing() -> None:
    """_check_tool(fatal=False) returns False (does not raise) when tool is absent."""
    stub = _make_stub()
    with patch("monitoring.monitoring.common.pdsh") as mock_pdsh:
        mock_pdsh.return_value.communicate.side_effect = Exception("exit 1")
        assert stub._check_tool("collectl", fatal=False) is False


def test_check_tool_fatal_false_logs_warning_when_tool_missing() -> None:
    """_check_tool(fatal=False) logs a warning when the tool is absent."""
    stub = _make_stub()
    with (
        patch("monitoring.monitoring.common.pdsh") as mock_pdsh,
        patch("monitoring.monitoring.logger") as mock_logger,
    ):
        mock_pdsh.return_value.communicate.side_effect = Exception("exit 1")
        stub._check_tool("collectl", fatal=False)

    warning_calls = [str(c) for c in mock_logger.warning.call_args_list]
    assert any("collectl" in c for c in warning_calls)


def test_check_tool_fatal_false_does_not_raise_when_tool_missing() -> None:
    """_check_tool(fatal=False) must not raise even when the tool is absent."""
    stub = _make_stub()
    with patch("monitoring.monitoring.common.pdsh") as mock_pdsh:
        mock_pdsh.return_value.communicate.side_effect = Exception("exit 1")
        # should complete without raising
        stub._check_tool("collectl", fatal=False)
