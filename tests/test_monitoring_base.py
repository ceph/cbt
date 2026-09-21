"""Tests for the monitoring base class."""

# pylint: disable=protected-access

from typing import ClassVar, Optional
from unittest.mock import patch

import pytest

from monitoring.monitoring import Monitoring


class MonitoringSubclass(Monitoring):
    """Concrete monitoring subclass used for base class tests."""

    DEFAULT_NODES: ClassVar[list[str]] = ["osds"]

    def start(self, directory: str) -> None:
        pass

    def stop(self, directory: Optional[str]) -> None:
        pass


class MissingDefaultNodesMonitoring(Monitoring):
    """Concrete monitoring subclass without default nodes for error tests."""

    def start(self, directory: str) -> None:
        pass

    def stop(self, directory: Optional[str]) -> None:
        pass


def test_init_uses_explicit_nodes() -> None:
    """Use configured nodes when they are provided in monitoring config."""
    with patch("monitoring.monitoring.settings") as mock_settings:
        mock_settings.getnodes.return_value = "node1,node2"

        monitor = MonitoringSubclass({"nodes": ["clients", "mons"]})

    mock_settings.getnodes.assert_called_once_with("clients", "mons")
    assert monitor._nodes == "node1,node2"


def test_init_falls_back_to_default_nodes() -> None:
    """Use subclass default nodes when config does not provide nodes."""
    with patch("monitoring.monitoring.settings") as mock_settings:
        mock_settings.getnodes.return_value = "osd-node"

        monitor = MonitoringSubclass({})

    mock_settings.getnodes.assert_called_once_with("osds")
    assert monitor._nodes == "osd-node"


def test_init_calls_settings_getnodes_with_resolved_nodes() -> None:
    """Pass the resolved node groups to settings.getnodes."""
    with patch("monitoring.monitoring.settings") as mock_settings:
        mock_settings.getnodes.return_value = "resolved-nodes"

        MonitoringSubclass({"nodes": ["rgws"]})

    mock_settings.getnodes.assert_called_once_with("rgws")


def test_missing_default_nodes_raises_attribute_error() -> None:
    """Raise an attribute error when a subclass omits default nodes."""
    with patch("monitoring.monitoring.settings") as mock_settings:
        mock_settings.getnodes.return_value = "unused"

        with pytest.raises(AttributeError):
            MissingDefaultNodesMonitoring({})
