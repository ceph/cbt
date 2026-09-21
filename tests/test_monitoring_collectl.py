"""Tests for the collectl monitoring backend."""

# pylint: disable=protected-access

from unittest.mock import MagicMock, patch

from monitoring.collectl_monitoring import CollectlMonitoring


def test_init_sets_default_args() -> None:
    """Use the default collectl argument string when args are not configured."""
    with patch("monitoring.monitoring.settings") as mock_settings:
        mock_settings.getnodes.return_value = "resolved-nodes"

        monitor = CollectlMonitoring({})

    assert monitor._args == CollectlMonitoring.DEFAULT_ARGS


def test_init_uses_custom_args() -> None:
    """Use custom collectl args from monitoring config when provided."""
    with patch("monitoring.monitoring.settings") as mock_settings:
        mock_settings.getnodes.return_value = "resolved-nodes"

        monitor = CollectlMonitoring({"args": "--custom {collectl_dir}"})

    assert monitor._args == "--custom {collectl_dir}"


def test_start_creates_directory_and_starts_collectl() -> None:
    """Create the collectl directory and invoke collectl through pdsh."""
    mkdir_runner = MagicMock()
    with (
        patch("monitoring.monitoring.settings") as mock_settings,
        patch("monitoring.collectl_monitoring.common.pdsh") as mock_pdsh,
    ):
        mock_settings.getnodes.return_value = "resolved-nodes"
        mock_pdsh.side_effect = [mkdir_runner, MagicMock()]
        monitor = CollectlMonitoring({})

        monitor.start("/tmp/output")

    mock_pdsh.assert_any_call("resolved-nodes", "mkdir -p -m0755 -- /tmp/output/collectl")
    mkdir_runner.communicate.assert_called_once_with()
    mock_pdsh.assert_any_call(
        "resolved-nodes",
        ["collectl", monitor._args.format(collectl_dir="/tmp/output/collectl")],
    )


def test_stop_calls_pdsh_with_collectl_pkill() -> None:
    """Stop collectl processes through pdsh."""
    stop_runner = MagicMock()
    with (
        patch("monitoring.monitoring.settings") as mock_settings,
        patch("monitoring.collectl_monitoring.common.pdsh", return_value=stop_runner) as mock_pdsh,
    ):
        mock_settings.getnodes.return_value = "resolved-nodes"
        monitor = CollectlMonitoring({})

        monitor.stop(None)

    mock_pdsh.assert_called_once_with("resolved-nodes", "pkill -SIGINT -f collectl")
    stop_runner.communicate.assert_called_once_with()


def test_default_nodes_matches_collectl_configuration() -> None:
    """Expose the expected default node groups for collectl monitoring."""
    assert CollectlMonitoring.DEFAULT_NODES == ["clients", "osds", "mons", "rgws"]
