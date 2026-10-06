"""Tests for the collectl monitoring backend."""

# pylint: disable=protected-access

from unittest.mock import MagicMock, patch

from monitoring.collectl_monitoring import CollectlMonitoring


def test_init_sets_default_args() -> None:
    """Use the default collectl argument string when args are not configured."""
    with patch("monitoring.monitoring.settings") as mock_settings:
        mock_settings.getnodes.return_value = "resolved-nodes"
        mock_settings.cluster.get.return_value = "ceph"

        monitor = CollectlMonitoring({})

    assert monitor._args == CollectlMonitoring.DEFAULT_ARGS


def test_init_uses_default_args_when_args_is_dict() -> None:
    """Fall back to default args when yaml sets args to an empty dict."""
    with patch("monitoring.monitoring.settings") as mock_settings:
        mock_settings.getnodes.return_value = "resolved-nodes"
        mock_settings.cluster.get.return_value = "ceph"

        monitor = CollectlMonitoring({"args": {}})

    assert monitor._args == CollectlMonitoring.DEFAULT_ARGS


def test_init_uses_custom_args() -> None:
    """Use custom collectl args from monitoring config when provided."""
    with patch("monitoring.monitoring.settings") as mock_settings:
        mock_settings.getnodes.return_value = "resolved-nodes"
        mock_settings.cluster.get.return_value = "ceph"

        monitor = CollectlMonitoring({"args": "--custom {collectl_dir}"})

    assert monitor._args == "--custom {collectl_dir}"


def test_start_creates_directory_and_starts_collectl() -> None:
    """Create the collectl directory and invoke collectl through pdsh."""
    check_runner = MagicMock()
    check_runner.communicate.return_value = ("/usr/bin/collectl\n", "")
    mkdir_runner = MagicMock()
    with (
        patch("monitoring.monitoring.settings") as mock_settings,
        patch("monitoring.collectl_monitoring.common.pdsh") as mock_pdsh,
    ):
        mock_settings.getnodes.return_value = "resolved-nodes"
        mock_settings.cluster.get.return_value = "ceph"
        mock_pdsh.side_effect = [check_runner, mkdir_runner, MagicMock()]
        monitor = CollectlMonitoring({})

        monitor.start("/tmp/output")

    mock_pdsh.assert_any_call("resolved-nodes", "command -v collectl", continue_if_error=False)
    mock_pdsh.assert_any_call("resolved-nodes", "mkdir -p -m0755 -- /tmp/output/collectl")
    mkdir_runner.communicate.assert_called_once_with()
    mock_pdsh.assert_any_call(
        "resolved-nodes",
        f"collectl {monitor._args.format(collectl_dir='/tmp/output/collectl')}",
    )


def test_stop_calls_pdsh_with_collectl_pkill() -> None:
    """Stop collectl processes through pdsh."""
    stop_runner = MagicMock()
    with (
        patch("monitoring.monitoring.settings") as mock_settings,
        patch("monitoring.collectl_monitoring.common.pdsh", return_value=stop_runner) as mock_pdsh,
    ):
        mock_settings.getnodes.return_value = "resolved-nodes"
        mock_settings.cluster.get.return_value = "ceph"
        monitor = CollectlMonitoring({})

        monitor.stop(None)

    mock_pdsh.assert_called_once_with("resolved-nodes", "pkill -SIGINT -f collectl")
    stop_runner.communicate.assert_called_once_with()


def test_default_nodes_matches_collectl_configuration() -> None:
    """Expose the expected default node groups for collectl monitoring."""
    assert CollectlMonitoring.DEFAULT_NODES == ["clients", "osds", "mons", "rgws"]


# ---------------------------------------------------------------------------
# Tool availability checks
# ---------------------------------------------------------------------------


def test_collectl_start_skips_when_collectl_not_installed() -> None:
    """CollectlMonitoring.start() skips all work and logs a warning when collectl is absent."""
    with patch("monitoring.monitoring.settings") as mock_settings:
        mock_settings.getnodes.return_value = "resolved-nodes"
        mock_settings.cluster.get.return_value = "ceph"
        monitor = CollectlMonitoring({})

    with (
        patch("monitoring.collectl_monitoring.common.pdsh") as mock_collectl_pdsh,
        patch("monitoring.monitoring.logger") as mock_logger,
    ):
        mock_collectl_pdsh.return_value.communicate.side_effect = Exception("exit 1")
        monitor.start("/tmp/output")

    # mkdir and collectl commands must never have been called (only the check)
    assert mock_collectl_pdsh.call_count == 1
    mock_collectl_pdsh.assert_called_once_with("resolved-nodes", "command -v collectl", continue_if_error=False)
    warning_calls = [str(c) for c in mock_logger.warning.call_args_list]
    assert any("collectl" in c for c in warning_calls)


def test_collectl_start_does_not_raise_when_collectl_not_installed() -> None:
    """CollectlMonitoring.start() must not raise even when collectl is absent."""
    with patch("monitoring.monitoring.settings") as mock_settings:
        mock_settings.getnodes.return_value = "resolved-nodes"
        mock_settings.cluster.get.return_value = "ceph"
        monitor = CollectlMonitoring({})

    with patch("monitoring.collectl_monitoring.common.pdsh") as mock_pdsh:
        mock_pdsh.return_value.communicate.side_effect = Exception("exit 1")
        monitor.start("/tmp/output")  # must not raise


def test_collectl_start_proceeds_when_collectl_is_installed() -> None:
    """CollectlMonitoring.start() creates directory and starts collectl when present."""
    check_runner = MagicMock()
    check_runner.communicate.return_value = ("/usr/bin/collectl\n", "")
    mkdir_runner = MagicMock()
    collectl_runner = MagicMock()

    with (
        patch("monitoring.monitoring.settings") as mock_settings,
        patch("monitoring.collectl_monitoring.common.pdsh") as mock_pdsh,
    ):
        mock_settings.getnodes.return_value = "resolved-nodes"
        mock_settings.cluster.get.return_value = "ceph"
        mock_pdsh.side_effect = [check_runner, mkdir_runner, collectl_runner]
        monitor = CollectlMonitoring({})
        monitor.start("/tmp/output")

    mock_pdsh.assert_any_call("resolved-nodes", "mkdir -p -m0755 -- /tmp/output/collectl")
