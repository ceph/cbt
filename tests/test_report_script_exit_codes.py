"""
Unit tests for the process exit status of the post-processing helper scripts in tools/

The scripts report failure through the integer returned by main(). That value
only reaches the calling shell, CI job or automation if the script passes it to
sys.exit(), so these tests run each script as __main__ and check the exit status.
"""

import runpy
import sys
from pathlib import Path

import pytest

from post_processing.formatter.common_output_formatter import CommonOutputFormatter
from post_processing.formatter.time_series_output_formatter import TimeSeriesOutputFormatter
from post_processing.report import Report

TOOLS_DIRECTORY: Path = Path(__file__).resolve().parent.parent / "tools"

REPORT_SCRIPTS: list[str] = [
    "generate_performance_report.py",
    "generate_timeseries_performance_report.py",
]


def _run_script_as_main(script_name: str, arguments: list[str], monkeypatch: pytest.MonkeyPatch) -> int:
    """
    Run a script from tools/ as __main__ and return the exit status the shell would see.
    """
    monkeypatch.setattr(sys, "argv", [script_name, *arguments])
    try:
        runpy.run_path(str(TOOLS_DIRECTORY / script_name), run_name="__main__")
    except SystemExit as exit_request:
        if exit_request.code is None:
            return 0
        assert isinstance(exit_request.code, int), f"Unexpected exit value {exit_request.code!r}"
        return exit_request.code

    return 0


def _report_arguments(script_name: str, tmp_path: Path) -> list[str]:
    """
    The command line for a report script that points at an archive that does not exist.
    """
    if script_name == "generate_comparison_performance_report.py":
        return [
            f"--baseline={tmp_path / 'missing_baseline'}",
            f"--archives={tmp_path / 'missing_archive'}",
            f"--output_directory={tmp_path / 'out'}",
        ]
    return [f"--archive={tmp_path / 'missing_archive'}", f"--output_directory={tmp_path / 'out'}"]


@pytest.mark.parametrize("script_name", [*REPORT_SCRIPTS, "generate_comparison_performance_report.py"])
def test_report_script_exits_non_zero_when_report_generation_fails(
    script_name: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    An archive that does not exist cannot produce a report, so the script must not exit with 0
    """
    exit_status: int = _run_script_as_main(script_name, _report_arguments(script_name, tmp_path), monkeypatch)

    assert exit_status != 0, f"{script_name} exited with 0 although report generation failed"


@pytest.mark.parametrize("script_name", [*REPORT_SCRIPTS, "generate_comparison_performance_report.py"])
def test_report_script_exits_zero_when_report_generation_succeeds(
    script_name: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    A successful run must still exit with 0
    """
    monkeypatch.setattr(Report, "generate", lambda self, throw_exception=False: None)

    exit_status: int = _run_script_as_main(script_name, _report_arguments(script_name, tmp_path), monkeypatch)

    assert exit_status == 0, f"{script_name} exited with {exit_status} although report generation succeeded"


def _fail_to_process(self: object) -> None:  # pylint: disable=unused-argument
    raise ValueError("simulated parsing failure")


def _do_nothing(self: object) -> None:  # pylint: disable=unused-argument
    return None


@pytest.mark.parametrize(
    ("formatter_class", "other_formatter_class"),
    [
        (CommonOutputFormatter, TimeSeriesOutputFormatter),
        (TimeSeriesOutputFormatter, CommonOutputFormatter),
    ],
)
def test_fio_common_output_wrapper_exits_non_zero_when_either_formatter_fails(
    formatter_class: type,
    other_formatter_class: type,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    A failure in either of the two formatters must be visible in the exit status
    """
    monkeypatch.setattr(formatter_class, "process", _fail_to_process)
    monkeypatch.setattr(other_formatter_class, "process", _do_nothing)

    exit_status: int = _run_script_as_main(
        "fio_common_output_wrapper.py", [f"--archive={tmp_path / 'archive'}"], monkeypatch
    )

    assert exit_status != 0, f"fio_common_output_wrapper.py exited with 0 although {formatter_class.__name__} failed"


def test_fio_common_output_wrapper_exits_zero_when_both_formatters_succeed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    A successful run must still exit with 0
    """
    monkeypatch.setattr(CommonOutputFormatter, "process", _do_nothing)
    monkeypatch.setattr(TimeSeriesOutputFormatter, "process", _do_nothing)

    exit_status: int = _run_script_as_main(
        "fio_common_output_wrapper.py", [f"--archive={tmp_path / 'archive'}"], monkeypatch
    )

    assert exit_status == 0, f"fio_common_output_wrapper.py exited with {exit_status} although both formatters worked"
