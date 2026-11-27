"""
Loader for regression detection.

Turns two archive directories into paired baseline/candidate workloads, plus
lists of missing (baseline-only) and extra (candidate-only) workload stems.

Raises ArchiveLoadError (maps to CLI exit code 3) for input problems:
nonexistent directories, no intermediate JSON found, unpairable archives,
and unreadable, empty, or corrupt JSON files.

Warns (but continues) for:
- duplicate stems across multiple visualization directories (keeps first seen)
- workloads present in one archive but not the other
- differing runtime_seconds between a paired baseline and candidate
"""

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from post_processing.common import (
    file_is_empty,
    find_hockey_stick_visualisation_directories,
)
from post_processing.post_processing_types import CommonFormatDataType

log = logging.getLogger("cbt.regression.loader")

_TIMESERIES_SUFFIX = "_timeseries"
_JSON_EXT = ".json"


class ArchiveLoadError(Exception):
    """Raised for any input problem that prevents comparison (exit code 3)."""


@dataclass
class WorkloadPair:
    """A matched baseline/candidate workload with their parsed JSON data."""

    stem: str
    baseline_data: CommonFormatDataType
    candidate_data: CommonFormatDataType


def _collect_json_files(directory: Path) -> dict[str, Path]:
    """
    Return a mapping of stem -> Path for all non-timeseries JSON files found
    in every hockey-stick visualisation directory under *directory*.

    Raises ArchiveLoadError if:
    - *directory* does not exist
    - no intermediate JSON is found
    """
    if not directory.exists():
        raise ArchiveLoadError(f"Directory does not exist: {directory}")
    if not directory.is_dir():
        raise ArchiveLoadError(f"Not a directory: {directory}")

    vis_dirs = find_hockey_stick_visualisation_directories(directory)

    stems: dict[str, Path] = {}
    for vis_dir in vis_dirs:
        for json_file in sorted(vis_dir.glob(f"*{_JSON_EXT}")):
            if json_file.stem.endswith(_TIMESERIES_SUFFIX):
                continue
            stem = json_file.stem
            if stem in stems:
                log.warning(
                    "Archive '%s' has duplicate stem '%s' in %s and %s; "
                    "keeping first (results from different iterations or client hosts are not summed)",
                    directory,
                    stem,
                    stems[stem],
                    json_file,
                )
                continue
            stems[stem] = json_file

    if not stems:
        raise ArchiveLoadError(
            f"No intermediate JSON files found under {directory}. "
            f"To create them, run tools/fio_common_output_wrapper.py first."
        )

    return stems


def _load_json(path: Path) -> CommonFormatDataType:
    """
    Load and return the parsed JSON from *path*.

    Raises ArchiveLoadError for empty, unreadable, or corrupt files.
    """
    try:
        if file_is_empty(path):
            raise ArchiveLoadError(f"Intermediate file is empty: {path}")
        parsed = json.loads(path.read_text(encoding="utf8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ArchiveLoadError(f"Cannot read or parse intermediate file {path}: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ArchiveLoadError(f"Intermediate file {path} is not a JSON object: got {type(parsed).__name__}")
    data: CommonFormatDataType = parsed
    return data


def _check_runtime_seconds(
    stem: str,
    baseline_data: CommonFormatDataType,
    candidate_data: CommonFormatDataType,
) -> None:
    """
    Warn if any iodepth entry has a differing runtime_seconds between baseline
    and candidate.  Iodepth entries are the dict-valued keys in the data.
    """
    for key, value in baseline_data.items():
        if not isinstance(value, dict):
            continue
        candidate_entry = candidate_data.get(key)
        if not isinstance(candidate_entry, dict):
            continue
        baseline_rt: Optional[str] = value.get("runtime_seconds")
        candidate_rt: Optional[str] = candidate_entry.get("runtime_seconds")
        if baseline_rt is not None and candidate_rt is not None and baseline_rt != candidate_rt:
            log.warning(
                "runtime_seconds differs for workload '%s' at iodepth %s: baseline=%s candidate=%s",
                stem,
                key,
                baseline_rt,
                candidate_rt,
            )
            return  # one warning per workload is enough


def load_archive_pairs(
    baseline_dir: Path,
    candidate_dir: Path,
) -> tuple[list[WorkloadPair], list[str], list[str]]:
    """
    Load and pair workloads from two archive directories.

    Returns:
        (pairs, missing, extra) where:
        - pairs   — WorkloadPair list for stems present in both archives
        - missing — stem list for stems in baseline but absent from candidate
        - extra   — stem list for stems in candidate but absent from baseline

    Raises ArchiveLoadError (exit 3) for any input problem.
    """
    baseline_stems = _collect_json_files(baseline_dir)
    candidate_stems = _collect_json_files(candidate_dir)

    all_baseline = set(baseline_stems)
    all_candidate = set(candidate_stems)

    common = all_baseline & all_candidate
    missing = sorted(all_baseline - all_candidate)
    extra = sorted(all_candidate - all_baseline)

    for stem in missing:
        log.warning("Workload '%s' is in baseline but missing from candidate", stem)
    for stem in extra:
        log.warning("Workload '%s' is in candidate but not in baseline", stem)

    if not common:
        raise ArchiveLoadError(
            f"No workloads could be paired between {baseline_dir} and {candidate_dir}. "
            f"Baseline stems: {sorted(all_baseline)}. "
            f"Candidate stems: {sorted(all_candidate)}."
        )

    pairs: list[WorkloadPair] = []
    for stem in sorted(common):
        baseline_data = _load_json(baseline_stems[stem])
        candidate_data = _load_json(candidate_stems[stem])
        _check_runtime_seconds(stem, baseline_data, candidate_data)
        pairs.append(WorkloadPair(stem=stem, baseline_data=baseline_data, candidate_data=candidate_data))

    return pairs, missing, extra
