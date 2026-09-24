"""
iodepth.py — shared iodepth distribution logic.

This module is intentionally dependency-free so it can be imported by both
``benchmark/base_fio.py`` and ``workloads/workload.py`` without creating a
circular import.
"""

import logging

logger = logging.getLogger("cbt")


def get_iodepth_key(config_keys: list[str]) -> str:
    """
    Return ``"total_iodepth"`` if that key is present in ``config_keys``,
    otherwise return ``"iodepth"``.

    This precedence rule is applied identically in ``FioBenchmark.__init__``
    and ``Workload._create_commands_from_options``.

    Examples::

        get_iodepth_key(["iodepth", "mode"])          # → "iodepth"
        get_iodepth_key(["total_iodepth", "mode"])     # → "total_iodepth"
    """
    return "total_iodepth" if "total_iodepth" in config_keys else "iodepth"


def calculate_iodepth_per_target(num_targets: int, iodepth: int, iodepth_key: str) -> dict[int, int]:
    """
    Return a mapping of ``{target_index: iodepth}`` for a single run.

    When ``iodepth_key`` is ``"total_iodepth"``, ``iodepth`` is treated as
    the *total* queue depth and distributed across ``num_targets`` via
    :func:`distribute_total_iodepth`.  When ``iodepth_key`` is ``"iodepth"``,
    every target receives the same ``iodepth`` value.

    Examples::

        calculate_iodepth_per_target(4, 16, "iodepth")        # → {0:16, 1:16, 2:16, 3:16}
        calculate_iodepth_per_target(4, 16, "total_iodepth")  # → {0:4,  1:4,  2:4,  3:4}
        calculate_iodepth_per_target(3, 10, "total_iodepth")  # → {0:4,  1:3,  2:3}
    """
    if iodepth_key == "total_iodepth":
        return distribute_total_iodepth(num_targets, iodepth)
    return {i: iodepth for i in range(num_targets)}


def distribute_total_iodepth(num_targets: int, total_iodepth: int) -> dict[int, int]:
    """
    Distribute ``total_iodepth`` across ``num_targets``.

    Underflow: when ``total_iodepth < num_targets``, the active target count
    is clamped to ``total_iodepth`` (each active target gets iodepth=1).
    Remainder: distributed front-loaded (targets 0..R-1 each get +1).

    Examples::

        distribute_total_iodepth(4, 16)  # → {0:4, 1:4, 2:4, 3:4}
        distribute_total_iodepth(3, 10)  # → {0:4, 1:3, 2:3}  (remainder=1)
        distribute_total_iodepth(5, 2)   # → {0:1, 1:1}        (underflow)
    """
    if num_targets > total_iodepth:
        logger.warning(
            "The total iodepth requested: %s is less than 1 per target (%s volumes/devices). "
            "Number of targets will be reduced from %s to %s.",
            total_iodepth,
            num_targets,
            num_targets,
            total_iodepth,
        )
        num_targets = total_iodepth

    base: int = total_iodepth // num_targets
    remainder: int = total_iodepth % num_targets
    result: dict[int, int] = {}
    for i in range(num_targets):
        result[i] = base + (1 if remainder > 0 else 0)
        if remainder > 0:
            remainder -= 1
    return result
