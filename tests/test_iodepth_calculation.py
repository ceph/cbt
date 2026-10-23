"""
Unit tests for total_iodepth distribution math implemented in
benchmark/iodepth.py and FioBenchmark (benchmark/base_fio.py).

These tests establish a firm behavioural contract for the iodepth distribution
logic lifted from LibrbdFio into FioBenchmark.  Any change to the distribution
algorithm must cause one or more tests here to fail.

Coverage:
  - iodepth key selection (inline in __init__, via calculate_iodepth_per_target)
  - per-target assignment with plain iodepth (calculate_iodepth_per_target "iodepth")
  - total_iodepth evenly divisible across targets
  - total_iodepth with remainder → first R targets get +1
  - total_iodepth < num_targets → underflow clamping (targets reduced)
  - total_iodepth == num_targets → exactly iodepth=1 per target
  - total_iodepth == 1 → single target with iodepth=1
  - large target count to confirm remainder distribution is front-loaded
"""

# pyright: strict, reportPrivateUsage=false

import logging
import unittest
from typing import Any, Optional
from unittest.mock import MagicMock, patch

from benchmark.base_fio import FioBenchmark
from iodepth import distribute_total_iodepth

# ---------------------------------------------------------------------------
# Concrete stub so we can instantiate FioBenchmark
# ---------------------------------------------------------------------------


class _ConcreteFioBenchmark(FioBenchmark):
    """Minimal concrete subclass used purely for testing FioBenchmark."""

    @property
    def num_targets(self) -> int:
        return 1

    @property
    def benchmark_type(self) -> str:
        return "test"

    def setup_targets(self) -> None:
        pass

    def prefill_targets(self) -> None:
        pass

    def cleanup_targets(self) -> None:
        pass

    def _run_io_loop(self) -> None:
        pass


def _make(extra: Optional[dict[str, Any]] = None) -> _ConcreteFioBenchmark:
    """Build a _ConcreteFioBenchmark with minimal config."""
    base_config: dict[str, Any] = {
        "iteration": 0,
        "mode": "write",
        "op_size": 4194304,
        "iodepth": 16,
        "numjobs": 1,
    }
    if extra:
        base_config.update(extra)
    cluster_mock = MagicMock()
    cluster_mock.config = {}
    with (
        patch("settings.getnodes", return_value="fake-client"),
        patch("settings.cluster", {"tmp_dir": "/tmp/cbt-test/"}),
        patch("common.get_osd_ra", return_value="0"),
    ):
        bm = _ConcreteFioBenchmark("/tmp/archive/", cluster_mock, base_config)
    return bm


def _make_with_iodepth(iodepth: int) -> _ConcreteFioBenchmark:
    return _make({"iodepth": iodepth})


# ---------------------------------------------------------------------------
# iodepth key selection (now inline logic, tested via precedence)
# ---------------------------------------------------------------------------


class TestGetIodepthKey(unittest.TestCase):
    """iodepth key precedence: total_iodepth wins when present, else iodepth."""

    def test_returns_iodepth_by_default(self) -> None:
        bm = _make({"iodepth": 8})
        # When only iodepth is present, self.iodepth should equal the iodepth value
        self.assertEqual(bm.iodepth, 8)

    def test_total_iodepth_takes_precedence(self) -> None:
        bm = _make({"iodepth": 8, "total_iodepth": 32})
        self.assertEqual(bm.iodepth, 32)

    def test_total_iodepth_only(self) -> None:
        bm = _make({"total_iodepth": 48})
        self.assertEqual(bm.iodepth, 48)

    def test_calculate_per_target_uses_iodepth_key(self) -> None:
        bm = _make_with_iodepth(8)
        result = bm.calculate_iodepth_per_target(3, "iodepth")
        self.assertEqual(result, {0: 8, 1: 8, 2: 8})

    def test_calculate_per_target_uses_total_iodepth_key(self) -> None:
        bm = _make({"total_iodepth": 10})
        result = bm.calculate_iodepth_per_target(3, "total_iodepth")
        self.assertEqual(sum(result.values()), 10)


# ---------------------------------------------------------------------------
# Flat assignment — equivalent of old _set_iodepth_for_every_volume
# ---------------------------------------------------------------------------


class TestSetIodepthForEveryTarget(unittest.TestCase):
    """calculate_iodepth_per_target('iodepth') assigns the same depth to every target."""

    def test_single_target(self) -> None:
        bm = _make_with_iodepth(16)
        result = bm.calculate_iodepth_per_target(1, "iodepth")
        self.assertEqual(result, {0: 16})

    def test_multiple_targets_all_equal(self) -> None:
        bm = _make_with_iodepth(8)
        result = bm.calculate_iodepth_per_target(4, "iodepth")
        self.assertEqual(result, {0: 8, 1: 8, 2: 8, 3: 8})

    def test_iodepth_one(self) -> None:
        bm = _make_with_iodepth(1)
        result = bm.calculate_iodepth_per_target(3, "iodepth")
        self.assertEqual(result, {0: 1, 1: 1, 2: 1})

    def test_zero_targets_returns_empty_dict(self) -> None:
        bm = _make_with_iodepth(16)
        result = bm.calculate_iodepth_per_target(0, "iodepth")
        self.assertEqual(result, {})


# ---------------------------------------------------------------------------
# Total iodepth distribution — equivalent of old _calculate_iodepth_per_volume_from_total_iodepth
# ---------------------------------------------------------------------------


class TestCalculateIodepthPerTargetFromTotalIodepth(unittest.TestCase):
    """
    distribute_total_iodepth distributes a total queue depth across N targets,
    spreading any remainder to the first R targets.
    """

    # ------------------------------------------------------------------
    # Even division
    # ------------------------------------------------------------------

    def test_evenly_divisible(self) -> None:
        result = distribute_total_iodepth(4, 16)
        self.assertEqual(result, {0: 4, 1: 4, 2: 4, 3: 4})

    def test_single_target_gets_all_depth(self) -> None:
        result = distribute_total_iodepth(1, 32)
        self.assertEqual(result, {0: 32})

    def test_total_equals_num_targets(self) -> None:
        # total_iodepth == num_targets → every target gets exactly 1
        result = distribute_total_iodepth(5, 5)
        self.assertEqual(result, {0: 1, 1: 1, 2: 1, 3: 1, 4: 1})

    # ------------------------------------------------------------------
    # Remainder distribution
    # ------------------------------------------------------------------

    def test_remainder_one(self) -> None:
        # 10 total, 3 targets → base=3, remainder=1 → target 0 gets 4
        result = distribute_total_iodepth(3, 10)
        self.assertEqual(result[0], 4)
        self.assertEqual(result[1], 3)
        self.assertEqual(result[2], 3)

    def test_remainder_two(self) -> None:
        # 10 total, 4 targets → base=2, remainder=2 → targets 0,1 get 3
        result = distribute_total_iodepth(4, 10)
        self.assertEqual(result[0], 3)
        self.assertEqual(result[1], 3)
        self.assertEqual(result[2], 2)
        self.assertEqual(result[3], 2)

    def test_six_targets_total_32(self) -> None:
        # 32 // 6 = 5, remainder 2 → targets 0,1 get 6; rest get 5
        result = distribute_total_iodepth(6, 32)
        self.assertEqual(result[0], 6)
        self.assertEqual(result[1], 6)
        self.assertEqual(result[2], 5)
        self.assertEqual(result[3], 5)
        self.assertEqual(result[4], 5)
        self.assertEqual(result[5], 5)

    def test_sum_equals_total_iodepth_with_remainder(self) -> None:
        total = 17
        num_targets = 5
        result = distribute_total_iodepth(num_targets, total)
        self.assertEqual(sum(result.values()), total)

    def test_sum_equals_total_iodepth_exact(self) -> None:
        total = 16
        num_targets = 4
        result = distribute_total_iodepth(num_targets, total)
        self.assertEqual(sum(result.values()), total)

    def test_no_target_has_iodepth_zero(self) -> None:
        # Ensure remainder distribution never produces 0
        for total in range(1, 20):
            for nvols in range(1, total + 1):
                result = distribute_total_iodepth(nvols, total)
                for vid, depth in result.items():
                    self.assertGreater(depth, 0, f"total={total} nvols={nvols} vol={vid}")

    # ------------------------------------------------------------------
    # Underflow clamping (total_iodepth < num_targets)
    # ------------------------------------------------------------------

    def test_underflow_reduces_target_count(self) -> None:
        # 2 total, 5 targets → only 2 targets should be in the result
        result = distribute_total_iodepth(5, 2)
        self.assertEqual(len(result), 2)

    def test_underflow_each_active_target_gets_iodepth_one(self) -> None:
        result = distribute_total_iodepth(5, 2)
        for depth in result.values():
            self.assertEqual(depth, 1)

    def test_underflow_total_iodepth_one(self) -> None:
        result = distribute_total_iodepth(10, 1)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], 1)

    def test_underflow_emits_warning(self) -> None:
        with self.assertLogs("cbt", level=logging.WARNING) as cm:
            distribute_total_iodepth(5, 2)
        self.assertTrue(
            any("total iodepth" in msg.lower() or "iodepth" in msg.lower() for msg in cm.output),
            f"Expected a warning about iodepth underflow, got: {cm.output}",
        )


# ---------------------------------------------------------------------------
# calculate_iodepth_per_target dispatch
# ---------------------------------------------------------------------------


class TestCalculateIodepthPerTargetDispatch(unittest.TestCase):
    """
    calculate_iodepth_per_target dispatches to the correct sub-method
    based on the iodepth_key argument.
    """

    def test_iodepth_key_uses_flat_assignment(self) -> None:
        bm = _make_with_iodepth(8)
        result = bm.calculate_iodepth_per_target(3, "iodepth")
        # Flat assignment: all targets get the same depth
        self.assertEqual(result, {0: 8, 1: 8, 2: 8})

    def test_total_iodepth_key_uses_distribution(self) -> None:
        # 3 targets, total=10 → 4, 3, 3
        bm = _make({"total_iodepth": 10})
        result = bm.calculate_iodepth_per_target(3, "total_iodepth")
        self.assertEqual(sum(result.values()), 10)
        self.assertEqual(result[0], 4)

    def test_total_iodepth_key_preserves_total(self) -> None:
        total = 13
        bm = _make({"total_iodepth": total})
        result = bm.calculate_iodepth_per_target(4, "total_iodepth")
        self.assertEqual(sum(result.values()), total)


if __name__ == "__main__":
    unittest.main()
