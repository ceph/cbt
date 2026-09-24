"""
Unit tests for concrete FIO benchmark classes initialization and properties.

Tests cover:
  - Fio (benchmark/fio.py)
  - LibrbdFio (benchmark/librbdfio.py)
  - RbdFio (benchmark/rbdfio.py)
  - KvmRbdFio (benchmark/kvmrbdfio.py)
  - RawFio (benchmark/rawfio.py)
"""

from typing import Any
from unittest.mock import MagicMock

import pytest

import benchmarkfactory
import settings
from benchmark.fio import Fio
from benchmark.kvmrbdfio import KvmRbdFio
from benchmark.librbdfio import LibrbdFio
from benchmark.rawfio import RawFio
from benchmark.rbdfio import RbdFio


@pytest.fixture(autouse=True)
def init_settings() -> None:
    """Initialize mock settings for cluster and nodes."""
    settings.mock_initialize(config_file="tools/invariant.yaml")


def _mock_cluster() -> MagicMock:
    """Return a mock Ceph cluster object."""
    cluster = MagicMock()
    cluster.config = {}
    return cluster


# ---------------------------------------------------------------------------
# Fio (Endpoint-based FIO)
# ---------------------------------------------------------------------------


class TestFioInitialization:
    """Test suite for Fio benchmark class initialization."""

    def test_default_initialization(self) -> None:
        """Verify default parameters on Fio instantiation."""
        cluster = _mock_cluster()
        fio = Fio(archive_dir="/tmp/archive", cluster=cluster, config={"iteration": 0})

        assert fio.ioengine == "libaio"
        assert fio.sync is None
        assert fio.procs_per_endpoint == 1
        assert fio.client_endpoints is None
        assert fio.prefill_flag is True
        assert fio.prefill_iodepth == 16
        assert fio.size == 4096
        assert fio.fio_out_format == "json,normal"
        assert fio.num_targets == 0

    def test_custom_parameters(self) -> None:
        """Verify custom configuration overrides on Fio instantiation."""
        cluster = _mock_cluster()
        config: dict[str, Any] = {
            "iteration": 0,
            "ioengine": "sync",
            "sync": 1,
            "procs_per_endpoint": 4,
            "size": 1024,
            "fio_out_format": "json",
            "prefill": False,
            "prefill_iodepth": 32,
            "mode": "randread",
            "op_size": 4096,
            "iodepth": 64,
        }
        fio = Fio(archive_dir="/tmp/archive", cluster=cluster, config=config)

        assert fio.ioengine == "sync"
        assert fio.sync == 1
        assert fio.procs_per_endpoint == 4
        assert fio.size == 1024
        assert fio.fio_out_format == "json"
        assert fio.prefill_flag is False
        assert fio.prefill_iodepth == 32
        assert fio.mode == "randread"
        assert fio.op_size == 4096
        assert fio.iodepth == 64

    def test_factory_creation(self) -> None:
        """Verify benchmarkfactory instantiates Fio correctly."""
        cluster = _mock_cluster()
        iteration: dict[str, Any] = {"acceptable": [1, 2, 3], "iteration": 0}
        obj = benchmarkfactory.get_object("/tmp", cluster, "fio", iteration)
        assert isinstance(obj, Fio)


# ---------------------------------------------------------------------------
# LibrbdFio (Ceph Librbd engine FIO)
# ---------------------------------------------------------------------------


class TestLibrbdFioInitialization:
    """Test suite for LibrbdFio benchmark class initialization."""

    def test_default_initialization(self) -> None:
        """Verify default parameters on LibrbdFio instantiation."""
        cluster = _mock_cluster()
        librbd = LibrbdFio(archive_dir="/tmp/archive", cluster=cluster, config={"iteration": 0})

        assert librbd.vol_size == 65536
        assert librbd.vol_object_size == 22
        assert librbd.volumes_per_client == 1
        assert librbd.procs_per_volume == 1
        assert librbd.no_sudo is False
        assert librbd.use_existing_volumes is False
        assert librbd.pgs == 2048
        assert librbd.pool_name == "cbt-librbdfio"
        assert librbd.data_pool is None
        assert librbd.num_targets == 1

    def test_custom_parameters(self) -> None:
        """Verify custom configuration overrides on LibrbdFio instantiation."""
        cluster = _mock_cluster()
        config: dict[str, Any] = {
            "iteration": 0,
            "vol_size": 4096,
            "vol_object_size": 24,
            "volumes_per_client": 8,
            "procs_per_volume": 2,
            "no_sudo": True,
            "use_existing_volumes": True,
            "poolname": "custom_pool",
            "time": 60,
            "ramp": 10,
        }
        librbd = LibrbdFio(archive_dir="/tmp/archive", cluster=cluster, config=config)

        assert librbd.vol_size == 4096
        assert librbd.vol_object_size == 24
        assert librbd.volumes_per_client == 8
        assert librbd.procs_per_volume == 2
        assert librbd.no_sudo is True
        assert librbd.use_existing_volumes is True
        assert librbd.pool_name == "custom_pool"
        assert librbd.time == 60
        assert librbd.ramp == 10
        assert librbd.num_targets == 8

    def test_factory_creation(self) -> None:
        """Verify benchmarkfactory instantiates LibrbdFio correctly."""
        cluster = _mock_cluster()
        iteration: dict[str, Any] = {"acceptable": [1, 2, 3], "iteration": 0}
        obj = benchmarkfactory.get_object("/tmp", cluster, "librbdfio", iteration)
        assert isinstance(obj, LibrbdFio)


# ---------------------------------------------------------------------------
# RbdFio (Kernel RBD mapped + XFS mounted)
# ---------------------------------------------------------------------------


class TestRbdFioInitialization:
    """Test suite for RbdFio benchmark class initialization."""

    def test_default_initialization(self) -> None:
        """Verify default parameters on RbdFio instantiation."""
        cluster = _mock_cluster()
        cluster.mnt_dir = "/srv"
        rbdfio = RbdFio(archive_dir="/tmp/archive", cluster=cluster, config={"iteration": 0})

        assert rbdfio.vol_size == 65536
        assert rbdfio.vol_object_size == "4M"
        assert rbdfio.ioengine == "libaio"
        assert rbdfio.concurrent_procs == 1
        assert rbdfio.poolname == "cbt-kernelrbdfio"
        assert rbdfio.num_targets == 1

    def test_custom_parameters(self) -> None:
        """Verify custom configuration overrides on RbdFio instantiation."""
        cluster = _mock_cluster()
        cluster.mnt_dir = "/srv"
        config: dict[str, Any] = {
            "iteration": 0,
            "vol_size": 2048,
            "vol_object_size": "8M",
            "ioengine": "sync",
            "concurrent_procs": 4,
            "time": 120,
            "ramp": 15,
        }
        rbdfio = RbdFio(archive_dir="/tmp/archive", cluster=cluster, config=config)

        assert rbdfio.vol_size == 2048
        assert rbdfio.vol_object_size == "8M"
        assert rbdfio.ioengine == "sync"
        assert rbdfio.concurrent_procs == 4
        assert rbdfio.time == 120
        assert rbdfio.ramp == 15
        assert rbdfio.num_targets == 4

    def test_factory_creation(self) -> None:
        """Verify benchmarkfactory instantiates RbdFio correctly."""
        cluster = _mock_cluster()
        iteration: dict[str, Any] = {"acceptable": [1, 2, 3], "iteration": 0}
        obj = benchmarkfactory.get_object("/tmp", cluster, "rbdfio", iteration)
        assert isinstance(obj, RbdFio)


# ---------------------------------------------------------------------------
# KvmRbdFio (Guest VM block devices)
# ---------------------------------------------------------------------------


class TestKvmRbdFioInitialization:
    """Test suite for KvmRbdFio benchmark class initialization."""

    def test_default_initialization(self) -> None:
        """Verify default parameters on KvmRbdFio instantiation."""
        cluster = _mock_cluster()
        kvm = KvmRbdFio(archive_dir="/tmp/archive", cluster=cluster, config={"iteration": 0})

        assert kvm.block_devices == ["/dev/vdb"]
        assert kvm.vol_size == pytest.approx(65536 * 0.9)
        assert kvm.ioengine == "libaio"
        assert kvm.concurrent_procs == 1
        assert kvm.num_targets == 1

    def test_custom_devices_and_procs(self) -> None:
        """Verify custom configuration overrides on KvmRbdFio instantiation."""
        cluster = _mock_cluster()
        config: dict[str, Any] = {
            "iteration": 0,
            "block_devices": "/dev/vdb, /dev/vdc, /dev/vdd",
            "vol_size": 512,
            "concurrent_procs": 3,
            "startdelay": 5,
        }
        kvm = KvmRbdFio(archive_dir="/tmp/archive", cluster=cluster, config=config)

        assert kvm.block_devices == ["/dev/vdb", "/dev/vdc", "/dev/vdd"]
        assert kvm.vol_size == pytest.approx(512 * 0.9)
        assert kvm.concurrent_procs == 3
        assert kvm.startdelay == 5
        assert kvm.num_targets == 3

    def test_factory_creation(self) -> None:
        """Verify benchmarkfactory instantiates KvmRbdFio correctly."""
        cluster = _mock_cluster()
        iteration: dict[str, Any] = {"acceptable": [1, 2, 3], "iteration": 0}
        obj = benchmarkfactory.get_object("/tmp", cluster, "kvmrbdfio", iteration)
        assert isinstance(obj, KvmRbdFio)


# ---------------------------------------------------------------------------
# RawFio (Raw block devices)
# ---------------------------------------------------------------------------


class TestRawFioInitialization:
    """Test suite for RawFio benchmark class initialization."""

    def test_default_initialization(self) -> None:
        """Verify default parameters on RawFio instantiation."""
        cluster = _mock_cluster()
        raw = RawFio(archive_dir="/tmp/archive", cluster=cluster, config={"iteration": 0})

        assert raw.block_devices == ["/dev/vdb"]
        assert raw.vol_size == pytest.approx(65536 * 0.9)
        assert raw.ioengine == "libaio"
        assert raw.concurrent_procs == 1
        assert raw.fio_out_format == "json"
        assert raw.num_targets == 1

    def test_custom_parameters(self) -> None:
        """Verify custom configuration overrides on RawFio instantiation."""
        cluster = _mock_cluster()
        config: dict[str, Any] = {
            "iteration": 0,
            "block_devices": "/dev/sda, /dev/sdb",
            "vol_size": 2048,
            "concurrent_procs": 2,
        }
        raw = RawFio(archive_dir="/tmp/archive", cluster=cluster, config=config)

        assert raw.block_devices == ["/dev/sda", "/dev/sdb"]
        assert raw.vol_size == pytest.approx(2048 * 0.9)
        assert raw.concurrent_procs == 2
        assert raw.fio_out_format == "json"
        assert raw.num_targets == 2

    def test_factory_creation(self) -> None:
        """Verify benchmarkfactory instantiates RawFio correctly."""
        cluster = _mock_cluster()
        iteration: dict[str, Any] = {"acceptable": [1, 2, 3], "iteration": 0}
        obj = benchmarkfactory.get_object("/tmp", cluster, "rawfio", iteration)
        assert isinstance(obj, RawFio)
