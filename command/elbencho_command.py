"""Builds the elbencho command line for a single S3 workload instance.

It returns the full executable string that can be used to run a cli command.
It is instantiated by ``Workload._create_command_class`` as part of the 
shared Workloads pipeline."""

import os
import re
import shlex
from logging import Logger, getLogger
from typing import ClassVar

from cli_options import CliOptions
from command.command import Command, require_int
from common import get_fqdn_cmd

log: Logger = getLogger("cbt")

_BS_SUFFIXES = {"k": 1024, "m": 1024**2, "g": 1024**3}


class ElbenchoCommand(Command):
    """A single elbencho S3 command line for one run cell."""

    _MODE_FLAGS: ClassVar[dict[str, list[str]]] = {
        "write": ["--write"],
        "read": ["--read"],
        "readwrite": ["--write", "--read"],
        "stat": ["--stat"],
        "list": ["--s3listobjpar"],
    }

    _MODES_NO_BLOCKSIZE: ClassVar[set[str]] = {"stat", "list"}

    @classmethod
    def mode_is_supported(cls, mode: str) -> bool:
        """Return True if mode is currently supported (stat/list are not yet)."""
        return mode not in cls._MODES_NO_BLOCKSIZE

    @staticmethod
    def target_count(options: dict[str, str]) -> int:
        """Elbencho fans out over S3 buckets, one process per bucket.

        num_buckets exists to divide an aggregate total_iodepth; on its own
        there is nothing to divide, so a single process runs against the base
        bucket (see docs/workloads/elbencho-s3.md).
        """
        if options.get("total_iodepth") is None:
            return 1
        return require_int("num_buckets", options.get("num_buckets", 1))

    @staticmethod
    def workload_output_directory(base_run_directory: str, workload_name: str) -> str:
        """Elbencho builds its own output path (mode/blocksize-bytes/threads/
        iodepth) from the base run directory, with no workload-name segment."""
        return base_run_directory

    @staticmethod
    def parse_blocksize_to_bytes(blocksize: str) -> int:
        """Convert a blocksize string (e.g. '4k', '16m') into byte count."""
        s = str(blocksize).strip().lower()
        m = re.fullmatch(r"(\d+(?:\.\d+)?)([kmg]?)", s)
        if not m:
            raise ValueError(f"Unrecognised blocksize format: {blocksize!r}")
        value, suffix = m.group(1), m.group(2)
        return int(float(value) * _BS_SUFFIXES.get(suffix, 1))

    @staticmethod
    def build_auth_flags(auth: dict[str, str]) -> list[str]:
        """Return S3 auth flags for whatever credentials are present; empty auth yields no flags."""
        flags: list[str] = []

        config_str = auth.get("config", "")
        if config_str:
            pairs = dict(
                kv.split("=", 1)
                for kv in config_str.split(";")
                if "=" in kv
            )
            if "url" in pairs:
                flags += ["--s3endpoints", pairs["url"]]
            if "access_key" in pairs:
                flags += ["--s3key", pairs["access_key"]]
            if "secret_key" in pairs:
                flags += ["--s3secret", pairs["secret_key"]]

        token = auth.get("s3_session_token", "")
        if token:
            flags += ["--s3authtoken", token]

        return flags

    # ------------------------------------------------------------------
    # Command ABC implementation
    # ------------------------------------------------------------------

    def _parse_options(self, options: dict[str, str]) -> CliOptions:
        # Populate CliOptions with parsed options and defaults.
        parsed_options: CliOptions = CliOptions()

        parsed_options["mode"] = options.get("mode")
        parsed_options["s3_bucket"] = options.get("s3_bucket")
        parsed_options["s3_region"] = options.get("s3_region", "default")
        parsed_options["threads"] = str(options.get("threads", 1))
        parsed_options["blocksize"] = str(options.get("blocksize", "4k"))
        parsed_options["iodepth"] = str(options.get("iodepth", 1))

        # Multi-bucket / aggregate-iodepth bookkeeping. These never appear on the
        # elbencho command line directly; they shape the output path and, when
        # this is one of several processes, the per-target bucket name.
        total_iodepth = options.get("total_iodepth")
        parsed_options["total_iodepth"] = str(total_iodepth) if total_iodepth is not None else None
        parsed_options["target_count"] = str(options.get("target_count", 1))
        parsed_options["target_number"] = str(options.get("target_number", 0))

        # Optional value flags
        for key in ("size", "num_objects", "num_dirs", "duration", "hosts"):
            value = options.get(key)
            parsed_options[key] = str(value) if value is not None else None

        # Boolean presence flags: "true" when enabled, None otherwise. The
        # workloads layer stringifies every value, so a yaml ``false`` arrives
        # as the string "false" — which is truthy. Compare, don't test.
        for key in ("deldirs", "s3nompcheck", "mkdirs"):
            parsed_options[key] = "true" if options.get(key, "false") == "true" else None

        parsed_options["s3_auth_config"] = options.get("s3_auth_config", "")
        parsed_options["s3_session_token"] = options.get("s3_session_token", "")

        return parsed_options

    def _parse_global_options(self, options: dict[str, str]) -> CliOptions:
        return CliOptions(options)

    def _generate_output_directory_path(self) -> str:
        # {base}/elbencho/{mode}_{blocksize_bytes}/threads-{NNN}/[total_iodepth-{N}/]iodepth-{NNN}
        options = self._options
        mode = str(options["mode"])
        blocksize = str(options["blocksize"])
        threads = require_int("threads", options["threads"])
        iodepth = require_int("iodepth", options["iodepth"])
        path_parts: list[str] = [
            self._workload_output_directory,
            self.benchmark,
            f"{mode}_{self.parse_blocksize_to_bytes(blocksize)}",
            f"threads-{threads:03d}",
        ]
        # The post-processor recovers the aggregate depth from this segment, so
        # it must sit between threads- and iodepth- when total_iodepth is in use.
        if options["total_iodepth"] is not None:
            path_parts.append(f"total_iodepth-{options['total_iodepth']}")
        path_parts.append(f"iodepth-{iodepth:03d}")
        return os.path.join(*path_parts)

    def _is_multi_bucket(self) -> bool:
        """True when this process is one of several sharing a run cell.

        The Workload decides the fan-out and reports it as target_count.
        Re-deriving it here from num_buckets would let the two disagree: a
        total_iodepth too small to split runs fewer buckets than were asked
        for, and this process would still suffix its bucket and result file as
        though the others existed.
        """
        return require_int("target_count", self._options["target_count"]) > 1

    def _bucket_target(self) -> str:
        """S3 target for this run cell. When multiple buckets share a
        total_iodepth, each target gets its own suffixed bucket so the parallel
        processes do not collide."""
        base_bucket = str(self._options["s3_bucket"])
        if not self._is_multi_bucket():
            return base_bucket
        return f"{base_bucket}-{require_int('target_number', self._options['target_number'])}"

    def _result_filename(self) -> str:
        """Per-target result file. The parallel per-bucket processes in one run
        cell share an output directory, so each must write a distinct result
        file (mirrors fio's output.{target}); a single result.csv would let them
        overwrite each other. Single-bucket runs keep the plain name."""
        if not self._is_multi_bucket():
            return "result.csv"
        return f"result-{require_int('target_number', self._options['target_number'])}.csv"

    @property
    def benchmark(self) -> str:
        return "elbencho"

    def _generate_full_command(self) -> str:
        if self._executable is None:
            return ""

        options = self._options
        mode = str(options["mode"])

        if not self.mode_is_supported(mode):
            log.warning(
                "Elbencho: mode '%s' is not yet supported by the formatter. "
                "Skipping run (blocksize=%s, threads=%s, iodepth=%s).",
                mode, options["blocksize"], options["threads"], options["iodepth"],
            )
            return ""

        mode_flags = self._MODE_FLAGS.get(mode)
        if mode_flags is None:
            raise ValueError(f"Unknown elbencho mode: {mode!r}")

        cmd_parts: list[str] = [self._executable]
        cmd_parts += mode_flags
        cmd_parts += ["--threads", str(options["threads"])]
        cmd_parts += ["--block", str(options["blocksize"])]
        cmd_parts += ["--iodepth", str(options["iodepth"])]

        if options["size"] is not None:
            cmd_parts += ["--size", str(options["size"])]
        if options["num_objects"] is not None:
            cmd_parts += ["--files", str(options["num_objects"])]
        if options["num_dirs"] is not None:
            cmd_parts += ["--dirs", str(options["num_dirs"])]
        if options["duration"] is not None:
            cmd_parts += ["--timelimit", str(options["duration"])]
        if options.get("deldirs") == "true":
            cmd_parts += ["--deldirs"]
        if options.get("s3nompcheck") == "true":
            cmd_parts += ["--s3nompcheck"]
        if options["hosts"] is not None:
            cmd_parts += ["--hosts", str(options["hosts"])]

        cmd_parts += self.build_auth_flags({
            "config": options["s3_auth_config"] or "",
            "s3_session_token": options["s3_session_token"] or "",
        })
        cmd_parts += ["--s3region", str(options["s3_region"])]
        cmd_parts += [
            "--resfile",
            os.path.join(self._generate_output_directory_path(), self._result_filename()),
        ]

        if options.get("mkdirs") == "true":
            cmd_parts += ["--mkdirs"]

        # shlex.join() quotes the $(hostname -s) in the target away, so join the
        # fixed arguments first and append the target unquoted.
        return f"{shlex.join(cmd_parts)} {self._s3_target()}"

    def _s3_target(self) -> str:
        """Positional S3 target, namespaced per client with the client's FQDN so
        that concurrent clients writing the same bucket do not overwrite each
        other's keys. The substitution is expanded by the remote client shell.
        Uses get_fqdn_cmd() (hostname -f) as the rbd fio command does, so hosts
        in different domains with the same short name do not collide."""
        return f"s3://{shlex.quote(self._bucket_target())}/$({get_fqdn_cmd()})"  # type: ignore[no-untyped-call]
