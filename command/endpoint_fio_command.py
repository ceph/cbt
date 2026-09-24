"""
An FioCommand subclass for benchmarks that use client_endpoints_factory
abstractions (the ``Fio`` benchmark class).

Client endpoints come in two flavours:

directory
    FIO targets are plain files inside a mounted directory.  Each process
    gets a --name flag of the form ``<endpoint_path>/`hostname`-<ep>-<proc>``.

rbd
    FIO targets are RBD images via the librbd engine.  The per-endpoint
    pool and image name are extracted from the endpoint string
    (``<pool>/<rbdname>``), and one --name flag is emitted per process.

Engine-specific options parsed from the ``options`` dict:
  endpoint_type      "directory" or "rbd" (required)
  endpoint_path      The directory path or ``<pool>/<rbdname>`` string
                     for the endpoint identified by ``target_number`` (required).
  ioengine           I/O engine name (default: "libaio"; forced to "rbd" for
                     rbd endpoint type).
  procs_per_endpoint Number of FIO processes per endpoint (default: 1).
  size               FIO --size value in MiB appended with "M" (optional).
  sync               FIO --sync value (optional).
  bssplit            FIO --bssplit value (optional; takes precedence over bsrange/bs/op_size).
  bsrange            FIO --bsrange value (optional; takes precedence over bs/op_size).
  bs                 FIO --bs value (optional; takes precedence over op_size).

Block-size precedence: bssplit > bsrange > bs > op_size (base class default).
WARNING: bssplit and bsrange are not supported by the CBT post-processing report
pipeline, which reads ``global_options['bs']`` from FIO JSON output.  Do not use
these options when report generation is required.  See post_processing/reports/README.md.
"""

from typing import Optional

from command.fio_command import FioCommand

# The Command hierarchy uses `hostname` (no -f) in --name values so that the
# space-based split in FioCommand._generate_full_command() does not break the
# token into two --name= flags.  This is consistent with _get_job_name() in
# the base class.
_HOSTNAME_CMD = "hostname"


class EndpointFioCommand(FioCommand):
    """
    An FioCommand type that deals specifically with client endpoint abstractions
    used by the ``Fio`` benchmark.
    """

    def __init__(self, options: dict[str, Optional[str]], workload_output_directory: str) -> None:
        self._endpoint_names: str = self._build_endpoint_names(options)
        super().__init__(options, workload_output_directory)

    @staticmethod
    def _build_endpoint_names(options: dict[str, Optional[str]]) -> str:
        """Build the space-separated list of --name values for this endpoint.

        Each token in the returned string becomes a separate ``--name=`` flag
        when ``FioCommand._generate_full_command()`` splits on spaces.
        Therefore, individual name tokens must themselves be space-free.
        """
        endpoint_type: str = options.get("endpoint_type") or "directory"
        endpoint_path: str = options.get("endpoint_path") or ""
        procs_per_endpoint: int = int(options.get("procs_per_endpoint") or "1")
        ep_num: int = int(options.get("target_number") or "0")

        if endpoint_type == "rbd":
            names = [f"{endpoint_path}-{proc_num}" for proc_num in range(procs_per_endpoint)]
        else:
            names = [f"{endpoint_path}/`{_HOSTNAME_CMD}`-{ep_num}-{proc_num}" for proc_num in range(procs_per_endpoint)]

        return " ".join(names)

    @property
    def benchmark(self) -> str:
        return "endpointfio"

    def _parse_ioengine_specific_parameters(self, options: dict[str, Optional[str]]) -> dict[str, Optional[str]]:
        ep_options: dict[str, Optional[str]] = {}

        endpoint_type: str = options.get("endpoint_type") or "directory"

        if endpoint_type == "rbd":
            ep_options["ioengine"] = "rbd"
            ep_options["clientname"] = "admin"
            path = options.get("endpoint_path") or "/"
            pool_name, rbd_base_name = path.split("/", 1)
            ep_options["pool"] = pool_name
            ep_options["rbdname"] = rbd_base_name
        else:
            ep_options["ioengine"] = options.get("ioengine", "libaio")

        if options.get("size", None) is not None:
            ep_options["size"] = f"{options['size']}M"

        if options.get("sync", None) is not None:
            ep_options["sync"] = options["sync"]

        # Block-size precedence: bssplit > bsrange > bs > op_size (handled by base).
        # When bssplit or bsrange wins, "bs" is set to None so the base class's
        # unconditional op_size→bs assignment is suppressed via CliOptions no-overwrite.
        if options.get("bssplit") is not None:
            ep_options["bssplit"] = options["bssplit"]
            ep_options["bs"] = None  # suppress base op_size→bs
        elif options.get("bsrange") is not None:
            ep_options["bsrange"] = options["bsrange"]
            ep_options["bs"] = None  # suppress base op_size→bs
        elif options.get("bs") is not None:
            ep_options["bs"] = options["bs"]
            # base op_size→bs is blocked by CliOptions no-overwrite; no sentinel needed

        return ep_options

    def _get_job_name(self, parent_workload_name: str, processes_per_volume: int) -> str:
        """Return the pre-built endpoint names for this command."""
        return self._endpoint_names
