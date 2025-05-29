'''
A subclasss of an FioCommand that deals with options that are specific the the
rbd I/O engine.

From the FIO documantation:
https://fio.readthedocs.io/en/latest/fio_doc.html

These are:
clientname
rbdname
clustername
pool
busy_poll

Of these clustername and busy_poll are not currently used by CBT
'''

from command.fio_command import FioCommand
from common import get_fqdn_cmd


class RbdFioCommand(FioCommand):
    """
    An FioCommand type that deals specifically with running I/O using the rbd io engine.
    """

    _RBD_DEFAULT_OPTIONS: dict[str, str] = {"ioengine": "rbd", "clientname": "admin"}

    def __init__(self, options: dict[str, str], workload_output_directory: str) -> None:
        super().__init__(options, workload_output_directory)

    def _parse_ioengine_specific_parameters(self, options: dict[str, str]) -> dict[str, str]:
        rbd_options: dict[str, str] = self._RBD_DEFAULT_OPTIONS

        rbd_name: str = options.get("rbdname", "")
        if rbd_name == "":
            rbd_name = f"cbt-fio-`{get_fqdn_cmd()}`-{self._target_number:d}"  # type: ignore[no-untyped-call]
        rbd_options["rbdname"] = rbd_name
        rbd_options["pool"] = options.get("poolname", "cbt-rbdfio")

        return rbd_options
