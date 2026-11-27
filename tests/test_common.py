"""Unit tests for the Common class"""

import os
import shutil
import tempfile
import unittest
import unittest.mock
import uuid
import warnings

import common
import settings

VAR_NAME = "CBT_TEST_NODES"
MSG = f"No test VM provided. Set {VAR_NAME} env var"


def iter_nodes(nodes):
    """
    Iterator to produce each individual node
    """
    for node in nodes.split(","):
        if "@" in node:
            node = node.split("@", 1)[1]
        yield node


class TestCommon(unittest.TestCase):
    """Sanity tests for common.py"""

    def test_mkdirp(self):
        """
        Can create a directory
        """
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with tempfile.TemporaryDirectory() as tmp:
                fname = os.path.join(tmp, "a", "b12", "zasdasd")
                common.mkdir_p(fname)
                self.assertTrue(os.path.isdir(fname))
                shutil.rmtree(fname)

    @unittest.skipIf(VAR_NAME not in os.environ, MSG)
    def test_pdsh(self):
        """
        Can issue a valid cli to the nodes
        """
        nodes = os.environ[VAR_NAME]
        out, _err = common.pdsh(nodes, "ls /").communicate()
        # output from the first node in the list, so we are interested
        # ib the contents
        for _node in iter_nodes(nodes):
            self.assertIn("etc\n", out)

    @unittest.skipIf(VAR_NAME not in os.environ, MSG)
    def test_pdsh_no_cmd(self):
        """
        Can issue an invalid cli to the node, get rc not 0
        """
        nodes = os.environ[VAR_NAME]
        proc = common.pdsh(nodes, "unknown_cmd_131321")
        proc.communicate()
        # log(proc)
        # self.assertNotEqual(proc.myrtncode, 0)
        self.assertEqual(proc.myrtncode, 0)

    @unittest.skipIf(VAR_NAME not in os.environ, MSG)
    def test_pdcp_rpdcp(self):
        """
        Can copy a file to the nodes
        """
        nodes = os.environ[VAR_NAME]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            tmp = uuid.uuid4().hex
            fname = os.path.join("/tmp/", tmp)
            val = str(uuid.uuid1())
            with open(fname, "w", encoding="UTF-8") as fd:
                fd.write(val)
            try:
                common.pdcp(nodes, None, fname, fname).communicate()
                out, _err = common.pdsh(nodes, "cat " + fname).communicate()
                for _node in iter_nodes(nodes):
                    # self.assertIn(f"{node}: {val}\n", out)
                    self.assertIn(out, f"{val}\n")
            finally:
                pass

            common.rpdcp(nodes, None, fname, os.path.dirname(fname)).communicate()
            try:
                with open(fname, encoding="UTF-8") as fd:
                    self.assertEqual(fd.read(), val)
            finally:
                try:
                    os.remove(fname)
                except OSError:
                    pass
            common.pdsh(nodes, "rm " + fname).communicate()


class TestPdshContinueOnError(unittest.TestCase):
    """
    Tests for the `common: continue_on_error` setting.

    "localhost" is routed through the local sh() path, which is the code under test.
    """

    def setUp(self) -> None:
        self._saved_common: dict[str, object] = settings.common
        settings.common = {}

    def tearDown(self) -> None:
        settings.common = self._saved_common

    def test_pdsh_checked_default_does_not_raise(self) -> None:
        """With no continue_on_error setting, pdsh_checked keeps going (existing default)."""
        proc = common.pdsh_checked("localhost", "exit 3")
        proc.communicate()
        self.assertEqual(proc.myrtncode, 3, "failing command should report its return code")

    def test_pdsh_checked_true_does_not_raise(self) -> None:
        """continue_on_error: true keeps going."""
        settings.common = {"continue_on_error": True}
        proc = common.pdsh_checked("localhost", "exit 3")
        proc.communicate()
        self.assertEqual(proc.myrtncode, 3, "failing command should report its return code")

    def test_pdsh_checked_false_raises(self) -> None:
        """continue_on_error: false makes a failing command raise."""
        settings.common = {"continue_on_error": False}
        with self.assertRaisesRegex(Exception, "rtncode=3", msg="pdsh_checked should raise on a failing command"):
            common.pdsh_checked("localhost", "exit 3").communicate()

    def test_pdsh_checked_false_success_does_not_raise(self) -> None:
        """continue_on_error: false does not affect commands that succeed."""
        settings.common = {"continue_on_error": False}
        proc = common.pdsh_checked("localhost", "exit 0")
        proc.communicate()
        self.assertEqual(proc.myrtncode, 0, "successful command should return 0")

    def test_plain_pdsh_ignores_setting(self) -> None:
        """Plain pdsh stays tolerant, so killall/pkill style teardowns never abort."""
        settings.common = {"continue_on_error": False}
        proc = common.pdsh("localhost", "exit 3")
        proc.communicate()
        self.assertEqual(proc.myrtncode, 3, "plain pdsh must not raise even when continue_on_error is false")
