import os
import uuid
import shutil
import unittest
import warnings


import common


var_name = "CBT_TEST_NODES"
message = "No test VM provided. Set {0} env var".format(var_name)


def iter_nodes(nodes):
    for node in nodes.split(","):
        if '@' in node:
            node = node.split("@", 1)[1]
        yield node


class TestCommon(unittest.TestCase):
    def test_mkdirp(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            fname = os.tempnam()
        fname = os.path.join(fname, 'a', 'b12', 'zasdasd')
        common.mkdir_p(fname)
        self.assertTrue(os.path.isdir(fname))
        shutil.rmtree(fname)

    @unittest.skipIf(var_name not in os.environ, message)
    def test_pdsh(self):
        nodes = os.environ[var_name]
        out, err = common.pdsh(nodes, "ls /").communicate()
        for node in iter_nodes(nodes):
            self.assertIn("{0}: etc\n".format(node), out)

    @unittest.skipIf(var_name not in os.environ, message)
    def test_pdsh_no_cmd(self):
        nodes = os.environ[var_name]
        proc = common.pdsh(nodes, "unknown_cmd_131321")
        proc.communicate()
        self.assertNotEqual(proc.returncode, 0)

    @unittest.skipIf(var_name not in os.environ, message)
    def test_pdcp_rpdcp(self):
        nodes = os.environ[var_name]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            fname = os.tempnam()

        val = str(uuid.uuid1())
        with open(fname, "w") as fd:
            fd.write(val)

        try:
            common.pdcp(nodes, None, fname, fname).communicate()
            out, err = common.pdsh(nodes, "cat " + fname).communicate()
            for node in iter_nodes(nodes):
                self.assertIn("{0}: {1}\n".format(node, val), out)
        finally:
            os.unlink(fname)

        common.rpdcp(nodes, None, fname, os.path.dirname(fname)).communicate()
        try:
            with open(fname) as fd:
                self.assertEqual(fd.read(), val)
        finally:
            os.unlink(fname)

        common.pdsh(nodes, "rm " + fname).communicate()

