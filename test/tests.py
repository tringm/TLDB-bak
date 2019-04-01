import filecmp
import os
import unittest
from abc import abstractmethod

from config import root_path
from pathlib import Path


class TestResultCompareFileMeld(unittest.TextTestResult):
    def addFailure(self, test, err):
        if hasattr(test, 'out_file') and hasattr(test, 'exp_file'):
            method_id = test.id().split('.')[-1]
            cont = True
            while cont:
                res = input("[d]iff, [c]ontinue or [f]reeze? ")
                if res == "f":
                    os.rename(test.out_file[method_id], test.exp_file[method_id])
                    cont = False
                elif res == "c":
                    cont = False
                elif res == "d":
                    os.system("meld " + str(test.exp_file[method_id]) + " " + str(test.out_file[method_id]))
        super(TestResultCompareFileMeld, self).addFailure(test, err)

    def addError(self, test, err):
        super(TestResultCompareFileMeld, self).addError(test, err)


class TestCaseCompare(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.output_folder = root_path() / 'test' / 'io' / 'out'
        cls.out_file = {}
        cls.exp_file = {}

    def file_compare(self, out_f: Path, exp_f: Path, msg=None):
        if not out_f.exists() or not exp_f.exists():
            raise ValueError("Either %s or %s does not exist" % (str(out_f), str(exp_f)))
        if not out_f.is_file() or not exp_f.is_file():
            raise ValueError("Either %s or %s is not a file" % (str(out_f), str(exp_f)))
        if not msg:
            self.assertTrue(filecmp.cmp(str(out_f), str(exp_f), shallow=False),
                            f"out file {str(out_f)} does not match exp file {str(exp_f)}")
        else:
            self.assertTrue(filecmp.cmp(str(out_f), str(exp_f), shallow=False), msg)

    def prepare_compare_files(self, methods_id):
        self.out_file[methods_id] = self.output_folder / (methods_id + '_out.txt')
        self.exp_file[methods_id] = self.output_folder / (methods_id + '_exp.txt')


def get_suites():
    suites_dir = [d for d in (root_path() / 'test').iterdir() if d.is_dir()]
    suites = {}
    for d in suites_dir:
        tests = unittest.TestLoader().discover(d)
        if tests.countTestCases() > 0:
            suites[d.stem] = tests
    return suites
