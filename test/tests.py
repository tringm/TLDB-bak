import filecmp
import os
import unittest

from config import root_path


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
    def file_compare(self, f1, f2, msg=None):
        if not f1.exists() or not f2.exists():
            raise ValueError("Either %s or %s does not exist" % (str(f1), str(f2)))
        if not f1.is_file() or not f2.is_file():
            raise ValueError("Either %s or %s is not a file" % (str(f1), str(f2)))
        if not msg:
            self.assertTrue(filecmp.cmp(str(f1), str(f2), shallow=False),
                            f"out file {str(f1)} does not match exp file {str(f2)}")
        else:
            self.assertTrue(filecmp.cmp(str(f1), str(f2), shallow=False), msg)


def get_suites():
    suites_dir = [d for d in (root_path() / 'test').iterdir() if d.is_dir()]
    suites = {}
    for d in suites_dir:
        tests = unittest.TestLoader().discover(d)
        if tests.countTestCases() > 0:
            suites[d.stem] = tests
    return suites
