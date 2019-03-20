import unittest

from config import root_path
import os


class TestResultCompareFileMeld(unittest.TestResult):
    def addFailure(self, test, err):
        print(f"{test.id()} failed")
        if hasattr(test, 'out_file') and hasattr(test, 'exp_file'):
            cont = True
            while cont:
                res = input("[d]iff, [c]ontinue or [f]reeze? ")
                if res == "f":
                    os.rename(test.out_file, test.exp_file)
                    cont = False
                elif res == "c":
                    cont = False
                elif res == "d":
                    os.system("meld " + str(test.exp_file) + " " + str(test.out_file))
        super(TestResultCompareFileMeld, self).addFailure(test, err)

    def addError(self, test, err):
        super(TestResultCompareFileMeld, self).addError(test, err)


def get_suites():
    suites_dir = [d for d in (root_path() / 'core' / 'test').iterdir() if d.is_dir()]
    suites = {}
    for d in suites_dir:
        tests = unittest.TestLoader().discover(d)
        if tests.countTestCases() == 0:
            continue
        suites[d.stem] = tests
    return suites
