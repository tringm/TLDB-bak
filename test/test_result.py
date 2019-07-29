import os
import unittest
from config import root_path


class TestResultLogMetrics(unittest.TextTestResult):
    def addSuccess(self, test):
        super().addSuccess(test)
        with (root_path() / 'test' / 'io' / 'out' / 'metrics.log').open(mode='a+') as f:
            f.write(f"{test.id()}, success, {test._elapsed:5f}\n")

    def addFailure(self, test, err):
        super().addFailure(test, err)
        with (root_path() / 'test' / 'io' / 'out' / 'metrics.log').open(mode='a+') as f:
            f.write(f"{test.id()}, fail, {test._elapsed:5f}\n")

    def addError(self, test, err) -> None:
        super().addError(test, err)
        with (root_path() / 'test' / 'io' / 'out' / 'metrics.log').open(mode='a+') as f:
            f.write(f"{test.id()}, error, {test._elapsed:5f}\n")


class TestResultCompareFileMeld(unittest.TextTestResult):
    def addFailure(self, test, err):
        super().addFailure(test, err)
        if hasattr(test, 'out_file') and hasattr(test, 'exp_file'):
            method_id = test.id().split('.')[-1]
            if method_id in test.out_file and method_id in test.exp_file:
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

    def addError(self, test, err):
        super(TestResultCompareFileMeld, self).addError(test, err)