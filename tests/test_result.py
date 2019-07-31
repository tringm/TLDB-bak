import os
import unittest

from test import METRICS_LOGS_PATH


class TestResultLogMetrics(unittest.TextTestResult):
    def addSuccess(self, test):
        super().addSuccess(test)
        with METRICS_LOGS_PATH.open(mode='a+') as f:
            f.write(f"{test.id()}, success, {test._elapsed:5f},\n")

    def addFailure(self, test, err):
        super().addFailure(test, err)
        with METRICS_LOGS_PATH.open(mode='a+') as f:
            f.write(f"{test.id()}, fail, {test._elapsed:5f}, {err}\n")

    def addError(self, test, err) -> None:
        super().addError(test, err)
        with METRICS_LOGS_PATH.open(mode='a+') as f:
            f.write(f"{test.id()}, error, {test._elapsed:5f}, {err}\n")


class TestResultCompareFileMeld(TestResultLogMetrics):
    def addFailure(self, test, err):
        super().addFailure(test, err)
        if hasattr(test, 'out_file') and hasattr(test, 'exp_file'):
            method_id = test.id().split('.')[-1]
            if method_id in test.out_file and method_id in test.exp_file:
                cont = True
                while cont:
                    res = input("[d]iff, [c]ontinue, or [f]reeze? ")
                    if res == "f":
                        os.rename(test.out_file[method_id], test.exp_file[method_id])
                        cont = False
                    elif res == "c":
                        cont = False
                    elif res == "d":
                        os.system("meld " + str(test.exp_file[method_id]) + " " + str(test.out_file[method_id]))