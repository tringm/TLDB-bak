import filecmp
import logging
import os
import unittest
from pathlib import Path

from config import root_path, set_up_logger


class TestResultCompareFileMeld(unittest.TextTestResult):
    def addFailure(self, test, err):
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
        super(TestResultCompareFileMeld, self).addFailure(test, err)

    def addError(self, test, err):
        super(TestResultCompareFileMeld, self).addError(test, err)


class TestCaseCompare(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.input_folder = root_path() / 'test' / 'io' / 'in'
        cls.output_folder = root_path() / 'test' / 'io' / 'out'
        cls.out_file = {}
        cls.exp_file = {}
        cls.in_file = {}
        cls.log_file = {}
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        set_up_logger()

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

    def set_up_compare_files(self, method_id):
        self.out_file[method_id] = self.output_folder / (method_id + '_out.txt')
        self.exp_file[method_id] = self.output_folder / (method_id + '_exp.txt')

    def set_up_logger(self, method_id, logging_level, path=None):
        def get_log_path():
            log_name = f"{method_id}_try_{n_try}.log"
            if not path:
                log_path = self.output_folder / log_name
            else:
                log_path = path / log_name
            return log_path

        n_try = 0
        log_path = get_log_path()
        while log_path.exists():
            n_try += 1
            log_path = get_log_path()

        self.log_file[method_id] = log_path
        logging.basicConfig(filename=str(log_path), level=logging_level,
                            format='%(asctime)-5s %(name)-5s %(levelname)-10s %(message)s',
                            datefmt='%H:%M:%S')


def get_suites():
    suites_dir = [d for d in (root_path() / 'test').iterdir() if d.is_dir()]
    suites = {}
    for d in suites_dir:
        tests = unittest.TestLoader().discover(d)
        if tests.countTestCases() > 0:
            suites[d.stem] = tests
    return suites
