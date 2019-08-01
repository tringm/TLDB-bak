import filecmp
import logging
import time
import unittest
from pathlib import Path
from typing import Optional, Union

from config import root_path, set_up_logger


class TestCaseTimer(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self._started_at = time.time()

    def tearDown(self) -> None:
        super().tearDown()
        self._elapsed = time.time() - self._started_at


class TestCaseCompare(TestCaseTimer):
    @classmethod
    def setUpClass(cls, test_path: Optional[Union[Path, str]] = None):
        """

        :param test_path: test path that will be used for the input and output folder. E.g: case/a_single_case
        :return:
        """
        if not test_path:
            cls.input_folder = root_path() / 'tests' / 'io' / 'in' / cls.__name__
            cls.output_folder = root_path() / 'tests' / 'io' / 'out' / cls.__name__
        else:
            cls.input_folder = (root_path() / 'tests' / 'io' / 'in').joinpath(test_path)
            cls.output_folder = (root_path() / 'tests' / 'io' / 'out').joinpath(test_path)
        cls.input_folder.mkdir(parents=True, exist_ok=True)
        cls.output_folder.mkdir(parents=True, exist_ok=True)
        cls.out_file = {}
        cls.exp_file = {}
        cls.in_file = {}

    def setUp(self, default_logging_level: Optional[int] = logging.INFO) -> None:
        super().setUp()
        method_name = self.id().split('.')[-1]
        self.out_file[method_name] = self.output_folder / (method_name + '_out.txt')
        self.exp_file[method_name] = self.output_folder / (method_name + '_exp.txt')
        set_up_logger(self.output_folder, method_name, default_logging_level)
        self.logger = logging.getLogger(method_name)

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

    def file_compare_by_method_id(self, method_id):
        self.file_compare(out_f=self.out_file[method_id], exp_f=self.exp_file[method_id])

    def file_compare_default(self):
        method_name = self.id().split('.')[-1]
        self.file_compare(out_f=self.out_file[method_name], exp_f=self.exp_file[method_name])