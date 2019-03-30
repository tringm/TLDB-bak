from tldb.core.main.tldb import TLDB
import unittest
from config import root_path


class TestTLDB(unittest.TestCase):
    def setUp(self):
        self.tldb = TLDB('local')
        self.input_folder = root_path() / 'tldb' / 'core' / 'io' / 'in' / 'test' / 'cases' / 'simple_small'

    def test_index_csv_file(self):
        self.tldb.index_csv_file('table', self.input_folder / 'haha.csv')
