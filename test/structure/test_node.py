import unittest

from config import root_path
from tldb.core.tldb import TLDB


class TestRangeSearch(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super(TestRangeSearch, cls).setUpClass()
        cls.tldb = TLDB('local')
        cls.input_folder = root_path() / 'test' / 'io' / 'in' / 'cases' / 'simple_small'
        cls.tldb.load_object_from_csv('table',
                                      cls.input_folder / 'A_B_D_table.dat',
                                      delimiter=' ',
                                      headers=['A', 'B', 'D'])
        cls.table = cls.tldb.objects['table']

    def test_out_of_range_below(self):
        r = [[6.0, 118.0], [15, 16], [13.0, 72.0]]
        result = self.table.range_search(r)
        self.assertIsNone(result, f"should not found any result for range {r}")

    def test_out_of_range_above(self):
        r = [[119, 120], [17.0, 600.0], [13.0, 72.0]]
        result = self.table.range_search(r)
        self.assertIsNone(result, f"should not found any result for range {r}")

    def test_parent_range(self):
        r = self.table.index_structure.root.boundary
        result = self.table.range_search(r)
        self.assertListEqual(result, self.table.index_structure.root.children,
                             f"should return children when search with range of boundary of parent")

    def test_floor_range(self):
        r = [[6.0, 118.0], [17.0, 600.0], [11, 13]]
        result = self.table.range_search(r)
        self.assertEqual(len(result), 1, "Should return only one result")
        expected = self.table.index_structure.root.children[1].children[0]
        self.assertEqual(result[0], expected)

    def test_ceil_range(self):
        r = [[6.0, 118.0], [17.0, 600.0], [72, 73]]
        result = self.table.range_search(r)
        self.assertEqual(len(result), 1, "Should return only one result")
        expected = self.table.index_structure.root.children[1].children[1]
        self.assertEqual(result[0], expected)

    def test_gap(self):
        r = [[6, 118], [19, 20], [13.0, 72.0]]
        result = self.table.range_search(r)
        self.assertIsNone(result, "Should not return any result due to the gap [19, 20]")
