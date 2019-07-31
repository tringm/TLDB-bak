import unittest

from config import root_path
from tests.test_case import TestCaseTimer
from tldb.core.client import TLDB
from tldb.core.structure.boundary import Boundary
from tldb.core.structure.dewey_id import DeweyID
from tldb.core.structure.entry import Entry
from tldb.core.structure.interval import Interval
from tldb.core.structure.node import Node


class TestNode(TestCaseTimer):
    def test_add_entry(self):
        node = Node(2, 'node')
        node.add_entry(Entry((1, 2, 3)))
        expected_entries = {Entry((1, 2, 3))}
        self.assertSetEqual(node.entries, expected_entries)
        self.assertEqual(node.boundary, Boundary([Interval((1, 1)), Interval((2, 2)), Interval((3, 3))]))

    def test_add_two_entry(self):
        node = Node(2, 'node')
        node.add_entry(Entry((10, 20)))
        node.add_entry(Entry((5, 15)))
        expected_entries = {Entry((10, 20)), Entry((5, 15))}
        self.assertSetEqual(node.entries, expected_entries)
        self.assertEqual(node.boundary, Boundary([Interval((5, 10)), Interval((15, 20))]))

    def test_add_child_node(self):
        node = Node(2, 'node')
        child_node = Node(2, 'child1')
        child_node.add_entry(Entry((10, 20)))
        child_node.add_entry(Entry((5, 15)))
        node.add_child_node(child_node)
        self.assertEqual(len(node.children), 1)
        self.assertSetEqual(node.children, {child_node})
        self.assertEqual(node.boundary, Boundary([Interval((5, 10)), Interval((15, 20))]))

    def test_add_two_child_node(self):
        node = Node(2, 'node')
        child1 = Node(2, 'child1')
        child1.add_entry(Entry((10, 20)))
        child2 = Node(2, 'child2')
        child2.add_entry(Entry((5, 15)))
        node.add_child_node(child1)
        node.add_child_node(child2)
        self.assertSetEqual(node.children, {child1, child2})
        self.assertEqual(node.boundary, Boundary([Interval((5, 10)), Interval((15, 20))]))

    def test_center_coordinates(self):
        node = Node(2, 'node')
        node.add_entry(Entry((10, 20)))
        node.add_entry(Entry((5, 15)))
        self.assertEqual(node.center_coord, Entry((7.5, 17.5)))

    def test_get_leaf_entries(self):
        node = Node(2, 'node')
        child1 = Node(2, 'child1', is_leaf=True)
        child1.add_entry(Entry((10, 20)))
        child2 = Node(2, 'child2', is_leaf=True)
        child2.add_entry(Entry((5, 15)))
        node.add_child_node(child1)
        node.add_child_node(child2)
        expected_entries = {Entry((10, 20)), Entry((5, 15))}
        self.assertSetEqual(node.get_leaf_entries(), expected_entries)

    def test_get_leaf_nodes(self):
        node = Node(2, 'node')
        child1 = Node(2, 'child1', is_leaf=True)
        child1.add_entry(Entry((10, 20)))
        child2 = Node(2, 'child2')
        child2.add_entry(Entry((5, 15)))
        node.add_child_node(child1)
        node.add_child_node(child2)
        self.assertSetEqual(node.get_leaf_nodes(), {child1})


class TestNodeRangeSearch(TestCaseTimer):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.tldb = TLDB('local')
        input_path = root_path() / 'tests' / 'io' / 'in' / 'cases' / 'simple_small' / 'A_B_D_table.dat'
        cls.tldb.load_table_object_from_csv('table', input_path, delimiter=' ', headers=['A', 'B', 'D'],
                                            max_n_children=2)
        cls.table = cls.tldb.get_object('table')

    def test_out_of_range_below(self):
        boundary = Boundary([Interval([6.0, 118.0]), Interval([15, 16]), Interval([13.0, 72.0])])
        result = self.table.range_search(boundary)
        self.assertFalse(result, f"should not found any result for range {boundary}")

    def test_out_of_range_above(self):
        boundary = Boundary([Interval([119, 120]), Interval([17, 600]), Interval([13.0, 72.0])])
        result = self.table.range_search(boundary)
        self.assertFalse(result, f"should not found any result for range {boundary}")

    def test_parent_range(self):
        boundary = self.table.index_structure.root.boundary
        result = self.table.range_search(boundary)
        expected = set(self.table.index_structure.root.children)
        self.assertSetEqual(result, expected, f"should return children when search with range of boundary of parent")

    def test_floor_range(self):
        boundary = Boundary([Interval([6.0, 118.0]), Interval([17, 600]), Interval([12, 13])])
        result = self.table.range_search(boundary)
        self.assertEqual(result.pop().boundary,
                         Boundary((Interval((6.0, 18.0)), Interval((69.0, 600.0)), Interval((13.0, 27.0)))))

    def test_ceil_range(self):
        boundary = Boundary([Interval([6.0, 118.0]), Interval([17, 600]), Interval([72, 73])])
        result = self.table.range_search(boundary)
        self.assertEqual(result.pop().boundary,
                         Boundary((Interval((72, 94)), Interval((85, 500)), Interval((19, 72)))))

    def test_gap(self):
        boundary = Boundary([Interval([6.0, 118.0]), Interval([19, 20]), Interval([13, 72])])
        result = self.table.range_search(boundary)
        self.assertFalse(result, "Should not return any result due to the gap [19, 20]")


class TestXMLNode(TestCaseTimer):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.tldb = TLDB('local')
        input_path = root_path() / 'tests' / 'io' / 'in' / 'cases' / 'simple_small'
        cls.tldb.load_from_folder(input_path, max_n_children=2)

    @unittest.SkipTest
    def test_descendant_range_search_only_v(self):
        root = self.tldb.get_object('A_B_C_D_xml').get_attribute('C').index_structure.root
        result = root.desc_range_search(idx_interval=None, v_interval=Interval((5, 15)))
        expected_boundary = Boundary((Interval((DeweyID('2.3'), DeweyID('2.6'))), Interval((13, 30))))
        self.assertEqual(1, len(result))
        self.assertEqual(result.pop().boundary, expected_boundary)

    def test_descendant_range_search_only_idx(self):
        root = self.tldb.get_object('A_B_C_D_xml').get_attribute('C').index_structure.root
        result = root.desc_range_search(idx_interval=Interval((DeweyID('2.8'), DeweyID('2.9'))), v_interval=None)
        expected_boundary = Boundary((Interval((DeweyID('2.7'), DeweyID('2.9.4'))), Interval((17, 24))))
        self.assertEqual(1, len(result))
        self.assertEqual(result.pop().boundary, expected_boundary)

    @unittest.SkipTest
    def test_descendant_range_search(self):
        root = self.tldb.get_object('A_B_C_D_xml').get_attribute('C').index_structure.root
        result = root.desc_range_search(idx_interval=Interval((DeweyID('2.3'), DeweyID('2.5'))),
                                        v_interval=Interval((5, 15)))
        expected_boundary = Boundary((Interval((DeweyID('2.3'), DeweyID('2.6'))), Interval((13, 30))))
        self.assertEqual(1, len(result))
        self.assertEqual(result.pop().boundary, expected_boundary)
