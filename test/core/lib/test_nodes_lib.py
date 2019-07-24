from test.tests import TestCaseCompare
from tldb.core.client import TLDB
from tldb.core.lib.nodes import nodes_range_search, nodes_to_boundary
from tldb.core.structure.boundary import Boundary
from tldb.core.structure.interval import Interval
from tldb.core.structure.node import Node


class TestMultiNodesRangeSearch(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super(TestMultiNodesRangeSearch, cls).setUpClass()
        cls.tldb = TLDB('local')
        cls.input_folder = cls.input_folder / 'cases' / 'simple_small'
        cls.tldb.load_table_object_from_csv('table',
                                            cls.input_folder / 'A_B_D_table.dat',
                                            delimiter=' ',
                                            headers=['A', 'B', 'D'])
        cls.table = cls.tldb.get_object('table')

    def test_gap(self):
        root_children = self.table.index_structure.root.children
        boundary = Boundary([Interval((6, 118)), Interval((19, 20)), Interval((13.0, 72.0))])
        result = nodes_range_search(root_children, boundary)
        self.assertEqual(set(), result, "Should not return any result due to the gap [19, 20]")

    def test_gap_fit_one(self):
        root_children = self.table.index_structure.root.children
        boundary = Boundary([Interval([80, 85]), Interval([17, 600]), Interval([13.0, 72.0])])
        result = nodes_range_search(root_children, boundary)
        self.assertEqual(1, len(result))
        self.assertEqual(result.pop().boundary,
                         Boundary((Interval((72.0, 94.0)), Interval((85.0, 500.0)), Interval((19.0, 72.0)))))

    def node_to_boundary(self):
        root = self.table.index_structure.root
        root_children = root.children
        self.assertEqual(nodes_to_boundary(root_children), root.boundary)
