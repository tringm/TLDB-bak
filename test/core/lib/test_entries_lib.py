from test.test_case import TestCaseCompare


# class TestEntriesLib(TestCaseCompare):
#     @classmethod
#     def setUpClass(cls):
#         super(TestEntriesLib, cls).setUpClass()
#         cls.tldb = TLDB('local')
#         cls.input_folder = cls.input_folder / 'cases' / 'simple_small'
#         cls.tldb.load_object_from_csv('table',
#                                       cls.input_folder / 'A_B_D_table.dat',
#                                       delimiter=' ',
#                                       headers=['A', 'B', 'D'])
#         cls.table = cls.tldb.get_object('table')
#
#     def test_gap(self):
#         root_children = self.table.index_structure.root.children
#         r = [[6, 118], [19, 20], [13.0, 72.0]]
#         result = nodes_range_search([root_children[0], root_children[1]], r)
#         self.assertEqual(0, len(result), "Should not return any result due to the gap [19, 20]")
#
#     def test_gap_fit_one(self):
#         root_children = self.table.index_structure.root.children
#         r = [[80, 85], [17, 600], [13.0, 72.0]]
#         result = nodes_range_search([root_children[0], root_children[1]], r)
#         self.assertEqual(1, len(result), "Should not return any result due to the gap [19, 20]")
#         self.assertEqual(result[0], root_children[1].children[1], "Second child should found a result")
#
#     def node_to_boundary(self):
#         root = self.table.index_structure.root
#         root_children = root.children
#         self.assertEqual(nodes_to_boundary(root_children), root.boundary)
