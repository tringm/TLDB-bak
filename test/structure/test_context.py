from test.tests import TestCaseCompare
from tldb.core.structure.context import RangeContext
import copy


class TestRangeContext(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super(TestRangeContext, cls).setUpClass()
        cls.out_file = {}
        cls.exp_file = {}
        cls.output_folder = cls.output_folder / 'structure' / 'context' / 'range_context'

    def test_str(self):
        method_id = self.id().split('.')[-1]
        self.prepare_compare_files(method_id)

        context = RangeContext(['A', 'B', 'D'], [[6.0, 118.0], [17.0, 600.0], [13.0, 72.0]])
        with self.out_file[method_id].open(mode='w') as f:
            f.write(str(context))
        self.file_compare(self.out_file[method_id], self.exp_file[method_id])

    def test_copy(self):
        method_id = self.id().split('.')[-1]
        self.prepare_compare_files(method_id)

        context1 = RangeContext(['A', 'B', 'D'], [[6.0, 118.0], [17.0, 600.0], [13.0, 72.0]])
        context2 = copy.copy(context1)
        context2.boundaries['A'] = [20, 25]
        with self.out_file[method_id].open(mode='w') as f:
            f.write(str(context1))
            f.write('\nChange boundary of A to [20, 25]\n')
            f.write(str(context2))
        self.assertNotEqual(context1, context2)
        self.file_compare(self.out_file[method_id], self.exp_file[method_id])

    def test_check_intersection(self):
        context = RangeContext(['A', 'B', 'D'], [[6.0, 118.0], [17.0, 600.0], [13.0, 72.0]])
        res = context.check_intersection_and_update_boundary('A', [100, 120])
        self.assertTrue(res)
        self.assertEqual(context, RangeContext(['A', 'B', 'D'], [[100, 118.0], [17.0, 600.0], [13.0, 72.0]]))

    def test_check_no_intersection(self):
        context = RangeContext(['A', 'B', 'D'], [[6.0, 118.0], [17.0, 600.0], [13.0, 72.0]])
        res = context.check_intersection_and_update_boundary('A', [120, 130])
        self.assertFalse(res)
        self.assertEqual(context, RangeContext(['A', 'B', 'D'], [[6.0, 118.0], [17.0, 600.0], [13.0, 72.0]]))
