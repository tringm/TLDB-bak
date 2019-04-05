from test.tests import TestCaseCompare
from tldb.core.structure.context import RangeContext
import copy

from tldb.core.structure.interval import Interval


class TestRangeContext(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super(TestRangeContext, cls).setUpClass()
        cls.output_folder = cls.output_folder / 'core' / 'structure' / 'context' / 'range_context'

    def test_str(self):
        method_id = self.id().split('.')[-1]
        self.set_up_compare_files(method_id)
        context = RangeContext(['A', 'B', 'D'],
                               [Interval([6.0, 118.0]), Interval([17.0, 600.0]), Interval([13.0, 72.0])])
        with self.out_file[method_id].open(mode='w') as f:
            f.write(str(context))
        self.file_compare(self.out_file[method_id], self.exp_file[method_id])

    # def test_copy(self):
    #     method_id = self.id().split('.')[-1]
    #     self.set_up_compare_files(method_id)
    #
    #     context1 = RangeContext(['A', 'B', 'D'],
    #                             [Interval([6.0, 118.0]), Interval([17.0, 600.0]), Interval([13.0, 72.0])])
    #     context2 = copy.copy(context1)
    #     context2.intervals =
    #     context2.boundaries['A'] = [20, 25]
    #     with self.out_file[method_id].open(mode='w') as f:
    #         f.write(str(context1))
    #         f.write('\nChange boundary of A to [20, 25]\n')
    #         f.write(str(context2))
    #     self.assertNotEqual(context1, context2)
    #     self.file_compare(self.out_file[method_id], self.exp_file[method_id])

    def test_check_intersection(self):
        context = RangeContext(['A', 'B', 'D'],
                               [Interval((6.0, 118.0)), Interval([17.0, 600.0]), Interval([13.0, 72.0])])

        res = context.check_intersection_and_update_boundaries(RangeContext(['A'], [Interval([100, 120])]))
        self.assertTrue(res)
        self.assertEqual(context.intervals, (Interval([100, 118]), Interval([17.0, 600.0]), Interval([13.0, 72.0])))

    def test_check_no_intersection(self):
        context = RangeContext(['A', 'B', 'D'],
                               [Interval([6.0, 118.0]), Interval([17.0, 600.0]), Interval([13.0, 72.0])])
        res = context.check_intersection_and_update_boundaries(RangeContext(['A'], [Interval([120, 130])]))
        self.assertFalse(res)
        self.assertEqual(context.intervals, (Interval([6.0, 118.0]), Interval([17.0, 600.0]), Interval([13.0, 72.0])))

    def test_check_multi_intersection(self):
        context = RangeContext(['A', 'B', 'D'],
                               [Interval([6.0, 118.0]), Interval([17.0, 600.0]), Interval([13.0, 72.0])])
        res = context.check_intersection_and_update_boundaries(
            RangeContext(['A', 'B'], [Interval([100, 120]), Interval([20, 30])]))
        self.assertTrue(res)
        self.assertEqual(context.intervals, (Interval([100, 118.0]), Interval([20, 30]), Interval([13.0, 72.0])))

    def test_check_one_failed_intersection(self):
        context = RangeContext(['A', 'B', 'D'],
                               [Interval([6.0, 118.0]), Interval([17.0, 600.0]), Interval([13.0, 72.0])])
        res = context.check_intersection_and_update_boundaries(
            RangeContext(['A', 'B'], [Interval([100, 120]), Interval([10, 15])]))
        self.assertFalse(res)
        self.assertEqual(context.intervals, (Interval([6.0, 118.0]), Interval([17.0, 600.0]), Interval([13.0, 72.0])))
