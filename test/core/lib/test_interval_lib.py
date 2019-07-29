from test.test_case import TestCaseTimer
from tldb.core.lib.interval import union_two_intervals, intersect_two_intervals, union_multiple_intervals
from tldb.core.structure.interval import Interval


class TestInterValLib(TestCaseTimer):
    def test_union_two_intervals(self):
        interval1 = Interval((1, 10))
        interval2 = Interval((5, 20))
        self.assertEqual(union_two_intervals(interval1, interval2).interval, (1, 20))

    def test_union_multiple_intervals(self):
        intervals = [Interval((5, 10)), Interval((15, 20)), Interval((25, 30))]
        self.assertEqual(union_multiple_intervals(intervals), Interval((5, 30)))

    def test_intersection_two_intervals(self):
        interval1 = Interval((1, 10))
        interval2 = Interval((5, 20))
        self.assertEqual(intersect_two_intervals(interval1, interval2).interval, (5, 10))
