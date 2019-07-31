import unittest

from tests.test_case import TestCaseTimer
from tldb.core.structure.interval import Interval


class TestInterval(TestCaseTimer):
    def test_interval_from_tuple_and_list(self):
        interval1 = Interval([1, 2])
        interval2 = Interval((1, 2))
        self.assertEqual(interval1.interval, interval2.interval)

    def test_str(self):
        interval = Interval((1, 2))
        self.assertEqual(str(interval), str((1, 2)))

    # def test_interval_from_list(self):
    #     interval = Interval(list=[1, 2])
    #     self.assertEqual(interval.interval, (1, 2))
    #
    # def test_interval_from_tuple(self):
    #     interval = Interval(tuple=(1, 2))
    #     self.assertEqual(interval.interval, (1, 2))
    #
    # def test_interval_from_low_high(self):
    #     interval = Interval(low=1, high=2)
    #     self.assertEqual(interval.interval, (1, 2))

    # @unittest.expectedFailure
    # def test_interval_only_low(self):
    #     Interval(low=1)
    #
    # @unittest.expectedFailure
    # def test_interval_only_high(self):
    #     Interval(high=2)
    #
    # @unittest.expectedFailure
    # def test_interval_low_larger_than_high(self):
    #     Interval(low=10, high=2)
    #
    # @unittest.expectedFailure
    # def wrong_list(self):
    #     Interval(list=(1, 2))
    #
    # @unittest.expectedFailure
    # def wrong_tuple(self):
    #     Interval(tuple=[1, 2])

    @unittest.expectedFailure
    def test_interval_low_larger_than_high(self):
        Interval((20, 10))

    def test_join_tuple(self):
        self.assertEqual(Interval((1, 10)).join_tuple((5, 20)), Interval((1, 20)))
