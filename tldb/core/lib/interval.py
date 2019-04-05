from tldb.core.structure.interval import Interval
from typing import Union, List


def union_two_intervals(interval1: Interval, interval2: Interval):
    """
    Return the join interval of 2 intervals
    >>> union_two_intervals(Interval(5, 10), Interval(1, 6))
    Interval(1, 10)
    :param interval1:
    :param interval2:
    :return:
    """
    return Interval((min(interval1.low, interval2.low), max(interval1.high, interval2.high)))


def union_multiple_intervals(intervals: List[Interval]):
    lows = [interval.low for interval in intervals]
    highs = [interval.high for interval in intervals]
    return Interval((min(lows), max(highs)))


def intersect_two_intervals(interval1: Interval, interval2: Interval) -> Union[Interval, None]:
    """
    Return the intersection of 2 interval if exist intersection, else return None
    :param interval1:
    :param interval2:
    :return:
    """
    if interval1.high < interval2.low or interval1.low > interval2.high:
        return None
    return Interval((max(interval1.low, interval2.low), min(interval1.high, interval2.high)))
