from typing import Tuple, Union, List

from tldb.core.lib.interval import union_two_intervals
from tldb.core.structure.interval import Interval


class Boundary:
    def __init__(self, intervals: Union[Tuple[Interval, ...], List[Interval]]):
        self._intervals = self.check_interval_types_and_convert(intervals)
        self._n_dimension = len(intervals)

    def __copy__(self):
        return Boundary(self._intervals)

    def __str__(self):
        return f"B:{self._intervals}"

    def __eq__(self, other):
        return self._intervals == other.intervals

    def __hash__(self):
        return hash(self._intervals)

    @property
    def n_dimension(self):
        return self._n_dimension
    
    @property
    def intervals(self):
        return self._intervals

    @intervals.setter
    def intervals(self, new_intervals: Tuple[Interval, ...]):
        self._intervals = self.check_interval_types_and_convert(new_intervals)
        self._n_dimension = len(new_intervals)

    def get_interval(self, dimension):
        return self._intervals[dimension]

    def join_and_update_boundary(self, other):
        if other.n_dimension != self.n_dimension:
            raise ValueError("Joining boundary must have the same number of dimension")
        self_intervals = list(self.intervals)

        other_intervals = other.intervals

        for idx, s_interval in enumerate(self_intervals):
            self_intervals[idx] = union_two_intervals(s_interval, other_intervals[idx])

        self._intervals = tuple(self_intervals)

    @staticmethod
    def check_interval_types_and_convert(intervals: Union[List[Interval], Tuple[Interval, ...]]):
        if not intervals:
            raise ValueError("intervals must not be empty")
        try:
            intervals = tuple(intervals)
        except TypeError:
            raise TypeError("Interval must be a tuple or can be converted to a tuple")
        # try:
        #     intervals = tuple(map(lambda x: Interval(x) if x else None, intervals))
        # except TypeError:
        #     raise ValueError("intervals must be a tuple or list of Interval or None")
        return intervals

    def extend(self, extend_intervals):
        extend_intervals = self.check_interval_types_and_convert(extend_intervals)
        self._intervals = self._intervals + extend_intervals

    def compare(self, other_boundary):
        """
        Compare pairwise the 2 boundary's intervals. Check if this boundary:
            - 0 : Not intersect with other boundary (One of the interval does not intersect)
            - 1 : Intersect but not inside other boundary (One of the interval is not inside)
            - 2 : Is inside other boundary (All interval is inside pairwise)
        :param other_boundary: Boundary
        :return: 0 1 or 2
        """
        if self.n_dimension != other_boundary.n_dimension:
            raise ValueError('Comparing boundary must have the same number of dimension')
        is_inside = True
        other_intervals = other_boundary.intervals
        self_intervals = self.intervals
        for idx, c_interval in enumerate(other_intervals):
            if c_interval:
                compare_res = self_intervals[idx].compare(other_intervals[idx])
                if compare_res == 0:
                    return 0
                elif compare_res == 1:
                    is_inside = False
        if not is_inside:
            return 1
        return 2
