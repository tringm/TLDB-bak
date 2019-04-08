from numpy import mean

from tldb.core.structure.dewey_id import DeweyID
from typing import List, Tuple, Union


class Interval:
    def __init__(self, interval: Union[List, Tuple]):
        """
        An interval can be load from
         list           : a list of 2 values
         tuple          : a tuple of 2 values
         low, high      : two parameter for lower and higher
        :param interval:
        """
        try:
            interval = tuple(interval)
        except TypeError:
            raise TypeError("other_tuple must be of type tuple or can be convert to tuple")
        self._interval = interval
        # else:
        #     if not kwargs:
        #         raise ValueError("Must init interval by either 'list', 'tuple', or 'low' and 'high' pair")
        #     if 'list' in kwargs:
        #         if 'tuple' in kwargs:
        #             raise ValueError("Only one of list or tuple should be specified")
        #         if 'low' in kwargs or 'high' in kwargs:
        #             raise ValueError("Only one of list or low, high pair should be specified")
        #         list_interval = kwargs['list']
        #         if not isinstance(list_interval, list):
        #             raise ValueError("List argument should be list")
        #         if len(list_interval) != 2:
        #             raise ValueError('interval must contains exact 2 component of low and high')
        #         self._interval = tuple(list_interval)
        #     elif 'tuple' in kwargs:
        #         if 'low' in kwargs or 'high' in kwargs:
        #             raise ValueError("Only one of tuple or low, high pair should be specified")
        #         tuple_interval = kwargs['tuple']
        #         if len(tuple_interval) != 2:
        #             raise ValueError('Interval must contains exact 2 component of low and high')
        #         if not isinstance(tuple_interval, tuple):
        #             raise ValueError("Tuple argument should be tuple")
        #         self._interval = tuple_interval
        #     elif 'low' in kwargs or 'high' in kwargs:
        #         if not ('low' in kwargs and 'high' in kwargs):
        #             raise ValueError('Both low and high should be specified')
        #         self._interval = (kwargs['low'], kwargs['high'])
        #
        if self._interval[0] > self._interval[1]:
            raise ValueError("Interval low should be smaller than interval high")

    @property
    def interval(self):
        return self._interval

    @property
    def low(self):
        return self._interval[0]

    @property
    def high(self):
        return self._interval[1]

    def __str__(self):
        return f"{self._interval}"

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        return self.interval.__eq__(other.interval)

    def __hash__(self):
        return hash(self.interval)

    def join_tuple(self, t):
        """
        Join the interval with another tuple
        >>> Interval((1, 10)).join_tuple((5, 20))
        Interval((1, 20))
        :return:
        """
        if len(t) != 2:
            raise ValueError("The join tuple must have length of 2")
        return Interval((min(self.low, t[0]), max(self.high, t[1])))

    def center(self):
        return mean(self.interval)

    def update(self, new_interval):
        if not isinstance(new_interval, tuple):
            raise ValueError("new interval must be a tuple")
        self._interval = new_interval

    def check_intersect(self, compare_interval):
        """
        Check if this interval is:
            - 0 : Not intersect with other interval
            - 1 : Intersect but not inside other boundary
            - 2 : Is inside other boundary
        :param compare_interval:
        :return:
        """
        s_interval = self.interval
        c_interval = compare_interval.interval
        if s_interval[1] < c_interval[0] or s_interval[0] > c_interval[1]:
            return 0
        if s_interval[0] >= c_interval[0] and s_interval[1] <= c_interval[1]:
            return 2
        return 1

    def check_is_descendant(self, compare_interval):
        """
        Check if this DeweyID interval is:
            - 0 : cannot be descendant
            - 1 : can be descendant with some intersection
            - 2 : can be descendant and is completely inside
        :param compare_interval:
        :return:
        """
        s_interval = self.interval
        c_interval = compare_interval.interval
        if not isinstance(s_interval[0], DeweyID):
            raise ValueError("This is not an interval of DeweyID")
        if not isinstance(c_interval[0], DeweyID):
            raise ValueError("Comparing interval is not an interval of DeweyID")
        if c_interval[0] > s_interval[1]:
            return 0
        c_interval_upper = c_interval[1].plus_one_last_divison()
        if s_interval[0] >= c_interval_upper:
            return 0
        if s_interval[0] >= c_interval[0] and s_interval[1] < c_interval_upper:
            return 2
        return 1

    def extend_interval(self, other_interval):
        try:
            other_interval = tuple(other_interval)
        except TypeError:
            raise TypeError("other_tuple must be of type tuple or can be convert to tuple")
        self._interval = self._interval + other_interval
