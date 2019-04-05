from typing import List, Union, Tuple

from tldb.core.lib.interval import intersect_two_intervals
from tldb.core.structure.interval import Interval


class Context:
    def __init__(self, attributes):
        self.attributes = attributes


class RangeContext(Context):
    """
    TODO Text range(?)
    """
    def __init__(self,
                 attributes: Union[List[str], Tuple[str, ...]],
                 intervals: Union[List[Interval], Tuple[Interval, ...]],
                 nodes=None):
        """
        RangeContext contains
        :param attributes: name of the attributes
        :param intervals: Tuple of intervals
        :param nodes: Checking node
        """
        if not (isinstance(intervals, List) or isinstance(intervals, Tuple)):
            raise ValueError(f"intervals must be of type list or tuple")
        if isinstance(intervals, List):
            intervals = tuple(intervals)
        if not (isinstance(attributes, List) or isinstance(attributes, Tuple)):
            raise ValueError(f"attributes must be of type list or tuple")
        if isinstance(attributes, List):
            attributes = tuple(attributes)
        if len(attributes) != len(intervals):
            raise ValueError(f"Attributes size({len(attributes)}) != boundaries size({len(intervals)})")
        super().__init__(attributes)
        self.n_dimension = len(attributes)
        self.intervals = intervals
        if not nodes:
            self.nodes = [None] * self.n_dimension
        else:
            self.nodes = nodes

    def __str__(self):
        rep_string = ''
        rep_string += f"Attribute: {self.attributes}\n"
        rep_string += 'Intervals: ' + str(self.intervals) + '\n'
        rep_string += 'Nodes: ' + str(self.nodes) + '\n'
        return rep_string

    def __repr__(self):
        rep = f"a: {self.attributes} i: {self.intervals[:2]}... n:  {self.nodes[:2]}..."
        return rep

    def __copy__(self):
        return RangeContext(self.attributes, self.intervals, self.nodes)

    def __eq__(self, other):
        return self.attributes == other.attributes and self.intervals == other.intervals

    def __hash__(self):
        return hash((self.attributes, self.intervals))

    def check_intersection_and_update_boundaries(self, other_context):
        """
        Check attr wise if this context interval intersect with other context
            - Not intersect -> Return False
            - Intersect -> Update boundary and return True
        :param other_context: RangeContext
        :return: False if one or all of attr does not intersect, True and update this context boundary otherwise
        """
        self_intv = self.intervals
        self_attr = self.attributes
        self_attr_to_idx = {self_attr[idx]: idx for idx in range(len(self_attr))}
        other_intv = other_context.intervals
        other_attr = other_context.attributes
        other_attr_to_idx = {other_attr[idx]: idx for idx in range(len(other_attr))}

        common_att = list(set(self_attr_to_idx.keys()).intersection(other_attr_to_idx.keys()))

        for idx, attr in enumerate(common_att):
            res = intersect_two_intervals(self_intv[self_attr_to_idx[attr]], other_intv[other_attr_to_idx[attr]])
            if not res:
                return False
            common_att[idx] = (attr, res)
        for attr, res in common_att:
            self_intv[self_attr_to_idx[attr]].update(res.interval)
        return True
