import queue

from tldb.core.lib.dewey_id import get_center_index
from tldb.core.lib.entries import entries_to_boundary
from tldb.core.lib.nodes import nodes_to_boundary
from tldb.core.structure.boundary import Boundary
from tldb.core.structure.entry import Entry
from tldb.core.structure.interval import Interval
import copy
from typing import Tuple, Union, Set, List


class Node:
    # TODO: id (basically Dewey ID for fast traversal)
    # TODO: range search: change depth so that if find node go extra 1 layer
    # TODO: better desc search (using different level of boundary)
    """RTree Node

    Attributes:
        max_n_children (int)        : maximum number of child Node
        parent (List[Node])         : None if is root
        children (List[Node])       : list of children node
        boundary (List[Interval])   : MBR Each tuple contains list of 2 ints [lower_bound, upper_bound] of a dimension
                                      XML Node: [str, str], [int, int]
                                      SQL NOde: [int, int], [int, int], ...
        entries ([Entry])           : list of entries if this node is a leaf node, empty if not leaf node
    """

    def __init__(self, max_n_children, name, parent=None, children=None, entries=None, is_leaf=False):
        self._max_n_children = max_n_children
        self.parent = parent
        self._name = name
        self._children = set() if children is None else self.check_if_iterable_and_convert_to_set(children)
        self._entries = set() if entries is None else self.check_if_iterable_and_convert_to_set(entries)
        self._boundary = None
        self.init_boundary()
        self.is_leaf = is_leaf
        self.leaf_entries = None

    @staticmethod
    def check_if_iterable_and_convert_to_set(iter_obj):
        if not (isinstance(iter_obj, list) or isinstance(iter_obj, set) or isinstance(iter_obj, tuple)):
            raise ValueError("must be a list or a set or a tuple")
        if not isinstance(iter_obj, set):
            iter_obj = set(iter_obj)
        return iter_obj

    def __str__(self):
        return self.name + ':' + str(self.boundary)

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        # TODO: fix this (what about children, entries, is_leaf)
        return (self.name == other.name) and (self.boundary == other.boundary)

    def __hash__(self):
        return hash((self.name, self.boundary))

    @property
    def children(self):
        return self._children

    @children.setter
    def children(self, new_children: Union[Set, List, Tuple]):
        self._children = self.check_if_iterable_and_convert_to_set(new_children)
        self._boundary = nodes_to_boundary(new_children)

    @property
    def entries(self):
        return self._entries

    @entries.setter
    def entries(self, value):
        self._entries = value
        self._boundary = entries_to_boundary(value)

    @property
    def boundary(self):
        return self._boundary

    @property
    def name(self):
        return self._name

    @property
    def n_dimension(self):
        return len(self.boundary.intervals)

    @property
    def center_coord(self):
        intervals = self.boundary.intervals
        return Entry(tuple([intervals[i].center() for i in range(self.n_dimension)]))

    def init_boundary(self):
        if self.children:
            self._boundary = nodes_to_boundary(self.children)
        elif self.entries:
            self._boundary = entries_to_boundary(self.entries)

    def add_entry(self, entry):
        boundary = self.boundary
        e_coors = entry.coordinates
        self._entries.add(entry)
        if not boundary:
            self._boundary = Boundary([Interval((e_coors[d], e_coors[d])) for d in range(len(e_coors))])
        else:
            b_intervals = boundary.intervals
            n_dimension = self.n_dimension
            if n_dimension != len(e_coors):
                raise ValueError('Boundary %s not match dimension with entry %s' % (str(boundary), str(e_coors)))
            b_intervals = [b_intervals[d].join_tuple((e_coors[d], e_coors[d])) for d in range(n_dimension)]
            self._boundary = Boundary(b_intervals)

    def add_child_node(self, node):
        # TODO: Re arrange if child node > max child node
        self._children.add(node)
        boundary = self._boundary
        if not boundary:
            self._boundary = copy.copy(node.boundary)
        else:
            self._boundary.join_and_update_boundary(node.boundary)

    def get_leaf_entries(self):
        if self.is_leaf:
            return self._entries
        if not self.leaf_entries:
            node_queue = queue.Queue()
            node_queue.put(self)
            entries = set()

            while not node_queue.empty():
                node = node_queue.get()
                # if this is a leaf
                if node.is_leaf:
                    for entry in node.entries:
                        entries.add(entry)
                else:
                    for child_node in node.children:
                        node_queue.put(child_node)
            self.leaf_entries = entries
        return self.leaf_entries
        # cache entries

    def get_leaf_nodes(self):
        """
        This function return a list of nodes which are the unfiltered leaves of this node
        :return: (List[Node]) leaf node of this not which are not filtered
        """
        leaf_nodes = set()

        if self.is_leaf:
            return [self]
        for child in self.children:
            for node in child.get_leaf_nodes():
                leaf_nodes.add(node)
        return leaf_nodes

    # def compare_with_boundary(self, compare_boundary: Boundary):
    #     """
    #     Check if this node is inside multiple v_boundaries
    #     3 cases:
    #         - 0 : Not intersect
    #         - 1 : Intersect but not inside
    #         - 2 : Is inside
    #     :param compare_boundary:
    #     :return:
    #     """
    #     print(f"Compare {self} with boundary self_d {self.n_dimension}, compare_boundary d {compare_boundary.n_dimension}")
    #     if self.n_dimension != compare_boundary.n_dimension:
    #         raise ValueError('Comparing boundary must have the same number of dimension')
    #     return self.boundary.compare(compare_boundary)

    def range_search(self, boundary: Boundary):
        if not self.n_dimension == boundary.n_dimension:
            raise ValueError('Boundary mismatch')

        if self.boundary.compare(boundary) == 0:
            return set()

        checking_nodes = {self}
        in_range_nodes = set()
        while checking_nodes:
            n = checking_nodes.pop()
            checking_nodes.add(n)
            if n.is_leaf:
                break

            checking_nodes = {child for node in checking_nodes for child in node.children}

            # expected = len(checking_nodes) * self.max_n_children

            intersect_nodes = set()

            for node in checking_nodes:
                compare_res = node.boundary.compare(boundary)
                if compare_res == 1:
                    intersect_nodes.add(node)
                if compare_res == 2:
                    in_range_nodes.add(node)

            if not in_range_nodes.union(intersect_nodes):
                return set()

            checking_nodes = intersect_nodes

            # if len(satisfy_nodes)/expected > 0.7 or satisfy_nodes[0].isLeaf:
            #     break

        return in_range_nodes.union(checking_nodes)


class XMLNode(Node):
    """XML Node
    Attributes:
        filtered (bool)             : True if this node is filtered
        reason_of_filtered (str)    : reason for being filtered

        link_xml {[str, [Node]}     : key is element name, value is list of Node connected
        link_sql {[str, [Node]}     : key is table name, value is list of Node connected
                                        for the element. It is initiated in value filtering
    """
    def __init__(self, max_n_children, name, parent=None, children=None, entries=None, is_leaf=False):
        super().__init__(max_n_children, name, parent, children, entries, is_leaf)

        # TODO: This should only be initiated when filtering. Migrate this to able to run multiple query
        self.filtered = False
        self.reason_of_filtered = ""
        self.link_xml = {}
        self.link_sql = {}
        self.join_intervals = set()
        self.join_boundaries_attributes = []
        self.join_intervals_combined = {}
        self.link_children = set()
        self.inited_contexts = set()
        self.inited_link = False
        self.timer = {'init_link': [-1, -1], 'init_link_xml': [-1, -1], 'init_link_sql': [-1, -1],
                      'init_link_children': [-1, -1]}

    @property
    def idx_interval(self):
        if not self.boundary:
            return None
        if self.n_dimension > 2:
            raise ValueError('XML Node should only have 2 dimension')
        return self.boundary.get_interval(0)

    @property
    def v_interval(self):
        if not self.boundary:
            return None
        if self.n_dimension > 2:
            raise ValueError('XML Node should only have 2 dimension')
        return self.boundary.get_interval(1)

    @property
    def center_coord(self):
        # Specific for index coordinate, naive algorithm
        mean_index = get_center_index(self.idx_interval.low, self.idx_interval.high)
        return Entry((mean_index, self.v_interval.center()))

    def get_unfiltered_leaf_node(self):
        """
        This function return a list of nodes which are the unfiltered leaves of this node
        :return: (List[Node]) leaf node of this not which are not filtered
        """
        leaf_nodes = set()

        if not self.filtered:
            # if this node is leaf
            if self.entries:
                return [self]

            for child in self.children:
                leaf_nodes_child = child.get_unfiltered_leaf_node()
                for node in leaf_nodes_child:
                    leaf_nodes.add(node)
        return leaf_nodes

    def compare_with_idx_and_v_boundary(self, idx_interval: Interval, v_interval: Interval):
        """
        Check if this node is inside idx_boundary and v_boundary
            - 0 if not intersect
            - 1 if can be desc and intersect v_boundary
            - 2 if can be desc and inside v_boudary
        :param idx_interval:
        :param v_interval:
        :return:
        """
        self_idx_interval = self.idx_interval.interval

        if not idx_interval and not v_interval:
            return 2
        if idx_interval:
            compare_idx_interval = idx_interval.interval
            if self_idx_interval[1] <= compare_idx_interval[0]:
                return 0
            if self_idx_interval[0] > compare_idx_interval[1]:
                if not compare_idx_interval[1].is_ancestor(self_idx_interval[0]):
                    return 0
        if v_interval:
            return self.v_interval.compare(v_interval)
        #     if self_v_interval[1] < v_interval[0] or self_v_interval[0] > v_interval[1]:
        #         return 0
        #     if not (self_v_interval[0] >= v_interval[0] and self_v_interval[1] <= v_interval[1]):
        #         return 1
        #
        # return 2

    def desc_range_search(self, idx_interval: Tuple, v_interval: Tuple):
        if self.compare_with_idx_and_v_boundary(idx_interval, v_interval) == 0:
            return set()

        checking_nodes = set()
        checking_nodes.add(self)
        in_range_nodes = set()

        while checking_nodes:
            n = checking_nodes.pop()
            checking_nodes.add(n)
            if n.is_leaf:
                break

            checking_nodes = {child for node in checking_nodes for child in node.children}
            intersect_nodes = set()

            for node in checking_nodes:
                if node.compare_with_idx_and_v_boundary(idx_interval, v_interval) == 1:
                    intersect_nodes.add(node)
                if node.compare_with_idx_and_v_boundary(idx_interval, v_interval) == 2:
                    in_range_nodes.add(node)

            if not in_range_nodes and not intersect_nodes:
                return set()

            checking_nodes = intersect_nodes

        return in_range_nodes.union(checking_nodes)

    def link_sql_first_ancestor(self):
        if self.link_sql:
            return self.link_sql
        upper = self
        while upper.parent is not None and not upper.link_sql:
            upper = upper.parent
        return upper.link_sql

    def link_xml_first_ancestor(self):
        if self.link_xml:
            return self.link_xml
        upper = self
        while upper.parent is not None and not upper.link_xml:
            upper = upper.parent
        return upper.link_xml
