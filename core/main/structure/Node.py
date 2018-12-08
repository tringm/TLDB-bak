import queue
from abc import ABC, abstractmethod
from typing import List

from numpy import mean

from core.main.lib.DeweyID import get_center_index
from core.main.lib.Entries import get_boundaries_from_entries
from core.main.lib.Nodes import get_boundaries_from_nodes
from core.main.lib.boundary import update_boundary_from_entry, update_boundary_from_node, value_boundary_intersection, \
    index_boundary_can_be_ancestor, boundary_is_inside
from core.main.structure.DeweyID import DeweyID


# TODO: id (basically Dewey ID for fast traversal)
# TODO: range search: change depth so that if find node go extra 1 layer
# TODO: ancestor link: What happened if it is empty

class Node(ABC):
    """RTree Node

    Attributes:
        max_n_children (int): maximum number of child Node
        parent (Node)               : None if is root
        children ([Node))           : list of children node
        boundary ([])               : MBR Each tuple contains list of 2 ints [lower_bound, upper_bound] of a dimension
                                      XML Node: [str, str], [int, int]
                                      SQL NOde: [int, int], [int, int], ...
        entries ([Entry])           : list of entries if this node is a leaf node, empty if not leaf node
        name (str)                  : name of element for XML node, name of table for SQL Node
        dimension (int)             : 1 for XML Node, index of highest element in SQL Node
    """

    def __init__(self, max_n_children, parent=None, children=None, entries=None, name='', dimension=-1):
        self.max_n_children = max_n_children
        self.parent = parent
        self.name = name
        self.dimension = dimension
        self._children = [] if children is None else children
        self._entries = [] if entries is None else entries
        self.original_boundary = self.init_boundary()
        self.boundary = self.original_boundary
        self.isLeaf = False

    def __str__(self):
        return self.name + ':' + str(self.boundary)

    def __repr__(self):
        return str(self)

    # def __eq__(self, other):
    #
    #
    # def __hash__(self):
    #     return hash

    @property
    def children(self):
        return self._children

    @children.setter
    def children(self, value):
        self._children = value
        self.boundary = get_boundaries_from_nodes(value)

    @property
    def entries(self):
        return self._entries

    @entries.setter
    def entries(self, value):
        self._entries = value
        self.boundary = get_boundaries_from_entries(value)

    @abstractmethod
    def get_center_coord(self):
        pass

    def init_boundary(self):
        if not self.children and not self.entries:
            return None
        if self.children:
            return get_boundaries_from_nodes(self.children)
        return get_boundaries_from_entries(self.entries)

    def add_entry(self, entry):
        self._entries.append(entry)
        self.boundary = update_boundary_from_entry(self.boundary, entry)

    def add_child_node(self, node):
        self._children.append(node)
        self.boundary = update_boundary_from_node(self.boundary, node)

    def get_entries(self):
        # if this is a leaf node or has already been get entries before
        if not self.entries:
            return self.entries

        node_queue = queue.Queue()
        node_queue.put(self)
        entries = []

        while not node_queue.empty():
            node = node_queue.get()
            # if this is a leaf
            if len(node.entries) > 0:
                for entry in node.entries:
                    entries.append(entry)
            else:
                for child_node in node.children:
                    node_queue.put(child_node)

        # cache entries
        self._entries = entries
        return self.entries

    def get_leaf_nodes(self):
        """
        This function return a list of nodes which are the unfiltered leaves of this node
        :return: (List[Node]) leaf node of this not which are not filtered
        """
        leaf_nodes = []

        if self.isLeaf:
            return [self]
        for child in self.children:
            for node in child.get_leaf_nodes():
                leaf_nodes.append(node)
        return leaf_nodes


class XMLNode(Node):
    """XML Node
    Attributes:
        filtered (bool)             : True if this node is filtered
        reason_of_filtered (str)    : reason for being filtered

        link_xml {[str, [Node]}         : key is element name, value is list of Node connected
        link_sql {[str, [Node]}         : key is table name, value is list of Node connected
                                                for the element. It is initiated in value filterings
    """
    def __init__(self, max_n_children, parent=None, children=None, entries=None, name='', dimension=-1):
        super().__init__(max_n_children, parent, children, entries, name, dimension)

        self.filtered = False
        self.reason_of_filtered = ""
        # self.value_filtering_visited = False
        # self.value_validation_visited = False
        self.link_xml = {}
        self.link_sql = {}
        # self.intersection_range = {}

        self.join_boundaries = {}
        self.join_boundaries_combined = {}
        self.link_children = []

        self.inited_contexts = []

        # self.validated_entries = []
        # self.validated = False

        self.inited_link = False

        # New time
        self.timer = {'init_link': [-1, -1], 'init_link_xml': [-1, -1], 'init_link_sql': [-1, -1],
                      'init_link_children': [-1, -1]}

        # # Filtering Time
        # self.start_full_filtering = -1
        # self.end_full_filtering = -1
        # self.start_value_filtering = -1
        # self.end_value_filtering = -1
        # self.start_ce_filtering = -1
        # self.end_ce_filtering = -1
        # self.start_check_lower_level = -1
        # self.end_check_lower_level = -1
        # self.start_init_children_link = -1
        # self.end_init_children_link = -1
        # self.start_filter_children = -1
        # self.end_filter_children = -1
        # self.start_filter_children_link_sql = -1
        # self.end_filter_children_link_sql = -1
        #
        # # Validation time
        # self.value_validation_time = -1
        # self.structure_validation_time = -1

    def get_center_coord(self):
        # Specific for index coordinate, naive algorithm
        mean_index = get_center_index(self.boundary[0][0], self.boundary[0][1])
        return [mean_index, mean(self.boundary[1])]

    def get_unfiltered_leaf_node(self):
        """
        This function return a list of nodes which are the unfiltered leaves of this node
        :return: (List[Node]) leaf node of this not which are not filtered
        """
        leaf_nodes = []

        if not self.filtered:
            # if this node is leaf
            if self.entries:
                return [self]

            for child in self.children:
                leaf_nodes_child = child.get_unfiltered_leaf_node()
                for node in leaf_nodes_child:
                    leaf_nodes.append(node)
        return leaf_nodes

    def desc_range_search(self, idx_range: List[DeweyID], v_range):
        if idx_range is None and v_range is None:
            raise ValueError('Must have some criteria, both idx_range and v_range is None')
        if idx_range is not None:
            if not index_boundary_can_be_ancestor(idx_range, self.boundary[0]):
                return
        if v_range is not None:
            if value_boundary_intersection(self.boundary[1], v_range) is None:
                return
        if self.isLeaf:
            return [self]

        checking_nodes = self.children.copy()
        satisfy_nodes = []
        while checking_nodes:
            expected = len(checking_nodes) * self.max_n_children
            satisfy_nodes = []
            for idx, node in enumerate(checking_nodes):
                if idx_range is not None:
                    if not index_boundary_can_be_ancestor(idx_range, node.boundary[0]):
                        continue
                if v_range is not None:
                    if value_boundary_intersection(node.boundary[1], v_range) is None:
                        continue
                satisfy_nodes.append(node)
            if not satisfy_nodes:
                return

            if satisfy_nodes[0].isLeaf:
                break

            if len(satisfy_nodes)/expected > 0.7 or satisfy_nodes[0].isLeaf:
                break

            next_layer = []
            for node in satisfy_nodes:
                for child in node.children:
                    next_layer.append(child)
            checking_nodes = next_layer

        return satisfy_nodes

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

    @property
    def idx_boundary(self):
        if not self.boundary:
            return None
        if len(self.boundary) > 2:
            raise ValueError('XML Node only allowed 2 dimensions')
        return self.boundary[0]

    @property
    def v_boundary(self):
        if not self.boundary:
            return None
        if len(self.boundary) > 2:
            raise ValueError('XML Node only allowed 2 dimensions')
        return self.boundary[1]

    # @property
    # def full_filtering_time(self):
    #     return self.end_full_filtering - self.start_full_filtering
    #
    # @property
    # def value_filtering_time(self):
    #     return self.end_value_filtering - self.start_value_filtering
    #
    # @property
    # def connected_element_filtering_time(self):
    #     return self.end_ce_filtering - self.start_ce_filtering
    #
    # @property
    # def check_lower_level_time(self):
    #     return self.end_check_lower_level - self.start_check_lower_level
    #
    # @property
    # def init_children_link_time(self):
    #     return self.end_init_children_link - self.start_init_children_link
    #
    # @property
    # def filter_children_time(self):
    #     return self.end_filter_children - self.start_filter_children
    #
    # @property
    # def filter_children_link_sql_time(self):
    #     return self.end_filter_children_link_sql - self.start_filter_children_link_sql


class SQLNode(Node):
    def __init__(self, max_n_children, parent=None, children=None, entries=None, name='', dimension=-1):
        super().__init__(max_n_children, parent, children, entries, name, dimension)

    def get_center_coord(self):
        return [mean(self.boundary[i]) for i in range(len(self.boundary))]

    def is_inside_boundary(self, boundaries):
        if not len(self.boundary) == len(boundaries):
            raise ValueError('Boundary mismatch')
        for d, boundary in enumerate(boundaries):
            if boundary is not None:
                if not boundary_is_inside(self.boundary[d], boundary):
                    return False
        return True

    def children_inside_boundary(self, boundaries):
        return list(filter(lambda child: child.is_inside_boundary, self.children))

    def intersect_with_boundary(self, boundaries):
        if not len(self.boundary) == len(boundaries):
            raise ValueError('Boundary mismatch')
        for d, boundary in enumerate(boundaries):
            if boundary is not None:
                if value_boundary_intersection(self.boundary[d], boundary) is None:
                    return False
        return True

    def range_search(self, boundaries: List[List[int]]):
        if not len(self.boundary) == len(boundaries):
            raise ValueError('Boundary mismatch')

        for idx, boundary in enumerate(boundaries):
            if boundary is not None:
                if value_boundary_intersection(self.boundary[idx], boundary) is None:
                    return

        if self.isLeaf:
            return [self]

        checking_nodes = self.children
        satisfy_nodes = []
        while checking_nodes:
            expected = len(checking_nodes) * self.max_n_children

            satisfy_nodes = list(filter(lambda node: node.intersect_with_boundary(boundaries), checking_nodes))

            # if checking_nodes[0].isLeaf:
            #     satisfy_nodes = list(filter(lambda node: node.intersect_with_boundary(boundaries), checking_nodes))
            # else:
            #     satisfy_nodes = list(filter(lambda node: node.is_inside_boundary(boundaries), checking_nodes))

            if not satisfy_nodes:
                return

            if satisfy_nodes[0].isLeaf:
                break

            if len(satisfy_nodes)/expected > 0.7 or satisfy_nodes[0].isLeaf:
                break

            checking_nodes = list(map(lambda node: node.children_inside_boundary(boundaries), satisfy_nodes))
            checking_nodes = [c for child in checking_nodes for c in child]

        return satisfy_nodes
