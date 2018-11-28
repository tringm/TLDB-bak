import queue
from abc import ABC, abstractmethod

from numpy import mean

from src.structure.DeweyID import DeweyID
from src.lib.DeweyID import get_center_index
from src.lib.Entries import get_boundaries_from_entries
from src.lib.Nodes import get_boundaries_from_nodes
from src.lib.boundary import *

from typing import List
# TODO: id (basically Dewey ID for fast traversal)
# TODO: Find node that fit this range

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

    def print_node_not_filtered_with_link(self, level=0):
        """Summary
        Simple implementation to print this node and its children
        Args:
            level (int, optional): current level for printing
        """
        if not self.filtered:
            if self.entries is not None:
                print('\t' * level, 'NODE ', self.boundary, 'is Leaf')
                print('\t' * (level + 1), 'Linked XML: ')
                for connected_element_name in self.link_XML:
                    print('\t' * (level + 1), connected_element_name, end=" ")
                    for node in self.link_XML[connected_element_name]:
                        print(node.boundary, end=" ")
                    print()
                print('\t' * (level + 1), 'Linked SQL: ')
                for table_name in self.link_SQL:
                    print('\t' * (level + 1), table_name, end=" ")
                    for node in self.link_SQL[table_name]:
                        print(node.boundary, end=" ")
                    print()
                print('\t' * (level + 1), 'Entries')
                for i in range(len(self.entries)):
                    print('\t' * (level + 1), self.entries[i].coordinates)

            else:
                print('\t' * level, 'NODE ', self.boundary)
                print('\t' * (level + 1), 'Linked XML: ')
                for connected_element_name in self.link_XML.keys():
                    print('\t' * (level + 1), connected_element_name, end=" ")
                    for node in self.link_XML[connected_element_name]:
                        print(node.boundary, end=" ")
                    print()
                print('\t' * (level + 1), 'Linked SQL: ')
                for table_name in self.link_SQL:
                    print('\t' * (level + 1), table_name, end=" ")
                    for node in self.link_SQL[table_name]:
                        print(node.boundary, end=" ")
                    print()

                for child in self.children:
                    child.print_node_not_filtered_with_link(level + 1)

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
        value_filtering_visited (bool)  : True if this node has been value filtered

        link_xml {[str, [Node]}         : key is element name, value is list of Node connected
        link_sql {[str, [Node]}         : key is table name, value is list of Node connected
        intersection_range ({str, [boundary]}): key is element, value is multiple boundary constraint
                                                for the element. It is initiated in value filterings
    """
    def __init__(self, max_n_children, parent=None, children=None, entries=None, name='', dimension=-1):
        super().__init__(max_n_children, parent, children, entries, name, dimension)

        self.filtered = False
        self.reason_of_filtered = ""
        self.value_filtering_visited = False
        # self.value_validation_visited = False
        self.link_xml = {}
        self.link_sql = {}
        self.intersection_range = {}
        # self.validated_entries = []
        self.validated = False

        # Filtering Time
        self.start_full_filtering = -1
        self.end_full_filtering = -1
        self.start_value_filtering = -1
        self.end_value_filtering = -1
        self.start_ce_filtering = -1
        self.end_ce_filtering = -1
        self.start_check_lower_level = -1
        self.end_check_lower_level = -1
        self.start_init_children_link = -1
        self.end_init_children_link = -1
        self.start_filter_children = -1
        self.end_filter_children = -1
        self.start_filter_children_link_sql = -1
        self.end_filter_children_link_sql = -1

        # Validation time
        self.value_validation_time = -1
        self.structure_validation_time = -1

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

    def desc_range_search(self, idx_range: List[DeweyID], v_range, max_depth=2):
        if idx_range is None and v_range is None:
            raise ValueError('Must have some criteria, both idx_range and v_range is None')
        if idx_range is not None:
            if not index_boundary_can_be_ancestor(idx_range, self.boundary[0]):
                return
        if v_range is not None:
            if value_boundary_intersection(self.boundary[1], v_range) is None:
                return
        if self.isLeaf:
            return self

        checking_nodes = self.children.copy()
        depth = 1
        while checking_nodes:
            satisfy_node = []
            for idx, node in enumerate(checking_nodes):
                if idx_range is not None:
                    if not index_boundary_can_be_ancestor(idx_range, node.boundary[0]):
                        continue
                if v_range is not None:
                    if value_boundary_intersection(node.boundary[1], v_range) is None:
                        continue
                satisfy_node.append(node)
            if not satisfy_node:
                return
            if depth == max_depth or satisfy_node[0].isLeaf:
                checking_nodes = satisfy_node
                break

            depth += 1
            next_layer = []
            for node in satisfy_node:
                for child in node.children:
                    next_layer.append(child)
            checking_nodes = next_layer

        if not checking_nodes:
            return None
        return checking_nodes

    @property
    def full_filtering_time(self):
        return self.end_full_filtering - self.start_full_filtering

    @property
    def value_filtering_time(self):
        return self.end_value_filtering - self.start_value_filtering

    @property
    def connected_element_filtering_time(self):
        return self.end_ce_filtering - self.start_ce_filtering

    @property
    def check_lower_level_time(self):
        return self.end_check_lower_level - self.start_check_lower_level

    @property
    def init_children_link_time(self):
        return self.end_init_children_link - self.start_init_children_link

    @property
    def filter_children_time(self):
        return self.end_filter_children - self.start_filter_children

    @property
    def filter_children_link_sql_time(self):
        return self.end_filter_children_link_sql - self.start_filter_children_link_sql


class SQLNode(Node):
    def __init__(self, max_n_children, parent=None, children=None, entries=None, name='', dimension=-1):
        super().__init__(max_n_children, parent, children, entries, name, dimension)

    def get_center_coord(self):
        return [mean(self.boundary[i]) for i in range(len(self.boundary))]

    def range_search(self, boundaries: List[List[int]], max_depth=2):
        if not len(self.boundary) == len(boundaries):
            raise ValueError('Range mismatch')

        for idx, boundary in enumerate(boundaries):
            if boundary is not None:
                if value_boundary_intersection(self.boundary[idx], boundary) is None:
                    return

        if self.isLeaf:
            return self

        checking_nodes = self.children.copy()
        depth = 1
        while checking_nodes:
            satisfy_node = []
            for idx, node in enumerate(checking_nodes):
                node_ok = True
                for d, boundary in enumerate(boundaries):
                    if boundary is not None:
                        if value_boundary_intersection(node.boundary[d], boundary) is None:
                            node_ok = False
                            break
                if node_ok:
                    satisfy_node.append(node)
            if not satisfy_node:
                return
            if depth == max_depth or satisfy_node[0].isLeaf:
                checking_nodes = satisfy_node
                break

            depth += 1
            next_layer = []
            for node in satisfy_node:
                for child in node.children:
                    next_layer.append(child)
            checking_nodes = next_layer

        if not checking_nodes:
            return None
        return checking_nodes
