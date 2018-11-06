from typing import Dict, Any, List

from .Entry import Entry
import math

import queue


class Node:
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

        filtered (bool)             : True if this node is filtered
        reason_of_filtered (str)    : reason for being filtered
        value_filtering_visited (bool)  : True if this node has been value filtered
        value_validation_visited (bool) : True if this node has been value validated

        link_xml {[str, [Node]}         : key is element name, value is list of Node connected
        link_sql {[str, [Node]}         : key is table name, value is list of Node connected
        intersection_range ({str, [boundary]}): key is element, value is multiple boundary constraint
                                                for the element. It is initiated in value filtering
    """

    def __init__(self, max_n_children):
        self.max_n_children = max_n_children
        self.parent = None
        self.children = []
        self.boundary = []
        self.entries = []
        self.name = ""
        self.dimension = -1

        # XML node attributes
        self.filtered = False
        self.reason_of_filtered = ""
        self.value_filtering_visited = False
        # self.value_validation_visited = False
        self.link_xml = {}
        self.link_sql = {}
        self.intersection_range = {}
        # self.validated_entries = []
        self.validated = False

        # Filter time
        self.value_filtering_time = -1
        self.connected_element_filtering_time = -1
        self.check_lower_level_time = -1
        self.init_children_time = -1
        self.filter_children_time = -1
        self.full_filtering_time = -1

        # Validation time
        self.value_validation_time = -1
        self.structure_validation_time = -1


    def __str__(self):
        return self.name + ':' + str(self.boundary)

    # def update_boundary(self, coordinates):
    # 	n_dimensions = len(coordinates)

    # 	# if boundary is empty
    # 	if (not self.boundary):
    # 		for i in range(n_dimensions):
    # 			self.boundary.append([coordinates[i], coordinates[i]])
    # 	# Go through each dimension
    # 	for i in range(n_dimensions):
    # 		boundary[i][0] = min(boundary[i][0], coordinates[i])
    # 		boundary[i][1] = max(boundary[i][1], coordinates[i])

    # def dynamic_add(self, entry : Entry):
    # 	# If this node is a leaf node
    # 	if (len(self.children) == 0):
    # 		# add entry to this node
    # 		self.entries.append(entry)
    # 		# if the boundary is empty or entry is not inside the boundary -> update boundary
    # 		if (not boundary) or (not entry.is_inside(boundary)):
    # 			self.update_boundary(entry.coordinates)
    # 		# if this leaf node contains more than allowed entries -> split
    # 		if (len(self.entries) > max_n_entries):
    # 			self.split

    def get_entries(self):
        # if this is a leaf node or has already been get entries before
        if len(self.entries) > 0:
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
        self.entries = entries
        return self.entries

    def get_entries_recursion(self):
        """
        This function return all entries contained in this node (included children's entries)
        :return: [Entry]
        """
        # if this node is a leaf node
        if len(self.entries) > 0:
            return self.entries
        # else get all child entry
        entries = []
        for child in self.children:
            child_entries = child.get_entries()
            for child_entry in child_entries:
                entries.append(child_entry)
        self.entries = entries
        return entries

    def get_unfiltered_leaf_node(self):
        """
        This function return a list of nodes which are the unfiltered leaves of this node
        :return: (List[Node]) leaf node of this not which are not filtered
        """
        leaf_nodes = []

        if not self.filtered:
            # if this node is leaf
            if len(self.entries) > 0:
                return [self]

            for child in self.children:
                leaf_nodes_child = child.get_unfiltered_leaf_node()
                for node in leaf_nodes_child:
                    leaf_nodes.append(node)
        return leaf_nodes

    def print_node(self, level=0):
        """Summary
        Simple implementation to print this node and its children
        Args:
            level (int, optional): current level for printing
        """
        if len(self.entries) > 0:
            print('\t' * (level + 1), 'NODE ', self.boundary, 'is Leaf')
            for i in range(len(self.entries)):
                print('\t' * (level + 1), self.entries[i].coordinates)
        else:
            print('\t' * (level + 1), 'NODE ', self.boundary)
            for child in self.children:
                child.print_node(level + 1)

    def print_node_not_filtered_with_link(self, level=0):
        """Summary
        Simple implementation to print this node and its children
        Args:
            level (int, optional): current level for printing
        """
        if not self.filtered:
            if len(self.entries) > 0:
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

    def print_node_not_filtered(self, level=0):
        """Summary
        Simple implementation to print this node and its children
        Args:
            level (int, optional): current level for printing
        """
        if not self.filtered:
            if len(self.entries) > 0:
                print('\t' * (level + 1), 'NODE ', self.boundary, 'is Leaf')
                for i in range(len(self.entries)):
                    print('\t' * (level + 1), self.entries[i].coordinates)
            else:
                print('\t' * (level + 1), 'NODE ', self.boundary)
                for child in self.children:
                    child.print_node_not_filtered(level + 1)

    def print_link(self, n_prefix_tabl = 1):
        print('\t' * n_prefix_tabl + 'Child ' + str(self))
        print('\t' * (n_prefix_tabl + 1) + 'link xml: ')
        for connected_element in self.link_xml:
            print('\t' * (n_prefix_tabl + 2) + str([str(node) for node in self.link_xml[connected_element]]))
        print('\t' * (n_prefix_tabl + 1) + 'link sql: ')
        for table_name in self.link_sql:
            print('\t' * (n_prefix_tabl + 2) + str([str(node) for node in self.link_sql[table_name]]))