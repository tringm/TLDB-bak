import logging
import timeit
from math import ceil, log
from queue import Queue

from tldb.core.structure.entry import Entry
from tldb.core.structure.node import Node
from .index_structure import IndexStructure
from tldb.core.lib.entries import entries_to_boundary


class RTree(IndexStructure):
    def __init__(self, object_name, max_n_children):
        super().__init__('rtree', object_name)
        self.root = None
        self.max_n_children = max_n_children
        if self.max_n_children < 2:
            raise ValueError('Maximum number of children nodes must be >= 2')
        self.max_n_children = max_n_children

    def __str__(self):
        tree_to_string = ''

        def pprint_node(node, n_prefix_tab=0):
            nonlocal tree_to_string
            if node.is_leaf:
                tree_to_string += '\t' * n_prefix_tab + str(node) + '(leaf)\n'
                for entry in node.entries:
                    tree_to_string += '\t' * (n_prefix_tab + 1) + str(entry) + '\n'
            else:
                tree_to_string += '\t' * n_prefix_tab + str(node) + '\n'
                for child in node.children:
                    pprint_node(child, n_prefix_tab + 1)

        pprint_node(self.root)
        return tree_to_string

    def ordered_str(self):
        tree_to_string = ''

        def pprint_node(node, n_prefix_tab=0):
            nonlocal tree_to_string
            if node.is_leaf:
                tree_to_string += '\t' * n_prefix_tab + str(node) + '(leaf)\n'
                for entry in sorted(node.entries, key=lambda e: e.coordinates[0]):
                    tree_to_string += '\t' * (n_prefix_tab + 1) + str(entry) + '\n'
            else:
                tree_to_string += '\t' * n_prefix_tab + str(node) + '\n'
                for child in sorted(node.children, key=lambda n: n.boundary.get_interval(0).low):
                    pprint_node(child, n_prefix_tab + 1)

        pprint_node(self.root)
        return tree_to_string

    def __repr__(self):
        return self.object_name + ':' + self.name + ':' + str(self.root)

    def load(self, entries, method='str', dimension=1, node_type=Node):
        if method == 'str':
            self.str_bulk_loading(node_type, entries, dimension)
        elif method == 'stripe':
            self.stripe_bulk_loading(node_type, entries, dimension)
        else:
            raise ValueError(f"Loading method {method} not supported. Use either str or stripe")

    def stripe_bulk_loading(self, node_type, entries: [Entry], dimension: int):
        """Summary
        Bulk loading RTree by dividing the hyperplane into hyper-rectangle

        :param node_type: Either XMLNode or SQLNode
        :param entries: list of input entries
        :param dimension: dimension to cut when bulk loading (currently only value dimension - 1)
        :return: root node of loaded RTree
        """
        logger = logging.getLogger(f"Indexer:{self.name}")

        start_sorting = timeit.default_timer()
        entries.sort(key=lambda entry: entry.coordinates[dimension])
        logger.debug('%s %d', 'sorting took:', timeit.default_timer() - start_sorting)
        n_entries = len(entries)

        # Configuration
        queue = Queue()

        # Initialization
        # Create root node
        root = node_type(self.max_n_children, name=self.object_name)
        # root.boundary = entries_to_boundary(entries)

        queue.put((root, (0, n_entries)))

        while not queue.empty():
            current_node, current_range = queue.get()
            current_n_entries = current_range[1] - current_range[0]  # Number of entries contained in this current node
            # Calculate the height of this subtree based on max_n_children
            height = ceil(round(log(current_n_entries, self.max_n_children), 5))
            # logger.debug('%s %s', 'current_range:', ','.join(str(number) for number in current_range))
            # logger.debug('%s %d', 'current_n_entries:', current_n_entries)
            # logger.debug('%s %d', 'height:', height)

            # if current node contains has n_entries <= max_n_children then this is a leaf and proceed to add entries
            if current_n_entries <= self.max_n_children:
                current_node.is_leaf = True
                # logger.debug("Found leaf => add entries")
                adding_entries = entries[current_range[0]:current_range[1]]
                # logger.debug('%s %d', "len(adding_entries):", len(adding_entries))
                for i in range(len(adding_entries)):
                    current_node.add_entry(adding_entries[i])

            else:
                logger.debug('Not a leaf => add new nodes')
                # Number of entries contained in the subtree of this node
                n_entries_subtree = self.max_n_children ** (height - 1)
                #  Number of slices according to the formula of OMT
                n_slices = ceil(current_n_entries / n_entries_subtree)

                # logger.debug('%s %d', 'n_entries_subtree', n_entries_subtree)
                # logger.debug('%s %d', 'n_slices', n_slices)

                # divide into n_slice + 1 nodes, add to current node
                for i in range(n_slices):
                    # n_entries_slice = current_n_entries
                    range_low = current_range[0] + i * n_entries_subtree
                    range_high = range_low + n_entries_subtree

                    # last group might have more than max_n_children
                    if i == n_slices - 1:
                        range_high = current_range[1]

                    # logger.debug('%s %d %s %d %d', "Child node index:", i, "range", range_low, range_high)

                    subtree_boundary = entries_to_boundary(set(entries[range_low:range_high]))
                    subtree_node = node_type(self.max_n_children, parent=current_node, name=self.object_name,
                                             boundary=subtree_boundary)
                    # logger.debug('%s %s', "Child node", str(subtree_node))
                    current_node.add_child_node(subtree_node)
                    queue.put((subtree_node, (range_low, range_high)))
        self.root = root

    def str_bulk_loading(self, node_type, entries: [Entry], dimension: int):
        """Summary
        Bulk loading using sort-tile-recursive

        :param node_type: Type of Node in this tree (XMLNode or SQLNode)
        :param entries: list of input entries
        :param tree_name: name of the tree
        :param max_n_children: maximum number of children in a node of RTree
        :param dimension: dimension to cut when bulk loading (start with value dimension in XMLRtree and order of XML
                                                              query in SQLRtree)
        :return: root node of loaded RTree
        """

        def divide_into_slices(items, n_slices, n_slice_member):
            divided = []
            for i in range(n_slices):
                if i != n_slices - 1:
                    divided.append([item for item in items[n_slice_member * i:n_slice_member * (i + 1)]])
                else:
                    divided.append([item for item in items[n_slice_member * i:len(items)]])
            return divided

        def entries_to_leaf_nodes(some_entries: [Entry]):
            if len(some_entries) > self.max_n_children:
                raise ValueError(f"A leaf {some_entries} has more than {self.max_n_children} allwed children")
            node = node_type(self.max_n_children, entries=some_entries, name=self.object_name, is_leaf=True)
            return node

        def child_nodes_to_parent_node(child_nodes: [Node]):
            if len(child_nodes) > self.max_n_children:
                raise ValueError(f"Child nodes {child_nodes} has more than allowed {self.max_n_children} children")
            node = node_type(self.max_n_children, children=child_nodes, name=self.object_name)
            for child_node in child_nodes:
                child_node.parent = node
            return node

        # First partition entries into nodes
        # Number of slicing step = n_dimension - 1
        n_dimension = len(entries[0].coordinates)
        dimension_order = [dimension] + [d for d in range(n_dimension) if d != dimension]
        groups = [entries]
        for slice_step in range(n_dimension):
            updated_groups = []
            for group in groups:
                n_leaves = len(group) / self.max_n_children
                # quick_sort_entries(group, dimension_order[slice_step])
                group.sort(key=lambda entry: entry.coordinates[dimension_order[slice_step]])
                # dividing in to slice
                n_slices = ceil(n_leaves ** (1 / (n_dimension - slice_step)))
                slice_size = ceil(len(group) / n_slices)
                for slice in divide_into_slices(group, n_slices, slice_size):
                    updated_groups.append(slice)
            groups = updated_groups
        # Initialize each group of entry into nodes
        nodes = [[entries_to_leaf_nodes(group) for group in groups]]
        while len(nodes[0]) != 1:
            if len(nodes[0]) == 0:
                raise ValueError('Some weird shit happens: empty node_group')
            for slice_step in range(n_dimension):
                upper_layer_nodes = []
                sort_dimension = dimension_order[slice_step]
                for nodes_group in nodes:
                    n_upper_layer = len(nodes_group) / self.max_n_children
                    nodes_group.sort(key=lambda node: node.center_coord.coordinates[sort_dimension])
                    # quick_sort_nodes(nodes_group, dimension_order[slice_step])
                    n_slices = ceil(n_upper_layer ** (1/(n_dimension - slice_step)))
                    slice_size = ceil(len(nodes_group) / n_slices)
                    for slice in divide_into_slices(nodes_group, n_slices, slice_size):
                        upper_layer_nodes.append(slice)
                nodes = upper_layer_nodes
            nodes = [[child_nodes_to_parent_node(group_nodes) for group_nodes in nodes]]

        # nodes[0][0].print_node()
        self.root = nodes[0][0]

    def range_search(self, boundaries):
        if not self.root:
            return None
        return self.root.range_search(boundaries)

