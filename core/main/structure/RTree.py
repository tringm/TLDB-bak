import logging
from math import ceil, log
import timeit
from queue import Queue

from core.main.lib.Entries import get_boundaries_from_entries
from core.main.lib.Nodes import get_boundaries_from_nodes
from core.main.structure.Entry import Entry
from .Node import *


class RTree(ABC):
    def __init__(self):
        self.root = None

    @abstractmethod
    def bulk_loading_from_entry(self,
                                load_method: str,
                                entries: [Entry],
                                tree_name: str,
                                max_n_children: int,
                                dimension: int):
        pass

    def stripe_bulk_loading(self, node_type, entries: [Entry], name: str, max_n_children: int, dimension: int):
        """Summary
        Bulk loading RTree by dividing the hyperplane into hyper-rectangle

        :param node_type: Either XMLNode or SQLNode
        :param entries: list of input entries
        :param name: name of the tree
        :param max_n_children: maximum number of children in a node of RTree
        :param dimension: dimension to cut when bulk loading (currently only value dimension - 1)
        :return: root node of loaded RTree
        """
        logger = logging.getLogger("RTree")

        if max_n_children == 1:
            raise ValueError('Maximum number of children nodes must be > 1')

        logger.debug("Start bulk loading")
        logger.debug('%s %s', "Tree name:", name)

        start_sorting = timeit.default_timer()
        # sort entries based on value
        entries.sort(key=lambda entry: entry.coordinates[dimension])
        # quick_sort_entries(entries, dimension)
        end_sorting = timeit.default_timer()
        logger.debug('%s %d', 'sorting took:', end_sorting - start_sorting)

        n_entries = len(entries)

        # Configuration
        queue_node = queue.Queue()  # type: Queue[Node] # Queue for node at each level
        queue_range = queue.Queue()  # Queue for range of entries contained in a node at each level

        # Initialization
        # Create root node
        root = node_type(max_n_children, name=name, dimension=dimension)
        root.boundary = get_boundaries_from_entries(entries)

        queue_node.put(root)
        queue_range.put([0, n_entries])

        while not queue_node.empty():
            current_node = queue_node.get()
            current_range = queue_range.get()
            current_n_entries = current_range[1] - current_range[0]  # Number of entries contained in this current node
            # Calculate the height of this subtree based on max_n_children
            height = ceil(round(log(current_n_entries, max_n_children), 5))
            logger.debug('%s %s', 'current_range:', ','.join(str(number) for number in current_range))
            logger.debug('%s %d', 'current_n_entries:', current_n_entries)
            logger.debug('%s %d', 'height:', height)

            # if current node contains has n_entries <= max_n_children then this is a leaf and proceed to add entries
            if current_n_entries <= max_n_children:
                current_node.isLeaf = True
                logger.debug("Found leaf => add entries")
                adding_entries = entries[current_range[0]:current_range[1]]
                logger.debug('%s %d', "len(adding_entries):", len(adding_entries))
                for i in range(len(adding_entries)):
                    current_node.add_entry(adding_entries[i])

            else:
                logger.debug('Not a leaf => add new nodes')
                # Number of entries contained in the subtree of this node
                n_entries_subtree = max_n_children ** (height - 1)
                # n_slices = math.floor(math.sqrt(math.ceil(current_n_entries / n_entries_subtree)))
                #  Number of slices according to the formula of OMT
                n_slices = ceil(current_n_entries / n_entries_subtree)

                # if n_entries_subtree == current_n_entries:
                # 	n_slices = 0

                logger.debug('%s %d', 'n_entries_subtree', n_entries_subtree)
                logger.debug('%s %d', 'n_slices', n_slices)

                # divide into n_slice + 1 nodes, add to current node
                for i in range(n_slices):
                    # n_entries_slice = current_n_entries
                    range_low = current_range[0] + i * n_entries_subtree
                    range_high = range_low + n_entries_subtree

                    # last group might have more than max_n_children
                    if i == n_slices - 1:
                        range_high = current_range[1]

                    logger.debug('%s %d %s %d %d', "Child node index:", i, "range", range_low, range_high)

                    subtree_node = node_type(max_n_children, parent=current_node, dimension=dimension, name=name)
                    subtree_node.boundary = get_boundaries_from_entries(entries[range_low:range_high])
                    logger.debug('%s %s', "Child node", str(subtree_node))
                    current_node.add_child_node(subtree_node)
                    queue_node.put(subtree_node)
                    queue_range.put([range_low, range_high])
        self.root = root

    def str_bulk_loading(self, node_type, entries: [Entry], tree_name: str, max_n_children: int, dimension: int):
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
        logger = logging.getLogger("RTree")
        logger.debug("Start STR bulk loading")
        logger.debug('%s %s', "Tree name:", tree_name)

        def divide_into_slices(items, n_slices, n_slice_member):
            divided = []
            for i in range(n_slices):
                if i != n_slices - 1:
                    divided.append([item for item in items[n_slice_member * i:n_slice_member * (i + 1)]])
                else:
                    divided.append([item for item in items[n_slice_member * i:len(items)]])
            return divided

        def entries_to_leaf_nodes(some_entries: [Entry]):
            if len(some_entries) > max_n_children:
                raise ValueError('Slicing error: A leaf ' + str(some_entries) + 'has more than ' + str(max_n_children))
            node = node_type(max_n_children, entries=some_entries, name=tree_name, dimension=dimension)
            node.boundary = get_boundaries_from_entries(some_entries)
            node.isLeaf = True
            return node

        def child_nodes_to_parent_node(child_nodes: [Node]):
            if len(child_nodes) > max_n_children:
                raise ValueError('Slicing error: ' + str(child_nodes) + 'children node ' + str(max_n_children))
            node = node_type(max_n_children, children=child_nodes, name=tree_name, dimension=dimension)
            node.boundary = get_boundaries_from_nodes(child_nodes)
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
                n_leaves = len(group) / max_n_children
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
                for nodes_group in nodes:
                    n_upper_layer = len(nodes_group) / max_n_children
                    nodes_group.sort(key=lambda node: node.get_center_coord()[dimension_order[slice_step]])
                    # quick_sort_nodes(nodes_group, dimension_order[slice_step])
                    n_slices = ceil(n_upper_layer ** (1/(n_dimension - slice_step)))
                    slice_size = ceil(len(nodes_group) / n_slices)
                    for slice in divide_into_slices(nodes_group, n_slices, slice_size):
                        upper_layer_nodes.append(slice)
                nodes = upper_layer_nodes
            nodes = [[child_nodes_to_parent_node(group_nodes) for group_nodes in nodes]]

        # nodes[0][0].print_node()
        self.root = nodes[0][0]


class XMLRTree(RTree):
    def __init__(self):
        super().__init__()

    def bulk_loading_from_entry(self,
                                load_method: str,
                                entries: [Entry],
                                tree_name: str,
                                max_n_children: int,
                                dimension=1):
        methods = {'stripe': self.stripe_bulk_loading, 'str': self.str_bulk_loading}
        methods[load_method](XMLNode, entries, tree_name, max_n_children, dimension)


class SQLRTree(RTree):
    def __init__(self):
        super().__init__()

    def bulk_loading_from_entry(self,
                                load_method: str,
                                entries: [Entry],
                                tree_name: str,
                                max_n_children: int,
                                dimension=1):
        methods = {'stripe': self.stripe_bulk_loading, 'str': self.str_bulk_loading}
        methods[load_method](SQLNode, entries, tree_name, max_n_children, dimension)
