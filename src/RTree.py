from abc import ABC, abstractmethod
from .Entry import Entry
from .Entries import quick_sort_entries, get_boundaries
from .Node import Node

import logging
import timeit
import queue
import math


class RTree(ABC):
    def __init__(self):
        self.root = None

    @staticmethod
    def bulk_loading(entries: [Entry], name: str, max_n_children: int, dimension: int):
        """Summary
        Bulk loading RTree based on Overlap Minimizing Top-down Bulk Loading Algorithm

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
        quick_sort_entries(entries, dimension)
        end_sorting = timeit.default_timer()
        logger.debug('%s %d', 'sorting took:', end_sorting - start_sorting)

        n_entries = len(entries)

        # Configuration
        queue_node = queue.Queue()  # Queue for node at each level
        queue_range = queue.Queue()  # Queue for range of entries contained in a node at each level

        # Initialization
        # Create root node
        root = Node(max_n_children)
        root.boundary = get_boundaries(entries)
        root.name = name
        root.dimension = dimension

        queue_node.put(root)
        queue_range.put([0, n_entries])

        while not queue_node.empty():
            current_node = queue_node.get()
            current_range = queue_range.get()
            current_n_entries = current_range[1] - current_range[0]  # Number of entries contained in this current node
            height = math.ceil(round(math.log(current_n_entries, max_n_children),
                                     5))  # Calculate the height of this subtree based on max_n_children
            logger.debug('%s %s', 'current_range:', ','.join(str(number) for number in current_range))
            logger.debug('%s %d', 'current_n_entries:', current_n_entries)
            logger.debug('%s %d', 'height:', height)

            # if current node contains has n_entries <= max_n_children then this is a leaf and proceed to add entries
            if current_n_entries <= max_n_children:
                logger.debug("Found leaf => add entries")
                adding_entries = entries[current_range[0]:current_range[1]]
                logger.debug('%s %d', "len(adding_entries):", len(adding_entries))
                for i in range(len(adding_entries)):
                    current_node.entries.append(adding_entries[i])

            else:
                logger.debug('Not a leaf => add new nodes')
                # Number of entries contained in the subtree of this node
                n_entries_subtree = max_n_children ** (height - 1)
                # n_slices = math.floor(math.sqrt(math.ceil(current_n_entries / n_entries_subtree)))
                #  Number of slices according to the formula of OMT
                n_slices = math.ceil(current_n_entries / n_entries_subtree)

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

                    # if range_high == range_low:
                    # 	break

                    subtree_node = Node(max_n_children)
                    subtree_node.parent = current_node
                    subtree_node.boundary = get_boundaries(entries[range_low:range_high])
                    subtree_node.dimension = dimension
                    subtree_node.name = name
                    logger.debug('%s %s', "Child node", str(subtree_node))
                    current_node.children.append(subtree_node)
                    queue_node.put(subtree_node)
                    queue_range.put([range_low, range_high])

        return root

    # @staticmethod
    # def str_dividing_node(self, nodes: [node]):




    def str_bulk_loading(self, entries: [Entry], name: str, max_n_children: int, dimension: [int]):
        """Summary
        Bulk loading RTree based on Overlap Minimizing Top-down Bulk Loading Algorithm

        :param entries: list of input entries
        :param name: name of the tree
        :param max_n_children: maximum number of children in a node of RTree
        :param dimension: dimension to cut when bulk loading (start with value dimension in XMLRtree and order of XML
                                                              query in SQLRtree)
        :return: root node of loaded RTree
        """

        logger = logging.getLogger("RTree")

        if max_n_children == 1:
            raise ValueError('Maximum number of children nodes must be > 1')

        logger.debug("Start STR bulk loading")
        logger.debug('%s %s', "Tree name:", name)

        start_sorting = timeit.default_timer()
        # sort entries based on value
        quick_sort_entries(entries, dimension)
        end_sorting = timeit.default_timer()
        logger.verbose('%s %d %s %d', 'sorting', len(entries), 'took:', end_sorting - start_sorting)

        n_entries = len(entries)


        divided_entries = []


        # Configuration
        queue_node = queue.Queue()  # Queue for node at each level
        queue_range = queue.Queue()  # Queue for range of entries contained in a node at each level

        # Initialization
        # Create root node
        root = Node(max_n_children)
        root.boundary = get_boundaries(entries)
        root.name = name
        root.dimension = dimension

        queue_node.put(root)
        queue_range.put([0, n_entries])

        while not queue_node.empty():
            current_node = queue_node.get()
            current_range = queue_range.get()
            current_n_entries = current_range[1] - current_range[0]  # Number of entries contained in this current node
            height = math.ceil(round(math.log(current_n_entries, max_n_children),
                                     5))  # Calculate the height of this subtree based on max_n_children
            logger.debug('%s %s', 'current_range:', ','.join(str(number) for number in current_range))
            logger.debug('%s %d', 'current_n_entries:', current_n_entries)
            logger.debug('%s %d', 'height:', height)

            # if current node contains has n_entries <= max_n_children then this is a leaf and proceed to add entries
            if current_n_entries <= max_n_children:
                logger.debug("Found leaf => add entries")
                adding_entries = entries[current_range[0]:current_range[1]]
                logger.debug('%s %d', "len(adding_entries):", len(adding_entries))
                for i in range(len(adding_entries)):
                    current_node.entries.append(adding_entries[i])

            else:
                logger.debug('Not a leaf => add new nodes')
                # Number of entries contained in the subtree of this node
                n_entries_subtree = max_n_children ** (height - 1)
                # n_slices = math.floor(math.sqrt(math.ceil(current_n_entries / n_entries_subtree)))
                #  Number of slices according to the formula of OMT
                n_slices = math.ceil(current_n_entries / n_entries_subtree)

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

                    # if range_high == range_low:
                    # 	break

                    subtree_node = Node(max_n_children)
                    subtree_node.parent = current_node
                    subtree_node.boundary = get_boundaries(entries[range_low:range_high])
                    subtree_node.dimension = dimension
                    subtree_node.name = name
                    logger.debug('%s %s', "Child node", str(subtree_node))
                    current_node.children.append(subtree_node)
                    queue_node.put(subtree_node)
                    queue_range.put([range_low, range_high])

        return root


class XMLRTree(RTree):
    def __init__(self):
        self.root = None

    def load(self, entries: [Entry], name: str, max_n_children: int):
        dimension = 1  # This is based on the current version that Entry of XMLRTree Node is: index, value
        self.root = super(XMLRTree, self).bulk_loading(entries, name, max_n_children, dimension)


class SQLRTree(RTree):
    def __init__(self):
        self.root = None

    def load(self, entries: [Entry], name: str, max_n_children: int, dimension: int):
        self.root = super(SQLRTree, self).bulk_loading(entries, name, max_n_children, dimension)