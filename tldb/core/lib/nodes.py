import copy
from typing import Set

from tldb.core.lib.interval import union_two_intervals
from tldb.core.structure.boundary import Boundary

"""
This module contains helper function for list of Entry [Entry]
    quick_sort_node ([Entry], dimension): quickSort a list of nodes by center point of a given dimension
"""


def quick_sort_nodes(nodes, dimension: int):
    """Summary
    Quick sort list of entries according to value of a dimension

    :param nodes: list of input nodes
    :param dimension: dimension of the entries to be sorted
    :return: sorted entries
    """

    def partition(l: int, h: int) -> int:
        pivot = nodes[l]
        i = l + 1
        j = h
        while 1:
            while i <= j and nodes[i].get_center_coord()[dimension] <= pivot.get_center_coord()[dimension]:
                i += 1
            while j >= i and nodes[j].get_center_coord()[dimension] >= pivot.get_center_coord()[dimension]:
                j -= 1
            if j <= i:
                break
            nodes[i], nodes[j] = nodes[j], nodes[i]

        nodes[l], nodes[j] = nodes[j], nodes[l]
        return j

    temp_stack = []
    low = 0
    high = len(nodes) - 1
    temp_stack.append((low, high))
    while temp_stack:
        pos = temp_stack.pop()
        low, high = pos[0], pos[1]
        partition_index = partition(low, high)
        if partition_index - 1 > low:
            temp_stack.append((low, partition_index - 1))
        if partition_index + 1 < high:
            temp_stack.append((partition_index + 1, high))

    return nodes


def nodes_to_boundary(nodes: Set) -> Boundary:
    """Summary
    Find the MBR of list of Nodes

    :param nodes:
    :return:
    """
    # init boundaries
    if not nodes:
        raise ValueError("nodes is empty")
    first_node = nodes.pop()
    nodes.add(first_node)
    boundary: Boundary = copy.copy(first_node.boundary)
    n_dimension = boundary.n_dimension

    for node in nodes:
        node_intervals = node.boundary.intervals
        intervals = boundary.intervals
        boundary.intervals = [union_two_intervals(intervals[d], node_intervals[d]) for d in range(n_dimension)]
    return boundary


def nodes_range_search(nodes, boundary):
    results = set()
    for node in nodes:
        nodes_in_range = node.range_search(boundary)
        if nodes_in_range is not None:
            results = results.union(set(nodes_in_range))
    return results
