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

    def partition(nodes, low: int, high: int, dimension: int) -> int:
        pivot = nodes[low]
        i = low + 1
        j = high
        while 1:
            while i <= j and nodes[i].get_center_coord()[dimension] \
                    <= pivot.get_center_coord()[dimension]:
                i += 1
            while j >= i and \
                    nodes[j].get_center_coord()[dimension] \
                    >= pivot.get_center_coord()[dimension]:
                j -= 1
            if j <= i:
                break
            nodes[i], nodes[j] = nodes[j], nodes[i]

        nodes[low], nodes[j] = nodes[j], nodes[low]
        return j

    temp_stack = []
    low = 0
    high = len(nodes) - 1
    temp_stack.append((low, high))
    while temp_stack:
        pos = temp_stack.pop()
        low, high = pos[0], pos[1]
        partition_index = partition(nodes, low, high, dimension)
        if partition_index - 1 > low:
            temp_stack.append((low, partition_index - 1))
        if partition_index + 1 < high:
            temp_stack.append((partition_index + 1, high))

    return nodes


def get_boundaries_from_nodes(nodes) -> [[int]]:
    """Summary
    Find the MBR of list of Nodes

    :param nodes:
    :return:
    """
    # init boundary
    n_dimensions = len(nodes[0].boundary)
    boundary = nodes[0].boundary

    for idx in range(1, len(nodes)):
        node_boundary = nodes[idx].boundary
        for dimension in range(n_dimensions):
            boundary[dimension][0] = min(boundary[dimension][0], node_boundary[dimension][0])
            boundary[dimension][1] = max(boundary[dimension][1], node_boundary[dimension][1])

    return boundary
