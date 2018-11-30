"""
This module contains helper function for list of Entry [Entry]
    quick_sort_entries ([Entry], dimension): quickSort a list of entries by value of given Entry's dimension
    get_boundaries_from_entries ([Entry]): Return MBR of list of entries
"""

from src.structure.Entry import Entry


def quick_sort_entries(entries: [Entry], dimension: int) -> [Entry]:
    """Summary
    Quick sort list of entries according to value of a dimension

    :param entries: list of input entries
    :param dimension: dimension of the entries to be sorted
    :return: sorted entries
    """

    def partition(entries: [Entry], low: int, high: int, dimension: int) -> int:
        pivot = entries[low]
        i = low + 1
        j = high
        while 1:
            while i <= j and entries[i].coordinates[dimension] <= pivot.coordinates[dimension]:
                i += 1
            while j >= i and entries[j].coordinates[dimension] >= pivot.coordinates[dimension]:
                j -= 1
            if j <= i:
                break
            entries[i], entries[j] = entries[j], entries[i]

        entries[low], entries[j] = entries[j], entries[low]
        return j

    temp_stack = []
    low = 0
    high = len(entries) - 1
    temp_stack.append((low, high))
    while temp_stack:
        pos = temp_stack.pop()
        low, high = pos[0], pos[1]
        partition_index = partition(entries, low, high, dimension)
        if partition_index - 1 > low:
            temp_stack.append((low, partition_index - 1))
        if partition_index + 1 < high:
            temp_stack.append((partition_index + 1, high))

    return entries


def get_boundaries_from_entries(entries: [Entry]) -> [[int]]:
    """Summary
    Find the MBR of list of Entries

    :param entries: list of input entries
    :return: list of size Entry.dimension contains boundaries of each dimension
    """
    boundary = []

    # init boundaries
    first_entry_coordinates = entries[0].coordinates.copy()
    n_dimensions = entries[0].n_dimensions

    for dimension in range(n_dimensions):
        boundary.append([first_entry_coordinates[dimension], first_entry_coordinates[dimension]])

    for entries_index in range(1, len(entries)):
        entry_coordinates = entries[entries_index].coordinates
        for dimension in range(n_dimensions):
            boundary[dimension][0] = min(boundary[dimension][0], entry_coordinates[dimension])
            boundary[dimension][1] = max(boundary[dimension][1], entry_coordinates[dimension])

    return boundary
