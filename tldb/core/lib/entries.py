"""
This module contains helper function for list of Entry [Entry]
    quick_sort_entries ([Entry], dimension): quickSort a list of entries by value of given Entry's dimension
    entries_to_boundary ([Entry]): Return MBR of list of entries
"""
from tldb.core.structure.boundary import Boundary
from tldb.core.structure.entry import Entry
from typing import List, Set

from tldb.core.structure.interval import Interval


def quick_sort_entries(entries: List[Entry], dimension: int) -> List[Entry]:
    """Summary
    Quick sort list of entries according to value of a dimension

    :param entries: list of input entries
    :param dimension: dimension of the entries to be sorted
    :return: sorted entries
    """

    def partition(l: int, h: int, d: int) -> int:
        pivot = entries[l]
        i = l + 1
        j = h
        while 1:
            while i <= j and entries[i].coordinates[d] <= pivot.coordinates[d]:
                i += 1
            while j >= i and entries[j].coordinates[d] >= pivot.coordinates[d]:
                j -= 1
            if j <= i:
                break
            entries[i], entries[j] = entries[j], entries[i]

        entries[l], entries[j] = entries[j], entries[l]
        return j

    temp_stack = []
    low = 0
    high = len(entries) - 1
    temp_stack.append((low, high))
    while temp_stack:
        pos = temp_stack.pop()
        low, high = pos[0], pos[1]
        partition_index = partition(low, high, dimension)
        if partition_index - 1 > low:
            temp_stack.append((low, partition_index - 1))
        if partition_index + 1 < high:
            temp_stack.append((partition_index + 1, high))

    return entries


def entries_to_boundary(entries: Set[Entry]) -> [[int]]:
    """Summary
    Find the MBR of list of Entries

    :param entries: list of input entries
    :return: list of size Entry.dimension contains boundaries of each dimension
    """
    # init boundaries

    if not entries:
        raise ValueError("entries must not be empty")

    first_entry = entries.pop()
    entries.add(first_entry)
    first_entry_coors = first_entry.coordinates
    n_dimensions = first_entry.n_dimension

    boundary = Boundary([Interval((first_entry_coors[d], first_entry_coors[d])) for d in range(n_dimensions)])

    for e in entries:
        e_coors = e.coordinates
        intervals = boundary.intervals
        boundary.intervals = [intervals[d].join_tuple((e_coors[d], e_coors[d])) for d in range(n_dimensions)]
    return boundary

