# This file contains helper functions for DeweyID (as str)
from math import floor


def is_ancestor(id1: str, id2: str) -> bool:
    """
    This function checks if a Dewey ID is an ancestor of another Dewey ID
    :param id1:
    :param id2:
    :return: True if id1 is an ancestor of id2
    """
    id1 = id1.split('.')
    id2 = id2.split('.')
    # id2 is shorter -> can't be descendant
    if len(id2) <= len(id1):
        return False
    # Compare element wise
    for i in range(len(id1)):
        if id1[i] != id2[i]:
            return False
    return True


def is_parent(id1: str, id2: str) -> bool:
    """
    This function checks if a Dewey ID is the parent of another Dewey ID
    :param id1:
    :param id2:
    :return: True if id1 is the parent of id2
    """
    id1 = id1.split('.')
    id2 = id2.split('.')
    if len(id2) != (len(id1) + 1):
        return False
    # Compare element wise
    for i in range(len(id1)):
        if id1[i] != id2[i]:
            return False
    return True


def relationship_satisfied(id1: str, id2: str, relationship: int) -> bool:
    """
    This function checks if 2 Dewey ID satisfy a given relationship requirements
    relationship == 1 -> check if id1 is parent of id2
    relationship == 2 -> Check if id1 is ancestor of id2
    :param id1:
    :param id2:
    :param relationship:
    :return: True if relationship requirement satisfied
    """
    if relationship == 1:
        return is_parent(id1, id2)
    if relationship == 2:
        return is_ancestor(id1, id2)


def get_center_index(id1: str, id2: str) -> str:
    """
    This function returns the "medium" of 2 Dewey ID index
    For example, medium of '1.2.1' and '1.1.2' is '1.1'6'
    Assuming that id2 is always "larger" than id1
    :param id1:
    :param id2:
    :return:
    """
    # TODO: handling 1.30.19 and 1.42.1.3 -> 1.35.9 case

    id1 = id1.split('.')
    id2 = id2.split('.')
    min_length = min(len(id1), len(id2))
    mean_index = []
    remember = False
    for i in range(min_length):
        mean_diff = (int(id2[i]) - int(id1[i]))/2
        if mean_diff < 0 and not remember:
            if len(mean_index) == 0 or (mean_index[len(mean_index) - 1] == 1):
                raise ValueError('Max index ' + '.'.join(id2) + 'smaller than min index ' + '.'.join(id1))
            else:
                mean_index[len(mean_index) - 1] -= 1
        if remember:
            mean_diff = mean_diff + 5
        if (mean_diff * 2) % 2 != 0:
            mean_diff = floor(mean_diff)
            remember = True
        mean_index.append(int(int(id1[i]) + mean_diff))
    if remember:
        mean_index.append(5)
    return '.'.join(str(num) for num in mean_index)
