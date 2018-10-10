# This file contains helper functions for DeweyID (as str)


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