# This file contains helper functions for DeweyID (as str)


def is_ancestor(id1: str, id2: str) -> bool:
    """
    This function check if a Dewey ID is an ancestor of another Dewey ID
    :param id1:
    :param id2:
    :return: True if id1 is an ancestor of id2
    """
    # id2 is shorter -> can't be descendant
    if len(id2) <= len(id1):
        return False
    id1 = id1.split('.')
    id2 = id2.split('.')
    # Compare element wise
    for i in range(len(id1)):
        if id1[i] != id2[i]:
            return False
    return True


def is_parent(id1: str, id2: str) -> bool:
    """
    This function check if a Dewey ID is the parent of another Dewey ID
    :param id1:
    :param id2:
    :return: True if id1 is the parent of id2
    """
    if len(id2) != (len(id1) + 1):
        return False
    id1 = id1.split('.')
    id2 = id2.split('.')
    # Compare element wise
    for i in range(len(id1)):
        if id1[i] != id2[i]:
            return False
    return True
