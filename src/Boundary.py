from .DeweyID import is_ancestor


def value_boundary_has_intersection(boundary1: [], boundary2: []) -> bool:
    """
    This function check if these 2 boundary intersect
    :param boundary1: list of size 2
    :param boundary2: list of size 2
    :return: True if they intersect
    """
    if (boundary1[1] < boundary2[0]) or (boundary1[0] > boundary2[1]):
        return False
    return True


def value_boundary_intersection(boundary1: [], boundary2: []) -> []:
    """
    This function found the intersection between 2 boundary of number
    :param boundary1: list of size 2
    :param boundary2: list of size 2
    :return: intersection of the 2 boundary
    """
    return [max(boundary1[0], boundary2[0]), min(boundary1[1], boundary2[1])]


def value_boundary_union(boundary1: [], boundary2: []) -> []:
    """
    This function found the union of 2 boundary of number
    :param boundary1: list of size 2
    :param boundary2: list of size 2
    :return: union of the 2 boundary
    """
    return [min(boundary1[0], boundary2[0]), max(boundary1[1], boundary2[1])]


def value_boundaries_union(boundaries: [[]]) -> []:
    """
    This function found the union of multiple boundaries
    :param boundaries: list of multiple boundaries
    :return: union of all the boundaries
    """
    firsts = [boundary[0] for boundary in boundaries]
    lasts = [boundary[1] for boundary in boundaries]
    return [min(firsts), max(lasts)]


def index_boundary_can_be_ancestor(boundary1: [], boundary2: []) -> bool:
    """
    This function check if index boundary 1 can be ancestor of index boudnary 2
    :param boundary1: index boundary
    :param boundary2: index boundary
    :return: True if boundary 1 can be ancestor of boundary 2
    """
    if boundary2[1] < boundary1[0]:
        return False

    if boundary2[0] > boundary1[1]:
        if not is_ancestor(boundary1[1], boundary2[0]):
            return False

    return True
