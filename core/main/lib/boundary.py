# Boundary is a list contains each item is a list of 2 storing min, max


def update_boundary_from_entry(boundary: [[]], entry):
    """
    Return updated boundaries when adding entry
    :param boundary:
    :param entry:
    :return:
    """
    if len(boundary) != len(entry.coordinates):
        raise ValueError(str(boundary) + ' not match dimension with ' + str(entry))
    update_boundary = boundary.copy()

    for dimension in range(len(boundary)):
        update_boundary[dimension][0] = min(boundary[dimension][0], entry.coordinates[dimension])
        update_boundary[dimension][1] = max(boundary[dimension][1], entry.coordinates[dimension])

    return update_boundary


def update_boundary_from_node(boundary, node):
    """
    Return updated boundaries when adding node
    :param boundary:
    :param node:
    :return:
    """
    if len(boundary) != len(node.boundary):
        raise ValueError(str(boundary) + ' not match dimension with ' + str(node))
    update_boundary = boundary.copy()

    for dimension in range(len(boundary)):
        update_boundary[dimension][0] = min(boundary[dimension][0], node.boundary[dimension][0])
        update_boundary[dimension][1] = max(boundary[dimension][1], node.boundary[dimension][1])

    return update_boundary


def compare_value_boundaries(boundary1: [], boundary2: []) -> float:
    """
    This function compare 2 boundaries and return case number
    If boundary1 does not intersect with boundaries 2:
        If boundary1 is on the left side of boundary2 -> 1.1
        If boundary1 is on the right side of boundary2 -> 1.2
    If boundary1 intersect with boundaries 2:
        If boundary1 is on the left side of boundary2 -> 2.1
        If boundary1 is on the right side of boundary2 -> 2.2
    :param boundary1:
    :param boundary2:
    :return: case number
    """
    # Does not intersect, on the left side
    if boundary1[1] < boundary2[0]:
        return 1.1
    # Does not intersect, on the right side
    elif boundary1[0] > boundary2[1]:
        return 1.2
    # Else: Intersect
    # On the left side
    elif boundary1[1] <= boundary2[1]:
        return 2.1
    # On the right side
    else:
        return 2.2
    return 0


# def value_boundary_has_intersection(boundary1: [], boundary2: []) -> bool:
#     """
#     This function check if these 2 boundaries intersect
#     :param boundary1: list of size 2
#     :param boundary2: list of size 2
#     :return: True if they intersect
#     """
#     if (boundary1[1] < boundary2[0]) or (boundary1[0] > boundary2[1]):
#         return False
#     return True


def value_boundary_intersection(boundary1: [], boundary2: []) -> []:
    """
    This function found the intersection between 2 boundaries of number
    :param boundary1: list of size 2
    :param boundary2: list of size 2
    :return: None if does not intersect else intersection of the 2 boundaries
    """
    if boundary1 is None or boundary2 is None:
        print(haha)
    if (boundary1[1] < boundary2[0]) or (boundary1[0] > boundary2[1]):
        return None
    return [max(boundary1[0], boundary2[0]), min(boundary1[1], boundary2[1])]


def value_boundary_union(boundary1: [], boundary2: []) -> []:
    """
    This function found the union of 2 boundaries of number
    :param boundary1: list of size 2
    :param boundary2: list of size 2
    :return: union of the 2 boundaries
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
    This function check if index boundaries 1 can be ancestor of index boudnary 2
    :param boundary1: index boundaries
    :param boundary2: index boundaries
    :return: True if boundaries 1 can be ancestor of boundaries 2
    """
    if boundary2[1] <= boundary1[0]:
        return False

    if boundary2[0] > boundary1[1]:
        if not boundary1[1].is_ancestor(boundary2[0]):
            return False

    return True
