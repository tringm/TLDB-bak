# This file contains helper functions for DeweyID
from math import floor

from tldb.core.main.structure.dewey_id import DeweyID


def get_center_index(id1: DeweyID, id2: DeweyID) -> str:
    """
    This function returns the "medium" of 2 Dewey ID index
    For example:
        medium of '1.2.1' and '1.1.2' is '1.1'6'
        medium of '1.42.1.3' and '1.30.19' is '1.35.9'
    :param id1:
    :param id2:
    :return:
    """
    if id1 > id2:
        id1, id2 = id2, id1

    min_length = min(len(id1.components), len(id2.components))
    mean_index = []
    remember = False
    for i in range(min_length):
        mean_diff = (id2.components[i] - id1.components[i])/2
        if mean_diff < 0 and not remember:
            mean_index[len(mean_index) - 1] -= 1
        if remember:
            mean_diff = mean_diff + 5
        if (mean_diff * 2) % 2 != 0:
            mean_diff = floor(mean_diff)
            remember = True
        mean_index.append(int(id1.components[i] + mean_diff))
    if remember:
        mean_index.append(5)
    return '.'.join(str(num) for num in mean_index)


def generate_dewey_id_from_dict(dictionary, dist=2, root_id='1'):
    elements = []

    def to_value(obj):
        if not obj or isinstance(obj, dict) or isinstance(obj, list):
            return None
        else:
            return obj

    def assign_label(obj, parent_id):
        if not obj:
            return
        last_division = dist + 1
        if isinstance(obj, dict):
            for k, v in obj.items():
                dewey_id = parent_id + '.' + str(last_division)
                elements.append((dewey_id, k, to_value(v)))
                last_division += dist
                assign_label(v, dewey_id)
        if isinstance(obj, list):
            for v in obj:
                dewey_id = parent_id + '.' + str(last_division)
                last_division += dist
                assign_label(v, dewey_id)

    assign_label(dictionary, root_id)

    return elements
