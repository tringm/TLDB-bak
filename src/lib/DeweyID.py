# This file contains helper functions for DeweyID
from math import floor
from src.structure.DeweyID import DeweyID


def get_center_index(id1: DeweyID, id2: DeweyID) -> str:
    """
    This function returns the "medium" of 2 Dewey ID index
    For example, medium of '1.2.1' and '1.1.2' is '1.1'6'
    Assuming that id2 is always "larger" than id1
    :param id1:
    :param id2:
    :return:
    """
    # TODO: handling 1.30.19 and 1.42.1.3 -> 1.35.9 case

    min_length = min(len(id1.components), len(id2.components))
    mean_index = []
    remember = False
    for i in range(min_length):
        mean_diff = (id2.components[i] - id1.components[i])/2
        if mean_diff < 0 and not remember:
            if len(mean_index) == 0 or (mean_index[i - 1] == id1.components[i - 1]):
                raise ValueError('Max index ' + str(id1) + 'smaller than min index ' + str(id2))
            else:
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
