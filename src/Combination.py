from typing import List


class Combination:
    def __init__(self, elements: List[str]):
        self.index = dict((element, None) for element in elements)
        self.value = dict((element, None) for element in elements)

    def __str__(self):
        return str(self.index) + str(self.value)

    def match_value_with(self, comparing_combination) -> bool:
        """
        This function checks if this combination is a match with other combination
        :param comparing_combination: Combination
        :return: True if is a match
        """
        if not set(self.value.keys()) == set(comparing_combination.value.keys()):
            return False
        for element in self.value:
            if (self.value[element] is not None) and (comparing_combination.value[element] is not None):
                if self.value[element] != comparing_combination.value[element]:
                    return False
        return True

    def combine_value_with(self, matching_combination):
        """
        This function returns the combined combination of 2 combination provided that they are a match
        :param matching_combination: Combination
        :return: combined combination
        """
        combined_combination = Combination(list(self.value.keys()))
        for element in self.value:
            if self.value[element] is not None:
                combined_combination.value[element] = self.value[element]
            else:
                combined_combination.value[element] = matching_combination.value[element]
        return combined_combination

    def match_with(self, comparing_combination) -> bool:
        """
        This function checks if this combination is a match with other combination (both index and value)
        :param comparing_combination: Combination
        :return: True if is a match
        """
        if not set(self.value.keys()) == set(comparing_combination.value.keys()):
            return False
        if not set(self.index.keys()) == set(comparing_combination.index.keys()):
            return False
        for element in self.value:
            if (self.value[element] is not None) and (comparing_combination.value[element] is not None):
                if self.value[element] != comparing_combination.value[element]:
                    return False
        for element in self.value:
            if (self.index[element] is not None) and (comparing_combination.index[element] is not None):
                if self.index[element] != comparing_combination.index[element]:
                    return False
        return True

    def combine_with(self, matching_combination):
        """
        This function returns the combined index and value combination of 2 combination provided that they are a match
        :param matching_combination: Combination
        :return: combined combination
        """
        combined_combination = Combination(list(self.value.keys()))
        for element in self.value:
            if self.value[element] is not None:
                combined_combination.value[element] = self.value[element]
            else:
                combined_combination.value[element] = matching_combination.value[element]

            if self.index[element] is not None:
                combined_combination.index[element] = self.index[element]
            else:
                combined_combination.index[element] = matching_combination.index[element]
        return combined_combination

