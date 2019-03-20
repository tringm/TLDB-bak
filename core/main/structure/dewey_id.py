class DeweyID:
    def __init__(self, string_id=''):
        self._id = string_id
        self._components = [int(component) for component in self._id.split('.')]

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        self._id = value
        self._components = [int(component) for component in value.split('.')]

    @property
    def components(self):
        return self._components

    def __lt__(self, other):
        min_length = min(len(self.components), len(other.components))
        for i in range(min_length):
            if self.components[i] < other.components[i]:
                return True
            if self.components[i] > other.components[i]:
                return False
        if len(self.components) < len(other.components):
            return True
        return False

    def __eq__(self, other):
        if len(self.components) != len(other.components):
            return False
        for i in range(len(self.components)):
            if self.components[i] != other.components[i]:
                return False
        return True

    def __ne__(self, other):
        return not self.__eq__(other)

    def __le__(self, other):
        return self.__lt__(other) or self.__eq__(other)

    def __gt__(self, other):
        return not self.__le__(other)

    def __ge__(self, other):
        return not self.__lt__(other)

    def __str__(self):
        return self._id

    def __repr__(self):
        return self._id

    def is_ancestor(self, another_id) -> bool:
        """
        This function checks if this Dewey ID is an ancestor of another Dewey ID
        :param another_id: another DeweyID
        :return: True if id1 is an ancestor of id2
        """
        # id2 is shorter -> can't be descendant
        if len(self.components) >= len(another_id.components):
            return False
        # Compare element wise
        for i in range(len(self.components)):
            if self.components[i] != another_id.components[i]:
                return False
        return True

    def is_parent(self, another_id) -> bool:
        """
        This function checks if this Dewey ID is the parent of another Dewey ID
        :param another_id:
        :return: True if id1 is the parent of id2
        """
        if len(another_id.components) != (len(self.components) + 1):
            return False
        # Compare element wise
        for i in range(len(self.components)):
            if self.components[i] != another_id.components[i]:
                return False
        return True

    def relationship_satisfied(self, another_id, relationship: int) -> bool:
        """
        This function checks if this Dewey ID satisfy a given relationship requirements wrt another DeweyID
        relationship == 1 -> check if id1 is parent of id2
        relationship == 2 -> Check if id1 is ancestor of id2
        :param another_id:
        :param relationship:
        :return: True if relationship requirement satisfied
        """
        if relationship == 1:
            return self.is_parent(another_id)
        if relationship == 2:
            return self.is_ancestor(another_id)
