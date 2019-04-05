class DeweyID:
    def __init__(self, id):
        """
        Load a DeweyID, either from string or list
        >>> DeweyID('1.2.3')
        '1.2.3'
        >>> DeweyID(division=(1, 2, 3))
        '1.2.3'
        :param kwargs:
        """
        if isinstance(id, str):
            self._id = id
            self._divisions = tuple([int(div) for div in self._id.split('.')])
        else:
            try:
                self._divisions = tuple(id)
                self._id = '.'.join(map(str, self._divisions))
            except TypeError:
                raise TypeError('Dewey must be of string or divisions')
        self._n_division = len(self._divisions)

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        self._id = value
        self._divisions = tuple([int(div) for div in value.split('.')])
        self._n_division = len(self._divisions)

    @property
    def divisions(self):
        return self._divisions

    @property
    def n_division(self):
        return self._n_division

    def __lt__(self, other):
        min_length = min(self.n_division, other.n_division)
        for i in range(min_length):
            if self.divisions[i] < other.divisions[i]:
                return True
            if self.divisions[i] > other.divisions[i]:
                return False
        if len(self.divisions) < len(other.divisions):
            return True
        return False

    def __eq__(self, other):
        if len(self.divisions) != len(other.divisions):
            return False
        return self.divisions == other.divisions

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

    def __hash__(self):
        return hash(self.divisions)

    def is_ancestor(self, another_id) -> bool:
        """
        This function checks if this Dewey ID is an ancestor of another Dewey ID
        :param another_id: another DeweyID
        :return: True if id1 is an ancestor of id2
        """
        # id2 is shorter -> can't be descendant
        if self.n_division >= another_id.n_division:
            return False
        # Compare element wise
        for i in range(len(self.divisions)):
            if self.divisions[i] != another_id.divisions[i]:
                return False
        return True

    def is_parent(self, another_id) -> bool:
        """
        This function checks if this Dewey ID is the parent of another Dewey ID
        :param another_id:
        :return: True if id1 is the parent of id2
        """
        if (self.n_division + 1) != another_id.n_division:
            return False
        # Compare element wise
        for i in range(len(self.divisions)):
            if self.divisions[i] != another_id.divisions[i]:
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

    def plus_one_last_divison(self):
        new_divisions = list(self.divisions)
        new_divisions[-1] += 1
        return DeweyID(new_divisions)

