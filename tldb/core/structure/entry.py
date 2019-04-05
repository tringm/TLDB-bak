from typing import Tuple


class Entry:
    """Summary
    Entry

    Attributes:
        coordinates [id, value]:  list contains index and value for RTree_XML Node
        coordinates [v1, v2, etc.]: list contains value for RTree_SQL Node
        link_XML (dict): a dict contains a list of entries contained in linked elements
        link_SQL (dict): a dict contains a list of entries contained in linked tables
    """

    def __init__(self, coordinates: Tuple):
        if not isinstance(coordinates, tuple):
            raise ValueError("coordinates must be a tuple of coordinate")
        self._coordinates = coordinates
        self._n_dimensions = len(coordinates)
        # self.link_XML = {}
        # self.link_SQL = {}
        # self.matching_value_entry = (None, None)

    def __str__(self):
        return str(self.coordinates)

    def __repr__(self):
        return str(self)

    def __hash__(self):
        return hash(self.coordinates)

    def __eq__(self, other):
        self_coors = self.coordinates
        other_coors = other.coordinates
        if len(self_coors) != len(other_coors):
            return False
        return self_coors == other_coors

    @property
    def coordinates(self):
        return self._coordinates

    @coordinates.setter
    def coordinates(self, new_coors):
        self._coordinates = new_coors
        self._n_dimensions = len(new_coors)

    @property
    def n_dimension(self):
        return self._n_dimensions

    def is_inside(self, boundary: [int]) -> bool:
        """Summary
        Check if this entry is inside a boundaries

        :param boundary: list of size dimension containing boundaries in each dimension
        :return: true if is inside
        """

        if self.n_dimensions != len(boundary):
            raise ValueError('Entry and boundaries has different number of dimensions')

        is_inside = True
        # Checking each dimension
        for dimension in range(self.n_dimensions):
            lower_bound = boundary[dimension][0]
            upper_bound = boundary[dimension][1]
            if (self.coordinates[dimension] < lower_bound) or (self.coordinates > upper_bound):
                is_inside = False
                break

        return is_inside

    def is_similar(self, entry) -> bool:
        """Summary
        Compare this Entry coordinate with another Entry coordinate
        :param entry: comparing Entry
        :return: true if coo
        """

