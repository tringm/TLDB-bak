class Entry:
    """Summary
    Entry

    Attributes:
        coordinates [id, value]:  list contains index and value for RTree_XML Node
        coordinates [v1, v2, etc.]: list contains value for RTree_SQL Node
        link_XML (dict): a dict contains a list of entries contained in linked elements
        link_SQL (dict): a dict contains a list of entries contained in linked tables
    """

    def __init__(self, coordinates: [int]):
        self.coordinates = coordinates
        self.n_dimensions = len(coordinates)
        self.link_XML = {}
        self.link_SQL = {}
        self.matching_entries = {}

    def __str__(self):
        return str(self.coordinates)

    def is_inside(self, boundary: [int]) -> bool:
        """Summary
        Check if this entry is inside a boundary

        :param boundary: list of size dimension containing boundary in each dimension
        :return: true if is inside
        """

        if self.n_dimensions != len(boundary):
            raise ValueError('Entry and boundary has different number of dimensions')

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
        if len(self.coordinates) != len(entry.coordinates):
            return False

        for i in range(len(self.coordinates)):
            if self.coordinates[i] != entry.coordinates[i]:
                return False
        return True
