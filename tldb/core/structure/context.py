from typing import List


class Context:
    def __init__(self, attributes):
        self.attributes = attributes


class RangeContext(Context):
    """
    TODO Text range(?) Range should be its own class
    """
    def __init__(self, attributes: List[str], boundaries: List[List[float]], nodes=None):
        """
        RangeContext contains
        :param attributes: name of the attributes to be search
        :param boundaries: range boundary as require
        :param nodes: Checking node
        """
        context_len = len(attributes)
        if context_len != len(boundaries):
            raise ValueError("Attributes length %d is different from boundaries length %d" %
                             (context_len, len(boundaries)))
        super().__init__(attributes)
        self.boundaries = dict(zip(self.attributes, boundaries))

        if not nodes:
            self.nodes = dict(zip(self.attributes, [None] * context_len))
        else:
            self.nodes = dict(zip(self.attributes, nodes))

    def __str__(self):
        rep_string = ''
        for attr in self.attributes:
            rep_string += 'Attribute: ' + attr + ':\n'
            rep_string += '\tBoundaries: ' + str(self.boundaries[attr]) + '\n'
            rep_string += '\tNodes: ' + str(self.nodes[attr]) + '\n'
        return rep_string

    def __repr__(self):
        rep = dict.fromkeys(self.attributes)
        for attr in rep:
            rep[attr] = 'Boundaries:' + str(self.boundaries[attr][0]) + '...' + \
                        ' Nodes:' + str(self.nodes[attr][0]) + '...'
        return str(rep)

    def __copy__(self):
        copy_attributes = self.attributes[:]
        copy_boundaries = [self.boundaries[attr][:] for attr in copy_attributes]
        if not self.nodes:
            copy_nodes = [self.nodes[attr][:] for attr in copy_attributes]
        else:
            copy_nodes = self.nodes
        return RangeContext(copy_attributes, copy_boundaries, copy_nodes)

    def __eq__(self, other):
        if len(self.attributes) != len(other.attributes):
            return False
        for idx, attr in enumerate(self.attributes):
            if attr != other.attributes[idx]:
                return False
            if self.boundaries[attr] != other.boundaries[attr]:
                return False
            if self.nodes[attr] != other.nodes[attr]:
                return False
        return True

    def check_intersection_and_update_boundary(self, attr: str, boundary):
        """
        Check if context boundary at element attr intersect with boundary
            - Not intersect -> Return False
            - Intersect -> Update boundary and return True
        :param attr: An attribute name
        :param boundary:
        :return:
        """
        self_b = self.boundaries[attr]
        if self_b is None or boundary is None:
            raise ValueError('either context boundary %s or given boundary %s is None' % (str(self_b), str(boundary)))
        if (self_b[1] < boundary[0]) or (self_b[0] > boundary[1]):
            return False
        self.boundaries[attr] = [max(self_b[0], boundary[0]), min(self_b[1], boundary[1])]
        return True
