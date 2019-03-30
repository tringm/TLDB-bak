from typing import List


class Context:
    def __init__(self, elements: List[str], boundaries=None, nodes=None):
        self.elements = elements
        if boundaries is None:
            self.boundaries = dict(zip(self.elements, [None] * len(self.elements)))
        else:
            self.boundaries = dict(zip(self.elements, boundaries))
        if nodes is None:
            self.nodes = dict(zip(self.elements, [None] * len(self.elements)))
        else:
            self.nodes = dict(zip(self.elements, nodes))

    @property
    def compacted(self):
        cmp = {}
        for e in self.elements:
            cmp[e] = (self.nodes[e], self.boundaries[e])
        return cmp

    def __str__(self):
        return str(self.compacted)

    def __repr__(self):
        return str(self)

    def __copy__(self):
        return Context([e for e in self.elements], [self.boundaries[e] for e in self.elements],
                       [self.nodes[e] for e in self.elements])

    def check_intersection_and_update_boundary(self, e, boundary):
        """
        Check if context boundary at element e intersect with boundary
            - Not intersect -> Return False
            - Intersect -> Update boundary and return True
        :param e:
        :param boundary:
        :return:
        """
        self_b = self.boundaries[e]
        if self_b is None or boundary is None:
            raise ValueError('boundary is None' + str(self.boundaries[e]) + ' - ' + str(boundary))
        if (self_b[1] < boundary[0]) or (self_b[0] > boundary[1]):
            return False
        self.boundaries[e] = [max(self_b[0], boundary[0]), min(self_b[1], boundary[1])]
        return True
