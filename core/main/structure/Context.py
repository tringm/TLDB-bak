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