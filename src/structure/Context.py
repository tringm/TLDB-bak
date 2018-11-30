from typing import List


class Context:
    def __init__(self, elements: List[str]):
        self.elements = elements
        self.boundaries = dict(zip(self.elements, [None] * len(self.elements)))
        self.nodes = dict(zip(self.elements, [None] * len(self.elements)))

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