from typing import List


class Context:
    def __init__(self, elements: List[str]):
        self.elements = elements
        self.boundary = [None] * len(elements)
