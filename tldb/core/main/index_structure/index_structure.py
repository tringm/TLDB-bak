from abc import ABC, abstractmethod


class IndexStructure(ABC):
    def __init__(self, name, type):
        self.name=name
        self.type = type

    @abstractmethod
    def load(self, entries):
        """

        :type entries: list(Entry)
        """
        pass
