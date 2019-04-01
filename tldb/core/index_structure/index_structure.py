from abc import ABC, abstractmethod


class IndexStructure(ABC):
    def __init__(self, name, object_name):
        self._name = name
        self._object_name = object_name

    @property
    def name(self):
        return self._name

    @property
    def object_name(self):
        return self._object_name

    @abstractmethod
    def load(self, entries):
        """

        :type entries: list(Entry)
        """
        pass
