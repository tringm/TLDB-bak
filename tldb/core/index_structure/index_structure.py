from abc import ABC, abstractmethod
from typing import List
from tldb.core.structure.entry import Entry
import logging


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
    def load(self, entries: List[Entry]):
        logger = logging.getLogger(f"Indexer:{self.name}")
        logger.debug(f"Start loading {self.object_name} with method {method}")

    @abstractmethod
    def range_search(self, boundaries):
        pass
