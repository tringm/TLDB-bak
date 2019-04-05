from abc import ABC, abstractmethod

from tldb.core.client import TLDB
from tldb.core.structure.context import Context


class Operator(ABC):
    def __init__(self, name: str, tldb: TLDB):
        self.name = name
        self.tldb = tldb

    @abstractmethod
    def perform(self):
        pass
