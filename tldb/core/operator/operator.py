from abc import ABC, abstractmethod

from tldb.core.tldb import TLDB
from tldb.core.structure.context import Context


class Operator(ABC):
    def __init__(self, name: str, tldb: TLDB, context: Context):
        self.name = name
        self.tldb = tldb
        self.context = context

    @abstractmethod
    def perform(self):
        pass
