from .attribute import TLDBAttribute
from tldb.core.index_structure.index_structure import IndexStructure
from typing import Dict


class TLDBObject:
    def __init__(self, obj_name: str, object_type:str, index_structure: IndexStructure = None):
        self._name = obj_name
        self._all_attributes = {}
        self._all_attributes_names = []
        self._type = object_type
        self._index_structure = index_structure
        self._cached_attributes = None

    def __str__(self):
        rep_string = 'TLDBObject' + ':' + self.type + ':' + self.name + '\n'
        rep_string += 'INDEX STRUCTURE: \n' + str(self.index_structure)
        rep_string += '\nATTRIBUTES\n'
        for attr in self.all_attributes_name:
            rep_string += str(self.get_attribute(attr))
            rep_string += '\n'
        return rep_string

    def ordered_str(self):
        rep_string = 'TLDBObject' + ':' + self.type + ':' + self.name + '\n'
        rep_string += f"INDEX STRUCTURE: \n{self.index_structure.ordered_str() if self.index_structure else None}\n"
        rep_string += 'ATTRIBUTES\n'
        for attr in self.all_attributes_name:
            rep_string += self.get_attribute(attr).ordered_str()
            rep_string += '\n'
        return rep_string

    def __repr__(self):
        return 'TLDBObject' + ':' + self.type + ':' + self.name

    @property
    def name(self):
        return self._name

    @property
    def type(self):
        return self._type

    @property
    def index_structure(self):
        return self._index_structure

    @property
    def all_attributes(self):
        return self._all_attributes

    @all_attributes.setter
    def all_attributes(self, new_attributes: Dict[str, TLDBAttribute]):
        self._cached_attributes = self._all_attributes
        self._all_attributes = new_attributes
        self._all_attributes_names = list(self._all_attributes.keys())

    @property
    def all_attributes_name(self):
        return self._all_attributes_names

    def add_attribute(self, attribute: TLDBAttribute):
        self._all_attributes[attribute.name] = attribute
        self._all_attributes_names.append(attribute.name)

    def get_attribute(self, attr_name: str) -> TLDBAttribute:
        return self._all_attributes[attr_name]


class TableObject(TLDBObject):
    def __init__(self, obj_name, index_structure, attributes=None):
        super(TableObject, self).__init__(obj_name, 'table', index_structure)
        self.attributes = attributes

    def range_search(self, boundaries):
        return self.index_structure.range_search(boundaries)


class HierarchyObject(TLDBObject):
    def __init__(self, obj_name, attributes=None):
        super(HierarchyObject, self).__init__(obj_name, 'hierarchy')
        self.attributes = attributes
