import xmltodict

from tldb.core.lib.dewey_id import generate_dewey_id_from_dict
from tldb.core.structure.dewey_id import DeweyID
from tldb.core.structure.entry import Entry
from tldb.core.structure.node import XMLNode
from .utils import SKIP_IN_PATH, INDEX_STRUCTURE_MAPPER


class TLDBObject:
    def __init__(self, obj_name, tldb_type, index_structure):
        self._name = obj_name
        self._all_attributes = {}
        self._all_attributes_names = []
        self._type = tldb_type
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
    def all_attributes(self, new_attributes):
        self._cached_attributes = self._all_attributes
        self._all_attributes = new_attributes
        self._all_attributes_names = list(self._all_attributes.keys())

    @property
    def all_attributes_name(self):
        return self._all_attributes_names

    def add_attribute(self, attr_name, attribute):
        self._all_attributes[attr_name] = attribute
        self._all_attributes_names.append(attr_name)

    def get_attribute(self, attr_name):
        return self._all_attributes[attr_name]


class TableObject(TLDBObject):
    def __init__(self, obj_name, index_structure, attributes=None):
        super(TableObject, self).__init__(obj_name, 'table', index_structure)
        self.attributes = attributes

    def range_search(self, boundaries):
        return self.index_structure.range_search(boundaries)


class HierarchyObject(TLDBObject):
    def __init__(self, obj_name, attributes=None):
        super(HierarchyObject, self).__init__(obj_name, 'hierarchy', None)
        self.attributes = attributes


class TLDBAttribute:
    def __init__(self, name, tldb_object, index_structure=None):
        self.name = name
        self.tldb_object = tldb_object
        self.index_structure = index_structure

    def __repr__(self):
        return 'TLDBAttribute' + ':' + self.tldb_object.name + '.' + self.name

    def __str__(self):
        rep_string = 'TLDBAttribute' + ':' + self.tldb_object.name + '.' + self.name + '\n'
        rep_string += 'INDEX STRUCTURE: \n' + str(self.index_structure)
        return rep_string


class TLDB:
    """
    The TLDB client
    """
    def __init__(self, host=None):
        self.host = host
        # TODO: REPLACE map by some hashmap?
        self.objects = {}

    # TODO: think about flexible change file path to an url
    def load_object_from_csv(self, obj_name, file_path, index_type='rtree', delimiter=',',
                             headers_first_line=True, headers=None):
        """

        :param obj_name:
        :param file_path:
        :param index_type:
        :param delimiter:
        :param headers_first_line: True if header is in first line of the file
        :param headers: a list of headers
        :return:
        """

        def row_to_entry(row):
            # TODO: handle text?
            cells = list(map(lambda x: float(x), row.split(delimiter)))
            return Entry(cells)

        if obj_name in self.objects:
            raise ValueError(f"Object name {obj_name} existed")

        for par in (obj_name, file_path):
            if par in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")
        if index_type not in INDEX_STRUCTURE_MAPPER:
            raise ValueError("Unsupported index_type. The index structure should be: ",
                             '|'.join(list(INDEX_STRUCTURE_MAPPER.keys())))
        with file_path.open() as f:
            contents = [line.rstrip() for line in f]

        if not headers:
            if not headers_first_line:
                raise ValueError("Must either provide headers or use first line as headers")
            else:
                headers = contents[0].split(delimiter)
                contents = contents[1:]
        entries = [row_to_entry(row) for row in contents]
        if len(headers) != len(entries[0].coordinates):
            raise ValueError(f"Number of headers {headers} is different from first entry {str(entries[0])}")

        index_structure = INDEX_STRUCTURE_MAPPER[index_type](obj_name)
        index_structure.load(entries)
        table_object = TableObject(obj_name, index_structure)
        table_object.attributes = {}
        for h in headers:
            table_object.add_attribute(h, TLDBAttribute(h, table_object))
        self.objects[obj_name] = table_object

    def load_object_from_xml(self, obj_name, file_path, index_type='rtree'):
        with file_path.open() as f:
            xml_dict = xmltodict.parse(f.read())
        elements = generate_dewey_id_from_dict(xml_dict)
        attributes = {}
        for e in elements:
            id_value_pair = (e[0], e[2])
            if e[1] not in attributes:
                attributes[e[1]] = [id_value_pair]
            else:
                attributes[e[1]].append(id_value_pair)
        xml_object = HierarchyObject(obj_name)
        for attr in attributes:
            index_structure = INDEX_STRUCTURE_MAPPER[index_type](attr)
            # TODO: handle null and text
            entries = [Entry([DeweyID(id_v[0]), float(id_v[1])]) for id_v in attributes[attr]]
            index_structure.load(entries, node_type=XMLNode)
            xml_object.add_attribute(attr, TLDBAttribute(attr, xml_object, index_structure))
        self.objects[obj_name] = xml_object

    # TODO: This is very bad adhoc, used for previous data format
    def load_from_folder(self, folder_path, index_type='rtree'):
        xml_element_files = folder_path.glob('*_id.dat')
        attributes = sorted([f.stem.split('_')[0] for f in xml_element_files])
        attributes = dict.fromkeys(attributes)
        xml_object = HierarchyObject('_'.join(list(attributes.keys())))
        for attr in attributes:
            with (folder_path / (attr + '_id.dat')).open() as f:
                attr_id = [line.rstrip() for line in f]
            with (folder_path / (attr + '_v.dat')).open() as f:
                attr_v = [line.rstrip() for line in f]
            # TODO: Text and null?
            attr_v = [float(v) for v in attr_v]
            entries = [Entry([DeweyID(id_v[0]), id_v[1]]) for id_v in zip(attr_id, attr_v)]
            index_structure = INDEX_STRUCTURE_MAPPER[index_type](attr)
            index_structure.load(entries, node_type=XMLNode)
            xml_object.add_attribute(attr, TLDBAttribute(attr, xml_object, index_structure))
        self.objects[xml_object.name] = xml_object
        table_files = folder_path.glob('*_table.dat')
        for table_f in table_files:
            table_name = table_f.stem.replace('_table', '')
            self.load_object_from_csv(table_name, table_f, delimiter=' ', headers=table_name.split('_'))
