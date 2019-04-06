import xmltodict

from .attribute import TLDBAttribute
from .object import HierarchyObject, TableObject
from tldb.core.lib.dewey_id import generate_dewey_id_from_dict
from tldb.core.structure.dewey_id import DeweyID
from tldb.core.structure.entry import Entry
from tldb.core.structure.node import XMLNode
from .utils import SKIP_IN_PATH, INDEX_STRUCTURE_MAPPER
from typing import List


class TLDB:
    # TODO: Change boundary to tuple
    # TODO: Change list to set if possible (desc_range_search, ancestor link, link_xml, link_sql)
    # TODO: Fix node.add_child_node (in case > max n_child) => insert
    # TODO: node contains set of entry => does not allow duplicate entry
    # TODO: should node be hashable as well? (hash = tuple(name, boundary))

    """
    The TLDB client
    """
    def __init__(self, host=None):
        self.host = host
        # TODO: REPLACE map by some hashmap?
        self._all_objects = {}
        self._all_objects_names = []

    @property
    def all_objects(self):
        return self._all_objects

    @property
    def all_objects_name(self):
        return self._all_objects_names

    def get_object(self, obj_name):
        if obj_name not in self._all_objects:
            raise Exception("Object %s does not exist" % obj_name)
        return self._all_objects[obj_name]

    def get_multiple_objects(self, objects_name: List[str]):
        objects = []
        for obj_name in objects_name:
            if obj_name not in self._all_objects:
                raise Exception("Object %s does not exist" % obj_name)
            objects.append(self._all_objects[obj_name])
        return objects

    def add_object(self, obj):
        if obj.name in self._all_objects:
            raise Exception("Object %s existed" % obj.name)
        self._all_objects_names.append(obj.name)
        self._all_objects[obj.name] = obj

    def add_multiple_objects(self, objects):
        for obj in objects:
            if obj.name in self._all_objects:
                raise Exception("Object %s existed" % obj.name)
        for obj in objects:
            self._all_objects_names.append(obj.name)
            self._all_objects[obj.name] = obj

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
            cells = tuple(map(lambda x: float(x), row.split(delimiter)))
            return Entry(cells)

        for par in (obj_name, file_path):
            if par in SKIP_IN_PATH:
                raise Exception("Empty value passed for a required argument.")
        if index_type not in INDEX_STRUCTURE_MAPPER:
            raise Exception("Unsupported index_type. The index structure should be: ",
                            '|'.join(list(INDEX_STRUCTURE_MAPPER.keys())))
        with file_path.open() as f:
            contents = [line.rstrip() for line in f]

        if not headers:
            if not headers_first_line:
                raise Exception("Must either provide headers or use first line as headers")
            else:
                headers = contents[0].split(delimiter)
                contents = contents[1:]
        entries = [row_to_entry(row) for row in contents]
        if len(headers) != len(entries[0].coordinates):
            raise Exception(f"Number of headers {headers} is different from first entry {str(entries[0])}")

        index_structure = INDEX_STRUCTURE_MAPPER[index_type](obj_name)
        index_structure.load(entries)
        table_object = TableObject(obj_name, index_structure)
        table_object.attributes = {}
        for h in headers:
            table_object.add_attribute(TLDBAttribute(h, table_object))
        self.add_object(table_object)

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
            xml_object.add_attribute(TLDBAttribute(attr, xml_object, index_structure))
        self.add_object(xml_object)

    # TODO: This is very bad adhoc, used for previous data format
    def load_from_folder(self, folder_path, index_type='rtree'):
        xml_element_files = folder_path.glob('*_id.dat')
        attributes = sorted([f.stem.split('_')[0] for f in xml_element_files])
        attributes = dict.fromkeys(attributes)
        xml_object = HierarchyObject('_'.join(list(attributes.keys())) + '_xml')
        for attr in attributes:
            with (folder_path / (attr + '_id.dat')).open() as f:
                attr_id = [line.rstrip() for line in f]
            with (folder_path / (attr + '_v.dat')).open() as f:
                attr_v = [line.rstrip() for line in f]
            # TODO: Text and null?
            attr_v = [float(v) for v in attr_v]
            entries = [Entry((DeweyID(id_v[0]), id_v[1])) for id_v in zip(attr_id, attr_v)]
            index_structure = INDEX_STRUCTURE_MAPPER[index_type](attr)
            index_structure.load(entries, node_type=XMLNode)
            xml_object.add_attribute(TLDBAttribute(attr, xml_object, index_structure))
        self.add_object(xml_object)
        table_files = folder_path.glob('*_table.dat')
        for table_f in sorted(table_files):
            table_name = table_f.stem.replace('_table', '')
            self.load_object_from_csv(table_name, table_f, delimiter=' ', headers=table_name.split('_'))
