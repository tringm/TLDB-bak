import logging

from test.tests import TestCaseCompare
from tldb.core.client import TLDB
from tldb.core.object import TableObject
from tldb.core.operator.join import ComplexXMLSQLJoin
from tldb.core.structure.context import RangeContext
from tldb.server.query.xml_query import XMLQuery


class TestCaseSimpleSmall(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super(TestCaseSimpleSmall, cls).setUpClass()
        cls.tldb = TLDB('local')
        cls.input_folder = cls.input_folder / 'cases' / 'simple_small'
        cls.output_folder = cls.output_folder / 'cases'

    def test_simple_small(self):
        method_id = self.id().split('.')[-1]
        self.set_up_compare_files(method_id)
        self.set_up_logger(method_id, logging.VERBOSE)
        self.tldb.load_from_folder(self.input_folder)
        xml_query = XMLQuery('A_B_C_D')
        xml_query.load_from_matrix_file(self.input_folder / 'XML_query.dat')

        attributes = xml_query.traverse_order
        initial_range = RangeContext(attributes,
                                     [self.tldb.get_object('A_B_C_D').get_attribute(a).index_structure.root.v_interval
                                      for a in attributes])
        tables_name = []
        for obj_name in self.tldb.all_objects_name:
            if isinstance(self.tldb.get_object(obj_name), TableObject):
                tables_name.append(obj_name)
        join_op = ComplexXMLSQLJoin(self.tldb, xml_query=xml_query, tables=tables_name, initial_range_context=initial_range)
        join_op.perform()
