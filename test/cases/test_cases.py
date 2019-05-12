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
        self.tldb.load_from_folder(self.input_folder, index_type='rtree', max_n_children=2)
        xml_query = XMLQuery('A_B_C_D_xml')
        xml_query.load_from_matrix_file(self.input_folder / 'XML_query.dat')

        attributes = xml_query.traverse_order
        initial_range = RangeContext(attributes,
                                     [self.tldb.get_object('A_B_C_D_xml').get_attribute(a).index_structure.root.v_interval
                                      for a in attributes])
        tables_name = []
        for obj_name in self.tldb.all_objects_name:
            if isinstance(self.tldb.get_object(obj_name), TableObject):
                tables_name.append(obj_name)
        join_op = ComplexXMLSQLJoin(self.tldb, xml_query=xml_query, tables=tables_name, initial_range_context=initial_range)
        join_op.perform()


class TestCaseOrderline(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super(TestCaseOrderline, cls).setUpClass()
        cls.tldb = TLDB('local')
        cls.input_folder = cls.input_folder / 'cases'
        cls.output_folder = cls.output_folder / 'cases'

    def test_orderline_price_asin_small_original(self):
        method_id = self.id().split('.')[-1]
        self.set_up_compare_files(method_id)
        self.set_up_logger(method_id, logging.INFO)
        self.tldb.load_from_folder(self.input_folder / 'orderline_price_asin_small_original', index_type='rtree', max_n_children=500)
        xml_query = XMLQuery('asin_orderline_price_xml')
        xml_query.load_from_matrix_file(self.input_folder / 'orderline_price_asin_small_original' / 'XML_query.dat')

        attributes = xml_query.traverse_order
        initial_range = RangeContext(attributes,
                                     [self.tldb.get_object('asin_orderline_price_xml').get_attribute(a).index_structure.root.v_interval
                                      for a in attributes])
        tables_name = []
        for obj_name in self.tldb.all_objects_name:
            if isinstance(self.tldb.get_object(obj_name), TableObject):
                tables_name.append(obj_name)
        join_op = ComplexXMLSQLJoin(self.tldb, xml_query=xml_query, tables=tables_name, initial_range_context=initial_range)
        join_op.perform()


class TestCaseInvoice(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super(TestCaseInvoice, cls).setUpClass()
        cls.tldb = TLDB('local')
        cls.input_folder = cls.input_folder / 'cases' / 'invoice_complex_small'
        cls.output_folder = cls.output_folder / 'cases'

    def test_invoice_smalle(self):
        method_id = self.id().split('.')[-1]
        print(method_id)
        self.set_up_compare_files(method_id)
        self.set_up_logger(method_id, logging.INFO)
        self.tldb.load_from_folder(self.input_folder, index_type='rtree', max_n_children=100)
        xml_query = XMLQuery('Invoice_OrderId_Orderline_asin_price_xml')
        xml_query.load_from_matrix_file(self.input_folder / 'XML_query.dat')

        attributes = xml_query.traverse_order
        initial_range = RangeContext(attributes,
                                     [self.tldb.get_object('Invoice_OrderId_Orderline_asin_price_xml').get_attribute(a).index_structure.root.v_interval
                                      for a in attributes])
        tables_name = []
        for obj_name in self.tldb.all_objects_name:
            if isinstance(self.tldb.get_object(obj_name), TableObject):
                tables_name.append(obj_name)
        join_op = ComplexXMLSQLJoin(self.tldb, xml_query=xml_query, tables=tables_name, initial_range_context=initial_range)
        join_op.perform()
