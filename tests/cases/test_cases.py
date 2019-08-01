import logging
import unittest
from pathlib import Path
from typing import Optional, Union

from tests.test_case import TestCaseCompare
from tldb.core.client import TLDB
from tldb.core.object import TableObject
from tldb.core.operator.join import ComplexXMLSQLJoin
from tldb.core.structure.context import RangeContext
from tldb.core.structure.interval import Interval
from tldb.server.query.xml_query import XMLQuery


class TestCaseSimpleSmall(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass('cases/simple_small')
        cls.tldb = TLDB('local')

    def test_simple_small(self):
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
    def setUpClass(cls, test_path: Optional[Union[Path, str]] = None):
        super().setUpClass('cases/orderline')
        cls.tldb = TLDB('local')

    def setUp(self, default_logging_level: Optional[int] = logging.INFO) -> None:
        super().setUp(default_logging_level=logging.VERBOSE)

    def test_orderline_price_asin_small_original(self):
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

    @unittest.skip("")
    def test_orderline_price_asin_medium(self):
        self.tldb.load_from_folder(self.input_folder / 'orderline_price_asin_medium', index_type='rtree', max_n_children=500)
        xml_query = XMLQuery('asin_orderline_price_xml')
        xml_query.load_from_matrix_file(self.input_folder / 'orderline_price_asin_medium' / 'XML_query.dat')

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

    @unittest.skip("")
    def test_orderline_price_asin_big(self):
        self.tldb.load_from_folder(self.input_folder / 'orderline_price_asin_big', index_type='rtree', max_n_children=1000)
        xml_query = XMLQuery('asin_orderline_price_xml')
        xml_query.load_from_matrix_file(self.input_folder / 'orderline_price_asin_big' / 'XML_query.dat')

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
        super().setUpClass('cases/invoice')
        cls.tldb = TLDB('local')

    @unittest.skip("")
    def test_invoice_small(self):
        self.tldb.load_from_folder(self.input_folder / 'invoice_complex_small', index_type='rtree', max_n_children=100)
        xml_query = XMLQuery('Invoice_OrderId_Orderline_asin_price_xml')
        xml_query.load_from_matrix_file(self.input_folder / 'invoice_complex_small' / 'XML_query.dat')

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

    @unittest.skip("")
    def test_invoice_medium(self):
        self.tldb.load_from_folder(self.input_folder / 'invoice_complex_medium', index_type='rtree', max_n_children=100)
        xml_query = XMLQuery('Invoice_OrderId_Orderline_asin_price_xml')
        xml_query.load_from_matrix_file(self.input_folder / 'invoice_complex_medium' / 'XML_query.dat')

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

    @unittest.skip("")
    def test_invoice_big(self):
        method_id = self.id().split('.')[-1]
        self.set_up_compare_files(method_id)
        self.set_up_logger(method_id, logging.INFO)
        self.tldb.load_from_folder(self.input_folder / 'invoice_complex_big', index_type='rtree', max_n_children=100)
        xml_query = XMLQuery('Invoice_OrderId_Orderline_asin_price_xml')
        xml_query.load_from_matrix_file(self.input_folder / 'invoice_complex_big' / 'XML_query.dat')

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

    @unittest.skip("")
    def test_invoice_small_with_range(self):
        self.tldb.load_from_folder(self.input_folder / 'invoice_complex_small', index_type='rtree', max_n_children=100)
        xml_query = XMLQuery('Invoice_OrderId_Orderline_asin_price_xml')
        xml_query.load_from_matrix_file(self.input_folder / 'invoice_complex_big' / 'XML_query.dat')

        attributes = xml_query.traverse_order
        ranges = [self.tldb.get_object('Invoice_OrderId_Orderline_asin_price_xml').get_attribute(a).index_structure.root.v_interval
                                      for a in attributes]
        ranges[1] = Interval((0, 200))
        initial_range = RangeContext(attributes,ranges)
        tables_name = []
        for obj_name in self.tldb.all_objects_name:
            if isinstance(self.tldb.get_object(obj_name), TableObject):
                tables_name.append(obj_name)
        join_op = ComplexXMLSQLJoin(self.tldb, xml_query=xml_query, tables=tables_name, initial_range_context=initial_range)
        join_op.perform()

    def test_invoice_medium_with_range(self):
        self.tldb.load_from_folder(self.input_folder / 'invoice_complex_medium', index_type='rtree', max_n_children=100)
        xml_query = XMLQuery('Invoice_OrderId_Orderline_asin_price_xml')
        xml_query.load_from_matrix_file(self.input_folder / 'invoice_complex_big' / 'XML_query.dat')

        attributes = xml_query.traverse_order
        ranges = [self.tldb.get_object('Invoice_OrderId_Orderline_asin_price_xml').get_attribute(a).index_structure.root.v_interval
                                      for a in attributes]
        ranges[1] = Interval((0, 200))
        initial_range = RangeContext(attributes,ranges)
        tables_name = []
        for obj_name in self.tldb.all_objects_name:
            if isinstance(self.tldb.get_object(obj_name), TableObject):
                tables_name.append(obj_name)
        join_op = ComplexXMLSQLJoin(self.tldb, xml_query=xml_query, tables=tables_name, initial_range_context=initial_range)
        join_op.perform()

    @unittest.skip("")
    def test_invoice_big_with_range(self):
        self.tldb.load_from_folder(self.input_folder / 'invoice_complex_big', index_type='rtree', max_n_children=100)
        xml_query = XMLQuery('Invoice_OrderId_Orderline_asin_price_xml')
        xml_query.load_from_matrix_file(self.input_folder / 'invoice_complex_big' / 'XML_query.dat')

        attributes = xml_query.traverse_order
        ranges = [self.tldb.get_object('Invoice_OrderId_Orderline_asin_price_xml').get_attribute(a).index_structure.root.v_interval
                                      for a in attributes]
        ranges[1] = Interval((0, 200))
        initial_range = RangeContext(attributes,ranges)
        tables_name = []
        for obj_name in self.tldb.all_objects_name:
            if isinstance(self.tldb.get_object(obj_name), TableObject):
                tables_name.append(obj_name)
        join_op = ComplexXMLSQLJoin(self.tldb, xml_query=xml_query, tables=tables_name, initial_range_context=initial_range)
        join_op.perform()

    # def test_invoice_all(self):
    #     method_id = self.id().split('.')[-1]
    #     self.set_up_compare_files(method_id)
    #     self.set_up_logger(method_id, logging.INFO)
    #     self.tldb.load_from_folder(self.input_folder / 'invoice_complex', index_type='rtree', max_n_children=500)
    #     xml_query = XMLQuery('Invoice_OrderId_Orderline_asin_price_xml')
    #     xml_query.load_from_matrix_file(self.input_folder / 'invoice_complex' / 'XML_query.dat')
    #
    #     attributes = xml_query.traverse_order
    #     initial_range = RangeContext(attributes,
    #                                  [self.tldb.get_object('Invoice_OrderId_Orderline_asin_price_xml').get_attribute(a).index_structure.root.v_interval
    #                                   for a in attributes])
    #     tables_name = []
    #     for obj_name in self.tldb.all_objects_name:
    #         if isinstance(self.tldb.get_object(obj_name), TableObject):
    #             tables_name.append(obj_name)
    #     join_op = ComplexXMLSQLJoin(self.tldb, xml_query=xml_query, tables=tables_name, initial_range_context=initial_range)
    #     join_op.perform()
