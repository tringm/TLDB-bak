import filecmp
import json
import unittest
from collections import OrderedDict

import xmltodict

from config import root_path
from tldb.core.lib.dewey_id import generate_dewey_id_from_dict
from test.tests import TestCaseCompare


class TestGenerateDewey(TestCaseCompare):
    def setUp(self):
        self.in_file = {}
        self.out_file = {}
        self.exp_file = {}

    def test_generate_dewey_id_from_json(self):
        """
        test description
        :return:
        """
        method_id = self.id().split('.')[-1]
        self.in_file[method_id] = root_path() / 'test' / 'io' / 'in' / 'lib' / 'messages.json'
        self.out_file[method_id] = root_path() / 'test' / 'io' / 'out' / 'lib' / 'dewey_id_from_json_out.txt'
        self.exp_file[method_id] = root_path() / 'test' / 'io' / 'out' / 'lib' / 'dewey_id_from_json_exp.txt'
        with self.in_file[method_id].open() as f:
            json_dict = json.load(f, object_pairs_hook=OrderedDict)
        elements = generate_dewey_id_from_dict(json_dict)
        with self.out_file[method_id].open(mode='w') as f:
            f.write(', '.join(['DeweyID', 'attribute', 'value']) + '\n')
            for e in elements:
                f.write(', '.join([str(comp) for comp in e]) + '\n')
        self.file_compare(self.out_file[method_id], self.exp_file[method_id])

    def test_generate_dewey_id_from_xml(self):
        method_id = self.id().split('.')[-1]
        self.in_file[method_id] = root_path() / 'test' / 'io' / 'in' / 'lib' / 'messages.xml'
        self.out_file[method_id] = root_path() / 'test' / 'io' / 'out' / 'lib' / 'dewey_id_from_xml_out.txt'
        self.exp_file[method_id] = root_path() / 'test' / 'io' / 'out' / 'lib' / 'dewey_id_from_xml_exp.txt'
        with self.in_file[method_id].open() as f:
            xml_dict = xmltodict.parse(f.read())
        elements = generate_dewey_id_from_dict(xml_dict)
        with self.out_file[method_id].open(mode='w') as f:
            f.write(', '.join(['DeweyID', 'attribute', 'value']) + '\n')
            for e in elements:
                f.write(', '.join([str(comp) for comp in e]) + '\n')
        self.file_compare(self.out_file[method_id], self.exp_file[method_id])
