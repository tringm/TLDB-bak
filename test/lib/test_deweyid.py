import json
from collections import OrderedDict
import unittest
from pprint import pprint
from config import root_path
from tldb.core.main import generate_dewey_id_from_dict
import xmltodict
import filecmp


class TestGenerateDeweyFromJSON(unittest.TestCase):
    def setUp(self):
        self.in_file = root_path() / 'core' / 'io' / 'in' / 'test' / 'lib' / 'messages.json'
        self.out_file = root_path() / 'core' / 'io' / 'out' / 'test' / 'lib' / 'dewey_id_from_json_out.txt'
        self.exp_file = root_path() / 'core' / 'io' / 'out' / 'test' / 'lib' / 'dewey_id_from_json_exp.txt'

    def test_generate_dewey_id_from_json(self):
        """
        test description
        :return:
        """
        with self.in_file.open() as f:
            json_dict = json.load(f, object_pairs_hook=OrderedDict)
        elements = generate_dewey_id_from_dict(json_dict)
        with self.out_file.open(mode='w') as f:
            res = [['DeweyID', 'attribute', 'value']]
            for e in elements:
                res.append(list(e))
            pprint(res, stream=f)
        self.assertTrue(filecmp.cmp(str(self.out_file), str(self.exp_file), shallow=False),
                        'out file does not match exp file')


class TestGenerateDeweyFromXML(unittest.TestCase):
    def setUp(self):
        self.in_file = root_path() / 'core' / 'io' / 'in' / 'test' / 'lib' / 'messages.xml'
        self.out_file = root_path() / 'core' / 'io' / 'out' / 'test' / 'lib' / 'dewey_id_from_xml_out.txt'
        self.exp_file = root_path() / 'core' / 'io' / 'out' / 'test' / 'lib' / 'dewey_id_from_xml_exp.txt'

    def test_generate_dewey_id_from_xml(self):
        with self.in_file.open() as f:
            xml_dict = xmltodict.parse(f.read())
        elements = generate_dewey_id_from_dict(xml_dict)
        with self.out_file.open(mode='w') as f:
            res = [['DeweyID', 'attribute', 'value']]
            for e in elements:
                res.append(list(e))
            pprint(res, stream=f)
        self.assertTrue(filecmp.cmp(str(self.out_file), str(self.exp_file), shallow=False),
                        'out file does not match exp file')
