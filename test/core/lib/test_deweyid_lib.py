import json
from collections import OrderedDict

import xmltodict

from test.tests import TestCaseCompare
from tldb.core.lib.dewey_id import generate_dewey_id_from_dict


class TestGenerateDewey(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super(TestGenerateDewey, cls).setUpClass()
        cls.input_folder = cls.input_folder / 'lib'
        cls.output_folder = cls.output_folder / 'core' / 'lib' / 'dewey_id'

    def test_generate_dewey_id_from_json(self):
        """
        test description
        :return:
        """
        method_id = self.id().split('.')[-1]
        self.set_up_compare_files(method_id)
        self.in_file[method_id] = self.input_folder / 'messages.json'

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
        self.set_up_compare_files(method_id)
        self.in_file[method_id] = self.input_folder / 'messages.xml'

        with self.in_file[method_id].open() as f:
            xml_dict = xmltodict.parse(f.read())
        elements = generate_dewey_id_from_dict(xml_dict)
        with self.out_file[method_id].open(mode='w') as f:
            f.write(', '.join(['DeweyID', 'attribute', 'value']) + '\n')
            for e in elements:
                f.write(', '.join([str(comp) for comp in e]) + '\n')
        self.file_compare(self.out_file[method_id], self.exp_file[method_id])