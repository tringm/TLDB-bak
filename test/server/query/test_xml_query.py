from test.tests import TestCaseCompare
from tldb.server.query.xml_query import XMLQuery


class TestXMLQuery(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super(TestXMLQuery, cls).setUpClass()
        cls.input_folder = cls.input_folder
        cls.output_folder = cls.output_folder / 'server' / 'query'

    def test_load_from_matrix_file(self):
        method_id = self.id().split('.')[-1]
        self.set_up_compare_files(method_id)
        xml_query = XMLQuery('A_B_C_D')
        xml_query.load_from_matrix_file(self.input_folder / 'cases' / 'simple_small' / 'XML_query.dat')
        with self.out_file[method_id].open(mode='w') as f:
            f.write(str(xml_query))
        self.file_compare(self.out_file[method_id], self.exp_file[method_id])
