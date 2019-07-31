from tests.test_case import TestCaseCompare
from tldb.server.query.xml_query import XMLQuery
from config import root_path


class TestXMLQuery(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass('server/query')

    def test_load_from_matrix_file(self):
        method_id = self.id().split('.')[-1]
        xml_query = XMLQuery('A_B_C_D')
        xml_query.load_from_matrix_file(root_path().joinpath('tests/io/in/cases/simple_small/XML_query.dat'))
        with self.out_file[method_id].open(mode='w') as f:
            f.write(str(xml_query))
        self.file_compare_default()
