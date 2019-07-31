from config import root_path
from tests.test_case import TestCaseCompare
from tldb.core.client import TLDB


class TestTLDB(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass('core/tldb')
        cls.tldb = TLDB('local')

    def test_rtree_index_csv_file(self):
        method_id = self.id().split('.')[-1]
        input_folder = root_path() / 'tests' / 'io' / 'in' / 'cases' / 'simple_small' / 'A_B_D_table.dat'
        self.tldb.load_table_object_from_csv('table', input_folder, delimiter=' ', index_type='rtree',
                                             headers=['A', 'B', 'D'], max_n_children=2)
        with self.out_file[method_id].open(mode='w') as f:
            f.write(self.tldb.get_object('table').ordered_str())
        self.file_compare_default()

    def test_rtree_index_xml_file(self):
        method_id = self.id().split('.')[-1]
        self.tldb.load_object_from_xml('xml', root_path().joinpath('tests/io/in/core/lib/dewey_id/messages.xml'),
                                       max_n_children=2)
        with self.out_file[method_id].open(mode='w') as f:
            f.write(self.tldb.get_object('xml').ordered_str())
        self.file_compare_default()

    def test_rtree_index_from_folder(self):
        method_id = self.id().split('.')[-1]
        tldb = TLDB('simple_small')
        tldb.load_from_folder(root_path() / 'tests' / 'io' / 'in' / 'cases' / 'simple_small', max_n_children=2)
        with self.out_file[method_id].open(mode='w') as f:
            for obj in tldb.all_objects_name:
                f.write(tldb.get_object(obj).ordered_str())
                f.write('-' * 20 + '\n')
        self.file_compare_default()

