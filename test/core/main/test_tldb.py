from config import root_path
from test.tests import TestCaseCompare
from tldb.core.client import TLDB


class TestTLDB(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super(TestTLDB, cls).setUpClass()
        cls.tldb = TLDB('local')
        cls.input_folder = cls.input_folder / 'cases' / 'simple_small'
        cls.output_folder = cls.output_folder / 'core' / 'tldb'

    def test_rtree_index_csv_file(self):
        method_id = self.id().split('.')[-1]
        self.set_up_compare_files(method_id)

        self.tldb.load_table_object_from_csv('table', self.input_folder / 'A_B_D_table.dat', delimiter=' ', index_type='rtree',
                                             headers=['A', 'B', 'D'])

        with self.out_file[method_id].open(mode='w') as f:
            f.write(self.tldb.get_object('table').ordered_str())

        self.file_compare(self.out_file[method_id], self.exp_file[method_id])

    def test_rtree_index_xml_file(self):
        method_id = self.id().split('.')[-1]
        self.set_up_compare_files(method_id)

        self.tldb.load_object_from_xml('xml', root_path() / 'test' / 'io' / 'in' / 'lib' / 'messages.xml')
        with self.out_file[method_id].open(mode='w') as f:
            f.write(self.tldb.get_object('xml').ordered_str())
        self.file_compare(self.out_file[method_id], self.exp_file[method_id])

    def test_rtree_index_from_folder(self):
        method_id = self.id().split('.')[-1]
        self.set_up_compare_files(method_id)

        tldb = TLDB('simple_small')
        tldb.load_from_folder(self.input_folder)
        with self.out_file[method_id].open(mode='w') as f:
            for obj in tldb.all_objects_name:
                f.write(tldb.get_object(obj).ordered_str())
                f.write('-' * 20 + '\n')
        self.file_compare(self.out_file[method_id], self.exp_file[method_id])

