from test.tests import TestCaseCompare
from tldb.core.structure.dewey_id import DeweyID


class TestDeweyID(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super(TestDeweyID, cls).setUpClass()
        cls.in_file = {}
        cls.input_folder = cls.input_folder / 'cases' / 'simple_smalle'
        cls.output_folder = cls.output_folder / 'structure' / 'dewey_id'

    def test_equal(self):
        self.assertEqual(DeweyID('1.1.1'), DeweyID('1.1.1'))

    def test_smaller(self):
        self.assertFalse(DeweyID('1.1') < DeweyID('1.1'))
        self.assertTrue(DeweyID('1.1') < DeweyID('1.1.2'))
        self.assertTrue(DeweyID('1.1.1.1.2.3.4') < DeweyID('1.1.2'))

    def test_is_ancestor(self):
        self.assertFalse(DeweyID('1.1').is_ancestor(DeweyID('1.1')))
        self.assertTrue(DeweyID('1.1').is_ancestor(DeweyID('1.1.1')))
        self.assertTrue(DeweyID('1.1').is_ancestor(DeweyID('1.1.1.1.20')))
        self.assertFalse(DeweyID('1.1').is_ancestor(DeweyID('1.2.1')))

    def test_is_parent(self):
        self.assertFalse(DeweyID('1.1').is_parent(DeweyID('1.1')))
        self.assertFalse(DeweyID('1.1').is_parent(DeweyID('1.2.1')))
        self.assertFalse(DeweyID('1.1').is_parent(DeweyID('1.1.1.1')))
        self.assertTrue(DeweyID('1.1').is_parent(DeweyID('1.1.2')))
