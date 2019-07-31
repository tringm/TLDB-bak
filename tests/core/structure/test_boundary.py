from tests.test_case import TestCaseTimer
from tldb.core.structure.boundary import Boundary
from tldb.core.structure.interval import Interval


class TestBoundary(TestCaseTimer):
    def test_str(self):
        boundary = Boundary((Interval((1, 2)), Interval((2, 3))))
        self.assertEqual(str((Interval((1, 2)), Interval((2, 3)))), str(boundary))

    def test_get_interval(self):
        boundary = Boundary((Interval((1, 2)), Interval((2, 3))))
        self.assertEqual(boundary.get_interval(0), Interval((1, 2)))

    def test_join_and_update_boundary(self):
        boundary1 = Boundary((Interval((5, 10)), Interval((15, 20))))
        boundary2 = Boundary((Interval((1, 15)), Interval((10, 25))))
        boundary1.join_and_update_boundary(boundary2)
        self.assertEqual(boundary1, Boundary((Interval((1, 15)), Interval((10, 25)))))
