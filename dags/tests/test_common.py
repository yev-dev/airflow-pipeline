import unittest

from airflow.models import DagBag


class TestDagIntegrity(unittest.TestCase):
    """
    DAG validation tests are common for all the DAGs in Airflow
    This test will check the correctness of each DAG. It will also check whether a graph contains cycle or not.
    Tests will fail even if we have a typo in any of the DAG.
    """

    LOAD_SECOND_THRESHOLD = 2

    def setUp(self):
        self.dagbag = DagBag()

    def test_import_dags(self):
        self.assertFalse(
            len(self.dagbag.import_errors), 'DAG import failures. Errors: {}'.format(self.dagbag.import_errors)
        )


suite = unittest.TestLoader().loadTestsFromTestCase(TestDagIntegrity)
unittest.TextTestRunner(verbosity=2).run(suite)

