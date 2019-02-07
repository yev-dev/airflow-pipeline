import unittest

from airflow.models import DagBag


class TestSignalsEtl(unittest.TestCase):
    """Check Signals Etl expectation"""

    def setUp(self):
        self.dagbag = DagBag()

    def test_task_count(self):
        """Check task count of test_signals_etl dag"""
        dag_id = 'signals_etl'
        dag = self.dagbag.get_dag(dag_id)
        self.assertEqual(len(dag.tasks), 5)

    def test_contain_tasks(self):
        """Check task contains in test_signals_etl dag"""
        dag_id = 'signals_etl'
        dag = self.dagbag.get_dag(dag_id)
        tasks = dag.tasks
        task_ids = list(map(lambda task: task.task_id, tasks))
        self.assertListEqual(task_ids, ['initialize_signals_etl_pipeline','mysql_get_max_signal_date_task', 'load_signals_task', 'insert_signals_task', 'send_email_task'])


suite = unittest.TestLoader().loadTestsFromTestCase(TestSignalsEtl)
unittest.TextTestRunner(verbosity=2).run(suite)