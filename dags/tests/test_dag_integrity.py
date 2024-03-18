import unittest
from airflow.models import DagBag

class TestDagIntegrity(unittest.TestCase):

    def setUp(self):
        self.dagbag = DagBag()

    def test_import_dags(self):
        self.assertFalse(
            len(self.dagbag.import_errors),
            'DAG import failures. Errors: {}'.format(
                self.dagbag.import_errors
            )
        )

    def test_alert_email_present(self):
        expected_alert_email = 'example@gmail.com'

        for dag_id, dag in self.dagbag.dags.items():
            # Check if the DAG is present
            self.assertIsNotNone(dag, f'DAG with ID {dag_id} not found.')

            # Check if alert email is present in the default args
            alert_email = dag.default_args.get('email', [])
            msg = f'Alert email not set for DAG {dag_id}'
            self.assertIn(expected_alert_email, alert_email, msg)

if __name__ == '__main__':
    unittest.main()
