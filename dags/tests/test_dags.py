import unittest
from airflow.models import DagBag
import pendulum
from airflow.utils.trigger_rule import TriggerRule

class TestLoadDag(unittest.TestCase):
    def setUp(self):
        self.dagbag = DagBag(include_examples=False)
        self.dag_ids = ['film_check_load', 'actor_check_load', 'film_actor_check_load', 'category_check_load'
                        , 'film_category_check_load', 'language_check_load', 'inventory_check_load']

    def test_dag_loaded(self):
        for dag_id in self.dag_ids:
            dag = self.dagbag.get_dag(dag_id)
            self.assertIsNotNone(dag)
            self.assertEqual(len(dag.tasks), 2)  

    def test_task_dependencies(self):
        for dag_id in self.dag_ids:
            dag = self.dagbag.get_dag(dag_id)
            quality_checks_task = dag.get_task('quality_checks')
            upload_task = dag.get_task('upload_table') 
            self.assertIn(quality_checks_task, upload_task.upstream_list)

   # Test if the DAG is created correctly 
    def test_dag_starts_on_correct_date(self):
        for dag_id in self.dag_ids:
            dag = self.dagbag.get_dag(dag_id)
            assert dag.start_date == pendulum.datetime(2024, 3, 17, tz="UTC")

    def test_dag_has_catchup_set_to_false(self):
        for dag_id in self.dag_ids:
            dag = self.dagbag.get_dag(dag_id)
            assert not dag.catchup

    # Test if the tasks are ordered correctly
    def test_dag_has_correct_task_order(self):
        for dag_id in self.dag_ids:
            dag = self.dagbag.get_dag(dag_id)

            quality_checks_task = dag.get_task('quality_checks')
            load_task = dag.get_task('upload_table')

            assert quality_checks_task.downstream_list == [load_task]

    # Test if the tasks are triggering on the correct rules
    def test_load_only_run_when_check_is_successful(self):
        for dag_id in self.dag_ids:
            dag = self.dagbag.get_dag(dag_id)
            load_task = dag.get_task('upload_table')
            assert load_task.trigger_rule == TriggerRule.ALL_SUCCESS


if __name__ == '__main__':
    unittest.main()