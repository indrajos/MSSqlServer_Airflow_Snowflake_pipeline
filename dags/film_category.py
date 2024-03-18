from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.generic_transfer import GenericTransfer
from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.exceptions import AirflowException
import pendulum


# Function to check characteristics and data quality before loading
def quality_checks(table_name, mssql_conn_id):
    # Establish connection to MSSQL database
    mssql_hook = MsSqlHook(mssql_conn_id)
    with mssql_hook.get_conn() as conn:
        with conn.cursor() as cursor:

            # Execute SQL query to get characteristics of the table
            char_query = f"""
                SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{table_name}'
            """
            cursor.execute(char_query)
            characteristics = cursor.fetchall()

            # Execute SQL query to check for uniqueness
            uniq_query = f"""
                SELECT COUNT(*)
                FROM {table_name}
                GROUP BY film_id
                HAVING COUNT(*) > 1
            """
            cursor.execute(uniq_query)
            uniqueness = cursor.fetchall()

            # If duplicate rows are found, raise exception
            if uniqueness:
                raise AirflowException(f"Duplicate records found in the table '{table_name}'")
            else:
                print(f"No duplicate values found in the table '{table_name}'")

    return characteristics

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['example@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
with DAG(dag_id='film_category_check_load',
        schedule=None,
        start_date=pendulum.datetime(2024, 3, 17, tz="UTC"),
        catchup=False,
        default_args=default_args) as dag:

    # Task to perform quality checks
    quality_checks_task = PythonOperator(
        task_id='quality_checks',
        python_callable=quality_checks,
        op_kwargs={'table_name': 'film_category', 'mssql_conn_id': 'mssql1'},
        dag=dag,
    )

    # Task to upload data if quality checks pass
    upload_task = GenericTransfer(
        task_id='upload_table',
        sql="SELECT * FROM film_category",
        destination_table="FILM_CATEGORY",
        source_conn_id="mssql1",
        destination_conn_id="sf1",
        preoperator="TRUNCATE TABLE IF EXISTS film_category",
        dag=dag
    )

# Set task dependencies
quality_checks_task >> upload_task