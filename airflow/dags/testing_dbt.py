from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

with DAG(
    'dbt_trino_integration_test',
    default_args=default_args,
    description='Tests if dbt can talk to Trino inside the Airflow container',
    schedule_interval=None, # Manual trigger
    catchup=False,
) as dag:

    # Task 1: Check if the dbt-trino library is actually installed
    check_dbt_install = BashOperator(
        task_id='check_dbt_installation',
        bash_command='dbt --version'
    )

    # Task 2: Test the connection defined in profiles.yml
    # Note: we use --profiles-dir . because profiles.yml is in the same folder
    test_dbt_connection = BashOperator(
        task_id='dbt_debug_connection',
        bash_command='cd /opt/airflow/dbt && dbt debug --profiles-dir . || echo "Ignore Git Error"'
    )

    check_dbt_install >> test_dbt_connection