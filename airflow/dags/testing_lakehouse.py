from airflow import DAG
from airflow.providers.trino.operators.trino import TrinoOperator
from datetime import datetime

with DAG(
    dag_id='lakehouse_integration_test',
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    # This task tells Trino to insert a record
    # proving Airflow -> Trino -> Hive -> MinIO works!
    test_query = TrinoOperator(
        task_id='insert_via_airflow',
        trino_conn_id='trino_conn',
        sql="""
        INSERT INTO hive.test_schema.integration_test 
        VALUES (2, 'Inserted via Airflow orchestrator')
        """
    )

    test_query