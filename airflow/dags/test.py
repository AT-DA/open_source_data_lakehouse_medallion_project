# airflow DAG
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

# Important: The word 'dag' must appear as a variable assignment
with DAG(dag_id='aaa_test_v1', start_date=datetime(2025,1,1), schedule=None) as dag:
    t1 = EmptyOperator(task_id='ping')