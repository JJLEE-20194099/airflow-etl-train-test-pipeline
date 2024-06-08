from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import airflow
from airflow.utils.dates import days_ago

args={'owner': 'airflow'}

default_args = {
    'owner': 'airflow',

    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="postgres_operator_dag",
    schedule="@once",
    start_date=datetime(2023, 11, 24),
    dagrun_timeout=timedelta(minutes=60),
    description='Test connect with postgresql',
    catchup=False
) as dag:

    create_table_sql_query = """
        CREATE TABLE clients (id SERIAL PRIMARY KEY, first_name VARCHAR, last_name VARCHAR, role VARCHAR);
    """

    insert_data_sql_query = """INSERT INTO clients (first_name, last_name, role) VALUES ('John', 'Smith', 'CEO');"""

    postgres_task = SQLExecuteQueryOperator(

        task_id='postgres_task',

        conn_id='postgres_local',

        sql=create_table_sql_query

    )

    postgres_task
