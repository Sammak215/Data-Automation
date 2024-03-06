from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
form db import create_new_tables_in_postgres,insert_product_reviews,insert_most_reviewed_products

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'amazon_reviews_workflow',
    default_args=default_args,
    description='Automated data ingestion, cleaning, analysis, and loading process',
    schedule_interval='@daily',
)

# Define functions for tasks

def create_new_tables():
    create_new_tables_in_postgres()
    # Code to load data into PostgreSQL database
    pass

def load_data_to_database():
    insert_product_reviews()
    insert_most_reviewed_products()
    # Code to load data into PostgreSQL database
    pass

# Define tasks
ingest_data_task = PythonOperator(
    task_id='create_new_tables',
    python_callable=ingest_data,
    dag=dag,
)

clean_data_task = PythonOperator(
    task_id='load_data_to_database',
    python_callable=clean_data,
    dag=dag,
)



# Define dependencies between tasks
create_new_tables >> load_data_to_database 

# Define final task
end_task = DummyOperator(
    task_id='end_task',
    dag=dag,
)

# Set the final task dependency
load_data_to_database_task >> end_task
