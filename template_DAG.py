from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import timedelta

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Fetch connection details from Airflow Variables
SNOWFLAKE_CONN_ID = Variable.get("SNOWFLAKE_CONN_ID", default_var="snowflake_connection")
SNOWFLAKE_SCHEMA = Variable.get("SNOWFLAKE_SCHEMA", default_var="TEST_DEV_DB.TEST_SCHEMA")

# Define the DAG
with DAG(
    dag_id='snowflake_sample_dag',
    default_args=default_args,
    description='A sample DAG to run a Snowflake query',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
    tags=["snowflake", "example", "sample"],
) as dag:

    # Task 1: Run a simple query in Snowflake
    run_query = SnowflakeOperator(
        task_id='run_query',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="SELECT CURRENT_DATE;",
        schema=SNOWFLAKE_SCHEMA,
    )

    # Task 2: Print results (placeholder function)
    def process_results():
        print("Query executed successfully in Snowflake.")

    process_results_task = PythonOperator(
        task_id='process_results',
        python_callable=process_results,
    )

    # Set task dependencies
    run_query >> process_results_task
