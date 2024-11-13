from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
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

# Define the DAG
with DAG(
    'snowflake_sample_dag',
    default_args=default_args,
    description='A sample DAG to run a Snowflake query',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Define Snowflake connection parameters from environment or variables
    SNOWFLAKE_CONN_ID = "{{ var.value.get('SNOWFLAKE_CONN_ID', 'snowflake_connection') }}"
    SNOWFLAKE_SCHEMA = "{{ var.value.get('SNOWFLAKE_SCHEMA', 'TEST_DEV_DB.TEST_SCHEMA') }}"

    # Task 1: Run a simple query in Snowflake
    run_query = SnowflakeOperator(
        task_id='run_query',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="SELECT CURRENT_DATE;",
        schema=SNOWFLAKE_SCHEMA,
    )

    # Task 2: Print results (just a placeholder function)
    def process_results():
        print("Query executed successfully in Snowflake.")

    process_results_task = PythonOperator(
        task_id='process_results',
        python_callable=process_results,
    )

    # Set task dependencies
    run_query >> process_results_task
