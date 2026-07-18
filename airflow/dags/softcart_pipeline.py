from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from softcart_utils.generators import generate_and_upload_orders, generate_and_upload_clickstream, generate_and_upload_returns
from softcart_utils.alerts import slack_failure_alert, slack_test_summary, slack_cost_alert


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": slack_failure_alert,
}

with DAG(
    dag_id="softcart_pipeline",
    default_args=default_args,
    description="Soft Cart end-to-end ELT pipeline",
    schedule_interval="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["softcart"],
) as dag:

    generate_data_task = PythonOperator(
        task_id="generate_and_upload_orders",
        python_callable=generate_and_upload_orders,
    )

    generate_clickstream_task = PythonOperator(
        task_id="generate_and_upload_clickstream",
        python_callable=generate_and_upload_clickstream,
    )

    load_to_snowflake_task = SnowflakeOperator(
        task_id="load_new_orders_to_snowflake",
        snowflake_conn_id="snowflake_default",
        sql="""
            COPY INTO softcart_db.raw.orders
            FROM @softcart_db.raw.bronze_stage/{{ ti.xcom_pull(task_ids='generate_and_upload_orders', key='uploaded_file') }}
            FILE_FORMAT = softcart_db.raw.csv_format
            ON_ERROR = 'CONTINUE';
        """,
    )

    load_clickstream_task = SnowflakeOperator(
        task_id="load_clickstream_to_snowflake",
        snowflake_conn_id="snowflake_default",
        sql="""
            COPY INTO softcart_db.raw.clickstream_events (raw_event)
            FROM @softcart_db.raw.bronze_stage/{{ ti.xcom_pull(task_ids='generate_and_upload_clickstream', key='uploaded_clickstream_file') }}
            FILE_FORMAT = softcart_db.raw.json_format
            ON_ERROR = 'CONTINUE';
        """,
    )
    generate_returns_task = PythonOperator(
        task_id="generate_and_upload_returns",
        python_callable=generate_and_upload_returns,
    )

    load_returns_task = SnowflakeOperator(
        task_id="load_returns_to_snowflake",
        snowflake_conn_id="snowflake_default",
        sql="""
            COPY INTO softcart_db.raw.returns
            FROM @softcart_db.raw.bronze_stage/{{ ti.xcom_pull(task_ids='generate_and_upload_returns', key='uploaded_returns_file') }}
            FILE_FORMAT = softcart_db.raw.csv_format
            ON_ERROR = 'CONTINUE';
        """,
    )

    run_dbt_task = BashOperator(
        task_id="run_dbt_build",
        bash_command=(
            "which docker || (apt-get update -qq && apt-get install -y -qq docker.io); "
            "docker exec airflow-dbt-1 dbt build --project-dir /opt/dbt_project"
        ),
    )

    slack_summary_task = PythonOperator(
        task_id="slack_test_summary",
        python_callable=slack_test_summary,
        trigger_rule="all_done",
    )
    slack_cost_task = PythonOperator(
        task_id="slack_cost_alert",
        python_callable=slack_cost_alert,
        trigger_rule="all_done",
    )

    generate_data_task >> load_to_snowflake_task
    generate_clickstream_task >> load_clickstream_task
    load_to_snowflake_task >> generate_returns_task >> load_returns_task
    [load_returns_task, load_clickstream_task] >> run_dbt_task >> slack_summary_task >> slack_cost_task