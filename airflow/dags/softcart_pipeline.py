from datetime import datetime, timedelta
import csv
import io
import random

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from faker import Faker

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def generate_and_upload_orders(**context):
    from azure.storage.filedatalake import DataLakeServiceClient

    conn = BaseHook.get_connection("azure_data_lake_default")
    connection_string = conn.extra_dejson.get("connection_string")

    service_client = DataLakeServiceClient.from_connection_string(connection_string)
    file_system_client = service_client.get_file_system_client(file_system="bronze")

    fake = Faker()
    Faker.seed()

    rows = []
    for i in range(50):
        rows.append({
            "order_id": int(datetime.now().strftime("%y%m%d%H%M%S")) * 1000 + i,
            "customer_id": random.randint(1, 500),
            "order_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "payment_method": random.choice(["credit_card", "debit_card", "paypal", "cash_on_delivery"]),
            "payment_status": random.choices(
                ["paid", "failed", "refunded", "pending"], weights=[0.85, 0.06, 0.04, 0.05]
            )[0],
            "shipment_status": random.choice(["delivered", "shipped", "processing", "cancelled"]),
            "order_total": round(random.uniform(10, 500), 2),
        })

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

    file_name = f"incremental/orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    file_client = file_system_client.get_file_client(file_name)
    file_client.upload_data(buffer.getvalue(), overwrite=True)

    print(f"Uploaded {len(rows)} new orders to {file_name}")
    context["ti"].xcom_push(key="uploaded_file", value=file_name)


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

    run_dbt_task = BashOperator(
        task_id="run_dbt_build",
        bash_command=(
            "which docker || (apt-get update -qq && apt-get install -y -qq docker.io); "
            "docker exec airflow-dbt-1 dbt build --project-dir /opt/dbt_project"
        ),
    )

    generate_data_task >> load_to_snowflake_task >> run_dbt_task