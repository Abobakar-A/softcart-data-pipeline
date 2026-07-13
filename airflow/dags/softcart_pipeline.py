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
            "order_id": int(context["ds_nodash"]) * 1000 + i,
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





def generate_and_upload_clickstream(**context):
    from azure.storage.filedatalake import DataLakeServiceClient
    import json

    conn = BaseHook.get_connection("azure_data_lake_default")
    connection_string = conn.extra_dejson.get("connection_string")
    service_client = DataLakeServiceClient.from_connection_string(connection_string)
    file_system_client = service_client.get_file_system_client(file_system="bronze")

    run_date = context["ds_nodash"]
    events = []
    num_sessions = 30
    event_counter = 0

    for session_num in range(num_sessions):
        session_id = f"sess_{run_date}_{session_num}"
        customer_id = random.randint(1, 500)
        base_time = datetime.now()

        for i in range(random.randint(1, 3)):
            event_counter += 1
            events.append({
                "event_id": f"evt_{run_date}_{event_counter}",
                "session_id": session_id,
                "customer_id": customer_id,
                "event_type": "page_view",
                "product_id": None,
                "event_timestamp": (base_time + timedelta(seconds=i * 5)).strftime("%Y-%m-%dT%H:%M:%S"),
            })

        if random.random() < 0.6:
            product_id = random.randint(1, 200)
            event_counter += 1
            events.append({
                "event_id": f"evt_{run_date}_{event_counter}",
                "session_id": session_id,
                "customer_id": customer_id,
                "event_type": "product_view",
                "product_id": product_id,
                "event_timestamp": (base_time + timedelta(seconds=20)).strftime("%Y-%m-%dT%H:%M:%S"),
            })

            if random.random() < 0.4:
                event_counter += 1
                events.append({
                    "event_id": f"evt_{run_date}_{event_counter}",
                    "session_id": session_id,
                    "customer_id": customer_id,
                    "event_type": "add_to_cart",
                    "product_id": product_id,
                    "event_timestamp": (base_time + timedelta(seconds=40)).strftime("%Y-%m-%dT%H:%M:%S"),
                })

                if random.random() < 0.5:
                    event_counter += 1
                    events.append({
                        "event_id": f"evt_{run_date}_{event_counter}",
                        "session_id": session_id,
                        "customer_id": customer_id,
                        "event_type": "purchase",
                        "product_id": product_id,
                        "event_timestamp": (base_time + timedelta(seconds=60)).strftime("%Y-%m-%dT%H:%M:%S"),
                    })

    buffer = io.StringIO()
    for event in events:
        buffer.write(json.dumps(event) + "\n")

    file_name = f"clickstream/events_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    file_client = file_system_client.get_file_client(file_name)
    file_client.upload_data(buffer.getvalue(), overwrite=True)

    print(f"Uploaded {len(events)} clickstream events to {file_name}")
    context["ti"].xcom_push(key="uploaded_clickstream_file", value=file_name)



def slack_failure_alert(context):
    import requests
    from airflow.models import Variable

    webhook_url = Variable.get("slack_webhook_url")
    task_id = context["task_instance"].task_id
    dag_id = context["task_instance"].dag_id
    execution_date = context["ds"]
    log_url = context["task_instance"].log_url

    message = {
        "text": (
            f":red_circle: *Pipeline Failure*\n"
            f"*DAG:* {dag_id}\n"
            f"*Task:* {task_id}\n"
            f"*Date:* {execution_date}\n"
            f"<{log_url}|View logs>"
        )
    }
    requests.post(webhook_url, json=message)

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

    run_dbt_task = BashOperator(
        task_id="run_dbt_build",
        bash_command=(
            "which docker || (apt-get update -qq && apt-get install -y -qq docker.io); "
            "docker exec airflow-dbt-1 dbt build --project-dir /opt/dbt_project"
        ),
    )


    generate_data_task >> load_to_snowflake_task
    generate_clickstream_task >> load_clickstream_task
    [load_to_snowflake_task, load_clickstream_task] >> run_dbt_task