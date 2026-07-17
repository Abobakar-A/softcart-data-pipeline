from datetime import datetime, timedelta
import csv
import io
import json
import random

from airflow.hooks.base import BaseHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
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


def generate_and_upload_returns(**context):
    from azure.storage.filedatalake import DataLakeServiceClient

    # Pull real order_ids AND their order_total from Snowflake, so every return
    # references a real order and its refund_amount can never exceed the order total.
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    order_rows = hook.get_records(
        """
        SELECT o.order_id, o.order_total
        FROM softcart_db.raw.orders o
        INNER JOIN softcart_db.raw.order_items oi
            ON o.order_id = oi.order_id
        GROUP BY o.order_id, o.order_total
        ORDER BY MAX(o.order_date) DESC
        LIMIT 500
        """
    )
    order_totals = {row[0]: float(row[1]) for row in order_rows}
    real_order_ids = list(order_totals.keys())

    if not real_order_ids:
        print("No orders found yet, skipping returns generation for this run.")
        return

    conn = BaseHook.get_connection("azure_data_lake_default")
    connection_string = conn.extra_dejson.get("connection_string")
    service_client = DataLakeServiceClient.from_connection_string(connection_string)
    file_system_client = service_client.get_file_system_client(file_system="bronze")

    run_date = context["ds_nodash"]
    rows = []
    num_returns = max(1, int(len(real_order_ids) * 0.05))  # ~5% of recent orders get a return

    for i in range(num_returns):
        chosen_order_id = random.choice(real_order_ids)
        order_total = order_totals[chosen_order_id]
        # Refund is a random portion of the order, never more than the order total itself
        max_refund = max(10, min(300, order_total))
        refund_amount = round(random.uniform(10, max_refund), 2)

        rows.append({
            "return_id": int(run_date) * 1000 + i,
            "order_id": chosen_order_id,
            "return_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "reason": random.choice(["damaged", "wrong_item", "no_longer_needed", "defective", "size_issue"]),
            "status": random.choices(
                ["requested", "approved", "rejected", "refunded"], weights=[0.2, 0.3, 0.1, 0.4]
            )[0],
            "refund_amount": refund_amount,
        })

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

    file_name = f"returns/returns_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    file_client = file_system_client.get_file_client(file_name)
    file_client.upload_data(buffer.getvalue(), overwrite=True)

    print(f"Uploaded {len(rows)} returns to {file_name}")
    context["ti"].xcom_push(key="uploaded_returns_file", value=file_name)