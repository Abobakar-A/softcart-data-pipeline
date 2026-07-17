import json

import requests
from airflow.models import Variable


def slack_failure_alert(context):
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


def slack_test_summary(**context):
    webhook_url = Variable.get("slack_webhook_url")

    with open("/opt/dbt_project/target/run_results.json") as f:
        results = json.load(f)

    test_results = [r for r in results["results"] if r["unique_id"].startswith("test.")]
    passed = sum(1 for r in test_results if r["status"] == "pass")
    total = len(test_results)
    failed = total - passed

    icon = ":white_check_mark:" if failed == 0 else ":warning:"
    message = {
        "text": (
            f"{icon} *dbt Test Summary*\n"
            f"Passed: {passed}/{total}\n"
            f"Failed: {failed}"
        )
    }
    requests.post(webhook_url, json=message)