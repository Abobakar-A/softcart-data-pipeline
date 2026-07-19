def slack_ai_investigate_failure(**context):
    """
    When a dbt test fails, this uses a Gemini-powered agent to investigate
    the failure by running follow-up SQL queries against Snowflake, then
    posts a plain-English summary to Slack.
    """
    import json
    import requests
    from google import genai
    from google.genai import types
    from airflow.models import Variable
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    # Load the dbt run results to find which tests failed
    with open("/opt/dbt_project/target/run_results.json") as f:
        results = json.load(f)

    failed_tests = [
        r for r in results["results"]
        if r["unique_id"].startswith("test.") and r["status"] == "fail"
    ]

    if not failed_tests:
        print("No failed tests - AI investigation skipped.")
        return

    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

    def run_sql_query(query: str) -> str:
        """Runs a read-only SQL query against Snowflake and returns the results as text."""
        try:
            rows = hook.get_records(query)
            return str(rows[:20])  # cap output size
        except Exception as e:
            return f"Query error: {e}"

    gemini_key = Variable.get("gemini_api_key")
    client = genai.Client(api_key=gemini_key)

    run_sql_tool = types.Tool(function_declarations=[
        types.FunctionDeclaration(
            name="run_sql_query",
            description="Run a read-only SQL query against the Snowflake warehouse to investigate a data issue.",
            parameters=types.Schema(
                type="OBJECT",
                properties={"query": types.Schema(type="STRING", description="The SQL query to run")},
                required=["query"],
            ),
        )
    ])

    failed_test_names = [t["unique_id"] for t in failed_tests]
    prompt = (
        f"The following dbt data quality tests failed: {failed_test_names}\n\n"
        f"Investigate by running SQL queries against the softcart_db Snowflake database "
        f"(schemas: raw, staging, marts) to find the likely root cause. "
        f"Then give a short, plain-English summary (3-4 sentences max) suitable for a Slack alert."
    )

    contents = [types.Content(role="user", parts=[types.Part(text=prompt)])]

    for _ in range(5):  # cap agent loop iterations
        response = client.models.generate_content(
            model="gemini-flash-latest",
            contents=contents,
            config=types.GenerateContentConfig(tools=[run_sql_tool]),
        )

        candidate = response.candidates[0]
        contents.append(candidate.content)

        function_calls = [
            part.function_call for part in candidate.content.parts if part.function_call
        ]

        if not function_calls:
            # Model gave a final text answer
            final_text = candidate.content.parts[0].text
            break

        for call in function_calls:
            query = call.args.get("query", "")
            result = run_sql_query(query)
            contents.append(types.Content(
                role="user",
                parts=[types.Part(function_response=types.FunctionResponse(
                    name="run_sql_query", response={"result": result}
                ))]
            ))
    else:
        final_text = "AI investigation did not converge to a conclusion within the step limit."

    webhook_url = Variable.get("slack_webhook_url")
    message = {
        "text": (
            f":robot_face: *AI Investigation: dbt Test Failure*\n"
            f"Failed tests: {', '.join(failed_test_names)}\n\n"
            f"{final_text}"
        )
    }
    requests.post(webhook_url, json=message)