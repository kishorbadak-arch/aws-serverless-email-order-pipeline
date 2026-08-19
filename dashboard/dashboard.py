import streamlit as st
import boto3
import time
import pandas as pd
from botocore.exceptions import ClientError

athena = boto3.client("athena", region_name="ap-south-1")
DB = "email_pipeline"
OUTPUT = "s3://<YOUR-ATHENA-RESULTS-BUCKET>/"  # <-- REPLACE with your athena-results bucket name


@st.cache_data(ttl=300)  # Athena bills per TB scanned — cache instead of
                          # re-running the same query on every UI interaction
def run_query(sql: str, max_wait_seconds: int = 60) -> pd.DataFrame:
    try:
        qid = athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": DB},
            ResultConfiguration={"OutputLocation": OUTPUT},
        )["QueryExecutionId"]
    except ClientError as e:
        st.error(f"Could not start query: {e}")
        return pd.DataFrame()

    waited = 0
    status = "RUNNING"
    while waited < max_wait_seconds:
        resp = athena.get_query_execution(QueryExecutionId=qid)
        status = resp["QueryExecution"]["Status"]["State"]
        if status in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
        waited += 1

    if status != "SUCCEEDED":
        reason = resp["QueryExecution"]["Status"].get("StateChangeReason", "timed out")
        st.error(f"Query {status}: {reason}")
        return pd.DataFrame()

    try:
        return pd.read_csv(f"{OUTPUT}{qid}.csv")
    except Exception as e:
        st.error(f"Query succeeded but results couldn't be read: {e}")
        return pd.DataFrame()


st.title("Order Pipeline Dashboard")

rev = run_query(
    "SELECT order_date_clean, SUM(quantity*unit_price) AS revenue "
    "FROM orders_gold GROUP BY order_date_clean ORDER BY order_date_clean"
)
if not rev.empty:
    st.line_chart(rev.set_index("order_date_clean"))
else:
    st.info("No revenue data yet")

cat = run_query(
    "SELECT category, SUM(quantity*unit_price) AS revenue "
    "FROM orders_gold GROUP BY category"
)
if not cat.empty:
    st.bar_chart(cat.set_index("category"))
else:
    st.info("No category data yet")
