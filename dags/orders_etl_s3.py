from datetime import timedelta
from io import StringIO

import boto3
import pandas as pd
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=Variable.get("s3_endpoint"),
        aws_access_key_id=Variable.get("s3_access_key"),
        aws_secret_access_key=Variable.get("s3_secret_key"),
    )


def extract_orders(raw_orders_key):
    orders = [
        {"order_id": 101, "amount": 1250, "status": "paid"},
        {"order_id": 102, "amount": 980, "status": "cancelled"},
        {"order_id": 103, "amount": 740, "status": "paid"},
        {"order_id": 104, "amount": 1600, "status": "paid"},
    ]

    s3_client = get_s3_client()
    bucket_name = Variable.get("s3_bucket")

    # Сохраняем исходную выгрузку в S3.
    orders_csv = pd.DataFrame(orders).to_csv(index=False)
    s3_client.put_object(
        Bucket=bucket_name,
        Key=raw_orders_key,
        Body=orders_csv.encode("utf-8"),
    )
    return raw_orders_key  # В XCom попадёт только ключ


def transform_orders(processed_orders_key, ti):
    raw_orders_key = ti.xcom_pull(task_ids="extract_orders")

    s3_client = get_s3_client()
    bucket_name = Variable.get("s3_bucket")

    # Читаем исходные данные по ключу из XCom.
    response = s3_client.get_object(Bucket=bucket_name, Key=raw_orders_key)
    orders_csv = response["Body"].read().decode("utf-8")
    orders = pd.read_csv(StringIO(orders_csv))
    paid_orders = orders.loc[orders["status"] == "paid"]

    # Сохраняем обработанный датасет отдельно.
    s3_client.put_object(
        Bucket=bucket_name,
        Key=processed_orders_key,
        Body=paid_orders.to_csv(index=False).encode("utf-8"),
    )
    return processed_orders_key  # Передаём ключ следующей задаче


def load_summary(ti):
    processed_orders_key = ti.xcom_pull(task_ids="transform_orders")

    s3_client = get_s3_client()
    bucket_name = Variable.get("s3_bucket")

    # Загружаем оплаченные заказы и считаем показатели.
    response = s3_client.get_object(
        Bucket=bucket_name,
        Key=processed_orders_key,
    )
    paid_orders_csv = response["Body"].read().decode("utf-8")
    paid_orders = pd.read_csv(StringIO(paid_orders_csv))
    summary = {
        "paid_orders_count": len(paid_orders),
        "total_revenue": float(paid_orders["amount"].sum()),
    }

    ti.xcom_push(key="etl_result", value=summary)
    print(summary)


default_args = {
    "owner": "ml_team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


dag = DAG(
    dag_id="orders_etl_s3",
    description="Ежедневная обработка заказов с хранением данных в S3",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "orders", "s3"],
)

extract_task = PythonOperator(
    task_id="extract_orders",
    python_callable=extract_orders,
    op_kwargs={
        "raw_orders_key": "orders/raw/orders_{{ ds_nodash }}.csv",
    },
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_orders",
    python_callable=transform_orders,
    op_kwargs={
        "processed_orders_key": (
            "orders/processed/paid_orders_{{ ds_nodash }}.csv"
        ),
    },
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_summary",
    python_callable=load_summary,
    dag=dag,
)

extract_task >> transform_task >> load_task
