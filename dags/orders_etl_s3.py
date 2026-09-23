from datetime import timedelta
from io import StringIO

import pandas as pd
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


RAW_ORDERS_KEY = "orders/raw/orders.csv"


def prepare_paid_orders(output_key):
    # Подключаемся к S3 через Airflow Connection.
    s3_hook = S3Hook(aws_conn_id="yandex_s3")
    source_bucket = Variable.get("source_s3_bucket")
    personal_bucket = Variable.get("personal_s3_bucket")

    # Читаем исходную выгрузку и оставляем оплаченные заказы.
    raw_orders_csv = s3_hook.read_key(
        key=RAW_ORDERS_KEY,
        bucket_name=source_bucket,
    )
    orders = pd.read_csv(StringIO(raw_orders_csv))
    paid_orders = orders.loc[
        orders["status"] == "paid",
        ["order_id", "amount", "status"],
    ]

    # Сохраняем крупный результат в S3, а не в XCom.
    s3_hook.load_string(
        string_data=paid_orders.to_csv(index=False),
        key=output_key,
        bucket_name=personal_bucket,
        replace=True,
    )
    return output_key


def calculate_order_summary(ti):
    # Получаем из XCom только ключ объекта в S3.
    processed_orders_key = ti.xcom_pull(task_ids="prepare_paid_orders")

    s3_hook = S3Hook(aws_conn_id="yandex_s3")
    personal_bucket = Variable.get("personal_s3_bucket")

    # Загружаем данные по ключу и рассчитываем итоговые показатели.
    paid_orders_csv = s3_hook.read_key(
        key=processed_orders_key,
        bucket_name=personal_bucket,
    )
    paid_orders = pd.read_csv(StringIO(paid_orders_csv))
    summary = {
        "paid_orders_count": len(paid_orders),
        "total_revenue": float(paid_orders["amount"].sum()),
    }
    print(summary)
    return summary


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

prepare_task = PythonOperator(
    task_id="prepare_paid_orders",
    python_callable=prepare_paid_orders,
    op_kwargs={
        "output_key": "orders/processed/paid_orders_{{ ds_nodash }}.csv",
    },
    dag=dag,
)

summary_task = PythonOperator(
    task_id="calculate_order_summary",
    python_callable=calculate_order_summary,
    dag=dag,
)

prepare_task >> summary_task
