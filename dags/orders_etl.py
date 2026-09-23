"""Учебный ETL-DAG: загрузка заказов, расчёт метрик и сохранение результата."""

import json
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


def extract_orders(ti):
    """Возвращает небольшую учебную выгрузку заказов через XCom."""
    orders = [
        {"order_id": 101, "amount": 1250, "status": "paid"},
        {"order_id": 102, "amount": 980, "status": "cancelled"},
        {"order_id": 103, "amount": 740, "status": "paid"},
        {"order_id": 104, "amount": -50, "status": "paid"},
        {"order_id": 105, "amount": 1600, "status": "paid"},
    ]

    # Именованный XCom пригодится следующей задаче вместе с return_value.
    ti.xcom_push(key="batch_id", value="demo_orders_2026_09")
    return orders


def transform_orders(ti):
    """Оставляет корректные оплаченные заказы и считает дневные метрики."""
    orders = ti.xcom_pull(task_ids="extract_orders")
    paid_orders = [
        order
        for order in orders
        if order["status"] == "paid" and order["amount"] > 0
    ]

    total_revenue = sum(order["amount"] for order in paid_orders)
    return {
        "paid_orders_count": len(paid_orders),
        "total_revenue": total_revenue,
        "average_order_value": (
            round(total_revenue / len(paid_orders), 2) if paid_orders else 0.0
        ),
    }


def load_summary(ti):
    """Сохраняет итог в XCom и выводит его в лог задачи."""
    summary = ti.xcom_pull(task_ids="transform_orders")
    batch_id = ti.xcom_pull(task_ids="extract_orders", key="batch_id")
    result = {"batch_id": batch_id, **summary}

    # В следующем гайде эту операцию можно заменить записью результата в S3.
    ti.xcom_push(key="etl_result", value=result)
    print(json.dumps(result, ensure_ascii=False, indent=2))


default_args = {
    "owner": "student",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


dag = DAG(
    dag_id="orders_etl",
    description="Учебный ETL-процесс для ежедневной обработки заказов",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "guide"],
)

extract_task = PythonOperator(
    task_id="extract_orders",
    python_callable=extract_orders,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_orders",
    python_callable=transform_orders,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_summary",
    python_callable=load_summary,
    dag=dag,
)

extract_task >> transform_task >> load_task
