from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


def extract_orders(ti):
    # Получаем очередную выгрузку заказов.
    orders = [
        {"order_id": 101, "amount": 1250, "status": "paid"},
        {"order_id": 102, "amount": 980, "status": "cancelled"},
        {"order_id": 103, "amount": 740, "status": "paid"},
        {"order_id": 104, "amount": 1600, "status": "paid"},
    ]
    # Сохраняем идентификатор загрузки и передаём заказы следующей задаче.
    ti.xcom_push(key="batch_id", value="daily_orders")
    return orders


def transform_orders(ti):
    # Забираем заказы из XCom и оставляем только оплаченные.
    orders = ti.xcom_pull(task_ids="extract_orders")
    paid_orders = [order for order in orders if order["status"] == "paid"]
    return {
        "paid_orders_count": len(paid_orders),
        "total_revenue": sum(order["amount"] for order in paid_orders),
    }


def load_summary(ti):
    # Получаем рассчитанные показатели и идентификатор загрузки.
    summary = ti.xcom_pull(task_ids="transform_orders")
    batch_id = ti.xcom_pull(task_ids="extract_orders", key="batch_id")
    result = {"batch_id": batch_id, **summary}
    # Сохраняем итог и выводим его в лог задачи.
    ti.xcom_push(key="etl_result", value=result)
    print(result)


# Повторяем задачу после временного сбоя не более двух раз.
default_args = {
    "owner": "student",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# Создаём DAG и задаём ежедневное расписание.
dag = DAG(
    dag_id="orders_etl",
    description="Ежедневная обработка заказов",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "orders"],
)

# Превращаем функции Python в задачи Airflow.
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

# Указываем порядок выполнения задач.
extract_task >> transform_task >> load_task
