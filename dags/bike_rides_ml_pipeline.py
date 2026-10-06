from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator


def train_model():
    # Загружаем код обучения при выполнении задачи.
    from bike_rides_ml import run_from_s3

    return run_from_s3()


# Создаём DAG для ручного запуска обучения.
dag = DAG(
    dag_id="bike_rides_ml_pipeline",
    description="Очистка поездок и обучение модели длительности",
    start_date=datetime(2026, 9, 1, tzinfo=timezone.utc),
    schedule_interval=None,
    catchup=False,
    tags=["bike_rides", "ml"],
)

# Задача вызывает функцию загрузки и обучения.
train_task = PythonOperator(
    task_id="train_model",
    python_callable=train_model,
    dag=dag,
)
