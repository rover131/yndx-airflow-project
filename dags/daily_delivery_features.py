"""Учебный DAG для кейса о запуске пайплайна в продакшн-режиме."""

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.utils.task_group import TaskGroup


@task
def load_source(source: str) -> str:
    """Имитирует загрузку одного источника в партицию запуска."""
    context = get_current_context()

    # Используем интервал запуска DAG, а не текущую дату на сервере.
    interval_start = context["data_interval_start"]
    interval_end = context["data_interval_end"]
    run_day = interval_start.format("YYYY-MM-DD")

    # Повторный запуск запишет результат в ту же партицию.
    source_path = f"s3://delivery-data/raw/{source}/dt={run_day}"
    print(
        f"Загружаем {source} за интервал "
        f"{interval_start} - {interval_end} в {source_path}"
    )
    return source_path


@task
def merge_sources(
    orders_path: str,
    shifts_path: str,
    weather_path: str,
) -> str:
    """Имитирует объединение трёх снимков данных."""
    context = get_current_context()
    run_day = context["data_interval_start"].format("YYYY-MM-DD")
    features_path = (
        f"s3://delivery-data/staging/delivery_features/dt={run_day}"
    )

    print("Объединяем источники:")
    print(orders_path)
    print(shifts_path)
    print(weather_path)
    print(f"Сохраняем витрину в {features_path}")
    return features_path


@task
def publish_features(features_path: str) -> None:
    """Имитирует публикацию витрины признаков."""
    # В рабочем проекте здесь выполнялся бы upsert по order_id.
    print(f"Публикуем витрину из {features_path}")


@dag(
    dag_id="daily_delivery_features",
    description="Ежедневная витрина признаков для прогноза опоздания доставки",
    schedule_interval="0 2 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "ml_team",
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["case", "delivery", "taskflow"],
)
def daily_delivery_features():
    """Описывает зависимости задач через TaskFlow API."""
    with TaskGroup(
        group_id="source_loading",
        tooltip="Параллельная загрузка исходных данных",
    ):
        # override задаёт понятные названия задач в интерфейсе Airflow.
        orders_path = load_source.override(task_id="load_orders")("orders")
        shifts_path = load_source.override(task_id="load_courier_shifts")(
            "courier_shifts"
        )
        weather_path = load_source.override(task_id="load_weather")("weather")

    features_path = merge_sources(
        orders_path,
        shifts_path,
        weather_path,
    )
    publish_features(features_path)


daily_delivery_features()
