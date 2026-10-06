from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

# Функции обработки лежат отдельно от описания DAG.
from bike_rides_processing import (
    clean_rides,
    load_rides,
    save_summary,
    transform_rides,
)


# Создаём DAG для ручного запуска.
dag = DAG(
    dag_id="bike_rides_etl",
    description="Подготовка данных о поездках на велосипедах",
    start_date=datetime(2026, 9, 1, tzinfo=timezone.utc),
    schedule_interval=None,
    catchup=False,
)

# Каждая задача вызывает одну функцию из файла обработки.
load_task = PythonOperator(
    task_id="load_rides",
    python_callable=load_rides,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_rides",
    python_callable=transform_rides,
    dag=dag,
)

# Новый оператор вызывает обёртку, которая передаёт данные в clean_data.
clean_task = PythonOperator(
    task_id="clean_rides",
    python_callable=clean_rides,
    dag=dag,
)

save_task = PythonOperator(
    task_id="save_summary",
    python_callable=save_summary,
    dag=dag,
)

# Очистка выполняется после преобразования и до расчёта сводки.
load_task >> transform_task >> clean_task >> save_task
