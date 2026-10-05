# Гайд 1. Как встроить функцию очистки данных в существующий DAG

Время прохождения: 25 минут

Представьте сервис проката велосипедов. После каждой поездки он получает запись с номером поездки, станцией и длительностью. Эти данные уже проходят через DAG: одна задача получает записи, другая приводит названия станций к единому виду, третья считает сводку за день. Но иногда длительность поездки не приходит, а одна поездка попадает в выгрузку дважды. Тогда сводка становится неверной.

Добавим в существующий DAG задачу очистки. Она уберёт записи без длительности и повторные записи, прежде чем следующая задача посчитает результат. В конце запустим обновлённый DAG и проверим не только статусы задач, но и данные после очистки.

Сейчас цепочка выглядит так:

```text
load_rides → transform_rides → save_summary
```

После изменения между преобразованием и расчётом появится `clean_rides`.

## Шаг 1. Отделим обработку данных от порядка запуска

DAG описывает, **какие задачи есть и в каком порядке они выполняются**. Это оркестрация. Сама очистка отвечает на другой вопрос: **какие строки удалить из таблицы**. Поэтому правила очистки запишем в обычной функции Python, которую можно вызвать и без Airflow.

Будут нужны два файла в каталоге `dags`:

```text
dags/
├── bike_rides_processing.py  # функции работы с данными
└── bike_rides_etl.py         # задачи и их зависимости
```

Между задачами передадим небольшие списки записей через XCom. Вы уже использовали этот механизм: значение, возвращённое задачей `PythonOperator`, Airflow сохраняет под ключом `return_value`. Следующая задача получает его через `ti.xcom_pull()`. Здесь в списке всего четыре записи. Для больших таблиц XCom не подходит — их хранят отдельно, а между задачами передают путь или ключ объекта.

## Шаг 2. Превратим разовую очистку в функцию

Посмотрим на входные данные. Вторая поездка не содержит длительности, а первая записана два раза:

```python
[
    {"ride_id": 501, "station": "  Центр ", "duration_min": 12},
    {"ride_id": 502, "station": "Парк", "duration_min": None},
    {"ride_id": 501, "station": "  Центр ", "duration_min": 12},
    {"ride_id": 503, "station": " Вокзал ", "duration_min": 25},
]
```

В ноутбуке мы могли бы один раз применить к таблице `dropna()` и `drop_duplicates()`. Чтобы использовать эту обработку в разных местах, оформим её как функцию в файле `bike_rides_processing.py`:

```python
import pandas as pd


def clean_data(df: pd.DataFrame, required_columns, duplicate_columns) -> pd.DataFrame:
    # Очищаем копию, сохраняя исходную таблицу.
    cleaned = df.copy()
    cleaned = cleaned.dropna(subset=required_columns)
    cleaned = cleaned.drop_duplicates(subset=duplicate_columns)
    return cleaned.reset_index(drop=True)
```

На входе функция получает `DataFrame`. Параметр `required_columns` задаёт столбцы, в которых нельзя оставлять пропуски, а `duplicate_columns` — столбцы, по которым определяются повторы. На выходе снова получается `DataFrame`. Исходная таблица не меняется, файлы не читаются и не записываются. Airflow в этой функции тоже нет.

Для поездок передадим в `required_columns` названия `ride_id` и `duration_min`, а в `duplicate_columns` — `ride_id`. При повторе одного номера останется первая запись. Если две записи с одним номером могут различаться, правило выбора потребуется определить отдельно.

## Шаг 3. Вызовем очистку из задачи

Функция `clean_data()` готова, но она ждёт `DataFrame`. Предыдущая задача передаёт через XCom список словарей. Нужна короткая функция `clean_rides()`, которая соединит эти две части:

```python
def clean_rides(ti):
    rides = ti.xcom_pull(task_ids="transform_rides")
    data = pd.DataFrame(rides)
    cleaned = clean_data(
        data,
        required_columns=["ride_id", "duration_min"],
        duplicate_columns=["ride_id"],
    )

    print(f"Поездок до очистки: {len(data)}")
    print(f"Поездок после очистки: {len(cleaned)}")
    return cleaned.to_dict(orient="records")
```

`ti` — объект текущего выполнения задачи. Через него мы забираем результат `transform_rides`. Затем превращаем короткий список в таблицу, передаём её функции очистки и возвращаем очищенные записи как список. `PythonOperator` сохранит этот список в XCom, и задача `save_summary` сможет его получить.

Здесь важно различать две функции. `clean_data()` содержит правило обработки и не зависит от Airflow. `clean_rides()` только получает вход задачи, вызывает готовую обработку и передаёт её результат дальше.

## Шаг 4. Вставим новую задачу в граф

Откроем `bike_rides_etl.py` и создадим задачу с помощью `PythonOperator`:

```python
clean_task = PythonOperator(
    task_id="clean_rides",
    python_callable=clean_rides,
    dag=dag,
)
```

`task_id` — имя задачи в интерфейсе Airflow. `python_callable` указывает функцию, которую оператор вызовет при выполнении задачи. Сам оператор не содержит правил очистки.

Теперь изменим зависимость. Раньше после `transform_task` сразу шла `save_task`. Новая цепочка выглядит так:

```python
load_task >> transform_task >> clean_task >> save_task
```

Знак `>>` задаёт порядок запуска. Он не переносит записи сам по себе: для этого функции получают результаты предыдущих задач через XCom.

## Шаг 5. Посмотрим на весь код

Мы добавили новую функцию и задачу по частям. Теперь соберём полный вариант, чтобы было видно, как связаны получение данных, преобразование, очистка и расчёт сводки.

Файл `dags/bike_rides_processing.py`:

```python
import pandas as pd


def load_rides():
    # Получаем небольшую выгрузку поездок.
    return [
        {"ride_id": 501, "station": "  Центр ", "duration_min": 12},
        {"ride_id": 502, "station": "Парк", "duration_min": None},
        {"ride_id": 501, "station": "  Центр ", "duration_min": 12},
        {"ride_id": 503, "station": " Вокзал ", "duration_min": 25},
    ]


def transform_rides(ti):
    rides = ti.xcom_pull(task_ids="load_rides")
    transformed = []

    for ride in rides:
        updated = ride.copy()
        # Приводим названия станций к одному виду.
        updated["station"] = updated["station"].strip().lower()
        transformed.append(updated)

    return transformed


def clean_data(df: pd.DataFrame, required_columns, duplicate_columns) -> pd.DataFrame:
    # Очищаем копию, сохраняя исходную таблицу.
    cleaned = df.copy()
    cleaned = cleaned.dropna(subset=required_columns)
    cleaned = cleaned.drop_duplicates(subset=duplicate_columns)
    return cleaned.reset_index(drop=True)


def clean_rides(ti):
    rides = ti.xcom_pull(task_ids="transform_rides")
    data = pd.DataFrame(rides)
    cleaned = clean_data(
        data,
        required_columns=["ride_id", "duration_min"],
        duplicate_columns=["ride_id"],
    )

    print(f"Поездок до очистки: {len(data)}")
    print(f"Поездок после очистки: {len(cleaned)}")
    return cleaned.to_dict(orient="records")


def save_summary(ti):
    rides = ti.xcom_pull(task_ids="clean_rides")
    summary = {
        "rides_count": len(rides),
        "total_minutes": int(sum(ride["duration_min"] for ride in rides)),
    }
    print(summary)
    return summary
```

Файл `dags/bike_rides_etl.py`:

```python
from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

from bike_rides_processing import (
    clean_rides,
    load_rides,
    save_summary,
    transform_rides,
)


dag = DAG(
    dag_id="bike_rides_etl",
    description="Подготовка данных о поездках на велосипедах",
    start_date=datetime(2026, 9, 1, tzinfo=timezone.utc),
    schedule_interval=None,
    catchup=False,
)

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
```

В DAG-файле остались настройки запуска, объявления задач и зависимости. Работа с записями находится в другом файле. Исходные четыре записи здесь заданы прямо в функции `load_rides()`, чтобы проследить весь путь небольшого значения между задачами. Когда записей много, их обычно получают из источника и хранят вне XCom.

## Шаг 6. Перезапустим DAG и проверим результат

После обновления файлов откройте в Airflow список DAG и найдите `bike_rides_etl`. Перейдите к его графу: между `transform_rides` и `save_summary` должна появиться задача `clean_rides`.

Запустите DAG вручную. Новый запуск создаст новые экземпляры всех четырёх задач, поэтому загрузка и преобразование тоже выполнятся снова. Успешные задачи в уже завершённом запуске сохранят прежние статусы: изменение кода само по себе их не перезапускает.

В новом запуске откройте журнал задачи `clean_rides`. Ожидаемые строки:

```text
Поездок до очистки: 4
Поездок после очистки: 2
```

Затем откройте XCom этой задачи. В результате `return_value` должны остаться поездки `501` и `503`. Их станции уже приведены к виду `центр` и `вокзал`. Поездка `502` удалена из-за отсутствующей длительности, а повтор поездки `501` — как дубликат. После преобразования списка в `DataFrame` длительность может отображаться как `12.0` и `25.0` вместо `12` и `25`: значения от этого не меняются.

Наконец, откройте журнал `save_summary`. Ожидаемый результат:

```text
{'rides_count': 2, 'total_minutes': 37}
```

Если все задачи стали зелёными, но числа в сводке отличаются, проверьте XCom задачи `clean_rides`: он покажет, какие именно записи дошли до расчёта.

## Проверьте себя

1. Выделили очистку в функцию `clean_data()`, которая принимает и возвращает `DataFrame`.
2. Передали правила очистки через параметры функции.
3. Получили короткий список записей из XCom, применили очистку и вернули результат в XCom.
4. Добавили `PythonOperator` для `clean_rides` после преобразования и перед расчётом сводки.
5. Создали новый запуск DAG и проверили журнал, очищенные записи и итоговые показатели.
