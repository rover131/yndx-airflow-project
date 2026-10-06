import pandas as pd


def load_rides():
    # Возвращаем четыре записи; PythonOperator передаст список через XCom.
    return [
        {"ride_id": 501, "station": "  Центр ", "duration_min": 12},
        {"ride_id": 502, "station": "Парк", "duration_min": None},
        {"ride_id": 501, "station": "  Центр ", "duration_min": 12},
        {"ride_id": 503, "station": " Вокзал ", "duration_min": 25},
    ]


def transform_rides(ti):
    # Получаем список, который вернула задача load_rides.
    rides = ti.xcom_pull(task_ids="load_rides")
    transformed = []

    for ride in rides:
        # Копируем запись, чтобы не менять исходный словарь.
        updated = ride.copy()
        # Убираем пробелы по краям и приводим название к нижнему регистру.
        updated["station"] = updated["station"].strip().lower()
        transformed.append(updated)

    # Airflow сохранит новый список в XCom под ключом return_value.
    return transformed


def clean_data(df: pd.DataFrame, required_columns, duplicate_columns) -> pd.DataFrame:
    # Копируем таблицу, чтобы не менять входной DataFrame.
    cleaned = df.copy()
    # Удаляем строки с пропуском хотя бы в одном обязательном столбце.
    cleaned = cleaned.dropna(subset=required_columns)
    # Для повторного номера поездки оставляем первую запись.
    cleaned = cleaned.drop_duplicates(subset=duplicate_columns)
    # Перенумеровываем строки после удаления.
    return cleaned.reset_index(drop=True)


def clean_rides(ti):
    # Получаем короткий список записей из предыдущей задачи.
    rides = ti.xcom_pull(task_ids="transform_rides")
    # Превращаем список словарей в таблицу для функции clean_data.
    data = pd.DataFrame(rides)
    cleaned = clean_data(
        data,
        required_columns=["ride_id", "duration_min"],
        duplicate_columns=["ride_id"],
    )

    print(f"Поездок до очистки: {len(data)}")
    print(f"Поездок после очистки: {len(cleaned)}")
    # Возвращаем небольшой список, который Airflow сможет передать дальше.
    return cleaned.to_dict(orient="records")


def save_summary(ti):
    # Получаем только те поездки, которые прошли очистку.
    rides = ti.xcom_pull(task_ids="clean_rides")
    # Считаем поездки и их общую длительность.
    summary = {
        "rides_count": len(rides),
        "total_minutes": int(sum(ride["duration_min"] for ride in rides)),
    }
    print(summary)
    # Сводка также сохранится в XCom под ключом return_value.
    return summary
