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


def save_summary(ti):
    # Получаем поездки после преобразования названий станций.
    rides = ti.xcom_pull(task_ids="transform_rides")
    # Пока считаем все записи, подставляя ноль вместо пропуска.
    summary = {
        "rides_count": len(rides),
        "total_minutes": sum(ride["duration_min"] or 0 for ride in rides),
    }
    print(summary)
    # Сводка также сохранится в XCom под ключом return_value.
    return summary
