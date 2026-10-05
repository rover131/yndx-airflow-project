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
