import json
from io import StringIO
from pathlib import Path

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, OneHotEncoder

from bike_rides_processing import clean_data


def load_config():
    # Находим JSON-файл рядом с кодом обработки.
    config_path = Path(__file__).with_name("bike_rides_ml_config.json")
    with config_path.open(encoding="utf-8") as file:
        # Получаем параметры в виде словаря.
        return json.load(file)


def train_and_evaluate(data, config):
    # Удаляем строки до отделения целевого признака.
    cleaned = clean_data(
        data,
        required_columns=config["required_features"] + [config["target"]],
        duplicate_columns=config["duplicate_columns"],
    )
    # Отделяем признаки от длительности поездки.
    X = cleaned[config["required_features"]]
    y = cleaned[config["target"]]

    # Оставляем часть поездок для проверки модели.
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=config["test_size"],
        random_state=config["random_state"],
    )

    # Оборачиваем готовую функцию в шаг sklearn Pipeline.
    clean_step = FunctionTransformer(
        clean_data,
        kw_args={
            "required_columns": config["required_features"],
            "duplicate_columns": config["duplicate_columns"],
        },
        validate=False,
    )
    # Кодируем станции; номер поездки в модель не передаём.
    columns_step = ColumnTransformer(
        transformers=[
            (
                "station",
                OneHotEncoder(handle_unknown="ignore"),
                config["categorical_columns"],
            ),
        ],
    )
    # Выполняем очистку, кодирование и обучение по порядку.
    model = Pipeline(
        steps=[
            ("clean", clean_step),
            ("columns", columns_step),
            ("model", Ridge()),
        ],
    )

    # Обучаем всю цепочку и измеряем ошибку на тестовой выборке.
    model.fit(X_train, y_train)
    predictions = model.predict(X_test)
    mae = mean_absolute_error(y_test, predictions)
    # Возвращаем небольшую сводку для журнала и XCom.
    return {
        "rows_before_cleaning": len(data),
        "rows_after_cleaning": len(cleaned),
        "train_rows": len(X_train),
        "test_rows": len(X_test),
        "mae_minutes": round(float(mae), 2),
    }


def run_from_s3():
    import boto3
    from airflow.models import Variable

    # Читаем путь к CSV и параметры обработки из конфигурации.
    config = load_config()
    # Получаем настройки доступа из Airflow Variables.
    s3 = boto3.client(
        "s3",
        endpoint_url=Variable.get("s3_endpoint"),
        aws_access_key_id=Variable.get("s3_access_key"),
        aws_secret_access_key=Variable.get("s3_secret_key"),
        region_name="ru-central1",
    )
    # Находим объект по имени бакета и ключу из конфигурации.
    response = s3.get_object(
        Bucket=Variable.get("s3_bucket"),
        Key=config["data_key"],
    )
    # Превращаем содержимое CSV в таблицу pandas.
    csv_text = response["Body"].read().decode("utf-8")
    data = pd.read_csv(StringIO(csv_text))
    # Запускаем обучение и возвращаем его результат.
    result = train_and_evaluate(data, config)
    print(result)
    return result
