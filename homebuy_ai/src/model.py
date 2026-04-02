from dataclasses import dataclass

import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split


@dataclass
class ModelResult:
    model: RandomForestRegressor
    mae: float
    feature_cols: list


def train_price_model(df: pd.DataFrame, cfg: dict) -> ModelResult:
    target_col = cfg["model"]["target_col"]
    feature_cols = [
        "m2",
        "rooms",
        "garage",
        "ascensor",
        "euribor_12m",
        "inflation_yoy",
        "unemployment_rate",
        "ibex_monthly_change_pct",
        "news_volume",
        "news_keyword_ratio",
        "news_sentiment",
    ]
    X = df[feature_cols].fillna(0)
    y = df[target_col]

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=cfg["model"]["test_size"],
        random_state=cfg["model"]["random_state"],
    )

    model = RandomForestRegressor(
        n_estimators=300,
        random_state=cfg["model"]["random_state"],
    )
    model.fit(X_train, y_train)
    preds = model.predict(X_test)
    mae = mean_absolute_error(y_test, preds)
    return ModelResult(model=model, mae=float(mae), feature_cols=feature_cols)


def infer_fair_price_per_m2(df: pd.DataFrame, model_result: ModelResult) -> pd.Series:
    X = df[model_result.feature_cols].fillna(0)
    return pd.Series(model_result.model.predict(X), index=df.index, name="fair_price_per_m2")
