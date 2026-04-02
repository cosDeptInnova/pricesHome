from dataclasses import dataclass

import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split


@dataclass
class ModelResult:
    global_model: RandomForestRegressor
    segment_models: dict
    mae: float
    feature_cols: list


def _build_feature_columns() -> list:
    return [
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
        "hist_series_count",
        "hist_latest_mean",
        "hist_trend_6m_mean",
    ]


def train_price_model(df: pd.DataFrame, cfg: dict) -> ModelResult:
    target_col = cfg["model"]["target_col"]
    feature_cols = _build_feature_columns()
    X = df[feature_cols].fillna(0)
    y = df[target_col]

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=cfg["model"]["test_size"],
        random_state=cfg["model"]["random_state"],
    )

    global_model = RandomForestRegressor(
        n_estimators=300,
        random_state=cfg["model"]["random_state"],
    )
    global_model.fit(X_train, y_train)
    preds = global_model.predict(X_test)
    mae = mean_absolute_error(y_test, preds)

    segment_models = {}
    min_rows = int(cfg["model"].get("segment_min_rows", 12))
    for (municipio, tipologia), group in df.groupby(["municipio", "tipologia"]):
        if len(group) < min_rows:
            continue
        X_seg = group[feature_cols].fillna(0)
        y_seg = group[target_col]
        m = RandomForestRegressor(
            n_estimators=200,
            random_state=cfg["model"]["random_state"],
        )
        m.fit(X_seg, y_seg)
        segment_models[(municipio, tipologia)] = m

    return ModelResult(
        global_model=global_model,
        segment_models=segment_models,
        mae=float(mae),
        feature_cols=feature_cols,
    )


def infer_fair_price_per_m2(df: pd.DataFrame, model_result: ModelResult) -> pd.Series:
    X_all = df[model_result.feature_cols].fillna(0)
    preds = model_result.global_model.predict(X_all)
    out = pd.Series(preds, index=df.index, name="fair_price_per_m2")

    for (municipio, tipologia), model in model_result.segment_models.items():
        mask = (df["municipio"] == municipio) & (df["tipologia"] == tipologia)
        if not mask.any():
            continue
        out.loc[mask] = model.predict(X_all.loc[mask])

    return out
