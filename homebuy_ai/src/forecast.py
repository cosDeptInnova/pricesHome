from __future__ import annotations

import pandas as pd
from sklearn.linear_model import LinearRegression


def build_forecast_frame(scored_df: pd.DataFrame, horizon_months: int = 6) -> pd.DataFrame:
    df = scored_df.copy()
    df["month"] = pd.to_datetime(df["date"]).dt.to_period("M").dt.to_timestamp()

    monthly = (
        df.groupby(["municipio", "tipologia", "month"], as_index=False)
        .agg(price_per_m2=("price_per_m2", "mean"), buy_score_0_100=("buy_score_0_100", "mean"))
        .sort_values(["municipio", "tipologia", "month"])
    )

    forecasts = []
    for (municipio, tipologia), group in monthly.groupby(["municipio", "tipologia"]):
        g = group.sort_values("month").reset_index(drop=True)
        g["t"] = range(len(g))
        if len(g) < 2:
            continue

        model = LinearRegression()
        X = g[["t"]]
        y = g["price_per_m2"]
        model.fit(X, y)

        start_t = int(g["t"].max()) + 1
        last_month = g["month"].max()
        for i in range(horizon_months):
            future_t = start_t + i
            pred = float(model.predict(pd.DataFrame({"t": [future_t]}))[0])
            forecasts.append(
                {
                    "municipio": municipio,
                    "tipologia": tipologia,
                    "month": (last_month + pd.DateOffset(months=i + 1)),
                    "forecast_price_per_m2": pred,
                    "series": "forecast",
                }
            )

    hist = monthly.rename(columns={"price_per_m2": "forecast_price_per_m2"})[
        ["municipio", "tipologia", "month", "forecast_price_per_m2"]
    ]
    hist["series"] = "historical"

    if not forecasts:
        return hist

    return pd.concat([hist, pd.DataFrame(forecasts)], ignore_index=True)
