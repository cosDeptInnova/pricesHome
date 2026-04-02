from __future__ import annotations

from pathlib import Path

import pandas as pd


SERIES_RENAME = {
    "value": "series_value",
    "valor": "series_value",
    "fecha": "date",
    "region": "scope",
    "municipio": "scope",
}


def load_historical_series(cfg: dict) -> pd.DataFrame:
    files = cfg.get("historical", {}).get("sources", [])
    if not files:
        return pd.DataFrame(columns=["source", "indicator", "scope", "date", "series_value"])

    frames = []
    for item in files:
        path = Path(item["path"])
        if not path.exists():
            continue
        source = item.get("source", path.stem)
        indicator = item.get("indicator", path.stem)
        scope = item.get("scope", "Comunidad de Madrid")

        df = pd.read_csv(path)
        df = df.rename(columns={k: v for k, v in SERIES_RENAME.items() if k in df.columns})

        if "date" not in df.columns or "series_value" not in df.columns:
            continue

        out = df[["date", "series_value"]].copy()
        out["source"] = source
        out["indicator"] = indicator
        out["scope"] = df["scope"] if "scope" in df.columns else scope
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        out["series_value"] = pd.to_numeric(out["series_value"], errors="coerce")
        out = out.dropna(subset=["date", "series_value"])
        frames.append(out)

    if not frames:
        return pd.DataFrame(columns=["source", "indicator", "scope", "date", "series_value"])

    return pd.concat(frames, ignore_index=True).sort_values(["indicator", "scope", "date"])


def build_historical_features(historical_df: pd.DataFrame) -> dict:
    if historical_df.empty:
        return {
            "hist_series_count": 0,
            "hist_latest_mean": 0.0,
            "hist_trend_6m_mean": 0.0,
        }

    latest_rows = historical_df.sort_values("date").groupby(["indicator", "scope"], as_index=False).tail(1)
    latest_mean = float(latest_rows["series_value"].mean()) if not latest_rows.empty else 0.0

    trend_values = []
    for _, group in historical_df.groupby(["indicator", "scope"]):
        g = group.sort_values("date")
        if len(g) < 2:
            continue
        tail = g.tail(7)
        first = float(tail["series_value"].iloc[0])
        last = float(tail["series_value"].iloc[-1])
        trend_values.append(last - first)

    trend_mean = float(sum(trend_values) / len(trend_values)) if trend_values else 0.0

    return {
        "hist_series_count": int(historical_df[["indicator", "scope"]].drop_duplicates().shape[0]),
        "hist_latest_mean": latest_mean,
        "hist_trend_6m_mean": trend_mean,
    }
