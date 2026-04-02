import pandas as pd


def load_macro_snapshot(cfg: dict) -> pd.DataFrame:
    m = cfg["macro"]
    return pd.DataFrame([
        {
            "euribor_12m": float(m["euribor_12m"]),
            "inflation_yoy": float(m["inflation_yoy"]),
            "unemployment_rate": float(m["unemployment_rate"]),
            "ibex_monthly_change_pct": float(m["ibex_monthly_change_pct"]),
        }
    ])
