import pandas as pd


def filter_listings(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    f = cfg["filters"]
    must_have = f["must_have"]

    out = df.copy()
    out["price_per_m2"] = out["price"] / out["m2"]

    out = out[(out["m2"] >= f["min_m2"]) & (out["price"] <= f["max_price"])]
    out = out[out["municipio"].isin(f["municipios_prioritarios"])]

    for feat in must_have:
        if feat in out.columns:
            out = out[out[feat] == 1]

    return out


def build_training_frame(
    listings: pd.DataFrame, macro_df: pd.DataFrame, news_df: pd.DataFrame
) -> pd.DataFrame:
    latest_macro = macro_df.iloc[0].to_dict()
    latest_news = news_df.iloc[0].to_dict()

    df = listings.copy()
    for k, v in latest_macro.items():
        df[k] = v
    for k, v in latest_news.items():
        if k != "snapshot_utc":
            df[k] = v

    return df
