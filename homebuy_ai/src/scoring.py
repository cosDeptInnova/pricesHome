import pandas as pd


def normalize(series: pd.Series) -> pd.Series:
    smin, smax = series.min(), series.max()
    if smax == smin:
        return pd.Series([0.5] * len(series), index=series.index)
    return (series - smin) / (smax - smin)


def _describe_driver(row: pd.Series) -> str:
    components = {
        "valoración": row["valuation_gap_component"],
        "accesibilidad macro": row["macro_affordability_component"],
        "presión oferta": row["inventory_pressure_component"],
        "señal noticias": row["news_sentiment_component"],
    }
    top_factor = max(components, key=components.get)
    return (
        f"Factor dominante: {top_factor}. "
        f"Gap valoración={row['valuation_gap']:.2%}, "
        f"fair €/m2={row['fair_price_per_m2']:.0f}, anuncio €/m2={row['price_per_m2']:.0f}."
    )


def compute_buy_score(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    weights = cfg["scoring"]["weights"]
    out = df.copy()

    out["valuation_gap"] = (out["fair_price_per_m2"] - out["price_per_m2"]) / out[
        "fair_price_per_m2"
    ].clip(lower=1)
    out["valuation_gap_n"] = normalize(out["valuation_gap"])

    macro_stress = out["euribor_12m"] + out["inflation_yoy"] + out["unemployment_rate"] / 2
    out["macro_affordability_n"] = 1 - normalize(macro_stress)

    supply = out.groupby("municipio")["listing_id"].transform("count")
    out["inventory_pressure_n"] = normalize(supply)

    out["news_sentiment_n"] = 1 - normalize(out["news_sentiment"])

    out["valuation_gap_component"] = 100 * weights["valuation_gap"] * out["valuation_gap_n"]
    out["macro_affordability_component"] = 100 * weights["macro_affordability"] * out["macro_affordability_n"]
    out["inventory_pressure_component"] = 100 * weights["inventory_pressure"] * out["inventory_pressure_n"]
    out["news_sentiment_component"] = 100 * weights["news_sentiment"] * out["news_sentiment_n"]

    out["buy_score_0_100"] = (
        out["valuation_gap_component"]
        + out["macro_affordability_component"]
        + out["inventory_pressure_component"]
        + out["news_sentiment_component"]
    )

    buy_thr = cfg["scoring"]["thresholds"]["buy"]
    wait_thr = cfg["scoring"]["thresholds"]["wait"]

    def label(score: float) -> str:
        if score >= buy_thr:
            return "BUY_WINDOW"
        if score >= wait_thr:
            return "NEUTRAL"
        return "WAIT"

    out["recommendation"] = out["buy_score_0_100"].apply(label)
    out["decision_rationale"] = out.apply(_describe_driver, axis=1)
    return out
