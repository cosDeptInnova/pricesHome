from pathlib import Path

import pandas as pd
import streamlit as st

st.set_page_config(page_title="HomeBuy AI", layout="wide")
st.title("🏠 HomeBuy AI - Timing de compra")

out = Path("data/output")
scored_path = out / "scored_all.csv"
brief_path = out / "daily_briefing.txt"
forecast_path = out / "forecast_prices.csv"
historical_path = out / "historical_series.csv"

if not scored_path.exists():
    st.warning("No hay resultados todavía. Ejecuta: python run.py")
    st.stop()

df = pd.read_csv(scored_path)
df["date"] = pd.to_datetime(df["date"])
st.metric("Listings analizados", len(df))
st.metric("Score medio", round(df["buy_score_0_100"].mean(), 2))

st.subheader("Top oportunidades")
st.dataframe(
    df.sort_values("buy_score_0_100", ascending=False)[
        [
            "listing_id",
            "municipio",
            "tipologia",
            "price",
            "m2",
            "price_per_m2",
            "fair_price_per_m2",
            "buy_score_0_100",
            "recommendation",
        ]
    ].head(20),
    use_container_width=True,
)

c1, c2 = st.columns(2)
with c1:
    st.subheader("Distribución score")
    st.bar_chart(df["buy_score_0_100"])

with c2:
    st.subheader("Gap valoración por municipio")
    gap = ((df["fair_price_per_m2"] - df["price_per_m2"]) / df["fair_price_per_m2"].clip(lower=1))
    gap_df = pd.DataFrame({"municipio": df["municipio"], "valuation_gap": gap})
    st.bar_chart(gap_df.groupby("municipio").mean(numeric_only=True))

st.subheader("Gráfico predictivo por municipio y tipología")
if forecast_path.exists():
    ff = pd.read_csv(forecast_path)
    ff["month"] = pd.to_datetime(ff["month"])

    municipios = sorted(ff["municipio"].unique().tolist())
    municipio_sel = st.selectbox("Municipio", municipios)
    tipologias = sorted(ff[ff["municipio"] == municipio_sel]["tipologia"].unique().tolist())
    tipologia_sel = st.selectbox("Tipología", tipologias)

    curve = ff[(ff["municipio"] == municipio_sel) & (ff["tipologia"] == tipologia_sel)].copy()
    chart_df = curve.pivot_table(index="month", columns="series", values="forecast_price_per_m2", aggfunc="mean")
    st.line_chart(chart_df)
else:
    st.info("No hay forecast aún. Ejecuta de nuevo el pipeline.")

st.subheader("Insights históricos abiertos")
if historical_path.exists():
    h = pd.read_csv(historical_path)
    h["date"] = pd.to_datetime(h["date"])
    st.dataframe(h.sort_values("date", ascending=False).head(20), use_container_width=True)

    last = h.sort_values("date").groupby(["indicator", "scope"], as_index=False).tail(1)
    st.caption("Último valor por indicador (INE / MITMA / BdE u otras fuentes que cargues en CSV).")
    st.dataframe(last[["indicator", "scope", "date", "series_value"]], use_container_width=True)
else:
    st.info("Sin históricos. Añade CSV en config.historical.sources.")

st.subheader("Briefing IA")
if brief_path.exists():
    st.text(brief_path.read_text(encoding="utf-8"))
else:
    st.info("No hay briefing generado aún.")
